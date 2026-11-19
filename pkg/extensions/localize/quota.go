package localize

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
)

// The local overlay writes straight to the host filesystem, so without bounds
// a single build that materializes node_modules (or any other local-only
// subtree) can fill the host disk — and take every other process in the same
// sandbox down with the mount. overlayQuota is the byte budget plus free-space
// guard for one overlay root, modelled on the write-back shadow store's quota
// (byte cap + free-space ratio, ENOSPC when either would be breached).

// defaultOverlayFreeRatio is the minimum free-space ratio the overlay keeps on
// the host filesystem. It matches the write-back shadow store's default so a
// mount does not reserve 10% for one local plane and nothing for the other.
const defaultOverlayFreeRatio = 0.10

// overlayDiskCheckInterval throttles the Statfs syscall: the free-space check
// is re-evaluated from the cached capacity inside the window.
const overlayDiskCheckInterval = 5 * time.Second

// overlayQuota bounds one overlay backing root. A nil *overlayQuota is a valid
// disabled quota (every method accepts it).
type overlayQuota struct {
	root      string
	limit     int64
	freeRatio float64

	// sizeFunc resolves the current on-disk size of a backing path. It is a
	// field so tests can drive capacity without allocating real files.
	sizeFunc func(root, rel string) (int64, error)
	statfs   func(root string) (total, free int64, err error)

	// mu serializes admission and guards used/lastCheck/cachedFree/
	// cachedTotal/refusals/reserved.
	mu          sync.Mutex
	used        int64
	lastCheck   int64
	cachedFree  int64
	cachedTotal int64
	refusals    int64
	// reserved maps a backing path to the furthest end a write has reserved
	// but that has not reached the file yet. Two handles appending to one file
	// would otherwise both read the same on-disk size and both charge the same
	// growth; entries are dropped as soon as the file's real size catches up.
	reserved map[string]int64
}

// newOverlayQuota builds the quota for root. Both bounds disabled returns nil
// (the pre-quota behavior). The starting usage is the current size of the
// backing tree, so a mount over an existing overlay accounts for it.
func newOverlayQuota(root string, limit int64, freeRatio float64) (*overlayQuota, error) {
	if math.IsNaN(freeRatio) || freeRatio >= 1 {
		// NaN compares false against everything, so it would silently disarm
		// the floor while the caller believes one is armed.
		return nil, fmt.Errorf("localize: overlay free-space ratio must be a finite value in [0,1), got %v", freeRatio)
	}
	if freeRatio < 0 {
		freeRatio = 0
	}
	if strings.TrimSpace(root) == "" || (limit <= 0 && freeRatio <= 0) {
		return nil, nil
	}
	q := &overlayQuota{
		root:      root,
		limit:     limit,
		freeRatio: freeRatio,
		sizeFunc:  statBackingSize,
		statfs:    statfsCapacity,
	}
	if limit <= 0 {
		// Only the free-space guard is armed: no per-byte accounting, and no
		// walk of a possibly huge overlay tree at mount time.
		return q, nil
	}
	used, err := q.measure()
	if err != nil {
		// An unreadable subtree must not fail the mount: start from zero and
		// let the running deltas bound new writes.
		logger.Warn(context.Background(), "localize_quota_seed_failed",
			zap.String("root", root), zap.Error(err))
		return q, nil
	}
	q.used = used
	return q, nil
}

// Used reports the bytes currently accounted to the overlay.
func (q *overlayQuota) Used() int64 {
	if q == nil {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.used
}

// Refusals reports how many writes the quota refused.
func (q *overlayQuota) Refusals() int64 {
	if q == nil {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.refusals
}

// writeReservation records the byte-quota movement one write made and the
// size it started from, so a write that fails part-way can hand back exactly
// the bytes it never landed.
type writeReservation struct {
	admitted int64
	preSize  int64
}

// resizeReservation records a truncate's movement: bytes admitted (growth) or
// released (shrinkage), so a failed truncate can be undone.
type resizeReservation struct {
	admitted int64
	released int64
}

// admitWrite accounts a write of writeLen bytes ending at end for the backing
// file rel. The size read, the byte-quota decision and the free-space estimate
// update all run under ONE lock: two handles appending to the same overlay file
// cannot both charge the same growth, and a concurrent truncate cannot slip
// between the read and the reservation.
func (q *overlayQuota) admitWrite(rel string, end, writeLen int64) (writeReservation, error) {
	if q == nil {
		return writeReservation{}, nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	cur, err := q.sizeFunc(q.root, rel)
	if err != nil {
		return writeReservation{}, err
	}
	// A concurrent append may already have reserved past the on-disk size.
	if reservedEnd, ok := q.reserved[rel]; ok {
		if reservedEnd > cur {
			cur = reservedEnd
		} else {
			delete(q.reserved, rel)
		}
	}
	res := writeReservation{preSize: cur}
	growth := int64(0)
	if end > cur {
		growth = end - cur
	}
	if err := q.admitLocked(growth, growth, writeLen); err != nil {
		return writeReservation{}, err
	}
	res.admitted = growth
	if growth > 0 {
		if q.reserved == nil {
			q.reserved = make(map[string]int64)
		}
		q.reserved[rel] = end
	}
	return res, nil
}

// resize accounts a truncate to newSize: growth is admitted, shrinkage
// released. The size read shares the admission lock.
func (q *overlayQuota) resize(rel string, newSize int64) (resizeReservation, error) {
	if q == nil {
		return resizeReservation{}, nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	cur, err := q.sizeFunc(q.root, rel)
	if err != nil {
		return resizeReservation{}, err
	}
	// A truncate makes the on-disk size authoritative again.
	delete(q.reserved, rel)
	if newSize <= cur {
		freed := cur - newSize
		q.used -= freed
		if q.used < 0 {
			q.used = 0
		}
		return resizeReservation{released: freed}, nil
	}
	growth := newSize - cur
	if err := q.admitLocked(growth, growth, growth); err != nil {
		return resizeReservation{}, err
	}
	return resizeReservation{admitted: growth}, nil
}

// admitLocked applies growth to the byte quota and the free-space floor. The
// caller must hold q.mu. required is the quantity the single-write floor check
// judges (the write's length when it grows nothing), while only the admitted
// GROWTH is persisted into the cached free estimate: an in-place overwrite
// consumes no new disk bytes and must not drain it.
func (q *overlayQuota) admitLocked(growth, required, writeLen int64) error {
	if growth < 0 {
		growth = 0
	}
	if required <= 0 {
		required = writeLen
	}
	if required < 0 {
		required = 0
	}
	if q.limit > 0 && growth > 0 && q.used+growth > q.limit {
		q.refuse("reason=byte_quota_exceeded",
			zap.Int64("runtime_used_bytes", q.used),
			zap.Int64("required_bytes", growth),
			zap.Int64("quota_bytes", q.limit))
		return syscall.ENOSPC
	}
	if err := q.checkFreeRatioLocked(required, growth); err != nil {
		return err
	}
	q.used += growth
	return nil
}

// release returns n bytes of usage (shrink, truncate, unlink).
func (q *overlayQuota) release(n int64) {
	if q == nil || n <= 0 {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.used -= n
	if q.used < 0 {
		q.used = 0
	}
}

// lowerReservation drops a path's reserved end after a write landed less than
// it reserved, so the next write starts from the bytes that actually exist.
func (q *overlayQuota) lowerReservation(rel string, end int64) {
	if q == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if end <= 0 {
		delete(q.reserved, rel)
		return
	}
	q.reserved[rel] = end
}

// forgetPath drops a path's reservation (the name is going away, so a later
// create must start from zero instead of inheriting the old watermark).
func (q *overlayQuota) forgetPath(rel string) {
	if q == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.reserved, rel)
}

// forceAdmit counts n bytes that are on disk but were never admitted (the
// revert of a released shrinkage whose truncate then failed). It cannot be
// refused: the bytes exist, so under-counting would let the overlay exceed the
// cap. It is the failure-path twin of admitLocked.
func (q *overlayQuota) forceAdmit(n int64) {
	if q == nil || n <= 0 {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.used += n
}

// checkFreeRatioLocked enforces the free-space guard, reusing the cached
// capacity inside the throttle window. required is judged for THIS write;
// only growth (the bytes it actually adds) is persisted into the cached free
// estimate. The caller must hold q.mu.
func (q *overlayQuota) checkFreeRatioLocked(required, growth int64) error {
	if q.freeRatio <= 0 {
		return nil
	}
	now := time.Now().UnixNano()
	if q.cachedTotal <= 0 || now-q.lastCheck >= int64(overlayDiskCheckInterval) {
		total, free, err := q.statfs(q.root)
		if err != nil {
			// Statfs failed: cannot check, allow the write.
			return nil
		}
		q.cachedTotal, q.cachedFree, q.lastCheck = total, free, now
	}
	total, free := q.cachedTotal, q.cachedFree
	if total <= 0 {
		return nil
	}
	// free is the LIVE estimate for the whole throttle window: every admission
	// that actually grows the tree subtracts exactly what it grew, so a burst
	// of appends cannot each pass the floor against the same starting value.
	// An in-place OVERWRITE grows nothing and therefore must not drain the
	// estimate; it only has to pass this write's own floor check.
	freeAfter := free - required
	ratioAfter := float64(freeAfter) / float64(total)
	if ratioAfter < q.freeRatio {
		q.refuse("reason=free_ratio_exceeded",
			zap.Int64("free_bytes", free),
			zap.Int64("required_bytes", required),
			zap.Float64("free_ratio", ratioAfter),
			zap.Float64("threshold", q.freeRatio))
		return syscall.ENOSPC
	}
	if growth > 0 {
		q.cachedFree = free - growth
	}
	return nil
}

// refuse logs one refusal. The message format mirrors the write-back shadow
// store's quota line so both refusals are greppable the same way.
func (q *overlayQuota) refuse(reason string, fields ...zap.Field) {
	q.refusals++
	fields = append(fields, zap.String("root", q.root), zap.String("localize_quota", reason))
	logger.Warn(context.Background(), "localize overlay write refused", fields...)
}

// measure sums the current backing tree size. A hardlinked file is counted
// once per INODE: the release path only returns bytes on the last name, so
// counting every name here would leave phantom usage after both are removed.
func (q *overlayQuota) measure() (int64, error) {
	var total int64
	var seen map[fileIdentity]struct{}
	err := filepath.WalkDir(q.root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			if errors.Is(ierr, fs.ErrNotExist) {
				return nil
			}
			return ierr
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		if id, ok := linkedFileIdentity(info); ok {
			if seen == nil {
				seen = make(map[fileIdentity]struct{})
			}
			if _, dup := seen[id]; dup {
				return nil
			}
			seen[id] = struct{}{}
		}
		total += info.Size()
		return nil
	})
	return total, err
}

// statBackingSize reports the on-disk size of <root>/<rel>, or 0 when the file
// does not exist yet.
func statBackingSize(root, rel string) (int64, error) {
	info, err := os.Lstat(filepath.Join(root, rel))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	if !info.Mode().IsRegular() {
		return 0, nil
	}
	return info.Size(), nil
}

// statfsCapacity reports the filesystem capacity behind root.
func statfsCapacity(root string) (int64, int64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(root, &stat); err != nil {
		return 0, 0, err
	}
	total := int64(stat.Blocks) * int64(stat.Bsize)
	free := int64(stat.Bavail) * int64(stat.Bsize)
	return total, free, nil
}
