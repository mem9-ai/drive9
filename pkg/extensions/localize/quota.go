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
	// cachedTotal/refusals/reserved/pending.
	mu          sync.Mutex
	used        int64
	lastCheck   int64
	cachedFree  int64
	cachedTotal int64
	refusals    int64
	// reserved maps a backing FILE (by identity when the caller has a
	// descriptor, else by path) to the end its in-flight writes have booked:
	// two handles appending to one file must not both charge the same growth,
	// and a rename must not move the booking to whatever now sits at the old
	// path. An entry retires when the last booking settles.
	reserved map[string]*pendingReservation
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

// pendingReservation is one file's shared in-flight booking: the furthest end
// its writers have booked, and how many of them still have to settle.
type pendingReservation struct {
	end      int64
	bookings int
}

// writeReservation records one write's reservation: the key it is booked under,
// the growth admitted for it, and the size the file had before the write. A
// write settles on completion (success or failure) against the file's resulting
// size, so the accounting follows what is actually on disk.
type writeReservation struct {
	key      string
	admitted int64
	preSize  int64
}

// resizeReservation records a truncate's movement: bytes admitted (growth) or
// released (shrinkage), so a failed truncate can be undone.
type resizeReservation struct {
	admitted int64
	released int64
}

// admitWrite accounts a PATH-addressed write of writeLen bytes ending at end.
// The size read, the concurrent-reservation check and the quota decision share
// one lock, so two handles appending to one file cannot charge the same growth
// twice.
func (q *overlayQuota) admitWrite(rel string, end, writeLen int64) (writeReservation, error) {
	if q == nil {
		return writeReservation{}, nil
	}
	key := "path:" + rel
	q.mu.Lock()
	defer q.mu.Unlock()
	cur, err := q.sizeFunc(q.root, rel)
	if err != nil {
		return writeReservation{}, err
	}
	return q.admitLocked(key, cur, end, writeLen)
}

// admitFileWrite accounts a write through an OPEN descriptor: the size comes
// from the descriptor (fstat) and the reservation is keyed by the file's
// identity, so a rename between the admission and the write cannot move the
// accounting to whatever now sits at the old path.
func (q *overlayQuota) admitFileWrite(f *os.File, rel string, end, writeLen int64) (writeReservation, error) {
	if q == nil {
		return writeReservation{}, nil
	}
	key, cur, err := fileReservation(f, rel)
	if err != nil {
		return writeReservation{}, err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.admitLocked(key, cur, end, writeLen)
}

// admitLocked books the admission for key. cur is the on-disk size; an
// outstanding reservation for the same key (another handle appending to the
// same file) extends it so the growth is only charged once. The caller must
// hold q.mu.
func (q *overlayQuota) admitLocked(key string, cur, end, writeLen int64) (writeReservation, error) {
	booking := q.reserved[key]
	if booking != nil {
		if booking.end > cur {
			cur = booking.end
		} else {
			// The file caught up: the booking is spent.
			booking = nil
			delete(q.reserved, key)
		}
	}
	res := writeReservation{key: key, preSize: cur}
	growth := int64(0)
	if end > cur {
		growth = end - cur
	}
	required := growth
	if required <= 0 {
		required = writeLen
	}
	if err := q.admitGrowthLocked(growth, required); err != nil {
		return writeReservation{}, err
	}
	res.admitted = growth
	if growth > 0 {
		q.used += growth
	}
	if q.reserved == nil {
		q.reserved = make(map[string]*pendingReservation)
	}
	if booking == nil {
		booking = &pendingReservation{}
		q.reserved[key] = booking
	}
	if end > booking.end {
		booking.end = end
	}
	booking.bookings++
	return res, nil
}

// fileReservation resolves a descriptor to its reservation key and current size
// (fstat): the descriptor is the truth after a rename, and on platforms with a
// stable identity the booking follows the FILE, not the name.
func fileReservation(f *os.File, rel string) (string, int64, error) {
	info, err := f.Stat()
	if err != nil {
		return "", 0, err
	}
	if key, ok := fileReservationKey(f, rel); ok {
		return key, info.Size(), nil
	}
	return "path:" + rel, info.Size(), nil
}

// settleWrite settles a reservation against the file's RESULTING size: the
// charge keeps exactly the bytes that landed (a partial write, a real sparse
// gap, or another writer's successful append to the same file) and returns the
// rest. The landed bytes also leave the free-space window's cached estimate, so
// a burst cannot cumulatively breach the floor.
func (q *overlayQuota) settleWrite(res writeReservation, resultingSize int64) {
	if q == nil || res.key == "" {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	landed := int64(0)
	if resultingSize > res.preSize {
		landed = resultingSize - res.preSize
	}
	if landed > res.admitted {
		landed = res.admitted
	}
	if unused := res.admitted - landed; unused > 0 {
		q.used -= unused
		if q.used < 0 {
			q.used = 0
		}
	}
	if landed > 0 && q.cachedTotal > 0 {
		q.cachedFree -= landed
	}
	// Retire THIS booking. The entry survives while another writer still has
	// to settle (it may have booked the same end), and then holds the furthest
	// end seen so far.
	if booking := q.reserved[res.key]; booking != nil {
		booking.bookings--
		if resultingSize > booking.end || booking.bookings <= 0 {
			delete(q.reserved, res.key)
		}
	}
}

// resize accounts a truncate to newSize: growth is admitted, shrinkage
// released. The size read shares the admission lock; a refused resize leaves
// outstanding reservations alone (nothing moved on disk).
func (q *overlayQuota) resize(rel string, newSize int64) (resizeReservation, error) {
	if q == nil {
		return resizeReservation{}, nil
	}
	key := "path:" + rel
	q.mu.Lock()
	defer q.mu.Unlock()
	cur, err := q.sizeFunc(q.root, rel)
	if err != nil {
		return resizeReservation{}, err
	}
	var out resizeReservation
	if newSize > cur {
		growth := newSize - cur
		if err := q.admitGrowthLocked(growth, growth); err != nil {
			// Refused: the file is untouched, so pending reservations stand.
			return resizeReservation{}, err
		}
		q.used += growth
		out.admitted = growth
	} else {
		freed := cur - newSize
		q.used -= freed
		if q.used < 0 {
			q.used = 0
		}
		out.released = freed
	}
	// The truncate makes the on-disk size authoritative for this path.
	delete(q.reserved, key)
	return out, nil
}

// admitGrowthLocked applies growth to the byte quota and checks the free-space
// floor for this write (required). It does not move used/cachedFree: the caller
// owns the reservation bookkeeping.
func (q *overlayQuota) admitGrowthLocked(growth, required int64) error {
	if growth < 0 {
		growth = 0
	}
	if required <= 0 {
		required = 0
	}
	if q.limit > 0 && growth > 0 && q.used+growth > q.limit {
		q.refuse("reason=byte_quota_exceeded",
			zap.Int64("runtime_used_bytes", q.used),
			zap.Int64("required_bytes", growth),
			zap.Int64("quota_bytes", q.limit))
		return syscall.ENOSPC
	}
	return q.checkFreeRatioLocked(required)
}

// release returns n bytes of usage (shrink, unlink, or an admission that never
// landed).
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

// forgetPath drops a removed (or truncated-away) name's reservation so a later
// create of the same path charges again instead of inheriting the watermark.
func (q *overlayQuota) forgetPath(rel string) {
	if q == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.reserved, "path:"+rel)
}

// forceAdmit counts n bytes that are on disk but were never admitted (the
// revert of a released shrinkage whose truncate then failed). It cannot be
// refused: the bytes exist, so under-counting would let the overlay exceed the
// cap.
func (q *overlayQuota) forceAdmit(n int64) {
	if q == nil || n <= 0 {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.used += n
}

// isReserved reports whether a write is currently booked for key (tests).
func (q *overlayQuota) isReserved(key string) bool {
	if q == nil {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.reserved[key]
	return ok
}

// checkFreeRatioLocked enforces the free-space guard, reusing the cached
// capacity inside the throttle window. The caller must hold q.mu.
func (q *overlayQuota) checkFreeRatioLocked(required int64) error {
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
