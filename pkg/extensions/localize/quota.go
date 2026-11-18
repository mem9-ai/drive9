package localize

import (
	"context"
	"errors"
	"io/fs"
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
	// cachedTotal/refusals.
	mu          sync.Mutex
	used        int64
	lastCheck   int64
	cachedFree  int64
	cachedTotal int64
	refusals    int64
}

// newOverlayQuota builds the quota for root. Both bounds disabled returns nil
// (the pre-quota behavior). The starting usage is the current size of the
// backing tree, so a mount over an existing overlay accounts for it.
func newOverlayQuota(root string, limit int64, freeRatio float64) (*overlayQuota, error) {
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

// admit reserves n bytes of growth. It returns ENOSPC when the byte quota or
// the free-space guard would be breached; the caller propagates that errno to
// the application instead of filling the disk.
func (q *overlayQuota) admit(n int64) error {
	if q == nil || n <= 0 {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.limit > 0 && q.used+n > q.limit {
		q.refuse("reason=byte_quota_exceeded",
			zap.Int64("runtime_used_bytes", q.used),
			zap.Int64("required_bytes", n),
			zap.Int64("quota_bytes", q.limit))
		return syscall.ENOSPC
	}
	if err := q.checkFreeRatioLocked(n); err != nil {
		return err
	}
	q.used += n
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

// grow accounts a write that extends a backing file to newSize. An overwrite
// that stays inside the existing size costs nothing: the file keeps its
// bytes, so nothing is released.
func (q *overlayQuota) grow(rel string, newSize int64) error {
	if q == nil {
		return nil
	}
	cur, err := q.sizeFunc(q.root, rel)
	if err != nil {
		return err
	}
	if newSize <= cur {
		return nil
	}
	return q.admit(newSize - cur)
}

// resize accounts a truncate: growth is admitted, shrinkage released.
func (q *overlayQuota) resize(rel string, newSize int64) error {
	if q == nil {
		return nil
	}
	cur, err := q.sizeFunc(q.root, rel)
	if err != nil {
		return err
	}
	if newSize <= cur {
		q.release(cur - newSize)
		return nil
	}
	return q.admit(newSize - cur)
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

// measure sums the current backing tree size.
func (q *overlayQuota) measure() (int64, error) {
	var total int64
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
		if info.Mode().IsRegular() {
			total += info.Size()
		}
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
