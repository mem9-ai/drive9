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
	// cachedTotal/refusals and reserved.
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

// pendingReservation is one file's shared in-flight booking: the size it had
// when the first writer booked, the furthest end booked since, and how many
// writers still have to settle. The LAST settler reconciles `used` against the
// file's real size, which makes every interleaving (one writer failing, both
// failing, one landing while the other fails) exact without per-writer refunds.
//
// A booking is addressed by IDENTITY (the *pendingReservation), not by map key:
// a later admission can legitimately replace the map entry while an earlier
// writer is still in flight, and that writer must settle its own booking, never
// the newcomer's. Retired bookings stay reachable through their reservations and
// settle as a no-op.
type pendingReservation struct {
	startSize int64
	end       int64
	bookings  int
	retired   bool
}

// writeReservation records one write's reservation: the booking it owns (by
// identity, not by key), the growth admitted for it, and the size the file had
// before the write. A write settles on completion (success or failure) against
// the file's resulting size, so the accounting follows what is actually on
// disk.
type writeReservation struct {
	key      string
	booking  *pendingReservation
	admitted int64
	preSize  int64
	// file is the descriptor a handle write went through, so settlement reads
	// the resulting size from it (a rename cannot redirect it).
	file *os.File
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
	// The fstat runs INSIDE the admission lock and yields both the size and the
	// identity key: a write that lands between an outside stat and the lock
	// would otherwise be charged twice.
	q.mu.Lock()
	defer q.mu.Unlock()
	info, err := f.Stat()
	if err != nil {
		return writeReservation{}, err
	}
	key := "path:" + rel
	if id, ok := fileReservationKey(f, rel); ok {
		key = id
	}
	res, aerr := q.admitLocked(key, info.Size(), end, writeLen)
	res.file = f
	return res, aerr
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
			// The file caught up: retire the old booking (its bytes are all on
			// disk, so there is nothing to refund) and start a fresh one.
			q.retireLocked(key, booking, cur)
			booking = nil
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
		// Drain the free-space estimate pessimistically: a concurrent burst
		// must not each pass the floor against the same cached sample. The
		// reconciler adds back whatever never lands.
		if q.cachedTotal > 0 {
			q.cachedFree -= growth
		}
	}
	if q.reserved == nil {
		q.reserved = make(map[string]*pendingReservation)
	}
	if booking == nil {
		booking = &pendingReservation{startSize: res.preSize}
		q.reserved[key] = booking
	}
	if end > booking.end {
		booking.end = end
	}
	booking.bookings++
	res.booking = booking
	return res, nil
}

// retireLocked reconciles a booking against size and retires it: the overhang
// (booked bytes not on disk) leaves `used` and returns to the free-space
// estimate, and the owning writers' later settlements become no-ops. The caller
// must hold q.mu.
func (q *overlayQuota) retireLocked(key string, booking *pendingReservation, size int64) {
	if booking == nil {
		return
	}
	if overhang := booking.end - size; overhang > 0 {
		q.used -= overhang
		if q.used < 0 {
			q.used = 0
		}
		if q.cachedTotal > 0 {
			q.cachedFree += overhang
		}
	}
	booking.retired = true
	if q.reserved[key] == booking {
		delete(q.reserved, key)
	}
}

// settleWrite settles one writer's booking against the file's RESULTING size.
// Only the LAST settler for a file reconciles the charge: it removes the
// overhang (the bytes booked but not on disk, which is exactly a failed or
// partial write) and takes the landed growth out of the free-space window's
// estimate. Intermediate settlers do nothing, so a shared booking cannot be
// refunded by one writer while another still owns (or later lands) the bytes.
func (q *overlayQuota) settleWrite(res writeReservation) {
	if q == nil || res.booking == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	booking := res.booking
	if booking.retired {
		// A mutation (truncate/rename/unlink) or a later admission already
		// reconciled this booking against the size it had then.
		return
	}
	booking.bookings--
	if booking.bookings > 0 {
		return
	}
	size, err := q.resultingSize(res)
	if err != nil {
		// Fail closed: keep the charge (the bytes may exist) but drop the
		// entry, so a phantom end cannot inflate a later admission's base.
		q.retireEntryLocked(res.key, booking)
		return
	}
	q.retireLocked(res.key, booking, size)
}

// retireEntryLocked drops a booking without reconciling (used keeps the
// charge); settlement could not measure the file. The caller must hold q.mu.
func (q *overlayQuota) retireEntryLocked(key string, booking *pendingReservation) {
	if booking == nil {
		return
	}
	booking.retired = true
	if q.reserved[key] == booking {
		delete(q.reserved, key)
	}
}

// resultingSize reports the file's size after the write: the descriptor's when
// the reservation carries one, else the backing path's.
func (q *overlayQuota) resultingSize(res writeReservation) (int64, error) {
	if res.file != nil {
		info, err := res.file.Stat()
		if err != nil {
			return 0, err
		}
		return info.Size(), nil
	}
	return q.sizeFunc(q.root, strings.TrimPrefix(res.key, "path:"))
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
	// An in-flight writer's charge is backed by the bytes this resize is about
	// to move: retire it against the PRE-resize size so its own settlement
	// cannot refund the shrink a second time.
	q.retireLocked(key, q.reserved[key], cur)
	var out resizeReservation
	if newSize > cur {
		growth := newSize - cur
		if err := q.admitGrowthLocked(growth, growth); err != nil {
			// Refused: the file is untouched (the booking was retired above
			// against that same size, so the accounting is unchanged).
			return resizeReservation{}, err
		}
		q.used += growth
		if q.cachedTotal > 0 {
			q.cachedFree -= growth
		}
		out.admitted = growth
	} else {
		freed := cur - newSize
		q.used -= freed
		if q.used < 0 {
			q.used = 0
		}
		out.released = freed
	}
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

// retirePath reconciles and retires a name's booking against the size the file
// has at the mutation boundary, so a later create of the same path charges from
// zero instead of inheriting the old end.
func (q *overlayQuota) retirePath(rel string, size int64) {
	if q == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	key := "path:" + rel
	if booking, ok := q.reserved[key]; ok {
		q.retireLocked(key, booking, size)
	}
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
