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
	// reserved maps an admission key (the backing path, or the file's inode
	// key for handle writes) to its in-flight booking, so two writers of one
	// file do not charge the same growth twice.
	reserved map[string]*pendingReservation
	// byPath indexes live bookings by view path, so a mutation retires the
	// writers of a NAME regardless of the key their admission used (handle
	// writes book by inode).
	byPath map[string]map[*pendingReservation]struct{}
	// unlanded is the admitted growth whose booking has not been resolved yet.
	// A Statfs sample already includes landed bytes, so refreshing
	// `cachedFree = free - unlanded` keeps in-flight admissions accounted
	// WITHOUT subtracting history (which would make the floor fire forever).
	unlanded int64
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

// pendingReservation is one file's shared in-flight booking. It is reachable
// three ways: from the map by its admission key (path or inode), from the
// per-path index (so a mutation can retire every writer of a name), and from
// each writer's reservation. The LAST settler reconciles the charge against the
// file's real size; a booking retired by a mutation keeps enough state to
// re-charge bytes that land after it.
type pendingReservation struct {
	key           string
	path          string // backing-relative view path (may be "")
	startSize     int64  // the file's size when the first writer booked
	end           int64  // the furthest end booked
	admittedTotal int64  // growth admitted across all of its writers
	bookings      int    // writers still to settle
	retired       bool
	// baseline is the file size AFTER the mutation that retired the booking
	// (0 when the mutation released the bytes). A retired writer that lands
	// later is charged only the growth beyond it.
	baseline int64
	// retiredCharged is the high-water growth already charged for a retired
	// booking: several writers share one booking, and each of them settling the
	// same landed extent must not charge it again.
	retiredCharged int64
}

// writeReservation records one write's reservation: the booking it owns (by
// identity, not by key), the growth admitted for it, and the size the file had
// before the write. A write settles on completion (success or failure) against
// the file's resulting size, so the accounting follows what is actually on
// disk.
type writeReservation struct {
	key     string
	path    string
	booking *pendingReservation
	// admitted is the growth this writer paid for; the booking holds the total.
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

// charge reserves growth with NO booking. It is the truncating write's form: the
// file is about to be reset, so there is no base size a booking could be settled
// against — the caller releases the unlanded remainder explicitly.
func (q *overlayQuota) charge(growth int64) error {
	if q == nil || growth <= 0 {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.admitGrowthLocked(growth, growth); err != nil {
		return err
	}
	q.used += growth
	q.chargeFreeEstimateLocked(growth)
	return nil
}

// admitFileWrite accounts a write through an OPEN descriptor: one fstat (under
// the admission lock) yields both the size and the identity key, so a write
// landing between an outside stat and the lock cannot be charged twice.
func (q *overlayQuota) admitFileWrite(f *os.File, rel string, end, writeLen int64) (writeReservation, error) {
	if q == nil {
		return writeReservation{}, nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	info, err := f.Stat()
	if err != nil {
		return writeReservation{}, err
	}
	key := "path:" + rel
	if id, ok := fileIdentityOf(info); ok {
		key = id
	}
	res, aerr := q.admitLocked(key, rel, info.Size(), end, writeLen)
	res.file = f
	return res, aerr
}

// fileIdentityOf derives the admission key from the FileInfo the admission
// already fetched: one stat per admitted write, not two.
func fileIdentityOf(info fs.FileInfo) (string, bool) {
	return fileReservationKey(info)
}

// admitLocked books the admission. cur is the on-disk size; an outstanding
// booking on the same key (another handle writing the same file) extends it so
// the growth is charged once. The caller must hold q.mu.
func (q *overlayQuota) admitLocked(key, rel string, cur, end, writeLen int64) (writeReservation, error) {
	booking := q.reserved[key]
	if booking != nil {
		if booking.end > cur {
			cur = booking.end
		} else {
			// The file caught up: detach the spent booking (its writers still
			// reconcile against the real size) and start a fresh one.
			q.detachLocked(booking)
			booking = nil
		}
	}
	res := writeReservation{key: key, path: rel, preSize: cur}
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
		q.chargeFreeEstimateLocked(growth)
	}
	if q.reserved == nil {
		q.reserved = make(map[string]*pendingReservation)
	}
	if booking == nil {
		booking = &pendingReservation{key: key, path: rel, startSize: res.preSize}
		q.reserved[key] = booking
		if rel != "" {
			if q.byPath == nil {
				q.byPath = make(map[string]map[*pendingReservation]struct{})
			}
			set := q.byPath[rel]
			if set == nil {
				set = make(map[*pendingReservation]struct{})
				q.byPath[rel] = set
			}
			set[booking] = struct{}{}
		}
	}
	if end > booking.end {
		booking.end = end
	}
	booking.admittedTotal += growth
	booking.bookings++
	res.booking = booking
	return res, nil
}

// detachLocked removes a booking from both indexes without reconciling: its
// writers' settlements still reconcile (or re-charge a post-detach landing), and
// a later admission at the same key starts from the on-disk size instead of
// inheriting the old end. The caller must hold q.mu.
func (q *overlayQuota) detachLocked(booking *pendingReservation) {
	if booking == nil {
		return
	}
	if q.reserved[booking.key] == booking {
		delete(q.reserved, booking.key)
	}
	if set := q.byPath[booking.path]; set != nil {
		delete(set, booking)
		if len(set) == 0 {
			delete(q.byPath, booking.path)
		}
	}
}

// retireLocked reconciles a booking against the size the file had AT the
// mutation boundary and retires it: the admitted bytes that are not on disk
// leave `used` and go back to the free-space estimate. A retired writer that
// lands bytes afterwards is re-charged by its own settlement. The caller must
// hold q.mu.
func (q *overlayQuota) retireLocked(booking *pendingReservation, size, baseline int64) {
	if booking == nil {
		return
	}
	q.detachLocked(booking)
	landed := int64(0)
	if size > booking.startSize {
		landed = size - booking.startSize
	}
	if landed > booking.admittedTotal {
		landed = booking.admittedTotal
	}
	unused := booking.admittedTotal - landed
	if unused > 0 {
		q.used -= unused
		if q.used < 0 {
			q.used = 0
		}
	}
	q.retireFreeEstimateLocked(booking.admittedTotal, unused)
	booking.retired = true
	booking.baseline = baseline
}

// settleWrite settles one writer's booking against the file's RESULTING size.
// The last settler of a live booking reconciles the whole booking; a settler of
// a retired booking re-charges bytes that landed after the retirement.
func (q *overlayQuota) settleWrite(res writeReservation) {
	if q == nil || res.booking == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	booking := res.booking
	size, err := q.resultingSize(res)
	if err != nil {
		// Fail closed: keep the charge (the bytes may exist) but detach, so a
		// phantom end cannot inflate a later admission's base.
		q.detachLocked(booking)
		return
	}
	if booking.retired {
		// Charge only growth beyond what this booking already accounted: two
		// writers sharing it each settle once, and both must see the same
		// landed extent charged exactly once.
		if extra := size - booking.baseline; extra > booking.retiredCharged {
			delta := extra - booking.retiredCharged
			booking.retiredCharged = extra
			q.used += delta
			q.drainFreeEstimateLocked(delta)
		}
		return
	}
	booking.bookings--
	if booking.bookings > 0 {
		return
	}
	q.retireLocked(booking, size, 0)
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
// released. Retroactive bookings are retired against the PRE-resize size only
// once the resize is admitted, so a refusal leaves them untouched.
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
	var out resizeReservation
	if newSize > cur {
		growth := newSize - cur
		if err := q.admitGrowthLocked(growth, growth); err != nil {
			return resizeReservation{}, err
		}
		out.admitted = growth
		q.retirePathLocked(rel, cur, newSize)
		q.used += growth
		q.chargeFreeEstimateLocked(growth)
		return out, nil
	}
	out.released = cur - newSize
	q.retirePathLocked(rel, cur, newSize)
	q.used -= out.released
	if q.used < 0 {
		q.used = 0
	}
	return out, nil
}

// retirePathLocked retires every booking reachable under a path (path-keyed AND
// inode-keyed writers) against size. The caller must hold q.mu.
func (q *overlayQuota) retirePathLocked(rel string, size, baseline int64) {
	for booking := range q.byPath[rel] {
		q.retireLocked(booking, size, baseline)
	}
	if booking := q.reserved["path:"+rel]; booking != nil {
		q.retireLocked(booking, size, baseline)
	}
}

// retirePath retires a name's bookings against the size it had at the mutation
// boundary (a later create charges from disk again).
func (q *overlayQuota) retirePath(rel string, size, baseline int64) {
	if q == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.retirePathLocked(rel, size, baseline)
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

// chargeFreeEstimateLocked drains the free-space estimate for admitted growth
// and counts it as unresolved (in flight). The caller must hold q.mu.
func (q *overlayQuota) chargeFreeEstimateLocked(n int64) {
	if n <= 0 {
		return
	}
	q.unlanded += n
	q.drainFreeEstimateLocked(n)
}

// drainFreeEstimateLocked lowers the cached free estimate by n bytes. The caller
// must hold q.mu.
func (q *overlayQuota) drainFreeEstimateLocked(n int64) {
	if n <= 0 {
		return
	}
	if q.cachedTotal > 0 {
		q.cachedFree -= n
	}
}

// refundFreeEstimateLocked returns freed (landed) bytes to the estimate.
func (q *overlayQuota) refundFreeEstimateLocked(n int64) {
	if n <= 0 {
		return
	}
	if q.cachedTotal > 0 {
		q.cachedFree += n
	}
}

// retireFreeEstimateLocked resolves a booking's admission: admittedTotal leaves
// the in-flight pool (part landed, part refunded), and the refunded part returns
// to the cached estimate. The caller must hold q.mu.
func (q *overlayQuota) retireFreeEstimateLocked(admittedTotal, unused int64) {
	q.unlanded -= admittedTotal
	if q.unlanded < 0 {
		q.unlanded = 0
	}
	if unused > 0 && q.cachedTotal > 0 {
		q.cachedFree += unused
	}
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
	q.refundFreeEstimateLocked(n)
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
	q.chargeFreeEstimateLocked(n)
}

// hasBookingForPath reports whether any in-flight writer is booked under the
// view path — the production (inode-keyed) form (tests).
func (q *overlayQuota) hasBookingForPath(rel string) bool {
	if q == nil {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.byPath[rel]) > 0
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
		q.cachedTotal, q.cachedFree, q.lastCheck = total, free-q.unlanded, now
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
