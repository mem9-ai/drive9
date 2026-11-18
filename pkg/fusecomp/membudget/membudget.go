// Package membudget tracks the in-memory footprint of a mount process
// against one process-wide byte budget.
//
// A mount process runs exactly one mount (cmd/drive9 mounts one mountpoint per
// process), so a Budget instance is a process budget: it covers every
// in-memory component that can grow with workload — the read cache, the
// write-back data cache, the per-open-file write buffers (including their
// base preload) and the inode table.
//
// The budget deliberately does not own the components. Each component charges
// the bytes it retains and releases them when an entry is dropped, which keeps
// the accounting exact instead of relying on periodic samples. Two admission
// paths exist because of lock ordering:
//
//   - Charge is pure accounting. It never runs a reclaimer, so a component may
//     call it while holding its own lock.
//   - Acquire additionally asks registered reclaimers to free bytes. It must
//     only be called from a path that holds no component lock (the write-buffer
//     growth path), so a cache eviction triggered here cannot deadlock against
//     a concurrent cache insert.
//
// A nil *Budget is a valid no-op budget: every method accepts it, so an
// unbudgeted mount runs exactly the pre-budget code path.
package membudget

import (
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// ErrExceeded reports that an allocation would take the process past its
// memory budget and the budget is not in warn mode.
var ErrExceeded = errors.New("membudget: process memory budget exceeded")

// Class names the component a byte charge belongs to.
type Class string

const (
	// ClassReadCache is the in-memory read cache (whole-file and pending
	// payload retention).
	ClassReadCache Class = "read_cache"
	// ClassWriteCache is the write-back cache's in-memory small-payload
	// retention (entries larger than the cutoff are disk-only already).
	ClassWriteCache Class = "write_cache"
	// ClassWriteBuffer is the staged write buffer held by open writable
	// handles, including the base preload taken at open.
	ClassWriteBuffer Class = "write_buffer"
	// ClassInodeTable is the inode table's entry + path storage.
	ClassInodeTable Class = "inode_table"
)

// Mode selects what an over-budget allocation means.
type Mode string

const (
	// ModeEnforce refuses the allocation (the caller degrades or fails the
	// operation). This is the default: it is what bounds RSS.
	ModeEnforce Mode = "enforce"
	// ModeWarn admits the allocation, records the overrun and logs it. The
	// caches still evict on their own quotas, so only buffers and the inode
	// table can actually cross the limit. Use it when failing user I/O is
	// worse than the RSS risk.
	ModeWarn Mode = "warn"
)

// ParseMode resolves a mode name, defaulting to ModeEnforce.
func ParseMode(name string) (Mode, error) {
	switch Mode(name) {
	case "", ModeEnforce:
		return ModeEnforce, nil
	case ModeWarn:
		return ModeWarn, nil
	default:
		return "", errors.New("membudget: unknown budget mode " + name + " (want enforce or warn)")
	}
}

// Reclaimer frees up to target bytes of a reclaimable class and returns how
// many bytes it actually freed. It runs with no budget lock held and may call
// Release; it must not call Acquire or Charge.
type Reclaimer func(target int64) int64

// Budget is one process-wide memory budget. A nil *Budget is a no-op.
type Budget struct {
	limit int64
	mode  Mode
	log   *zap.Logger

	// used is the aggregate of the per-class counters. It is authoritative:
	// admission decisions CAS on it.
	used atomic.Int64

	readCache   atomic.Int64
	writeCache  atomic.Int64
	writeBuffer atomic.Int64
	inodeTable  atomic.Int64
	other       atomic.Int64

	// acquireMu serializes reclamation so two acquisitions cannot both
	// count the same freed bytes.
	acquireMu  sync.Mutex
	reclaimMu  sync.Mutex
	reclaimers map[Class]Reclaimer

	overruns   atomic.Int64
	refusals   atomic.Int64
	lastReport atomic.Int64 // unix nanos of the last overrun log
	peak       atomic.Int64
}

// New builds a budget. A non-positive limit disables it and returns nil, so
// callers can pass the result around without further checks.
func New(limit int64, mode Mode, log *zap.Logger) *Budget {
	if limit <= 0 {
		return nil
	}
	if mode == "" {
		mode = ModeEnforce
	}
	return &Budget{
		limit:      limit,
		mode:       mode,
		log:        log,
		reclaimers: map[Class]Reclaimer{},
	}
}

// Limit reports the configured budget in bytes (0 when disabled).
func (b *Budget) Limit() int64 {
	if b == nil {
		return 0
	}
	return b.limit
}

// Mode reports the enforcement mode (ModeEnforce when disabled).
func (b *Budget) Mode() Mode {
	if b == nil {
		return ModeEnforce
	}
	return b.mode
}

// Used reports the currently charged bytes.
func (b *Budget) Used() int64 {
	if b == nil {
		return 0
	}
	return b.used.Load()
}

// Peak reports the high-water mark of Used.
func (b *Budget) Peak() int64 {
	if b == nil {
		return 0
	}
	return b.peak.Load()
}

// CanFit reports whether n more bytes fit under the limit right now. It is an
// advisory check for optional work (a base preload) that can be skipped
// instead of refused mid-way.
func (b *Budget) CanFit(n int64) bool {
	if b == nil {
		return true
	}
	if n <= 0 {
		return true
	}
	return b.used.Load()+n <= b.limit
}

// OverLimit reports whether the charged bytes currently exceed the limit.
func (b *Budget) OverLimit() bool {
	if b == nil {
		return false
	}
	return b.used.Load() > b.limit
}

// UsedBy reports the bytes charged to one class.
func (b *Budget) UsedBy(class Class) int64 {
	if b == nil {
		return 0
	}
	return b.counter(class).Load()
}

// Overruns reports how many allocations crossed the limit (admitted in warn
// mode) and Refusals how many were refused.
func (b *Budget) Overruns() int64 {
	if b == nil {
		return 0
	}
	return b.overruns.Load()
}

// Refusals reports how many allocations were refused by the budget.
func (b *Budget) Refusals() int64 {
	if b == nil {
		return 0
	}
	return b.refusals.Load()
}

// RegisterReclaimer installs (or with nil clears) the reclaim hook for a
// class. Only the caches register one; buffers and the inode table are not
// reclaimable.
func (b *Budget) RegisterReclaimer(class Class, fn Reclaimer) {
	if b == nil {
		return
	}
	b.reclaimMu.Lock()
	defer b.reclaimMu.Unlock()
	if fn == nil {
		delete(b.reclaimers, class)
		return
	}
	b.reclaimers[class] = fn
}

// Charge adds n bytes to the class without attempting reclamation. It returns
// ErrExceeded when the allocation does not fit and the budget is enforcing;
// the caller then degrades (drops a cache entry) or fails the operation.
func (b *Budget) Charge(class Class, n int64) error {
	if b == nil || n <= 0 {
		return nil
	}
	if b.tryReserve(n) {
		b.counter(class).Add(n)
		return nil
	}
	if b.mode == ModeWarn {
		b.admitOverLimit(class, n)
		return nil
	}
	b.refuse(class, n)
	return ErrExceeded
}

// Acquire is Charge plus reclamation: when the allocation does not fit it asks
// the registered reclaimers to free the shortfall first. Callers must hold no
// component lock (a reclaimer takes the evicted cache's lock).
func (b *Budget) Acquire(class Class, n int64) error {
	if b == nil || n <= 0 {
		return nil
	}
	if b.tryReserve(n) {
		b.counter(class).Add(n)
		return nil
	}
	b.acquireMu.Lock()
	// Recheck under the lock: a concurrent release may have made room.
	if b.tryReserve(n) {
		b.acquireMu.Unlock()
		b.counter(class).Add(n)
		return nil
	}
	deficit := b.used.Load() + n - b.limit
	freed := b.reclaim(deficit)
	ok := b.tryReserve(n)
	b.acquireMu.Unlock()
	if ok {
		b.counter(class).Add(n)
		if freed > 0 {
			b.note("memory_budget_reclaimed", class, n, freed)
		}
		return nil
	}
	if b.mode == ModeWarn {
		b.admitOverLimit(class, n)
		return nil
	}
	b.refuse(class, n)
	return ErrExceeded
}

// ForceCharge adds n bytes without the option of refusal. It is for
// allocations that cannot fail without breaking the namespace — the inode
// table, where a refused charge would have to abort a lookup. The bytes are
// still counted, so an over-limit inode table squeezes the caches and the
// write buffers, which can degrade or fail.
func (b *Budget) ForceCharge(class Class, n int64) {
	if b == nil || n <= 0 {
		return
	}
	if b.tryReserve(n) {
		b.counter(class).Add(n)
		return
	}
	b.overruns.Add(1)
	b.used.Add(n)
	b.counter(class).Add(n)
	for {
		cur := b.used.Load()
		peak := b.peak.Load()
		if cur <= peak || b.peak.CompareAndSwap(peak, cur) {
			break
		}
	}
	b.report(class, n, "memory_budget_overrun")
}

// Release returns n previously charged bytes to the budget. It is safe on a
// nil budget and clamps at zero so a double release cannot corrupt admission.
func (b *Budget) Release(class Class, n int64) {
	if b == nil || n <= 0 {
		return
	}
	if b.used.Add(-n) < 0 {
		// A release without a matching charge means the accounting is
		// broken; reset rather than let a negative total admit the world.
		b.used.Store(0)
		b.note("memory_budget_release_underflow", class, n, 0)
	}
	c := b.counter(class)
	if c.Add(-n) < 0 {
		c.Store(0)
	}
}

// Snapshot is a point-in-time view of the budget (for logs and tests).
type Snapshot struct {
	Limit    int64           `json:"limit"`
	Used     int64           `json:"used"`
	Peak     int64           `json:"peak"`
	Mode     Mode            `json:"mode"`
	Over     bool            `json:"over_limit"`
	Overruns int64           `json:"overruns"`
	Refusals int64           `json:"refusals"`
	Classes  map[Class]int64 `json:"classes"`
}

// Snapshot reports the current accounting.
func (b *Budget) Snapshot() Snapshot {
	if b == nil {
		return Snapshot{Mode: ModeEnforce, Classes: map[Class]int64{}}
	}
	used := b.used.Load()
	return Snapshot{
		Limit:    b.limit,
		Used:     used,
		Peak:     b.peak.Load(),
		Mode:     b.mode,
		Over:     used > b.limit,
		Overruns: b.overruns.Load(),
		Refusals: b.refusals.Load(),
		Classes: map[Class]int64{
			ClassReadCache:   b.readCache.Load(),
			ClassWriteCache:  b.writeCache.Load(),
			ClassWriteBuffer: b.writeBuffer.Load(),
			ClassInodeTable:  b.inodeTable.Load(),
		},
	}
}

// String renders the one-line summary the mount logs at start and on overrun.
func (s Snapshot) String() string {
	return "used=" + strconv.FormatInt(s.Used, 10) +
		" limit=" + strconv.FormatInt(s.Limit, 10) +
		" mode=" + string(s.Mode) +
		" read_cache=" + strconv.FormatInt(s.Classes[ClassReadCache], 10) +
		" write_cache=" + strconv.FormatInt(s.Classes[ClassWriteCache], 10) +
		" write_buffer=" + strconv.FormatInt(s.Classes[ClassWriteBuffer], 10) +
		" inode_table=" + strconv.FormatInt(s.Classes[ClassInodeTable], 10)
}

func (b *Budget) counter(class Class) *atomic.Int64 {
	switch class {
	case ClassReadCache:
		return &b.readCache
	case ClassWriteCache:
		return &b.writeCache
	case ClassWriteBuffer:
		return &b.writeBuffer
	case ClassInodeTable:
		return &b.inodeTable
	default:
		return &b.other
	}
}

func (b *Budget) tryReserve(n int64) bool {
	for {
		cur := b.used.Load()
		if cur+n > b.limit {
			return false
		}
		if b.used.CompareAndSwap(cur, cur+n) {
			for {
				peak := b.peak.Load()
				if cur+n <= peak || b.peak.CompareAndSwap(peak, cur+n) {
					break
				}
			}
			return true
		}
	}
}

// admitOverLimit charges an allocation the budget refused (warn mode).
func (b *Budget) admitOverLimit(class Class, n int64) {
	b.used.Add(n)
	b.counter(class).Add(n)
	for {
		cur := b.used.Load()
		peak := b.peak.Load()
		if cur <= peak || b.peak.CompareAndSwap(peak, cur) {
			break
		}
	}
	b.overruns.Add(1)
	b.report(class, n, "memory_budget_overrun")
}

func (b *Budget) refuse(class Class, n int64) {
	b.refusals.Add(1)
	b.report(class, n, "memory_budget_refused")
}

// report logs an overrun at most once per second (the first one always logs)
// so a sustained workload cannot turn the budget into a log flood.
func (b *Budget) report(class Class, n int64, msg string) {
	if b.log == nil {
		return
	}
	now := time.Now().UnixNano()
	last := b.lastReport.Load()
	if last != 0 && now-last < int64(time.Second) {
		return
	}
	if !b.lastReport.CompareAndSwap(last, now) {
		return
	}
	b.log.Warn(msg,
		zap.String("class", string(class)),
		zap.Int64("request", n),
		zap.Int64("used", b.used.Load()),
		zap.Int64("limit", b.limit),
		zap.String("mode", string(b.mode)),
		zap.Int64("peak", b.peak.Load()),
	)
}

// note logs a state change that is not an overrun (reclaim success, broken
// accounting) under the same rate limit.
func (b *Budget) note(msg string, class Class, n, freed int64) {
	if b.log == nil {
		return
	}
	now := time.Now().UnixNano()
	last := b.lastReport.Load()
	if last != 0 && now-last < int64(time.Second) {
		return
	}
	if !b.lastReport.CompareAndSwap(last, now) {
		return
	}
	b.log.Info(msg,
		zap.String("class", string(class)),
		zap.Int64("request", n),
		zap.Int64("freed", freed),
		zap.Int64("used", b.used.Load()),
		zap.Int64("limit", b.limit),
	)
}

// reclaim asks every registered reclaimer for a share of the deficit and
// returns the bytes freed in total.
func (b *Budget) reclaim(deficit int64) int64 {
	if deficit <= 0 {
		return 0
	}
	b.reclaimMu.Lock()
	reclaimers := make([]Reclaimer, 0, len(b.reclaimers))
	// Deterministic order: the read cache is the cheapest tier to lose.
	for _, class := range [...]Class{ClassReadCache, ClassWriteCache} {
		if fn, ok := b.reclaimers[class]; ok {
			reclaimers = append(reclaimers, fn)
		}
	}
	b.reclaimMu.Unlock()
	var freed int64
	for _, fn := range reclaimers {
		if freed >= deficit {
			break
		}
		freed += fn(deficit - freed)
	}
	return freed
}
