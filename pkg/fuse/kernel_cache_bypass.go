package fuse

import (
	"sync"
	"sync/atomic"
	"time"
)

// Kernel cache bypass markers, ported from pkg/fuse/kernel_cache_bypass.go:
// after a truncate-first sync commit, Linux can still serve stale page/attr
// cache even after a successful InodeNotify, so a short per-inode bypass
// window forces DIRECT_IO on the next open. One coalesced sweeper reclaims
// expired markers (no per-commit timers).

const kernelCacheBypassFallbackTTL = 2 * time.Second

type kernelCacheBypassMarker struct {
	ino       uint64
	path      string
	revision  int64
	size      int64
	reason    string
	expiresAt time.Time
}

type kernelCacheBypassTable struct {
	mu         sync.Mutex
	markers    map[uint64]kernelCacheBypassMarker
	sweepWhen  atomic.Int64 // unix nanos of next scheduled sweep; 0 = none
	sweepTimer *time.Timer
}

func newKernelCacheBypassTable() *kernelCacheBypassTable {
	return &kernelCacheBypassTable{markers: map[uint64]kernelCacheBypassMarker{}}
}

// arm records a bypass marker with the default TTL.
func (t *kernelCacheBypassTable) arm(ino uint64, path string, revision, size int64, reason string) {
	t.armUntil(ino, path, revision, size, reason, time.Now().Add(kernelCacheBypassFallbackTTL))
}

// armUntil is the testable form with an explicit expiry.
func (t *kernelCacheBypassTable) armUntil(ino uint64, path string, revision, size int64, reason string, expiresAt time.Time) {
	if t == nil || ino == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.markers[ino] = kernelCacheBypassMarker{
		ino:       ino,
		path:      path,
		revision:  revision,
		size:      size,
		reason:    reason,
		expiresAt: expiresAt,
	}
	t.scheduleSweepLocked(expiresAt)
}

// active reports whether the marker for ino is live, expiring it lazily.
func (t *kernelCacheBypassTable) active(ino uint64) bool {
	if t == nil || ino == 0 {
		return false
	}
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	marker, ok := t.markers[ino]
	if !ok {
		return false
	}
	if !marker.expiresAt.IsZero() && now.After(marker.expiresAt) {
		delete(t.markers, ino)
		t.rescheduleSweepLocked()
		return false
	}
	return marker.ino == ino
}

// scheduleSweepLocked advances the single sweeper deadline and arms the
// coalesced timer (caller holds mu; v1 time.AfterFunc sweeper).
func (t *kernelCacheBypassTable) scheduleSweepLocked(expiresAt time.Time) {
	ns := expiresAt.UnixNano()
	for {
		cur := t.sweepWhen.Load()
		if cur != 0 && cur <= ns {
			return
		}
		if t.sweepWhen.CompareAndSwap(cur, ns) {
			delay := time.Until(expiresAt)
			if delay < 0 {
				delay = 0
			}
			if t.sweepTimer != nil {
				t.sweepTimer.Stop()
			}
			t.sweepTimer = time.AfterFunc(delay, t.sweep)
			return
		}
	}
}

// rescheduleSweepLocked recomputes the earliest deadline after a removal.
func (t *kernelCacheBypassTable) rescheduleSweepLocked() {
	var earliest int64
	for _, m := range t.markers {
		ns := m.expiresAt.UnixNano()
		if earliest == 0 || ns < earliest {
			earliest = ns
		}
	}
	t.sweepWhen.Store(earliest)
}

// sweep drops every expired marker (called on the sweeper cadence).
func (t *kernelCacheBypassTable) sweep() {
	if t == nil {
		return
	}
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	for ino, m := range t.markers {
		if !m.expiresAt.IsZero() && now.After(m.expiresAt) {
			delete(t.markers, ino)
		}
	}
	t.rescheduleSweepLocked()
}

func (t *kernelCacheBypassTable) clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.markers = map[uint64]kernelCacheBypassMarker{}
	t.sweepWhen.Store(0)
	if t.sweepTimer != nil {
		t.sweepTimer.Stop()
		t.sweepTimer = nil
	}
}

func containsUint32(list []uint32, v uint32) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}
