package fuse

import "time"

// testHookNotifyInodeSync lets cache-boundary tests observe the production
// synchronous notify path without mounting a real FUSE server.
var testHookNotifyInodeSync func(ino uint64)

// testHookBeforeKernelCacheBypassSweep lets race-focused tests pause a real
// cleanup timer callback before it inspects current markers.
var testHookBeforeKernelCacheBypassSweep func()

const kernelCacheBypassFallbackTTL = 2 * time.Second

type kernelCacheBypassMarker struct {
	ino       uint64
	path      string
	revision  int64
	size      int64
	reason    string
	expiresAt time.Time
}

// armKernelCacheBypass records a short-lived per-inode cache-bypass marker for
// close-to-open readers that must not trust stale kernel pages immediately
// after a truncate-first sync commit.
func (fs *Dat9FS) armKernelCacheBypass(ino uint64, localPath string, revision, size int64, reason string) {
	fs.armKernelCacheBypassUntil(ino, localPath, revision, size, reason, time.Now().Add(kernelCacheBypassFallbackTTL))
}

// armKernelCacheBypassUntil is the testable form of armKernelCacheBypass with
// an explicit expiry. Re-arming markers uses one coalesced mount-level sweeper
// rather than one timer per commit or inode.
func (fs *Dat9FS) armKernelCacheBypassUntil(ino uint64, localPath string, revision, size int64, reason string, expiresAt time.Time) {
	if fs == nil || ino == 0 {
		return
	}
	fs.kernelCacheBypassMu.Lock()
	defer fs.kernelCacheBypassMu.Unlock()
	if fs.kernelCacheBypass == nil {
		fs.kernelCacheBypass = make(map[uint64]kernelCacheBypassMarker)
	}
	fs.kernelCacheBypass[ino] = kernelCacheBypassMarker{
		ino:       ino,
		path:      localPath,
		revision:  revision,
		size:      size,
		reason:    reason,
		expiresAt: expiresAt,
	}
	fs.debugf("kernel cache bypass armed path=%s ino=%d rev=%d size=%d reason=%s", localPath, ino, revision, size, reason)
	fs.scheduleKernelCacheBypassSweepLocked(expiresAt)
}

// scheduleKernelCacheBypassSweepLocked schedules or advances the single cleanup
// timer used to reclaim expired markers that are never reopened. Caller
// must hold kernelCacheBypassMu.
func (fs *Dat9FS) scheduleKernelCacheBypassSweepLocked(expiresAt time.Time) {
	if fs == nil || expiresAt.IsZero() {
		return
	}
	// Invariant: when kernelCacheBypassSweepTimer is non-nil,
	// kernelCacheBypassSweepAt is the deadline of a pending sweep. A later marker
	// does not need to reset that timer because the pending sweep re-derives the
	// earliest remaining live expiry under the mutex before rescheduling.
	if fs.kernelCacheBypassSweepTimer != nil && !fs.kernelCacheBypassSweepAt.IsZero() && !expiresAt.Before(fs.kernelCacheBypassSweepAt) {
		return
	}
	delay := time.Until(expiresAt)
	if delay < 0 {
		delay = 0
	}
	fs.kernelCacheBypassSweepAt = expiresAt
	if fs.kernelCacheBypassSweepTimer == nil {
		fs.kernelCacheBypassSweepTimer = time.AfterFunc(delay, func() {
			fs.sweepKernelCacheBypass()
		})
		return
	}
	// Ignore Reset's boolean result deliberately: if a previous callback is
	// already running, sweepKernelCacheBypass re-checks all current markers under
	// the same mutex and only deletes markers whose own expiresAt has passed.
	fs.kernelCacheBypassSweepTimer.Reset(delay)
}

// rescheduleKernelCacheBypassSweepLocked resets the single sweeper to the
// earliest live marker, or stops it when no markers remain. Caller must hold
// kernelCacheBypassMu.
func (fs *Dat9FS) rescheduleKernelCacheBypassSweepLocked() {
	var next time.Time
	for _, marker := range fs.kernelCacheBypass {
		if marker.expiresAt.IsZero() {
			continue
		}
		if next.IsZero() || marker.expiresAt.Before(next) {
			next = marker.expiresAt
		}
	}
	if next.IsZero() {
		if fs.kernelCacheBypassSweepTimer != nil {
			fs.kernelCacheBypassSweepTimer.Stop()
			fs.kernelCacheBypassSweepTimer = nil
		}
		fs.kernelCacheBypassSweepAt = time.Time{}
		return
	}
	fs.kernelCacheBypassSweepAt = time.Time{}
	fs.scheduleKernelCacheBypassSweepLocked(next)
}

// sweepKernelCacheBypass removes expired markers from the timer callback. A
// stale callback is benign: every marker's current expiresAt is checked under
// kernelCacheBypassMu before deletion, so a re-armed marker survives and the
// sweeper reschedules itself to the next live deadline.
func (fs *Dat9FS) sweepKernelCacheBypass() {
	if fs == nil {
		return
	}
	if testHookBeforeKernelCacheBypassSweep != nil {
		testHookBeforeKernelCacheBypassSweep()
	}
	now := time.Now()
	fs.kernelCacheBypassMu.Lock()
	defer fs.kernelCacheBypassMu.Unlock()
	fs.kernelCacheBypassSweepAt = time.Time{}
	for ino, marker := range fs.kernelCacheBypass {
		if marker.expiresAt.IsZero() {
			continue
		}
		if now.Before(marker.expiresAt) {
			continue
		}
		delete(fs.kernelCacheBypass, ino)
		fs.debugf("kernel cache bypass expired path=%s ino=%d rev=%d size=%d reason=%s", marker.path, marker.ino, marker.revision, marker.size, marker.reason)
	}
	fs.rescheduleKernelCacheBypassSweepLocked()
}

// clearKernelCacheBypassMarkers stops the cleanup sweeper and drops all marker
// state during graceful shutdown.
func (fs *Dat9FS) clearKernelCacheBypassMarkers() {
	if fs == nil {
		return
	}
	fs.kernelCacheBypassMu.Lock()
	defer fs.kernelCacheBypassMu.Unlock()
	if fs.kernelCacheBypassSweepTimer != nil {
		fs.kernelCacheBypassSweepTimer.Stop()
		fs.kernelCacheBypassSweepTimer = nil
	}
	fs.kernelCacheBypassSweepAt = time.Time{}
	for ino := range fs.kernelCacheBypass {
		delete(fs.kernelCacheBypass, ino)
	}
}

// kernelCacheBypassActive reports whether a clean read-only open on fh should
// temporarily bypass the kernel page cache. Expired markers are reclaimed
// opportunistically here in addition to the timer-driven cleanup path.
func (fs *Dat9FS) kernelCacheBypassActive(fh *FileHandle) bool {
	if fs == nil || fh == nil || fh.Ino == 0 {
		return false
	}
	now := time.Now()
	fs.kernelCacheBypassMu.Lock()
	defer fs.kernelCacheBypassMu.Unlock()
	marker, ok := fs.kernelCacheBypass[fh.Ino]
	if !ok {
		return false
	}
	if !marker.expiresAt.IsZero() && now.After(marker.expiresAt) {
		delete(fs.kernelCacheBypass, fh.Ino)
		fs.debugf("kernel cache bypass expired path=%s ino=%d rev=%d size=%d reason=%s", marker.path, marker.ino, marker.revision, marker.size, marker.reason)
		fs.rescheduleKernelCacheBypassSweepLocked()
		return false
	}
	return marker.ino == fh.Ino
}

func (fs *Dat9FS) finishSyncCommitKernelCacheBoundary(ino uint64, localPath string, revision, size int64) {
	if fs == nil || ino == 0 {
		return
	}
	// InodeNotify is best-effort: Linux may still serve stale page/attr cache
	// immediately after a truncate-first close-sync/write-sync commit even when
	// the notification call reports success. Keep the per-inode
	// short-window bypass armed until its TTL expires; this remains narrower
	// than making every clean reader on sync-durability mounts DIRECT_IO.
	fs.armKernelCacheBypass(ino, localPath, revision, size, "sync-commit")
	fs.notifyInodeSync(ino)
}
