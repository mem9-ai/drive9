package fuse

// Zombie handles: with the kernel FUSE writeback cache negotiated, dirty
// pages produced through mmap(MAP_SHARED) can be written back to the daemon
// after the last RELEASE of the file (the mapping may outlive the fd; the
// kernel flushes them at the latest before forgetting the inode). drive9
// answers such a late WRITE with the handle it was opened with, so the
// handle must outlive RELEASE. A "zombie" is a released classic (non-extent)
// handle that stays registered until the kernel FORGETs the inode, absorbing
// late writebacks through the normal Write path.
//
// Design: docs/design/kernel-writeback-cache-classic-durability.md
//
// Scope guards:
//   - Never created when the kernel writeback cache is off (write-sync
//     mounts, macFUSE/older kernels, --writeback-cache off): there every
//     write() reaches the daemon synchronously and no late writeback exists.
//   - Extent handles have their own post-Release path (extentWriteKernelFh /
//     extentWriteByNode) and never become zombies.
//   - Append-log paths never become zombies: their Flush already syncs
//     remotely, and a late writeback there surfaces as a logged ENOENT
//     instead of being silently accepted by routing code.

import (
	"context"
	"strings"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

const (
	// zombieCommitDebounce coalesces late writebacks absorbed by a zombie
	// before the background remote commit runs.
	zombieCommitDebounce = 150 * time.Millisecond
	// zombieCommitMaxBackoff caps the retry backoff after a failed zombie
	// commit. Data stays dirty between retries, so retrying is idempotent.
	zombieCommitMaxBackoff = 30 * time.Second
	// zombieMaxAge bounds a zombie's lifetime when the kernel never sends
	// FORGET. A dirty page can never outlive the kernel inode, but a clean
	// inode may stay cached for as long as the kernel likes; the janitor
	// force-syncs and frees zombies older than this.
	zombieMaxAge = 5 * time.Minute
	// zombieJanitorPeriod is how often the janitor sweeps expired zombies.
	zombieJanitorPeriod = time.Minute
	// zombieSoftCap bounds live zombies; registering beyond it evicts the
	// oldest zombies (synced first) until the count drops below half the cap.
	zombieSoftCap = 4096
)

// kernelWritebackCacheOn reports whether this mount negotiated the kernel
// FUSE writeback cache. The zombie lifecycle exists only then.
func (fs *Dat9FS) kernelWritebackCacheOn() bool {
	return fs != nil && fs.kernelWritebackCache
}

// shouldZombifyHandleLocked reports whether a successfully released handle
// may become a zombie. Caller holds fh.mu.
func (fs *Dat9FS) shouldZombifyHandleLocked(fh *FileHandle) bool {
	if fh == nil || fh.Zombie || fh.isExtent() || fh.Unlinked {
		return false
	}
	// A read-only handle cannot produce dirty pages (PROT_WRITE MAP_SHARED
	// requires a writable fd).
	if !localFileHandleOpenedWritable(fh) {
		return false
	}
	// Append-log handles commit through the server-owned append endpoint at
	// Flush; the append routing state only exists on a live handle.
	if fs.appendLogConfiguredLocked(fh) {
		return false
	}
	return true
}

// zombifyReleasedHandle marks a released handle as a zombie and registers it
// in zombiesByInode. Returns false when the handle is ineligible and the
// caller should retire it normally.
func (fs *Dat9FS) zombifyReleasedHandle(fh *FileHandle) bool {
	if !fs.kernelWritebackCacheOn() {
		return false
	}
	fh.Lock()
	if !fs.shouldZombifyHandleLocked(fh) {
		fh.Unlock()
		return false
	}
	fh.Zombie = true
	fh.ZombifiedAt = time.Now()
	fh.Unlock()
	// The zombie is a closed handle for every namespace purpose: it must not
	// count as an open handle, or openHandleEntry/SnapshotPath lookups would
	// report a deleted path as still present (the truncate-then-unlink local
	// resurrection). It stays in fileHandles (fh-addressed writebacks) and
	// zombiesByInode (node-addressed writebacks); unlink/rename cover it
	// through the explicit zombie helpers.
	fs.openHandles.Remove(fh)
	fs.registerZombie(fh)
	fs.startZombieJanitor()
	if fs.debugEnabled() {
		fs.debugf("zombie handle registered path=%s ino=%d fh=%d", fh.Path, fh.Ino, fh.handleID)
	}
	return true
}

// finishReleasedHandle retires a released handle: with the kernel writeback
// cache on, an eligible handle becomes a zombie and stays registered until
// FORGET; otherwise (or after a failed release) it is unregistered exactly
// as before.
func (fs *Dat9FS) finishReleasedHandle(fhID uint64, fh *FileHandle, flushStatus gofuse.Status) {
	if flushStatus == gofuse.OK && fs.zombifyReleasedHandle(fh) {
		return
	}
	// A handle that stays non-zombie must not hold a kept local fd.
	fh.Lock()
	lf := fh.LocalFile
	fh.LocalFile = nil
	fh.Unlock()
	if lf != nil {
		_ = lf.Close()
	}
	fs.deleteFileHandle(fhID, fh)
	fs.cleanupReleasedInode(fh.Ino, fh.Path)
}

func (fs *Dat9FS) registerZombie(fh *FileHandle) {
	// At most one zombie per inode: an older zombie's release commit already
	// landed, and the kernel's page cache is per-inode, so a late writeback
	// must land in the newest handle's buffer (the one whose BaseRev covers
	// everything committed so far). Purge the predecessors first — purge
	// commits any data they absorbed before unregistering.
	for _, old := range fs.zombieHandlesForInode(fh.Ino) {
		if old != fh {
			fs.purgeZombie(old, "superseded-by-new-zombie")
		}
	}
	fs.zombieMu.Lock()
	set := fs.zombiesByInode[fh.Ino]
	if set == nil {
		set = make(map[*FileHandle]struct{})
		fs.zombiesByInode[fh.Ino] = set
	}
	set[fh] = struct{}{}
	total := 0
	for _, s := range fs.zombiesByInode {
		total += len(s)
	}
	fs.zombieMu.Unlock()
	if total > zombieSoftCap {
		fs.evictOldestZombies(total - zombieSoftCap/2)
	}
}

func (fs *Dat9FS) unregisterZombie(fh *FileHandle) {
	fs.zombieMu.Lock()
	defer fs.zombieMu.Unlock()
	if set := fs.zombiesByInode[fh.Ino]; set != nil {
		delete(set, fh)
		if len(set) == 0 {
			delete(fs.zombiesByInode, fh.Ino)
		}
	}
}

// zombieHandleForNode returns a zombie registered for the inode, if any. The
// kernel may address a post-Release writeback by node without the released
// file handle.
func (fs *Dat9FS) zombieHandleForNode(nodeId uint64) *FileHandle {
	fs.zombieMu.Lock()
	defer fs.zombieMu.Unlock()
	for fh := range fs.zombiesByInode[nodeId] {
		return fh
	}
	return nil
}

func (fs *Dat9FS) zombieHandlesForInode(ino uint64) []*FileHandle {
	fs.zombieMu.Lock()
	defer fs.zombieMu.Unlock()
	set := fs.zombiesByInode[ino]
	out := make([]*FileHandle, 0, len(set))
	for fh := range set {
		out = append(out, fh)
	}
	return out
}

// scheduleZombieCommitLocked (re)arms the zombie's debounced remote commit.
// A zombie has no future Flush/Fsync/Release to drive the upload, so the
// timer is its only commit trigger. Caller holds fh.mu.
func (fs *Dat9FS) scheduleZombieCommitLocked(fh *FileHandle) {
	if fh == nil || !fh.Zombie || fh.Unlinked {
		return
	}
	delay := zombieCommitDebounce
	if fh.zombieCommitFails > 0 {
		delay = zombieCommitDebounce << fh.zombieCommitFails
		if delay > zombieCommitMaxBackoff {
			delay = zombieCommitMaxBackoff
		}
	}
	if fh.zombieTimer != nil {
		fh.zombieTimer.Stop()
	}
	fh.zombieTimer = time.AfterFunc(delay, func() {
		fs.runZombieCommit(fh)
	})
}

// runZombieCommit is the zombie timer body: commit the absorbed dirty data
// remote-durably, re-arming when a write raced the commit or the commit
// failed (with backoff). Never drops data silently: an unlinked zombie
// discards through the same anonymous-fd path as a live unlinked handle.
func (fs *Dat9FS) runZombieCommit(fh *FileHandle) {
	fh.Lock()
	defer fh.Unlock()
	if !fh.Zombie {
		return
	}
	fh.zombieTimer = nil
	if fh.Unlinked {
		fs.discardUnlinkedHandleStateLocked(fh)
		return
	}
	if isLocalFileHandle(fh) || isGitWorkspaceLocalFileHandle(fh) {
		// Local handles have no remote commit; their dirty pages landed on
		// the local fd through the normal Write path already.
		if fh.LocalFile != nil {
			_ = syncOpenLocalFile(fh.LocalFile)
		}
		fh.zombieCommitFails = 0
		return
	}
	if fh.Dirty == nil || !fh.Dirty.HasDirtyParts() {
		fh.zombieCommitFails = 0
		return
	}
	size := fh.Dirty.Size()
	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout(size))
	st := fs.syncHandleToRemoteLocked(ctx, fh)
	cancel()
	if st != gofuse.OK {
		fh.zombieCommitFails++
		safeLogPrintf("zombie commit failed for %s (attempt %d): status=%d", fh.Path, fh.zombieCommitFails, st)
		fs.scheduleZombieCommitLocked(fh)
		return
	}
	fh.zombieCommitFails = 0
	if fh.Dirty != nil && fh.Dirty.HasDirtyParts() {
		// A write landed while the commit was in flight (flushHandle leaves
		// newer dirty parts untouched). Commit them too.
		fs.scheduleZombieCommitLocked(fh)
	}
}

// syncZombieHandle commits one zombie's dirty data now (used by a sibling
// fd's fsync and by path truncates). No-op for clean zombies.
func (fs *Dat9FS) syncZombieHandle(cancel <-chan struct{}, zfh *FileHandle) gofuse.Status {
	zfh.Lock()
	defer zfh.Unlock()
	if !zfh.Zombie || zfh.Unlinked {
		return gofuse.OK
	}
	if zfh.zombieTimer != nil {
		zfh.zombieTimer.Stop()
		zfh.zombieTimer = nil
	}
	if isLocalFileHandle(zfh) || isGitWorkspaceLocalFileHandle(zfh) {
		if zfh.LocalFile != nil {
			return localErrToFuseStatus(syncOpenLocalFile(zfh.LocalFile))
		}
		return gofuse.OK
	}
	if zfh.Dirty == nil || !zfh.Dirty.HasDirtyParts() {
		return gofuse.OK
	}
	ctx, cf := fuseCtxWithTimeout(cancel, releaseTimeout(zfh.Dirty.Size()))
	defer cf()
	st := fs.syncHandleToRemoteLocked(ctx, zfh)
	if st == gofuse.OK {
		zfh.zombieCommitFails = 0
	}
	return st
}

// syncZombieHandlesForInode commits dirty zombies of one inode. A reopened
// fd's fsync must cover data the kernel wrote back to a zombie after the
// previous close (mmap write -> close -> reopen -> fsync).
func (fs *Dat9FS) syncZombieHandlesForInode(cancel <-chan struct{}, ino uint64, exclude *FileHandle) gofuse.Status {
	for _, zfh := range fs.zombieHandlesForInode(ino) {
		if zfh == exclude {
			continue
		}
		if st := fs.syncZombieHandle(cancel, zfh); st != gofuse.OK {
			return st
		}
	}
	return gofuse.OK
}

// syncZombieHandlesForPath commits dirty zombies of one path before a remote
// mutation (truncate) that must be ordered after everything the kernel has
// already written back.
func (fs *Dat9FS) syncZombieHandlesForPath(cancel <-chan struct{}, p string) gofuse.Status {
	ino, ok := fs.inodes.GetInode(p)
	if !ok {
		return gofuse.OK
	}
	return fs.syncZombieHandlesForInode(cancel, ino, nil)
}

// truncateZombieBuffersForInode drops zombie-buffered data beyond newSize
// after an ftruncate on a sibling handle, mirroring the kernel's own page
// invalidation for the inode so a pending zombie commit cannot resurrect the
// truncated tail.
func (fs *Dat9FS) truncateZombieBuffersForInode(ino uint64, newSize int64) {
	for _, zfh := range fs.zombieHandlesForInode(ino) {
		zfh.Lock()
		if zfh.Zombie && zfh.Dirty != nil && zfh.Dirty.Size() > newSize {
			// A zombie with no dirty sequence and no dirty parts was clean
			// before this sync: the dirty state the truncation creates is
			// phantom bookkeeping, not absorbed writeback data. Keep it
			// clean — committing it would publish the truncated (usually
			// zero) image with the zombie's stale BaseRev.
			wasDirty := zfh.DirtySeq != 0 || zfh.Dirty.HasDirtyParts()
			if err := zfh.Dirty.Truncate(newSize); err != nil {
				safeLogPrintf("zombie truncate sync failed for %s: %v", zfh.Path, err)
			} else if !wasDirty {
				zfh.Dirty.ClearDirty()
				zfh.ZeroBase = false
			} else {
				zfh.DirtySeq = fs.markDirtySize(ino, newSize)
				if newSize == 0 {
					zfh.ZeroBase = true
				}
			}
		}
		zfh.Unlock()
	}
}

// markZombiesUnlinkedForPath marks the zombies of the inode behind p as
// unlinked, so their absorbed writebacks keep anonymous-fd semantics (never
// staged or uploaded, never resurrecting the removed path). Called from
// markOpenHandlesUnlinked — zombies are deliberately not in openHandles, so
// the unlink flow needs this explicit pass.
func (fs *Dat9FS) markZombiesUnlinkedForPath(p string) {
	if p == "" {
		return
	}
	ino, ok := fs.inodes.GetInode(p)
	if !ok {
		return
	}
	for _, zfh := range fs.zombieHandlesForInode(ino) {
		zfh.Lock()
		zfh.Unlinked = true
		if zfh.zombieTimer != nil {
			zfh.zombieTimer.Stop()
			zfh.zombieTimer = nil
		}
		zfh.Unlock()
	}
}

// retargetZombiePathsForRename moves zombie handle paths across a rename,
// mirroring retargetOpenHandlesForRename for the open-handle index. Without
// it a zombie commit would write to the pre-rename path.
func (fs *Dat9FS) retargetZombiePathsForRename(oldP, newP string) {
	if oldP == "" || newP == "" {
		return
	}
	oldPrefix := oldP + "/"
	fs.zombieMu.Lock()
	var zombies []*FileHandle
	for _, set := range fs.zombiesByInode {
		for fh := range set {
			zombies = append(zombies, fh)
		}
	}
	fs.zombieMu.Unlock()
	for _, fh := range zombies {
		fh.Lock()
		if fh.Path == oldP || strings.HasPrefix(fh.Path, oldPrefix) {
			newPath := newP + strings.TrimPrefix(fh.Path, oldP)
			fh.Path = newPath
			if fh.Dirty != nil {
				fh.Dirty.path = newPath
			}
		}
		fh.Unlock()
	}
}

// purgeZombie retires a zombie: it commits absorbed dirty data (or discards
// it through the anonymous-fd path when unlinked), closes a kept local fd,
// and unregisters the handle exactly as a normal Release would have.
func (fs *Dat9FS) purgeZombie(fh *FileHandle, reason string) {
	if fh == nil {
		return
	}
	fs.unregisterZombie(fh)
	fh.Lock()
	if !fh.Zombie {
		fh.Unlock()
		return
	}
	fh.Zombie = false
	if fh.zombieTimer != nil {
		fh.zombieTimer.Stop()
		fh.zombieTimer = nil
	}
	if fh.Dirty != nil && fh.Dirty.HasDirtyParts() {
		if fh.Unlinked {
			fs.discardUnlinkedHandleStateLocked(fh)
		} else {
			ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout(fh.Dirty.Size()))
			st := fs.syncHandleToRemoteLocked(ctx, fh)
			cancel()
			if st != gofuse.OK {
				safeLogPrintf("zombie purge commit failed for %s (reason=%s): status=%d", fh.Path, reason, st)
			}
		}
	}
	localFile := fh.LocalFile
	fh.LocalFile = nil
	ino, p, fhID := fh.Ino, fh.Path, fh.handleID
	fh.Unlock()
	if localFile != nil {
		// The fd absorbed late writebacks; flush it before closing so the
		// local overlay never loses pages the kernel already wrote back.
		_ = syncOpenLocalFile(localFile)
		_ = localFile.Close()
	}
	fs.deleteFileHandle(fhID, fh)
	fs.cleanupReleasedInode(ino, p)
	if fs.debugEnabled() {
		fs.debugf("zombie handle purged path=%s ino=%d fh=%d reason=%s", p, ino, fhID, reason)
	}
}

// purgeZombiesForInode retires every zombie of the inode. The kernel sends
// FORGET only after all dirty pages of the inode have been written back, so
// no writeback can arrive for these handles afterwards.
func (fs *Dat9FS) purgeZombiesForInode(ino uint64) {
	for {
		fh := fs.zombieHandleForNode(ino)
		if fh == nil {
			return
		}
		fs.purgeZombie(fh, "forget")
	}
}

// purgeAllZombies retires every remaining zombie; called from FlushAll after
// the per-handle flush loop drained their dirty data.
func (fs *Dat9FS) purgeAllZombies(reason string) {
	for {
		fs.zombieMu.Lock()
		var victim *FileHandle
		for _, set := range fs.zombiesByInode {
			for fh := range set {
				victim = fh
				break
			}
			if victim != nil {
				break
			}
		}
		fs.zombieMu.Unlock()
		if victim == nil {
			return
		}
		fs.purgeZombie(victim, reason)
	}
}

// evictOldestZombies force-retires the oldest zombies until `excess` of them
// are gone, bounding zombie residency when the kernel holds dentries cached
// for a long time.
func (fs *Dat9FS) evictOldestZombies(excess int) {
	if excess <= 0 {
		return
	}
	type aged struct {
		fh *FileHandle
		at time.Time
	}
	var all []aged
	fs.zombieMu.Lock()
	for _, set := range fs.zombiesByInode {
		for fh := range set {
			all = append(all, aged{fh: fh, at: fh.ZombifiedAt})
		}
	}
	fs.zombieMu.Unlock()
	// Oldest first without importing sort for one field.
	for i := 0; i < len(all); i++ {
		for j := i + 1; j < len(all); j++ {
			if all[j].at.Before(all[i].at) {
				all[i], all[j] = all[j], all[i]
			}
		}
	}
	for i := 0; i < excess && i < len(all); i++ {
		safeLogPrintf("zombie cap exceeded: force-purging zombie for %s", all[i].fh.Path)
		fs.purgeZombie(all[i].fh, "soft-cap")
	}
}

// startZombieJanitor starts the TTL sweep for zombies whose FORGET never
// arrived. Idempotent.
func (fs *Dat9FS) startZombieJanitor() {
	fs.zombieMu.Lock()
	defer fs.zombieMu.Unlock()
	if fs.zombieJanitor != nil {
		return
	}
	stop := make(chan struct{})
	fs.zombieJanitor = stop
	go fs.zombieJanitorLoop(stop)
}

// stopZombieJanitor halts the janitor; called from FlushAll during unmount.
func (fs *Dat9FS) stopZombieJanitor() {
	fs.zombieMu.Lock()
	ch := fs.zombieJanitor
	fs.zombieJanitor = nil
	fs.zombieMu.Unlock()
	if ch != nil {
		close(ch)
	}
}

func (fs *Dat9FS) zombieJanitorLoop(stop chan struct{}) {
	ticker := time.NewTicker(zombieJanitorPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			fs.sweepExpiredZombies()
		}
	}
}

func (fs *Dat9FS) sweepExpiredZombies() {
	deadline := time.Now().Add(-zombieMaxAge)
	var expired []*FileHandle
	fs.zombieMu.Lock()
	for _, set := range fs.zombiesByInode {
		for fh := range set {
			if fh.ZombifiedAt.Before(deadline) {
				expired = append(expired, fh)
			}
		}
	}
	fs.zombieMu.Unlock()
	for _, fh := range expired {
		safeLogPrintf("zombie janitor: force-purging zombie for %s (older than %s without FORGET)", fh.Path, zombieMaxAge)
		fs.purgeZombie(fh, "janitor-ttl")
	}
}
