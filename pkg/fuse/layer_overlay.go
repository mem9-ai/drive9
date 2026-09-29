package fuse

import (
	"fmt"
	"os"
	"strings"
	"syscall"
)

func (fs *Dat9FS) layerRef() string {
	if fs == nil || fs.opts == nil {
		return ""
	}
	return strings.TrimSpace(fs.opts.LayerRef)
}

func (fs *Dat9FS) layerEnabled() bool {
	return fs.layerRef() != ""
}

func (fs *Dat9FS) setLayerAbandoned() {
	if fs != nil {
		fs.layerAbandoned.Store(true)
	}
}

func (fs *Dat9FS) isLayerAbandoned() bool {
	return fs != nil && fs.layerAbandoned.Load()
}

// clearLayerOverlay drops the entire in-memory layer overlay so the mount
// reflects the base view. Called by applyLayerRollback after the layer is
// rolled back. Rebuilding from a fresh replay would re-add the abandoned
// layer's still-present fs_layer_entries, so we zero the maps directly.
func (fs *Dat9FS) clearLayerOverlay() {
	if fs == nil {
		return
	}
	fs.layerMu.Lock()
	fs.layerWhiteouts = make(map[string]struct{})
	fs.layerFiles = make(map[string]uint32)
	fs.layerDirs = make(map[string]uint32)
	fs.layerSymlinks = make(map[string]layerSymlinkState)
	fs.layerMu.Unlock()
}

// applyLayerRollback reacts to a rollback event observed by the layer-event
// watcher. It clears the in-memory overlay so the mount reflects the base
// view, halts the commit queue so no further writes reach the abandoned
// layer, preserves any locally staged (not-yet-committed) writes as
// PendingConflict for manual recovery, and flushes userspace + kernel caches
// so the new view is visible without a remount.
//
// The shadow store and pending index are passed explicitly (rather than
// read from fs fields) to match the watcher's existing call signature which
// already holds references to them.
func (fs *Dat9FS) applyLayerRollback(shadows *ShadowStore, pending *PendingIndex) {
	if fs == nil {
		return
	}

	// 1. Mark abandoned: future writes return ESTALE, commit queue rejects.
	fs.setLayerAbandoned()

	// 2. Halt the commit queue (non-blocking; in-flight uploads will fail
	//    with 409 and fall to onCommitTerminalFailure → MarkConflict).
	if fs.commitQueue != nil {
		fs.commitQueue.AbandonLayer()
	}

	// 3. Discard abandoned durable caches, retaining unsent bytes as conflicts.
	if pending != nil {
		pending.abandonLayer()
		for p := range pending.ListPendingPaths() {
			meta, ok := pending.GetMeta(p)
			if !ok {
				continue
			}
			if meta.LayerClean && meta.Kind != PendingConflict {
				if err := pending.discardLayerCache(p, meta.Generation, shadows); err != nil {
					safeLogPrintf("layer rollback: discard cache %s: %v", p, err)
					_, _ = pending.MarkConflictIfGeneration(p, meta.Generation)
				}
			} else {
				_, _ = pending.MarkConflictIfGeneration(p, meta.Generation)
			}
		}
	}

	// 4. Clear the in-memory overlay so the base view reappears.
	fs.clearLayerOverlay()

	// 5. Flush userspace caches + notify kernel (shared with SSE reset).
	fs.resetMountView()

	// 6. Force re-fetch on next Getattr/Read (belt + suspenders with the
	//    cache flush above; matches SSE OnDisconnected, sse.go:38-42).
	fs.markStatCacheUnverified()

	fmt.Fprintf(os.Stderr, "drive9: fs layer rolled back — mount now reflects base view; pending local writes preserved as conflict for manual recovery\n")
}

func (fs *Dat9FS) markLayerWhiteout(localPath string) {
	if fs == nil || localPath == "" {
		return
	}
	fs.layerMu.Lock()
	// Skip overlay mutation if the layer was rolled back — a racing write
	// that completed on the server just before applyLayerRollback cleared
	// the overlay must not re-populate it.
	if fs.layerAbandoned.Load() {
		fs.layerMu.Unlock()
		return
	}
	if fs.layerWhiteouts == nil {
		fs.layerWhiteouts = make(map[string]struct{})
	}
	fs.layerWhiteouts[localPath] = struct{}{}
	delete(fs.layerFiles, localPath)
	delete(fs.layerDirs, localPath)
	delete(fs.layerSymlinks, localPath)
	fs.layerMu.Unlock()
}

func (fs *Dat9FS) markLayerFile(localPath string) {
	if fs == nil || localPath == "" {
		return
	}
	fs.layerMu.Lock()
	if fs.layerAbandoned.Load() {
		fs.layerMu.Unlock()
		return
	}
	delete(fs.layerWhiteouts, localPath)
	delete(fs.layerFiles, localPath)
	delete(fs.layerDirs, localPath)
	delete(fs.layerSymlinks, localPath)
	fs.layerMu.Unlock()
}

func (fs *Dat9FS) markLayerFileMode(localPath string, mode uint32) {
	if fs == nil || localPath == "" {
		return
	}
	fs.layerMu.Lock()
	if fs.layerAbandoned.Load() {
		fs.layerMu.Unlock()
		return
	}
	if fs.layerFiles == nil {
		fs.layerFiles = make(map[string]uint32)
	}
	fs.layerFiles[localPath] = mode & 0o777
	delete(fs.layerWhiteouts, localPath)
	delete(fs.layerDirs, localPath)
	delete(fs.layerSymlinks, localPath)
	fs.layerMu.Unlock()
}

func (fs *Dat9FS) markLayerDir(localPath string, mode uint32) {
	if fs == nil || localPath == "" {
		return
	}
	fs.layerMu.Lock()
	if fs.layerAbandoned.Load() {
		fs.layerMu.Unlock()
		return
	}
	if fs.layerDirs == nil {
		fs.layerDirs = make(map[string]uint32)
	}
	fs.layerDirs[localPath] = mode & 0o777
	delete(fs.layerWhiteouts, localPath)
	delete(fs.layerFiles, localPath)
	delete(fs.layerSymlinks, localPath)
	fs.layerMu.Unlock()
}

func (fs *Dat9FS) markLayerSymlink(localPath, target string, mode uint32) {
	if fs == nil || localPath == "" {
		return
	}
	if mode == 0 {
		mode = symlinkMode()
	} else if mode&uint32(syscall.S_IFMT) == 0 {
		mode = uint32(syscall.S_IFLNK) | (mode & 0o777)
	}
	fs.layerMu.Lock()
	if fs.layerAbandoned.Load() {
		fs.layerMu.Unlock()
		return
	}
	if fs.layerSymlinks == nil {
		fs.layerSymlinks = make(map[string]layerSymlinkState)
	}
	fs.layerSymlinks[localPath] = layerSymlinkState{Target: target, Mode: mode}
	delete(fs.layerWhiteouts, localPath)
	delete(fs.layerFiles, localPath)
	delete(fs.layerDirs, localPath)
	fs.layerMu.Unlock()
}

func (fs *Dat9FS) isLayerWhiteout(localPath string) bool {
	if fs == nil || !fs.layerEnabled() {
		return false
	}
	fs.layerMu.RLock()
	_, ok := fs.layerWhiteouts[localPath]
	fs.layerMu.RUnlock()
	return ok
}

func (fs *Dat9FS) layerDirMode(localPath string) (uint32, bool) {
	if fs == nil || !fs.layerEnabled() {
		return 0, false
	}
	fs.layerMu.RLock()
	mode, ok := fs.layerDirs[localPath]
	fs.layerMu.RUnlock()
	return mode, ok
}

func (fs *Dat9FS) layerFileMode(localPath string) (uint32, bool) {
	if fs == nil || !fs.layerEnabled() {
		return 0, false
	}
	fs.layerMu.RLock()
	mode, ok := fs.layerFiles[localPath]
	fs.layerMu.RUnlock()
	return mode, ok
}

func (fs *Dat9FS) layerSymlink(localPath string) (string, uint32, bool) {
	if fs == nil || !fs.layerEnabled() {
		return "", 0, false
	}
	fs.layerMu.RLock()
	state, ok := fs.layerSymlinks[localPath]
	fs.layerMu.RUnlock()
	return state.Target, state.Mode, ok
}

func (fs *Dat9FS) applyLayerFileMode(localPath string, ino uint64, entry *InodeEntry) {
	if entry == nil || entry.IsDir || entryIsSymlink(entry) {
		return
	}
	mode, ok := fs.layerFileMode(localPath)
	if !ok {
		return
	}
	entry.Mode = mode & 0o777
	entry.HasMode = true
	if ino != 0 {
		fs.inodes.UpdateMode(ino, entry.Mode)
	}
}

func (fs *Dat9FS) applyLayerFileModeToCachedInfo(localPath string, item *CachedFileInfo) {
	if item == nil || item.IsDir {
		return
	}
	if item.HasMode && isSymlinkMode(item.Mode) {
		return
	}
	mode, ok := fs.layerFileMode(localPath)
	if !ok {
		return
	}
	item.Mode = mode & 0o777
	item.HasMode = true
}

func (fs *Dat9FS) layerDirHasOverlayChildren(localPath string) bool {
	if fs == nil || !fs.layerEnabled() {
		return false
	}
	prefix := strings.TrimRight(localPath, "/") + "/"
	if prefix == "/" {
		return true
	}
	if fs.pendingIndex != nil && len(fs.pendingIndex.ListByPrefix(prefix)) > 0 {
		return true
	}
	if fs.writeBack != nil && len(fs.writeBack.ListByPrefix(prefix)) > 0 {
		return true
	}
	if fs.layerDirHasOpenChild(prefix) {
		return true
	}
	fs.layerMu.RLock()
	for p := range fs.layerFiles {
		if strings.HasPrefix(p, prefix) {
			fs.layerMu.RUnlock()
			return true
		}
	}
	for p := range fs.layerDirs {
		if p != localPath && strings.HasPrefix(p, prefix) {
			fs.layerMu.RUnlock()
			return true
		}
	}
	for p := range fs.layerSymlinks {
		if strings.HasPrefix(p, prefix) {
			fs.layerMu.RUnlock()
			return true
		}
	}
	fs.layerMu.RUnlock()
	return false
}

func (fs *Dat9FS) layerDirHasOpenChild(prefix string) bool {
	if fs == nil || fs.fileHandles == nil || prefix == "" {
		return false
	}
	for _, fh := range fs.fileHandles.Snapshot() {
		if fh == nil {
			continue
		}
		fh.Lock()
		path := fh.Path
		// fh.Unlinked is set by markOpenHandlesUnlinked for every open-unlinked
		// handle. UnlinkedData alone is incomplete: dirty open-unlinked handles
		// keep their Dirty buffer and may never populate UnlinkedData.
		unlinked := fh.Unlinked || fh.UnlinkedData != nil
		fh.Unlock()
		if unlinked {
			continue
		}
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}
