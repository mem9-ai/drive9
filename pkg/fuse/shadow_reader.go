package fuse

// openReadOnlyShadowLocked discards restart orphans once. Read retries in memory.
func (fs *Dat9FS) openReadOnlyShadowLocked(fh *FileHandle) {
	if fs.shadowStore == nil {
		return
	}
	pending := fs.pendingIndex.shadowReadGeneration(fh.Path, fs.shadowStore)
	// Unbound pending metadata protects recovery data from deletion, but
	// cannot make that data readable. WriteBackCache only protects its .dat.
	if pending == 0 && (fs.pendingIndex == nil || !fs.pendingIndex.HasPending(fh.Path)) {
		fs.shadowStore.discardDiskOnly(fh.Path)
	}
	fs.refreshReadOnlyShadowWithPendingLocked(fh, pending)
}

// refreshReadOnlyShadowLocked checks every path against the known revision,
// not just append-log paths or paths committed by this mount. Pending metadata
// has its own authority; retired inode/WAL snapshots keep their lifetime.
// The caller owns the new handle or holds fh.mu, including through a gen read.
func (fs *Dat9FS) refreshReadOnlyShadowLocked(fh *FileHandle) {
	if fs.shadowStore == nil || fh.Dirty != nil || fh.Unlinked || fh.UnlinkedSnapshot {
		return
	}
	if !fh.ShadowPinned && !fs.shadowStore.hasResident(fh.Path) {
		return
	}
	fs.refreshReadOnlyShadowWithPendingLocked(fh, fs.pendingIndex.shadowReadGeneration(fh.Path, fs.shadowStore))
}

// Open and Read use the same revision/content-generation eligibility check.
// Recovery must publish a bound resident payload before it becomes readable.
func (fs *Dat9FS) refreshReadOnlyShadowWithPendingLocked(fh *FileHandle, pending uint64) {
	committedRevision := fs.latestCommittedRevision(fh.Path)
	// Pending metadata deliberately exposes the current staged image even
	// when its CAS base predates a known commit. It never validates a retired
	// image: metadata for a replacement must not resurrect the old pin.
	minRevision := max(fh.BaseRev, committedRevision, fs.inodes.GetRevision(fh.Ino))
	if fh.ShadowPinned {
		if fs.shadowStore.canReadGeneration(fh.ShadowGen, minRevision, pending) {
			return
		}
		gen := fh.ShadowGen
		fh.ShadowPinned, fh.ShadowGen = false, 0
		fs.shadowStore.Unpin(gen)
		clearReadTargetForLockedHandle(fh)
		prefetchSize := int64(-1)
		if revision, size, ok := fs.latestCommittedRevisionWithSize(fh.Path); ok && revision >= minRevision {
			fh.BaseRev, fh.OrigSize = revision, size
			prefetchSize = size
		}
		if fh.Prefetch != nil {
			// An unknown committed size does not make old prefetched bytes
			// valid after rejecting their shadow source.
			fh.Prefetch.invalidateWithSize(prefetchSize)
		}
	}
	// A previously rejected or retired pin is retryable: a newer cache or
	// pending writer may now have supplied a usable generation.
	gen, ok := fs.shadowStore.pinReadable(fh.Path, minRevision, pending)
	if ok {
		fh.ShadowGen, fh.ShadowPinned = gen, true
	}
}
