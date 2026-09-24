package fuse

// openReadOnlyShadowLocked discards restart orphans once. Read retries in memory.
func (fs *Dat9FS) openReadOnlyShadowLocked(fh *FileHandle) {
	if fs.shadowStore == nil {
		return
	}
	pending := fs.hasPendingMetadataState(fh.Path)
	if !pending {
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
		// A normal cache miss needs neither inode copying nor a write-back
		// metadata path lock. Pending recovery published after Open may
		// still authorize a disk-only image; this probe is memory-only.
		if fs.pendingIndex == nil || !fs.pendingIndex.HasPending(fh.Path) {
			return
		}
	}
	fs.refreshReadOnlyShadowWithPendingLocked(fh, fs.hasPendingMetadataState(fh.Path))
}

// Open checks pending recovery even without a resident candidate. Read can
// skip that work on misses; both use exactly the same source-selection rules.
func (fs *Dat9FS) refreshReadOnlyShadowWithPendingLocked(fh *FileHandle, pending bool) {
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
	var gen uint64
	var ok bool
	if pending {
		gen, ok = fs.shadowStore.PinIfExists(fh.Path)
	} else {
		gen, ok = fs.shadowStore.PinResident(fh.Path, minRevision)
	}
	if ok {
		fh.ShadowGen, fh.ShadowPinned = gen, true
	}
}
