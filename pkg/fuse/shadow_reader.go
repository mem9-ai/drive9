package fuse

// openReadOnlyShadowLocked discards restart orphans once. Read retries in memory.
func (fs *Dat9FS) openReadOnlyShadowLocked(fh *FileHandle) {
	if fs.shadowStore != nil && !fs.hasPendingMetadataState(fh.Path) &&
		(fs.appendLogPathConfigured(fh.Path) || fs.latestCommittedRevision(fh.Path) > 0) {
		fs.shadowStore.discardDiskOnly(fh.Path)
	}
	fs.refreshReadOnlyShadowLocked(fh)
}

// refreshReadOnlyShadowLocked is shared by Open and Read. An append-log cache,
// or any cache superseded by a locally observed commit, must match the known
// revision at each read, not just when the descriptor opened. Pending metadata
// has its own authority; retired reset/unlink snapshots keep their lifetime.
// The caller owns the new handle or holds fh.mu, including through a gen read.
func (fs *Dat9FS) refreshReadOnlyShadowLocked(fh *FileHandle) {
	if fs.shadowStore == nil || fh.Dirty != nil || fh.Unlinked || fh.UnlinkedSnapshot {
		return
	}
	committedRevision := fs.latestCommittedRevision(fh.Path)
	checkRevision := fs.appendLogPathConfigured(fh.Path) || committedRevision > 0
	// Pending metadata deliberately exposes the current staged image even
	// when its CAS base predates a known commit. It never validates a retired
	// image: metadata for a replacement must not resurrect the old pin.
	pending := fs.hasPendingMetadataState(fh.Path)
	minRevision := max(fh.BaseRev, committedRevision)
	if entry, ok := fs.inodes.GetEntry(fh.Ino); ok && entry != nil {
		minRevision = max(minRevision, entry.Revision)
	}
	if fh.ShadowPinned {
		if fs.shadowStore.canReadGeneration(fh.ShadowGen, minRevision, pending || !checkRevision) {
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
	if checkRevision && !pending {
		gen, ok = fs.shadowStore.PinResident(fh.Path, minRevision)
	} else {
		gen, ok = fs.shadowStore.PinIfExists(fh.Path)
	}
	if ok {
		fh.ShadowGen, fh.ShadowPinned = gen, true
	}
}
