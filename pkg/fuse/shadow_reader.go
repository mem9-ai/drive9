package fuse

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
	if !checkRevision && fh.ShadowPinned {
		return
	}
	pending := fs.hasPendingMetadataState(fh.Path)
	minRevision := max(fh.BaseRev, committedRevision)
	if entry, ok := fs.inodes.GetEntry(fh.Ino); ok && entry != nil {
		minRevision = max(minRevision, entry.Revision)
	}
	if fh.ShadowPinned {
		if fs.shadowStore.canReadGeneration(fh.ShadowGen, minRevision, pending) {
			return
		}
		gen := fh.ShadowGen
		fh.ShadowPinned, fh.ShadowGen = false, 0
		fs.shadowStore.Unpin(gen)
		clearReadTargetForLockedHandle(fh)
		if revision, size, ok := fs.latestCommittedRevisionWithSize(fh.Path); ok && revision >= minRevision {
			fh.BaseRev, fh.OrigSize = revision, size
			if fh.Prefetch != nil {
				fh.Prefetch.invalidateWithSize(size)
			}
		}
	}
	// A previously rejected or retired pin is retryable: a newer cache or
	// pending writer may now have supplied a usable generation.
	var gen uint64
	var ok bool
	if checkRevision && !pending {
		gen, ok = fs.shadowStore.PinResidentOrDiscardDisk(fh.Path, minRevision)
	} else {
		gen, ok = fs.shadowStore.PinIfExists(fh.Path)
	}
	if ok {
		fh.ShadowGen, fh.ShadowPinned = gen, true
	}
}
