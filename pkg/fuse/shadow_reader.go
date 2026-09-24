package fuse

// refreshReadOnlyShadowLocked is shared by Open and Read. A resident append-log
// cache must be based on the currently known committed revision at each read,
// not just when the descriptor opened. Durable pending metadata has its own
// authority; explicitly retired reset/unlink snapshots keep their lifetime.
// The caller owns the new handle or holds fh.mu, including through a gen read.
func (fs *Dat9FS) refreshReadOnlyShadowLocked(fh *FileHandle) {
	if fs.shadowStore == nil || fh.Dirty != nil || fh.Unlinked || fh.UnlinkedSnapshot {
		return
	}
	appendLog := fs.appendLogPathConfigured(fh.Path)
	if !appendLog && fh.ShadowPinned {
		return
	}
	pending := fs.hasPendingMetadataState(fh.Path)
	minRevision := max(fh.BaseRev, fs.latestCommittedRevision(fh.Path))
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
	if appendLog && !pending {
		gen, ok = fs.shadowStore.PinResidentOrDiscardDisk(fh.Path, minRevision)
	} else {
		gen, ok = fs.shadowStore.PinIfExists(fh.Path)
	}
	if ok {
		fh.ShadowGen, fh.ShadowPinned = gen, true
	}
}
