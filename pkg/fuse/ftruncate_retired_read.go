package fuse

import (
	"context"
	"slices"
	"syscall"

	"github.com/pingcap/failpoint"
)

// retiredReadImage belongs to one buffer's content epoch. It is never writable
// ancestry or staging ownership. Replacing/restoring a buffer creates a new epoch.
type retiredReadImage struct {
	ino          uint64
	resource     string
	view         uint64
	baseline     *retiredReadBaseline
	loader       LoadPartFunc
	completed    *retiredReadPublication
	publications []*retiredReadPublication
}

type retiredReadBaseline struct{ revision, size int64 }

// Source fields are frozen before queue/cache publication. Only ack is mutable;
// fs.dirtyMu protects it, so a worker never needs the owner's handle mutex.
type retiredReadPublication struct {
	path, resource, snapshot string
	ino, view, seq, version  uint64
	size, base               int64
	gens                     StagingGens
	ack                      *retiredReadAck
}

type retiredReadAck struct {
	revision int64
	actual   bool
}

func cloneRetiredReadImage(p *retiredReadImage) *retiredReadImage {
	if p == nil {
		return nil
	}
	copy := *p
	copy.publications = append([]*retiredReadPublication(nil), p.publications...)
	return &copy
}

// Existing acknowledged supersession may replace an image without claiming
// that its old publications uploaded. Preserve eligibility, not their authority.
func invalidateRetiredReadImage(wb *WriteBuffer) {
	if wb != nil && wb.retiredRead != nil {
		old := wb.retiredRead
		wb.retiredRead = &retiredReadImage{ino: old.ino, resource: old.resource, view: old.view, loader: old.loader}
	}
}

func hasRetiredRead(fh *FileHandle) bool {
	return fh != nil && fh.Dirty != nil && fh.Dirty.retiredRead != nil && fh.pendingFtruncate.Load() == nil
}

func (fs *Dat9FS) retiredReadNeedsOwner(fh *FileHandle) bool {
	if !hasRetiredRead(fh) {
		return false
	}
	i := fh.Dirty.retiredRead
	if n := len(i.publications); n != 0 {
		last := i.publications[n-1]
		fs.dirtyMu.Lock()
		defer fs.dirtyMu.Unlock()
		return i.completed != last || i.baseline == nil || last.ack == nil || i.baseline.revision < last.ack.revision
	}
	return false
}

func (fs *Dat9FS) retiredReadOwnCommit(fh *FileHandle, revision, size int64) bool {
	if !hasRetiredRead(fh) {
		return false
	}
	fs.dirtyMu.Lock()
	defer fs.dirtyMu.Unlock()
	for _, p := range fh.Dirty.retiredRead.publications {
		if p.ack != nil && p.ack.revision == revision && p.size == size {
			return true
		}
	}
	return false
}

func (fs *Dat9FS) retiredReadCASMatches(fh *FileHandle) bool {
	i := fh.Dirty.retiredRead
	if i.baseline != nil && fh.BaseRev == i.baseline.revision {
		return true
	}
	fs.dirtyMu.Lock()
	defer fs.dirtyMu.Unlock()
	for _, p := range i.publications {
		if p.ack != nil && (fh.BaseRev == p.base || fh.BaseRev == p.ack.revision) {
			return true
		}
	}
	return false
}

func (fs *Dat9FS) retiredReadPending(fh *FileHandle) bool {
	if !hasRetiredRead(fh) {
		return false
	}
	p := fh.Dirty.retiredRead.publications
	fs.dirtyMu.Lock()
	defer fs.dirtyMu.Unlock()
	return len(p) != 0 && p[len(p)-1].ack == nil
}

func (fs *Dat9FS) captureRetiredReadLocked(fh *FileHandle) *retiredReadPublication {
	if !hasRetiredRead(fh) || fh.pendingFtruncate.Load() != nil || fs.layerEnabled() {
		return nil
	}
	i := fh.Dirty.retiredRead
	p := &retiredReadPublication{path: fh.Path, ino: fh.Ino, resource: i.resource, view: i.view,
		seq: fh.DirtySeq, version: fh.Dirty.contentVersion, size: fh.Dirty.Size(), base: fh.BaseRev,
		snapshot: fh.StagedSnapshotID, gens: fs.captureHandleStagingGensLocked(fh)}
	i.publications = append(i.publications, p)
	failpoint.InjectCall("retiredFtruncatePublicationCaptured", fs, fh, p)
	return p
}

func ownsRetiredReadPublication(fh *FileHandle, source *retiredReadPublication) bool {
	if source == nil || !hasRetiredRead(fh) {
		return false
	}
	for _, p := range fh.Dirty.retiredRead.publications {
		if p == source {
			return true
		}
	}
	return false
}

func (fs *Dat9FS) forgetRetiredReadCapture(fh *FileHandle, p *retiredReadPublication) {
	if p == nil || !hasRetiredRead(fh) {
		return
	}
	i := fh.Dirty.retiredRead
	if n := len(i.publications); n != 0 && i.publications[n-1] == p {
		i.publications = i.publications[:n-1]
	}
}

func (p *retiredReadPublication) matches(path string, ino, seq uint64, size int64, id string, gens StagingGens) bool {
	return p != nil && p.path == path && p.ino == ino && p.seq == seq && p.size == size &&
		p.snapshot == id && p.gens == gens
}

func (fs *Dat9FS) recordRetiredReadAck(p *retiredReadPublication, revision int64, actual bool) bool {
	if p == nil || revision <= 0 {
		return false
	}
	// Completion is a fact about the captured source, not current read authority.
	fs.dirtyMu.Lock()
	if p.ack == nil {
		p.ack = &retiredReadAck{revision: revision, actual: actual}
	}
	fs.dirtyMu.Unlock()
	entry, ok := fs.inodes.GetEntry(p.ino)
	return ok && entry.ResourceID == p.resource && p.view == fs.mountViewGeneration.Load() && fs.sameLinkedInode(p.path, p.ino)
}

// Completion survives replacement of the inode's latest ledger. An own full
// image ACK also covers its captured ancestors, but never a later publication.
func (fs *Dat9FS) reconcileRetiredReadLocked(ctx context.Context, fh *FileHandle, remote bool) error {
	if !hasRetiredRead(fh) || fh.pendingFtruncate.Load() != nil {
		return nil
	}
	i := fh.Dirty.retiredRead
	fs.dirtyMu.Lock()
	index := -1
	var ack *retiredReadAck
	for n, p := range i.publications {
		if p.ack != nil {
			index, ack = n, p.ack
		}
	}
	fs.dirtyMu.Unlock()
	if index < 0 {
		return nil
	}
	p := i.publications[index]
	i.publications = i.publications[index:]
	entry, exists := fs.inodes.GetEntry(fh.Ino)
	if !exists || entry.ResourceID != i.resource || p.ino != fh.Ino || p.resource != i.resource || !fs.ftruncateAliasLinked(fh) {
		return syscall.EAGAIN
	}
	i.completed = p
	if i.view != fs.mountViewGeneration.Load() {
		if fh.DirtySeq == 0 && !fh.Dirty.HasDirtyParts() && !fs.retiredReadPending(fh) {
			i.baseline = nil
			return nil
		}
		return syscall.EAGAIN
	}
	// Enqueue transfers the generation to the queue but keeps ShadowReady and
	// its sequence on the handle. Retire that read/write-through flag only for
	// this completed publication; a later staged generation remains live.
	shadowFromPublication := fh.ShadowStageGen == p.gens.ShadowGen ||
		(fh.ShadowStageGen == 0 && fh.ShadowStageSeq == p.seq)
	if p.gens.ShadowGen != 0 && shadowFromPublication && fs.shadowStore != nil && fs.shadowStore.ActiveGeneration(fh.Path) != p.gens.ShadowGen {
		clearHandleShadowClaimLocked(fh)
	}
	if i.baseline != nil && i.baseline.revision >= ack.revision {
		return nil
	}
	if !ack.actual {
		if !remote {
			return nil // The local image remains readable without a remote fetch.
		}
		stat, err := fs.client.StatCtx(ctx, fs.remotePath(fh.Path))
		if err != nil {
			return err
		}
		if stat == nil || stat.ResourceID != i.resource || stat.Revision != ack.revision || stat.Size != p.size {
			if stat != nil && stat.ResourceID == i.resource && stat.Revision > ack.revision &&
				fh.DirtySeq == 0 && !fh.Dirty.HasDirtyParts() && !fs.retiredReadPending(fh) {
				i.baseline = nil // Completed, but the current remote image is newer.
				return nil
			}
			return syscall.EAGAIN
		}
	}
	i.baseline = &retiredReadBaseline{revision: ack.revision, size: p.size}
	if fh.BaseRev <= ack.revision {
		fs.updateHandleRevisionMetadataLocked(fh, ack.revision, p.size)
		fs.recordCommittedRevisionWithSize(fh.Path, ack.revision, p.size)
		fs.inodes.advanceRevision(fh.Ino, ack.revision)
	}
	return nil
}

func (fs *Dat9FS) prepareRetiredReadLocked(ctx context.Context, fh *FileHandle) error {
	if !hasRetiredRead(fh) || fh.Unlinked {
		return nil
	}
	if err := fs.reconcileRetiredReadLocked(ctx, fh, true); err != nil {
		return err
	}
	i := fh.Dirty.retiredRead
	entry, ok := fs.inodes.GetEntry(fh.Ino)
	if !ok || i.ino != fh.Ino || entry.ResourceID != i.resource || !fs.ftruncateAliasLinked(fh) {
		return syscall.EAGAIN
	}
	known := max(entry.Revision, fs.ftruncateAliasRevision(fh))
	if i.baseline != nil && i.view == fs.mountViewGeneration.Load() && i.baseline.revision >= known && fh.BaseRev == i.baseline.revision {
		return nil
	}
	if fh.DirtySeq != 0 || fh.Dirty.HasDirtyParts() || fs.retiredReadPending(fh) {
		return syscall.EAGAIN
	}
	view, paths := fs.mountViewGeneration.Load(), fs.ftruncateAliasPaths(fh.Ino)
	if fh.RemoteCommitUnlock == nil {
		unlock, locked := fs.tryLockFtruncateAliases(fh)
		if !locked {
			return errFtruncateBusy
		}
		defer unlock()
	} else if !fh.ftruncateFence {
		if err := fs.fenceFtruncateLocked(fh); err != nil {
			return err
		}
	}
	fs.dirtyMu.Lock()
	dirty, committed := fs.dirtyInodes[fh.Ino], fs.mutationInodes[fh.Ino]
	fs.dirtyMu.Unlock()
	observed := fs.openHandles.HasVisibleTruncate(fh.Ino) || fs.hasFtruncateInheritance(fh.Ino)
	if (observed && dirty.seq > committed.committedSeq) || fs.retiredReadOtherPending(fh, committed.committedSeq) {
		return syscall.EAGAIN
	}
	stat, err := fs.client.StatCtx(ctx, fs.remotePath(fh.Path))
	if err != nil {
		return err
	}
	if stat == nil || stat.ResourceID != i.resource || stat.Revision <= 0 || stat.Revision < known || stat.Size < 0 || !fs.lockMountViewRead(view) {
		return syscall.EAGAIN
	}
	defer fs.mountViewMu.RUnlock()
	fs.dirtyMu.Lock()
	stable := fs.dirtyInodes[fh.Ino] == dirty && fs.mutationInodes[fh.Ino] == committed
	fs.dirtyMu.Unlock()
	current, present := fs.inodes.GetEntry(fh.Ino)
	if !stable || !present || current.ResourceID != i.resource || !fs.ftruncateAliasesValid(fh.Ino, paths, view) || fs.retiredReadOtherPending(fh, committed.committedSeq) {
		return syscall.EAGAIN
	}
	fh.BaseRev, fh.OrigSize = stat.Revision, stat.Size
	i.publications = nil // All own obligations are complete; this HEAD authorizes replacement.
	fs.rebindCleanWriteBufferToRemoteLocked(fh, stat.Size)
	fs.bindRetiredReadLocked(fh, i.resource, view, stat.Revision, stat.Size)
	fs.recordCommittedRevisionWithSize(fh.Path, stat.Revision, stat.Size)
	fs.inodes.advanceRevision(fh.Ino, stat.Revision)
	fs.inodes.UpdateSize(fh.Ino, stat.Size)
	return nil
}

func (fs *Dat9FS) retiredReadOtherPending(fh *FileHandle, seq uint64) bool {
	_, pending := fs.retiredReadPendingSnapshot(fh, seq)
	return pending
}

type retiredPendingRead struct {
	path, cacheID, pendingID string
	queue, cache, pending    uint64
}

func (fs *Dat9FS) retiredReadPendingSnapshot(fh *FileHandle, seq uint64) ([]retiredPendingRead, bool) {
	var snapshot []retiredPendingRead
	for _, path := range fs.ftruncateAliasPaths(fh.Ino) {
		state := retiredPendingRead{path: path}
		if fs.commitQueue != nil {
			state.queue = fs.commitQueue.mutationSeqForPath(path)
			if state.queue > seq {
				return nil, true
			}
		}
		if fs.writeBack != nil {
			if m, ok := fs.writeBack.GetMeta(path); ok {
				state.cache, state.cacheID = m.Generation, m.SnapshotID
				if m.Kind != PendingChmod && (m.MutationSeq == 0 || m.MutationSeq > seq) {
					return nil, true
				}
			}
		}
		if fs.pendingIndex != nil {
			if m, ok := fs.pendingIndex.GetMeta(path); ok {
				state.pending, state.pendingID = m.Generation, m.SnapshotID
				if m.Kind != PendingChmod && (m.MutationSeq == 0 || m.MutationSeq > seq) {
					return nil, true
				}
			}
		}
		snapshot = append(snapshot, state)
	}
	return snapshot, false
}

func (fs *Dat9FS) bindRetiredReadLocked(fh *FileHandle, resource string, view uint64, revision, size int64) {
	if fh.Dirty == nil {
		return
	}
	loader := fh.Dirty.LoadPart
	if old := fh.Dirty.retiredRead; old != nil && old.loader != nil {
		loader = old.loader
	}
	fh.Dirty.retiredRead = &retiredReadImage{ino: fh.Ino, resource: resource, view: view,
		baseline: &retiredReadBaseline{revision: revision, size: size}, loader: loader}
	fs.bindRetiredReadLoaderLocked(fh)
}

func (fs *Dat9FS) bindRetiredReadLoaderLocked(fh *FileHandle) {
	wb := fh.Dirty
	if wb.retiredRead.loader == nil {
		wb.retiredRead.loader = wb.LoadPart
	}
	wb.LoadPart = func(part int) ([]byte, error) {
		if fh.Unlinked {
			// Unlink owns a private snapshot/pinned shadow for retained bytes.
			// The old pathname may already belong to a different inode.
			if fh.Dirty == wb {
				if data, handled, err := fs.loadUnlinkedHandlePart(fh, int64(part-1)*wb.PartSize(), wb.PartSize()); handled {
					return data, err
				}
			}
			return nil, syscall.EAGAIN
		}
		if i := wb.retiredRead; i != nil && fh.pendingFtruncate.Load() != nil && i.loader != nil {
			return i.loader(part) // A new truncate uses its existing inheritance rules.
		}
		ctx, cancel := context.WithTimeout(context.Background(), fuseTimeout)
		defer cancel()
		if err := fs.reconcileRetiredReadLocked(ctx, fh, true); err != nil {
			return nil, err
		}
		i := wb.retiredRead
		if fh.Dirty != wb || i == nil || i.ino != fh.Ino || i.baseline == nil || i.view != fs.mountViewGeneration.Load() || !fs.ftruncateAliasLinked(fh) {
			return nil, syscall.EAGAIN
		}
		base, path := i.baseline, fh.Path
		offset := int64(part-1) * wb.PartSize()
		length := min(wb.PartSize(), min(wb.remoteSize, base.size)-offset)
		if length <= 0 {
			return nil, nil
		}
		check := func() bool {
			stat, err := fs.client.StatCtx(ctx, fs.remotePath(path))
			entry, ok := fs.inodes.GetEntry(fh.Ino)
			return err == nil && stat != nil && ok && entry.ResourceID == i.resource &&
				stat.ResourceID == i.resource && stat.Revision == base.revision && stat.Size == base.size
		}
		if !check() {
			return nil, syscall.EAGAIN
		}
		data, err := fs.client.ReadAtCtx(ctx, fs.remotePath(path), offset, length)
		if err != nil || int64(len(data)) != length || !check() || fh.Path != path ||
			wb.retiredRead != i || i.baseline != base || i.view != fs.mountViewGeneration.Load() || !fs.ftruncateAliasLinked(fh) {
			return nil, syscall.EAGAIN
		}
		return data, nil
	}
}

// Select a proven own dirty/queued image before an old truncate marker rejects
// its pending publication. This never borrows another writer's cleanup rights.
func (fs *Dat9FS) readRetiredFtruncateLocked(ctx context.Context, fh *FileHandle, offset int64, size uint32) ([]byte, bool, error) {
	if !hasRetiredRead(fh) || fh.pendingFtruncate.Load() != nil || fh.Unlinked || fh.UnlinkedSnapshot || fs.layerEnabled() || isSQLiteDirectIOPath(fh.Path) {
		return nil, false, nil
	}
	if offset < 0 || offset > int64(^uint64(0)>>1)-int64(size) {
		return nil, true, syscall.EINVAL
	}
	hadFence := fh.ftruncateFence
	defer func() {
		if !hadFence && fh.ftruncateFence {
			fs.releaseHandleRemoteCommitPathLocked(fh)
		}
	}()
	if err := fs.reconcileRetiredReadLocked(ctx, fh, false); err != nil {
		return nil, true, err
	}
	wb, i := fh.Dirty, fh.Dirty.retiredRead
	end := min(offset+int64(size), wb.Size())
	if end > offset && (!fh.ShadowReady || fh.ShadowStageGen == 0) {
		for part := int(offset / wb.PartSize()); part <= int((end-1)/wb.PartSize()); part++ {
			if !wb.IsPartLoaded(part) {
				if err := fs.reconcileRetiredReadLocked(ctx, fh, true); err != nil {
					return nil, true, err
				}
				break
			}
		}
	}
	entry, ok := fs.inodes.GetEntry(fh.Ino)
	if !ok || i.ino != fh.Ino || entry.ResourceID != i.resource || !fs.ftruncateAliasLinked(fh) {
		return nil, true, syscall.EAGAIN
	}
	seq := fh.DirtySeq
	if n := len(i.publications); n != 0 {
		p := i.publications[n-1]
		if seq == 0 && (p.version == wb.contentVersion || !wb.HasDirtyParts()) {
			seq = p.seq
		}
	}
	fs.dirtyMu.Lock()
	dirty, committed := fs.dirtyInodes[fh.Ino], fs.mutationInodes[fh.Ino]
	ownCommit := false
	for _, p := range i.publications {
		ownCommit = ownCommit || (p.ack != nil && p.seq == committed.committedSeq && p.ack.revision == committed.committedRevision)
	}
	fs.dirtyMu.Unlock()
	known := max(entry.Revision, fs.ftruncateAliasRevision(fh))
	if fh.DirtySeq == 0 && !wb.HasDirtyParts() && !fs.retiredReadPending(fh) &&
		!fs.openHandles.HasVisibleTruncate(fh.Ino) && !fs.hasFtruncateInheritance(fh.Ino) &&
		(dirty.seq > committed.committedSeq || fs.retiredReadOtherPending(fh, committed.committedSeq)) {
		return nil, false, nil // Preserve ordinary clean-reader/active-append selection.
	}
	refresh := !fs.retiredReadCASMatches(fh) || (i.baseline != nil && known > i.baseline.revision && (!ownCommit || known > committed.committedRevision))
	if seq == 0 || (committed.committedSeq > seq && fh.DirtySeq == 0 && !fs.retiredReadPending(fh)) || i.baseline == nil || i.view != fs.mountViewGeneration.Load() || refresh {
		if err := fs.prepareRetiredReadLocked(ctx, fh); err != nil {
			return nil, true, err
		}
		wb, i = fh.Dirty, fh.Dirty.retiredRead
		entry, ok = fs.inodes.GetEntry(fh.Ino)
		if !ok || entry.ResourceID != i.resource {
			return nil, true, syscall.EAGAIN
		}
		known = max(entry.Revision, fs.ftruncateAliasRevision(fh))
		fs.dirtyMu.Lock()
		dirty, committed = fs.dirtyInodes[fh.Ino], fs.mutationInodes[fh.Ino]
		fs.dirtyMu.Unlock()
		seq = committed.committedSeq
	}
	pending, busy := fs.retiredReadPendingSnapshot(fh, seq)
	if dirty.seq > seq || committed.committedSeq > seq || busy ||
		i.view != fs.mountViewGeneration.Load() || (i.baseline != nil && known > i.baseline.revision && len(i.publications) == 0) {
		return nil, true, syscall.EAGAIN
	}
	version, base, path, shadow := wb.contentVersion, i.baseline, fh.Path, fh.ShadowStageGen
	readerSeq, baseRev, originalSize := fh.DirtySeq, fh.BaseRev, fh.OrigSize
	var data []byte
	var loaded []int
	valid := false
	defer func() {
		if !valid {
			discardFtruncateLoadedParts(wb, loaded)
		}
	}()
	var err error
	data, loaded, err = fs.readFtruncateLiveRangeLocked(ctx, fh, offset, size)
	if err != nil {
		return nil, true, err
	}
	failpoint.InjectCall("retiredFtruncateReadBeforeValidate", fs, fh)
	if !fs.lockMountViewRead(i.view) {
		return nil, true, syscall.EAGAIN
	}
	defer fs.mountViewMu.RUnlock()
	current, exists := fs.inodes.GetEntry(fh.Ino)
	fs.dirtyMu.Lock()
	stable := fs.dirtyInodes[fh.Ino] == dirty && fs.mutationInodes[fh.Ino] == committed
	fs.dirtyMu.Unlock()
	nowPending, busy := fs.retiredReadPendingSnapshot(fh, seq)
	if !stable || !exists || current.ResourceID != i.resource || current.ResourceID != entry.ResourceID || current.Revision != entry.Revision ||
		fh.Dirty != wb || wb.retiredRead != i || wb.contentVersion != version || i.baseline != base || fh.Path != path ||
		fh.DirtySeq != readerSeq || fh.BaseRev != baseRev || fh.OrigSize != originalSize ||
		fh.pendingFtruncate.Load() != nil || !fs.ftruncateAliasLinked(fh) || fh.ShadowStageGen != shadow || busy || !slices.Equal(pending, nowPending) ||
		(shadow != 0 && (fs.shadowStore == nil || fs.shadowStore.ActiveGeneration(path) != shadow)) {
		return nil, true, syscall.EAGAIN
	}
	valid = true
	return data, true, nil
}
