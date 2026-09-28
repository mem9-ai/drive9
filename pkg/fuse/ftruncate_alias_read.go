package fuse

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"syscall"
	"time"

	"github.com/pingcap/failpoint"
)

// Alias readers borrow only comparable complete images. Stack-local proofs
// are rechecked before returning; staging cleanup ownership never transfers.
type aliasLiveProof struct {
	fh       *FileHandle
	seq      uint64
	id, path string
}
type aliasStageProof struct {
	path      string
	meta      *WriteBackMeta
	shadowGen uint64
}

func (fs *Dat9FS) readFtruncateAliasLocked(ctx context.Context, fh *FileHandle, offset int64, size uint32) ([]byte, bool, error) {
	data, handled, err := fs.readFtruncateAliasOnceLocked(ctx, fh, offset, size)
	if !errors.Is(err, errFtruncateBusy) {
		return data, handled, err
	}
	event := fh.pendingFtruncate.Load()
	deadline := time.Now().Add(samePathDirtyWaitTimeout)
	for errors.Is(err, errFtruncateBusy) && time.Now().Before(deadline) && ctx.Err() == nil {
		fh.Unlock()
		time.Sleep(samePathDirtyWaitInterval)
		fh.Lock()
		data, handled, err = fs.readFtruncateAliasOnceLocked(ctx, fh, offset, size)
		if !handled && fh.pendingFtruncate.Load() != event {
			return nil, true, syscall.EAGAIN
		}
	}
	return data, handled, err
}

func (fs *Dat9FS) readFtruncateAliasOnceLocked(ctx context.Context, fh *FileHandle, offset int64, size uint32) ([]byte, bool, error) {
	if !fs.ftruncateParticipates(fh) || fh.Dirty == nil || fh.DirtySeq != 0 || fh.Dirty.HasDirtyParts() {
		return nil, false, nil
	}
	event := fh.pendingFtruncate.Load()
	if event.view != fs.mountViewGeneration.Load() || !fs.ftruncateAliasLinked(fh) {
		return nil, true, syscall.EAGAIN
	}
	paths := fs.ftruncateAliasPaths(fh.Ino)
	if len(paths) <= 1 {
		return nil, false, nil
	}
	unlock, ok := fs.tryLockFtruncateAliases(fh)
	if !ok {
		return nil, true, errFtruncateBusy
	}
	defer unlock()
	entry, _ := fs.inodes.GetEntry(fh.Ino)
	fs.dirtyMu.Lock()
	committed := fs.mutationInodes[fh.Ino]
	fs.dirtyMu.Unlock()
	chosenSeq := committed.committedSeq
	chosenID := ""
	var data []byte
	var chosenAncestors []string
	proof := fs.ftruncateAliasProof(fh)
	if proof.rev == committed.committedRevision {
		chosenID = proof.snapshotID
		chosenAncestors = proof.ancestors
	}
	origin := "committed"
	if event.seq > chosenSeq {
		if !event.complete {
			return nil, true, syscall.EAGAIN
		}
		chosenSeq, chosenID, data, origin = event.seq, event.id, append([]byte(nil), event.data...), "event"
		chosenAncestors = event.ancestors
	}
	var live []aliasLiveProof
	for _, other := range fs.openHandles.SnapshotInode(fh.Ino) {
		if other == fh || other.Flags&syscall.O_ACCMODE == syscall.O_RDONLY {
			continue
		}
		if !other.TryLock() {
			return nil, true, errFtruncateBusy
		}
		if other.DirtySeq > committed.committedSeq && other.Dirty != nil {
			if other.LineageTrusted && other.DirtySeq < chosenSeq && slices.Contains(chosenAncestors, ftruncateSnapshotID(other)) {
				other.Unlock()
				continue // A known ancestor is not an independent dirty branch.
			}
			if !fs.ftruncateAliasLinked(other) || !ftruncateDescends(other, event.id) || !other.Dirty.CanMaterializeFull() || other.Dirty.Size() > maxLandedPayloadBytes {
				other.Unlock()
				return nil, true, syscall.EAGAIN
			}
			id := ftruncateSnapshotID(other)
			if other.DirtySeq == chosenSeq && chosenID != "" && id != chosenID {
				other.Unlock()
				return nil, true, syscall.EAGAIN
			}
			ancestors := other.contentAncestors
			if other.StagedSnapshotSeq == other.DirtySeq && other.StagedSnapshotID != "" {
				ancestors = other.stagedAncestors
			}
			if id == chosenID && other.DirtySeq == chosenSeq {
				chosenAncestors = ancestors
			}
			live = append(live, aliasLiveProof{other, other.DirtySeq, id, other.Path})
			if other.DirtySeq > chosenSeq {
				if chosenID != "" && id != chosenID && !slices.Contains(other.contentAncestors, chosenID) && !slices.Contains(other.stagedAncestors, chosenID) {
					other.Unlock()
					return nil, true, syscall.EAGAIN
				}
				chosenAncestors = append([]string(nil), ancestors...)
				chosenSeq, chosenID, data, origin = other.DirtySeq, id, other.Dirty.Bytes(), "live"
			}
		}
		other.Unlock()
	}
	var staged []aliasStageProof
	for p := range entry.Paths {
		if fs.pendingIndex == nil {
			break
		}
		meta, exists := fs.pendingIndex.GetMeta(p)
		if !exists {
			continue
		}
		staged = append(staged, aliasStageProof{path: p, meta: meta})
		// A live/committed descendant can prove an older staged image is
		// obsolete even before its owner's next Flush republishes metadata.
		if meta.lineageTrusted && meta.SnapshotID != chosenID && slices.Contains(chosenAncestors, meta.SnapshotID) {
			continue
		}
		if !meta.lineageTrusted || !meta.ownedStagingKnown || meta.Inode != fh.Ino || meta.MutationSeq == 0 {
			return nil, true, syscall.EAGAIN
		}
		if meta.MutationSeq <= committed.committedSeq {
			continue
		}
		staged[len(staged)-1].shadowGen = meta.ownedStagingGens.ShadowGen
		if meta.SnapshotID != event.id && !slices.Contains(processLocalMetaAncestors(meta), event.id) {
			return nil, true, syscall.EAGAIN
		}
		if meta.MutationSeq == chosenSeq && chosenID != "" && meta.SnapshotID != chosenID {
			return nil, true, syscall.EAGAIN
		}
		if meta.MutationSeq > chosenSeq {
			if chosenID != "" && meta.SnapshotID != chosenID && !slices.Contains(processLocalMetaAncestors(meta), chosenID) {
				return nil, true, syscall.EAGAIN
			}
			chosenAncestors = processLocalMetaAncestors(meta)
			if fs.shadowStore == nil || meta.Size > maxLandedPayloadBytes || meta.ownedStagingGens.ShadowGen == 0 {
				return nil, true, syscall.EAGAIN
			}
			next, err := fs.shadowStore.ReadAllIfGeneration(p, meta.ownedStagingGens.ShadowGen)
			if err != nil || int64(len(next)) != meta.Size {
				return nil, true, syscall.EAGAIN
			}
			chosenSeq, chosenID, data, origin = meta.MutationSeq, meta.SnapshotID, next, "staged"
		}
	}
	warmID := ""
	if proof.rev == committed.committedRevision {
		warmID = proof.snapshotID
	}
	if warmID == "" && committed.committedSeq == event.seq && event.complete {
		warmID = event.id
	}
	warm := origin == "committed" && warmID != "" &&
		fh.BaseRev == committed.committedRevision && fh.ContentSnapshotID == warmID && fh.LineageTrusted &&
		fh.Dirty.Size() == committed.committedSize && fh.Dirty.CanMaterializeFull()
	if warm {
		data = fh.Dirty.bytesView()
		chosenAncestors = proof.ancestors
	}
	if origin == "committed" && !warm {
		if committed.committedSeq == 0 || committed.committedRevision <= 0 || committed.committedSize > maxLandedPayloadBytes {
			return nil, true, syscall.EAGAIN
		}
		failpoint.InjectCall("ftruncateAliasBeforeRemoteRead", fs, fh)
		reader := &CommitQueue{client: fs.client, remoteRoot: fs.remoteRoot()}
		rev, n, body, err := reader.readRemoteSnapshot(ctx, fh.Path)
		if err != nil || rev != committed.committedRevision || n != committed.committedSize {
			return nil, true, syscall.EAGAIN
		}
		matched := landedIdentityMatches(proof, rev, n, body) && (proof.snapshotID == event.id || slices.Contains(proof.ancestors, event.id))
		// Root commits may use the existing legacy synchronous path. The
		// immutable complete root provides an exact byte proof in that case.
		if !matched && committed.committedSeq == event.seq && event.complete && bytes.Equal(body, event.data) {
			matched = true
		}
		if !matched {
			return nil, true, syscall.EAGAIN
		}
		chosenID, data, chosenAncestors = proof.snapshotID, body, proof.ancestors
		if chosenID == "" {
			chosenID = event.id
		}
	}
	failpoint.InjectCall("ftruncateAliasReadSelected", fs, fh, origin)
	// Revalidate actual surviving snapshots, not allocated/latestSeq counters.
	for _, ref := range live {
		found := false
		for _, h := range fs.openHandles.SnapshotInode(fh.Ino) {
			found = found || h == ref.fh
		}
		if !found || !ref.fh.TryLock() {
			return nil, true, syscall.EAGAIN
		}
		valid := ref.fh.Path == ref.path && ref.fh.DirtySeq == ref.seq && ftruncateSnapshotID(ref.fh) == ref.id && fs.ftruncateAliasLinked(ref.fh)
		ref.fh.Unlock()
		if !valid {
			return nil, true, syscall.EAGAIN
		}
	}
	for _, ref := range staged {
		current, exists := fs.pendingIndex.GetMeta(ref.path)
		if !exists || current.Generation != ref.meta.Generation || current.SnapshotID != ref.meta.SnapshotID || current.Kind != ref.meta.Kind ||
			(ref.shadowGen != 0 && (fs.shadowStore == nil || fs.shadowStore.ActiveGeneration(ref.path) != ref.shadowGen)) {
			return nil, true, syscall.EAGAIN
		}
	}
	// Detect a new pending entry on a previously empty alias.
	for p := range entry.Paths {
		if fs.pendingIndex == nil {
			break
		}
		_, exists := fs.pendingIndex.GetMeta(p)
		known := false
		for _, ref := range staged {
			known = known || ref.path == p
		}
		if exists != known {
			return nil, true, syscall.EAGAIN
		}
	}
	fs.dirtyMu.Lock()
	now := fs.mutationInodes[fh.Ino]
	dirtySeq := fs.dirtyInodes[fh.Ino].seq
	fs.dirtyMu.Unlock()
	current, exists := fs.inodes.GetEntry(fh.Ino)
	if !exists || len(current.Paths) != len(entry.Paths) || !fs.ftruncateAliasLinked(fh) || fh.pendingFtruncate.Load() != event ||
		fs.mountViewGeneration.Load() != event.view || now.committedSeq != committed.committedSeq ||
		now.committedRevision != committed.committedRevision || now.committedSize != committed.committedSize || dirtySeq > chosenSeq {
		return nil, true, syscall.EAGAIN
	}
	for p := range entry.Paths {
		ino, ok := fs.inodes.GetInode(p)
		if !ok || ino != fh.Ino {
			return nil, true, syscall.EAGAIN
		}
	}
	if origin == "committed" && !warm && fh.WriteBackGen == 0 && fh.PendingIndexGen == 0 && fh.ShadowStageGen == 0 && fs.handleCanAdoptCommittedRevisionLocked(fh) {
		fs.installFtruncateImageLocked(fh, data, committed.committedRevision, chosenID, chosenAncestors)
	}
	if offset < 0 {
		return nil, true, syscall.EINVAL
	}
	if offset >= int64(len(data)) {
		return nil, true, nil
	}
	end := min(offset+int64(size), int64(len(data)))
	if end < offset {
		return nil, true, syscall.EINVAL
	}
	return append([]byte(nil), data[offset:end]...), true, nil
}
