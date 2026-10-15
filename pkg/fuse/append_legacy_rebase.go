package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"syscall"
)

// rebaseLegacyAppendOntoLandedParentLocked preserves a live descendant while
// advancing only its verified remote baseline. Caller owns fh.mu and the
// existing path commit fence; no bytes or live snapshot identity are retired.
func (fs *Dat9FS) rebaseLegacyAppendOntoLandedParentLocked(ctx context.Context, fh *FileHandle, proof pathCommitLandmark) error {
	return fs.rebaseLegacyAppendOntoLandedParentWithRetryBudgetLocked(ctx, fh, proof, maxLiveSnapshotAncestors)
}

// rebaseLegacyAppendOntoLandedParentWithRetryBudgetLocked shares its wrapper's
// caller-held fh.mu and path fence. Proof retries retain the original context;
// metadata rebasing never rewrites the owned cache data or acknowledged buffer.
func (fs *Dat9FS) rebaseLegacyAppendOntoLandedParentWithRetryBudgetLocked(ctx context.Context, fh *FileHandle, proof pathCommitLandmark, proofRetries int) error {
	if fs.commitQueue != nil || fs.layerEnabled() || fh.Unlinked || fh.Dirty == nil ||
		!fh.LineageTrusted || fh.DirtySeq == 0 || !fh.Dirty.HasDirtyParts() ||
		(!fh.appendSnapshot && fh.Flags&uint32(syscall.O_APPEND) == 0) ||
		fh.Dirty.Size() > maxLandedPayloadBytes || !fh.Dirty.CanMaterializeFull() ||
		fh.ShadowSpill || (fh.Streamer != nil && fh.Streamer.Started()) ||
		proof.rev <= fh.BaseRev || !fs.sameLinkedInode(fh.Path, fh.Ino) {
		return syscall.EAGAIN
	}
	linked, ok := fs.inodes.GetEntry(fh.Ino)
	if !ok || linked.ResourceID == "" {
		return syscall.EAGAIN
	}
	// Build an immutable validation entry without preparing or publishing a
	// new handle SID on a failed check. A later upload captures its own SID.
	entry := &CommitEntry{
		Path: fh.Path, Inode: fh.Ino, MutationSeq: fh.DirtySeq,
		BaseRev: expectedRevisionForHandle(fh), PayloadBaseRev: fh.BaseRev,
		PayloadBaseRevSet: true, Kind: fs.pendingKindForHandle(fh), Size: fh.Dirty.Size(),
		SnapshotID: fh.StagedSnapshotID, ParentSnapshotID: fh.StagedParentSnapshotID,
		liveAncestors: fh.stagedAncestors, liveLineageProof: fh.StagedLineageTrusted,
	}
	if entry.SnapshotID == "" || fh.StagedSnapshotSeq != fh.DirtySeq {
		entry.SnapshotID, entry.ParentSnapshotID = generateMountID(), fh.ContentSnapshotID
		entry.liveAncestors = snapshotAncestors(fh.ContentSnapshotID, fh.contentAncestors)
		entry.liveLineageProof = fh.LineageTrusted
	}
	if !entryCanRebaseOntoLandedParent(entry, proof) ||
		(entry.Kind == PendingNew && entry.Size <= proof.size) {
		return syscall.EAGAIN
	}
	stat, data, err := readBoundedRemoteSnapshotStat(ctx, fs.client, fs.remotePath(fh.Path))
	if err != nil {
		return err
	}
	if stat.IsDir || stat.ResourceID != linked.ResourceID {
		return syscall.EAGAIN
	}
	if !landedIdentityMatches(proof, stat.Revision, stat.Size, data) || !bytes.HasPrefix(fh.Dirty.bytesView(), data) {
		return fs.retryLegacyAppendProofAdvanceLocked(ctx, fh, proof, proofRetries)
	}
	current := fs.landedAppendCommit(fh.Path)
	if !fs.sameLinkedInode(fh.Path, fh.Ino) {
		return syscall.EAGAIN
	}
	if current.rev != proof.rev || current.size != proof.size || current.snapshotID != proof.snapshotID || current.checksum != proof.checksum {
		return fs.retryLegacyAppendProofAdvanceLocked(ctx, fh, proof, proofRetries)
	}
	linkedNow, ok := fs.inodes.GetEntry(fh.Ino)
	if !ok || linkedNow.ResourceID != linked.ResourceID {
		return syscall.EAGAIN
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// The existing durable WB payload remains intact. A fresh metadata
	// generation prevents an older upload from removing this rebased cache.
	if fh.WriteBackSeq != 0 {
		if fs.writeBack == nil || fh.WriteBackGen == 0 {
			return syscall.EAGAIN
		}
		gen, err := fs.writeBack.rebaseAppendBaseIfGeneration(ctx, fh.Path, fh.WriteBackGen, proof, data)
		if err != nil {
			return err
		}
		fh.WriteBackGen = gen
	}
	fh.BaseRev, fh.OrigSize = proof.rev, proof.size
	fh.IsNew, fh.ZeroBase = false, false
	fh.appendLogRebindLayout(proof.rev, proof.size)
	fh.Dirty.contentVersion++
	fh.Dirty.prefixRevision, fh.Dirty.prefixEnd = proof.rev, proof.size
	return nil
}

// A changed remote read is retriable only when this process already landed
// a proven successor of the captured parent. Identity/namespace mismatches and
// unrelated content remain errors; the original context and proof bound hold.
func (fs *Dat9FS) retryLegacyAppendProofAdvanceLocked(ctx context.Context, fh *FileHandle, proof pathCommitLandmark, proofRetries int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	current := fs.landedAppendCommit(fh.Path)
	if proofRetries <= 0 || current.rev <= proof.rev || current.snapshotID == "" || current.checksum == "" ||
		!slices.Contains(current.ancestors, proof.snapshotID) {
		return syscall.EAGAIN
	}
	return fs.adoptLandedAppendSnapshotWithRetryBudgetLocked(ctx, fh, proofRetries-1)
}

// rebaseAppendBaseIfGeneration changes only metadata for an owned, causally
// newer immutable WB image. It uses MarkChmodPending's existing path-lock and
// atomic metadata publication pattern; the durable .dat is never rewritten.
func (c *WriteBackCache) rebaseAppendBaseIfGeneration(ctx context.Context, path string, generation uint64, proof pathCommitLandmark, remote []byte) (uint64, error) {
	pl := c.acquirePathLock(path)
	defer c.releasePathLock(path, pl)
	c.mu.Lock()
	meta, ok := c.metas[path]
	if !ok || generation == 0 || meta.Generation != generation {
		c.mu.Unlock()
		return 0, syscall.EAGAIN
	}
	updated := cloneWriteBackMeta(meta)
	c.mu.Unlock()
	entry := &CommitEntry{
		Kind: updated.Kind, BaseRev: updated.BaseRev,
		PayloadBaseRev: updated.BaseRev, PayloadBaseRevSet: true,
		Size: updated.Size, SnapshotID: updated.SnapshotID,
		ParentSnapshotID: updated.ParentSnapshotID,
		liveLineageProof: updated.lineageTrusted, liveAncestors: updated.liveAncestors,
	}
	if !entryCanRebaseOntoLandedParent(entry, proof) || updated.Size > maxLandedPayloadBytes ||
		(entry.Kind == PendingNew && entry.Size <= proof.size) {
		return 0, syscall.EAGAIN
	}
	file, err := os.Open(c.datFile(path))
	if err != nil {
		return 0, err
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, maxLandedPayloadBytes+1))
	if err != nil {
		return 0, err
	}
	if int64(len(data)) != updated.Size || !bytes.HasPrefix(data, remote) {
		return 0, syscall.EAGAIN
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	updated.Kind, updated.BaseRev = PendingOverwrite, proof.rev
	updated.Generation = c.nextGen.Add(1)
	metaBytes, err := json.Marshal(updated)
	if err != nil {
		return 0, fmt.Errorf("writeback marshal append-rebase meta: %w", err)
	}
	if err := atomicWrite(c.metaFile(path), metaBytes); err != nil {
		return 0, fmt.Errorf("writeback update append-rebase meta: %w", err)
	}
	c.mu.Lock()
	c.metas[path] = &updated
	c.mu.Unlock()
	return updated.Generation, nil
}
