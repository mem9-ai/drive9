package fuse

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"time"
)

// pathCommitLandmark is the last exact snapshot this queue itself landed for
// a path. It is never persisted: after restart there is no safe causal proof
// between mutable shadow bytes and recovered metadata.
type pathCommitLandmark struct {
	rev        int64
	size       int64
	checksum   string
	snapshotID string
	lastUsed   uint64
}

const (
	maxLandedCommitLandmarks = 4096
	maxLandedPayloadBytes    = 1 << 20
)

func (cq *CommitQueue) landedLimitLocked() int {
	limit := maxLandedCommitLandmarks
	if cq.maxPending > limit/2 && cq.maxPending <= int(^uint(0)>>1)/2 {
		limit = 2 * cq.maxPending
	}
	return limit
}

func payloadChecksum(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func directParentPendingNew(old, newer *CommitEntry) bool {
	if old == nil || newer == nil || old == newer || old.Path != newer.Path ||
		old.Kind != PendingNew || newer.Kind != PendingNew ||
		old.BaseRev != 0 || newer.BaseRev != 0 ||
		!old.PayloadBaseRevSet || old.PayloadBaseRev != 0 ||
		!newer.PayloadBaseRevSet || newer.PayloadBaseRev != 0 ||
		!old.liveLineageProof || !newer.liveLineageProof ||
		old.SnapshotID == "" || newer.SnapshotID == "" ||
		newer.ParentSnapshotID != old.SnapshotID || old.payloadBound {
		return false
	}
	if old.HasMode != newer.HasMode {
		return false
	}
	return !old.HasMode || old.Mode == newer.Mode
}

func (cq *CommitQueue) reparentQueuedSnapshotLocked(entry *CommitEntry, oldParentID, newParentID string) bool {
	if cq == nil || entry == nil || cq.index == nil || entry.PendingIndexGen == 0 {
		return false
	}
	// Lineage is intentionally process-local: mutable ShadowStore bytes and
	// their JSON metadata are not atomically replaced across a crash. Recovery
	// therefore loses this proof and fails closed rather than trusting it.
	ok, err := cq.index.ReparentSnapshotIfGeneration(entry.Path, entry.PendingIndexGen, entry.SnapshotID, oldParentID, newParentID)
	if err != nil {
		safeLogPrintf("commit queue: direct-parent compression failed for %s: %v", entry.Path, err)
		return false
	}
	return ok
}

// coalesceDirectParentQueuedLocked replaces broad seq/size-based superseding.
// A full child snapshot can safely skip an unlanded queued parent only when
// its process-local ParentSnapshotID names that exact entry. The child is
// reparented in generation-checked in-memory metadata before cancellation;
// crash recovery intentionally loses the proof.
func (cq *CommitQueue) coalesceDirectParentQueuedLocked(entry *CommitEntry) {
	if cq == nil || entry == nil || entry.ParentSnapshotID == "" {
		return
	}
	candidates := cq.queuedByPath[entry.Path]
	if cq.queuedByPath == nil {
		candidates = make(map[*CommitEntry]struct{})
		for _, queued := range cq.queue {
			if queued != nil && queued.Path == entry.Path {
				candidates[queued] = struct{}{}
			}
		}
	}
	for queued := range candidates {
		if !directParentPendingNew(queued, entry) || queued.canceled || cq.inFlight[queued.Path] == queued {
			continue
		}
		oldParentID := entry.ParentSnapshotID
		newParentID := queued.ParentSnapshotID
		if !cq.reparentQueuedSnapshotLocked(entry, oldParentID, newParentID) {
			return
		}
		entry.ParentSnapshotID = newParentID
		queued.canceled = true
		cq.stopDelayedLocked(queued)
		if queued.cancelCommit != nil {
			queued.cancelCommit()
		}
		if queued.cancelUpload != nil {
			queued.cancelUpload()
		}
		remaining := cq.queue[:0]
		for _, candidate := range cq.queue {
			if candidate != queued {
				remaining = append(remaining, candidate)
			}
		}
		cq.queue = remaining
		cq.rebuildQueuedIndexLocked()
		safeLogPrintf("commit queue: coalesced exact queued parent for %s (snapshot %s -> child %s)", entry.Path, queued.SnapshotID, entry.SnapshotID)
		return
	}
}

func (cq *CommitQueue) checksumLandedPayload(entry *CommitEntry) string {
	if entry == nil {
		return ""
	}
	if entry.Size < 0 || entry.Size > maxLandedPayloadBytes {
		return ""
	}
	// Once upload has bound a payload, that private copy is the exact image
	// accepted by the server. A newer same-path writer may already have
	// replaced the active shadow generation before this success tail runs, so
	// consulting the path-local shadow first would lose valid lineage proof.
	if entry.payloadBound {
		if int64(len(entry.payload)) != entry.Size {
			return ""
		}
		return payloadChecksum(entry.payload)
	}
	if cq == nil || cq.shadows == nil || entry.ShadowGen == 0 {
		return ""
	}
	data, err := cq.shadows.ReadAllIfGeneration(entry.Path, entry.ShadowGen)
	if err != nil || int64(len(data)) != entry.Size {
		return ""
	}
	return payloadChecksum(data)
}

// pruneLandedLocked bounds process-local lineage metadata. Evicting an
// unreferenced landmark can only make a future growth attempt fail closed;
// landmarks needed by already queued or in-flight direct children are kept.
func (cq *CommitQueue) pruneLandedLocked() {
	for len(cq.landed) > cq.landedLimitLocked() {
		type lineageRef struct {
			path     string
			parentID string
		}
		referenced := make(map[lineageRef]struct{}, len(cq.queue)+len(cq.inFlight)+len(cq.immediate))
		rememberReference := func(entry *CommitEntry) {
			if entry != nil && !entry.canceled && entry.Path != "" && entry.ParentSnapshotID != "" {
				referenced[lineageRef{path: entry.Path, parentID: entry.ParentSnapshotID}] = struct{}{}
			}
		}
		for _, entry := range cq.queue {
			rememberReference(entry)
		}
		for _, entry := range cq.inFlight {
			rememberReference(entry)
		}
		for entry := range cq.immediate {
			rememberReference(entry)
		}
		oldestPath := ""
		oldestClock := ^uint64(0)
		for path, landmark := range cq.landed {
			if _, ok := referenced[lineageRef{path: path, parentID: landmark.snapshotID}]; ok {
				continue
			}
			if landmark.lastUsed < oldestClock {
				oldestPath = path
				oldestClock = landmark.lastUsed
			}
		}
		if oldestPath == "" {
			return
		}
		delete(cq.landed, oldestPath)
	}
}

func (cq *CommitQueue) rememberLanded(path string, rev, size int64, checksum, snapshotID string) {
	if cq == nil || path == "" || rev <= 0 {
		return
	}
	cq.mu.Lock()
	if cq.landed == nil {
		cq.landed = make(map[string]pathCommitLandmark)
	}
	// Without both causal lineage and a verifiable remote identity this commit
	// cannot authorize a rebase. It still supersedes any older landmark for the
	// path, so invalidate that proof instead of leaving stale metadata behind.
	if snapshotID == "" || checksum == "" {
		delete(cq.landed, path)
		cq.pruneLandedLocked()
		cq.mu.Unlock()
		return
	}
	// Same-path queue commits are serialized. Always replace the prior proof:
	// delete/recreate can legitimately reset the server revision, so numeric
	// ordering alone must not retain a landmark from the old incarnation.
	cq.landedClock++
	cq.landed[path] = pathCommitLandmark{rev: rev, size: size, checksum: checksum, snapshotID: snapshotID, lastUsed: cq.landedClock}
	cq.pruneLandedLocked()
	cq.mu.Unlock()
}

func (cq *CommitQueue) landedCommit(path string) pathCommitLandmark {
	if cq == nil {
		return pathCommitLandmark{}
	}
	cq.mu.Lock()
	defer cq.mu.Unlock()
	landmark := cq.landed[path]
	if landmark.snapshotID != "" {
		cq.landedClock++
		landmark.lastUsed = cq.landedClock
		cq.landed[path] = landmark
	}
	return landmark
}

// maybeRebaseGrownPayloadOntoWatermark detects issue #896: a later same-path
// mutation grew the file after this queue already landed a smaller snapshot.
// Those bytes are newer than the watermark, so CAS should overwrite that
// revision instead of being fenced as a stale #876 checkpoint snapshot.
func (cq *CommitQueue) maybeRebaseGrownPayloadOntoWatermark(ctx context.Context, entry *CommitEntry, watermark int64) bool {
	if cq == nil || entry == nil || watermark <= 0 {
		return false
	}
	payloadBase := entry.payloadBaseRevision()
	if payloadBase >= watermark {
		return false
	}
	landed := cq.landedCommit(entry.Path)
	if landed.rev != watermark || !entryCanRebaseOntoLandedParent(entry, landed) {
		return false
	}
	rev, size, body, err := cq.readRemoteSnapshot(ctx, entry.Path)
	if err != nil {
		return false
	}
	return cq.maybeRebaseGrownPayloadAgainstRemote(entry, rev, size, body)
}

func (cq *CommitQueue) readRemoteSnapshot(parent context.Context, path string) (rev, size int64, body []byte, err error) {
	if cq == nil || cq.client == nil || path == "" {
		return 0, 0, nil, fmt.Errorf("missing remote snapshot source")
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	apiPath := cq.remotePath(path)
	st, err := cq.client.StatCtx(ctx, apiPath)
	if err != nil {
		return 0, 0, nil, err
	}
	if st == nil || st.Size < 0 || st.Size > maxLandedPayloadBytes {
		return 0, 0, nil, fmt.Errorf("remote snapshot exceeds lineage proof limit")
	}
	// Bound the response even if the remote object grows after Stat.
	reader, err := cq.client.ReadStreamRange(ctx, apiPath, 0, maxLandedPayloadBytes+1)
	if err != nil {
		return 0, 0, nil, err
	}
	defer func() { _ = reader.Close() }()
	body, err = io.ReadAll(io.LimitReader(reader, maxLandedPayloadBytes+1))
	return st.Revision, st.Size, body, err
}

func landedIdentityMatches(proof pathCommitLandmark, serverRev, serverSize int64, serverBody []byte) bool {
	if proof.rev != serverRev || proof.size != serverSize {
		return false
	}
	if proof.checksum == "" || int64(len(serverBody)) != proof.size {
		return false
	}
	return payloadChecksum(serverBody) == proof.checksum
}

// maybeRebaseGrownPayloadAgainstRemote authorizes the #896 growth rebase only
// when the remote image still matches the in-memory landed parent (SHA-256
// of a payload no larger than maxLandedPayloadBytes).
func (cq *CommitQueue) maybeRebaseGrownPayloadAgainstRemote(entry *CommitEntry, serverRev, serverSize int64, serverBody []byte) bool {
	proof := cq.landedCommit(entry.Path)
	if !entryCanRebaseOntoLandedParent(entry, proof) || !landedIdentityMatches(proof, serverRev, serverSize, serverBody) {
		return false
	}
	return cq.authorizeGrownPayloadRebase(entry, proof)
}

func entryCanRebaseOntoLandedParent(entry *CommitEntry, proof pathCommitLandmark) bool {
	if entry == nil || entry.Kind != PendingNew || entry.BaseRev != 0 ||
		!entry.PayloadBaseRevSet || entry.PayloadBaseRev != 0 ||
		!entry.liveLineageProof || entry.SnapshotID == "" ||
		entry.ParentSnapshotID == "" || proof.snapshotID == "" {
		return false
	}
	if entry.recovered {
		return false
	}
	return entry.ParentSnapshotID == proof.snapshotID
}

func (cq *CommitQueue) authorizeGrownPayloadRebase(entry *CommitEntry, proof pathCommitLandmark) bool {
	if cq == nil || !entryCanRebaseOntoLandedParent(entry, proof) || proof.rev <= 0 || entry.Size <= proof.size {
		return false
	}
	entry.BaseRev = proof.rev
	entry.Kind = PendingOverwrite
	if entry.DurableWatermarkRev < proof.rev {
		entry.DurableWatermarkRev = proof.rev
	}
	entry.DisableAutoResolveLWW = true
	entry.growthRebaseRev = proof.rev
	entry.growthRebaseParentSnapshotID = entry.ParentSnapshotID
	safeLogPrintf("commit queue: rebasing direct-child grown payload for %s onto rev %d (size %d -> %d)", entry.Path, proof.rev, proof.size, entry.Size)
	return true
}

// commitGrownPayload consumes the one-shot authorization through the same
// upload/finalization path for buffered and ShadowSpill entries.
func (cq *CommitQueue) commitGrownPayload(ctx context.Context, entry *CommitEntry) {
	if cq.discardSupersededEntry(entry) {
		return
	}
	uploadCtx, cancel, ok := cq.entryUploadContext(ctx, entry, releaseTimeout(entry.Size))
	if !ok {
		cq.removeFromQueue(entry)
		return
	}
	rev, err := cq.uploadEntry(uploadCtx, entry)
	cancel()
	if err != nil || cq.isEntryCanceled(entry) || ctx.Err() != nil {
		cq.finishResolveFailure(ctx, entry, err)
		return
	}
	if err := cq.onCommitSuccess(entry, entry.BaseRev, rev); err != nil {
		cq.onCommitPostUploadFailure(entry, err)
	}
}

func (cq *CommitQueue) finishResolveFailure(ctx context.Context, entry *CommitEntry, err error) {
	if cq.isEntryCanceled(entry) || ctx.Err() != nil {
		// Cancellation preserves owned staging for retry; it is not a content conflict.
		cq.removeFromQueue(entry)
		return
	}
	cq.onCommitTerminalFailure(entry, err)
}
