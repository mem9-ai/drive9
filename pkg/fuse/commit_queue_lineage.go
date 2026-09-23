package fuse

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

// pathCommitLandmark is the last exact snapshot this queue itself landed for
// a path. It is never persisted: after restart there is no safe causal proof
// between mutable shadow bytes and recovered metadata.
type pathCommitLandmark struct {
	rev        int64
	size       int64
	checksum   string
	snapshotID string
	ancestors  []string
	lastUsed   uint64
}

const (
	maxLandedCommitLandmarks = 4096
	maxLandedPayloadBytes    = 1 << 20
	maxLiveSnapshotAncestors = 64
)

// snapshotAncestors extends an immutable, bounded process-local proof. Losing
// older identities only makes an old entry fail closed; none are persisted.
func snapshotAncestors(parent string, inherited []string) []string {
	if parent == "" {
		return nil
	}
	return append([]string{parent}, inherited[:min(len(inherited), maxLiveSnapshotAncestors-1)]...)
}

// discardLandedAncestor acknowledges an exact ancestor already superseded by
// this process's verified remote image. Byte prefixes alone cannot prove this.
func (cq *CommitQueue) discardLandedAncestor(ctx context.Context, entry *CommitEntry) bool {
	if entry == nil || entry.recovered || !entry.liveLineageProof || entry.SnapshotID == "" ||
		shouldApplyRemoteMode(entry.Kind, entry.HasMode, entry.Mode) {
		return false
	}
	proof := cq.landedCommit(entry.Path)
	if !slices.Contains(proof.ancestors, entry.SnapshotID) {
		return false
	}
	rev, size, body, err := cq.readRemoteSnapshot(ctx, entry.Path)
	if err != nil || !landedIdentityMatches(proof, rev, size, body) || ctx.Err() != nil {
		return false
	}
	cq.discardEntry(entry)
	return true
}

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
		// An in-flight worker owns mutable payload/rebase fields outside cq.mu.
		// Exclude it before the lineage predicate reads any of those fields.
		if queued == nil || queued.canceled || cq.inFlight[queued.Path] == queued || !directParentPendingNew(queued, entry) {
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
// landmarks needed by already queued or in-flight descendants are kept.
func (cq *CommitQueue) pruneLandedLocked() {
	limit := cq.landedLimitLocked()
	if len(cq.landed) <= limit {
		return
	}
	type lineageRef struct {
		path     string
		parentID string
	}
	referenced := make(map[lineageRef]struct{}, len(cq.queue)+len(cq.inFlight)+len(cq.immediate))
	rememberReference := func(entry *CommitEntry) {
		if entry == nil || entry.canceled || entry.Path == "" {
			return
		}
		if entry.ParentSnapshotID != "" {
			referenced[lineageRef{path: entry.Path, parentID: entry.ParentSnapshotID}] = struct{}{}
		}
		for _, ancestorID := range entry.liveAncestors {
			if ancestorID != "" {
				referenced[lineageRef{path: entry.Path, parentID: ancestorID}] = struct{}{}
			}
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
	for len(cq.landed) > limit {
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

func (cq *CommitQueue) rememberLanded(path string, rev, size int64, checksum, snapshotID string, ancestors ...string) {
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
	cq.landed[path] = pathCommitLandmark{rev: rev, size: size, checksum: checksum, snapshotID: snapshotID, ancestors: append([]string(nil), ancestors...), lastUsed: cq.landedClock}
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

// maybeRebaseGrownPayloadOntoWatermark detects a causal same-path descendant
// of an image this queue already landed. Issue #896: a later mutation grew the
// file after the queue landed a smaller snapshot. Issue #935: rapid rewrites
// staged a later snapshot while the dirty handle kept its original base rev,
// so the payload base now predates the landed revision. In both cases the
// bytes are newer than the watermark, so CAS should overwrite that revision
// instead of being fenced as a stale #876 checkpoint snapshot.
func (cq *CommitQueue) maybeRebaseGrownPayloadOntoWatermark(ctx context.Context, entry *CommitEntry, watermark int64) bool {
	if cq == nil || entry == nil || watermark <= 0 {
		return false
	}
	payloadBase := entry.payloadBaseRevision()
	if payloadBase >= watermark {
		return false
	}
	landed := cq.landedCommit(entry.Path)
	if landed.rev != watermark {
		return false
	}
	if !entryCanRebaseOntoLandedParent(entry, landed) {
		return false
	}
	rev, size, body, err := cq.readRemoteSnapshot(ctx, entry.Path)
	if err != nil {
		return false
	}
	return cq.maybeRebaseGrownPayloadAgainstRemote(entry, rev, size, body)
}

func (cq *CommitQueue) readRemoteSnapshot(parent context.Context, path string) (rev, size int64, body []byte, err error) {
	st, body, err := cq.readRemoteSnapshotStat(parent, path)
	if err != nil {
		return 0, 0, nil, err
	}
	return st.Revision, st.Size, body, nil
}

// readRemoteSnapshotStat is readRemoteSnapshot with the stat the body was read
// from, for callers that need more than the revision (mode, for instance).
func (cq *CommitQueue) readRemoteSnapshotStat(parent context.Context, path string) (*client.StatResult, []byte, error) {
	if cq == nil || cq.client == nil || path == "" {
		return nil, nil, fmt.Errorf("missing remote snapshot source")
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	apiPath := cq.remotePath(path)
	st, err := cq.client.StatCtx(ctx, apiPath)
	if err != nil {
		return nil, nil, err
	}
	if st == nil || st.Size < 0 || st.Size > maxLandedPayloadBytes {
		return nil, nil, fmt.Errorf("remote snapshot exceeds lineage proof limit")
	}
	// Bound the response even if the remote object grows after Stat.
	reader, err := cq.client.ReadStreamRange(ctx, apiPath, 0, maxLandedPayloadBytes+1)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = reader.Close() }()
	body, err := io.ReadAll(io.LimitReader(reader, maxLandedPayloadBytes+1))
	if err != nil {
		return nil, nil, err
	}
	return st, body, nil
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

// resolveStalePayloadAsIdempotent acknowledges a queued entry that the
// preflight rejected as stale while its staged bytes are exactly what is
// already durable at that path.
//
// Two upload paths can carry the same bytes for one path: the direct-PUT path
// on Release commits the file first, and the queued entry — staged before that
// commit and still carrying base revision 0 — is then rejected by
// validateEntryPayloadFreshCtx because its base revision is older than the
// path's durable watermark. That is a redundant upload, not a conflict:
// reporting it as one marks a fully durable file PendingConflict and emits a
// conflict event for a path no other writer touched. The preflight fails
// before any PUT, so the 409 auto-resolve path — the only other place that
// compares payload bytes with the durable image — never runs.
//
// The comparison is the proof, and it has to be a proof of the whole entry,
// not just of its bytes:
//
//   - The entry's staging generation must still be active, so a newer
//     same-path write cannot be answered for by superseded bytes (this also
//     covers an already-bound payload from an earlier attempt).
//   - The bytes must equal the durable image, and the revision that image was
//     read from must still be current afterwards.
//   - If the entry carries a mode to apply, the durable file must already
//     carry it; otherwise a genuinely newer writer landed the same bytes with
//     different permissions and this entry must not overwrite them.
//
// Every ambiguous case (payload unbound, image too large to compare, remote
// read failure, changed revision, mode mismatch, cancellation) fails closed
// and leaves the entry on the terminal path. Returns (false, nil) when the
// caller must continue with its terminal handling, and (true, err) when the
// entry was consumed — with err non-nil if its post-commit bookkeeping (such
// as the pending chmod) could not be completed.
func (cq *CommitQueue) resolveStalePayloadAsIdempotent(ctx context.Context, entry *CommitEntry) (bool, error) {
	if cq == nil || entry == nil || cq.shadows == nil {
		return false, nil
	}
	// Layer mounts upload through layer endpoints; Stat/Read here would
	// compare against the base FS namespace, where the entry's bytes may not
	// (and need not) appear. Keep layer rejections terminal, matching
	// tryAutoResolveConflict.
	if cq.layerRefSnapshot() != "" {
		return false, nil
	}
	// ShadowSpill payloads stream from the shadow file instead of being held
	// in memory, so the whole-payload comparison below does not apply.
	if entry.ShadowSpill {
		return false, nil
	}
	// A replaced generation means a newer writer superseded this entry: its
	// bytes may still match the remote by coincidence, but consuming this
	// entry would run success bookkeeping for superseded state.
	if entry.ShadowGen != 0 {
		if active := cq.shadows.ActiveGeneration(entry.Path); active != entry.ShadowGen {
			return false, nil
		}
	}
	local, err := cq.readEntryPayloadRaw(entry)
	if err != nil {
		return false, nil
	}
	if int64(len(local)) > maxLandedPayloadBytes {
		return false, nil
	}
	// The entry's own size is part of the claim being proved. Corrupt or stale
	// queue metadata whose Size disagrees with the staged bytes must not be
	// consumed here: the success tail records that size in the landed proof and
	// writes it to the inode and dir cache while deleting the only local
	// pending/shadow copy, so accepting it would publish a wrong size and
	// destroy the data that could correct it.
	if entry.Size != int64(len(local)) {
		return false, nil
	}
	if cq.isEntryCanceled(entry) || (ctx != nil && ctx.Err() != nil) {
		return false, nil
	}
	stat, body, err := cq.readRemoteSnapshotStat(ctx, entry.Path)
	if err != nil || stat == nil {
		return false, nil
	}
	if stat.Size != int64(len(local)) || !bytes.Equal(local, body) {
		return false, nil
	}
	// The read carries no revision condition, so a same-size write between the
	// Stat and the read could have returned newer bytes while this entry would
	// record the older revision. Require the revision to be unchanged.
	//
	// A pending mode is part of the entry's intent, so it has to be part of
	// the proof: the same bytes with different permissions is a different
	// writer's outcome, and applying this entry's mode would overwrite it.
	// Compute the expectation once and re-verify it on the confirmation stat
	// below, because a chmod does not advance the content revision: a peer can
	// change the durable mode mid-proof and leave the revision identical, so
	// checking the mode only on the first stat would accept an entry whose mode
	// no longer matches.
	remoteModeApplied := false
	modeKnown := false
	if shouldApplyRemoteMode(entry.Kind, entry.HasMode, entry.Mode) {
		if !stat.HasMode || stat.Mode&posixPermissionModeMask != entry.Mode&posixPermissionModeMask {
			return false, nil
		}
		// The durable mode already satisfies this entry; skip the redundant
		// chmod rather than issue one for metadata the server already has.
		remoteModeApplied = true
		modeKnown = true
	}
	confirmCtx, confirmCancel := context.WithTimeout(context.Background(), 10*time.Second)
	confirm, cerr := cq.client.StatCtx(confirmCtx, cq.remotePath(entry.Path))
	confirmCancel()
	if cerr != nil || confirm == nil || confirm.Revision != stat.Revision {
		return false, nil
	}
	if modeKnown {
		want := entry.Mode & posixPermissionModeMask
		if !confirm.HasMode || confirm.Mode&posixPermissionModeMask != want {
			return false, nil
		}
	}
	// Re-check after the round trips: an Unlink/Rmdir during them must win
	// over turning this entry into a success.
	if cq.isEntryCanceled(entry) || (ctx != nil && ctx.Err() != nil) {
		return false, nil
	}
	safeLogPrintf("commit queue: stale payload for %s is byte-identical to durable rev %d; resolving the rejected entry as an already-landed duplicate", entry.Path, stat.Revision)
	if err := cq.onCommitSuccessWithOptions(entry, stat.Revision, stat.Revision, remoteModeApplied); err != nil {
		cq.onCommitPostUploadFailure(entry, err)
		return true, err
	}
	return true, nil
}

// maybeRebaseGrownPayloadAgainstRemote authorizes the #896/#935 descendant
// rebase only when the remote image still matches the in-memory landed parent
// (SHA-256 of a payload no larger than maxLandedPayloadBytes). If any other
// writer changed the revision since, the proof is invalid and the entry stays
// fenced.
func (cq *CommitQueue) maybeRebaseGrownPayloadAgainstRemote(entry *CommitEntry, serverRev, serverSize int64, serverBody []byte) bool {
	proof := cq.landedCommit(entry.Path)
	if !entryCanRebaseOntoLandedParent(entry, proof) || !landedIdentityMatches(proof, serverRev, serverSize, serverBody) {
		return false
	}
	return cq.authorizeGrownPayloadRebase(entry, proof)
}

// entryCanRebaseOntoLandedParent reports whether entry is a proven causal
// descendant of the image this queue landed, so overwriting that revision is
// not a silent rollback. Two shapes qualify:
//
//   - #896 growth: a base-zero PendingNew create whose direct parent snapshot
//     is the landed image and whose payload grew.
//   - #935 superseded rewrite: a later PendingOverwrite whose payload base
//     predates the landed revision because the dirty handle kept its original
//     base rev while the queue landed its direct parent snapshot. npm/npx
//     rewrites small _cacache index files this way.
//
// Both rely on process-local ancestry; recovery loses it and
// fails closed.
func entryCanRebaseOntoLandedParent(entry *CommitEntry, proof pathCommitLandmark) bool {
	if entry == nil || !entry.liveLineageProof || entry.SnapshotID == "" ||
		entry.ParentSnapshotID == "" || proof.snapshotID == "" {
		return false
	}
	if entry.recovered {
		return false
	}
	if entry.ParentSnapshotID != proof.snapshotID && !slices.Contains(entry.liveAncestors, proof.snapshotID) {
		return false
	}
	if entry.Kind == PendingNew && entry.BaseRev == 0 &&
		entry.PayloadBaseRevSet && entry.PayloadBaseRev == 0 {
		return true
	}
	if entry.Kind == PendingOverwrite && entry.BaseRev > 0 &&
		entry.PayloadBaseRevSet && entry.PayloadBaseRev > 0 {
		return true
	}
	return false
}

func (cq *CommitQueue) authorizeGrownPayloadRebase(entry *CommitEntry, proof pathCommitLandmark) bool {
	if cq == nil || !entryCanRebaseOntoLandedParent(entry, proof) || proof.rev <= 0 {
		return false
	}
	// A #896 growth must be strictly larger than the landed image. A #935
	// superseded rewrite may be any size (npm rewrites can shrink or grow):
	// the direct-parent lineage proof, not the size delta, is what makes
	// overwriting the landed revision safe.
	if entry.Kind == PendingNew && entry.Size <= proof.size {
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
	safeLogPrintf("commit queue: rebasing direct-child payload for %s onto rev %d (size %d -> %d)", entry.Path, proof.rev, proof.size, entry.Size)
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
