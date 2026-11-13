package writeback

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

func (cq *CommitQueue) commitFixedNow(ctx context.Context, entry *CommitEntry) error {
	if entry == nil {
		return fmt.Errorf("missing fixed queue entry")
	}
	if err := cq.rejectAbandonedLayer(); err != nil {
		return err
	}
	release, discard, err := cq.beginImmediateMutationCommit(ctx, entry, false)
	if err != nil {
		return err
	}
	if release != nil {
		defer release()
	}
	// Even a superseded request may already have committed. Its durable request
	// is resolved before considering whether the current entry needs uploading.
	if err := cq.lockFixedRead(ctx, entry); err != nil {
		return err
	}
	defer cq.fixedNamespaceMu.RUnlock()
	if err := cq.recoverFixedCachePath(ctx, entry.Path); err != nil {
		return err
	}
	unlock := cq.lockPath(entry.Path)
	defer unlock()
	err = cq.commitFixedEntry(ctx, entry, discard)
	if err != nil {
		cq.recordCommitTerminalFailure(entry, err)
	}
	return err
}

func (cq *CommitQueue) commitFixedWorker(entry *CommitEntry) {
	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout(entry.Size))
	defer cancel()
	cq.mu.Lock()
	entry.cancelCommit = cancel
	canceled := entry.canceled
	cq.mu.Unlock()
	defer func() { cq.mu.Lock(); entry.cancelCommit = nil; cq.mu.Unlock() }()
	err := cq.lockFixedRead(ctx, entry)
	if err != nil {
		cq.recordCommitTerminalFailure(entry, err)
		cq.removeFromQueue(entry)
		return
	}
	defer cq.fixedNamespaceMu.RUnlock()
	err = cq.recoverFixedCachePath(ctx, entry.Path)
	if err == nil {
		unlock := cq.lockPath(entry.Path)
		err = cq.commitFixedEntry(ctx, entry, canceled)
		unlock()
	}
	if err != nil {
		cq.recordCommitTerminalFailure(entry, err)
	}
	// Failure leaves the original WAL and bytes for Flush/restart. Do not mark
	// the path committed or allocate another completion as generic retries do.
	cq.removeFromQueue(entry)
}

// Cache callbacks take the shared path lock, so this runs BEFORE taking it.
// The uploader's gate prevents two owners from settling a cache record at once.
func (cq *CommitQueue) recoverFixedCachePath(ctx context.Context, path string) error {
	if cq.fixedCache == nil {
		return nil
	}
	u := cq.fixedCache
	release := u.acquirePath(path)
	defer release()
	records, err := durableFixedPublications(cq.client, cq.journal, cq.remoteRoot)
	if err != nil {
		return err
	}
	for _, p := range records {
		if p.Path != path || p.CacheMetadata == nil {
			continue
		}
		receipt, err := p.complete(ctx, cq.client)
		if err != nil {
			return err
		}
		if _, err := u.finishFixedCache(ctx, p, receipt, nil); err != nil {
			return err
		}
	}
	return nil
}

// Called with the shared namespace and path locks. Publication recovery never
// reads the current shadow: it replays the exact immutable request in the WAL.
func (cq *CommitQueue) commitFixedEntry(ctx context.Context, entry *CommitEntry, discard bool) error {
	records, err := durableFixedPublications(cq.client, cq.journal, cq.remoteRoot)
	if err != nil {
		return err
	}
	resolved := false
	for _, p := range records {
		if p.Path != entry.Path {
			continue
		}
		if p.QueueMetadata == nil {
			return fmt.Errorf("fixed cache publication must settle first")
		}
		receipt, err := p.complete(ctx, cq.client)
		if err != nil {
			return err
		}
		if err := cq.finishFixedQueue(ctx, p, receipt); err != nil {
			return err
		}
		resolved = resolved || p.PendingGeneration == entry.PendingIndexGen
	}
	if resolved {
		cq.removeFromQueue(entry)
		return nil
	}
	if discard || cq.isEntryCanceled(entry) {
		return fmt.Errorf("fixed queue entry superseded before publication")
	}
	if cq.shadows == nil || cq.index == nil || entry.ShadowGen == 0 || entry.PendingIndexGen == 0 {
		return fmt.Errorf("fixed queue entry lacks its local owners")
	}
	meta, ok := cq.index.GetMeta(entry.Path)
	if !ok || meta.Generation != entry.PendingIndexGen || meta.Size != entry.Size || meta.BaseRev != entry.BaseRev || meta.Kind != entry.Kind || meta.Mode != entry.Mode || meta.HasMode != entry.HasMode {
		return fmt.Errorf("fixed pending image changed")
	}
	if entry.Size < 0 || entry.Size > 256*(64<<20) || entry.BaseRev < 0 || entry.Kind != staging.PendingNew && entry.Kind != staging.PendingOverwrite || entry.Kind == staging.PendingOverwrite && entry.BaseRev == 0 {
		return fmt.Errorf("invalid fixed queue image")
	}
	// Existing live lineage checks can authorize only a proven direct child.
	// No conflict handler may retry these bytes under a newly observed revision.
	if err := cq.validateEntryPayloadFreshCtx(ctx, entry); err != nil {
		return err
	}
	token, err := cq.shadows.ProtectFixedShadow(entry.Path, entry.ShadowGen)
	if err != nil {
		return err
	}
	raw, _ := json.Marshal(struct {
		Token, Path    string
		Binding        client.ViewBinding
		Size, Revision int64
	}{token, cq.remotePath(entry.Path), cq.client.ViewBinding(), entry.Size, entry.BaseRev})
	hash := sha256.Sum256(raw)
	initID := hex.EncodeToString(hash[:])
	p := fixedExtentPublication{Version: 1, Binding: cq.client.ViewBinding(), Path: entry.Path, RemotePath: cq.remotePath(entry.Path), OperationID: fixedCacheModeID("complete:" + initID), Size: entry.Size, PendingGeneration: entry.PendingIndexGen, ShadowToken: token, QueueMetadata: &fixedQueueMetadata{BaseRevision: entry.BaseRev, Kind: entry.Kind, Mode: entry.Mode, HasMode: entry.HasMode}}
	if entry.Size == 0 {
		expected := entry.BaseRev
		p.EmptyExpectedRevision = &expected
	} else {
		if err := cq.transferFixedQueue(ctx, entry, initID, &p); err != nil {
			return err
		}
	}
	receipt, err := publishFixedExtent(ctx, cq.client, cq.journal, cq.index, nil, p)
	if err != nil {
		return err
	}
	if err := cq.finishFixedQueue(ctx, p, receipt); err != nil {
		return err
	}
	cq.removeFromQueue(entry)
	return nil
}

func (cq *CommitQueue) transferFixedQueue(ctx context.Context, entry *CommitEntry, initID string, p *fixedExtentPublication) error {
	fd, size, release, err := cq.shadows.OpenIfGeneration(entry.Path, entry.ShadowGen)
	if err != nil {
		return err
	}
	// Release before cancellation/cleanup, which take the shadow path lock.
	parts, plan, transferErr := func() ([]client.FixedExtentPart, client.FixedExtentUploadPlan, error) {
		defer release()
		defer func() { _ = fd.Close() }()
		if size != entry.Size {
			return nil, client.FixedExtentUploadPlan{}, fmt.Errorf("fixed queue source size changed")
		}
		plan, err := cq.client.InitiateFixedExtentUpload(ctx, initID, p.RemotePath, size, entry.BaseRev, "")
		if err != nil {
			return nil, plan, err
		}
		parts, err := cq.client.UploadFixedExtentParts(ctx, plan, fd, size, nil)
		return parts, plan, err
	}()
	if transferErr != nil {
		if plan.UploadID == "" {
			return transferErr
		}
		cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		outcome, err := cq.client.AbortFixedUpload(cancelCtx, fixedCacheModeID("abort:"+initID), plan.UploadID)
		cancel()
		if err != nil {
			return errors.Join(transferErr, err)
		}
		if outcome.Status != "ABORTED" && outcome.Status != "EXPIRED" {
			return fmt.Errorf("fixed queue transfer unexpectedly published: %w", transferErr)
		}
		return errors.Join(transferErr, cq.shadows.ReleaseFixedShadowAttempt(entry.Path, p.ShadowToken))
	}
	p.UploadID, p.Parts = plan.UploadID, parts
	return nil
}

func (cq *CommitQueue) finishFixedQueue(ctx context.Context, p fixedExtentPublication, receipt client.FixedUploadCompletion) error {
	rev := receipt.Revision
	m := p.QueueMetadata
	if m.HasMode {
		var err error
		rev, err = cq.client.ChmodFixedFile(ctx, fixedCacheModeID(p.OperationID), p.RemotePath, receipt.ResourceID, receipt.Revision, m.Mode)
		if err != nil {
			return err
		}
	}
	retired, err := cq.index.RetireFixedShadowUpload(p.Path, p.PendingGeneration, cq.shadows, p.ShadowToken)
	if err != nil {
		return err
	}
	if err := cq.journal.SettleFixedUpload(p.OperationID); err != nil {
		return err
	}
	cq.recordCommitSeq(p.Path)
	// Never seed a newer local image with this older receipt's revision.
	if retired {
		cq.clearTerminalFailure(p.Path)
		e := &CommitEntry{Path: p.Path, BaseRev: m.BaseRevision, Size: p.Size, Kind: m.Kind, Mode: m.Mode, HasMode: m.HasMode, PendingIndexGen: p.PendingGeneration}
		if cq.OnUploaded != nil {
			cq.OnUploaded(e, rev)
		}
		if cq.OnSuccess != nil {
			cq.OnSuccess(e, rev)
		}
		if cq.OnCleanup != nil {
			cq.OnCleanup(e)
		}
	}
	return nil
}

func (w *Writeback) recoverFixedPublications(ctx context.Context) error {
	var records []fixedExtentPublication
	var err error
	if w.queue != nil {
		if err := w.queue.shadows.EnableFixedUploadRecovery(); err != nil {
			return err
		}
		records, err = durableFixedPublications(w.queue.client, w.queue.journal, w.queue.remoteRoot)
	} else if w.uploader != nil {
		records, err = w.uploader.fixedCachePublications()
	} else {
		return nil
	}
	if err != nil {
		return err
	}

	// Verify all installed owners before making a request.
	for _, p := range records {
		if p.QueueMetadata != nil && w.queue == nil || p.CacheMetadata != nil && w.uploader == nil {
			return fmt.Errorf("fixed publication recovery owner unavailable")
		}
	}
	for _, p := range records {
		receipt, err := p.complete(ctx, w.opts.Client)
		if err != nil {
			return err
		}
		if p.QueueMetadata != nil {
			err = w.queue.finishFixedQueue(ctx, p, receipt)
		} else {
			_, err = w.uploader.finishFixedCache(ctx, p, receipt, nil)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Writeback) lockFixedNamespace(paths ...string) (func(), error) {
	if w == nil || w.opts.Client == nil || w.opts.Client.ViewBinding().LayerID == "" {
		return func() {}, nil
	}
	if w.queue == nil {
		return w.uploader.lockFixedNamespace(paths...)
	}
	q := w.queue
	unlock, err := lockFixedPublicationNamespace(q.client, q.journal, q.remoteRoot, q.fixedNamespaceMu, paths...)
	if err != nil {
		return nil, err
	}
	// A worker may have entered inFlight but still be waiting for this lock.
	// Refuse rather than wait for it while holding the namespace write lock.
	q.mu.Lock()
	busy := false
	for _, path := range paths {
		for p := range q.inFlight {
			if p == path || strings.HasPrefix(p, strings.TrimSuffix(path, "/")+"/") {
				busy = true
			}
		}
		for e := range q.immediate {
			if e.Path == path || strings.HasPrefix(e.Path, strings.TrimSuffix(path, "/")+"/") {
				busy = true
			}
		}
	}
	q.mu.Unlock()
	if busy {
		unlock()
		return nil, syscall.EAGAIN
	}
	return unlock, nil
}

// Namespace cleanup may cancel an entry that entered inFlight before this
// lock was available. Its cancellation must be able to finish under that lock.
func (cq *CommitQueue) lockFixedRead(ctx context.Context, entry *CommitEntry) error {
	for !cq.fixedNamespaceMu.TryRLock() {
		if cq.isEntryCanceled(entry) {
			return context.Canceled
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	return nil
}
