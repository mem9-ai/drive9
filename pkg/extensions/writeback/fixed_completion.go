package writeback

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"

	"github.com/mem9-ai/drive9/pkg/client"
)

// Accessed under fixedCommitMu. The buffer can receive newer writes while this
// request is unresolved; only its captured generation is retired on success.
type pendingFixedCompletion struct {
	request         *client.FixedExtentCompletionError
	direct          *fixedDirectRequest
	generation      uint64
	size            int64
	cacheGeneration uint64
}

// fixedDirectRequest owns the exact image and condition before dispatch. It is
// retained by the live synchronous handle, just like an extent completion.
type fixedDirectRequest struct {
	client   *client.Client
	id, path string
	data     []byte
	expected int64
	create   bool
}

func (r *fixedDirectRequest) retry(ctx context.Context) (client.FixedUploadCompletion, error) {
	ctx, err := client.WithViewOperationID(ctx, r.id)
	if err != nil {
		return client.FixedUploadCompletion{}, err
	}
	var rev int64
	if r.create {
		rev, err = r.client.CreateFileCtx(ctx, r.path)
	} else {
		rev, err = r.client.WriteCtxConditionalWithRevision(ctx, r.path, r.data, r.expected)
	}
	return client.FixedUploadCompletion{Revision: rev, Size: int64(len(r.data))}, err
}

func (h *bufferingHandle) commitFixedDirect(ctx context.Context, image []byte, create bool, revision int64, generation, cacheGeneration uint64) error {
	h.fixedCompletion = &pendingFixedCompletion{
		direct:     &fixedDirectRequest{client: h.w.opts.Client, id: "fs-" + rand.Text(), path: h.w.remotePath(h.path), data: bytes.Clone(image), expected: revision, create: create},
		generation: generation, size: int64(len(image)), cacheGeneration: cacheGeneration,
	}
	return h.resumeFixedCompletion(ctx)
}

func (h *bufferingHandle) resumeFixedCompletion(ctx context.Context) error {
	pending := h.fixedCompletion
	if pending == nil {
		return nil
	}
	ctx, cancel := h.w.detachCommitContext(ctx, pending.size)
	defer cancel()
	var receipt client.FixedUploadCompletion
	var err error
	if pending.direct != nil {
		receipt, err = pending.direct.retry(ctx)
	} else {
		receipt, err = pending.request.Retry(ctx)
	}
	if err != nil {
		return errors.Join(mapErr(err), err)
	}
	h.fixedCompletion = nil
	if pending.cacheGeneration != 0 && h.w.wbCache != nil {
		h.w.wbCache.RemoveIfGeneration(h.path, pending.cacheGeneration)
	}
	h.w.landedAt(h.path, receipt.Revision, pending.size, 0, nil)
	h.mu.Lock()
	h.retireDirtyLocked(pending.generation)
	h.landed, h.isNew = true, false
	h.baseRev = receipt.Revision
	h.selfLandSize, h.selfLandSum = 0, [sha256.Size]byte{}
	h.mu.Unlock()
	if h.w.opts.Policy == PolicyWriteSync || h.w.opts.Policy == PolicyCloseSync {
		h.w.armBypass(h.path, 0, pending.size, "sync-commit")
	}
	h.w.invalidateRev(h.path, receipt.Revision, pending.size)
	return h.applyPendingMode(ctx)
}
