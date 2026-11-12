package writeback

import (
	"context"
	"crypto/sha256"
	"errors"

	"github.com/mem9-ai/drive9/pkg/client"
)

// Accessed under fixedCommitMu. The buffer can receive newer writes while this
// request is unresolved; only its captured generation is retired on success.
type pendingFixedCompletion struct {
	request         *client.FixedExtentCompletionError
	generation      uint64
	size            int64
	cacheGeneration uint64
}

func (h *bufferingHandle) resumeFixedCompletion(ctx context.Context) error {
	pending := h.fixedCompletion
	if pending == nil {
		return nil
	}
	receipt, err := pending.request.Retry(ctx)
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
