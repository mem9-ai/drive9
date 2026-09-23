package fuse

import (
	"context"
	"fmt"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/logger"
)

const closeSyncBatchRetryInterval = time.Minute

type closeSyncModeCommit struct {
	mode       uint32
	generation uint64
}

// closeSyncCreateModeLocked limits the combined commit to the first upload of
// a new, nonempty inline file. All other paths retain their existing upload and
// deferred-chmod behavior. Caller holds fh.mu.
func (fs *Dat9FS) closeSyncCreateModeLocked(fh *FileHandle, size, expectedRevision int64, durability shadowUploadDurability) *closeSyncModeCommit {
	if durability != shadowUploadRemoteDurable || fh.WritePolicy != WritePolicyCloseSync || !fh.IsNew || expectedRevision != 0 ||
		fh.ShadowStageGen == 0 || fh.PendingModeGen == 0 || time.Now().UnixNano() < fs.closeSyncBatchRetryAfter.Load() ||
		fs.layerEnabled() || fs.appendLogConfiguredLocked(fh) || fh.GitWorkspaceID != "" || fh.isExtent() {
		return nil
	}
	threshold := fs.client.CachedSmallFileThreshold()
	if size <= 0 || threshold <= 0 || size >= threshold || size > client.MaxBatchWriteBytes {
		return nil
	}
	entry, ok := fs.inodes.GetEntry(fh.Ino)
	if !ok || entry.IsDir || entry.Unlinked || entry.Nlink > 1 {
		return nil
	}
	if kind := entry.Mode & syscall.S_IFMT; kind != 0 && kind != syscall.S_IFREG {
		return nil
	}
	mode, hasMode := fs.modeForPendingHandle(fh)
	if !shouldApplyRemoteMode(PendingNew, hasMode, mode) {
		return nil
	}
	return &closeSyncModeCommit{mode: mode, generation: fh.PendingModeGen}
}

type closeSyncUploadResult struct {
	revision int64
	mode     *closeSyncModeCommit
}

// uploadCloseSyncShadow selects the combined create or the ordinary shadow
// upload while preserving the caller's explicit local durability requirement.
func (fs *Dat9FS) uploadCloseSyncShadow(ctx context.Context, path string, size, expectedRevision int64, generation uint64, durability shadowUploadDurability, mode *closeSyncModeCommit) (closeSyncUploadResult, error) {
	if mode != nil && durability == shadowUploadRemoteDurable {
		result, err := fs.uploadCloseSyncCreateWithMode(ctx, path, size, generation, mode)
		if err != nil || result.mode != nil {
			return result, err
		}
	}
	revision, err := uploadFromShadowRemote(ctx, fs.client, fs.shadowStore, path, fs.remotePath(path), expectedRevision, generation, durability)
	return closeSyncUploadResult{revision: revision}, err
}

// uploadCloseSyncCreateWithMode returns an empty result with a nil error only
// when the batch endpoint is explicitly unsupported. Transport,
// decoding and per-item errors must not trigger a fallback PUT: the transaction
// may already have committed. The caller retains dirty state on those errors.
func (fs *Dat9FS) uploadCloseSyncCreateWithMode(ctx context.Context, localPath string, expectedSize int64, generation uint64, mode *closeSyncModeCommit) (closeSyncUploadResult, error) {
	if generation == 0 {
		return closeSyncUploadResult{}, fmt.Errorf("%w: combined create requires a shadow generation", errCommitPayloadStale)
	}
	fd, size, release, err := fs.shadowStore.OpenIfGeneration(localPath, generation)
	if err != nil {
		return closeSyncUploadResult{}, err
	}
	defer release()
	defer func() { _ = fd.Close() }()
	if size != expectedSize {
		return closeSyncUploadResult{}, fmt.Errorf("%w: shadow size changed before combined create", errCommitPayloadStale)
	}
	data := make([]byte, size)
	if _, err := fd.ReadAt(data, 0); err != nil {
		return closeSyncUploadResult{}, err
	}
	results, err := fs.client.BatchWriteCtx(ctx, []client.BatchWriteItem{{
		Path:             fs.remotePath(localPath),
		Data:             data,
		ExpectedRevision: 0,
		Mode:             remoteChmodMode(mode.mode),
		HasMode:          true,
	}})
	if err != nil {
		// Only a top-level 404/405 means this endpoint is unavailable. A
		// per-item error, including 404, is an ordinary commit failure.
		if isBatchWriteUnsupported(err) {
			now := time.Now()
			retryAfter := now.Add(closeSyncBatchRetryInterval).UnixNano()
			if fs.closeSyncBatchRetryAfter.Swap(retryAfter) <= now.UnixNano() {
				logger.Warn(ctx, "close-sync batch endpoint unavailable; using PUT and chmod until retry",
					zap.Error(err), zap.Duration("retry_after", closeSyncBatchRetryInterval))
			}
			return closeSyncUploadResult{}, nil
		}
		return closeSyncUploadResult{}, err
	}
	// BatchWriteCtx validates both the result count and the returned path.
	if !results[0].OK() {
		return closeSyncUploadResult{}, batchWriteResultError(results[0])
	}
	if results[0].Revision <= 0 {
		return closeSyncUploadResult{}, fmt.Errorf("combined create returned no committed revision")
	}
	return closeSyncUploadResult{revision: results[0].Revision, mode: mode}, nil
}

// adoptCombinedCommitLocked acknowledges content and its mode without retiring
// shadow staging: the caller must reseed the read cache before removing it.
// It returns false if mode acknowledgement unlocked fh and its ownership moved.
// The confirmed revision must survive a later chmod failure so retry uses CAS
// against this commit rather than attempting another create-only upload.
func (fs *Dat9FS) adoptCombinedCommitLocked(fh *FileHandle, path string, mutationSeq uint64, size int64, result closeSyncUploadResult) bool {
	if result.mode == nil {
		return true
	}
	fh.IsNew = false
	fh.BaseRev = result.revision
	fh.OrigSize = size
	fs.inodes.UpdateRevision(fh.Ino, result.revision)
	mode := result.mode
	if fh.HasPendingMode && !pendingModeMatchesLocked(fh, mode.mode, mode.generation) {
		fh.PreviousMode = mode.mode
		fh.HasPreviousMode = true
		fh.PreviousModeKnown = true
	}
	fs.finishPendingModeForHandleLocked(fh, mode.mode, mode.generation)
	return !fh.Unlinked && fh.Path == path && fh.DirtySeq == mutationSeq
}
