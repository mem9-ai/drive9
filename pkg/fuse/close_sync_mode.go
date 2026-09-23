package fuse

import (
	"context"
	"fmt"
	"net/http"
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

func (fs *Dat9FS) closeSyncBatchAvailable(now time.Time) bool {
	retryAt := fs.closeSyncBatchRetryAt.Load()
	return retryAt == nil || !now.Before(*retryAt)
}

// deferCloseSyncBatchRetry returns true only for the caller starting a cooldown.
// Keep time.Time's monotonic reading, and do not extend an active window for
// other uploads that were already in flight when the endpoint failed.
func (fs *Dat9FS) deferCloseSyncBatchRetry(now time.Time) bool {
	retryAt := now.Add(closeSyncBatchRetryInterval)
	for {
		previous := fs.closeSyncBatchRetryAt.Load()
		if previous != nil && now.Before(*previous) {
			return false
		}
		if fs.closeSyncBatchRetryAt.CompareAndSwap(previous, &retryAt) {
			return true
		}
	}
}

// closeSyncCreateModeLocked limits the combined commit to the first upload of
// a new, nonempty inline file. All other paths retain their existing upload and
// deferred-chmod behavior. Caller holds fh.mu.
func (fs *Dat9FS) closeSyncCreateModeLocked(fh *FileHandle, size, expectedRevision int64, durability shadowUploadDurability) *closeSyncModeCommit {
	if durability != shadowUploadRemoteDurable || fh.WritePolicy != WritePolicyCloseSync || !fh.IsNew || expectedRevision != 0 ||
		fh.ShadowStageGen == 0 || fh.PendingModeGen == 0 || !fs.client.CachedBatchWriteModeSupported() || !fs.closeSyncBatchAvailable(time.Now()) ||
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

type closeSyncUploadOutcome uint8

const (
	closeSyncUploadUnconfirmed closeSyncUploadOutcome = iota
	closeSyncUploadContentCommitted
	closeSyncUploadContentAndModeCommitted
	closeSyncUploadUnsupported
	closeSyncUploadForbidden
)

type closeSyncUploadResult struct {
	outcome  closeSyncUploadOutcome
	revision int64
	mode     closeSyncModeCommit
}

// uploadCloseSyncShadow selects the combined create or the ordinary shadow
// upload. The caller obtains mode from closeSyncCreateModeLocked, which owns
// eligibility; durability still controls the ordinary/fallback shadow upload.
func (fs *Dat9FS) uploadCloseSyncShadow(ctx context.Context, path string, size, expectedRevision int64, generation uint64, durability shadowUploadDurability, mode *closeSyncModeCommit) (closeSyncUploadResult, error) {
	if mode != nil {
		result, err := fs.uploadCloseSyncCreateWithMode(ctx, path, size, generation, mode)
		if err != nil {
			return result, err
		}
		switch result.outcome {
		case closeSyncUploadContentAndModeCommitted:
			return result, nil
		case closeSyncUploadUnsupported, closeSyncUploadForbidden:
			// Both are definite non-commits. The ordinary upload preserves CAS
			// and path authorization; pending chmod still has to succeed.
		default:
			return closeSyncUploadResult{}, fmt.Errorf("combined create returned an unconfirmed outcome")
		}
	}
	revision, err := uploadFromShadowRemote(ctx, fs.client, fs.shadowStore, path, fs.remotePath(path), expectedRevision, generation, durability)
	if err != nil {
		return closeSyncUploadResult{}, err
	}
	return closeSyncUploadResult{outcome: closeSyncUploadContentCommitted, revision: revision}, nil
}

// uploadCloseSyncCreateWithMode explicitly distinguishes confirmed commits from
// definite rejections: top-level 404/405 and per-item 403 without a revision.
// Other failures must not trigger a fallback PUT: the transaction may already
// have committed. A zero result is unconfirmed and must never imply fallback.
func (fs *Dat9FS) uploadCloseSyncCreateWithMode(ctx context.Context, localPath string, expectedSize int64, generation uint64, mode *closeSyncModeCommit) (closeSyncUploadResult, error) {
	// Keep the I/O boundary fail-closed even if a future caller bypasses the gate.
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
		// Only a top-level 404/405 means this endpoint is unavailable.
		if isBatchWriteUnsupported(err) {
			if fs.deferCloseSyncBatchRetry(time.Now()) {
				logger.Warn(ctx, "close-sync batch endpoint unavailable; using PUT and chmod until retry",
					zap.Error(err), zap.Duration("retry_after", closeSyncBatchRetryInterval))
			}
			return closeSyncUploadResult{outcome: closeSyncUploadUnsupported}, nil
		}
		return closeSyncUploadResult{}, err
	}
	// BatchWriteCtx validates both the result count and the returned path.
	if results[0].Status == http.StatusForbidden && results[0].Revision == 0 {
		// The server rejects authorization before backend mutation. Retry only
		// content via PUT, leaving mode pending so chmod reports its own denial.
		// Do not disable batching for other paths or treat this as a mode ACK.
		if fs.perf.isEnabled() {
			fs.perf.closeSyncModeForbiddenFallback.add(1)
		}
		return closeSyncUploadResult{outcome: closeSyncUploadForbidden}, nil
	}
	if !results[0].OK() {
		return closeSyncUploadResult{}, batchWriteResultError(results[0])
	}
	if results[0].Revision <= 0 {
		return closeSyncUploadResult{}, fmt.Errorf("combined create returned no committed revision")
	}
	return closeSyncUploadResult{outcome: closeSyncUploadContentAndModeCommitted, revision: results[0].Revision, mode: *mode}, nil
}

// adoptCombinedCommitLocked acknowledges content and its mode without retiring
// shadow staging: the caller must reseed the read cache before removing it.
// It returns false if mode acknowledgement unlocked fh and it was unlinked,
// renamed, or received a newer content mutation while sibling modes were cleared.
// The caller's earlier checks cannot cover the new unlock/relock window inside
// finishPendingModeForHandleLocked, so this second ownership check is required.
// The confirmed revision must survive a later chmod failure so retry uses CAS
// against this commit rather than attempting another create-only upload.
func (fs *Dat9FS) adoptCombinedCommitLocked(fh *FileHandle, path string, mutationSeq uint64, size int64, result closeSyncUploadResult) bool {
	if result.outcome != closeSyncUploadContentAndModeCommitted {
		// Ordinary and fallback uploads acknowledge content only.
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
