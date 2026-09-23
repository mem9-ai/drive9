package fuse

import (
	"context"
	"fmt"
	"syscall"

	"github.com/mem9-ai/drive9/pkg/client"
)

type closeSyncModeCommit struct {
	mode       uint32
	generation uint64
}

// closeSyncCreateModeLocked limits the combined commit to the first upload of
// a new, nonempty inline file. All other paths retain their existing upload and
// deferred-chmod behavior. Caller holds fh.mu.
func (fs *Dat9FS) closeSyncCreateModeLocked(fh *FileHandle, size, expectedRevision int64) (closeSyncModeCommit, bool) {
	if fh.WritePolicy != WritePolicyCloseSync || !fh.IsNew || expectedRevision != 0 ||
		fh.ShadowStageGen == 0 || fh.PendingModeGen == 0 || fs.closeSyncBatchUnsupported.Load() ||
		fs.layerEnabled() || fs.appendLogConfiguredLocked(fh) || fh.GitWorkspaceID != "" || fh.isExtent() {
		return closeSyncModeCommit{}, false
	}
	threshold := fs.client.CachedSmallFileThreshold()
	if size <= 0 || threshold <= 0 || size >= threshold || size > client.MaxBatchWriteBytes {
		return closeSyncModeCommit{}, false
	}
	entry, ok := fs.inodes.GetEntry(fh.Ino)
	if !ok || entry.IsDir || entry.Unlinked || entry.Nlink > 1 {
		return closeSyncModeCommit{}, false
	}
	if kind := entry.Mode & syscall.S_IFMT; kind != 0 && kind != syscall.S_IFREG {
		return closeSyncModeCommit{}, false
	}
	mode, hasMode := fs.modeForPendingHandle(fh)
	if !shouldApplyRemoteMode(PendingNew, hasMode, mode) {
		return closeSyncModeCommit{}, false
	}
	return closeSyncModeCommit{mode: mode, generation: fh.PendingModeGen}, true
}

// uploadCloseSyncCreateWithMode returns committed=false with a nil error only
// when the batch endpoint is explicitly unsupported. Transport,
// decoding and per-item errors must not trigger a fallback PUT: the transaction
// may already have committed. The caller retains dirty state on those errors.
func (fs *Dat9FS) uploadCloseSyncCreateWithMode(ctx context.Context, localPath string, expectedSize int64, generation uint64, mode uint32) (revision int64, committed bool, err error) {
	fd, size, release, err := fs.shadowStore.OpenIfGeneration(localPath, generation)
	if err != nil {
		return 0, false, err
	}
	defer release()
	defer func() { _ = fd.Close() }()
	if size != expectedSize {
		return 0, false, fmt.Errorf("%w: shadow size changed before combined create", errCommitPayloadStale)
	}
	data := make([]byte, size)
	if _, err := fd.ReadAt(data, 0); err != nil {
		return 0, false, err
	}
	results, err := fs.client.BatchWriteCtx(ctx, []client.BatchWriteItem{{
		Path:             fs.remotePath(localPath),
		Data:             data,
		ExpectedRevision: 0,
		Mode:             remoteChmodMode(mode),
		HasMode:          true,
	}})
	if err != nil {
		// Only a top-level 404/405 means this endpoint is unavailable. A
		// per-item error, including 404, is an ordinary commit failure.
		if isBatchWriteUnsupported(err) {
			fs.closeSyncBatchUnsupported.Store(true)
			return 0, false, nil
		}
		return 0, false, err
	}
	// BatchWriteCtx validates both the result count and the returned path.
	if !results[0].OK() {
		return 0, false, batchWriteResultError(results[0])
	}
	if results[0].Revision <= 0 {
		return 0, false, fmt.Errorf("combined create returned no committed revision")
	}
	return results[0].Revision, true, nil
}
