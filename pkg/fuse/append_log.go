package fuse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

type appendLogHandleState struct {
	initialized          bool
	appendSafe           bool
	unsupported          bool
	sqliteWALConfirmed   bool
	sqliteWALTruncated   bool
	sqliteWALWriteAtZero bool
	// sqliteWALWriteBeyondHeader fences reset to a header-only fsync. Any
	// frame or other user write in this dirty generation requires a full image.
	sqliteWALWriteBeyondHeader bool
	// sqliteWALHeaderDirtyByteMask tracks coverage of [0, 32) by the current
	// dirty generation. It prevents a partial header fsync from rotating a WAL.
	sqliteWALHeaderDirtyByteMask uint32
	sqliteWALCommittedHeader     sqliteWALHeader
	layout                       client.ContentLayout
	revision                     int64
	size                         int64
	rewriteBaseRevision          int64
	rewriteBaseSize              int64
	hasRewriteBase               bool
}

func (state appendLogHandleState) genericSingleFallback() bool {
	return state.unsupported && state.layout == client.ContentLayoutSingle
}

type appendLogRoute uint8

const (
	appendLogRouteNotApplicable appendLogRoute = iota
	appendLogRouteCommitted
	appendLogRouteNeedsRewrite
	appendLogRouteFailed
)

type appendLogAttemptResult struct {
	route  appendLogRoute
	status gofuse.Status
}

func (fh *FileHandle) appendLogCanUseTail() bool {
	if fh == nil || fh.appendLog.unsupported {
		return false
	}
	if fh.IsNew {
		return true
	}
	return fh.appendLog.initialized && fh.appendLog.appendSafe
}

func (fh *FileHandle) appendLogRecordUserWrite(preWriteSize, offset, written int64) {
	if fh == nil || fh.IsNew || written <= 0 {
		return
	}
	if !fh.appendLog.initialized {
		fh.appendLog.initialized = true
		fh.appendLog.appendSafe = true
	}
	if offset != preWriteSize {
		fh.appendLog.appendSafe = false
	}
	fh.appendLog.recordSQLiteWALHeaderWrite(offset, written)
}

func (fh *FileHandle) appendLogRecordTruncate() {
	if fh == nil || fh.IsNew {
		return
	}
	if fh.BaseRev > 0 && fh.OrigSize >= 0 {
		fh.appendLog.rewriteBaseRevision = fh.BaseRev
		fh.appendLog.rewriteBaseSize = fh.OrigSize
		fh.appendLog.hasRewriteBase = true
	}
	fh.appendLog.initialized = true
	fh.appendLog.appendSafe = false
	fh.appendLog.sqliteWALTruncated = true
	fh.appendLog.clearSQLiteWALHeaderWrite()
}

func (state *appendLogHandleState) recordSQLiteWALHeaderWrite(offset, written int64) {
	if state == nil || written <= 0 {
		return
	}
	if offset+written > sqliteWALHeaderSize {
		state.sqliteWALWriteBeyondHeader = true
	}
	if offset >= sqliteWALHeaderSize || offset+written <= 0 {
		return
	}
	if offset == 0 {
		state.sqliteWALWriteAtZero = true
	}
	start := offset
	if start < 0 {
		start = 0
	}
	end := offset + written
	if end > sqliteWALHeaderSize {
		end = sqliteWALHeaderSize
	}
	for index := start; index < end; index++ {
		state.sqliteWALHeaderDirtyByteMask |= uint32(1) << uint(index)
	}
}

func (state *appendLogHandleState) clearSQLiteWALHeaderWrite() {
	if state == nil {
		return
	}
	state.sqliteWALWriteAtZero = false
	state.sqliteWALHeaderDirtyByteMask = 0
	state.sqliteWALWriteBeyondHeader = false
}

func (state *appendLogHandleState) sqliteWALHeaderDirtyComplete() bool {
	return state != nil && state.sqliteWALWriteAtZero && state.sqliteWALHeaderDirtyByteMask == ^uint32(0)
}

func (fh *FileHandle) appendLogObserveCommittedSQLiteWALHeader(header sqliteWALHeader) {
	if fh == nil {
		return
	}
	fh.appendLog.sqliteWALConfirmed = true
	fh.appendLog.sqliteWALCommittedHeader = header
	fh.appendLog.clearSQLiteWALHeaderWrite()
}

func (fh *FileHandle) appendLogCommittedBaseline() (revision, size int64) {
	if fh == nil {
		return 0, 0
	}
	if fh.appendLog.hasRewriteBase {
		return fh.appendLog.rewriteBaseRevision, fh.appendLog.rewriteBaseSize
	}
	return fh.BaseRev, fh.OrigSize
}

func (fh *FileHandle) appendLogObserveLayout(layout client.ContentLayout, revision, size int64) {
	if fh == nil || revision <= 0 || size < 0 {
		return
	}
	if layout != client.ContentLayoutSingle && layout != client.ContentLayoutAppendLog {
		return
	}
	fh.appendLog.layout = layout
	fh.appendLog.revision = revision
	fh.appendLog.size = size
}

func (fh *FileHandle) appendLogLayoutAt(revision, size int64) client.ContentLayout {
	if fh == nil || revision < 0 || size < 0 || fh.appendLog.revision != revision || fh.appendLog.size != size {
		return ""
	}
	return fh.appendLog.layout
}

func (fh *FileHandle) appendLogMarkAppendSuccess(revision, size int64) {
	if fh == nil {
		return
	}
	fh.appendLogObserveLayout(client.ContentLayoutAppendLog, revision, size)
	fh.appendLogAdoptCommittedBaseline(revision, size)
}

func (fh *FileHandle) appendLogMarkUnsupported() {
	if fh != nil {
		fh.appendLog.unsupported = true
	}
}

func (fh *FileHandle) appendLogAdoptCommittedBaseline(revision, size int64) {
	if fh == nil {
		return
	}
	fh.appendLog.initialized = true
	fh.appendLog.appendSafe = true
	fh.appendLog.hasRewriteBase = false
	fh.appendLog.rewriteBaseRevision = 0
	fh.appendLog.rewriteBaseSize = 0
	fh.appendLog.sqliteWALTruncated = false
	fh.appendLog.clearSQLiteWALHeaderWrite()
	if fh.appendLog.layout != "" && revision > 0 && size >= 0 {
		fh.appendLog.revision = revision
		fh.appendLog.size = size
	} else if revision <= 0 {
		fh.appendLog.layout = ""
		fh.appendLog.revision = 0
		fh.appendLog.size = 0
	}
}

func (fh *FileHandle) appendLogRebindLayout(revision, size int64) {
	if fh == nil || fh.appendLog.layout == "" || revision <= 0 || size < 0 {
		return
	}
	fh.appendLog.revision = revision
	fh.appendLog.size = size
	if fh.appendLog.hasRewriteBase {
		fh.appendLog.rewriteBaseRevision = revision
		fh.appendLog.rewriteBaseSize = size
	}
}

func (fs *Dat9FS) appendLogReadSQLiteWALHeaderLocked(fh *FileHandle) (sqliteWALHeader, bool) {
	if fh == nil || fh.Dirty == nil || fh.Dirty.Size() < sqliteWALHeaderSize {
		return sqliteWALHeader{}, false
	}
	if !fh.Dirty.IsPartLoaded(0) {
		if err := fh.Dirty.EnsureLoaded(0); err != nil {
			return sqliteWALHeader{}, false
		}
	}
	var raw [sqliteWALHeaderSize]byte
	if n := fh.Dirty.ReadAt(0, raw[:]); n != len(raw) {
		return sqliteWALHeader{}, false
	}
	return parseSQLiteWALHeader(raw[:])
}

// appendLogCaptureSQLiteWALPreWriteLocked snapshots H0 before a write can
// overwrite it. It is deliberately lazy and applies only to an explicitly
// configured existing append-log candidate.
func (fs *Dat9FS) appendLogCaptureSQLiteWALPreWriteLocked(fh *FileHandle, offset, written int64) {
	if fs == nil || fh == nil || fh.IsNew || !fs.appendLogConfiguredLocked(fh) || fh.appendLog.sqliteWALConfirmed || written <= 0 || offset >= sqliteWALHeaderSize || offset+written <= 0 {
		return
	}
	if header, ok := fs.appendLogReadSQLiteWALHeaderLocked(fh); ok {
		fh.appendLogObserveCommittedSQLiteWALHeader(header)
	}
}

func (fs *Dat9FS) appendLogConfirmSQLiteWALLocked(fh *FileHandle) bool {
	if fh == nil {
		return false
	}
	if fh.appendLog.sqliteWALConfirmed {
		return true
	}
	header, ok := fs.appendLogReadSQLiteWALHeaderLocked(fh)
	if !ok {
		return false
	}
	fh.appendLogObserveCommittedSQLiteWALHeader(header)
	return true
}

func appendLogSnapshotSQLiteWALHeader(snapshot *appendLogSnapshot) (sqliteWALHeader, bool) {
	if snapshot == nil || snapshot.Size() < sqliteWALHeaderSize {
		return sqliteWALHeader{}, false
	}
	reader, err := snapshot.Open()
	if err != nil {
		return sqliteWALHeader{}, false
	}
	defer func() { _ = reader.Close() }()
	var raw [sqliteWALHeaderSize]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return sqliteWALHeader{}, false
	}
	return parseSQLiteWALHeader(raw[:])
}

func (fs *Dat9FS) tryAppendLogLocked(ctx context.Context, fh *FileHandle) appendLogAttemptResult {
	if !fs.appendLogEligibleLocked(fh) {
		return appendLogAttemptResult{route: appendLogRouteNotApplicable, status: gofuse.OK}
	}

	snapshotPath := fh.Path
	snapshotSize := fh.Dirty.Size()
	snapshotDirtySeq := fh.DirtySeq
	snapshotIsNew := fh.IsNew
	expectedRevision := fh.BaseRev
	expectedSize := fh.OrigSize
	start := expectedSize
	if snapshotIsNew {
		expectedRevision = 0
		expectedSize = 0
		start = 0
	} else if snapshotSize < start || expectedRevision < 0 || expectedSize < 0 {
		return appendLogAttemptResult{route: appendLogRouteNotApplicable, status: gofuse.OK}
	}

	remoteCommitLockStarted := time.Now()
	unlockRemoteCommit := fs.takeHandleRemoteCommitPathLocked(fh)
	remoteCommitLockWait := time.Since(remoteCommitLockStarted)
	snapshotOwnsShadow := fh.ShadowReady || fh.ShadowSpill
	snapshot, err := fs.newAppendLogSnapshotLocked(fh, start, snapshotSize-start)
	if err != nil {
		unlockRemoteCommit()
		return appendLogAttemptResult{route: appendLogRouteFailed, status: gofuse.EIO}
	}
	defer func() { _ = snapshot.Close() }()
	appendStarted := time.Now()
	fs.debugf("append-log trace event=append_attempt path=%q base_rev=%d base_size=%d snapshot_size=%d tail_size=%d dirty_seq=%d wall_unix_nano=%d remote_commit_lock_wait_ns=%d", snapshotPath, expectedRevision, expectedSize, snapshotSize, snapshot.Size(), snapshotDirtySeq, appendStarted.UnixNano(), remoteCommitLockWait.Nanoseconds())

	appendOnce := func(revision int64) (client.AppendLogResult, error) {
		reader, openErr := snapshot.Open()
		if openErr != nil {
			return client.AppendLogResult{}, openErr
		}
		defer func() { _ = reader.Close() }()
		start := fs.perfStart()
		result, appendErr := fs.client.AppendLog(ctx, fs.remotePath(snapshotPath), reader, snapshot.Size(), revision, expectedSize)
		fs.perfRecordRemote(perfRemoteAppendLog, start, appendErr, uint64(snapshot.Size()))
		fs.recordAppendLogOutcome(appendErr)
		return result, appendErr
	}

	fh.Unlock()
	result, err := appendOnce(expectedRevision)
	if appendLogErrorCode(err) == client.AppendLogCodeRebased {
		stat, statErr := fs.client.StatCtx(ctx, fs.remotePath(snapshotPath))
		if statErr != nil || stat == nil || stat.Revision <= 0 || stat.Revision == expectedRevision || stat.Size != expectedSize {
			if statErr != nil {
				err = statErr
			} else {
				err = fmt.Errorf("append-log rebase did not provide the same size with a new positive revision")
			}
		} else {
			fs.recordAppendLogRebaseRetry()
			result, err = appendOnce(stat.Revision)
		}
	}
	if err == nil {
		if snapshotIsNew {
			fs.replaceCommittedRevision(snapshotPath, result.Revision)
		} else {
			fs.recordCommittedRevision(snapshotPath, result.Revision)
		}
		if !snapshotOwnsShadow && fs.shadowStore != nil {
			fs.shadowStore.Remove(snapshotPath)
		}
	}
	unlockRemoteCommit()
	fh.Lock()
	if err == nil {
		fs.debugf("append-log trace event=append_result path=%q result=ok revision=%d size=%d dirty_seq=%d wall_unix_nano=%d duration_ns=%d", snapshotPath, result.Revision, result.Size, snapshotDirtySeq, time.Now().UnixNano(), time.Since(appendStarted).Nanoseconds())
	} else {
		fs.debugf("append-log trace event=append_result path=%q result=error error=%q dirty_seq=%d wall_unix_nano=%d duration_ns=%d", snapshotPath, err, snapshotDirtySeq, time.Now().UnixNano(), time.Since(appendStarted).Nanoseconds())
	}

	if err != nil {
		if appendLogErrorCode(err) == client.AppendLogCodeUnsupported {
			fh.appendLogMarkUnsupported()
			if fh.Unlinked || fh.Path != snapshotPath || fh.DirtySeq != snapshotDirtySeq {
				return appendLogAttemptResult{route: appendLogRouteFailed, status: gofuse.EAGAIN}
			}
			return appendLogAttemptResult{route: appendLogRouteNeedsRewrite, status: gofuse.OK}
		}
		if appendLogErrorCode(err) == client.AppendLogCodeTooLarge {
			return appendLogAttemptResult{route: appendLogRouteFailed, status: gofuse.Status(syscall.EFBIG)}
		}
		return appendLogAttemptResult{route: appendLogRouteFailed, status: httpToFuseStatus(err)}
	}

	if fh.Unlinked || fh.Path != snapshotPath {
		return appendLogAttemptResult{route: appendLogRouteCommitted, status: gofuse.OK}
	}
	if err := fs.applyPendingModeWithTimeoutLocked(fh); err != nil {
		safeLogPrintf("append-log pending chmod failed for %s: %v", snapshotPath, err)
		return appendLogAttemptResult{route: appendLogRouteFailed, status: httpToFuseStatus(err)}
	}
	fh.IsNew = false
	fh.BaseRev = result.Revision
	fh.OrigSize = result.Size
	fh.appendLogObserveLayout(client.ContentLayoutAppendLog, result.Revision, result.Size)
	if fh.DirtySeq == snapshotDirtySeq {
		fh.Dirty.ClearDirty()
		fs.clearDirtySize(fh.Ino, snapshotDirtySeq)
		fh.DirtySeq = 0
		fh.appendLogAdoptCommittedBaseline(result.Revision, result.Size)
		if snapshotIsNew {
			if header, ok := appendLogSnapshotSQLiteWALHeader(snapshot); ok {
				fh.appendLogObserveCommittedSQLiteWALHeader(header)
			}
		}
	} else {
		fh.appendLogRebindLayout(result.Revision, result.Size)
	}
	fs.inodes.UpdateRevision(fh.Ino, result.Revision)
	fs.inodes.UpdateSize(fh.Ino, result.Size)
	fs.refreshCommittedRevisionForOpenHandlesWithSize(snapshotPath, result.Revision, fh, result.Size)
	fs.cacheFileForPath(snapshotPath, result.Size, time.Now(), result.Revision)
	if snapshotIsNew && snapshot.Size() <= fs.readCache.MaxFileSize() {
		if reader, openErr := snapshot.Open(); openErr == nil {
			if data, readErr := io.ReadAll(reader); readErr == nil {
				fs.readCache.Put(snapshotPath, data, result.Revision)
			}
			_ = reader.Close()
		}
	}
	return appendLogAttemptResult{route: appendLogRouteCommitted, status: gofuse.OK}
}

func (fs *Dat9FS) appendLogEligibleLocked(fh *FileHandle) bool {
	if fs == nil || fs.client == nil || fh == nil || fh.Dirty == nil || fs.appendLogMatcher == nil {
		return false
	}
	if fh.Layer == PathLayerLocalOnly || fh.Layer == PathLayerGitWorkspace || !fs.appendLogMatcher.Matches(fh.Path) {
		return false
	}
	return fs.client.CachedAppendLogSupported() && fh.appendLogCanUseTail()
}

func (fs *Dat9FS) newAppendLogSnapshotLocked(fh *FileHandle, offset, size int64) (*appendLogSnapshot, error) {
	if fh == nil || fh.Dirty == nil || offset < 0 || size < 0 {
		return nil, fmt.Errorf("invalid append-log snapshot source")
	}
	tempDir, err := fs.appendLogSnapshotDir()
	if err != nil {
		return nil, err
	}
	if fh.ShadowSpill {
		if fs.shadowStore == nil || !fh.ShadowReady {
			return nil, fmt.Errorf("append-log shadow snapshot is unavailable")
		}
		file, shadowSize, openErr := fs.shadowStore.Open(fh.Path)
		if openErr != nil {
			return nil, openErr
		}
		defer func() { _ = file.Close() }()
		if shadowSize < offset || shadowSize-offset < size {
			return nil, fmt.Errorf("append-log shadow snapshot is shorter than requested range")
		}
		return newAppendLogSnapshotFromReaderAt(tempDir, file, offset, size)
	}
	if fh.Dirty.Size() < offset || fh.Dirty.Size()-offset < size {
		return nil, fmt.Errorf("append-log buffer is shorter than requested range")
	}
	return newAppendLogSnapshotFromReaderAt(tempDir, appendLogWriteBufferReaderAt{buffer: fh.Dirty}, offset, size)
}

func (fs *Dat9FS) appendLogSnapshotDir() (string, error) {
	if fs == nil {
		return "", fmt.Errorf("append-log snapshot cache directory is unavailable")
	}
	dir := strings.TrimSpace(fs.appendLogSnapshotRoot)
	if dir == "" && fs.opts != nil {
		dir = strings.TrimSpace(fs.opts.CacheDir)
	}
	if dir == "" {
		return "", fmt.Errorf("append-log snapshot cache directory is unavailable")
	}
	if filepath.Base(dir) != "append-log-snapshots" {
		dir = filepath.Join(dir, "append-log-snapshots")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create append-log snapshot directory: %w", err)
	}
	fs.appendLogSnapshotSweepOnce.Do(func() {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return
		}
		for _, entry := range entries {
			if entry.Type().IsRegular() && strings.HasPrefix(entry.Name(), ".drive9-append-log-") {
				_ = os.Remove(filepath.Join(dir, entry.Name()))
			}
		}
	})
	return dir, nil
}

type appendLogWriteBufferReaderAt struct {
	buffer *WriteBuffer
}

func (reader appendLogWriteBufferReaderAt) ReadAt(buf []byte, offset int64) (int, error) {
	if reader.buffer == nil || offset < 0 {
		return 0, io.EOF
	}
	n := reader.buffer.ReadAt(offset, buf)
	if n != len(buf) {
		return n, io.EOF
	}
	return n, nil
}

func appendLogErrorCode(err error) string {
	var statusErr *client.StatusError
	if errors.As(err, &statusErr) {
		return statusErr.Code
	}
	return ""
}

func (fs *Dat9FS) recordAppendLogOutcome(err error) {
	if !fs.perfEnabled() {
		return
	}
	if err == nil {
		fs.perf.recordAppendLogOutcome(appendLogPerfOutcomeSuccess)
		return
	}
	switch appendLogErrorCode(err) {
	case client.AppendLogCodeRebased:
		fs.perf.recordAppendLogOutcome(appendLogPerfOutcomeRebased)
	case client.AppendLogCodeConflict:
		fs.perf.recordAppendLogOutcome(appendLogPerfOutcomeConflict)
	case client.AppendLogCodeUnsupported:
		fs.perf.recordAppendLogOutcome(appendLogPerfOutcomeUnsupported)
	case client.AppendLogCodeTooLarge:
		fs.perf.recordAppendLogOutcome(appendLogPerfOutcomeTooLarge)
	default:
		fs.perf.recordAppendLogOutcome(appendLogPerfOutcomeError)
	}
}

func (fs *Dat9FS) recordAppendLogRebaseRetry() {
	if fs.perfEnabled() {
		fs.perf.recordAppendLogRebaseRetry()
	}
}

func (fs *Dat9FS) recordAppendLogFullRewrite(bytes uint64) {
	if fs.perfEnabled() {
		fs.perf.recordAppendLogFullRewrite(bytes)
	}
}

func (fs *Dat9FS) recordAppendLogGenerationReset(bytes uint64) {
	if fs.perfEnabled() {
		fs.perf.recordAppendLogGenerationReset(bytes)
	}
}

func (fs *Dat9FS) recordAppendLogGenerationResetShadowReady() {
	if fs.perfEnabled() {
		fs.perf.recordAppendLogGenerationResetShadowReady()
	}
}

func (fs *Dat9FS) recordAppendLogGenerationResetShadowDegraded() {
	if fs.perfEnabled() {
		fs.perf.recordAppendLogGenerationResetShadowDegraded()
	}
}

// rotateAppendLogGenerationShadowLocked replaces a prior WAL generation's
// active shadow with the committed 32-byte header of the next generation.
// Callers hold fh.Lock() and the same-path remote commit lock.
func (fs *Dat9FS) rotateAppendLogGenerationShadowLocked(fh *FileHandle, path string, header sqliteWALHeader, revision int64) {
	if fh == nil {
		return
	}
	hadShadow := fh.ShadowReady || fh.ShadowSpill || fh.ShadowPinned ||
		(fs != nil && fs.shadowStore != nil && fs.shadowStore.Has(path))
	if !hadShadow {
		return
	}

	shadowGen := fh.ShadowGen
	shadowPinned := fh.ShadowPinned
	fh.ShadowReady = false
	fh.ShadowSpill = false
	fh.ShadowCommitReady = false
	fh.ShadowCommitSeq = 0
	fh.ShadowPinned = false
	fh.ShadowGen = 0
	if fs == nil || fs.shadowStore == nil {
		fs.recordAppendLogGenerationResetShadowDegraded()
		return
	}

	fs.shadowStore.Remove(path)
	if shadowPinned {
		fs.shadowStore.Unpin(shadowGen)
	}
	if err := fs.shadowStore.WriteFull(path, header.raw[:], revision); err != nil {
		// WriteFull can leave a newly opened or partially written active shadow.
		// The reset already committed remotely, so remove it and degrade reads.
		fs.shadowStore.Remove(path)
		fs.recordAppendLogGenerationResetShadowDegraded()
		return
	}
	fh.ShadowReady = true
	fh.ShadowSpill = true
	fs.recordAppendLogGenerationResetShadowReady()
}

// tryAppendLogGenerationResetLocked turns a proven SQLite WAL header-only
// recycle into a new 32-byte append-log generation. Callers hold fh.Lock().
func (fs *Dat9FS) tryAppendLogGenerationResetLocked(ctx context.Context, fh *FileHandle) appendLogAttemptResult {
	if fs == nil || fs.client == nil || fh == nil || fh.Dirty == nil || fh.IsNew || !fs.appendLogConfiguredLocked(fh) {
		return appendLogAttemptResult{route: appendLogRouteNotApplicable, status: gofuse.OK}
	}
	if !fs.appendLogConfirmSQLiteWALLocked(fh) {
		return appendLogAttemptResult{route: appendLogRouteNotApplicable, status: gofuse.OK}
	}
	if fh.appendLog.sqliteWALTruncated || fh.appendLog.sqliteWALWriteBeyondHeader || !fh.appendLog.sqliteWALHeaderDirtyComplete() {
		return appendLogAttemptResult{route: appendLogRouteNotApplicable, status: gofuse.OK}
	}
	newHeader, ok := fs.appendLogReadSQLiteWALHeaderLocked(fh)
	if !ok || !fh.appendLog.sqliteWALCommittedHeader.saltsDiffer(newHeader) {
		return appendLogAttemptResult{route: appendLogRouteNotApplicable, status: gofuse.OK}
	}

	expectedRevision, expectedSize := fh.appendLogCommittedBaseline()
	if expectedRevision <= 0 || expectedSize < sqliteWALHeaderSize {
		return appendLogAttemptResult{route: appendLogRouteNotApplicable, status: gofuse.OK}
	}
	snapshotPath := fh.Path
	snapshotDirtySeq := fh.DirtySeq
	remoteCommitLockStarted := time.Now()
	unlockRemoteCommit := fs.takeHandleRemoteCommitPathLocked(fh)
	remoteCommitLockWait := time.Since(remoteCommitLockStarted)
	layout := fh.appendLogLayoutAt(expectedRevision, expectedSize)
	if layout == "" {
		stat, err := fs.client.StatCtx(ctx, fs.remotePath(snapshotPath))
		if err != nil {
			unlockRemoteCommit()
			return appendLogAttemptResult{route: appendLogRouteFailed, status: httpToFuseStatus(err)}
		}
		if stat == nil || stat.Revision != expectedRevision || stat.Size != expectedSize {
			unlockRemoteCommit()
			return appendLogAttemptResult{route: appendLogRouteFailed, status: gofuse.Status(syscall.EEXIST)}
		}
		fh.appendLogObserveLayout(stat.ContentLayout, stat.Revision, stat.Size)
		layout = fh.appendLogLayoutAt(expectedRevision, expectedSize)
	}
	if layout == client.ContentLayoutSingle {
		unlockRemoteCommit()
		return appendLogAttemptResult{route: appendLogRouteNotApplicable, status: gofuse.OK}
	}
	if layout != client.ContentLayoutAppendLog {
		unlockRemoteCommit()
		return appendLogAttemptResult{route: appendLogRouteFailed, status: gofuse.EIO}
	}

	snapshot, err := fs.newAppendLogSnapshotLocked(fh, 0, sqliteWALHeaderSize)
	if err != nil {
		unlockRemoteCommit()
		return appendLogAttemptResult{route: appendLogRouteFailed, status: gofuse.EIO}
	}
	defer func() { _ = snapshot.Close() }()
	reader, err := snapshot.Open()
	if err != nil {
		unlockRemoteCommit()
		return appendLogAttemptResult{route: appendLogRouteFailed, status: gofuse.EIO}
	}
	resetStarted := time.Now()
	fs.debugf("append-log trace event=generation_reset_attempt path=%q base_rev=%d base_size=%d snapshot_size=%d dirty_seq=%d wall_unix_nano=%d remote_commit_lock_wait_ns=%d", snapshotPath, expectedRevision, expectedSize, snapshot.Size(), snapshotDirtySeq, resetStarted.UnixNano(), remoteCommitLockWait.Nanoseconds())
	fh.Unlock()
	writeStart := fs.perfStart()
	revision, err := fs.client.WriteServerStreamConditional(ctx, fs.remotePath(snapshotPath), reader, snapshot.Size(), expectedRevision)
	fs.perfRecordRemote(perfRemoteWrite, writeStart, err, uint64(snapshot.Size()))
	fs.recordAppendLogFullRewrite(uint64(snapshot.Size()))
	fs.recordAppendLogOutcome(err)
	_ = reader.Close()
	if err == nil {
		fs.recordAppendLogGenerationReset(uint64(snapshot.Size()))
		fs.recordCommittedRevision(snapshotPath, revision)
	}
	unlockRemoteCommit()
	fh.Lock()
	unlockRemoteCommit = fs.lockHandleRemoteCommitPathLocked(fh)
	remoteCommitHeld := true
	defer func() {
		if remoteCommitHeld {
			unlockRemoteCommit()
		}
	}()
	if err != nil {
		fs.debugf("append-log trace event=generation_reset_result path=%q result=error error=%q dirty_seq=%d wall_unix_nano=%d duration_ns=%d", snapshotPath, err, snapshotDirtySeq, time.Now().UnixNano(), time.Since(resetStarted).Nanoseconds())
		if appendLogErrorCode(err) == client.AppendLogCodeTooLarge {
			return appendLogAttemptResult{route: appendLogRouteFailed, status: gofuse.Status(syscall.EFBIG)}
		}
		return appendLogAttemptResult{route: appendLogRouteFailed, status: httpToFuseStatus(err)}
	}
	fs.debugf("append-log trace event=generation_reset_result path=%q result=ok revision=%d size=%d dirty_seq=%d wall_unix_nano=%d duration_ns=%d", snapshotPath, revision, snapshot.Size(), snapshotDirtySeq, time.Now().UnixNano(), time.Since(resetStarted).Nanoseconds())
	if fh.Unlinked || fh.Path != snapshotPath {
		return appendLogAttemptResult{route: appendLogRouteCommitted, status: gofuse.OK}
	}

	fh.BaseRev = revision
	fh.OrigSize = sqliteWALHeaderSize
	fh.appendLogObserveLayout(client.ContentLayoutAppendLog, revision, sqliteWALHeaderSize)
	if fh.DirtySeq == snapshotDirtySeq {
		if err := fh.Dirty.Truncate(sqliteWALHeaderSize); err != nil {
			return appendLogAttemptResult{route: appendLogRouteFailed, status: gofuse.EIO}
		}
		fh.Dirty.ClearDirty()
		fs.clearDirtySize(fh.Ino, snapshotDirtySeq)
		fh.DirtySeq = 0
		fh.appendLogAdoptCommittedBaseline(revision, sqliteWALHeaderSize)
		fh.appendLogObserveCommittedSQLiteWALHeader(newHeader)
		fs.rotateAppendLogGenerationShadowLocked(fh, snapshotPath, newHeader, revision)
		clearReadTargetForLockedHandle(fh)
		fs.readCache.Put(snapshotPath, newHeader.raw[:], revision)
	} else {
		fh.appendLogRebindLayout(revision, sqliteWALHeaderSize)
		fh.appendLog.sqliteWALConfirmed = true
		fh.appendLog.sqliteWALCommittedHeader = newHeader
		fh.appendLog.appendSafe = false
	}
	fs.inodes.UpdateRevision(fh.Ino, revision)
	fs.inodes.UpdateSize(fh.Ino, sqliteWALHeaderSize)
	fs.refreshCommittedRevisionForOpenHandlesWithSize(snapshotPath, revision, fh, sqliteWALHeaderSize)
	fs.cacheFileForPath(snapshotPath, sqliteWALHeaderSize, time.Now(), revision)
	unlockRemoteCommit()
	remoteCommitHeld = false
	if err := fs.applyPendingModeWithTimeoutLocked(fh); err != nil {
		safeLogPrintf("append-log generation-reset pending chmod failed for %s: %v", snapshotPath, err)
		return appendLogAttemptResult{route: appendLogRouteFailed, status: httpToFuseStatus(err)}
	}
	return appendLogAttemptResult{route: appendLogRouteCommitted, status: gofuse.OK}
}

func (fs *Dat9FS) tryAppendLogFullRewriteLocked(ctx context.Context, fh *FileHandle) appendLogAttemptResult {
	if fs == nil || fs.client == nil || fh == nil || fh.Dirty == nil || fh.IsNew || !fs.appendLogConfiguredLocked(fh) {
		return appendLogAttemptResult{route: appendLogRouteNotApplicable, status: gofuse.OK}
	}
	expectedRevision, expectedSize := fh.appendLogCommittedBaseline()
	if expectedRevision <= 0 || expectedSize < 0 {
		return appendLogAttemptResult{route: appendLogRouteFailed, status: gofuse.EIO}
	}

	snapshotPath := fh.Path
	snapshotDirtySeq := fh.DirtySeq
	snapshotSize := fh.Dirty.Size()
	remoteCommitLockStarted := time.Now()
	unlockRemoteCommit := fs.takeHandleRemoteCommitPathLocked(fh)
	remoteCommitLockWait := time.Since(remoteCommitLockStarted)
	layout := fh.appendLogLayoutAt(expectedRevision, expectedSize)
	if layout == "" {
		stat, err := fs.client.StatCtx(ctx, fs.remotePath(snapshotPath))
		if err != nil {
			unlockRemoteCommit()
			return appendLogAttemptResult{route: appendLogRouteFailed, status: httpToFuseStatus(err)}
		}
		if stat == nil || stat.Revision != expectedRevision || stat.Size != expectedSize {
			unlockRemoteCommit()
			return appendLogAttemptResult{route: appendLogRouteFailed, status: gofuse.Status(syscall.EEXIST)}
		}
		fh.appendLogObserveLayout(stat.ContentLayout, stat.Revision, stat.Size)
		layout = fh.appendLogLayoutAt(expectedRevision, expectedSize)
	}
	if layout == client.ContentLayoutSingle {
		unlockRemoteCommit()
		return appendLogAttemptResult{route: appendLogRouteNotApplicable, status: gofuse.OK}
	}
	if layout != client.ContentLayoutAppendLog {
		unlockRemoteCommit()
		return appendLogAttemptResult{route: appendLogRouteFailed, status: gofuse.EIO}
	}
	if !fh.ShadowSpill && !fs.materializeFullForUploadLocked(fh) {
		unlockRemoteCommit()
		return appendLogAttemptResult{route: appendLogRouteFailed, status: gofuse.EIO}
	}
	snapshot, err := fs.newAppendLogSnapshotLocked(fh, 0, snapshotSize)
	if err != nil {
		unlockRemoteCommit()
		return appendLogAttemptResult{route: appendLogRouteFailed, status: gofuse.EIO}
	}
	defer func() { _ = snapshot.Close() }()
	rewriteStarted := time.Now()
	fs.debugf("append-log trace event=rewrite_attempt path=%q base_rev=%d base_size=%d snapshot_size=%d append_safe=%t dirty_seq=%d wall_unix_nano=%d remote_commit_lock_wait_ns=%d", snapshotPath, expectedRevision, expectedSize, snapshotSize, fh.appendLog.appendSafe, snapshotDirtySeq, rewriteStarted.UnixNano(), remoteCommitLockWait.Nanoseconds())

	reader, err := snapshot.Open()
	if err != nil {
		unlockRemoteCommit()
		return appendLogAttemptResult{route: appendLogRouteFailed, status: gofuse.EIO}
	}
	fh.Unlock()
	writeStart := fs.perfStart()
	revision, err := fs.client.WriteServerStreamConditional(ctx, fs.remotePath(snapshotPath), reader, snapshot.Size(), expectedRevision)
	fs.perfRecordRemote(perfRemoteWrite, writeStart, err, uint64(snapshot.Size()))
	fs.recordAppendLogFullRewrite(uint64(snapshot.Size()))
	fs.recordAppendLogOutcome(err)
	_ = reader.Close()
	if err == nil {
		fs.recordCommittedRevision(snapshotPath, revision)
	}
	unlockRemoteCommit()
	fh.Lock()
	if err == nil {
		fs.debugf("append-log trace event=rewrite_result path=%q result=ok revision=%d size=%d dirty_seq=%d wall_unix_nano=%d duration_ns=%d", snapshotPath, revision, snapshot.Size(), snapshotDirtySeq, time.Now().UnixNano(), time.Since(rewriteStarted).Nanoseconds())
	} else {
		fs.debugf("append-log trace event=rewrite_result path=%q result=error error=%q dirty_seq=%d wall_unix_nano=%d duration_ns=%d", snapshotPath, err, snapshotDirtySeq, time.Now().UnixNano(), time.Since(rewriteStarted).Nanoseconds())
	}
	if err != nil {
		if appendLogErrorCode(err) == client.AppendLogCodeTooLarge {
			return appendLogAttemptResult{route: appendLogRouteFailed, status: gofuse.Status(syscall.EFBIG)}
		}
		return appendLogAttemptResult{route: appendLogRouteFailed, status: httpToFuseStatus(err)}
	}
	if fh.Unlinked || fh.Path != snapshotPath {
		return appendLogAttemptResult{route: appendLogRouteCommitted, status: gofuse.OK}
	}
	if err := fs.applyPendingModeWithTimeoutLocked(fh); err != nil {
		safeLogPrintf("append-log full-rewrite pending chmod failed for %s: %v", snapshotPath, err)
		return appendLogAttemptResult{route: appendLogRouteFailed, status: httpToFuseStatus(err)}
	}
	fh.BaseRev = revision
	fh.OrigSize = snapshot.Size()
	fh.appendLogObserveLayout(client.ContentLayoutAppendLog, revision, snapshot.Size())
	if fh.DirtySeq == snapshotDirtySeq {
		fh.Dirty.ClearDirty()
		fs.clearDirtySize(fh.Ino, snapshotDirtySeq)
		fh.DirtySeq = 0
		fh.appendLogAdoptCommittedBaseline(revision, snapshot.Size())
		if header, ok := appendLogSnapshotSQLiteWALHeader(snapshot); ok {
			fh.appendLogObserveCommittedSQLiteWALHeader(header)
		}
	} else {
		fh.appendLogRebindLayout(revision, snapshot.Size())
	}
	fs.inodes.UpdateRevision(fh.Ino, revision)
	fs.inodes.UpdateSize(fh.Ino, snapshot.Size())
	fs.refreshCommittedRevisionForOpenHandlesWithSize(snapshotPath, revision, fh, snapshot.Size())
	fs.cacheFileForPath(snapshotPath, snapshot.Size(), time.Now(), revision)
	if snapshot.Size() <= fs.readCache.MaxFileSize() {
		if reader, openErr := snapshot.Open(); openErr == nil {
			if data, readErr := io.ReadAll(reader); readErr == nil {
				fs.readCache.Put(snapshotPath, data, revision)
			}
			_ = reader.Close()
		}
	}
	return appendLogAttemptResult{route: appendLogRouteCommitted, status: gofuse.OK}
}

// routeAppendLogLocked attempts the append-specific transport before any
// generic upload selection. A false handled result leaves legacy routing in
// control only when append-log has no applicable transport (for example an
// existing single-layout file).
func (fs *Dat9FS) routeAppendLogLocked(ctx context.Context, fh *FileHandle) (handled bool, status gofuse.Status, fullRewrite bool) {
	if !fs.appendLogConfiguredLocked(fh) {
		return false, gofuse.OK, false
	}
	if fh.Dirty == nil || (!fh.IsNew && !fh.Dirty.HasDirtyParts()) {
		return false, gofuse.OK, false
	}
	reset := fs.tryAppendLogGenerationResetLocked(ctx, fh)
	if reset.route != appendLogRouteNotApplicable {
		return true, reset.status, true
	}
	capabilityEnabled := fs.client != nil && fs.client.CachedAppendLogSupported()
	if capabilityEnabled && fh.appendLogCanUseTail() {
		result := fs.tryAppendLogLocked(ctx, fh)
		switch result.route {
		case appendLogRouteCommitted, appendLogRouteFailed:
			return true, result.status, false
		case appendLogRouteNeedsRewrite:
			// Continue directly to layout-aware rewrite below.
		}
	}
	if !fh.IsNew && (!capabilityEnabled || !fh.appendLogCanUseTail()) {
		result := fs.tryAppendLogFullRewriteLocked(ctx, fh)
		if result.route != appendLogRouteNotApplicable {
			return true, result.status, true
		}
	}
	return false, gofuse.OK, false
}

// routeAppendLogGenericUnsupportedLocked handles the server's definitive
// append_log_unsupported response from a generic PATCH/V2/direct-upload plan.
// That response proves the target layout and permits one full-PUT reroute; it
// must never retry the rejected generic plan.
func (fs *Dat9FS) routeAppendLogGenericUnsupportedLocked(ctx context.Context, fh *FileHandle, snapshotPath string, snapshotDirtySeq uint64, err error) (handled bool, status gofuse.Status) {
	if appendLogErrorCode(err) != client.AppendLogCodeUnsupported || !fs.appendLogConfiguredLocked(fh) ||
		fh.Unlinked || fh.Path != snapshotPath || fh.DirtySeq != snapshotDirtySeq {
		return false, gofuse.OK
	}
	baselineRevision, baselineSize := fh.appendLogCommittedBaseline()
	fh.appendLogObserveLayout(client.ContentLayoutAppendLog, baselineRevision, baselineSize)
	result := fs.tryAppendLogFullRewriteLocked(ctx, fh)
	if result.route == appendLogRouteNotApplicable {
		return true, gofuse.EIO
	}
	return true, result.status
}

func (fs *Dat9FS) appendLogConfiguredLocked(fh *FileHandle) bool {
	return fh != nil && fs.appendLogPathConfigured(fh.Path) && fh.Layer != PathLayerLocalOnly && fh.Layer != PathLayerGitWorkspace
}

func (fs *Dat9FS) appendLogPathConfigured(path string) bool {
	return fs != nil && !fs.layerEnabled() && fs.appendLogMatcher != nil && fs.appendLogMatcher.Matches(path)
}

func (fs *Dat9FS) appendLogNewPathActive(path string) bool {
	return fs.appendLogPathConfigured(path) && fs.client != nil && fs.client.CachedAppendLogSupported()
}

// tryAppendLogPathTruncate handles a truncate that has no file handle. An
// existing append-log object must still use the server-proxied conditional PUT
// rather than the generic truncate helper, which may select multipart upload.
func (fs *Dat9FS) tryAppendLogPathTruncate(ctx context.Context, entry *InodeEntry, ino uint64, pid uint32, newSize int64, data []byte) (bool, gofuse.Status) {
	if fs == nil || fs.client == nil || entry == nil || entry.Revision <= 0 || !fs.appendLogPathConfigured(entry.Path) {
		return false, gofuse.OK
	}

	unlockRemoteCommit := fs.lockWritableRemoteCommitPath(entry.Path)
	defer unlockRemoteCommit()
	apiPath := fs.remotePath(entry.Path)
	statStart := fs.perfStart()
	stat, err := fs.client.StatCtx(ctx, apiPath)
	fs.perfRecordRemote(perfRemoteStat, statStart, err, 0)
	if err != nil {
		return true, httpToFuseStatus(err)
	}
	if stat == nil || stat.Revision != entry.Revision || stat.Size != entry.Size {
		return true, gofuse.Status(syscall.EEXIST)
	}
	if stat.ContentLayout == client.ContentLayoutSingle {
		return false, gofuse.OK
	}
	if stat.ContentLayout != client.ContentLayoutAppendLog {
		return true, gofuse.EIO
	}

	tempDir, err := fs.appendLogSnapshotDir()
	if err != nil {
		return true, gofuse.EIO
	}
	var snapshot *appendLogSnapshot
	if int64(len(data)) == newSize {
		snapshot, err = newAppendLogSnapshotFromReader(tempDir, newSize, bytes.NewReader(data))
	} else {
		existingSize := entry.Size
		if existingSize > newSize {
			existingSize = newSize
		}
		source := &remoteTruncateReaderAt{
			ctx:          ctx,
			client:       fs.client,
			remotePath:   apiPath,
			existingSize: existingSize,
			totalSize:    newSize,
		}
		snapshot, err = newAppendLogSnapshotFromReaderAt(tempDir, source, 0, newSize)
	}
	if err != nil {
		return true, gofuse.EIO
	}
	defer func() { _ = snapshot.Close() }()

	reader, err := snapshot.Open()
	if err != nil {
		return true, gofuse.EIO
	}
	writeStart := fs.perfStart()
	revision, err := fs.client.WriteServerStreamConditional(ctx, apiPath, reader, snapshot.Size(), entry.Revision)
	fs.perfRecordRemote(perfRemoteWrite, writeStart, err, uint64(snapshot.Size()))
	fs.recordAppendLogFullRewrite(uint64(snapshot.Size()))
	fs.recordAppendLogOutcome(err)
	_ = reader.Close()
	if err != nil {
		if appendLogErrorCode(err) == client.AppendLogCodeTooLarge {
			return true, gofuse.Status(syscall.EFBIG)
		}
		return true, httpToFuseStatus(err)
	}

	fs.recordCommittedRevision(entry.Path, revision)
	entry.Revision = revision
	entry.Size = newSize
	fs.inodes.UpdateRevision(ino, revision)
	fs.inodes.UpdateSize(ino, newSize)
	fs.updateOpenHandleBaseRevision(entry.Path, revision, pid, newSize)
	fs.invalidateReadCacheAndTargets(entry.Path)
	fs.cacheFileForPath(entry.Path, newSize, entry.Mtime, revision)
	return true, gofuse.OK
}
