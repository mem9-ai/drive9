// Package appendlog provides the append-protocol extension: for paths
// matching explicit append-log declarations it owns the write path. Writes
// buffer locally (the appendLogHandleState machine tracks tail safety and
// SQLite WAL headers), and Flush routes through three transports before
// falling back to the chain below: WAL generation reset (a bounded 32-byte
// header PUT), tail append (client.AppendLog with one rebase retry), and
// full rewrite (a conditional whole-image PUT). A generation reset needs a
// salt proof — the dirty header must differ from the proven committed
// header — and every transport learns and gates on the server's content
// layout.
package appendlog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/httperr"
	"github.com/mem9-ai/drive9/pkg/fusecomp/writebuffer"
)

// bufferedMaxTotal caps the local write buffer of a protocol handle.
const bufferedMaxTotal = 10 << 30

// Options configures the extension.
type Options struct {
	// Patterns are explicit append-log declarations; matching is exact
	// pattern match against the canonicalized path.
	Patterns []string
	Client   *client.Client
	// RemoteRoot prefixes engine paths before they reach the client.
	RemoteRoot string
	Events     *fs.EventBus
	// TempDir spills append-log snapshots larger than the 1MiB memory
	// limit; empty uses os.CreateTemp's default.
	TempDir string
	// SyncPolicy mirrors the mount's write policy (v1 write-sync): a
	// synchronous policy must complete the protocol commit before the
	// Write returns, not only touch RAM.
	SyncPolicy string
}

// landedGeneration records the remote landing of one dirty generation so a
// replayed finalize of the same generation adopts it instead of uploading
// a duplicate. pathTruncateSeq marks handle-less path rewrites, which
// share the map but never match a handle generation.
type landedGeneration struct {
	seq  int64
	rev  int64
	size int64
	// fileIncarnation distinguishes recreated files: a NEW handle's local
	// seq restarts at 1, so (path, seq, size) alone matched the deleted
	// previous file's landed generation and skipped committing the new
	// content entirely (unlink + recreate + one write = empty remote).
	// The incarnation counter is per AppendLog instance, minted on every
	// NEW handle claim; a landed generation only dedups a handle of the
	// SAME incarnation.
	fileIncarnation uint64
}

const pathTruncateSeq = math.MinInt64

// AppendLog is the fs.Handler middleware installing protocol handles on
// matching paths.
type AppendLog struct {
	matcher    *AppendLogMatcher
	cl         *client.Client
	root       string
	events     *fs.EventBus
	tempDir    string
	syncPolicy string
	// live indexes open protocol handles by path (SetAttr interception,
	// unlink/rename retargeting).
	live sync.Map
	// unlinked hides paths whose name is gone but whose open handle still
	// holds the image; the deferred remote delete runs at Release.
	unlinked sync.Map
	// facadeDeferred records names whose delete the FACADE already
	// deferred (MarkPathUnlinked): the later flush dispatch must land the
	// remote delete instead of deferring a second time.
	facadeDeferred sync.Map
	// committed remembers the last landed generation per path.
	committed sync.Map
	// incarnation mints a fresh file identity for every NEW handle claim
	// (dedup must never bridge a deleted file and its recreation).
	incarnation atomic.Uint64
}

func New(opts Options) (*AppendLog, error) {
	root := strings.TrimSpace(opts.RemoteRoot)
	if root == "" || root == "/" {
		root = "/"
	} else {
		root = "/" + strings.Trim(root, "/")
	}
	return &AppendLog{
		matcher:    NewAppendLogMatcher(opts.Patterns),
		cl:         opts.Client,
		root:       root,
		events:     opts.Events,
		tempDir:    opts.TempDir,
		syncPolicy: opts.SyncPolicy,
	}, nil
}

// Do claims creates on matching paths (wrapping the chain's handle),
// intercepts size- and mode-changing SetAttr on live protocol handles, and
// defers the remote delete of unlinked-but-open paths to their last close.
func (a *AppendLog) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	if a.cl == nil {
		return next(ctx, op)
	}
	// A DIRECTORY rename does not match the file pattern itself, but it
	// must still retarget the tracked append handles under it: skipping the
	// op left their fsync committing to the old subtree (EAGAIN) while the
	// namespace had already moved. Unlink is likewise handled for tracked
	// handles only (a no-op otherwise).
	if !a.matcher.Matches(op.Path) && op.Kind != fs.OpRename && op.Kind != fs.OpUnlink {
		// A claimed FD keeps its data-plane ownership even after a rename
		// moved the name OUT of the pattern (e.g. `/app.log` →
		// `/app.txt`): a FD-directed setattr must truncate THIS plane's
		// buffer, not fall through to the lower write plane.
		if op.Kind != fs.OpSetAttr || appendLogHandleOf(op.Handle) == nil {
			return next(ctx, op)
		}
	}
	switch op.Kind {
	case fs.OpLookup, fs.OpGetAttr:
		if _, hidden := a.unlinked.Load(op.Path); hidden {
			return fs.Result{}, fs.ErrNotFound
		}
		return next(ctx, op)

	case fs.OpCreate:
		// Resolve the authoritative baseline before claiming: isNew means
		// the file does not exist yet, not just O_CREAT.
		st, statErr := a.cl.StatCtx(ctx, a.remotePath(op.Path))
		isNew := statErr != nil
		if statErr != nil && !client.IsNotFound(statErr) {
			return fs.Result{}, mapErr(statErr)
		}
		res, err := next(ctx, op)
		if err != nil {
			return res, err
		}
		h := &Handle{
			a:     a,
			path:  op.Path,
			inner: res.Handle,
			flags: op.Flags,
			isNew: isNew,
			// A fresh claim on a MISSING path is a new file incarnation:
			// mint an identity the landed-generation dedup cannot bridge
			// across an unlink + recreate of the same name.
			incarnation: a.incarnation.Add(1),
		}
		if !isNew && st != nil {
			h.baseRev, h.origSize = st.Revision, st.Size
			h.observeLayoutLocked(st.ContentLayout, st.Revision, st.Size)
		}
		a.track(h)
		res.Handle = h
		return res, nil

	case fs.OpRead:
		if op.Handle != nil || op.Fh != 0 {
			return next(ctx, op)
		}
		if _, hidden := a.unlinked.Load(op.Path); hidden {
			return fs.Result{}, fs.ErrNotFound
		}
		return next(ctx, op)

	case fs.OpSetAttr:
		h := a.handleFor(op.Path)
		if op.Handle != nil {
			// FATTR_FH names a descriptor: the per-path slot holds only the
			// newest claim, so a truncate of an older fd hit someone else's
			// handle (two opens: h1's size=2 truncate mutated h2 and h1's
			// sync published the untruncated image).
			if ah := appendLogHandleOf(op.Handle); ah != nil {
				h = ah
			}
		}
		if op.HasMode {
			// A chmod on a live handle defers to the next successful
			// commit; handle-less paths pass the mode through the chain.
			if h != nil {
				h.setPendingMode(op.Mode)
				if op.Size < 0 {
					return fs.Result{}, nil
				}
			}
		}
		if op.Size >= 0 {
			// A live handle truncates through the protocol handle so the
			// state machine can fence tail appends; only handle-less paths
			// rewrite the remote object directly, and single-layout files
			// stay on the generic setattr path.
			if h != nil {
				if err := h.Truncate(op.Size); err != nil {
					return fs.Result{}, err
				}
				return fs.Result{}, nil
			}
			handled, err := a.truncatePath(ctx, op.Path, op.Size)
			if !handled {
				return next(ctx, op)
			}
			return fs.Result{}, err
		}
		return next(ctx, op)

	case fs.OpUnlink:
		if h := a.handleFor(op.Path); h != nil {
			if _, deferred := a.facadeDeferred.LoadAndDelete(op.Path); deferred {
				// The facade already deferred this delete (MarkPathUnlinked)
				// and is landing it now, before a same-name create: the
				// name must go NOW. The old fd keeps a local image.
				if err := h.ensureBuffer(ctx); err != nil {
					return fs.Result{}, err
				}
				h.mu.Lock()
				h.unlinked = true
				h.unlinkAtRelease = nil
				h.mu.Unlock()
				a.unlinked.Store(op.Path, struct{}{})
				_, derr := next(ctx, &fs.Op{Kind: fs.OpUnlink, Path: op.Path})
				if derr != nil && !fs.IsNotFound(derr) {
					return fs.Result{}, derr
				}
				return fs.Result{}, nil
			}
			h.mu.Lock()
			already := h.unlinked
			pending := h.unlinkAtRelease
			if already {
				h.unlinkAtRelease = nil
			}
			h.mu.Unlock()
			if already {
				// The facade already deferred this delete and is landing it
				// NOW (before a same-name create). Deferring a second time
				// kept the remote name alive and the recreate hit EEXIST;
				// the old fd keeps its local image either way.
				if pending != nil {
					if err := pending(ctx); err != nil && !fs.IsNotFound(err) {
						h.mu.Lock()
						if h.unlinkAtRelease == nil {
							h.unlinkAtRelease = pending
						}
						h.mu.Unlock()
						return fs.Result{}, err
					}
				}
				return fs.Result{}, nil
			}
			// POSIX unlink-while-open: the name disappears now, the image
			// lives in the open handle, and the remote delete is deferred
			// to the last close.
			if err := h.markUnlinked(ctx, next); err != nil {
				return fs.Result{}, err
			}
			return fs.Result{}, nil
		}
		return next(ctx, op)

	case fs.OpRename:
		res, err := next(ctx, op)
		if err == nil {
			a.retargetHandles(op.Path, op.Path2)
			// The pending delete follows the file (or a renamed
			// directory's subtree) to its new name.
			a.moveUnlinkedPrefix(op.Path, op.Path2)
		}
		return res, err
	}
	return next(ctx, op)
}

// routeFlush orders the transports: WAL generation reset first, then tail
// append, then full rewrite. A flush without dirty bytes still applies a
// deferred chmod.
func (a *AppendLog) routeFlush(ctx context.Context, h *Handle) (bool, error) {
	h.mu.Lock()
	if h.unlinked {
		h.mu.Unlock()
		return false, nil
	}
	if !h.dirty || h.buf == nil {
		hasMode := h.hasPendingMode
		h.mu.Unlock()
		if hasMode {
			return false, h.applyPendingMode(ctx)
		}
		return false, nil
	}
	h.mu.Unlock()

	if err := a.tryGenerationReset(ctx, h); err != nil {
		return false, err
	}
	if a.handleDone(h) {
		return true, nil
	}
	if err := a.tryTailAppend(ctx, h); err != nil {
		return false, err
	}
	if a.handleDone(h) {
		return true, nil
	}
	if err := a.fullRewrite(ctx, h); err != nil {
		return false, err
	}
	return true, nil
}

// handleDone reports a flush as finished (nothing dirty left or the handle
// went away).
func (a *AppendLog) handleDone(h *Handle) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.unlinked || !h.dirty
}

// tryGenerationReset handles the SQLite WAL case: a fully dirty WAL header
// after a rotation fence is published as a fresh 32-byte generation with
// one bounded PUT. The reset requires a salt proof — the dirty header must
// differ from the proven committed header — and an append-log layout.
func (a *AppendLog) tryGenerationReset(ctx context.Context, h *Handle) error {
	h.mu.Lock()
	if !h.dirty || h.isNew || h.st.sqliteWALTruncated || h.st.sqliteWALWriteBeyondHeader ||
		!h.st.sqliteWALHeaderDirtyComplete() || h.buf == nil || h.buf.Size() < sqliteWALHeaderSize {
		h.mu.Unlock()
		return nil
	}
	newHeader, ok := h.readWALHeaderLocked()
	if !ok || !h.st.sqliteWALConfirmed || !h.st.sqliteWALCommittedHeader.saltsDiffer(newHeader) {
		h.mu.Unlock()
		return nil
	}
	expectedRevision, expectedSize := h.committedBaselineLocked()
	if expectedRevision <= 0 || expectedSize < sqliteWALHeaderSize {
		h.mu.Unlock()
		return nil
	}
	layout := h.layoutAtLocked(expectedRevision, expectedSize)
	h.mu.Unlock()

	if layout == "" {
		stat, err := a.cl.StatCtx(ctx, a.remotePath(h.path))
		if err != nil {
			return mapErr(err)
		}
		h.mu.Lock()
		if stat == nil || stat.Revision != expectedRevision || stat.Size != expectedSize {
			h.mu.Unlock()
			return syscall.EEXIST
		}
		h.observeLayoutLocked(stat.ContentLayout, stat.Revision, stat.Size)
		layout = h.layoutAtLocked(expectedRevision, expectedSize)
		h.mu.Unlock()
	}
	if layout == client.ContentLayoutSingle {
		return nil // plain object: no generation to reset
	}
	if layout != client.ContentLayoutAppendLog {
		return syscall.EIO
	}

	h.mu.Lock()
	snapshotSeq := h.seq
	h.mu.Unlock()
	revision, err := a.cl.WriteServerStreamConditional(ctx, a.remotePath(h.path), bytes.NewReader(newHeader.raw[:]), sqliteWALHeaderSize, expectedRevision)
	if err != nil {
		if appendLogErrorCode(err) == client.AppendLogCodeTooLarge {
			return syscall.EFBIG
		}
		return mapErr(err)
	}
	h.mu.Lock()
	h.isNew = false
	h.baseRev = revision
	h.origSize = sqliteWALHeaderSize
	h.observeLayoutLocked(client.ContentLayoutAppendLog, revision, sqliteWALHeaderSize)
	if h.seq == snapshotSeq && !h.unlinked {
		if err := h.buf.Truncate(sqliteWALHeaderSize); err != nil {
			h.mu.Unlock()
			return err
		}
		h.buf.ResetSequentialState(sqliteWALHeaderSize)
		h.buf.ResetStreamingState()
		h.buf.ClearDirty()
		h.dirty = false
		h.adoptCommittedBaselineLocked(revision, sqliteWALHeaderSize)
		h.observeCommittedWALHeaderLocked(newHeader)
	}
	h.mu.Unlock()
	a.committed.Store(h.path, landedGeneration{seq: snapshotSeq, rev: revision, size: sqliteWALHeaderSize, fileIncarnation: h.incarnation})
	if err := h.applyPendingMode(ctx); err != nil {
		return err
	}
	a.emit(fs.Event{Kind: fs.EventLanded, Path: h.path, Rev: revision, Size: sqliteWALHeaderSize, HasSize: true})
	return nil
}

// tryTailAppend appends the [origSize, size) range through client.AppendLog
// with one rebase retry. A rebase observation is only adopted when the
// handle still carries the same dirty generation.
func (a *AppendLog) tryTailAppend(ctx context.Context, h *Handle) error {
	if !a.cl.CachedAppendLogSupported() {
		return nil
	}
	h.mu.Lock()
	if h.unlinked || !h.dirty || h.buf == nil || !h.canUseTail() {
		h.mu.Unlock()
		return nil
	}
	size := h.buf.Size()
	expectedRevision, expectedSize := h.baseRev, h.origSize
	start := expectedSize
	if h.isNew {
		expectedRevision, expectedSize, start = 0, 0, 0
	} else if size < start || expectedRevision < 0 || expectedSize < 0 {
		h.mu.Unlock()
		return nil
	}
	snapshotSeq := h.seq
	snapshotPath := h.path
	snapshotIsNew := h.isNew
	snapshotBaseRev, snapshotOrigSize := h.baseRev, h.origSize
	// Same-generation finalize dedup: the identical dirty generation
	// already landed; adopt it instead of uploading a duplicate.
	if prev, ok := a.committed.Load(h.path); ok {
		if g := prev.(landedGeneration); g.seq == snapshotSeq && g.rev > 0 && g.size == size && g.fileIncarnation == h.incarnation {
			h.isNew = false
			h.baseRev = g.rev
			h.origSize = g.size
			h.observeLayoutLocked(client.ContentLayoutAppendLog, g.rev, g.size)
			h.buf.ClearDirty()
			h.dirty = false
			h.adoptCommittedBaselineLocked(g.rev, g.size)
			h.mu.Unlock()
			return nil
		}
	}
	snapshot, snapErr := newAppendLogSnapshotFromReaderAt(a.tempDir, readerAtFunc(func(off int64, p []byte) (int, error) {
		n := h.buf.ReadAt(off, p)
		return n, nil
	}), start, size-start)
	h.mu.Unlock()
	if snapErr != nil {
		return snapErr
	}
	defer func() { _ = snapshot.Close() }()

	appendOnce := func(revision int64) (client.AppendLogResult, error) {
		tail, oerr := snapshot.Open()
		if oerr != nil {
			return client.AppendLogResult{}, oerr
		}
		defer func() { _ = tail.Close() }()
		return a.cl.AppendLog(ctx, a.remotePath(h.path), tail, snapshot.Size(), revision, expectedSize)
	}
	result, err := appendOnce(expectedRevision)
	if appendLogErrorCode(err) == client.AppendLogCodeRebased {
		stat, statErr := a.cl.StatCtx(ctx, a.remotePath(h.path))
		switch {
		case statErr != nil:
			err = statErr
		case stat == nil || stat.Revision <= 0 || stat.Revision == expectedRevision || stat.Size != expectedSize:
			// A rebase only helps a PURE append: when the sibling image
			// diverged in size the tail contract cannot hold, so the
			// handle must fail and re-read rather than compose a guess
			// over someone else's bytes (EEXIST-on-drift is pinned by
			// engine_port_test.go).
			err = fmt.Errorf("append-log rebase did not provide the same size with a new positive revision")
		default:
			// The rebase observation belongs to the immutable snapshot. A
			// concurrent write may have advanced the dirty generation
			// while StatCtx ran — only a same-generation handle adopts it.
			h.mu.Lock()
			if h.unlinked || h.path != snapshotPath || h.seq != snapshotSeq ||
				h.isNew != snapshotIsNew || h.baseRev != snapshotBaseRev || h.origSize != snapshotOrigSize {
				h.mu.Unlock()
				return syscall.EAGAIN
			}
			h.st.rewriteBaseRevision = stat.Revision
			h.st.rewriteBaseSize = stat.Size
			h.st.hasRewriteBase = true
			h.observeLayoutLocked(stat.ContentLayout, stat.Revision, stat.Size)
			h.mu.Unlock()
			result, err = appendOnce(stat.Revision)
		}
	}
	if err != nil {
		if appendLogErrorCode(err) == client.AppendLogCodeUnsupported {
			h.mu.Lock()
			h.st.unsupported = true
			h.mu.Unlock()
			return nil // fall through to full rewrite
		}
		if appendLogErrorCode(err) == client.AppendLogCodeTooLarge {
			return syscall.EFBIG
		}
		return mapErr(err)
	}
	h.mu.Lock()
	if h.unlinked || h.path != snapshotPath {
		// The remote landing is recorded; local publication belongs to
		// whichever generation still owns the handle.
		h.mu.Unlock()
		a.committed.Store(snapshotPath, landedGeneration{seq: snapshotSeq, rev: result.Revision, size: result.Size, fileIncarnation: h.incarnation})
		return nil
	}
	h.isNew = false
	h.baseRev = result.Revision
	h.origSize = result.Size
	h.observeLayoutLocked(client.ContentLayoutAppendLog, result.Revision, result.Size)
	if h.seq == snapshotSeq {
		h.buf.ClearDirty()
		h.dirty = false
		h.adoptCommittedBaselineLocked(result.Revision, result.Size)
		if snapshotIsNew {
			if header, ok := appendLogSnapshotSQLiteWALHeader(snapshot); ok {
				h.observeCommittedWALHeaderLocked(header)
			}
		}
	}
	h.mu.Unlock()
	a.committed.Store(h.path, landedGeneration{seq: snapshotSeq, rev: result.Revision, size: result.Size})
	if err := h.applyPendingMode(ctx); err != nil {
		return err
	}
	a.refreshCleanSiblings(h.path, result.Revision, result.Size, h)
	a.emit(fs.Event{Kind: fs.EventLanded, Path: h.path, Rev: result.Revision, Size: result.Size, HasSize: true})
	return nil
}

// fullRewrite commits the whole buffered image through a conditional PUT
// with revision-conflict retries. Append-log layouts stream in one server
// stream; single-layout files use the generic multipart-capable commit.
func (a *AppendLog) fullRewrite(ctx context.Context, h *Handle) error {
	h.mu.Lock()
	if h.unlinked || !h.dirty || h.buf == nil {
		h.mu.Unlock()
		return nil
	}
	expectedRevision, expectedSize := h.committedBaselineLocked()
	isNew := h.isNew
	if !h.isNew && (expectedRevision <= 0 || expectedSize < 0) {
		h.mu.Unlock()
		return syscall.EIO
	}
	layout := h.layoutAtLocked(expectedRevision, expectedSize)
	h.mu.Unlock()

	if layout == "" {
		stat, err := a.cl.StatCtx(ctx, a.remotePath(h.path))
		switch {
		case err != nil && client.IsNotFound(err) && isNew:
			// v1: a never-landed create has no remote image to reconcile —
			// the generic commit creates it (expected revision 0), instead
			// of failing the flush with ENOENT on servers without
			// append-log support.
			layout = client.ContentLayoutSingle
			expectedRevision = 0
		case err != nil:
			return mapErr(err)
		default:
			// The remote drifted from this handle's captured baseline: a
			// sibling landing (or a rename retarget) changed the image
			// under us, so the buffered image is not a composition over
			// the current remote. Fail with EEXIST and let the caller
			// re-read; adopting the sibling revision here would publish
			// this handle's stale bytes over its tail.
			h.mu.Lock()
			if stat == nil || stat.Revision != expectedRevision || stat.Size != expectedSize {
				h.mu.Unlock()
				return syscall.EEXIST
			}
			h.observeLayoutLocked(stat.ContentLayout, stat.Revision, stat.Size)
			layout = h.layoutAtLocked(expectedRevision, expectedSize)
			h.mu.Unlock()
		}
	}
	if layout != "" && layout != client.ContentLayoutAppendLog && layout != client.ContentLayoutSingle {
		return syscall.EIO
	}

	h.mu.Lock()
	snapshotSeq := h.seq
	snapshotIsNew := h.isNew
	// The whole image becomes an immutable snapshot (memory below 1MiB,
	// temp file above); untouched base parts load first so the image is
	// complete.
	if err := h.buf.LoadMissingParts(); err != nil {
		h.mu.Unlock()
		return err
	}
	size := h.buf.Size()
	snapshot, snapErr := newAppendLogSnapshotFromReaderAt(a.tempDir, readerAtFunc(func(off int64, p []byte) (int, error) {
		n := h.buf.ReadAt(off, p)
		return n, nil
	}), 0, size)
	h.mu.Unlock()
	if snapErr != nil {
		return snapErr
	}
	defer func() { _ = snapshot.Close() }()

	var revision int64
	var err error
	if layout == client.ContentLayoutSingle {
		revision, err = a.commitGeneric(ctx, h.path, snapshot, expectedRevision)
	} else {
		revision, err = a.commitWholeSnapshot(ctx, h.path, snapshot, expectedRevision)
	}
	if err != nil {
		return err
	}
	h.mu.Lock()
	h.isNew = false
	h.baseRev = revision
	h.origSize = size
	h.observeLayoutLocked(client.ContentLayoutAppendLog, revision, size)
	if h.seq == snapshotSeq && !h.unlinked {
		h.buf.ClearDirty()
		h.dirty = false
		h.adoptCommittedBaselineLocked(revision, size)
		h.st.appendSafe = true
		h.st.initialized = true
		if snapshotIsNew {
			if header, ok := appendLogSnapshotSQLiteWALHeader(snapshot); ok {
				h.observeCommittedWALHeaderLocked(header)
			}
		}
	}
	h.mu.Unlock()
	a.committed.Store(h.path, landedGeneration{seq: snapshotSeq, rev: revision, size: size})
	if err := h.applyPendingMode(ctx); err != nil {
		return err
	}
	a.refreshCleanSiblings(h.path, revision, size, h)
	a.emit(fs.Event{Kind: fs.EventLanded, Path: h.path, Rev: revision, Size: size, HasSize: true})
	return nil
}

// commitGeneric conditionally commits through the size-aware client path
// (inline PUT below the threshold, multipart above) against the captured
// base revision.
func (a *AppendLog) commitGeneric(ctx context.Context, path string, snapshot *appendLogSnapshot, expectedRevision int64) (int64, error) {
	rp := a.remotePath(path)
	// The conditional write carries the handle's CAPTURED base revision: a
	// foreign landing since must CONFLICT here. Refreshing the base from a
	// stat and retrying the SAME snapshot published a stale image over the
	// foreign tail (abcdef + local X + foreign TAIL became Xbcdef), so a
	// conflict is EAGAIN — the caller re-reads and re-materializes.
	body, oerr := snapshot.Open()
	if oerr != nil {
		return 0, oerr
	}
	st, werr := a.cl.WriteStreamConditionalWithChecksum(ctx, rp, body, snapshot.Size(), nil, expectedRevision, "")
	_ = body.Close()
	if werr != nil {
		if errors.Is(werr, client.ErrConflict) {
			return 0, &fs.StatusError{Path: path, Status: gofuse.EAGAIN}
		}
		return 0, mapErr(werr)
	}
	rev := int64(0)
	if st != nil {
		rev = st.Revision
	}
	return rev, nil
}

// commitWholeSnapshot streams the whole image against the captured base;
// small snapshots reuse the in-memory commit path.
func (a *AppendLog) commitWholeSnapshot(ctx context.Context, path string, snapshot *appendLogSnapshot, expectedRevision int64) (int64, error) {
	if snapshot.Size() <= appendLogSnapshotMemoryLimit {
		rc, err := snapshot.Open()
		if err != nil {
			return 0, err
		}
		defer func() { _ = rc.Close() }()
		data, err := io.ReadAll(rc)
		if err != nil {
			return 0, err
		}
		return a.commitWhole(ctx, path, data, expectedRevision)
	}
	rp := a.remotePath(path)
	// Same contract as commitGeneric: the captured base is the CAS
	// condition, and a conflict fails with EAGAIN instead of republishing
	// the stale snapshot at a refreshed revision.
	body, oerr := snapshot.Open()
	if oerr != nil {
		return 0, oerr
	}
	rev, werr := a.cl.WriteServerStreamConditional(ctx, rp, body, snapshot.Size(), expectedRevision)
	_ = body.Close()
	if werr != nil {
		if errors.Is(werr, client.ErrConflict) {
			return 0, &fs.StatusError{Path: path, Status: gofuse.EAGAIN}
		}
		return 0, mapErr(werr)
	}
	return rev, nil
}

// commitWhole conditionally PUTs an in-memory image against the captured
// base revision. A conflict is EAGAIN (a sibling landing changed the
// image; republishing the stale composition would drop its bytes), and a
// vanished path is NOT recreated — v1's CAS rejected there, and
// resurrecting a deleted name from a stale writer's buffer loses the
// delete.
func (a *AppendLog) commitWhole(ctx context.Context, path string, image []byte, expectedRevision int64) (int64, error) {
	rp := a.remotePath(path)
	rev, werr := a.cl.WriteCtxConditionalWithRevision(ctx, rp, image, expectedRevision)
	if werr != nil {
		if errors.Is(werr, client.ErrConflict) {
			return 0, &fs.StatusError{Path: path, Status: gofuse.EAGAIN}
		}
		if client.IsNotFound(werr) {
			return 0, fs.ErrNotFound
		}
		return 0, mapErr(werr)
	}
	return rev, nil
}

func (a *AppendLog) remotePath(p string) string {
	if a.root == "/" {
		return p
	}
	if p == "/" {
		return a.root
	}
	return a.root + p
}

func (a *AppendLog) emit(e fs.Event) {
	if a.events != nil {
		a.events.Emit(e)
	}
}

// MarkPathUnlinked implements the facade's deferred-unlink handshake: the
// facade has ALREADY postponed the remote delete (the name is gone from its
// view), so the later flush dispatch performs it instead of deferring a
// second time.
func (a *AppendLog) MarkPathUnlinked(p string) {
	if a.cl == nil {
		return
	}
	a.facadeDeferred.Store(p, struct{}{})
}

// refreshCleanSiblings adopts a committed generation in every CLEAN sibling
// of the same path: their stale revision/size would CAS the next write on
// the pre-commit revision (EAGAIN), and a loaded buffer would serve (and
// re-cache) the pre-commit image under the new revision. Dirty siblings
// keep their own CAS base (their composition still owns the tail).
func (a *AppendLog) refreshCleanSiblings(path string, rev, size int64, self *Handle) {
	v, ok := a.live.Load(path)
	if !ok {
		return
	}
	set := v.(*liveHandleSet)
	set.mu.Lock()
	candidates := make([]*Handle, 0, len(set.set))
	for other := range set.set {
		candidates = append(candidates, other)
	}
	set.mu.Unlock()
	for _, other := range candidates {
		if other == self {
			continue
		}
		other.mu.Lock()
		if !other.dirty && !other.unlinked {
			other.isNew = false
			other.baseRev = rev
			other.origSize = size
			other.buf = nil // re-read lazily from the committed image
			other.observeLayoutLocked(client.ContentLayoutAppendLog, rev, size)
		}
		other.mu.Unlock()
	}
}

// track/untrack maintain the live-handle index.
// liveHandleSet is one path's open protocol handles. The table supports
// MULTIPLE concurrent handles of one path (v1 openHandles): the current
// head serves SetAttr interception, and every dirty member is a
// read-your-writes / append-baseline source for the others.
type liveHandleSet struct {
	mu  sync.Mutex
	cur *Handle
	set map[*Handle]struct{}
}

func (s *liveHandleSet) add(h *Handle) {
	s.mu.Lock()
	if s.set == nil {
		s.set = map[*Handle]struct{}{}
	}
	s.set[h] = struct{}{}
	s.cur = h
	s.mu.Unlock()
}

func (s *liveHandleSet) remove(h *Handle) {
	s.mu.Lock()
	delete(s.set, h)
	if s.cur == h {
		s.cur = nil
		for other := range s.set {
			s.cur = other
			break
		}
	}
	s.mu.Unlock()
}

// head returns the most recently tracked handle (SetAttr interception).
func (s *liveHandleSet) head() *Handle {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cur
}

// forEach visits every live handle of the path.
func (s *liveHandleSet) forEach(fn func(*Handle) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for h := range s.set {
		if !fn(h) {
			return
		}
	}
}

func (a *AppendLog) liveSet(path string) *liveHandleSet {
	v, _ := a.live.LoadOrStore(path, &liveHandleSet{})
	return v.(*liveHandleSet)
}

func (a *AppendLog) track(h *Handle) { a.liveSet(h.path).add(h) }

func (a *AppendLog) untrack(h *Handle) {
	if v, ok := a.live.Load(h.path); ok {
		v.(*liveHandleSet).remove(h)
	}
}

func (a *AppendLog) handleFor(path string) *Handle {
	if v, ok := a.live.Load(path); ok {
		return v.(*liveHandleSet).head()
	}
	return nil
}

// truncatePath rewrites the remote object of a handle-less path. It
// returns handled=false for single-layout files so the generic setattr
// path takes over; only the surviving prefix is fetched.
func (a *AppendLog) truncatePath(ctx context.Context, path string, size int64) (bool, error) {
	rp := a.remotePath(path)
	st, err := a.cl.StatCtx(ctx, rp)
	if err != nil {
		return true, mapErr(err)
	}
	if st.ContentLayout == client.ContentLayoutSingle {
		return false, nil
	}
	if st.ContentLayout != client.ContentLayoutAppendLog {
		return true, syscall.EIO
	}
	existing := st.Size
	if existing > size {
		existing = size
	}
	var src io.Reader = emptyReader{}
	if existing > 0 {
		rc, rerr := a.cl.ReadStreamRange(ctx, rp, 0, existing)
		if rerr != nil {
			return true, mapErr(rerr)
		}
		defer func() { _ = rc.Close() }()
		src = rc
	}
	snapshot, snapErr := newAppendLogSnapshotFromReader(a.tempDir, size, &truncatePadReader{src: src, existing: existing, total: size})
	if snapErr != nil {
		return true, snapErr
	}
	defer func() { _ = snapshot.Close() }()
	body, oerr := snapshot.Open()
	if oerr != nil {
		return true, oerr
	}
	revision, werr := a.cl.WriteServerStreamConditional(ctx, rp, body, snapshot.Size(), st.Revision)
	_ = body.Close()
	if werr != nil {
		if appendLogErrorCode(werr) == client.AppendLogCodeTooLarge {
			return true, syscall.EFBIG
		}
		return true, mapErr(werr)
	}
	a.committed.Store(path, landedGeneration{seq: pathTruncateSeq, rev: revision, size: size})
	a.emit(fs.Event{Kind: fs.EventLanded, Path: path, Rev: revision, Size: size, HasSize: true})
	return true, nil
}

// truncatePadReader serves the surviving remote prefix then zero-fills the
// growth of a path truncate.
type truncatePadReader struct {
	src      io.Reader
	existing int64
	total    int64
	read     int64
}

func (r *truncatePadReader) Read(p []byte) (int, error) {
	if int64(len(p))+r.read > r.total {
		p = p[:max(0, r.total-r.read)]
	}
	if len(p) == 0 {
		return 0, io.EOF
	}
	if r.read < r.existing {
		n, err := r.src.Read(p)
		r.read += int64(n)
		if err == io.EOF {
			return n, nil
		}
		return n, err
	}
	n := len(p)
	for i := range p {
		p[i] = 0
	}
	r.read += int64(n)
	return n, nil
}

type emptyReader struct{}

func (emptyReader) Read([]byte) (int, error) { return 0, io.EOF }

// Handle is the append-protocol handle: local buffering with the state
// machine, three-transport Flush.
type Handle struct {
	a     *AppendLog
	path  string
	inner fs.Handle
	flags uint32

	mu             sync.Mutex
	buf            *writebuffer.WriteBuffer
	st             appendLogHandleState
	dirty          bool
	isNew          bool
	baseRev        int64
	origSize       int64
	seq            int64
	incarnation    uint64
	unlinked       bool
	pendingMode    uint32
	hasPendingMode bool
	// unlinkAtRelease performs the deferred remote delete of an
	// unlinked-but-open path at the last close.
	unlinkAtRelease func(ctx context.Context) error
}

func (h *Handle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.buf == nil {
		// Read-your-writes across fds (v1 readActiveAppendVisibleRange /
		// readSamePathDirtyHandle): another live handle's unflushed buffer
		// is visible to this fd — serve it instead of the stale remote.
		if n, ok := h.a.readLiveAppendBuffer(ctx, h, off, dest); ok {
			return n, nil
		}
		if h.inner != nil {
			return h.inner.Read(ctx, off, dest)
		}
		return 0, nil
	}
	// Load every part the read touches before serving: ReadAt zero-fills
	// unloaded parts, which is only correct beyond the remote base.
	if err := h.buf.EnsurePartsForWrite(off, int64(len(dest))); err != nil {
		return 0, err
	}
	for i := range dest {
		dest[i] = 0
	}
	n := h.buf.ReadAt(off, dest)
	if end := h.buf.Size() - off; end < int64(len(dest)) {
		if end < 0 {
			end = 0
		}
		n = int(end)
	}
	return n, nil
}

func (h *Handle) Write(ctx context.Context, off int64, data []byte) error {
	h.mu.Lock()
	if err := h.ensureBuffer(ctx); err != nil {
		h.mu.Unlock()
		return err
	}
	// O_APPEND: the kernel may deliver off=0; the write lands at the
	// buffer tail. First re-derive that tail from the newest same-path
	// content (v1 refreshAppendBufferLocked, issue #935): a concurrent
	// appender's unflushed bytes must not be overwritten — the flush-time
	// rebase check would reject the whole image instead.
	if h.flags&uint32(syscall.O_APPEND) != 0 && off == 0 {
		if err := h.refreshAppendBaselineLocked(ctx); err != nil {
			h.mu.Unlock()
			return err
		}
		off = h.buf.Size()
	}
	// Capture the committed WAL header before a write can overwrite it;
	// the proof is the salt baseline a generation reset must differ from.
	if !h.isNew && !h.st.sqliteWALConfirmed && len(data) > 0 &&
		off < sqliteWALHeaderSize && off+int64(len(data)) > 0 {
		if header, ok := h.readWALHeaderLocked(); ok {
			h.st.sqliteWALConfirmed = true
			h.st.sqliteWALCommittedHeader = header
		}
	}
	pre := h.buf.Size()
	if _, err := h.buf.Write(off, data); err != nil {
		h.mu.Unlock()
		return err
	}
	h.recordUserWrite(pre, off, int64(len(data)))
	h.seq++
	h.dirty = true
	syncIntent := h.flags&uint32(syscall.O_SYNC) != 0 ||
		h.flags&uint32(syscall.O_DSYNC) != 0 ||
		h.a.syncPolicy == "write-sync" ||
		h.a.syncPolicy == "strict"
	h.mu.Unlock()
	if syncIntent {
		// A synchronous write acknowledges only after the protocol commit
		// (generation/tail/rewrite upload) completes — RAM-only
		// acknowledgment violated --durability=write-sync and O_SYNC.
		published, err := h.a.routeFlush(ctx, h)
		if err != nil {
			return err
		}
		// The write ALREADY published the payload: hand publication
		// ownership over to this plane. Otherwise the forwarded Flush saw a
		// clean append handle, still considered the lower create pending,
		// and materialized its empty image over the upload (O_SYNC write
		// followed by close wiped the file).
		if published && h.inner != nil {
			if mp, ok := unwrapMarkPublished(h.inner); ok {
				mp.MarkPublished()
			}
		}
	}
	return nil
}

func (h *Handle) Sync(ctx context.Context) error {
	if err := h.Flush(ctx); err != nil {
		return err
	}
	if h.inner != nil {
		return h.inner.Sync(ctx)
	}
	return nil
}

func (h *Handle) Flush(ctx context.Context) error {
	published, err := h.a.routeFlush(ctx, h)
	if err != nil {
		return err
	}
	if h.inner != nil {
		// The append plane owns this path's publication once its protocol
		// commit uploaded the payload: retire the lower plane's pending
		// creation first, or the forwarded Flush/Sync/Release materializes
		// the lower handle's empty image over the upload. A no-op flush
		// (nothing dirty, e.g. a brand-new empty file) published NOTHING:
		// retiring the lower create there made the empty file vanish.
		if published {
			if mp, ok := unwrapMarkPublished(h.inner); ok {
				mp.MarkPublished()
			}
		}
		return h.inner.Flush(ctx)
	}
	return nil
}

// readLiveAppendBuffer serves one read from any OTHER live handle's dirty
// buffer of the same path (v1 read-your-writes across fds). ok=false lets
// the caller fall through to the inner (remote) tier.
func (a *AppendLog) readLiveAppendBuffer(ctx context.Context, reader *Handle, off int64, dest []byte) (int, bool) {
	v, ok := a.live.Load(reader.path)
	if !ok {
		return 0, false
	}
	set := v.(*liveHandleSet)
	// Snapshot the candidates under the SET mutex, then inspect each
	// handle's dirty/buffer state under ITS OWN mutex: the old forEach
	// body read h.dirty/h.buf while another fd's Write mutated them (a
	// targeted -race probe flagged it), and locking only after selection
	// both raced and could miss the newest local image. TryLock keeps the
	// caller's h.mu from deadlocking against a sibling mid-refresh.
	var candidates []*Handle
	set.mu.Lock()
	for h := range set.set {
		if h != reader {
			candidates = append(candidates, h)
		}
	}
	set.mu.Unlock()
	var served *Handle
	for _, h := range candidates {
		if !h.mu.TryLock() {
			continue
		}
		ready := h.dirty && h.buf != nil
		h.mu.Unlock()
		if ready {
			served = h
			break
		}
	}
	if served == nil {
		return 0, false
	}
	served.mu.Lock()
	defer served.mu.Unlock()
	if !served.dirty || served.buf == nil {
		return 0, false
	}
	if err := served.buf.EnsurePartsForWrite(off, int64(len(dest))); err != nil {
		return 0, false
	}
	n := served.buf.ReadAt(off, dest)
	if end := served.buf.Size() - off; end < int64(len(dest)) {
		if end < 0 {
			end = 0
		}
		n = int(end)
	}
	return n, true
}

// refreshAppendBaselineLocked extends this handle's buffer tail with
// another live handle's unflushed appends (v1 refreshAppendBufferLocked's
// local source). Only extension is safe: the caller's own unflushed bytes
// stay authoritative for the range they cover. Caller holds h.mu.
func (h *Handle) refreshAppendBaselineLocked(ctx context.Context) error {
	if h.buf == nil || h.isNew {
		return nil
	}
	v, ok := h.a.live.Load(h.path)
	if !ok {
		return nil
	}
	set := v.(*liveHandleSet)
	var newest *Handle
	newestSize := h.buf.Size()
	// NON-BLOCKING sibling admission (v1's shape): the caller holds h.mu,
	// and every sibling refresh does too — blocking on another handle's
	// mutex here deadlocks two O_APPEND writers head-on (A holds h.mu and
	// waits for B.mu inside the set mutex; B holds B.mu and waits for the
	// set mutex). A busy sibling simply sits this refresh out; the flush
	// rebase retry covers the missed bytes.
	set.mu.Lock()
	candidates := make([]*Handle, 0, len(set.set))
	for other := range set.set {
		candidates = append(candidates, other)
	}
	set.mu.Unlock()
	for _, other := range candidates {
		if other == h {
			continue
		}
		if !other.mu.TryLock() {
			continue
		}
		size := int64(-1)
		dirty := other.dirty
		if other.buf != nil {
			size = other.buf.Size()
		}
		other.mu.Unlock()
		if dirty && size > newestSize {
			newest, newestSize = other, size
		}
	}
	if newest == nil {
		return nil
	}
	ourSize := h.buf.Size()
	tail := make([]byte, newestSize-ourSize)
	if !newest.mu.TryLock() {
		return nil // raced with the sibling's own refresh; retry at flush
	}
	defer newest.mu.Unlock()
	if !newest.dirty || newest.buf == nil || newest.buf.Size() < newestSize {
		return nil // raced with a flush; the rebase retry at flush covers it
	}
	if err := newest.buf.EnsurePartsForWrite(ourSize, int64(len(tail))); err != nil {
		return err
	}
	n := newest.buf.ReadAt(ourSize, tail)
	if n <= 0 {
		return nil
	}
	if _, err := h.buf.Write(ourSize, tail[:n]); err != nil {
		return err
	}
	return nil
}

func (h *Handle) Release(ctx context.Context) error {
	h.a.untrack(h)
	h.mu.Lock()
	unlinked := h.unlinked
	unlink := h.unlinkAtRelease
	path := h.path
	h.mu.Unlock()
	var err error
	if unlinked {
		// Last close of an unlinked path: publish the deferred delete and
		// unhide any successor created at the same name meanwhile.
		if unlink != nil {
			err = unlink(ctx)
		}
		h.a.unlinked.Delete(path)
	} else {
		err = h.Flush(ctx)
	}
	if h.inner != nil {
		if relErr := h.inner.Release(ctx); err == nil {
			err = relErr
		}
	}
	return err
}

// Truncate applies a local truncation through the state machine (used by
// SetAttr interception on live paths).
func (h *Handle) Truncate(size int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.ensureBuffer(context.Background()); err != nil {
		return err
	}
	if err := h.buf.Truncate(size); err != nil {
		return err
	}
	h.recordTruncate()
	h.seq++
	h.dirty = true
	return nil
}

// setPendingMode defers a chmod to the next successful commit.
func (h *Handle) setPendingMode(mode uint32) {
	h.mu.Lock()
	h.pendingMode = mode & 0o777
	h.hasPendingMode = true
	h.mu.Unlock()
}

// applyPendingMode publishes a deferred chmod once the image it describes
// has landed.
func (h *Handle) applyPendingMode(ctx context.Context) error {
	h.mu.Lock()
	mode, has := h.pendingMode, h.hasPendingMode
	h.mu.Unlock()
	if !has {
		return nil
	}
	if err := h.a.cl.ChmodCtx(ctx, h.a.remotePath(h.path), mode); err != nil {
		return mapErr(err)
	}
	h.mu.Lock()
	h.hasPendingMode = false
	h.mu.Unlock()
	return nil
}

// appendLogHandleOf walks decorator wrappers (UnwrapHandle) to the append
// plane's own handle; nil when the chain holds none.
func appendLogHandleOf(h any) *Handle {
	for range 8 {
		if ah, ok := h.(*Handle); ok {
			return ah
		}
		u, ok := h.(interface{ UnwrapHandle() fs.Handle })
		if !ok {
			return nil
		}
		h = u.UnwrapHandle()
	}
	return nil
}

// unwrapMarkPublished finds the lower plane's MarkPublished capability
// through decorator wrappers.
func unwrapMarkPublished(h any) (interface{ MarkPublished() }, bool) {
	for range 8 {
		if mp, ok := h.(interface{ MarkPublished() }); ok {
			return mp, true
		}
		u, ok := h.(interface{ UnwrapHandle() fs.Handle })
		if !ok {
			return nil, false
		}
		h = u.UnwrapHandle()
	}
	return nil, false
}

// retargetHandles moves EVERY open descriptor of the renamed object to its
// new name (and a renamed directory's descendants under the new prefix):
// the per-path slot only holds the newest claim, so an older dirty fd's
// commit republished the old name after the rename.
func (a *AppendLog) retargetHandles(oldPath, newPath string) {
	var handles []*Handle
	a.live.Range(func(key, v any) bool {
		p, ok := key.(string)
		if !ok {
			return true
		}
		set, ok := v.(*liveHandleSet)
		if !ok {
			return true
		}
		if p != oldPath && !strings.HasPrefix(p, strings.TrimSuffix(oldPath, "/")+"/") {
			return true
		}
		set.forEach(func(h *Handle) bool {
			handles = append(handles, h)
			return true
		})
		return true
	})
	for _, h := range handles {
		h.mu.Lock()
		p := h.path
		h.mu.Unlock()
		var target string
		switch {
		case p == oldPath:
			target = newPath
		case strings.HasPrefix(p, strings.TrimSuffix(oldPath, "/")+"/"):
			target = strings.TrimSuffix(newPath, "/") + "/" + strings.TrimPrefix(p, strings.TrimSuffix(oldPath, "/")+"/")
		default:
			continue
		}
		h.retarget(target)
	}
}

// moveUnlinkedPrefix moves the pending-delete marks of a renamed path (or
// directory subtree) to the new name.
func (a *AppendLog) moveUnlinkedPrefix(oldPath, newPath string) {
	prefix := strings.TrimSuffix(oldPath, "/") + "/"
	newPrefix := strings.TrimSuffix(newPath, "/") + "/"
	var moves [][2]string
	a.unlinked.Range(func(key, _ any) bool {
		p, ok := key.(string)
		if !ok {
			return true
		}
		switch {
		case p == oldPath:
			moves = append(moves, [2]string{p, newPath})
		case strings.HasPrefix(p, prefix):
			moves = append(moves, [2]string{p, newPrefix + strings.TrimPrefix(p, prefix)})
		}
		return true
	})
	for _, m := range moves {
		a.unlinked.Delete(m[0])
		a.unlinked.Store(m[1], struct{}{})
	}
}

// markUnlinked hides the path and keeps the image in the open handle; the
// remote delete is deferred to Release. A clean handle preloads its base
// first so post-unlink reads keep serving the image.
func (h *Handle) markUnlinked(ctx context.Context, next fs.Next) error {
	h.mu.Lock()
	if h.unlinked {
		h.mu.Unlock()
		return nil
	}
	if h.buf == nil && !h.isNew {
		if err := h.ensureBuffer(ctx); err != nil {
			h.mu.Unlock()
			return err
		}
	}
	h.unlinked = true
	path := h.path
	h.unlinkAtRelease = func(ctx context.Context) error {
		_, err := next(ctx, &fs.Op{Kind: fs.OpUnlink, Path: path})
		return err
	}
	h.mu.Unlock()
	h.a.unlinked.Store(path, struct{}{})
	return nil
}

// retarget moves the handle to a renamed path, reindexing the live table.
func (h *Handle) retarget(newPath string) {
	h.mu.Lock()
	old := h.path
	if old == newPath {
		h.mu.Unlock()
		return
	}
	// Untrack while h.path still names the old path: the live set is
	// path-keyed, and re-keying first left the released handle indexed
	// under the old name forever — a later recreate+unlink then deferred
	// the deletion to a Release that never came.
	h.a.untrack(h)
	h.path = newPath
	h.mu.Unlock()
	h.a.track(h)
}

// recordUserWrite updates tail-safety and the WAL header coverage mask
// from one user write.
func (h *Handle) recordUserWrite(preWriteSize, offset, written int64) {
	if h.isNew || written <= 0 {
		return
	}
	if !h.st.initialized {
		h.st.initialized = true
		h.st.appendSafe = true
	}
	if offset != preWriteSize {
		h.st.appendSafe = false
	}
	h.st.recordSQLiteWALHeaderWrite(offset, written)
}

// recordTruncate fences tail appends and the WAL reset for the current
// generation after a truncate.
func (h *Handle) recordTruncate() {
	if h.isNew {
		return
	}
	if !h.st.hasRewriteBase && h.baseRev > 0 && h.origSize >= 0 {
		h.st.rewriteBaseRevision = h.baseRev
		h.st.rewriteBaseSize = h.origSize
		h.st.hasRewriteBase = true
	}
	h.st.initialized = true
	h.st.appendSafe = false
	h.st.sqliteWALTruncated = true
	h.st.clearSQLiteWALHeaderWrite()
}

// canUseTail reports whether the tail-append transport may run: never for
// an unsupported server, always for new files, else only when every write
// so far landed at the committed tail.
func (h *Handle) canUseTail() bool {
	if h.st.unsupported {
		return false
	}
	if h.isNew {
		return true
	}
	return h.st.initialized && h.st.appendSafe
}

// committedBaselineLocked is the revision/size a rewrite compares against:
// a rebase-learned base when present, else the adopted baseline.
func (h *Handle) committedBaselineLocked() (int64, int64) {
	if h.st.hasRewriteBase {
		return h.st.rewriteBaseRevision, h.st.rewriteBaseSize
	}
	return h.baseRev, h.origSize
}

// adoptCommittedBaselineLocked resets the state machine onto a fresh
// committed baseline.
func (h *Handle) adoptCommittedBaselineLocked(revision, size int64) {
	h.st.initialized = true
	h.st.appendSafe = true
	h.st.hasRewriteBase = false
	h.st.rewriteBaseRevision = 0
	h.st.rewriteBaseSize = 0
	h.st.sqliteWALTruncated = false
	h.st.clearSQLiteWALHeaderWrite()
	if h.st.layout != "" && revision > 0 && size >= 0 {
		h.st.revision = revision
		h.st.size = size
	} else if revision <= 0 {
		h.st.layout = ""
		h.st.revision = 0
		h.st.size = 0
	}
}

// observeLayoutLocked records a server-observed layout at one baseline.
func (h *Handle) observeLayoutLocked(layout client.ContentLayout, revision, size int64) {
	if revision <= 0 || size < 0 {
		return
	}
	if layout != client.ContentLayoutSingle && layout != client.ContentLayoutAppendLog {
		return
	}
	h.st.layout = layout
	h.st.revision = revision
	h.st.size = size
}

// layoutAtLocked answers the layout observed at one exact baseline.
func (h *Handle) layoutAtLocked(revision, size int64) client.ContentLayout {
	if revision < 0 || size < 0 || h.st.revision != revision || h.st.size != size {
		return ""
	}
	return h.st.layout
}

// readWALHeaderLocked parses the buffer's first 32 bytes as a WAL header,
// loading the covering part first.
func (h *Handle) readWALHeaderLocked() (sqliteWALHeader, bool) {
	if h.buf == nil || h.buf.Size() < sqliteWALHeaderSize {
		return sqliteWALHeader{}, false
	}
	if !h.buf.IsPartLoaded(0) {
		if err := h.buf.EnsureLoaded(0); err != nil {
			return sqliteWALHeader{}, false
		}
	}
	var raw [sqliteWALHeaderSize]byte
	if h.buf.ReadAt(0, raw[:]) != len(raw) {
		return sqliteWALHeader{}, false
	}
	return parseSQLiteWALHeader(raw[:])
}

// observeCommittedWALHeaderLocked proofs the just-committed header and
// clears the dirty coverage of the finished generation.
func (h *Handle) observeCommittedWALHeaderLocked(header sqliteWALHeader) {
	h.st.sqliteWALConfirmed = true
	h.st.sqliteWALCommittedHeader = header
	h.st.clearSQLiteWALHeaderWrite()
}

// ensureBuffer creates the write buffer. Existing files keep their base
// lazy: parts load from the chain on first touch, so memory stays
// proportional to the touched regions.
func (h *Handle) ensureBuffer(ctx context.Context) error {
	if h.buf != nil {
		return nil
	}
	h.buf = writebuffer.NewWriteBuffer(h.path, bufferedMaxTotal, 0)
	if h.isNew || h.inner == nil || h.origSize <= 0 {
		return nil
	}
	partSize := h.buf.PartSize()
	remoteSize := h.origSize
	inner := h.inner
	h.buf.LoadPart = func(partNum int) ([]byte, error) {
		start := int64(partNum-1) * partSize
		end := start + partSize
		if end > remoteSize {
			end = remoteSize
		}
		if end <= start {
			return nil, nil
		}
		part := make([]byte, end-start)
		// The loader outlives the write that installed it (its ctx is
		// canceled when that FUSE request returns): a later Read/Write
		// loading an untouched part must use its OWN live context.
		loadCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := inner.Read(loadCtx, start, part); err != nil {
			return nil, err
		}
		return part, nil
	}
	h.buf.SeedRemote(remoteSize)
	return nil
}

// readerAtFunc adapts a closure to io.ReaderAt (snapshot capture under the
// handle lock).
type readerAtFunc func(off int64, p []byte) (int, error)

func (f readerAtFunc) ReadAt(p []byte, off int64) (int, error) { return f(off, p) }

func appendLogErrorCode(err error) string {
	var se *client.StatusError
	if errors.As(err, &se) {
		return se.Code
	}
	return ""
}

func mapErr(err error) error {
	if err == nil {
		return nil
	}
	var se *client.StatusError
	if errors.As(err, &se) {
		switch se.StatusCode {
		case http.StatusNotFound:
			return fs.ErrNotFound
		case http.StatusConflict:
			return syscall.EAGAIN
		}
	}
	if errno, ok := err.(syscall.Errno); ok {
		return errno
	}
	// Everything else keeps the frozen v1 errno table (401/403 EACCES,
	// 5xx/timeouts EAGAIN, quota EDQUOT/EFBIG, ...) instead of collapsing
	// to EIO at the facade.
	return &fs.StatusError{Status: httperr.HTTPStatusOf(err)}
}
