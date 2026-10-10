// Package core implements the drive9 remote terminal as a plain fs.Handler
// tail: there is no privileged core in the framework, this is simply the
// last handler of the chain — Do never calls next. Every operation is
// issued synchronously through pkg/client, so a successful Write has
// already been committed by the server (write-sync durability:
// Flush/Fsync/Release are no-ops by construction). Writes are whole-file
// compare-and-set uploads patched in place, with a small
// revision-conflict retry budget; reads are range requests. Efficiency
// layers compose above this terminal. Error classification and errno
// mapping live in errors.go.
package core

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/singleflight"
)

const (
	// defaultWriteRetries bounds compare-and-set retries on revision
	// conflicts.
	defaultWriteRetries = 3
	// Default detached stat-retry budget.
	defaultStatRetryCount   = 3
	defaultStatRetryTimeout = 2 * time.Second
	// defaultReadConcurrency bounds concurrent remote reads by default.
	defaultReadConcurrency = 24

	// readTransientRetry* bound the read-path detached retry.
	readTransientRetryCount   = 2
	readTransientRetryTimeout = 2 * time.Second
	// remoteFetchTimeout bounds one shared singleflight fetch.
	remoteFetchTimeout = 30 * time.Second

	// namespaceMutationRetryTimeout bounds each detached probe and retry
	// after a transient namespace-mutation failure.
	namespaceMutationRetryTimeout = 2 * time.Second
	// mutationCommitTimeout caps a detached namespace-mutation commit
	// commit: a kernel interrupt after the request hit the wire
	// must not abort the server-side commit into an ambiguous state.
	mutationCommitTimeout = 30 * time.Second

	// postUploadMode retries a mode application that 404s against backend
	// replica lag right after an upload.
	postUploadModeAttempts     = 5
	postUploadModeInitialDelay = 10 * time.Millisecond
)

// errReadRetriesExhausted marks a read whose transient retries are spent;
// it wraps the last error so the errno table still sees its cause.
var errReadRetriesExhausted = errors.New("read retries exhausted")

// Options configures the terminal.
type Options struct {
	// RemoteRoot prefixes every engine path before it reaches the client.
	RemoteRoot string

	// WriteRetries bounds compare-and-set retries on revision conflicts.
	// Zero means defaultWriteRetries.
	WriteRetries int

	// StatRetryCount bounds detached retries after transient Lookup/GetAttr
	// stat failures: 0 disables, negative takes the
	// default, positive is the count itself.
	StatRetryCount int
	// StatRetryTimeout bounds each detached retry (default 2s).
	StatRetryTimeout time.Duration

	// ReadConcurrency bounds concurrent remote reads (default 24); the
	// backend loses its backpressure without it.
	ReadConcurrency int

	// LegacyInterruptibleMutations keeps namespace-mutation commits bound
	// to the FUSE cancel channel (legacy mode). Default false: commits
	// run detached so an interrupt cannot abort them into an ambiguous
	// server-side state.
	LegacyInterruptibleMutations bool

	// LegacyDirStatFallback serves directory stats by listing the parent
	// when the backend rejects HEAD on directories (some deployments
	// 403/404 directory stats).
	LegacyDirStatFallback bool
}

// NamespaceCache answers path lookups from the mount's directory-listing
// state (implemented by the facade's listing cache; attached after
// assembly).
type NamespaceCache interface {
	LookupNamespace(parent, name string) (fs.DirEntryInfo, fs.LookupKind)
}

// Core is the remote terminal: the last handler of the chain, serving
// every op synchronously against the HTTP backend.
type Core struct {
	cl      *client.Client
	root    string
	retries int

	statRetryCount   int
	statRetryTimeout time.Duration
	dirCache         NamespaceCache
	// readFlight dedups concurrent remote reads of one range:
	// thundering-herd FUSE reads on an uncached file collapse into a
	// single HTTP fetch.
	readFlight *singleflight.SingleFlight
	// readSlots bounds concurrent remote reads (default 24).
	readSlots chan struct{}

	legacyDirStatFallback        bool
	legacyInterruptibleMutations bool
}

// New constructs the terminal. It negotiates the server's inline threshold
// once (cl.Warm) so commits route small files through conditional PUT
// instead of the threshold-0 safe default (multipart).
func New(cl *client.Client, opts Options) (*Core, error) {
	if cl == nil {
		return nil, fmt.Errorf("core: client is required")
	}
	cl.Warm(context.Background())
	retries := opts.WriteRetries
	if retries <= 0 {
		retries = defaultWriteRetries
	}
	statRetries := opts.StatRetryCount
	if statRetries < 0 {
		statRetries = defaultStatRetryCount
	}
	statTimeout := opts.StatRetryTimeout
	if statTimeout <= 0 {
		statTimeout = defaultStatRetryTimeout
	}
	readConcurrency := opts.ReadConcurrency
	if readConcurrency <= 0 {
		readConcurrency = defaultReadConcurrency
	}
	return &Core{cl: cl, root: normalizeRoot(opts.RemoteRoot), retries: retries,
		statRetryCount: statRetries, statRetryTimeout: statTimeout,
		readFlight:                   singleflight.New(),
		readSlots:                    make(chan struct{}, readConcurrency),
		legacyDirStatFallback:        opts.LegacyDirStatFallback,
		legacyInterruptibleMutations: opts.LegacyInterruptibleMutations}, nil
}

// Attach wires the facade-owned namespace cache into the terminal so
// Lookups/GetAttrs resolve from fresh listings without a remote stat.
func (c *Core) Attach(dirCache NamespaceCache) {
	c.dirCache = dirCache
}

// Do serves an operation synchronously against the backend. As the chain
// tail it never calls next.
func (c *Core) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	switch op.Kind {
	case fs.OpLookup, fs.OpGetAttr:
		return c.statOp(ctx, op.Path)
	case fs.OpList:
		return c.listOp(ctx, op.Path)
	case fs.OpRead:
		return c.readOp(ctx, op.Path, op.Offset, op.Limit)
	case fs.OpCreate:
		return c.createOp(ctx, op)
	case fs.OpWrite:
		if err := c.WriteSync(ctx, op.Path, op.Offset, op.Data, op.Mode); err != nil {
			return fs.Result{}, err
		}
		return fs.Result{Written: uint32(len(op.Data))}, nil
	case fs.OpMkdir:
		mode := op.Mode & 0o7777
		if err := c.mkdirRemote(ctx, op.Path, mode); err != nil {
			return fs.Result{}, mapErr(err)
		}
		return fs.Result{Attr: &fs.Attr{Kind: fs.KindDir, Mode: mode}}, nil
	case fs.OpRmdir:
		if err := c.deleteRemote(ctx, op.Path, true); err != nil {
			// Tolerate 404: a stale kernel dentry (or a concurrent
			// removal) makes unlink/rmdir of an already-gone path succeed.
			if client.IsNotFound(err) {
				return fs.Result{}, nil
			}
			return fs.Result{}, mapErr(err)
		}
		return fs.Result{}, nil
	case fs.OpUnlink:
		if err := c.deleteRemote(ctx, op.Path, false); err != nil {
			if client.IsNotFound(err) {
				return fs.Result{}, nil
			}
			return fs.Result{}, mapErr(err)
		}
		return fs.Result{}, nil
	case fs.OpRename:
		if err := c.renameRemote(ctx, op.Path, op.Path2); err != nil {
			return fs.Result{}, mapErr(err)
		}
		return fs.Result{}, nil
	case fs.OpSymlink:
		if err := c.symlinkRemote(ctx, op.Target, op.Path); err != nil {
			return fs.Result{}, mapErr(err)
		}
		// Synthesize the entry: symlink mode, size = target length;
		// the target is also the link's stored content.
		return fs.Result{Attr: &fs.Attr{Kind: fs.KindSymlink, Mode: 0o777, Size: int64(len(op.Target))}}, nil
	case fs.OpReadlink:
		// drive9 stores a symlink's target as the link's file content.
		data, err := c.fetchRemote(ctx, c.remotePath(op.Path), 0, 0)
		if err != nil {
			return fs.Result{}, mapErr(err)
		}
		return fs.Result{Data: data}, nil
	case fs.OpLink:
		if err := c.hardlinkRemote(ctx, op.Path, op.Path2); err != nil {
			return fs.Result{}, mapErr(err)
		}
		// The kernel sizes the new name from this reply; a missing Attr
		// reads as an empty file.
		st, err := c.cl.StatCtx(ctx, c.remotePath(op.Path2))
		if err != nil {
			return fs.Result{}, mapErr(err)
		}
		return fs.Result{Attr: attrOf(st)}, nil
	case fs.OpMknod:
		if op.Mode&uint32(syscall.S_IFMT) != 0 && op.Mode&uint32(syscall.S_IFMT) != syscall.S_IFREG {
			// The terminal creates regular files only; any other node
			// type is unsupported here.
			return fs.Result{}, fs.ErrUnsupported
		}
		return c.mknodRegular(ctx, op)
	case fs.OpSetAttr:
		return c.setAttrOp(ctx, op)
	case fs.OpFlush, fs.OpFsync, fs.OpRelease:
		// Write-sync: nothing is buffered locally, so durability
		// boundaries carry no extra work.
		return fs.Result{}, nil
	}
	return fs.Result{}, fs.ErrUnsupported
}

// WriteSync durably writes data at off: it returns only after the server
// has committed the whole file image (write-sync durability).
func (c *Core) WriteSync(ctx context.Context, path string, off int64, data []byte, mode uint32) error {
	rp := c.remotePath(path)
	for attempt := 0; attempt <= c.retries; attempt++ {
		st, err := c.cl.StatCtx(ctx, rp)
		if err != nil {
			if !client.IsNotFound(err) {
				return mapErr(err)
			}
			if off != 0 {
				// write past EOF on a missing path: materialize an empty
				// file first, then retry the stat
				if _, cerr := c.cl.CreateFileCtx(ctx, rp); cerr != nil && !errors.Is(cerr, client.ErrConflict) {
					return mapErr(cerr)
				}
				continue
			}
			if werr := c.cl.WriteStream(ctx, rp, bytes.NewReader(data), int64(len(data)), nil); werr != nil {
				if errors.Is(werr, client.ErrConflict) {
					continue // lost the create race; retry as an existing file
				}
				return mapErr(werr)
			}
			if mode != 0 {
				if err := retryPostUploadMode(ctx, func() error {
					return c.cl.ChmodCtx(ctx, rp, mode&0o777)
				}); err != nil {
					return mapErr(err)
				}
			}
			return nil
		}
		content, merr := c.mergeAt(ctx, rp, st, off, data)
		if merr != nil {
			return merr
		}
		if werr := c.cl.WriteStreamConditional(ctx, rp, bytes.NewReader(content), int64(len(content)), nil, st.Revision); werr != nil {
			if errors.Is(werr, client.ErrConflict) {
				continue // revision moved under us; re-stat and re-patch
			}
			return mapErr(werr)
		}
		if mode == 0 && st.HasMode && st.Mode&0o7777 != 0o644 {
			// The server resets permissions on whole-image PUTs; an
			// explicitly chmod'd file keeps its mode across writes. The
			// reapply is best-effort (v1 postUploadMode tolerated lag): a
			// failing mode restore must not fail an already-committed
			// write.
			_ = retryPostUploadMode(ctx, func() error {
				return c.cl.ChmodCtx(ctx, rp, st.Mode&0o7777)
			})
		}
		return nil
	}
	return &fs.StatusError{Path: path, Status: gofuse.EAGAIN}
}

// SyncHandle is the terminal data-plane handle: every Read is a range
// request through the shared read machinery and every Write a durable
// whole-file commit. Sync/Flush/Release are no-ops — nothing is buffered.
type SyncHandle struct {
	core *Core
	path string
}

// Read fills dest with content at off.
func (h *SyncHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	data, err := h.core.fetchRemote(ctx, h.core.remotePath(h.path), off, int64(len(dest)))
	if err != nil {
		return 0, mapErr(err)
	}
	return copy(dest, data), nil
}

// Write durably commits data at off.
func (h *SyncHandle) Write(ctx context.Context, off int64, data []byte) error {
	return h.core.WriteSync(ctx, h.path, off, data, 0)
}

// Sync implements fs.Handle.
func (h *SyncHandle) Sync(ctx context.Context) error { return nil }

// Flush implements fs.Handle.
func (h *SyncHandle) Flush(ctx context.Context) error { return nil }

// Release implements fs.Handle.
func (h *SyncHandle) Release(ctx context.Context) error { return nil }

// statOp serves OpLookup / OpGetAttr: the namespace cache first (positive
// and negative rows), then a remote stat with transient-retry absorption,
// with the legacy parent-listing fallback as the last resort.
func (c *Core) statOp(ctx context.Context, path string) (fs.Result, error) {
	if attr, ok := c.statFromNamespaceCache(path); ok {
		if attr == nil {
			// Cached-negative rows (negative entry, complete miss, session
			// miss) answer ENOENT without a remote stat. Returning a nil
			// attr as success here would render the missing name as an
			// empty file.
			return fs.Result{}, fs.ErrNotFound
		}
		return fs.Result{Attr: attr}, nil
	}
	st, err := c.statWithTransientRetry(ctx, path)
	if err != nil {
		if attr, ok := c.statFromParentListing(ctx, path, err); ok {
			return fs.Result{Attr: attr}, nil
		}
		return fs.Result{}, mapErr(err)
	}
	return fs.Result{Attr: attrOf(st)}, nil
}

// statFromParentListing serves a stat by matching the parent listing when
// the backend cannot HEAD the path itself: some deployments 403/404
// directory stats while listings work.
func (c *Core) statFromParentListing(ctx context.Context, path string, statErr error) (*fs.Attr, bool) {
	if !c.legacyDirStatFallback {
		return nil, false
	}
	var se *client.StatusError
	if !errors.As(statErr, &se) {
		return nil, false
	}
	switch se.StatusCode {
	case http.StatusForbidden, http.StatusNotFound, http.StatusMethodNotAllowed:
	default:
		return nil, false
	}
	parent, name := splitDirBase(path)
	if parent == "" || name == "" {
		return nil, false
	}
	infos, err := c.cl.ListCtx(ctx, c.remotePath(parent))
	if err != nil {
		return nil, false
	}
	for _, fi := range infos {
		if fi.Name != name {
			continue
		}
		kind := fs.KindFile
		mode := fi.Mode & 0o7777
		if fi.IsDir {
			kind = fs.KindDir
			if !fi.HasMode {
				mode = 0o755
			}
		} else if !fi.HasMode {
			mode = 0o644
		}
		mtime := time.Time{}
		if fi.Mtime > 0 {
			mtime = time.Unix(fi.Mtime, 0)
		}
		return &fs.Attr{Kind: kind, Mode: mode, Size: fi.Size, Rev: fi.Revision, Mtime: mtime}, true
	}
	return nil, false
}

// statFromNamespaceCache serves Lookup/GetAttr from a fresh cached listing
// (positive/negative/complete-miss rows). Lock files
// never use it: O_CREAT|O_EXCL probes must revalidate against the remote.
func (c *Core) statFromNamespaceCache(path string) (*fs.Attr, bool) {
	if c.dirCache == nil || path == "/" || path == "" {
		return nil, false
	}
	parent, name := splitDirBase(path)
	if parent == "" || name == "" || name == "." || name == ".." || strings.HasSuffix(name, ".lock") {
		return nil, false
	}
	item, kind := c.dirCache.LookupNamespace(parent, name)
	switch kind {
	case fs.LookupPositive:
		itemKind := fs.KindFile
		if item.IsDir {
			itemKind = fs.KindDir
		} else if item.HasMode && item.Mode&uint32(syscall.S_IFMT) == uint32(syscall.S_IFLNK) {
			itemKind = fs.KindSymlink
		}
		mode := item.Mode
		if !item.HasMode {
			mode = 0o644
			if item.IsDir {
				mode = 0o755
			}
		}
		if itemKind == fs.KindSymlink {
			mode = uint32(syscall.S_IFLNK) | mode&0o777
		} else {
			mode &= 0o7777
		}
		return &fs.Attr{Kind: itemKind, Mode: mode, Size: item.Size, Rev: item.Rev, Mtime: item.Mtime,
			Nlink: item.Nlink, Uid: item.Uid, Gid: item.Gid, HasOwner: item.HasUID || item.HasGID}, true
	case fs.LookupNegative, fs.LookupCompleteMiss, fs.LookupSessionMiss:
		return nil, true // known absent: ENOENT without a remote stat
	}
	return nil, false
}

// statWithTransientRetry absorbs short-lived backend jitter with detached,
// bounded retries: the initial probe honors the
// caller's context; retries do not re-bind it, so a kernel interrupt cannot
// cancel the absorb window.
func (c *Core) statWithTransientRetry(ctx context.Context, path string) (*client.StatResult, error) {
	rp := c.remotePath(path)
	st, err := c.cl.StatCtx(ctx, rp)
	if err == nil || client.IsNotFound(err) || !isTransientStatErr(err) || c.statRetryCount <= 0 {
		return st, err
	}
	for range c.statRetryCount {
		retryCtx, cancel := context.WithTimeout(context.Background(), c.statRetryTimeout)
		st, err = c.cl.StatCtx(retryCtx, rp)
		cancel()
		if err == nil || client.IsNotFound(err) || !isTransientStatErr(err) {
			return st, err
		}
	}
	return st, err
}

// listOp serves OpList: it lists one directory, dropping unusable rows
// (empty, dotdot, separator-bearing) and classifying symlinks by their
// mode bits.
func (c *Core) listOp(ctx context.Context, dir string) (fs.Result, error) {
	infos, err := c.cl.ListCtx(ctx, c.remotePath(dir))
	if err != nil {
		return fs.Result{}, mapErr(err)
	}
	entries := make([]fs.Entry, 0, len(infos))
	for _, fi := range infos {
		if fi.Name == "" || fi.Name == "." || fi.Name == ".." ||
			strings.ContainsRune(fi.Name, '/') || strings.ContainsRune(fi.Name, 0) {
			continue
		}
		attr := fs.Attr{Size: fi.Size, Rev: fi.Revision, Mode: fi.Mode & 0o7777, Mtime: time.Unix(fi.Mtime, 0), Nlink: fi.Nlink, ExtentIno: fi.ExtentIno}
		if fi.IsDir {
			attr.Kind = fs.KindDir
			if !fi.HasMode {
				attr.Mode = 0o755
			}
		} else if fi.HasMode && fi.Mode&uint32(syscall.S_IFMT) == uint32(syscall.S_IFLNK) {
			attr.Kind = fs.KindSymlink
			// Keep the type bit: cached entries restore the symlink kind
			// from S_IFMT (a perms-only mask would retype the link as a
			// regular file and break follow semantics).
			attr.Mode = uint32(syscall.S_IFLNK) | 0o777
		} else if !fi.HasMode {
			attr.Mode = 0o644
		}
		entries = append(entries, fs.Entry{Name: fi.Name, Attr: attr})
	}
	return fs.Result{Entries: entries}, nil
}

// createOp serves OpCreate (open-or-create). The existence probe goes
// through statOp: the namespace cache answers paths this mount just
// created/committed, so an open racing backend replica lag reads the local
// view instead of a stale remote stat.
func (c *Core) createOp(ctx context.Context, op *fs.Op) (fs.Result, error) {
	rp := c.remotePath(op.Path)
	probe, statErr := c.statOp(ctx, op.Path)
	exists := statErr == nil && probe.Attr != nil
	if statErr != nil && statErr != fs.ErrNotFound {
		return fs.Result{}, statErr
	}
	var st *client.StatResult
	if exists {
		st = &client.StatResult{
			IsDir: probe.Attr.IsDir(), Size: probe.Attr.Size,
			Revision: probe.Attr.Rev, Mtime: probe.Attr.Mtime,
			Nlink: probe.Attr.Nlink,
		}
	}
	flags := op.Flags
	if flags&uint32(syscall.O_CREAT) != 0 {
		if exists && flags&uint32(syscall.O_EXCL) != 0 {
			return fs.Result{}, syscall.EEXIST
		}
		if !exists {
			commitCtx, cancel := c.namespaceMutationCommitContext(ctx)
			_, err := c.cl.CreateFileCtx(commitCtx, rp)
			cancel()
			if err != nil {
				// A conflict against a path this mount believes gone is
				// backend replica lag (create-after-unlink/rename races a
				// stale replica): re-probe detached and retry once before
				// surfacing EEXIST (v1 createFile conflict tolerance).
				if errors.Is(err, client.ErrConflict) {
					// Bounded backoff: cross-pod replica lag on this
					// backend outlasts sub-second ladders — budget a few
					// seconds before surfacing EEXIST (v1's retry ladder).
					for range 12 {
						if _, serr := c.remoteStatDetached(rp); serr != nil && client.IsNotFound(serr) {
							retryCtx, rcancel := c.namespaceMutationCommitContext(ctx)
							_, err = c.cl.CreateFileCtx(retryCtx, rp)
							rcancel()
							if err == nil {
								st, statErr := c.cl.StatCtx(ctx, rp)
								if statErr != nil {
									return fs.Result{}, mapErr(statErr)
								}
								return fs.Result{Attr: attrOf(st), Handle: &SyncHandle{core: c, path: op.Path}}, nil
							}
							if !errors.Is(err, client.ErrConflict) {
								break
							}
						}
						time.Sleep(250 * time.Millisecond)
					}
				}
				return fs.Result{}, mapErr(err)
			}
			// An explicit create mode is always meaningful (mode 0 must be
			// applied, not skipped: pjdfstest creates 00-mode files); the
			// full 0o7777 range keeps SUID/SGID at create (chmod/12).
			if mode := op.Mode & 0o7777; mode != 0o644 {
				if err := retryPostUploadMode(ctx, func() error {
					return c.cl.ChmodCtx(ctx, rp, mode)
				}); err != nil {
					return fs.Result{}, mapErr(err)
				}
			}
			st, statErr = c.cl.StatCtx(ctx, rp)
			if statErr != nil {
				return fs.Result{}, mapErr(statErr)
			}
		}
	} else if !exists {
		return fs.Result{}, fs.ErrNotFound
	}
	// open(O_TRUNC) truncates locally only: truncation is published
	// at a commit boundary above the terminal — PUTting the remote here
	// would change crash semantics (an open that crashes before any write
	// must leave the remote intact).
	return fs.Result{Attr: attrOf(st), Handle: &SyncHandle{core: c, path: op.Path}}, nil
}

// setAttrOp serves OpSetAttr. The probe comes first: it yields the kind (directory chmod goes to the
// trailing-slash form), the truncate base revision, and the reply
// attributes — no extra remote stat follows the mutations.
func (c *Core) setAttrOp(ctx context.Context, op *fs.Op) (fs.Result, error) {
	probe, statErr := c.statOp(ctx, op.Path)
	if statErr != nil {
		return fs.Result{}, statErr
	}
	if probe.Attr == nil {
		return fs.Result{}, fs.ErrNotFound
	}
	attr := *probe.Attr
	rp := c.remotePath(op.Path)
	if op.Mode != 0 {
		mode := op.Mode & 0o7777
		target := rp
		if attr.IsDir() {
			target = remoteDirectoryPath(rp)
		}
		commitCtx, cancel := c.namespaceMutationCommitContext(ctx)
		// retryPostUploadMode (v1 Setattr): a chmod can 404 while this
		// mount's own async create/upload is still landing (writeback's
		// RELEASE races ahead of the queue) — retry across that window
		// before failing, exactly like v1 did.
		err := retryPostUploadMode(commitCtx, func() error {
			return c.cl.ChmodCtx(commitCtx, target, mode)
		})
		cancel()
		if err != nil && !c.chmodNotFoundCanStayLocal(rp, target, err) {
			return fs.Result{}, mapErr(err)
		}
		attr.Mode = mode
		// The chmod bumps the remote revision; the probed (cached) rev is
		// stale now. Refresh it so a follow-up conditional write (truncate)
		// does not CAS against the pre-chmod revision and 409.
		if st, serr := c.remoteStatDetached(rp); serr == nil && st != nil {
			attr.Rev = st.Revision
		}
	}
	if op.Size >= 0 && attr.Size != op.Size {
		if op.Size > maxTruncateImageSize {
			// Whole-image truncate materializes the target; absurd sizes
			// (the kernel can forward near-2^63 lengths) must fail fast
			// instead of attempting the allocation.
			return fs.Result{}, fs.StatusErrorOf(gofuse.Status(syscall.EFBIG))
		}
		// Truncate (v1): read only the kept prefix — min(newSize, oldSize)
		// — and zero-extend to the target, so a shrink or a truncate-to-
		// zero never downloads the discarded tail. The commit CASes on the
		// probed revision directly: the buffered-write path would re-read
		// the whole old image for a shrink (mergeAt), defeating the prefix
		// read.
		keep := op.Size
		if attr.Size < keep {
			keep = attr.Size
		}
		var old []byte
		if keep > 0 {
			var err error
			old, err = c.cl.ReadAtCtx(ctx, rp, 0, keep)
			if err != nil {
				return fs.Result{}, mapErr(err)
			}
		}
		next := make([]byte, op.Size)
		copy(next, old)
		// A cached namespace row may carry no revision (Rev 0); a
		// conditional commit with revision 0 tells the server "expect the
		// path to be absent" and fails with a conflict. Take a fresh
		// detached stat before CASing.
		if attr.Rev <= 0 {
			if st, serr := c.remoteStatDetached(rp); serr == nil && st != nil {
				attr.Rev = st.Revision
			}
		}
		// v1 write-sync: commits go through the streaming upload path,
		// which routes payloads over the server's inline threshold through
		// the multipart protocol with part checksums — a direct conditional
		// PUT is rejected there with "missing X-Dat9-Part-Checksums".
		// The truncate CASes on the probed revision; a just-landed async
		// create can leave the probe's replica view stale (409 → EIO,
		// truncate/05). Re-stat and re-CAS like the commit paths (v1).
		var werr error
		for attempt := 0; attempt < 3; attempt++ {
			if attempt > 0 {
				if st, serr := c.remoteStatDetached(rp); serr == nil && st != nil {
					attr.Rev = st.Revision
				}
			}
			werr = c.cl.WriteStreamConditional(ctx, rp, bytes.NewReader(next), int64(len(next)), nil, attr.Rev)
			if werr == nil {
				break
			}
			if !errors.Is(werr, client.ErrConflict) {
				return fs.Result{}, mapErr(werr)
			}
		}
		if werr != nil {
			return fs.Result{}, mapErr(werr)
		}
		attr.Size = op.Size
		if st, serr := c.remoteStatDetached(rp); serr == nil && st != nil {
			attr.Rev = st.Revision
		}
	}
	// op.Mtime: the backend derives mtimes from writes and has no mtime
	// verb, so timestamp changes are accepted as no-ops rather than lying
	// with an error to every utimens(2).
	return fs.Result{Attr: &attr}, nil
}

// mknodRegular creates a regular file: create-on-missing with EEXIST on
// collision, a conditional chmod for the requested permissions, and a
// rollback delete when the mode application fails.
func (c *Core) mknodRegular(ctx context.Context, op *fs.Op) (fs.Result, error) {
	rp := c.remotePath(op.Path)
	// The existence probe goes through statOp so this mount's own negative
	// rows answer first: a recreate right after this mount deleted the name
	// must not race backend replica lag on the stale remote stat.
	_, statErr := c.statOp(ctx, op.Path)
	if statErr == nil {
		return fs.Result{}, syscall.EEXIST
	} else if !fs.IsNotFound(statErr) {
		return fs.Result{}, statErr
	}
	commitCtx, cancel := c.namespaceMutationCommitContext(ctx)
	_, createErr := c.cl.CreateFileCtx(commitCtx, rp)
	cancel()
	if createErr != nil {
		return fs.Result{}, mapErr(createErr)
	}
	mode := op.Mode & 0o777
	if mode == 0 {
		mode = 0o644
	}
	rollback := func() { _ = c.cl.DeleteFileCtx(context.WithoutCancel(ctx), rp) }
	if mode != 0o644 {
		if err := retryPostUploadMode(ctx, func() error {
			return c.cl.ChmodCtx(ctx, rp, mode)
		}); err != nil {
			rollback()
			return fs.Result{}, mapErr(err)
		}
	}
	st, err := c.cl.StatCtx(ctx, rp)
	if err != nil {
		rollback()
		return fs.Result{}, mapErr(err)
	}
	return fs.Result{Attr: attrOf(st)}, nil
}

// readOp serves OpRead: a range (limit>0) or whole-file fetch through the
// shared read machinery, slicing whole-file results by the offset.
func (c *Core) readOp(ctx context.Context, path string, off, limit int64) (fs.Result, error) {
	data, err := c.fetchRemote(ctx, c.remotePath(path), off, limit)
	if err != nil {
		return fs.Result{}, mapErr(err)
	}
	if limit <= 0 && off > 0 {
		if off >= int64(len(data)) {
			data = nil
		} else {
			data = data[off:]
		}
	}
	return fs.Result{Data: data}, nil
}

// fetchRemote fetches content through the mount-wide singleflight and slot
// limiter with bounded transient retries (request dedup, mount-wide
// backpressure, and detached retry loops). limit>0 reads one range;
// limit<=0 reads the whole file.
func (c *Core) fetchRemote(ctx context.Context, rp string, off, limit int64) ([]byte, error) {
	key := rp + "@" + strconv.FormatInt(off, 10) + "@" + strconv.FormatInt(limit, 10)
	data, err, _ := c.readFlight.Do(ctx, key, func() ([]byte, error) {
		// The shared fetch runs detached from the owner's context:
		// cancelling one piggybacking FUSE request must not fail the
		// fetch every other waiter shares; a fresh bound keeps it finite.
		fctx, cancel := context.WithTimeout(context.Background(), remoteFetchTimeout)
		defer cancel()
		data, err := c.readOnce(fctx, rp, off, limit)
		if err == nil || !isTransientReadErr(err) {
			return data, err
		}
		var lastErr = err
		for range readTransientRetryCount {
			retryCtx, retryCancel := context.WithTimeout(context.Background(), readTransientRetryTimeout)
			data, err = c.readOnce(retryCtx, rp, off, limit)
			retryCancel()
			if err == nil {
				return data, nil
			}
			if !isTransientReadErr(err) {
				return nil, err
			}
			lastErr = err
		}
		return nil, fmt.Errorf("%w: %s: %v", errReadRetriesExhausted, rp, lastErr)
	})
	return data, err
}

// readOnce performs one bounded fetch attempt under the slot limiter
// request: wait a concurrency slot before the HTTP fetch so the
// backend is not stampeded.
func (c *Core) readOnce(ctx context.Context, rp string, off, limit int64) ([]byte, error) {
	select {
	case c.readSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-c.readSlots }()
	if limit > 0 {
		return c.cl.ReadAtCtx(ctx, rp, off, limit)
	}
	return c.cl.ReadCtx(ctx, rp)
}

// mergeAt builds the whole-file image for a write at off. The overwrite
// fast path skips the read when the write covers the whole file.
func (c *Core) mergeAt(ctx context.Context, rp string, st *client.StatResult, off int64, data []byte) ([]byte, error) {
	if off == 0 && int64(len(data)) >= st.Size {
		return data, nil
	}
	old, err := c.cl.ReadCtx(ctx, rp)
	if err != nil {
		return nil, mapErr(err)
	}
	end := off + int64(len(data))
	if end > int64(len(old)) {
		grown := make([]byte, end)
		copy(grown, old)
		old = grown
	}
	copy(old[off:], data)
	return old, nil
}

// namespaceMutationCommitContext detaches a namespace-mutation commit from
// the FUSE cancel channel: a kernel interrupt after the request hit
// the wire must not abort the server-side commit into an ambiguous state.
// The mount-wide timeout stays clamped to the caller's own deadline; the
// legacy-interruptible mode keeps the caller's context.
func (c *Core) namespaceMutationCommitContext(parent context.Context) (context.Context, context.CancelFunc) {
	if c.legacyInterruptibleMutations {
		return parent, func() {}
	}
	timeout := mutationCommitTimeout
	if deadline, ok := parent.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < timeout {
			timeout = remaining
		}
	}
	if timeout <= 0 {
		timeout = time.Nanosecond
	}
	return context.WithTimeout(context.WithoutCancel(parent), timeout)
}

// dirNotEmpty retry knobs: the lag window between a child deletion and a
// parent rmdir is short; a few quick probes cover it.
const (
	dirNotEmptyRetryCount = 8
	dirNotEmptyRetryDelay = 100 * time.Millisecond
)

// maxTruncateImageSize bounds a whole-image truncate: 1 TiB matches the
// StatFs capacity the facade advertises.
const maxTruncateImageSize = 1 << 40

// remoteDirectoryPath returns the directory form of a remote path:
// directories carry a trailing slash on the wire.
func remoteDirectoryPath(p string) string {
	if p == "/" || strings.HasSuffix(p, "/") {
		return p
	}
	return p + "/"
}

// remoteStatDetached stats a remote path detached from any request
// context, bounded by the mutation retry timeout.
func (c *Core) remoteStatDetached(rp string) (*client.StatResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), namespaceMutationRetryTimeout)
	defer cancel()
	return c.cl.StatCtx(ctx, rp)
}

// remoteDirExistsDetached reports whether a directory landed remotely.
func (c *Core) remoteDirExistsDetached(path string) bool {
	st, err := c.remoteStatDetached(c.remotePath(path))
	return err == nil && st.IsDir
}

// remotePathGoneDetached reports whether a path is already gone remotely.
func (c *Core) remotePathGoneDetached(path string) bool {
	_, err := c.remoteStatDetached(c.remotePath(path))
	return client.IsNotFound(err)
}

// remoteRenameCommittedDetached reports whether a rename landed: target
// visible AND source gone — target visibility alone is not enough because
// server-side rename replaces an existing target.
func (c *Core) remoteRenameCommittedDetached(oldP, newP string) bool {
	if _, err := c.remoteStatDetached(c.remotePath(newP)); err != nil {
		return false
	}
	_, err := c.remoteStatDetached(c.remotePath(oldP))
	return client.IsNotFound(err)
}

// remoteHardlinkCommittedDetached reports whether a hardlink landed: both
// ends exist as files sharing one resource identity.
func (c *Core) remoteHardlinkCommittedDetached(srcP, dstP string) bool {
	dstStat, err := c.remoteStatDetached(c.remotePath(dstP))
	if err != nil || dstStat == nil || dstStat.IsDir || dstStat.ResourceID == "" {
		return false
	}
	srcStat, err := c.remoteStatDetached(c.remotePath(srcP))
	if err != nil || srcStat == nil || srcStat.IsDir || srcStat.ResourceID == "" {
		return false
	}
	return srcStat.ResourceID == dstStat.ResourceID
}

// mkdirRemote commits a directory creation with transient recovery: a
// transient failure first probes detached whether the directory actually
// landed (an interrupted or blipped request may have committed), then
// retries with conflict shortcuts.
func (c *Core) mkdirRemote(ctx context.Context, path string, mode uint32) error {
	rp := c.remotePath(path)
	commitCtx, cancel := c.namespaceMutationCommitContext(ctx)
	err := c.cl.MkdirCtx(commitCtx, rp, mode)
	cancel()
	if err == nil || !isTransientMutationErr(err) {
		return err
	}
	if c.remoteDirExistsDetached(path) {
		return nil
	}
	if c.statRetryCount <= 0 {
		return err
	}
	lastErr := err
	for range c.statRetryCount {
		retryCtx, retryCancel := context.WithTimeout(context.Background(), namespaceMutationRetryTimeout)
		err = c.cl.MkdirCtx(retryCtx, rp, mode)
		retryCancel()
		if err == nil {
			return nil
		}
		if isConflictErr(err) && c.remoteDirExistsDetached(path) {
			return nil
		}
		if !isTransientMutationErr(err) {
			return err
		}
		if c.remoteDirExistsDetached(path) {
			return nil
		}
		lastErr = err
	}
	return lastErr
}

// deleteRemote commits a deletion with interrupt recovery: after a
// transient failure it probes detached whether the path is already gone
// and reports success; it deliberately does NOT retry — another actor may
// have recreated the name after the ambiguous first delete.
func (c *Core) deleteRemote(ctx context.Context, path string, dir bool) error {
	rp := c.remotePath(path)
	commitCtx, cancel := c.namespaceMutationCommitContext(ctx)
	var err error
	if dir {
		err = c.cl.DeleteDirCtx(commitCtx, rp)
	} else {
		err = c.cl.DeleteFileCtx(commitCtx, rp)
	}
	cancel()
	if err == nil || !isTransientMutationErr(err) {
		if dir && isDirNotEmptyErr(err) {
			// A just-issued child deletion may not be visible yet on the
			// pod serving this rmdir (replica lag): the deferred-unlink
			// flush issues delete-then-rmdir back to back. Bounded retry
			// before surfacing ENOTEMPTY.
			for range dirNotEmptyRetryCount {
				time.Sleep(dirNotEmptyRetryDelay)
				dctx, dcancel := c.namespaceMutationCommitContext(ctx)
				retry := c.cl.DeleteDirCtx(dctx, rp)
				dcancel()
				if retry == nil {
					return nil
				}
				if !isDirNotEmptyErr(retry) {
					return retry
				}
			}
		}
		return err
	}
	if c.remotePathGoneDetached(path) {
		return nil
	}
	return err
}

// isDirNotEmptyErr matches the backend's directory-not-empty conflict.
func isDirNotEmptyErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.Contains(err.Error(), "not empty")
	var se *client.StatusError
	if errors.As(err, &se) {
		return se.StatusCode == http.StatusConflict && msg
	}
	return msg
}

// renameRemote commits a rename with transient recovery: a transient
// failure probes whether the rename committed, then retries with
// not-found and transient shortcuts.
func (c *Core) renameRemote(ctx context.Context, oldP, newP string) error {
	oldRemote, newRemote := c.remotePath(oldP), c.remotePath(newP)
	commitCtx, cancel := c.namespaceMutationCommitContext(ctx)
	err := c.cl.RenameCtx(commitCtx, oldRemote, newRemote)
	cancel()
	if err != nil {
		println("DBG renameRemote old=" + oldP + " new=" + newP + " err=" + err.Error())
	}
	if err == nil || !isTransientMutationErr(err) {
		return err
	}
	if c.remoteRenameCommittedDetached(oldP, newP) {
		return nil
	}
	if c.statRetryCount <= 0 {
		return err
	}
	lastErr := err
	for range c.statRetryCount {
		retryCtx, retryCancel := context.WithTimeout(context.Background(), namespaceMutationRetryTimeout)
		err = c.cl.RenameCtx(retryCtx, oldRemote, newRemote)
		retryCancel()
		if err == nil {
			return nil
		}
		if client.IsNotFound(err) && c.remoteRenameCommittedDetached(oldP, newP) {
			return nil
		}
		if !isTransientMutationErr(err) {
			return err
		}
		if c.remoteRenameCommittedDetached(oldP, newP) {
			return nil
		}
		lastErr = err
	}
	return lastErr
}

// hardlinkRemote commits a hardlink with transient recovery: a transient
// failure probes whether the link landed (shared resource identity), then
// retries with conflict shortcuts.
func (c *Core) hardlinkRemote(ctx context.Context, srcP, dstP string) error {
	srcRemote, dstRemote := c.remotePath(srcP), c.remotePath(dstP)
	commitCtx, cancel := c.namespaceMutationCommitContext(ctx)
	err := c.cl.HardlinkCtx(commitCtx, srcRemote, dstRemote)
	cancel()
	if err == nil || !isTransientMutationErr(err) {
		return err
	}
	if c.remoteHardlinkCommittedDetached(srcP, dstP) {
		return nil
	}
	if c.statRetryCount <= 0 {
		return err
	}
	lastErr := err
	for range c.statRetryCount {
		retryCtx, retryCancel := context.WithTimeout(context.Background(), namespaceMutationRetryTimeout)
		err = c.cl.HardlinkCtx(retryCtx, srcRemote, dstRemote)
		retryCancel()
		if err == nil {
			return nil
		}
		if isConflictErr(err) && c.remoteHardlinkCommittedDetached(srcP, dstP) {
			return nil
		}
		if !isTransientMutationErr(err) {
			return err
		}
		if c.remoteHardlinkCommittedDetached(srcP, dstP) {
			return nil
		}
		lastErr = err
	}
	return lastErr
}

// symlinkRemote commits a symlink with committed-probe recovery only: a
// transient failure probes detached whether the link landed and really is
// a symlink; there is no retry loop.
func (c *Core) symlinkRemote(ctx context.Context, target, path string) error {
	rp := c.remotePath(path)
	commitCtx, cancel := c.namespaceMutationCommitContext(ctx)
	err := c.cl.SymlinkCtx(commitCtx, target, rp)
	cancel()
	if err == nil || !isTransientMutationErr(err) {
		return err
	}
	if st, probeErr := c.remoteStatDetached(rp); probeErr == nil && st != nil && st.HasMode &&
		st.Mode&uint32(syscall.S_IFMT) == uint32(syscall.S_IFLNK) {
		return nil
	}
	return err
}

// chmodNotFoundCanStayLocal tolerates a chmod 404: it is accepted when a
// detached stat still sees the target
// (backend replica lag, or the directory trailing-slash form), keeping the
// mode change mount-local instead of failing setattr.
func (c *Core) chmodNotFoundCanStayLocal(rp, chmodTarget string, chmodErr error) bool {
	if !client.IsNotFound(chmodErr) {
		return false
	}
	if _, err := c.remoteStatDetached(rp); err == nil {
		return true
	}
	if strings.HasSuffix(chmodTarget, "/") {
		withoutSlash := strings.TrimRight(chmodTarget, "/")
		if withoutSlash != "" && withoutSlash != chmodTarget {
			if _, err := c.remoteStatDetached(withoutSlash); err == nil {
				return true
			}
		}
	}
	return false
}

// retryPostUploadMode retries a mode application that can 404 against
// backend replica lag right after an upload, with a short bounded backoff
// before failing.
func retryPostUploadMode(ctx context.Context, apply func() error) error {
	var lastErr error
	for attempt := 0; attempt < postUploadModeAttempts; attempt++ {
		lastErr = apply()
		if lastErr == nil {
			return nil
		}
		if !client.IsNotFound(lastErr) || attempt == postUploadModeAttempts-1 {
			return lastErr
		}
		timer := time.NewTimer(postUploadModeInitialDelay << attempt)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}

// normalizeRoot canonicalizes the configured remote root to "/" or a
// "/"-prefixed path without a trailing slash.
func normalizeRoot(root string) string {
	root = strings.TrimSpace(root)
	if root == "" || root == "/" {
		return "/"
	}
	if !strings.HasPrefix(root, "/") {
		root = "/" + root
	}
	return strings.TrimSuffix(root, "/")
}

// remotePath prefixes a mount path with the remote root.
func (c *Core) remotePath(p string) string {
	if c.root == "/" {
		return p
	}
	if p == "/" {
		return c.root
	}
	return c.root + p
}

// splitDirBase splits an absolute path into its parent directory and base
// name; the root parent keeps its slash.
func splitDirBase(p string) (dir, base string) {
	i := strings.LastIndexByte(p, '/')
	if i < 0 {
		return "", p
	}
	if i == 0 {
		return "/", p[1:]
	}
	return p[:i], p[i+1:]
}

// attrOf converts a backend stat into the framework attribute vocabulary.
func attrOf(st *client.StatResult) *fs.Attr {
	kind := fs.KindFile
	if st.IsDir {
		kind = fs.KindDir
	} else if st.HasMode && st.Mode&uint32(syscall.S_IFMT) == uint32(syscall.S_IFLNK) {
		kind = fs.KindSymlink
	}
	mode := st.Mode & 0o7777
	if !st.HasMode {
		mode = 0o644
		if kind == fs.KindDir {
			mode = 0o755
		}
	}
	return &fs.Attr{Kind: kind, Mode: mode, Size: st.Size, Rev: st.Revision, Mtime: st.Mtime, Nlink: st.Nlink}
}
