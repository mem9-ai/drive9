// Package bulkfile is the large-object data plane for fresh
// truncate-writers: a create with truncation (and write intent) is claimed
// and its full image is buffered in memory for read-your-writes. The
// durability boundaries — Flush, Fsync, Release — push the image into the
// write plane below and delegate to it: the plane below owns staging,
// upload strategy (direct PUT or multipart by size), asynchronous upload,
// and revision bases, per the mount's write policy. Nothing touches the
// network during Write; the truncation itself also stays local until the
// first boundary.
//
// Flush is never terminal: the handle stays writable across flush cycles —
// each cycle pushes the current image and re-arms below — and only Release
// finishes the handle. Unlink marks the handle anonymous so its boundary
// push is discarded instead of resurrecting the path, and rename retargets
// live handles.
package bulkfile

import (
	"context"
	"strings"
	"sync"
	"syscall"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// policyWriteSync names the mount policy whose writes commit inline at
// every Write below; the buffered-image claim defers to that track there.
const policyWriteSync = "write-sync"

// Options configures the data plane.
type Options struct {
	Client     *client.Client
	RemoteRoot string
	// Events is accepted for wiring symmetry; the write plane below emits
	// its own post-commit invalidations, so the claim itself has nothing
	// to announce.
	Events *fs.EventBus
	// Policy is the mount write policy ("writeback", "close-sync",
	// "write-sync"); blank behaves as writeback. Under write-sync the
	// claim stays out of the way — durability belongs to every Write,
	// not to a boundary.
	Policy string
}

// resolveStreamHandle finds our claim behind decorator wrappers: a
// FD-directed setattr must act on the DESCRIPTOR's object, and the default
// chain hands us the cache's (or localize's) wrapper, whose direct type is
// not *streamHandle. Without the unwrap the request fell back to the PATH
// and truncated a same-name replacement.
func (b *Bulkfile) resolveStreamHandle(handle any) *streamHandle {
	h, _ := handle.(fs.Handle)
	for depth := 0; h != nil && depth < 16; depth++ {
		if sh, ok := h.(*streamHandle); ok {
			return sh
		}
		u, ok := h.(interface{ Unwrap() fs.Handle })
		if !ok {
			return nil
		}
		h = u.Unwrap()
	}
	return nil
}

// Bulkfile is the handler claiming fresh truncate-writers. Everything else
// passes to the next handler untouched.
type Bulkfile struct {
	cl     *client.Client
	root   string
	policy string
	events *fs.EventBus

	mu sync.Mutex
	// live is the CLAIM registry: handles whose uncommitted image answers
	// lookups and read-only re-opens. A surrendered claim leaves it.
	live map[*streamHandle]struct{}
	// fds is the DESCRIPTOR registry: every open handle, including a
	// surrendered passthrough descriptor. Rename retargeting walks THIS
	// set — retiring a claim must not hide a still-open fd from the
	// namespace tracking (a surrendered fd's later write re-opened /a
	// after /a had been renamed to /b).
	fds map[*streamHandle]struct{}
}

// New builds the data plane. A missing client is a configuration error.
func New(opts Options) (*Bulkfile, error) {
	if opts.Client == nil {
		return nil, fs.ErrUnsupported
	}
	root := strings.TrimSpace(opts.RemoteRoot)
	if root == "" || root == "/" {
		root = "/"
	} else {
		root = "/" + strings.Trim(root, "/")
	}
	return &Bulkfile{
		cl: opts.Client, root: root, policy: opts.Policy, events: opts.Events,
		live: make(map[*streamHandle]struct{}),
		fds:  make(map[*streamHandle]struct{}),
	}, nil
}

// Do serves one operation. The claim resolves existence before taking the
// handle, serves lookups of live claims from the buffered image, and keeps
// unlinks and renames of never-committed claims local.
func (b *Bulkfile) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	if op.Kind == fs.OpCreate && writeIntent(op.Flags) &&
		op.Flags&uint32(syscall.O_CREAT) != 0 && op.Flags&uint32(syscall.O_TRUNC) != 0 &&
		op.Flags&uint32(syscall.O_APPEND) == 0 && !hasSyncOpenFlag(op.Flags) &&
		b.policy != policyWriteSync {
		// A prior claim on this path must hand off BEFORE the new claim
		// takes the name: its buffered bytes are the acknowledged state
		// (the same flush-or-abort contract as the non-truncate writer
		// below) — without this, a truncate-open over a live claim
		// stranded the old descriptor reading its stale image forever.
		if prior := b.liveHandleAt(op.Path); prior != nil {
			if _, ok := prior.overlayAttr(); ok {
				if ferr := prior.Flush(ctx); ferr != nil {
					return fs.Result{}, ferr
				}
				prior.surrenderToBelow()
			}
		}
		// Resolve existence BEFORE claiming, so the claim knows whether the
		// first boundary must express a truncation (an existing file) or
		// nothing at all (a fresh create that materializes at the boundary).
		st, statErr := b.cl.StatCtx(ctx, b.remotePath(op.Path))
		if statErr != nil && !client.IsNotFound(statErr) {
			return fs.Result{}, statErr
		}
		h := &streamHandle{
			b: b, path: op.Path,
			// The claim itself is the first reference to the image.
			refs: 1,
			mode: op.Mode & 0o777,
			// An O_CREAT open carries the request's mode, 000 included; a
			// plain open of an existing file leaves it unknown.
			modeKnown: op.Flags&uint32(syscall.O_CREAT) != 0 || op.Mode&0o7777 != 0,
			below:     fs.HandlerFilesystem(next),
		}
		h.openFlags = op.Flags&^uint32(syscall.O_TRUNC|syscall.O_EXCL) | uint32(syscall.O_CREAT)
		innerOp := *op
		innerOp.Flags &^= uint32(syscall.O_TRUNC)
		if statErr == nil {
			if op.Flags&uint32(syscall.O_EXCL) != 0 {
				return fs.Result{}, syscall.EEXIST
			}
			// Existing file: open it plainly below (no create/truncate
			// flags — the write plane takes its existing-file branch with a
			// real revision base) and this handle commits the truncated
			// image against it. The truncation is pending until the first
			// boundary pushes the (shorter) image.
			innerOp.Flags &^= uint32(syscall.O_CREAT)
			h.pushed = st.Size
			h.dirty = st.Size > 0
		}
		// Fresh file: keep O_CREAT so the plane below answers with its
		// lazy create (no remote effect until the boundary); even `: >
		// newfile`, which closes without a single write, materializes at
		// Release.
		res, err := next(ctx, &innerOp)
		if err != nil {
			return res, err
		}
		if res.Handle == nil {
			return res, nil
		}
		h.inner = res.Handle
		b.track(h)
		res.Handle = h
		return res, nil
	}
	// A live claim owns the file's current image: a read-only re-open
	// serves those bytes — falling through to the plane below would read
	// the not-yet-pushed remote state (a checksum spanning the landing
	// mixes the old and new images). A writer first lands the claim.
	if op.Kind == fs.OpCreate && op.Flags&uint32(syscall.O_CREAT) == 0 {
		if hit := b.liveHandleAt(op.Path); hit != nil {
			if attr, ok := hit.overlayAttr(); ok {
				if !writeIntent(op.Flags) {
					hit.addRef()
					return fs.Result{Attr: &attr, Handle: &claimReadHandle{claim: hit}}, nil
				}
				if ferr := hit.Flush(ctx); ferr != nil {
					// The handoff is aborted: the claim keeps its buffered
					// image (the acknowledged bytes' only copy) — erasing
					// it on a failed flush lost the write and left the
					// remote at its pre-claim content.
					return fs.Result{}, ferr
				}
				// A second writer now owns the file through the plane
				// below: retire the flushed claim entirely — leaving it
				// tracked kept its stale image answering lookups and
				// read-only re-opens over the second writer's landed
				// state (a new reader saw the old `hello` while the
				// backend already held `Yello`).
				hit.surrenderToBelow()
			}
		}
	}
	switch op.Kind {
	case fs.OpLookup, fs.OpGetAttr:
		// Same overlay role as a lazy create below: a live claimed file
		// resolves locally from its image until Release.
		if hit := b.liveHandleAt(op.Path); hit != nil {
			if attr, ok := hit.overlayAttr(); ok {
				return fs.Result{Attr: &attr}, nil
			}
		}
		return next(ctx, op)
	case fs.OpSetAttr:
		// A setattr on a live claim folds into its pending image — a size
		// change edits the image in place and stays uncommitted until the
		// boundary; sending it below would truncate or chmod the stale
		// remote state instead. A mode change is forwarded to the write
		// plane's pending handle directly.
		if h := b.resolveStreamHandle(op.Handle); h != nil {
			attr, err := h.applyAttr(ctx, op)
			if err != nil {
				return fs.Result{}, err
			}
			return fs.Result{Attr: &attr}, nil
		}
		if hit := b.liveHandleAt(op.Path); hit != nil {
			attr, err := hit.applyAttr(ctx, op)
			if err != nil {
				return fs.Result{}, err
			}
			return fs.Result{Attr: &attr}, nil
		}
		return next(ctx, op)
	case fs.OpUnlink, fs.OpRmdir:
		// Surrendered descriptors must bind their anonymous backing BEFORE
		// the name disappears: session() cannot re-open by path afterwards.
		b.prepareUnlinkSurvivors(ctx, op.Path, op.Kind == fs.OpRmdir)
		// A claim that owns its create has nothing remote to delete (the
		// boundary never ran); the op is forwarded anyway so the write
		// plane below drops its own pending state for the path, then the
		// claim's handles go anonymous.
		if _, statErr := b.cl.StatCtx(ctx, b.remotePath(op.Path)); statErr != nil {
			if !client.IsNotFound(statErr) {
				return fs.Result{}, statErr
			}
			res, err := next(ctx, op)
			if err == nil {
				// The plane below owned the name (a specials record, pending
				// writeback state): its success IS the answer. Overriding it
				// with ENOENT leaves the kernel's dentry live — a stale fifo
				// dentry then wedges mkdir (kernel-side EEXIST, no FUSE op)
				// and a later plain open blocks in fifo_open forever
				// (open/06 directory section).
				if op.Kind == fs.OpUnlink {
					b.markUnlinked(op.Path)
				} else {
					b.markUnlinkedUnder(op.Path)
				}
				return res, nil
			}
			if !fs.IsNotFound(err) {
				return res, err
			}
			if !b.hasLiveHandleAt(op.Path, op.Kind == fs.OpRmdir) {
				return fs.Result{}, fs.ErrNotFound
			}
			if op.Kind == fs.OpUnlink {
				b.markUnlinked(op.Path)
			} else {
				b.markUnlinkedUnder(op.Path)
			}
			return fs.Result{}, nil
		}
		res, err := next(ctx, op)
		if err != nil {
			return res, err
		}
		if op.Kind == fs.OpUnlink {
			b.markUnlinked(op.Path)
		} else {
			b.markUnlinkedUnder(op.Path)
		}
		return res, nil
	case fs.OpRename:
		// Same lazy rule as unlink: a never-committed claim renames only
		// its in-memory image (the plane below renames its own pending
		// state for the path).
		if _, statErr := b.cl.StatCtx(ctx, b.remotePath(op.Path)); statErr != nil {
			if !client.IsNotFound(statErr) {
				return fs.Result{}, statErr
			}
			res, err := next(ctx, op)
			if err == nil {
				// Same rule as unlink: the below plane's success owns the
				// answer — never mask it with ENOENT.
				b.retarget(ctx, op.Path, op.Path2)
				return res, nil
			}
			if !fs.IsNotFound(err) {
				return res, err
			}
			if !b.hasLiveHandleAt(op.Path, false) {
				return fs.Result{}, fs.ErrNotFound
			}
			b.retarget(ctx, op.Path, op.Path2)
			return fs.Result{}, nil
		}
		res, err := next(ctx, op)
		if err != nil {
			return res, err
		}
		b.retarget(ctx, op.Path, op.Path2)
		return res, nil
	}
	return next(ctx, op)
}

// hasLiveHandleAt reports whether a live (unreleased, un-anonymized) claim
// owns p exactly, or — for a removed directory — under p.
func (b *Bulkfile) hasLiveHandleAt(p string, under bool) bool {
	prefix := strings.TrimSuffix(p, "/") + "/"
	b.mu.Lock()
	defer b.mu.Unlock()
	for h := range b.live {
		h.mu.Lock()
		match := !h.released && !h.unlinked &&
			(h.path == p || (under && strings.HasPrefix(h.path, prefix)))
		h.mu.Unlock()
		if match {
			return true
		}
	}
	return false
}

// liveHandleAt returns a live claim that owns p exactly, if any.
func (b *Bulkfile) liveHandleAt(p string) *streamHandle {
	b.mu.Lock()
	defer b.mu.Unlock()
	for h := range b.live {
		h.mu.Lock()
		match := !h.released && !h.unlinked && h.path == p
		h.mu.Unlock()
		if match {
			return h
		}
	}
	return nil
}

// prepareUnlinkSurvivors opens a below session for every SURRENDERED
// (passthrough) descriptor at p (or under it) BEFORE the delete runs: once
// the name is gone, session() cannot re-open by path, and the fd's
// anonymous read/write backing must already be bound. The below plane's
// unlink marks its own open handles anonymous, so this session survives the
// deletion.
func (b *Bulkfile) prepareUnlinkSurvivors(ctx context.Context, p string, under bool) {
	prefix := strings.TrimSuffix(p, "/") + "/"
	var pending []*streamHandle
	b.mu.Lock()
	for h := range b.fds {
		h.mu.Lock()
		path := h.path
		match := !h.released && (path == p || (under && strings.HasPrefix(path, prefix)))
		passthrough := h.passthrough && h.inner == nil
		h.mu.Unlock()
		if match && passthrough {
			pending = append(pending, h)
		}
	}
	b.mu.Unlock()
	for _, h := range pending {
		// Bind the anonymous backing: the below plane's session is opened
		// while the name still exists, then the handle becomes a local
		// survivor over a bounded snapshot — an unlinked fd's reads and
		// writes must neither re-open the deleted name nor hit the below
		// plane's post-delete path resolution. Files beyond the buffered
		// bound keep the session (best effort).
		inner, err := h.session(ctx)
		if err != nil || inner == nil {
			continue
		}
		st, serr := b.cl.StatCtx(ctx, b.remotePath(h.path))
		if serr != nil || st == nil || st.Size > bulkfileMaxBufferedBytes {
			continue
		}
		snapshot := make([]byte, st.Size)
		if n, rerr := inner.Read(ctx, 0, snapshot); rerr != nil || int64(n) != st.Size {
			continue
		}
		_ = inner.Release(ctx)
		h.mu.Lock()
		h.image = snapshot
		h.pushed = int64(len(snapshot))
		h.dirty = false
		h.passthrough = false
		h.inner = nil
		h.mu.Unlock()
	}
}

// markUnlinked flags every live handle at p anonymous: the fd keeps serving
// reads from the image, but the boundary push must not resurrect the
// path. Per-handle, so a re-created p stays committable.
func (b *Bulkfile) markUnlinked(p string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for h := range b.fds {
		h.mu.Lock()
		if h.path == p && !h.released {
			h.unlinked = true
		}
		h.mu.Unlock()
	}
}

// markUnlinkedUnder flags every live handle under a removed directory
// anonymous.
func (b *Bulkfile) markUnlinkedUnder(dir string) {
	prefix := strings.TrimSuffix(dir, "/") + "/"
	b.mu.Lock()
	defer b.mu.Unlock()
	for h := range b.fds {
		h.mu.Lock()
		if strings.HasPrefix(h.path, prefix) && !h.released {
			h.unlinked = true
		}
		h.mu.Unlock()
	}
}

// retarget moves live handles onto the renamed path, including handles
// nested under a renamed directory. The below sessions are detached and
// released: the plane has already re-keyed its own state for the renamed
// path (the rename op was forwarded first), and the next boundary opens a
// fresh session there.
func (b *Bulkfile) retarget(ctx context.Context, oldPath, newPath string) {
	prefix := strings.TrimSuffix(oldPath, "/") + "/"
	var detached []fs.Handle
	b.mu.Lock()
	for h := range b.fds {
		h.mu.Lock()
		switch {
		case h.path == oldPath:
			h.path = newPath
		case strings.HasPrefix(h.path, prefix):
			h.path = newPath + strings.TrimPrefix(h.path, oldPath)
		default:
			h.mu.Unlock()
			continue
		}
		if h.inner != nil {
			detached = append(detached, h.inner)
			h.inner = nil
		}
		h.mu.Unlock()
	}
	b.mu.Unlock()
	for _, inner := range detached {
		_ = inner.Release(ctx)
	}
}

func (b *Bulkfile) remotePath(p string) string {
	if b.root == "/" {
		return p
	}
	if p == "/" {
		return b.root
	}
	return b.root + p
}

func (b *Bulkfile) track(h *streamHandle) {
	b.mu.Lock()
	if b.live == nil {
		b.live = make(map[*streamHandle]struct{})
	}
	if b.fds == nil {
		b.fds = make(map[*streamHandle]struct{})
	}
	b.live[h] = struct{}{}
	b.fds[h] = struct{}{}
	b.mu.Unlock()
}

func (b *Bulkfile) untrack(h *streamHandle) {
	b.mu.Lock()
	delete(b.live, h)
	delete(b.fds, h)
	b.mu.Unlock()
}

// untrackClaim retires the pathname CLAIM only: the descriptor stays in the
// namespace-tracking registry so a rename still follows it.
func (b *Bulkfile) untrackClaim(h *streamHandle) {
	b.mu.Lock()
	delete(b.live, h)
	b.mu.Unlock()
}

// writeIntent reports whether the open flags carry write access.
func writeIntent(flags uint32) bool {
	return flags&(uint32(syscall.O_WRONLY)|uint32(syscall.O_RDWR)) != 0
}
