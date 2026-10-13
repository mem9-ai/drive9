// The staging handle: the large-file takeover's claim on a fresh
// truncate-writer. The image is buffered in memory (read-your-writes, no
// network during Write) while it stays below the promotion threshold; the
// durability boundaries — Flush, Fsync, Release — push it into the write
// plane below and delegate to it. Crossing the threshold promotes the
// claim onto the extent plane: the below session is discarded (its lazy
// create is unlinked so its Release cannot materialize an empty classic
// file next to the extent projection), the buffered image is created and
// written through the JuiceFS VFS in one step, and every later operation
// serves through the promoted extent handle. A promotion that cannot
// build the runtime falls back to the classic plane for good — the claim
// behaves exactly like a pure buffering claim from then on.
package extent

import (
	"context"
	"sync"
	"syscall"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// stagingHandle buffers a claimed image and promotes it to the extent
// plane at the threshold, pushing it into the write plane below at every
// durability boundary otherwise.
//
// Locking: h.mu guards every field and stays held across the network work
// of a boundary push or a promotion (established practice — push already
// does it), so a promotion observes a stable image. The path router never
// nests h.mu under its own lock: its lookups snapshot the candidate
// handles first and probe them after releasing, keeping the order strictly
// h.mu → router.mu.
type stagingHandle struct {
	e         *Extent
	path      string
	caller    fs.Caller
	below     fs.Filesystem
	inner     fs.Handle
	openFlags uint32 // claim flags for re-opening the session below after a rename

	mu     sync.Mutex
	image  []byte
	mode   uint32
	pushed int64 // image size already expressed below; a shorter image needs a truncation
	dirty  bool  // image differs from the state expressed below
	// promoted serves every operation once the claim moved to the extent
	// plane; image is released with the swap.
	promoted *extentHandle
	// promoting single-flights the promotion against writers that cross
	// the threshold while the move is between its phases.
	promoting bool
	// passthrough marks an image that overflowed the memory bound: the
	// buffered prefix landed below and the stream forwards to the plane
	// under it (whose own buffering spills to parts/shadow files).
	passthrough bool
	// promotable marks a fresh (not-yet-materialized) claim eligible for
	// the extent plane; it clears on the first classic push (the file
	// exists classically from then on), on promotion failure, and for
	// claims on paths that already existed with the classic layout — an
	// existing layout never converts.
	promotable bool

	unlinked bool // directory entry gone; boundary pushes are discarded
	released bool
}

// claimLive reports whether the claim still owns its name and has not
// promoted or released; the router probes it without holding its lock.
func (h *stagingHandle) claimLive() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return !h.released && !h.unlinked && h.promoted == nil && !h.passthrough
}

// markUnlinked flags the claim anonymous: the fd keeps serving reads from
// the image, but the boundary push must not resurrect the path.
func (h *stagingHandle) markUnlinked() {
	h.mu.Lock()
	h.unlinked = true
	h.mu.Unlock()
}

// retarget moves the claim onto a renamed path and detaches the below
// session: the plane below has re-keyed its own state for the renamed
// path, and the next boundary opens a fresh session there.
func (h *stagingHandle) retarget(ctx context.Context, newPath string) {
	h.mu.Lock()
	h.path = newPath
	inner := h.inner
	h.inner = nil
	h.mu.Unlock()
	if inner != nil {
		// The release can stage or commit through network I/O: it rides
		// the renaming request's context (its cancellation applies, and a
		// stalled backend cannot wedge the rename forever).
		_ = inner.Release(ctx)
	}
}

// applyAttr folds a setattr into the pending image (or the promoted
// inode): a size change edits the image in place — or truncates the VFS
// inode after promotion — and a mode change is forwarded to the write
// plane's pending state.
func (h *stagingHandle) applyAttr(ctx context.Context, op *fs.Op) (fs.Attr, error) {
	h.mu.Lock()
	promoted := h.promoted
	h.mu.Unlock()
	if promoted != nil {
		if op.Size >= 0 {
			if err := h.e.truncateIno(ctx, h.path, uint64(promoted.ino), op.Size); err != nil {
				return fs.Attr{}, err
			}
		}
		// The promoted inode is the attribute authority: mode, owner and
		// times apply through the VFS in one call (the session below was
		// discarded at promotion, and the classic chmod would not touch the
		// jfs inode the stats read).
		if op.HasMode || op.HasSetUID || op.HasSetGID || !op.Mtime.IsZero() {
			res, err := h.e.setAttrViaVFS(ctx, op, uint64(promoted.ino))
			if err != nil {
				return fs.Attr{}, err
			}
			if res.Attr != nil {
				return *res.Attr, nil
			}
		}
		return h.e.promotedAttr(ctx, promoted)
	}
	if op.HasMode {
		mode := op.Mode & 0o777
		h.mu.Lock()
		h.mode = mode
		h.mu.Unlock()
		if err := h.below.SetAttr(ctx, h.path, fs.AttrPatch{Mode: &mode}); err != nil {
			return fs.Attr{}, err
		}
	}
	h.mu.Lock()
	passthrough := h.passthrough
	h.mu.Unlock()
	if passthrough {
		// The stream lives in the plane below now; the emptied image is
		// not the file. Delegate size changes there (a truncate against
		// len(nil) would compare equal to 0 and skip the shrink).
		if op.Size >= 0 {
			size := op.Size
			if err := h.below.SetAttr(ctx, h.path, fs.AttrPatch{Size: &size}); err != nil {
				return fs.Attr{}, err
			}
			h.mu.Lock()
			h.pushed = size
			a := h.attrLocked()
			h.mu.Unlock()
			return a, nil
		}
		h.mu.Lock()
		a := h.attrLocked()
		h.mu.Unlock()
		return a, nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if op.Size >= 0 && op.Size != int64(len(h.image)) {
		size := op.Size
		if size < int64(len(h.image)) {
			h.image = h.image[:size]
			h.dirty = true
		} else if size > h.memBoundLocked() {
			// A zero-fill grow past the bound must not allocate the hole:
			// land the buffered prefix and express the truncation in the
			// plane below (its SetAttr holds the size without the bytes).
			// The promotion window is gone — the claim parks classically.
			h.mu.Unlock()
			if err := h.push(ctx); err != nil {
				h.mu.Lock()
				return fs.Attr{}, err
			}
			if err := h.below.SetAttr(ctx, h.path, fs.AttrPatch{Size: &size}); err != nil {
				h.mu.Lock()
				return fs.Attr{}, err
			}
			h.mu.Lock()
			h.passthrough = true
			h.promotable = false
			h.image = nil
			h.pushed = size
			h.dirty = false
			h.e.paths.untrackStaging(h)
			return h.attrLocked(), nil
		} else {
			grown := make([]byte, size)
			copy(grown, h.image)
			h.image = grown
			h.dirty = true
		}
	}
	return h.attrLocked(), nil
}

// stagingMaxBufferedBytes bounds the claimed image retained in memory
// before the claim hands the stream to the plane below (whose own
// buffering spills to parts/shadow files) — or, for a promotable claim,
// before the takeover threshold takes over as the effective bound. A write
// ending far beyond must not allocate the full logical image (one byte at
// 64 MiB must not allocate 64 MiB).
const stagingMaxBufferedBytes = 8 << 20

// memBoundLocked is the image bound while the caller holds h.mu: the
// takeover threshold when it sits above the generic bound (promotion
// replaces the forward), the generic bound otherwise.
func (h *stagingHandle) memBoundLocked() int64 {
	if h.promotable && h.e.opts.LargeFileThreshold > stagingMaxBufferedBytes {
		return h.e.opts.LargeFileThreshold
	}
	return stagingMaxBufferedBytes
}

func (h *stagingHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	h.mu.Lock()
	promoted := h.promoted
	passthrough := h.passthrough
	h.mu.Unlock()
	if promoted != nil {
		return promoted.Read(ctx, off, dest)
	}
	if passthrough {
		inner, err := h.session(ctx)
		if err != nil {
			return 0, err
		}
		return inner.Read(ctx, off, dest)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if off >= int64(len(h.image)) {
		return 0, nil
	}
	n := copy(dest, h.image[off:])
	return n, nil
}

// readAll serves a path-addressed read from the buffered image (limit<=0
// reads to the end of the image).
func (h *stagingHandle) readAll(ctx context.Context, off, limit int64) ([]byte, error) {
	h.mu.Lock()
	promoted := h.promoted
	h.mu.Unlock()
	if promoted != nil {
		return h.e.readViaVFS(ctx, h.path, uint64(promoted.ino), off, limit)
	}
	h.mu.Lock()
	passthrough := h.passthrough
	h.mu.Unlock()
	if passthrough {
		inner, err := h.session(ctx)
		if err != nil {
			return nil, err
		}
		if limit <= 0 {
			return nil, nil
		}
		buf := make([]byte, limit)
		n, rerr := inner.Read(ctx, off, buf)
		if rerr != nil {
			return nil, rerr
		}
		return buf[:n], nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if off >= int64(len(h.image)) {
		return nil, nil
	}
	end := int64(len(h.image))
	if limit > 0 && off+limit < end {
		end = off + limit
	}
	data := make([]byte, end-off)
	copy(data, h.image[off:end])
	return data, nil
}

func (h *stagingHandle) Write(ctx context.Context, off int64, data []byte) error {
	h.mu.Lock()
	if h.released {
		// Unreachable per the kernel contract (RELEASE is the last op on
		// an fh); refuse rather than drop bytes.
		h.mu.Unlock()
		return syscall.EBADF
	}
	if h.promoted != nil {
		promoted := h.promoted
		h.mu.Unlock()
		return promoted.Write(ctx, off, data)
	}
	if len(data) == 0 {
		h.mu.Unlock()
		return nil
	}
	end := off + int64(len(data))
	if h.passthrough {
		h.mu.Unlock()
		inner, err := h.session(ctx)
		if err != nil {
			return err
		}
		return inner.Write(ctx, off, data)
	}
	// A write that crosses the takeover threshold takes the promotion (the
	// crossing write itself may exceed the threshold by one chunk); a
	// write landing far beyond it is a sparse jump — the promotion window
	// is gone and the claim forwards below instead of allocating the hole.
	threshold := h.e.opts.LargeFileThreshold
	crossing := h.promotable && !h.promoting && end >= threshold &&
		end <= threshold+stagingMaxBufferedBytes
	if crossing || h.unlinked || end <= h.memBoundLocked() {
		switch {
		case off == int64(len(h.image)):
			h.image = append(h.image, data...)
		case off > int64(len(h.image)):
			grown := make([]byte, end)
			copy(grown, h.image)
			copy(grown[off:], data)
			h.image = grown
		default:
			if int64(len(h.image)) < end {
				grown := make([]byte, end)
				copy(grown, h.image)
				h.image = grown
			}
			copy(h.image[off:], data)
		}
		h.dirty = true
		promote := crossing && int64(len(h.image)) >= threshold
		if promote {
			// The promotion runs under h.mu (see the struct comment);
			// release it here only to re-acquire inside.
			h.mu.Unlock()
			return h.promote(ctx)
		}
		h.mu.Unlock()
		return nil
	}
	h.mu.Unlock()
	// The image would exceed the memory bound: land what is buffered and
	// hand the stream to the write plane below — its buffering spills to
	// parts/shadow files instead of one slice. Failures keep the claim
	// dirty so the next boundary retries the landing.
	if err := h.push(ctx); err != nil {
		return err
	}
	h.mu.Lock()
	h.passthrough = true
	h.promotable = false
	h.image = nil // the bytes now live in the plane below
	h.mu.Unlock()
	// The claim no longer owns the freshest bytes: stop serving the
	// path's lookups from this handle so new opens read the plane's
	// pending state instead of a stale image.
	h.e.paths.untrackStaging(h)
	inner, err := h.session(ctx)
	if err != nil {
		return err
	}
	return inner.Write(ctx, off, data)
}

func (h *stagingHandle) Sync(ctx context.Context) error {
	h.mu.Lock()
	promoted := h.promoted
	h.mu.Unlock()
	if promoted != nil {
		return promoted.Sync(ctx)
	}
	if err := h.push(ctx); err != nil {
		return err
	}
	inner, err := h.session(ctx)
	if err != nil {
		return err
	}
	// Passthrough claims: the push no-ops (dirty was cleared at the
	// forward) and the inner session carries the boundary.
	return inner.Sync(ctx)
}

func (h *stagingHandle) Flush(ctx context.Context) error {
	h.mu.Lock()
	promoted := h.promoted
	h.mu.Unlock()
	if promoted != nil {
		return promoted.Flush(ctx)
	}
	if err := h.push(ctx); err != nil {
		return err
	}
	inner, err := h.session(ctx)
	if err != nil {
		return err
	}
	// Passthrough claims: same shape — the push no-ops and the inner
	// session carries the durability boundary.
	return inner.Flush(ctx)
}

func (h *stagingHandle) Release(ctx context.Context) error {
	h.mu.Lock()
	release := !h.released
	h.released = true
	promoted := h.promoted
	h.mu.Unlock()
	if promoted != nil {
		if release {
			h.e.paths.untrackStaging(h)
		}
		return promoted.Release(ctx)
	}
	// The boundary push happens before the inner release so a fresh create
	// materializes the image (never an empty file) and a detached session
	// is re-opened first; failures are reported after the release so no
	// descriptor is leaked either way.
	//
	// The claim stays tracked until the push below has staged the image:
	// untracking first would leave a window where a re-open reads the
	// stale remote rows while the freshest bytes exist only on this
	// handle (read-your-writes across close).
	pushErr := h.push(ctx)
	if release {
		h.e.paths.untrackStaging(h)
	}
	inner, serr := h.session(ctx)
	if serr != nil && pushErr == nil {
		pushErr = serr
	}
	h.mu.Lock()
	h.inner = nil
	h.mu.Unlock()
	if inner != nil {
		if err := inner.Release(ctx); err != nil && pushErr == nil {
			pushErr = err
		}
	}
	return pushErr
}

// session returns the below handle, opening it at the current path when a
// rename detached the previous one; an unlinked claim needs no session.
func (h *stagingHandle) session(ctx context.Context) (fs.Handle, error) {
	h.mu.Lock()
	inner := h.inner
	if inner != nil || h.unlinked {
		h.mu.Unlock()
		return inner, nil
	}
	h.mu.Unlock()
	inner, err := h.below.Open(ctx, h.path, fs.OpenFlags(h.openFlags))
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	if h.inner == nil && !h.released {
		h.inner = inner
	}
	h.mu.Unlock()
	return inner, nil
}

// push expresses the current image in the write plane below: a truncation
// first (a shorter write cannot shrink the plane's buffered state), then
// the full image as one write. Failures leave the handle dirty so the next
// boundary — or Release — retries. The first push settles the claim on
// the classic plane: it stops being promotable from then on.
func (h *stagingHandle) push(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.dirty {
		return nil
	}
	if h.unlinked {
		return nil
	}
	if h.inner == nil {
		h.mu.Unlock()
		inner, err := h.below.Open(ctx, h.path, fs.OpenFlags(h.openFlags))
		h.mu.Lock()
		if err != nil {
			return err
		}
		h.inner = inner
	}
	if int64(len(h.image)) < h.pushed {
		size := int64(len(h.image))
		h.mu.Unlock()
		err := h.below.SetAttr(ctx, h.path, fs.AttrPatch{Size: &size})
		h.mu.Lock()
		if err != nil {
			return err
		}
	}
	if len(h.image) > 0 {
		if err := h.inner.Write(ctx, 0, h.image); err != nil {
			return err
		}
	}
	h.pushed = int64(len(h.image))
	h.dirty = false
	h.promotable = false
	return nil
}

// promote moves the claim onto the extent plane, holding h.mu across the
// whole move so no other operation can take a classic path mid-transition
// (a concurrent Write or Flush would otherwise push a classic copy beside
// the extent projection). The below session is discarded first — the
// forwarded unlink removes the write plane's pending state for the path,
// so releasing the session cannot materialize an empty classic file beside
// the projection — then the buffered image is created and written through
// the VFS in one move.
//
// Once the extent inode exists the path is projected as content_layout=extent
// and the classic plane refuses whole-object writes to it, so a failure
// after the create no longer parks the claim on the classic plane: the
// inode is remembered for discovery and the error propagates (the write
// that carried it reports EIO while the file stays on the extent plane).
func (h *stagingHandle) promote(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.promoted != nil || !h.promotable || h.promoting || h.unlinked || h.released {
		return nil
	}
	if err := h.e.beginPromotion(); err != nil {
		h.promotable = false
		return nil
	}
	h.promoting = true
	defer func() { h.promoting = false }()
	if err := h.below.Unlink(ctx, h.path); err != nil {
		h.promotable = false
		return nil
	}
	inner := h.inner
	h.inner = nil
	if inner != nil {
		_ = inner.Release(ctx)
	}
	openFlags := h.openFlags &^ createFlagMask
	ino, errn := h.e.createInVFS(ctx, &fs.Op{Path: h.path, Mode: h.mode, HasMode: true, Flags: openFlags, Caller: h.caller})
	if errn != 0 || ino == 0 {
		h.promotable = false
		return nil
	}
	h.e.paths.rememberIno(h.path, ino)
	handle, _, _, oerr := h.e.openExtent(ctx, h.path, ino, openFlags, h.caller)
	if oerr != nil || handle == nil {
		h.promotable = false
		if oerr != nil {
			return oerr
		}
		return syscall.EIO
	}
	if len(h.image) > 0 {
		if err := handle.Write(ctx, 0, h.image); err != nil {
			_ = handle.Release(ctx)
			h.promotable = false
			return err
		}
	}
	h.promoted = handle
	h.image = nil
	h.dirty = false
	h.promotable = false
	return nil
}

// createFlagMask strips the create/exclusive/truncate flags a staging claim
// carries before its image is opened on the extent plane (a typed constant:
// the flag bits are constants and the mask never narrows a runtime value).
const createFlagMask uint32 = syscall.O_CREAT | syscall.O_EXCL | syscall.O_TRUNC

// attrLocked renders the handle's reply attributes; callers hold h.mu.
func (h *stagingHandle) attrLocked() fs.Attr {
	mode := h.mode
	if mode == 0 {
		mode = 0o644
	}
	return fs.Attr{Kind: fs.KindFile, Mode: mode, Size: int64(len(h.image))}
}

// overlayAttr renders the lookup reply for a live claim: the buffered image
// is the file until it promotes or releases.
func (h *stagingHandle) overlayAttr() (fs.Attr, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.unlinked || h.released || h.promoted != nil || h.passthrough {
		return fs.Attr{}, false
	}
	return h.attrLocked(), true
}

// claimReadHandle serves reads of a live claim's pending image (a
// read-only re-open before the boundary push).
type claimReadHandle struct {
	claim *stagingHandle
}

func (c *claimReadHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	c.claim.mu.Lock()
	if c.claim.passthrough || c.claim.promoted != nil {
		// The claim handed its stream to the plane below (the bounded
		// forward) or onto the extent plane (promotion): the pending bytes
		// now live there, and the emptied image would answer (0, nil)
		// forever. Delegation runs without the claim lock — claim.Read
		// takes it itself.
		c.claim.mu.Unlock()
		return c.claim.Read(ctx, off, dest)
	}
	defer c.claim.mu.Unlock()
	if off < 0 || off >= int64(len(c.claim.image)) {
		return 0, nil
	}
	return copy(dest, c.claim.image[off:]), nil
}

// UnwrapHandle exposes the promoted extent handle for the facade's
// capability probing: after promotion the descriptor's POSIX locks belong
// to the JuiceFS VFS, not the mount-local table. Nil (nothing to unwrap)
// keeps the buffered claim on the local table.
func (h *stagingHandle) UnwrapHandle() fs.Handle {
	h.mu.Lock()
	promoted := h.promoted
	h.mu.Unlock()
	if promoted == nil {
		// A typed nil would defeat the facade's nil check and hand its
		// capability probes a nil extent handle.
		return nil
	}
	return promoted
}

func (c *claimReadHandle) Write(ctx context.Context, off int64, data []byte) error {
	return syscall.EROFS
}

func (c *claimReadHandle) Sync(ctx context.Context) error    { return nil }
func (c *claimReadHandle) Flush(ctx context.Context) error   { return nil }
func (c *claimReadHandle) Release(ctx context.Context) error { return nil }
