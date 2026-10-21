package bulkfile

import (
	"context"
	"sync"
	"syscall"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// streamHandle buffers the full claimed image and pushes it into the write
// plane below at every durability boundary.
type streamHandle struct {
	b         *Bulkfile
	path      string
	inner     fs.Handle
	below     fs.Filesystem
	openFlags uint32 // claim flags for re-opening the session below after a rename

	mu          sync.Mutex
	image       []byte
	mode        uint32
	pushed      int64 // image size already expressed below; a shorter image needs a truncation
	dirty       bool  // image differs from the state expressed below
	unlinked    bool  // directory entry gone; boundary pushes are discarded
	released    bool
	passthrough bool // image overflowed the bound; the stream forwards to the plane below
	// modeKnown distinguishes an explicit chmod(000) from "no mode
	// recorded": attrLocked's 0644 default must not answer a 000 mode the
	// caller just set.
	modeKnown bool
}

// bulkfileMaxBufferedBytes bounds the claimed image retained in memory
// before the claim hands the stream to the write plane below (whose own
// buffering spills to parts/shadow files). A write ending beyond the bound
// — a sparse hole or a sequential stream growing past it — forwards
// instead of allocating the full logical image (one byte at 64 MiB must
// not allocate 64 MiB).
const bulkfileMaxBufferedBytes = 8 << 20

// applyAttr folds a setattr into the pending image: a size change edits the
// image in place (the truncation stays local until the boundary pushes
// it), a mode change is forwarded to the write plane's pending handle.
func (h *streamHandle) applyAttr(ctx context.Context, op *fs.Op) (fs.Attr, error) {
	if op.HasMode {
		mode := op.Mode & 0o777
		h.mu.Lock()
		h.mode = mode
		h.modeKnown = true
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
	if op.Size >= 0 && op.Size != int64(len(h.image)) {
		size := op.Size
		h.mu.Lock()
		if size < int64(len(h.image)) {
			h.image = h.image[:size]
			h.dirty = true
			h.mu.Unlock()
		} else if size > bulkfileMaxBufferedBytes {
			// A zero-fill grow past the bound must not allocate the hole:
			// land the buffered prefix and express the truncation in the
			// plane below (its SetAttr holds the size without the bytes).
			h.mu.Unlock()
			if err := h.push(ctx); err != nil {
				return fs.Attr{}, err
			}
			if err := h.below.SetAttr(ctx, h.path, fs.AttrPatch{Size: &size}); err != nil {
				return fs.Attr{}, err
			}
			h.mu.Lock()
			h.passthrough = true
			h.image = nil
			h.pushed = size
			h.dirty = false
			attr := h.attrLocked()
			h.mu.Unlock()
			// Registry removal takes b.mu while every lookup takes
			// (b.mu, h.mu) in that order — dropping h.mu first keeps one
			// lock order (deadlock: a lookup racing this transition).
			// Retire only the CLAIM: the descriptor is still open and a
			// later rename must follow it.
			h.b.untrackClaim(h)
			return attr, nil
		} else {
			grown := make([]byte, size)
			copy(grown, h.image)
			h.image = grown
			h.dirty = true
			h.mu.Unlock()
		}
	}
	h.mu.Lock()
	attr := h.attrLocked()
	h.mu.Unlock()
	return attr, nil
}

// Anonymous propagates the inner handle's anonymous identity through the
// bulk claim wrapper.
func (h *streamHandle) Anonymous() bool {
	h.mu.Lock()
	inner := h.inner
	h.mu.Unlock()
	if a, ok := inner.(interface{ Anonymous() bool }); ok {
		return a.Anonymous()
	}
	return false
}

func (h *streamHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	h.mu.Lock()
	passthrough := h.passthrough
	h.mu.Unlock()
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

func (h *streamHandle) Write(ctx context.Context, off int64, data []byte) error {
	h.mu.Lock()
	if h.released {
		// Unreachable per the kernel contract (RELEASE is the last op on
		// an fh); refuse rather than drop bytes.
		h.mu.Unlock()
		return syscall.EBADF
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
	// An unlinked claim has nowhere to forward: the below plane holds no
	// entry, so the stream stays buffered regardless of the bound.
	if h.unlinked || end <= bulkfileMaxBufferedBytes {
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
	h.image = nil // the bytes now live in the plane below
	h.mu.Unlock()
	// The claim no longer owns the freshest bytes: stop serving the
	// path's lookups from this handle so new opens read the plane's
	// pending state instead of a stale image. The DESCRIPTOR stays
	// tracked: a suffix write must follow a rename.
	h.b.untrackClaim(h)
	inner, err := h.session(ctx)
	if err != nil {
		return err
	}
	return inner.Write(ctx, off, data)
}

func (h *streamHandle) Sync(ctx context.Context) error {
	if err := h.push(ctx); err != nil {
		return err
	}
	inner, err := h.session(ctx)
	if err != nil {
		return err
	}
	if inner == nil {
		// An anonymous survivor has no below session: its buffered image
		// is discarded with the name, so sync is local-only.
		return nil
	}
	return inner.Sync(ctx)
}

func (h *streamHandle) Flush(ctx context.Context) error {
	if err := h.push(ctx); err != nil {
		return err
	}
	inner, err := h.session(ctx)
	if err != nil {
		return err
	}
	if inner == nil {
		return nil
	}
	return inner.Flush(ctx)
}

// surrenderToBelow retires a flushed claim whose file another writer
// now owns through the lower plane: the claim stops answering lookups
// and overlays (its image is published), so every later open reads the
// shared lower state. Live descriptors on this claim switch to
// passthrough — the flushed image lives below now, so their reads and
// appends address the shared lower state instead of an erased buffer.
func (h *streamHandle) surrenderToBelow() {
	h.mu.Lock()
	h.passthrough = true
	h.image = nil
	h.dirty = false
	h.inner = nil // re-opened lazily at the CURRENT shared state
	h.pushed = 0
	h.mu.Unlock()
	// The flushed write-plane handle below keeps serving its own landed
	// buffer (read-your-writes prefers it), which would answer the
	// original descriptor's reads with the pre-handoff image forever.
	// Dropping the session makes every later read/write open a fresh
	// handle against the shared state the second writer now owns.
	// Retire the pathname CLAIM only: this descriptor is still open, and a
	// later rename must retarget it (untracking it here left the fd
	// re-opening the OLD name — a surrendered fd recreated /a with its
	// O_CREAT flags after /a had moved to /b).
	h.b.untrackClaim(h)
}

// session returns the below handle, opening it at the current path when a
// rename detached the previous one; an unlinked claim needs no session.
func (h *streamHandle) session(ctx context.Context) (fs.Handle, error) {
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
// boundary — or Release — retries. A rename detaches the below session;
// it is re-opened at the handle's current path here, binding to whatever
// state the plane re-keyed for that path.
func (h *streamHandle) push(ctx context.Context) error {
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
	return nil
}

func (h *streamHandle) Release(ctx context.Context) error {
	h.mu.Lock()
	release := !h.released
	h.released = true
	h.mu.Unlock()
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
		h.b.untrack(h)
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

// attrLocked renders the handle's reply attributes; callers hold h.mu.
func (h *streamHandle) attrLocked() fs.Attr {
	mode := h.mode
	if !h.modeKnown {
		mode = 0o644
	}
	size := int64(len(h.image))
	if h.passthrough && h.image == nil {
		// The stream lives in the plane below now: the authoritative size
		// is the pushed one (the emptied image would answer 0, and the
		// facade copies that into the inode and the SETATTR reply).
		size = h.pushed
	}
	return fs.Attr{Kind: fs.KindFile, Mode: mode, Size: size}
}

// overlayAttr renders the lookup reply for a live claim: the buffered image
// is the file until Release. Reported as not-claimed once the handle went
// anonymous or finished.
func (h *streamHandle) overlayAttr() (fs.Attr, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.unlinked || h.released {
		return fs.Attr{}, false
	}
	return h.attrLocked(), true
}

// claimReadHandle serves reads of a live claim's pending image (a
// read-only re-open before the boundary push).
type claimReadHandle struct {
	claim *streamHandle
}

func (c *claimReadHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	c.claim.mu.Lock()
	image := c.claim.image
	passthrough := c.claim.passthrough
	c.claim.mu.Unlock()
	if passthrough {
		// The claim handed its stream to the plane below (the bounded
		// forward); the pending bytes now live there, and the emptied
		// image would answer (0, nil) forever.
		return c.claim.Read(ctx, off, dest)
	}
	if off < 0 || off >= int64(len(image)) {
		return 0, nil
	}
	return copy(dest, image[off:]), nil
}

func (c *claimReadHandle) Write(ctx context.Context, off int64, data []byte) error {
	return syscall.EROFS
}

func (c *claimReadHandle) Sync(ctx context.Context) error    { return nil }
func (c *claimReadHandle) Flush(ctx context.Context) error   { return nil }
func (c *claimReadHandle) Release(ctx context.Context) error { return nil }
