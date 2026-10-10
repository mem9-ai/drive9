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

	mu       sync.Mutex
	image    []byte
	mode     uint32
	pushed   int64 // image size already expressed below; a shorter image needs a truncation
	dirty    bool  // image differs from the state expressed below
	unlinked bool  // directory entry gone; boundary pushes are discarded
	released bool
}

// applyAttr folds a setattr into the pending image: a size change edits the
// image in place (the truncation stays local until the boundary pushes
// it), a mode change is forwarded to the write plane's pending handle.
func (h *streamHandle) applyAttr(ctx context.Context, op *fs.Op) (fs.Attr, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if op.Mode != 0 {
		mode := op.Mode & 0o777
		h.mode = mode
		h.mu.Unlock()
		err := h.below.SetAttr(ctx, h.path, fs.AttrPatch{Mode: &mode})
		h.mu.Lock()
		if err != nil {
			return fs.Attr{}, err
		}
	}
	if op.Size >= 0 && op.Size != int64(len(h.image)) {
		size := op.Size
		if size < int64(len(h.image)) {
			h.image = h.image[:size]
		} else {
			grown := make([]byte, size)
			copy(grown, h.image)
			h.image = grown
		}
		h.dirty = true
	}
	return h.attrLocked(), nil
}

func (h *streamHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
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
	defer h.mu.Unlock()
	if h.released {
		// Unreachable per the kernel contract (RELEASE is the last op on
		// an fh); refuse rather than drop bytes.
		return syscall.EBADF
	}
	if len(data) == 0 {
		return nil
	}
	end := off + int64(len(data))
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
	return nil
}

func (h *streamHandle) Sync(ctx context.Context) error {
	if err := h.push(ctx); err != nil {
		return err
	}
	inner, err := h.session(ctx)
	if err != nil {
		return err
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
	return inner.Flush(ctx)
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
	if release {
		h.b.untrack(h)
	}
	// The boundary push happens before the inner release so a fresh create
	// materializes the image (never an empty file) and a detached session
	// is re-opened first; failures are reported after the release so no
	// descriptor is leaked either way.
	pushErr := h.push(ctx)
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
	if mode == 0 {
		mode = 0o644
	}
	return fs.Attr{Kind: fs.KindFile, Mode: mode, Size: int64(len(h.image))}
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
