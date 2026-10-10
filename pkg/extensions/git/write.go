// The workspace write plane: claimed files buffer locally and land as one
// overlay entry per durability boundary instead of one HTTP round trip
// per write.
package git

import (
	"context"
	"fmt"
	"io"
	"sync"
	"syscall"

	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/writebuffer"
)

// bufferedMaxTotal caps a claimed file's local buffer.
const bufferedMaxTotal = 10 << 30

// gitHandle is the claimed data-plane handle of a workspace file: writes
// buffer over the current image (overlay content or the base object) and
// land as one overlay entry at the next durability boundary.
type gitHandle struct {
	g     *Git
	w     *workspace
	path  string
	rel   string
	flags uint32
	mode  uint32

	mu       sync.Mutex
	buf      *writebuffer.WriteBuffer
	dirty    bool
	unlinked bool
}

// claimWorkspaceOpen wraps a write-intent open of a workspace file.
func (g *Git) claimWorkspaceOpen(w *workspace, path, rel string, flags uint32, mode uint32) (*gitHandle, error) {
	e, hit := w.lookup(rel)
	if hit && e.Op == "delete" {
		return nil, fs.ErrNotFound
	}
	if hit && e.Kind != "file" {
		return nil, syscall.EISDIR
	}
	h := &gitHandle{g: g, w: w, path: path, rel: rel, flags: flags}
	if hit {
		h.mode = parseMode(e.Mode)
	}
	if h.mode == 0 {
		h.mode = mode & 0o777
	}
	if h.mode == 0 {
		h.mode = 0o644
	}
	return h, nil
}

func (h *gitHandle) Attr() fs.Attr {
	h.mu.Lock()
	defer h.mu.Unlock()
	size := int64(0)
	if e, hit := h.w.lookup(h.rel); hit && e.Op == "upsert" {
		size = int64(len(e.Content))
	}
	if h.buf != nil {
		size = h.buf.Size()
	}
	return fs.Attr{Kind: fs.KindFile, Mode: h.mode, Size: size}
}

func (h *gitHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.buf == nil {
		if e, hit := h.w.lookup(h.rel); hit && e.Op == "upsert" {
			if off >= int64(len(e.Content)) {
				return 0, nil
			}
			return copy(dest, e.Content[off:]), nil
		}
		rc, err := h.g.cl.ReadStreamRange(ctx, h.path, off, int64(len(dest)))
		if err != nil {
			return 0, err
		}
		defer func() { _ = rc.Close() }()
		return io.ReadFull(rc, dest)
	}
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

func (h *gitHandle) Write(ctx context.Context, off int64, data []byte) error {
	h.mu.Lock()
	if err := h.ensureBufferLocked(ctx); err != nil {
		h.mu.Unlock()
		return err
	}
	if h.flags&uint32(syscall.O_APPEND) != 0 && off == 0 {
		off = h.buf.Size()
	}
	if _, err := h.buf.Write(off, data); err != nil {
		h.mu.Unlock()
		return err
	}
	h.dirty = true
	h.mu.Unlock()
	return nil
}

func (h *gitHandle) Sync(ctx context.Context) error  { return h.commit(ctx) }
func (h *gitHandle) Flush(ctx context.Context) error { return h.commit(ctx) }

func (h *gitHandle) Release(ctx context.Context) error {
	h.g.untrackWorkspace(h.path)
	if h.unlinked {
		return nil
	}
	return h.commit(ctx)
}

// Truncate applies a local truncation; the image lands at the boundary.
func (h *gitHandle) Truncate(size int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.ensureBufferLocked(context.Background()); err != nil {
		return err
	}
	if err := h.buf.Truncate(size); err != nil {
		return err
	}
	h.dirty = true
	return nil
}

// SetMode records a chmod into the next commit.
func (h *gitHandle) SetMode(mode uint32) {
	h.mu.Lock()
	h.mode = mode & 0o777
	h.mu.Unlock()
}

// markUnlinked stops the handle from ever publishing; the unlink op has
// already written the overlay delete.
func (h *gitHandle) markUnlinked() {
	h.mu.Lock()
	h.unlinked = true
	h.mu.Unlock()
}

// retarget moves the handle with a renamed path.
func (h *gitHandle) retarget(path, rel string) {
	h.mu.Lock()
	old := h.path
	h.path, h.rel = path, rel
	h.mu.Unlock()
	if old != path {
		h.g.retargetWorkspace(old, path, h)
	}
}

// commit lands the dirty image as one overlay entry.
func (h *gitHandle) commit(ctx context.Context) error {
	h.mu.Lock()
	if h.unlinked || !h.dirty || h.buf == nil {
		h.mu.Unlock()
		return nil
	}
	if err := h.buf.LoadMissingParts(); err != nil {
		h.mu.Unlock()
		return err
	}
	image := append([]byte(nil), h.buf.Bytes()...)
	mode := fmt.Sprintf("%04o", 0o100|h.mode)
	h.mu.Unlock()
	if err := h.g.putOverlay(ctx, h.w, h.rel, overlayEntry{Op: "upsert", Kind: "file", Content: image, Mode: mode}); err != nil {
		return err
	}
	h.mu.Lock()
	h.dirty = false
	h.buf.ClearDirty()
	h.mu.Unlock()
	h.g.emit(h.path)
	return nil
}

// ensureBufferLocked creates the write buffer over the current image:
// overlay content preloads, base objects stay lazy.
func (h *gitHandle) ensureBufferLocked(ctx context.Context) error {
	if h.buf != nil {
		return nil
	}
	h.buf = writebuffer.NewWriteBuffer(h.path, bufferedMaxTotal, 0)
	if e, hit := h.w.lookup(h.rel); hit && e.Op == "upsert" && len(e.Content) > 0 {
		if _, err := h.buf.Write(0, e.Content); err != nil {
			return err
		}
		h.buf.ClearDirty()
		return nil
	}
	if st, err := h.g.cl.StatCtx(ctx, h.path); err == nil && st != nil && st.Size > 0 {
		partSize := h.buf.PartSize()
		remoteSize := st.Size
		g := h.g
		path := h.path
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
			rc, err := g.cl.ReadStreamRange(ctx, path, start, end-start)
			if err != nil {
				return nil, err
			}
			defer func() { _ = rc.Close() }()
			_, err = io.ReadFull(rc, part)
			if err == io.ErrUnexpectedEOF {
				err = nil
			}
			return part, err
		}
		h.buf.SeedRemote(remoteSize)
	}
	return nil
}

// hasWriteIntent reports an open that can produce writes.
func hasWriteIntent(flags uint32) bool {
	return flags&uint32(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_APPEND|syscall.O_CREAT) != 0
}
