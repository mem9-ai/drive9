// The workspace write plane: claimed files buffer locally and land as one
// whole-image mirror write per durability boundary instead of one store
// round trip per write chunk. The buffer seeds lazily from the resolved
// layer (mirror image or clean tree content) through the store view.
package git

import (
	"context"
	"sync"
	"syscall"
	"time"

	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/writebuffer"
	"github.com/mem9-ai/drive9/pkg/gitws"
)

// bufferedMaxTotal caps a claimed file's local buffer.
const bufferedMaxTotal = 10 << 30

// gitHandle is the claimed data-plane handle of a workspace file: writes
// buffer over the current image (mirror content or clean tree content)
// and land as one mirror write at the next durability boundary.
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
	res := g.resolveIn(w, rel)
	if res.layer == layerHidden && !res.tracked() {
		return nil, fs.ErrNotFound
	}
	if res.layer == layerOverlay && res.node.kind != fs.KindFile {
		return nil, syscall.EISDIR
	}
	if res.layer == layerTree && kindOfEntry(res.ent.Kind) != fs.KindFile {
		return nil, syscall.EISDIR
	}
	h := &gitHandle{g: g, w: w, path: path, rel: rel, flags: flags}
	if res.layer == layerOverlay {
		h.mode = res.node.mode
	}
	if h.mode == 0 {
		h.mode = mode & 0o777
	}
	if h.mode == 0 {
		h.mode = parseMode(res.ent.Mode)
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
	mtime := h.w.cleanTreeMtime()
	if node, hit := h.w.lookup(h.rel); hit && !node.blackout {
		size = node.size
		if !node.mtime.IsZero() {
			mtime = node.mtime
		}
	} else if ent, ok := h.w.isTracked(h.rel); ok {
		size = ent.Size
	}
	if h.buf != nil {
		size = h.buf.Size()
		mtime = time.Now()
	}
	return fs.Attr{Kind: fs.KindFile, Mode: h.mode, Size: size, Mtime: mtime}
}

func (h *gitHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.buf == nil {
		store := h.g.storeView()
		if store == nil {
			return 0, fs.ErrUnsupported
		}
		res := h.g.resolveIn(h.w, h.rel)
		switch res.layer {
		case layerOverlay, layerTree:
			data, err := store.Read(ctx, res.storePath, off, int64(len(dest)))
			if err != nil {
				return 0, err
			}
			return copy(dest, data), nil
		}
		return 0, fs.ErrNotFound
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
// already written the blackout.
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

// commit lands the dirty image as one mirror write.
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
	mode := h.mode
	h.mu.Unlock()
	if err := h.g.landOverlayFile(ctx, h.w, h.rel, image, mode); err != nil {
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
// mirror content preloads, clean tree content stays lazy part-loading.
func (h *gitHandle) ensureBufferLocked(ctx context.Context) error {
	if h.buf != nil {
		return nil
	}
	h.buf = writebuffer.NewWriteBuffer(h.path, bufferedMaxTotal, 0)
	store := h.g.storeView()
	if store == nil {
		return fs.ErrUnsupported
	}
	res := h.g.resolveIn(h.w, h.rel)
	switch res.layer {
	case layerOverlay:
		content, err := store.Read(ctx, res.storePath, 0, 0)
		if err == nil && len(content) > 0 {
			if _, err := h.buf.Write(0, content); err != nil {
				return err
			}
			h.buf.ClearDirty()
		}
		return nil
	case layerTree:
		attr, err := store.GetAttr(ctx, res.storePath)
		if err != nil || attr.Size <= 0 {
			return nil // not hydrated yet: the buffer starts empty
		}
		partSize := h.buf.PartSize()
		remoteSize := attr.Size
		g := h.g
		w := h.w
		rel := h.rel
		h.buf.LoadPart = func(partNum int) ([]byte, error) {
			start := int64(partNum-1) * partSize
			end := start + partSize
			if end > remoteSize {
				end = remoteSize
			}
			if end <= start {
				return nil, nil
			}
			// The commit may have moved under us; resolve fresh each load.
			rr := g.resolveIn(w, rel)
			if rr.layer != layerTree {
				return nil, nil
			}
			part, err := g.storeView().Read(ctx, gitws.TreePath(w.id, w.head(), rel), start, end-start)
			if err != nil {
				return nil, err
			}
			if len(part) < int(end-start) {
				part = append(part, make([]byte, int(end-start)-len(part))...)
			}
			return part, nil
		}
		h.buf.SeedRemote(remoteSize)
	}
	return nil
}

// hasWriteIntent reports an open that can produce writes.
func hasWriteIntent(flags uint32) bool {
	return flags&uint32(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_APPEND|syscall.O_CREAT) != 0
}
