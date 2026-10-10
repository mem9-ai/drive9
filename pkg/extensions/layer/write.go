// The layer write plane: claimed files buffer locally and commit once per
// durability boundary — a single layer entry per flush (inline body, or a
// streamed object upload for large images) instead of one HTTP round trip
// per write.
package layer

import (
	"context"
	"io"
	"sync"
	"syscall"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/writebuffer"
)

// bufferedMaxTotal caps a claimed file's local buffer.
const bufferedMaxTotal = 10 << 30

// layerHandle is the claimed data-plane handle of a layer file: writes
// buffer over the current image (overlay content, the base object, or a
// fresh file) and land as one layer entry at the next durability boundary.
type layerHandle struct {
	l        *Layer
	path     string
	flags    uint32
	isNew    bool
	baseRev  int64 // CAS base of the underlying base object (0 for overlay-only files)
	origSize int64 // base extent the buffer seeds from
	mode     uint32

	mu       sync.Mutex
	buf      *writebuffer.WriteBuffer
	dirty    bool
	unlinked bool
	// unlinkAtRelease publishes the layer whiteout of an unlinked-but-open
	// path at the last close.
	unlinkAtRelease func(ctx context.Context) error
}

// claim wraps an open of a layer-view file with write intent.
func (l *Layer) claim(path string, flags uint32, mode uint32) (*layerHandle, error) {
	kind, overlayMode, _, content, ok := l.lookup(path)
	h := &layerHandle{l: l, path: path, flags: flags, mode: mode, isNew: false}
	switch {
	case ok && (kind == "whiteout"):
		return nil, fs.ErrNotFound
	case ok && kind == "symlink", ok && kind == "dir":
		// namespace entries have no data plane here
		return nil, syscall.EISDIR
	case ok && kind == "file":
		h.mode = overlayMode
		h.origSize = int64(len(content))
		return h, nil
	}
	// base object (or a fresh create): stat resolves the CAS base and the
	// lazy-load extent.
	if mode == 0 {
		h.mode = 0o644
	}
	st, err := l.cl.StatCtx(context.Background(), l.remotePath(path))
	if err == nil && st != nil {
		h.baseRev = st.Revision
		h.origSize = st.Size
		if st.Mode != 0 {
			h.mode = st.Mode & 0o777
		}
		return h, nil
	}
	if client.IsNotFound(err) {
		h.isNew = true
		return h, nil
	}
	if err != nil {
		return nil, err
	}
	return h, nil
}

func (h *layerHandle) Attr() fs.Attr {
	h.mu.Lock()
	defer h.mu.Unlock()
	size := h.origSize
	if h.buf != nil {
		size = h.buf.Size()
	}
	return fs.Attr{Kind: fs.KindFile, Mode: h.mode, Size: size}
}

func (h *layerHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.buf == nil {
		// Overlay files serve their committed image; base files stream the
		// remote object.
		if _, _, _, content, ok := h.l.lookup(h.path); ok {
			if off >= int64(len(content)) {
				return 0, nil
			}
			n := copy(dest, content[off:])
			return n, nil
		}
		rc, err := h.l.cl.ReadStreamRange(ctx, h.l.remotePath(h.path), off, int64(len(dest)))
		if err != nil {
			return 0, err
		}
		defer func() { _ = rc.Close() }()
		return io.ReadFull(rc, dest)
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

func (h *layerHandle) Write(ctx context.Context, off int64, data []byte) error {
	h.mu.Lock()
	if err := h.ensureBufferLocked(ctx); err != nil {
		h.mu.Unlock()
		return err
	}
	// O_APPEND: the kernel may deliver off=0; the write lands at the
	// buffer tail.
	if h.flags&uint32(syscall.O_APPEND) != 0 && off == 0 {
		off = h.buf.Size()
	}
	if _, err := h.buf.Write(off, data); err != nil {
		h.mu.Unlock()
		return err
	}
	h.dirty = true
	policy := h.l.policy
	h.mu.Unlock()
	if policy == policyWriteSync {
		return h.commit(ctx)
	}
	return nil
}

func (h *layerHandle) Sync(ctx context.Context) error { return h.commit(ctx) }

func (h *layerHandle) Flush(ctx context.Context) error { return h.commit(ctx) }

func (h *layerHandle) Release(ctx context.Context) error {
	h.mu.Lock()
	unlinked := h.unlinked
	unlink := h.unlinkAtRelease
	path := h.path
	h.mu.Unlock()
	if unlinked {
		_ = unlink // the unlink op already published the whiteout
		h.l.untrack(path)
		return nil
	}
	err := h.commit(ctx)
	h.l.untrack(path)
	return err
}

// Truncate applies a local truncation; the image lands at the next
// boundary (a write-less close still publishes the new size).
func (h *layerHandle) Truncate(size int64) error {
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
func (h *layerHandle) SetMode(mode uint32) {
	h.mu.Lock()
	h.mode = mode & 0o777
	h.mu.Unlock()
}

// markUnlinked stops the handle from ever publishing: the unlink op has
// already whiteouted the name, and the open handle keeps serving its
// buffered image until close.
func (h *layerHandle) markUnlinked() {
	h.mu.Lock()
	h.unlinked = true
	h.mu.Unlock()
}

// retarget moves the handle to a renamed path.
func (h *layerHandle) retarget(newPath string) {
	h.mu.Lock()
	old := h.path
	h.path = newPath
	h.mu.Unlock()
	if old == newPath {
		return
	}
	h.l.untrack(old)
	h.l.liveStore(newPath, h)
}

// commit lands the dirty image as one layer entry: streamed as an object
// upload beyond the inline threshold, else an inline upsert against the
// base revision.
func (h *layerHandle) commit(ctx context.Context) error {
	h.mu.Lock()
	if h.unlinked || !h.dirty || h.buf == nil {
		h.mu.Unlock()
		return nil
	}
	if err := h.buf.LoadMissingParts(); err != nil {
		h.mu.Unlock()
		return err
	}
	size := h.buf.Size()
	mode, baseRev, path := h.mode, h.baseRev, h.path
	l := h.l
	h.mu.Unlock()

	l.mu.RLock()
	abandoned := l.abandoned
	l.mu.RUnlock()
	if abandoned {
		return syscall.ESTALE
	}
	unlock := l.mutLocks.lock(path)
	defer unlock()
	var entry *client.FSLayerEntry
	var err error
	if size > l.inlineMax() {
		h.mu.Lock()
		reader := &bufferReader{h: h, size: size}
		h.mu.Unlock()
		entry, err = l.cl.UploadFSLayerFile(ctx, l.layerRef, l.remotePath(path), reader, size, baseRev, mode, true)
	} else {
		h.mu.Lock()
		image := append([]byte(nil), h.buf.Bytes()...)
		h.mu.Unlock()
		entry, err = l.cl.UpsertFSLayerEntry(ctx, l.layerRef, client.FSLayerEntryRequest{
			Path:         l.remotePath(path),
			Op:           "upsert",
			Kind:         "file",
			Content:      image,
			Mode:         mode,
			BaseRevision: baseRev,
			SizeBytes:    int64(len(image)),
		})
	}
	if err != nil {
		return err
	}
	h.l.mu.Lock()
	if !h.l.abandoned {
		h.l.files[path] = mode & 0o777
		h.l.contents[path] = append([]byte(nil), h.buf.Bytes()...)
		delete(h.l.whiteouts, path)
		delete(h.l.dirs, path)
		delete(h.l.symlinks, path)
	}
	h.l.mu.Unlock()
	h.mu.Lock()
	h.dirty = false
	h.isNew = false
	h.buf.ClearDirty()
	h.mu.Unlock()
	_ = entry
	h.l.emit(fs.Event{Kind: fs.EventLanded, Path: path, Size: size})
	h.l.emit(fs.Event{Kind: fs.EventInvalidated, Path: path})
	return nil
}

// ensureBufferLocked creates the write buffer over the current image:
// overlay content preloads, base objects stay lazy (parts load from the
// remote on first touch), fresh files start empty.
func (h *layerHandle) ensureBufferLocked(ctx context.Context) error {
	if h.buf != nil {
		return nil
	}
	h.buf = writebuffer.NewWriteBuffer(h.path, bufferedMaxTotal, 0)
	if _, _, _, content, ok := h.l.lookup(h.path); ok && len(content) > 0 {
		if _, err := h.buf.Write(0, content); err != nil {
			return err
		}
		h.buf.ClearDirty()
		return nil
	}
	if h.origSize > 0 {
		partSize := h.buf.PartSize()
		remoteSize := h.origSize
		l := h.l
		remote := l.remotePath(h.path)
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
			rc, err := l.cl.ReadStreamRange(ctx, remote, start, end-start)
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
		return nil
	}
	return nil
}

// bufferReader streams a claimed file's image chunk-by-chunk: each Read
// loads only the parts it touches, so committing a sparse multi-GiB image
// never materializes the whole image in memory.
type bufferReader struct {
	h    *layerHandle
	off  int64
	size int64
}

func (r *bufferReader) Read(p []byte) (int, error) {
	n, err := r.ReadAt(p, r.off)
	if n > 0 {
		r.off += int64(n)
	}
	return n, err
}

// ReadAt satisfies the large-upload contract: multipart uploads
// type-assert io.ReaderAt and seek per part.
func (r *bufferReader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= r.size {
		return 0, io.EOF
	}
	n := int64(len(p))
	if off+n > r.size {
		n = r.size - off
	}
	if n <= 0 {
		return 0, io.EOF
	}
	r.h.mu.Lock()
	defer r.h.mu.Unlock()
	if r.h.buf == nil {
		return 0, fs.ErrUnsupported
	}
	if err := r.h.buf.EnsurePartsForWrite(off, n); err != nil {
		return 0, err
	}
	got := r.h.buf.ReadAt(off, p[:n])
	if got <= 0 {
		return 0, io.EOF
	}
	return got, nil
}

// hasWriteIntent reports an open that can produce writes.
func hasWriteIntent(flags uint32) bool {
	return flags&uint32(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_APPEND|syscall.O_CREAT) != 0
}

// policyWriteSync mirrors the mount's write-sync policy name.
const policyWriteSync = "write-sync"
