// The layer write plane: claimed files buffer locally and commit once per
// durability boundary — a single layer entry per flush (inline body, or a
// streamed object upload for large images) instead of one HTTP round trip
// per write.
package layer

import (
	"bytes"
	"context"
	"io"
	"sync"
	"syscall"
	"time"

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
	// wgen fences commit completion against writes that arrive while an
	// older snapshot uploads: only the generation that landed may clear
	// the dirty state.
	wgen uint64
	// released marks a descriptor whose Release already published: the
	// registry must never re-select it as the path's surviving claim.
	released bool
	// conflicted marks a handle whose path changed underneath it in an
	// external layer refresh while it held uncommitted writes (v1 replay
	// conflict): writes and commits answer EAGAIN — the buffer is kept, and
	// publishing it onto the new tip would splice stale bytes.
	conflicted bool
	// unlinkAtRelease publishes the layer whiteout of an unlinked-but-open
	// path at the last close.
	unlinkAtRelease func(ctx context.Context) error
}

// claim wraps an open of a layer-view file with write intent.
// claimWithMode distinguishes an explicit create mode (including the
// valid zero mask) from an absent one: only the absent mode takes the
// 0644 default.
func (l *Layer) claimWithMode(path string, flags uint32, mode uint32, hasMode bool) (*layerHandle, error) {
	// An uploaded-but-unmaterialized image is the authoritative content:
	// hydrate it so the handle reopens the real image (a missing entry
	// answered attrs 0/EOF and a pwrite-then-flush dropped the rest).
	if err := l.hydrateRemote(context.Background(), path); err != nil {
		return nil, err
	}
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
	// lazy-load extent. An explicitly requested zero create mode is a
	// valid permission mask (create(0000) must not widen to 0644); the
	// 0644 default applies only when the caller sent no mode at all.
	if mode == 0 && !hasMode {
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
	if h.unlinked {
		// An externally whiteouted path serves ENOENT even on an open fd
		// (v1 survivor semantics: the fd must not resurrect deleted content).
		return 0, fs.ErrNotFound
	}
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
		// A read extending past EOF returns a short body with
		// ErrUnexpectedEOF — those bytes are the file's real tail, not an
		// error (the facade only tolerates io.EOF and turned them into
		// EIO).
		n, rerr := io.ReadFull(rc, dest)
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			rerr = nil
		}
		return n, rerr
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
	if h.conflicted {
		h.mu.Unlock()
		return syscall.EAGAIN
	}
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
	h.wgen++
	policy := h.l.policy
	syncIntent := h.flags&uint32(syscall.O_SYNC) != 0 || h.flags&uint32(syscall.O_DSYNC) != 0
	h.mu.Unlock()
	if policy == policyWriteSync || syncIntent {
		// A synchronous descriptor (or write-sync mount) acknowledges only
		// after the layer commit lands — the deferred path returned with
		// the entry absent from the layer log.
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
	h.mu.Unlock()
	if unlinked {
		_ = unlink // the unlink op already published the whiteout
		h.l.untrackHandle(h)
		return nil
	}
	err := h.commit(ctx)
	h.l.untrackHandle(h)
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
	// An acknowledged truncation is a write-generation boundary: without
	// bumping wgen, a drain upload that started before the truncate could
	// complete afterwards and retire the dirty flag, persisting only the
	// pre-truncate image.
	h.wgen++
	h.dirty = true
	return nil
}

// SetMode records a chmod into the next commit. A metadata-only change on
// an otherwise clean handle still owes a commit: without marking dirty, a
// synced chmod reported success while a fresh replay kept the old mode.
func (h *layerHandle) SetMode(mode uint32) {
	h.mu.Lock()
	h.mode = mode & 0o777
	if !h.dirty {
		h.wgen++
		h.dirty = true
	}
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
	// A registry MOVE, not a retirement: the open fd keeps its lifetime —
	// retiring here marked still-open descriptors released and later
	// renames/writes lost them. The OLD slot is detached by name: h.path
	// already reads newPath here, and keying the detach on it removed the
	// target slot while live[old] kept pointing at this retargeted fd —
	// a later SetAttr/Unlink on the old name then hit the new name's fd.
	h.l.detachFromRegistry(h, old)
	h.l.liveStore(newPath, h)
}

// commit lands the dirty image as one layer entry: streamed as an object
// upload beyond the inline threshold, else an inline upsert against the
// base revision.
func (h *layerHandle) commit(ctx context.Context) error {
	h.mu.Lock()
	if h.unlinked || !h.dirty {
		h.mu.Unlock()
		return nil
	}
	if h.conflicted {
		h.mu.Unlock()
		return syscall.EAGAIN
	}
	// A create that never wrote has no buffer yet: the empty image must
	// still land (lock files, markers — an O_CREAT close with zero bytes
	// published nothing and the next stat ENOENT'd).
	if h.buf == nil {
		if err := h.ensureBufferLocked(ctx); err != nil {
			h.mu.Unlock()
			return err
		}
	}
	if err := h.buf.LoadMissingParts(); err != nil {
		h.mu.Unlock()
		return err
	}
	size := h.buf.Size()
	mode, baseRev, path := h.mode, h.baseRev, h.path
	wgen := h.wgen
	image := append([]byte(nil), h.buf.Bytes()...)
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
		entry, err = l.cl.UploadFSLayerFile(ctx, l.layerRef, l.remotePath(path), bytes.NewReader(image), size, baseRev, mode, true)
	} else {
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
		h.l.contents[path] = image
		delete(h.l.remoteSizes, path)
		delete(h.l.whiteouts, path)
		delete(h.l.dirs, path)
		delete(h.l.symlinks, path)
	}
	h.l.mu.Unlock()
	h.mu.Lock()
	if h.wgen == wgen {
		// No write arrived while the snapshot uploaded: this commit is
		// the newest state and may retire the dirty flag. A newer wgen
		// keeps the handle dirty — its bytes are still unacknowledged.
		h.dirty = false
		h.isNew = false
		h.buf.ClearDirty()
	}
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
			// The opening request's context is canceled when Write
			// returns, but parts materialize at flush/commit time long
			// after — a fresh bounded context per load keeps an
			// acknowledged edit committable (a captured request context
			// failed every later LoadMissingParts with "context
			// canceled").
			loadCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			rc, err := l.cl.ReadStreamRange(loadCtx, remote, start, end-start)
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

// hasWriteIntent reports an open that can produce writes.
func hasWriteIntent(flags uint32) bool {
	return flags&uint32(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_APPEND|syscall.O_CREAT) != 0
}

// policyWriteSync mirrors the mount's write-sync policy name.
const policyWriteSync = "write-sync"
