// The layer write plane: claimed files buffer locally and commit once per
// durability boundary — a single layer entry per flush (inline body, or a
// streamed object upload for large images) instead of one HTTP round trip
// per write.
package layer

import (
	"context"
	"io"
	"os"
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
	// unlinkedExternal marks a deletion observed from OUTSIDE (an event
	// sweep saw the whiteout): the fd serves ENOENT. A LOCAL unlink's
	// anonymous fd keeps reading its buffered image — only republishing
	// the old name is forbidden.
	unlinkedExternal bool
	// landed/landedSeq identify the entry THIS handle last landed (the
	// server's entry sequence): the checkout sweep compares them against
	// the observed event sequence so the handle's own commits never fence
	// its newer dirty buffer — while a foreign entry, even one with
	// byte-identical content, still does.
	landed    bool
	landedSeq int64
	// landing marks a commit whose server call is in flight: the server
	// publishes the entry/event BEFORE the HTTP reply returns, so a
	// concurrent event poll can observe it while landed/landedSeq still
	// describe the PREVIOUS landing — the sweep defers such paths instead
	// of misjudging them foreign (a false conflicted never cleared).
	landing bool
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
		if flags&uint32(syscall.O_CREAT) == 0 {
			return nil, fs.ErrNotFound
		}
		// A create over a whiteout owns the name FRESH: the whiteout
		// hides the deleted base object from plain opens, but a new
		// create must neither be refused (the name could never be
		// recreated) nor inherit the hidden base's identity — it starts
		// as a new empty file.
		h.mode = mode & 0o777
		if h.mode == 0 && !hasMode {
			h.mode = 0o644
		}
		h.isNew = true
		return h, nil
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
	if h.unlinkedExternal {
		// An externally whiteouted path serves ENOENT even on an open fd
		// (v1 survivor semantics: the fd must not resurrect deleted
		// content). A locally-unlinked anonymous fd is NOT invalidated:
		// it keeps serving its own image (POSIX unlink semantics).
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
	// Every acknowledged mutation advances the generation: a chmod racing
	// an older snapshot's upload passed the completion's generation check
	// otherwise and was silently dropped from the persisted entry.
	h.wgen++
	h.dirty = true
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

// retarget moves the handle to a renamed path. The registry move runs
// under regMu and re-checks the descriptor's lifetime before registering
// the new-name slot: a Release that completed in between already removed
// the fd from both indices (its retirement is serialized on the same
// lock), and re-registering it would resurrect a descriptor whose commit
// boundary will never come — later path-keyed SetAttr/rename could then
// pin acknowledged mutations to a dead handle.
func (h *layerHandle) retarget(newPath string) {
	l := h.l
	l.regMu.Lock()
	defer l.regMu.Unlock()
	// Re-read under the registry lock: a concurrent rename may have moved
	// the fd again before this move acquired it.
	h.mu.Lock()
	old := h.path
	if old == newPath {
		h.mu.Unlock()
		return
	}
	h.path = newPath
	h.mu.Unlock()
	// A registry MOVE, not a retirement: the open fd keeps its lifetime —
	// retiring here marked still-open descriptors released and later
	// renames/writes lost them. The OLD slot is detached by name: h.path
	// already reads newPath here, and keying the detach on it removed the
	// target slot while live[old] kept pointing at this retargeted fd —
	// a later SetAttr/Unlink on the old name then hit the new name's fd.
	l.detachFromRegistry(h, old)
	h.mu.Lock()
	released := h.released
	h.mu.Unlock()
	if !released {
		l.liveStoreLocked(newPath, h)
	}
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
	l := h.l
	size := h.buf.Size()
	streamed := size > l.inlineMax()
	if streamed {
		// Bounded memory: the streamed path uploads through a chunked
		// reader over the buffer (ReadAt lazy-loads parts one at a time
		// and zero-fills holes) — materializing the whole image with
		// Bytes() first (plus the slice copy) held multiples of the file
		// in memory and OOMed large mounts before the upload even began.
	} else if err := h.buf.LoadMissingParts(); err != nil {
		h.mu.Unlock()
		return err
	}
	mode, baseRev, path := h.mode, h.baseRev, h.path
	wgen := h.wgen
	image := []byte(nil)
	if !streamed {
		image = append([]byte(nil), h.buf.Bytes()...)
	}
	h.landing = true
	buf := h.buf
	h.mu.Unlock()
	landingDone := func(entrySeq int64) {
		h.mu.Lock()
		h.landing = false
		if entrySeq > 0 {
			h.landed = true
			if entrySeq > h.landedSeq {
				h.landedSeq = entrySeq
			}
		}
		h.mu.Unlock()
	}

	l.mu.RLock()
	abandoned := l.abandoned
	l.mu.RUnlock()
	if abandoned {
		return syscall.ESTALE
	}
	unlock := l.mutLocks.lock(path)
	// The lock wait outlives the snapshot: a rename holding the path
	// locks may have completed the move and retargeted this fd while the
	// commit waited — publishing the captured image under the OLD path
	// would resurrect the renamed-away name. Fail closed; the retry
	// captures the new path. An unlink that held the lock first may have
	// anonymized this fd and landed the whiteout — the captured snapshot
	// is discarded with it (the entry check at function entry ran before
	// the wait and cannot protect the publish boundary).
	h.mu.Lock()
	moved := h.path != path
	unlinked := h.unlinked
	h.mu.Unlock()
	if moved || unlinked {
		unlock()
		// The aborted attempt still ends its landing window: a marker
		// left set kept the fd's path permanently deferred in the event
		// sweep, skipping foreign-change conflict checks forever after.
		landingDone(0)
		if moved {
			return syscall.EAGAIN
		}
		return nil
	}
	defer unlock()
	var entry *client.FSLayerEntry
	var err error
	if streamed {
		// The spill holds the handle's mutex for the copy: the buffer is
		// not thread-safe, and a concurrent Write (a Drain flush racing
		// the same fd) would otherwise splice into the streamed image.
		h.mu.Lock()
		spill, serr := spillLayerImage(buf, size)
		h.mu.Unlock()
		if serr != nil {
			landingDone(0)
			return serr
		}
		entry, err = l.cl.UploadFSLayerFile(ctx, l.layerRef, l.remotePath(path), spill.file, size, baseRev, mode, true)
		spill.close()
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
		landingDone(0)
		return err
	}
	landingDone(entrySeqOf(entry))
	h.l.mu.Lock()
	if !h.l.abandoned {
		h.l.files[path] = mode & 0o777
		if streamed {
			// Object-backed entries do NOT mirror their bytes in memory:
			// lookups re-hydrate from the uploaded object on demand (the
			// remoteSizes marker drives that lazy hydration).
			delete(h.l.contents, path)
			h.l.remoteSizes[path] = size
		} else {
			h.l.contents[path] = image
			delete(h.l.remoteSizes, path)
		}
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

// layerSpill is a stable, bounded-memory snapshot of the handle's image
// on a local temp file.
type layerSpill struct {
	file *os.File
}

func (s *layerSpill) close() {
	if s.file != nil {
		name := s.file.Name()
		_ = s.file.Close()
		_ = os.Remove(name)
		s.file = nil
	}
}

// spillLayerImage copies size bytes of the buffer to a temp file under
// the handle's OWN mutex (WriteBuffer is not thread-safe — a Drain flush
// racing a Write on the same fd read mixed images through a reader that
// held no lock), in bounded memory: each chunk loads the retained base
// parts it spans (ReadAt alone zero-fills unloaded parts — a 16MiB
// partially-modified file persisted 8MiB of zeros) and releases the
// clean ones it loaded; dirty parts are the fd's own data and stay.
func spillLayerImage(buf *writebuffer.WriteBuffer, size int64) (*layerSpill, error) {
	tmp, err := os.CreateTemp("", "drive9-layer-commit-*")
	if err != nil {
		return nil, err
	}
	s := &layerSpill{file: tmp}
	if err := func() error {
		const chunkSize = 1 << 20
		chunk := make([]byte, chunkSize)
		partSize := buf.PartSize()
		for off := int64(0); off < size; {
			want := int64(chunkSize)
			if rem := size - off; rem < want {
				want = rem
			}
			// Load the retained base parts this chunk spans, remembering
			// which we caused (clean ones are released after the copy).
			first := int(off / partSize)
			last := int((off + want - 1) / partSize)
			var loaded []int
			for p := first; p <= last; p++ {
				if !buf.IsPartLoaded(p) {
					if err := buf.EnsureLoaded(p); err != nil {
						return err
					}
					loaded = append(loaded, p)
				}
			}
			n := buf.ReadAt(off, chunk[:want])
			if n <= 0 {
				return io.ErrUnexpectedEOF
			}
			if _, werr := tmp.Write(chunk[:n]); werr != nil {
				return werr
			}
			for _, p := range loaded {
				buf.ReleaseCleanPart(p)
			}
			off += int64(n)
		}
		return nil
	}(); err != nil {
		s.close()
		return nil, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		s.close()
		return nil, err
	}
	return s, nil
}

// entrySeqOf extracts the server entry sequence from a commit reply
// (0 when the reply carried none).
func entrySeqOf(e *client.FSLayerEntry) int64 {
	if e == nil {
		return 0
	}
	return e.EntrySeq
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
