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
	// treeSHA set on a clean-tree read handle: reads materialize the
	// blob instead of falling to the (absent) base-plane entry.
	treeSHA string

	mu       sync.Mutex
	buf      *writebuffer.WriteBuffer
	dirty    bool
	unlinked bool
	// fresh marks a create over a whiteout: the name was deleted in the
	// overlay, and the new object must NOT inherit the hidden base (or
	// clean-tree) identity — it starts empty.
	fresh bool
	// wgen fences commit completion against mutations acknowledged while
	// an older snapshot uploads (the mutex is released across
	// putOverlay): only the generation that landed may clear dirty state.
	wgen uint64
}

// claimWorkspaceOpen wraps a write-intent open of a workspace file.
func (g *Git) claimWorkspaceOpen(w *workspace, path, rel string, flags uint32, mode uint32) (*gitHandle, error) {
	e, hit := w.lookup(rel)
	// A whiteouted name still answers ENOENT to plain opens; a create
	// (O_CREAT) instead claims the name fresh — the whiteout is replaced
	// by the new upsert at the next commit boundary.
	if hit && e.Op == "whiteout" && flags&uint32(syscall.O_CREAT) == 0 {
		return nil, fs.ErrNotFound
	}
	if hit && e.Op != "whiteout" && e.Kind != "file" {
		return nil, syscall.EISDIR
	}
	fresh := hit && e.Op == "whiteout"
	if fresh {
		hit = false // fresh create over the whiteout
	}
	h := &gitHandle{g: g, w: w, path: path, rel: rel, flags: flags, fresh: fresh}
	if hit {
		h.mode = parseMode(e.Mode)
	}
	if h.mode == 0 {
		h.mode = mode & 0o777
	}
	if h.mode == 0 {
		// An in-place modify of a clean tree file carries no create mode
		// (append opens, O_TRUNC rewrites of existing names): the file
		// keeps the tree node's mode — a stash/checkout restore that
		// rewrites content in place must not regress an 0755 fixture
		// file to 0644 in the published overlay entry.
		if n, tok := w.treeLookup(rel); tok {
			h.mode = parseMode(n.Mode)
		}
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
	if e, hit := h.w.lookup(h.rel); hit && e.Op == "upsert" {
		size = int64(len(e.Content))
		if !e.mtime.IsZero() {
			mtime = e.mtime
		}
	}
	if h.buf != nil {
		size = h.buf.Size()
	}
	if size == 0 && h.treeSHA != "" {
		if n, tok := h.w.treeLookup(h.rel); tok {
			size = n.SizeBytes
			if size < 0 {
				size = h.g.blobSize(h.w, h.treeSHA)
				h.w.noteTreeSize(h.rel, size)
			}
		}
	}
	return fs.Attr{Kind: fs.KindFile, Mode: h.mode, Size: size, Mtime: mtime}
}

func (h *gitHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.buf == nil && h.treeSHA != "" {
		if _, hit := h.w.lookup(h.rel); !hit {
			data, err := h.g.materializeBlob(h.w, h.treeSHA)
			if err != nil {
				// The blob's local .git may simply not be here yet: the
				// discovery-time restore ran before the CLI uploaded its
				// checkpoint. Pull the state once, then retry.
				if rerr := h.g.ensureWorkspaceState(ctx, h.w); rerr == nil {
					data, err = h.g.materializeBlob(h.w, h.treeSHA)
				}
			}
			if err != nil {
				return 0, err
			}
			h.w.noteTreeSize(h.rel, int64(len(data)))
			if off >= int64(len(data)) {
				return 0, nil
			}
			return copy(dest, data[off:]), nil
		}
	}
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
	h.wgen++
	syncIntent := h.flags&uint32(syscall.O_SYNC) != 0 || h.flags&uint32(syscall.O_DSYNC) != 0 ||
		h.g.syncPolicy == "write-sync" || h.g.syncPolicy == "strict"
	h.mu.Unlock()
	if syncIntent {
		// The effective synchronous policy (mount write-sync or the
		// descriptor's O_SYNC/O_DSYNC) acknowledges only after the overlay
		// commit — the RAM-only return left the server's overlay without
		// the entry until a later Flush (main committed before acking).
		return h.commit(ctx)
	}
	return nil
}

func (h *gitHandle) Sync(ctx context.Context) error  { return h.commit(ctx) }
func (h *gitHandle) Flush(ctx context.Context) error { return h.commit(ctx) }

func (h *gitHandle) Release(ctx context.Context) error {
	h.g.untrackHandle(h)
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
	h.wgen++
	return nil
}

// SetMode records a chmod into the next commit.
func (h *gitHandle) SetMode(mode uint32) {
	h.mu.Lock()
	h.mode = mode & 0o777
	// Every acknowledged mutation advances the generation: a chmod racing
	// an older snapshot's upload must not be retired by its completion.
	h.wgen++
	h.dirty = true
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
	if h.unlinked || !h.dirty {
		h.mu.Unlock()
		return nil
	}
	if h.buf == nil {
		// A created-but-never-written file still owes its existence: an
		// empty image must publish at the durability boundary (Sync,
		// Flush, and Release all skipped it and the name vanished with
		// the close).
		if err := h.ensureBufferLocked(ctx); err != nil {
			h.mu.Unlock()
			return err
		}
		if h.buf == nil {
			h.mu.Unlock()
			return nil
		}
	}
	if err := h.buf.LoadMissingParts(); err != nil {
		h.mu.Unlock()
		return err
	}
	image := append([]byte(nil), h.buf.Bytes()...)
	// Regular-file tree modes are 6-digit octal (100644): a stray 0o100
	// OR planted a phantom owner-exec bit (0744) on every published entry.
	mode := fmt.Sprintf("%o", 0o100000|h.mode)
	wgen := h.wgen
	ws := h.w
	h.mu.Unlock()
	// Publication is serialized per workspace: an older snapshot's PUT
	// must never overtake a newer one (the newer completion clearing its
	// matching generation left nothing to republish the newer image).
	ws.uploadMu.Lock()
	defer ws.uploadMu.Unlock()
	if err := h.g.putOverlay(ctx, ws, h.rel, overlayEntry{Op: "upsert", Kind: "file", Content: image, Mode: mode, Materialized: true}); err != nil {
		return err
	}
	h.mu.Lock()
	// Only the generation that uploaded may retire the dirty state: a
	// Write/SetMode/Truncate acknowledged while the mutex was released
	// across putOverlay keeps its own bytes owed to a later boundary
	// (the stale completion persisted only the older image otherwise).
	if h.wgen == wgen {
		h.dirty = false
		h.buf.ClearDirty()
	}
	h.mu.Unlock()
	h.g.emit(h.path)
	return nil
}

// ensureBufferLocked creates the write buffer over the current image:
// overlay content preloads first, then the clean-tree blob (an in-place
// modify of a tree file buffers the tree object — v1 readGitCleanFile),
// and only then a lazy remote base. The lazy loader must NOT capture the
// opening op's context: parts materialize at commit time (Flush/Release),
// long after that context was canceled, and a canceled-context read turns
// the seeded image into zero-filled garbage.
func (h *gitHandle) ensureBufferLocked(ctx context.Context) error {
	if h.buf != nil {
		return nil
	}
	h.buf = writebuffer.NewWriteBuffer(h.path, bufferedMaxTotal, 0)
	if h.fresh {
		// A recreated name owns an empty image: preloading the overlay,
		// clean-tree blob or remote base would resurrect the deleted
		// object's bytes under the new create (NEW over hello answered
		// NEWlo).
		return nil
	}
	if e, hit := h.w.lookup(h.rel); hit && e.Op == "upsert" && e.Materialized {
		// An EMPTY upsert is the authoritative image (a synced
		// ftruncate(0)): the len>0 guard skipped it and reloaded the old
		// HEAD/base bytes, resurrecting the truncated tail (X over a
		// zeroed hello answered Xello). Materialized distinguishes an
		// actual image from a metadata-only entry.
		if _, err := h.buf.Write(0, e.Content); err != nil {
			return err
		}
		h.buf.ClearDirty()
		return nil
	}
	if n, ok := h.w.treeLookup(h.rel); ok && n.Kind == "file" && n.ObjectSHA != "" {
		data, err := h.g.materializeBlob(h.w, n.ObjectSHA)
		if err != nil {
			if rerr := h.g.ensureWorkspaceState(ctx, h.w); rerr == nil {
				data, err = h.g.materializeBlob(h.w, n.ObjectSHA)
			}
		}
		if err == nil {
			if _, werr := h.buf.Write(0, data); werr != nil {
				return werr
			}
			h.buf.ClearDirty()
			return nil
		}
		// Fall through to the remote base: the blob may be materializable
		// there even though the local git dir cannot serve it.
	}
	if st, err := h.g.cl.StatCtx(ctx, h.path); err == nil && st != nil && st.Size > 0 {
		partSize := h.buf.PartSize()
		remoteSize := st.Size
		g := h.g
		path := h.path
		loadCtx := context.Background()
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
			rc, err := g.cl.ReadStreamRange(loadCtx, path, start, end-start)
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
