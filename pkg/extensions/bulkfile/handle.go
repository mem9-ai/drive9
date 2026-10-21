package bulkfile

import (
	"context"
	"io"
	"os"
	"sync"
	"syscall"
	"time"

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

	mu    sync.Mutex
	image []byte
	// spill backs an unlinked claim's image once it exceeds the memory
	// bound: a sparse/multi-GiB offset must not allocate the whole hole.
	spill     *os.File
	spillSize int64
	// logicalSize is the newest size this descriptor observed; `pushed` only
	// advances at push boundaries and is stale for passthrough writes.
	logicalSize int64
	// refs counts the claim plus every live claimReadHandle: the spill (and
	// the anonymous image) must outlive the WRITE descriptor while a
	// read-only re-open still serves it.
	refs        int
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
			if attr, ok, berr := h.setAttrBound(ctx, op); berr != nil {
				return fs.Attr{}, berr
			} else if ok {
				h.mu.Lock()
				h.pushed = size
				h.mu.Unlock()
				return attr, nil
			}
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
	if h.spill != nil {
		if op.Size >= 0 && op.Size != h.spillSize {
			if err := h.spill.Truncate(op.Size); err != nil {
				h.mu.Unlock()
				return fs.Attr{}, err
			}
			h.spillSize = op.Size
			h.dirty = true
		}
		attr := h.attrLocked()
		h.mu.Unlock()
		return attr, nil
	}
	h.mu.Unlock()
	if op.Size >= 0 && op.Size != int64(len(h.image)) {
		size := op.Size
		h.mu.Lock()
		h.logicalSize = size
		h.mu.Unlock()
		h.mu.Lock()
		if size < int64(len(h.image)) {
			h.image = h.image[:size]
			h.dirty = true
			h.mu.Unlock()
		} else if h.unlinked && (h.spill != nil || size > bulkfileMaxBufferedBytes) {
			// An anonymous grow stays on the claim's own disk image: the
			// below plane has no object for this name, and switching to
			// passthrough discarded the writes buffered here.
			if err := h.spillImageLocked(0, nil, int64(len(h.image))); err != nil {
				h.mu.Unlock()
				return fs.Attr{}, err
			}
			if err := h.spill.Truncate(size); err != nil {
				h.mu.Unlock()
				return fs.Attr{}, err
			}
			h.spillSize = size
			h.image = nil
			h.dirty = true
			attr := h.attrLocked()
			h.mu.Unlock()
			return attr, nil
		} else if size > bulkfileMaxBufferedBytes {
			// A zero-fill grow past the bound must not allocate the hole:
			// land the buffered prefix and express the truncation in the
			// plane below (its SetAttr holds the size without the bytes).
			h.mu.Unlock()
			if err := h.push(ctx); err != nil {
				return fs.Attr{}, err
			}
			if _, ok, berr := h.setAttrBound(ctx, op); berr != nil {
				return fs.Attr{}, berr
			} else if !ok {
				if err := h.below.SetAttr(ctx, h.path, fs.AttrPatch{Size: &size}); err != nil {
					return fs.Attr{}, err
				}
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
	unlinked := h.unlinked
	inner := h.inner
	h.mu.Unlock()
	if unlinked {
		return true
	}
	if a, ok := inner.(interface{ Anonymous() bool }); ok {
		return a.Anonymous()
	}
	return false
}

func (h *streamHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	h.mu.Lock()
	passthrough := h.passthrough
	inner := h.inner
	h.mu.Unlock()
	// A released writer may have handed its stream below and committed:
	// readers fall back to the claim's own image/spill, then the path.
	if inner == nil && h.hasOwnImage() {
		passthrough = false
	}
	if passthrough {
		if inner == nil {
			data, err := h.below.Read(ctx, h.path, off, int64(len(dest)))
			if err != nil {
				return 0, err
			}
			return copy(dest, data), nil
		}
		return inner.Read(ctx, off, dest)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.spill != nil {
		if off < 0 || off >= h.spillSize {
			return 0, nil
		}
		if want := h.spillSize - off; int64(len(dest)) > want {
			dest = dest[:want]
		}
		n, rerr := h.spill.ReadAt(dest, off)
		if rerr == io.EOF {
			rerr = nil
		}
		return n, rerr
	}
	if off >= int64(len(h.image)) {
		return 0, nil
	}
	n := copy(dest, h.image[off:])
	return n, nil
}

// captureReaderImage copies the bound object's bytes into the claim's own
// spill so a surviving reader keeps the OLD object after the writer's
// session is released (re-addressing the path would read a replacement).
func (h *streamHandle) captureReaderImage(ctx context.Context, backing fs.Handle, size int64) error {
	if size <= 0 {
		if st, err := h.below.GetAttr(ctx, h.path); err == nil {
			size = st.Size
		}
	}
	if size <= 0 {
		return nil
	}
	tmp, err := os.CreateTemp("", "drive9-bulk-read-*")
	if err != nil {
		return err
	}
	_ = os.Remove(tmp.Name())
	for off := int64(0); off < size; {
		chunk := int64(4 << 20)
		if off+chunk > size {
			chunk = size - off
		}
		part := make([]byte, chunk)
		n, rerr := backing.Read(ctx, off, part)
		if int64(n) != chunk || (rerr != nil && rerr != io.EOF) {
			_ = tmp.Close()
			return io.ErrUnexpectedEOF
		}
		if _, werr := tmp.WriteAt(part, off); werr != nil {
			_ = tmp.Close()
			return werr
		}
		off += chunk
	}
	h.mu.Lock()
	if h.spill == nil {
		h.spill, h.spillSize = tmp, size
		// The claim's own image is authoritative for its readers now.
		h.passthrough = false
	} else {
		_ = tmp.Close()
	}
	h.mu.Unlock()
	return nil
}

// hasOwnImage reports whether the claim still holds readable bytes of its
// own (image or spill).
func (h *streamHandle) hasOwnImage() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.spill != nil || len(h.image) > 0
}

// setAttrBound forwards a FD-directed attribute change to the BOUND handle
// below. Re-addressing the path would act on whatever object now owns the
// name (an anonymous fd's truncate landed on a same-name replacement).
func (h *streamHandle) setAttrBound(ctx context.Context, op *fs.Op) (fs.Attr, bool, error) {
	if op.Handle == nil {
		// A path-directed request has no descriptor identity to preserve;
		// the caller's path-based fallback is correct.
		return fs.Attr{}, false, nil
	}
	h.mu.Lock()
	inner := h.inner
	h.mu.Unlock()
	do, ok := h.below.(fs.HandlerFilesystem)
	if !ok || inner == nil {
		return fs.Attr{}, false, nil
	}
	// Copy the WHOLE request and only replace the handle: a combined
	// MODE+SIZE setattr must keep HasSize, or the write plane splits the
	// request, returns on the mode branch, and silently drops the truncate.
	bound := *op
	bound.Handle = inner
	res, err := do(ctx, &bound)
	if err != nil {
		return fs.Attr{}, true, err
	}
	if res.Attr != nil {
		return *res.Attr, true, nil
	}
	return fs.Attr{}, true, nil
}

// addRef/dropRef keep the anonymous image alive while a read-only re-open
// still serves it (the write descriptor's Release is not the last reference).
func (h *streamHandle) addRef() {
	h.mu.Lock()
	h.refs++
	h.mu.Unlock()
}

func (h *streamHandle) dropRef() {
	h.mu.Lock()
	h.refs--
	var toRelease fs.Handle
	lastRef := h.released && h.refs <= 0
	if lastRef {
		if h.spill != nil {
			_ = h.spill.Close()
			h.spill = nil
		}
		// The passthrough backing may still be serving the last reader:
		// release it only when the LAST reference goes away.
		toRelease = h.inner
		h.inner = nil
	}
	h.mu.Unlock()
	if toRelease != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_ = toRelease.Release(ctx)
		cancel()
	}
	if lastRef && h.b != nil {
		h.b.untrack(h)
	}
}

// spillImageLocked moves the buffered image into a temp file and writes the
// new range there. The caller holds h.mu.
func (h *streamHandle) spillImageLocked(off int64, data []byte, end int64) error {
	if h.spill == nil {
		tmp, err := os.CreateTemp("", "drive9-bulk-anon-*")
		if err != nil {
			return err
		}
		_ = os.Remove(tmp.Name())
		if len(h.image) > 0 {
			if _, werr := tmp.WriteAt(h.image, 0); werr != nil {
				_ = tmp.Close()
				return werr
			}
		}
		h.spill = tmp
		h.spillSize = int64(len(h.image))
		h.image = nil
	}
	if len(data) > 0 {
		if _, err := h.spill.WriteAt(data, off); err != nil {
			return err
		}
	}
	if end > h.spillSize {
		h.spillSize = end
	}
	return nil
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
		if end > h.logicalSize {
			h.logicalSize = end
		}
		h.mu.Unlock()
		inner, err := h.session(ctx)
		if err != nil {
			return err
		}
		return inner.Write(ctx, off, data)
	}
	// An unlinked claim has nowhere to forward. Small images stay in memory
	// (bounded); a large/sparse one spills to a temp file so a multi-GiB
	// offset cannot allocate the whole hole on the heap.
	if h.unlinked && (h.spill != nil || end > bulkfileMaxBufferedBytes) {
		if end > h.logicalSize {
			h.logicalSize = end
		}
		if err := h.spillImageLocked(off, data, end); err != nil {
			h.mu.Unlock()
			return err
		}
		h.dirty = true
		h.mu.Unlock()
		return nil
	}
	if h.unlinked || end <= bulkfileMaxBufferedBytes {
		if end > h.logicalSize {
			h.logicalSize = end
		}
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
	old := h.inner
	h.inner = nil // re-opened lazily at the CURRENT shared state
	h.pushed = 0
	h.mu.Unlock()
	if old != nil {
		// The replaced session is still tracked by the plane below (the
		// writeback liveSet): dropping it without a Release left a ghost
		// writer that later deferred a chmod onto a handle nobody closes.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_ = old.Release(ctx)
		cancel()
	}
	// The flushed write-plane handle below keeps serving its own landed
	// buffer (read-your-writes prefers it), which would answer the
	// original descriptor's reads with the pre-handoff image forever.
	// Dropping the session makes every later read/write open a fresh
	// handle against the shared state the second writer now owns.
	// Retire the pathname CLAIM only: this descriptor is still open, and a
	// later rename must retarget it (untracking it here left the fd
	// re-opening the OLD name — a surrendered fd recreated /a with its
	// O_CREAT flags after /a had moved to /b).
	if h.b != nil {
		h.b.untrackClaim(h)
	}
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
	keepTracked := false
	h.mu.Lock()
	keepTracked = h.refs > 1
	h.mu.Unlock()
	if release && h.b != nil && !keepTracked {
		h.b.untrack(h)
	}
	inner, serr := h.session(ctx)
	if serr != nil && pushErr == nil {
		pushErr = serr
	}
	// The write descriptor's close COMMITS: the below handle's Release is
	// what submits the writeback snapshot, so it must run now — deferring
	// it to the last claim reader left a closed file unpublished. Readers
	// get a BOUND copy of the object's bytes first (a path read would bind
	// a same-name replacement).
	h.mu.Lock()
	backing := h.inner
	// Freeze a reader image ONLY for a detached object (unlinked or about
	// to be replaced): a still-linked object must stay visible to the
	// reader through the shared state (later legitimate writes must be
	// seen), not a private snapshot.
	needReaderImage := h.refs > 1 && backing != nil && h.passthrough && h.unlinked
	size := h.logicalSize
	if size <= 0 {
		size = h.attrLocked().Size
	}
	h.mu.Unlock()
	if needReaderImage {
		if cerr := h.captureReaderImage(ctx, backing, size); cerr != nil {
			// The handover failed: KEEP the bound session for the reader
			// instead of silently switching it to the reused pathname.
			h.dropRef()
			return cerr
		}
	}
	h.mu.Lock()
	h.inner = nil
	h.mu.Unlock()
	released := map[fs.Handle]bool{}
	if backing != nil {
		released[backing] = true
		if err := backing.Release(ctx); err != nil && pushErr == nil {
			pushErr = err
		}
	}
	if inner != nil && !released[inner] {
		// A session opened during Release after a retarget cleared h.inner:
		// it was never stored, so this is its only release (leaking it kept
		// the below registry entry alive after every fd closed).
		if err := inner.Release(ctx); err != nil && pushErr == nil {
			pushErr = err
		}
	}
	h.dropRef()
	return pushErr
}

// attrLocked renders the handle's reply attributes; callers hold h.mu.
func (h *streamHandle) attrLocked() fs.Attr {
	mode := h.mode
	if !h.modeKnown {
		mode = 0o644
	}
	size := int64(len(h.image))
	if h.spill != nil {
		size = h.spillSize
	}
	if h.passthrough && h.image == nil {
		// The stream lives in the plane below now: prefer the size this
		// descriptor observed (pushed only advances at push boundaries).
		size = h.pushed
		if h.logicalSize > size {
			size = h.logicalSize
		}
	}
	if h.logicalSize > size && h.spill == nil && !h.passthrough {
		size = h.logicalSize
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

// Anonymous forwards the claim's anonymous identity: an outer path-keyed
// cache must bypass for a read-only re-open of an unlinked/overwritten
// object.
func (c *claimReadHandle) Anonymous() bool {
	return c.claim.Anonymous()
}

func (c *claimReadHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	c.claim.mu.Lock()
	image := c.claim.image
	spill := c.claim.spill
	passthrough := c.claim.passthrough
	c.claim.mu.Unlock()
	if spill != nil {
		return c.claim.Read(ctx, off, dest)
	}
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

func (c *claimReadHandle) Sync(ctx context.Context) error  { return nil }
func (c *claimReadHandle) Flush(ctx context.Context) error { return nil }
func (c *claimReadHandle) Release(ctx context.Context) error {
	if c.claim != nil {
		c.claim.dropRef()
		c.claim = nil
	}
	return nil
}
