// The staging handle: the takeover's claim on a creating writer. The
// image is buffered in memory (read-your-writes, no network during Write)
// while it stays below the promotion line — the server's inline cutoff,
// the size at which the classic plane would fall back to the
// multipart/presign direct-upload link; the durability boundaries —
// Flush, Fsync, Release — push it into the write plane below and delegate
// to it. Crossing the line promotes the claim onto the extent plane: the
// below session is discarded (its lazy create is unlinked so its Release
// cannot materialize an empty classic file next to the extent
// projection), the buffered image is created and written through the
// JuiceFS VFS in one step, and every later operation serves through the
// promoted extent handle. A promotion that cannot build the runtime falls
// back to the classic plane for good — the claim behaves exactly like a
// pure buffering claim from then on.
package extent

import (
	"context"
	"io"
	"os"
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
	e *Extent
	// path is the below-plane path (kept for the session and pushes); the
	// ROUTER's key is routerPath, owned and mutated under pathRouter.mu so
	// map operations never read a path field another lock guards.
	path string
	// routerPath is the pathRouter staging-map key: set under router.mu at
	// claim time, moved under router.mu by retargetStaging. untrackStaging
	// reads it under the same lock, so a Release racing a rename can never
	// miss its entry and leak the claim (and its image) for the mount's
	// lifetime.
	routerPath string
	caller     fs.Caller
	below      fs.HandlerFilesystem
	inner      fs.Handle
	openFlags  uint32 // claim flags for re-opening the session below after a rename

	mu    sync.Mutex
	image []byte
	mode  uint32
	// modeKnown records that mode carries an EXPLICIT mode (create or
	// chmod): an absent mode defaults to 0644, an explicit 000 or a setuid
	// bit is reported and applied as-is.
	modeKnown bool
	pushed    int64 // image size already expressed below; a shorter image needs a truncation
	dirty     bool  // image differs from the state expressed below
	// promoted serves every operation once the claim moved to the extent
	// plane; image is released with the swap.
	promoted *extentHandle
	// promoting single-flights the promotion against writers that cross
	// the threshold while the move is between its phases.
	promoting bool
	// surrendering marks the second-writer handoff between its phases
	// (flush + below-session release while h.mu is released). A Write
	// that arrives in the window must NOT append to the image — the
	// handoff clears it — so it waits on handoffCond until the claim is
	// fully passthrough, and only then routes its bytes (nothing
	// acknowledged is discarded).
	surrendering bool
	handoffCond  *sync.Cond
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
	// spill serves an open-unlinked claim with no below session: a local
	// temporary file keeps the descriptor writable without buffering the
	// logical hole in memory.
	spill     *os.File
	spillSize int64

	// testHookFlush, when set, runs at the top of Flush (with NO locks
	// held): tests use it to hold the surrender's flush phase open and
	// place a write inside the handoff window deterministically.
	testHookFlush func()
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
		// The promoted inode is the attribute authority: size, mode, owner
		// and times apply through the VFS in ONE call (the session below
		// was discarded at promotion, and the classic chmod would not
		// touch the jfs inode the stats read). The descriptor's own fh
		// rides the call: fh=0 would make JuiceFS treat an FD truncate as
		// a PATH truncate and re-check permissions against the CURRENT
		// mode.
		if op.Size >= 0 || op.HasMode || op.HasSetUID || op.HasSetGID || !op.Mtime.IsZero() {
			res, err := h.e.setAttrViaVFS(ctx, op, uint64(promoted.ino), promoted.fh)
			if err != nil {
				return fs.Attr{}, err
			}
			if res.Attr != nil {
				return *res.Attr, nil
			}
		}
		return h.e.promotedAttr(ctx, promoted)
	}

	h.mu.Lock()
	spilled := h.spill
	h.mu.Unlock()
	if spilled != nil {
		// The spill is the object (anonymous claim, no below session):
		// size changes truncate/extend the spill directly, and mode
		// changes only adjust the reported mode — an image edit would be
		// invisible to reads and attrs.
		if op.Size >= 0 {
			if err := spilled.Truncate(op.Size); err != nil {
				return fs.Attr{}, err
			}
			h.mu.Lock()
			h.spillSize = op.Size
			h.mu.Unlock()
		}
		if op.HasMode {
			h.mu.Lock()
			h.mode = op.Mode & 0o7777
			h.modeKnown = true
			h.mu.Unlock()
		}
		h.mu.Lock()
		a := h.attrLocked()
		h.mu.Unlock()
		return a, nil
	}
	if op.HasMode {
		mode := op.Mode & 0o7777
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
		// The stream lives in the plane below now. The setattr must be
		// DESCRIPTOR-bound: forwarded with the below session's handle, so
		// an ftruncate on this fd truncates the opened object even after a
		// rename moved the path and a replacement took the old name (a
		// path-bound SetAttr would truncate the replacement instead).
		inner, serr := h.session(ctx)
		if serr != nil {
			return fs.Attr{}, serr
		}
		if inner != nil {
			fop := *op
			fop.Handle = inner
			res, ferr := h.below(ctx, &fop)
			if ferr != nil {
				return fs.Attr{}, ferr
			}
			if res.Attr != nil {
				return *res.Attr, nil
			}
		}
		// No session (anonymous claim): the image is empty; a size change
		// has nothing local to edit — report the current attributes.
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
			// A zero-fill grow past the bound must not allocate the hole.
			// A promotable claim promotes instead — the VFS SetAttr
			// expresses the size sparsely (a hole costs nothing); only a
			// failed promotion parks classically (push + below SetAttr +
			// passthrough), the legacy multipart path. Every path below
			// returns with h.mu held: the function's defer unlocks once.
			if h.promotable && !h.promoting {
				h.mu.Unlock()
				perr := h.promote(ctx)
				h.mu.Lock()
				if perr != nil {
					return fs.Attr{}, perr
				}
				if promoted := h.promoted; promoted != nil {
					h.mu.Unlock()
					res, aerr := h.e.setAttrViaVFS(ctx, op, uint64(promoted.ino), promoted.fh)
					h.mu.Lock()
					if aerr != nil {
						return fs.Attr{}, aerr
					}
					if res.Attr != nil {
						return *res.Attr, nil
					}
					h.mu.Unlock()
					a, prerr := h.e.promotedAttr(ctx, promoted)
					h.mu.Lock()
					if prerr != nil {
						return fs.Attr{}, prerr
					}
					return a, nil
				}
				// The promotion parked classically (runtime unavailable):
				// the classic forward below takes over, lock held.
			}
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

// surrender hands the path to the planes below for a SECOND writable open:
// the buffered image lands there (one push, synchronous) and this
// descriptor switches to passthrough, so every fd of the path shares the
// lower plane's coherent per-path state instead of holding a private full
// image whose later push would overwrite the other writer's committed
// bytes. The claim settles classically from here on (promotion is off): a
// file written through two descriptors at once is exactly the shape the
// buffered single-writer claim is not. This is the handoff the bulk data
// plane (bulkfile) implemented before the takeover subsumed it.
func (h *stagingHandle) surrender(ctx context.Context) error {
	h.mu.Lock()
	if h.released || h.unlinked || h.promoted != nil || h.passthrough {
		h.mu.Unlock()
		return nil
	}
	// From here on, concurrent Writes must not take the image path: the
	// handoff flushes the image and then clears it. They wait on the cond
	// until the claim is fully passthrough, so an acknowledged write in
	// the window can never be discarded (the previous race: append under
	// passthrough=false, then image=nil dropped the bytes and the next
	// push truncated the shared file to the stale image's length).
	h.surrendering = true
	h.mu.Unlock()

	// A full Flush, not a bare push: the bytes must be in the lower
	// plane's PENDING state (staged/queued), which is what a second open
	// of the path reads while the landing is in flight. A write that
	// raced the first flush's unlocked window appended to the image and
	// made it dirty again — flush a second time (the window is closed to
	// new writers now) so the second writer opens onto every acknowledged
	// byte.
	for range 2 {
		if err := h.Flush(ctx); err != nil {
			h.mu.Lock()
			h.surrendering = false
			h.handoffCond.Broadcast()
			h.mu.Unlock()
			return err
		}
	}

	// Release the private below session: every later access re-enters the
	// lower plane BY PATH (or through a freshly opened handle), so this
	// descriptor sees the second writer's committed bytes instead of a
	// session-local buffer snapshot.
	if inner := h.detachBelowLocked(); inner != nil {
		_ = inner.Release(ctx)
	}
	h.mu.Lock()
	h.passthrough = true
	h.promotable = false
	h.image = nil // the bytes now live in the plane below
	h.dirty = false
	h.pushed = 0
	h.surrendering = false
	h.handoffCond.Broadcast()
	h.mu.Unlock()
	// The claim no longer owns the path's freshest state: stop answering
	// the path's lookups from this handle.
	h.e.paths.untrackStaging(h)
	return nil
}

// stagingMaxBufferedBytes bounds the claimed image retained in memory
// before the claim hands the stream to the plane below (whose own
// buffering spills to parts/shadow files). The promotion line (the inline
// cutoff) sits far below this bound, so a promotable claim promotes well
// before the bound; a write ending far beyond either must not allocate
// the full logical image (one byte at 64 MiB must not allocate 64 MiB).
const stagingMaxBufferedBytes = 8 << 20

// memBoundLocked is the image bound while the caller holds h.mu.
func (h *stagingHandle) memBoundLocked() int64 {
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
	if h.spill != nil {
		if off < 0 || off >= h.spillSize {
			return 0, nil
		}
		n, err := h.spill.ReadAt(dest, off)
		if err == io.EOF {
			err = nil
		}
		return n, err
	}
	if off < 0 || off >= int64(len(h.image)) {
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
		return h.e.readViaVFS(ctx, h.path, uint64(promoted.ino), off, limit, h.caller)
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
	for h.surrendering {
		// A handoff is mid-flight (flush/session-release with h.mu
		// released): the image is about to be cleared, so appending here
		// would discard acknowledged bytes. Wait for the passthrough
		// switch and route through the plane below instead.
		h.handoffCond.Wait()
	}
	if h.spill != nil {
		// The anonymous claim spilled: from here the spill file IS the
		// object — every later write lands on it (the in-memory image is
		// gone), so overwrites and rewrites stay visible to reads.
		h.mu.Unlock()
		return h.spillWrite(ctx, off, data)
	}
	if h.passthrough {
		h.mu.Unlock()
		inner, err := h.session(ctx)
		if err != nil {
			return err
		}
		return inner.Write(ctx, off, data)
	}
	// A write that crosses the inline cutoff takes the promotion (the
	// crossing write itself may exceed the cutoff by one chunk); a write
	// landing far beyond it is a sparse jump — it takes the promotion TOO
	// (the VFS writes sparsely, a hole costs nothing) and the jump's bytes
	// land on the promoted handle. Only a promotion that cannot build the
	// runtime falls back to the classic forward below (the legacy
	// multipart path).
	threshold := h.e.inlineThreshold()
	crossing := h.promotable && !h.promoting && end >= threshold &&
		end <= threshold+stagingMaxBufferedBytes
	// An open-unlinked claim buffers like any other, but only within the
	// memory bound: a sparse jump on an anonymous fd must not allocate the
	// hole (nothing can be gained — the boundary pushes are discarded and
	// promotion refuses unlinked claims). Past the bound the write forwards
	// to the below session when one exists (its buffering spills to shadow
	// files) and fails ENOSPC when it does not.
	unlinkedBounded := h.unlinked && end <= h.memBoundLocked()
	if crossing || unlinkedBounded || end <= h.memBoundLocked() {
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
	if h.promotable && !h.promoting {
		// The sparse jump: promote the buffered prefix first (the image
		// may be empty — the inode is created regardless), then land the
		// jump's bytes sparsely through the extent handle.
		h.mu.Unlock()
		if err := h.promote(ctx); err != nil {
			return err
		}
		h.mu.Lock()
		promoted := h.promoted
		h.mu.Unlock()
		if promoted != nil {
			return promoted.Write(ctx, off, data)
		}
		// The promotion parked classically (runtime unavailable): the
		// classic forward below takes over — the legacy multipart path.
	} else {
		h.mu.Unlock()
	}
	if h.unlinked {
		// Nothing to land (pushes are discarded for anonymous claims) and
		// nowhere to re-open: forward to the detached session if the claim
		// ever got one. With NO session the claim spills to a local
		// temporary file instead of refusing — POSIX keeps an open-unlinked
		// descriptor writable — and never allocates the hole in memory.
		// (Entered with h.mu already released by the routing above.)
		inner, serr := h.session(ctx)
		if serr != nil {
			return serr
		}
		if inner != nil {
			h.mu.Lock()
			h.passthrough = true
			h.promotable = false
			h.image = nil
			h.mu.Unlock()
			return inner.Write(ctx, off, data)
		}
		return h.spillWrite(ctx, off, data)
	}
	// The image would exceed the memory bound and no extent plane could be
	// built: land what is buffered and hand the stream to the write plane
	// below — its buffering spills to parts/shadow files instead of one
	// slice. Failures keep the claim dirty so the next boundary retries
	// the landing.
	if err := h.push(ctx); err != nil {
		return err
	}
	h.mu.Lock()
	if promoted := h.promoted; promoted != nil {
		// A concurrent promotion landed while this write was between its
		// decision and the landing (h.mu serializes the two — at this
		// point the promotion's outcome is settled): route the bytes
		// through the extent handle instead of re-creating a classic file
		// beside the projection.
		h.mu.Unlock()
		return promoted.Write(ctx, off, data)
	}
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
	spill := h.spill
	h.mu.Unlock()
	if promoted != nil {
		return promoted.Sync(ctx)
	}
	if spill != nil {
		// The anonymous claim's bytes live in the local spill: its fsync
		// IS the durability boundary (there is no below session to carry
		// one — session() returns nil, nil for a detached claim).
		return spill.Sync()
	}
	if err := h.push(ctx); err != nil {
		return err
	}
	inner, err := h.session(ctx)
	if err != nil {
		return err
	}
	if inner == nil {
		// An unlinked claim with no below session: the image is the whole
		// object and nothing remote to sync — the boundary is a no-op.
		// (Previously this dereferenced the nil session and panicked.)
		return nil
	}
	// Passthrough claims: the push no-ops (dirty was cleared at the
	// forward) and the inner session carries the boundary.
	return inner.Sync(ctx)
}

func (h *stagingHandle) Flush(ctx context.Context) error {
	if hook := h.testHookFlush; hook != nil {
		hook()
	}
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
	if inner == nil {
		h.mu.Lock()
		spill := h.spill
		h.mu.Unlock()
		if spill != nil {
			return spill.Sync()
		}
		// Detached anonymous claim with nothing to flush: a no-op, never
		// a nil-deref panic on the absent below session.
		return nil
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
	h.closeSpillLocked()
	h.mu.Unlock()
	if inner != nil {
		if err := inner.Release(ctx); err != nil && pushErr == nil {
			pushErr = err
		}
	}
	return pushErr
}

// spillWrite lands a write on the claim's local spill file, creating it
// (seeded with the buffered image) on first use. Only open-unlinked claims
// without a below session reach here: the spill keeps the descriptor
// writable without allocating sparse holes in memory.
func (h *stagingHandle) spillWrite(ctx context.Context, off int64, data []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.spill == nil {
		tmp, err := os.CreateTemp("", "drive9-claim-spill-*")
		if err != nil {
			return err
		}
		_ = os.Remove(tmp.Name()) // keep the fd anonymous like every other spill
		if len(h.image) > 0 {
			if _, err := tmp.WriteAt(h.image, 0); err != nil {
				_ = tmp.Close()
				return err
			}
			h.spillSize = int64(len(h.image))
		}
		h.spill = tmp
		h.image = nil
	}
	if _, err := h.spill.WriteAt(data, off); err != nil {
		return err
	}
	if end := off + int64(len(data)); end > h.spillSize {
		h.spillSize = end
	}
	return nil
}

// closeSpillLocked releases the spill file (callers hold h.mu).
func (h *stagingHandle) closeSpillLocked() {
	if h.spill != nil {
		_ = h.spill.Close()
		h.spill = nil
	}
}

// detachBelowLocked drops the private below session (the bytes stay in the
// lower plane's per-path pending state) and returns the released handle.
func (h *stagingHandle) detachBelowLocked() fs.Handle {
	h.mu.Lock()
	inner := h.inner
	h.inner = nil
	h.mu.Unlock()
	return inner
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
	if h.spill != nil {
		// The spill owns the object; there is no below session to push
		// to (and opening one by the recycled path would be wrong).
		return nil
	}
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
	ino, fh, cattr, errn := h.e.createInVFS(ctx, &fs.Op{Path: h.path, Mode: h.mode, HasMode: true, Flags: openFlags, Caller: h.caller})
	if errn != 0 || ino == 0 {
		h.promotable = false
		return nil
	}
	h.e.paths.rememberIno(h.path, ino)
	var handle *extentHandle
	var oerr error
	if fh != 0 {
		// Reuse the fh VFS.Create allocated (a second Open leaked the
		// first VFS file reference forever).
		handle, _, _, oerr = h.e.adoptCreatedHandle(h.path, ino, fh, cattr, openFlags, h.caller)
	} else {
		handle, _, _, oerr = h.e.openExtent(ctx, h.path, ino, openFlags, h.caller)
	}
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
	// An explicit mode is reported as-is — 000 included; only an ABSENT
	// mode falls back to the 0644 default.
	mode := h.mode
	if mode == 0 && !h.modeKnown {
		mode = 0o644
	}
	size := int64(len(h.image))
	if h.spill != nil {
		size = h.spillSize
	}
	return fs.Attr{Kind: fs.KindFile, Mode: mode, Size: size}
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

// newClaimReadHandle builds the read-only re-open with a snapshot of the
// image at open time: the descriptor keeps those bytes readable after the
// writer surrenders, releases or is unlinked.
func newClaimReadHandle(h *stagingHandle) *claimReadHandle {
	h.mu.Lock()
	image := append([]byte(nil), h.image...)
	h.mu.Unlock()
	return &claimReadHandle{claim: h, image: image}
}

// claimReadHandle serves reads of a live claim's pending image (a
// read-only re-open before the boundary push). It carries a SNAPSHOT of
// the image at open time: if the writer later surrenders, releases or is
// unlinked, the still-open reader keeps reading the bytes that were
// visible when it opened instead of hitting EOF on the emptied claim.
type claimReadHandle struct {
	claim *stagingHandle
	image []byte
}

func (c *claimReadHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	c.claim.mu.Lock()
	// A live buffered claim serves from its (growing) image — the reader
	// sees the writer's newest bytes, matching two fds on one inode. The
	// copy runs UNDER the claim lock: the writer mutates the same backing
	// array under this lock, and copying after unlocking raced it.
	if !c.claim.released && !c.claim.unlinked && c.claim.promoted == nil && !c.claim.passthrough {
		if off < 0 || off >= int64(len(c.claim.image)) {
			c.claim.mu.Unlock()
			return 0, nil
		}
		n := copy(dest, c.claim.image[off:])
		c.claim.mu.Unlock()
		return n, nil
	}
	if c.claim.promoted != nil && !c.claim.released {
		// Promotion moved the object to the extent plane and the handle is
		// still live: serve through it (no snapshot copy needed).
		c.claim.mu.Unlock()
		return c.claim.Read(ctx, off, dest)
	}
	// The writer surrendered, released or was unlinked: the opened
	// descriptor keeps reading its OWN snapshot (the bytes visible at open
	// time), never EOF-from-emptied-state.
	c.claim.mu.Unlock()
	if off < 0 || off >= int64(len(c.image)) {
		return 0, nil
	}
	return copy(dest, c.image[off:]), nil
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
