package writeback

// #1023 (#1024 port): append-image composition. Independent O_APPEND
// descriptors (and hardlink aliases) of one file must never lose an
// acknowledged record: a write composes onto the path's CURRENT newest
// image — the newest live sibling buffer, or the landed image when a
// sibling commit advanced it past this handle's base. The composition
// relation is the append-prefix rule (v1's "verified descendant" for
// append workloads): whichever image contains the other as a prefix wins;
// diverged images retain their data and reject further appends with
// EAGAIN until the conflict is resolved.

import (
	"bytes"
	"context"
	"io"
	"syscall"

	"github.com/mem9-ai/drive9/pkg/fusecomp/writebuffer"
)

// appendRefreshMaxBytes bounds the landed image fetched for composition
// (v1 maxLandedPayloadBytes): an append handle whose prospective image
// exceeds it refuses to compose in memory.
const appendRefreshMaxBytes = 1 << 20

// appendParticipant reports whether the handle participates in append
// composition: an O_APPEND descriptor, or any handle that adopted a
// sibling image (its buffer no longer describes only its own writes).
func (h *bufferingHandle) appendParticipant() bool {
	return h.flags&uint32(syscall.O_APPEND) != 0 || h.appendSnapshot
}

// appendCandidatePaths lists the paths whose newest state this handle's
// appends must compose onto: its own path plus every still-linked alias
// of the same inode (hardlinks share one file).
func (w *Writeback) appendCandidatePaths(h *bufferingHandle) []string {
	paths := []string{h.path}
	if w.inodes == nil {
		return paths
	}
	ino, ok := w.inodes.Ino(h.path)
	if !ok || ino == 0 {
		return paths
	}
	aliaser, ok := w.inodes.(interface{ Aliases(ino uint64) []string })
	if !ok {
		return paths
	}
	for _, alias := range aliaser.Aliases(ino) {
		if alias != h.path && alias != "" {
			paths = append(paths, alias)
		}
	}
	return paths
}

// newestLiveAppendImage returns the newest dirty image among this path's
// live handles (any handle of the same file — a sibling writer, an alias
// descriptor), or nil when no sibling carries state newer than mine.
// The comparison is the append-prefix rule over materialized images.
func (w *Writeback) newestLiveAppendImage(h *bufferingHandle, mine []byte) []byte {
	candidates := make(map[string]struct{})
	for _, path := range w.appendCandidatePaths(h) {
		candidates[path] = struct{}{}
	}
	var best []byte
	// liveSet holds EVERY open handle (w.live keeps only the newest per
	// path — a later open would otherwise hide the sibling writer).
	w.liveSet.Range(func(key, _ any) bool {
		sib, ok := key.(*bufferingHandle)
		if !ok || sib == h {
			return true
		}
		sib.mu.Lock()
		eligible := !sib.unlinked && sib.buf != nil && sib.buf.Size() > 0 && sib.buf.CanMaterializeFull()
		var img []byte
		if eligible {
			img = append([]byte(nil), sib.buf.Bytes()...)
		}
		path := sib.path
		sib.mu.Unlock()
		if len(img) == 0 {
			return true
		}
		if _, relevant := candidates[path]; !relevant {
			return true
		}
		if appendImageNewer(img, best) {
			best = img
		}
		return true
	})
	if len(mine) == 0 {
		return best
	}
	if appendImageNewer(best, mine) {
		return best
	}
	return nil
}

// appendImageNewer reports whether a is a strictly newer append image
// than b under the prefix rule: a extends b, or b is empty/nil.
func appendImageNewer(a, b []byte) bool {
	if len(b) == 0 {
		return len(a) > 0
	}
	return len(a) > len(b) && bytes.HasPrefix(a, b)
}

// landedAppendImage fetches the committed image for the path when the
// remote advanced past baseRev (bounded). ok=false when the landed state
// is not newer, unavailable, or too large to compose.
func (w *Writeback) landedAppendImage(ctx context.Context, path string, baseRev int64) ([]byte, int64, bool) {
	if w.opts.Client == nil {
		return nil, 0, false
	}
	rp := w.remotePath(path)
	st, err := w.opts.Client.StatCtx(ctx, rp)
	if err != nil || st == nil || st.IsDir || st.Revision <= baseRev || st.Size > appendRefreshMaxBytes {
		return nil, 0, false
	}
	rc, err := w.opts.Client.ReadStreamRange(ctx, rp, 0, st.Size)
	if err != nil {
		return nil, 0, false
	}
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(io.LimitReader(rc, appendRefreshMaxBytes+1))
	if err != nil || int64(len(data)) != st.Size {
		return nil, 0, false
	}
	return data, st.Revision, true
}

// landedAdvanced reports whether any candidate path's image moved past
// baseRev. The process-local landed table answers first (v1's
// latestCommittedRevision watermark — a remote stat per append write
// would price a network round trip into every record); foreign mounts'
// advances surface at the upload CAS and are resolved by the refresh
// there.
func (w *Writeback) landedAdvanced(ctx context.Context, h *bufferingHandle, baseRev int64) (string, int64, bool) {
	for _, path := range w.appendCandidatePaths(h) {
		if attr, ok := w.landedAttr(path); ok && attr.Rev > baseRev {
			return path, attr.Rev, true
		}
	}
	return "", 0, false
}

// refreshAppendBase composes this handle's image with the file's newest
// state before another append (or an append staging boundary) uses it:
// the newest live sibling image first, then the landed image when the
// remote advanced. Returns the composed image (nil = keep mine) and the
// newest landed revision seen. syscall.EAGAIN keeps acknowledged data but
// rejects further composition (diverged images: v1's retain-and-reject).
func (w *Writeback) refreshAppendBase(ctx context.Context, h *bufferingHandle) ([]byte, int64, error) {
	h.mu.Lock()
	if h.unlinked || h.buf == nil {
		h.mu.Unlock()
		return nil, 0, nil
	}
	// An empty image adopts any newest state: it has no records to lose.
	// A non-materializable buffer (spilled parts) cannot compose here.
	var mine []byte
	if h.buf.CanMaterializeFull() {
		mine = append([]byte(nil), h.buf.Bytes()...)
	} else {
		h.mu.Unlock()
		return nil, 0, nil
	}
	baseRev := h.baseRev
	h.mu.Unlock()

	// Sibling live images: newest wins under the prefix rule; a diverged
	// sibling (records mine does not contain, missing mine's records)
	// must not be silently dropped by either side.
	if sib := w.newestLiveAppendImage(h, mine); sib != nil {
		if len(mine) == 0 || bytes.HasPrefix(sib, mine) {
			mine = sib
		} else if !bytes.HasPrefix(mine, sib) {
			return nil, 0, syscall.EAGAIN
		}
	}

	// Landed image: only when the remote advanced past our base. A
	// diverged landed image (neither side contains the other) retains
	// local data and rejects the append (v1's zero-byte EAGAIN).
	newestRev := int64(0)
	if path, _, advanced := w.landedAdvanced(ctx, h, baseRev); advanced {
		data, lrev, ok := w.landedAppendImage(ctx, path, baseRev)
		if ok {
			if len(mine) == 0 || bytes.HasPrefix(data, mine) {
				mine, newestRev = data, lrev
			} else if !bytes.HasPrefix(mine, data) {
				return nil, 0, syscall.EAGAIN
			} else {
				newestRev = lrev
			}
		}
	}
	return mine, newestRev, nil
}

// fenceRefreshAppendBase is the staging-boundary refresh: unlike the
// write-time refresh (process-local watermarks only), the fence also
// checks the remote — stageToWriteCache refreshes the CAS base to the
// current remote revision before staging, and adopting that revision
// without the foreign bytes would let the conditional PUT overwrite a
// concurrent foreign append (v1's bounded remote read at the stage
// fence).
func (w *Writeback) fenceRefreshAppendBase(ctx context.Context, h *bufferingHandle) ([]byte, int64, error) {
	h.mu.Lock()
	if h.unlinked || h.buf == nil || !h.buf.CanMaterializeFull() {
		h.mu.Unlock()
		return nil, 0, nil
	}
	mine := append([]byte(nil), h.buf.Bytes()...)
	baseRev := h.baseRev
	h.mu.Unlock()

	if sib := w.newestLiveAppendImage(h, mine); sib != nil {
		if len(mine) == 0 || bytes.HasPrefix(sib, mine) {
			mine = sib
		} else if !bytes.HasPrefix(mine, sib) {
			return nil, 0, syscall.EAGAIN
		}
	}
	newestRev := int64(0)
	for _, path := range w.appendCandidatePaths(h) {
		if attr, ok := w.landedAttr(path); ok && attr.Rev > baseRev {
			data, lrev, ok := w.landedAppendImage(ctx, path, baseRev)
			if ok {
				if len(mine) == 0 || bytes.HasPrefix(data, mine) {
					mine, newestRev = data, lrev
				} else if !bytes.HasPrefix(mine, data) {
					return nil, 0, syscall.EAGAIN
				} else if lrev > newestRev {
					newestRev = lrev
				}
			}
			break
		}
	}
	// The remote check: a foreign commit is invisible to every local
	// table, and the staging's own base refresh would otherwise adopt its
	// revision silently.
	if w.opts.Client != nil {
		for _, path := range w.appendCandidatePaths(h) {
			st, err := w.opts.Client.StatCtx(ctx, w.remotePath(path))
			if err != nil || st == nil || st.IsDir {
				continue
			}
			if st.Revision <= baseRev {
				continue
			}
			data, lrev, ok := w.landedAppendImage(ctx, path, baseRev)
			if !ok {
				continue
			}
			if len(mine) == 0 || bytes.HasPrefix(data, mine) {
				mine, newestRev = data, lrev
			} else if !bytes.HasPrefix(mine, data) {
				return nil, 0, syscall.EAGAIN
			} else if lrev > newestRev {
				newestRev = lrev
			}
			break
		}
	}
	return mine, newestRev, nil
}

// adoptAppendImageLocked replaces the handle's buffer with a composed
// append image (h.mu held): the new image contains every record the old
// one did (the prefix rule ran in refreshAppendBase), so this is a
// rebase, not a loss. landedRev > 0 also advances the CAS base.
func (h *bufferingHandle) adoptAppendImageLocked(image []byte, landedRev int64) error {
	// maxSize 0 keeps the default cap: sizing to the adopted image alone
	// rejected the caller's subsequent append at the old tail.
	next := writebuffer.NewWriteBuffer(h.path, 0, 0)
	if len(image) > 0 {
		if _, err := next.Write(0, image); err != nil {
			return err
		}
	}
	next.ClearDirty()
	// The handle's own writes must still flush: re-dirty so the commit
	// boundary republishes the composed image (the caller's write lands
	// on top immediately after).
	h.buf = next
	h.appendSnapshot = true
	if landedRev > h.baseRev {
		h.baseRev = landedRev
		h.isNew = false
	}
	return nil
}
