// ftruncate.go ports #984 (fd-truncate contents exposed to sibling
// handles) onto the write plane's handle registry. POSIX makes an
// ftruncate(fd, n) visible to every fd of the inode at once, but the
// mutated image lives only in the truncating handle's buffer until the
// commit boundary — siblings, hardlink-alias readers and re-opens must
// serve that buffer (or the staged image it produced) instead of the
// stale remote bytes, and a shrink must never resurrect truncated-away
// bytes when the file regrows.
package writeback

import (
	"context"
	"sort"
	"syscall"
	"time"

	"github.com/mem9-ai/drive9/pkg/fusecomp/writebuffer"
)

// Bounded busy-wait policy for sibling visibility (v1
// samePathDirtyWaitTimeout / samePathDirtyWaitInterval): a reader that
// collides with a busy same-inode WRITABLE handle retries briefly and then
// fails closed (EAGAIN) — never blocks indefinitely and never serves
// bytes it cannot prove are the newest. Busy read-only handles cannot
// change the image and never trigger a retry.
const (
	samePathDirtyWaitTimeout  = 250 * time.Millisecond
	samePathDirtyWaitInterval = time.Millisecond
)

// errFtruncateBusy reports an fd-truncate admission that could not
// reconcile with a busy sibling within its budget (v1 errFtruncateBusy).
var errFtruncateBusy = syscall.EAGAIN

// identityOf snapshots a handle's (inode, path) under its lock and
// resolves the inode lazily from the facade's table (OPEN and SETATTR
// carry it on the op; creates only learn theirs once the kernel assigns
// the entry). Callers must not read h.path bare: rename/retarget writes
// it under the same lock, and an unlocked read is a data race that can
// also key a lookup by a path the fd no longer has.
func (w *Writeback) identityOf(h *bufferingHandle) (uint64, string) {
	h.mu.Lock()
	ino, path := h.ino, h.path
	h.mu.Unlock()
	if ino != 0 || w.inodes == nil {
		return ino, path
	}
	if resolved, ok := w.inodes.Ino(path); ok {
		h.mu.Lock()
		if h.ino == 0 {
			h.ino = resolved
		}
		h.mu.Unlock()
		return resolved, path
	}
	return 0, path
}

// inoOf is identityOf's inode half.
func (w *Writeback) inoOf(h *bufferingHandle) uint64 {
	ino, _ := w.identityOf(h)
	return ino
}

// inodeAliasPaths lists every pathname currently bound to the inode —
// the hardlink survivor set (v1 ftruncateAliasPaths). The named path is
// always included so callers work before the table learns the binding.
func (w *Writeback) inodeAliasPaths(ino uint64, path string) []string {
	paths := map[string]struct{}{path: {}}
	if ino != 0 {
		if aliaser, ok := w.inodes.(interface{ Aliases(ino uint64) []string }); ok {
			for _, alias := range aliaser.Aliases(ino) {
				paths[alias] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(paths))
	for p := range paths {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// inodeHandles lists every live handle of the inode: same-path fds beyond
// the newest one (liveSet keeps them all) plus every hardlink alias's live
// writer (v1 OpenHandleIndex.SnapshotInode + the alias path set). The
// named path's live handle leads so admission sweeps lock in a
// deterministic order.
func (w *Writeback) inodeHandles(ino uint64, path string) []*bufferingHandle {
	paths := map[string]struct{}{}
	for _, p := range w.inodeAliasPaths(ino, path) {
		paths[p] = struct{}{}
	}
	seen := make(map[*bufferingHandle]struct{})
	var out []*bufferingHandle
	// liveSet keys on the handle itself (the value is a set marker).
	w.liveSet.Range(func(key, _ any) bool {
		h, ok := key.(*bufferingHandle)
		if !ok {
			return true
		}
		h.mu.Lock()
		hpath, hino := h.path, h.ino
		h.mu.Unlock()
		if _, dup := seen[h]; dup {
			return true
		}
		if (ino != 0 && hino == ino) || hpath == path {
			seen[h] = struct{}{}
			out = append(out, h)
			return true
		}
		if _, aliased := paths[hpath]; aliased {
			seen[h] = struct{}{}
			out = append(out, h)
		}
		return true
	})
	if h := w.handleFor(path); h != nil {
		if _, dup := seen[h]; dup {
			for i, cur := range out {
				if cur == h {
					out = append(out[:i], out[i+1:]...)
					break
				}
			}
		}
		out = append([]*bufferingHandle{h}, out...)
	}
	return out
}

// visibleImageOwner picks the inode's newest uncommitted image holder: a
// writable, dirty, non-anonymous handle with the highest mutation epoch
// (the fd-truncate owner, or any sibling that wrote since — v1
// newest-marked-wins). busy=true means a writable holder is mid-mutation
// and the caller must retry within its budget rather than serve an older
// image. nil without busy means every handle is clean: the staged/remote
// tiers own the reads.
func (w *Writeback) visibleImageOwner(ino uint64, path string, skip *bufferingHandle) (best *bufferingHandle, bestSeq uint64, busy bool) {
	for _, h := range w.inodeHandles(ino, path) {
		if h == skip {
			continue
		}
		if !h.mu.TryLock() {
			if h.flags&uint32(syscall.O_ACCMODE) != uint32(syscall.O_RDONLY) {
				return nil, 0, true
			}
			continue
		}
		cand := h.dirty && h.buf != nil && !h.unlinked
		seq := h.mutSeq
		h.mu.Unlock()
		if cand && (best == nil || seq > bestSeq) {
			best, bestSeq = h, seq
		}
	}
	return best, bestSeq, false
}

// readVisibleSiblingImage serves a clean reader from the newest
// uncommitted image of its inode (#983's core contract): the fd-truncate
// owner's buffer — including for hardlink-alias readers whose own path
// holds no writer — with EOF clamped to the image. ok=false when no
// sibling owns an image (fall through to the staged/remote tiers); an
// EAGAIN return means the owner stayed busy past the bounded retry, never
// that an unrelated path is contended.
func (w *Writeback) readVisibleSiblingImage(ctx context.Context, reader *bufferingHandle, off int64, dest []byte) (int, bool, error) {
	ino, path := w.identityOf(reader)
	if ino == 0 {
		return 0, false, nil
	}
	deadline := time.Now().Add(samePathDirtyWaitTimeout)
	for {
		if err := ctx.Err(); err != nil {
			return 0, false, err
		}
		src, _, busy := w.visibleImageOwner(ino, path, reader)
		if busy {
			if time.Now().After(deadline) {
				return 0, false, syscall.EAGAIN
			}
			time.Sleep(samePathDirtyWaitInterval)
			continue
		}
		if src == nil {
			return 0, false, nil
		}
		if !src.mu.TryLock() {
			if time.Now().After(deadline) {
				return 0, false, syscall.EAGAIN
			}
			time.Sleep(samePathDirtyWaitInterval)
			continue
		}
		if !src.dirty || src.buf == nil || src.unlinked {
			src.mu.Unlock()
			return 0, false, nil
		}
		// Lazy base parts must load before the read: ReadAt zero-fills
		// unloaded parts, which is only correct beyond the retained remote
		// prefix (the truncate already clamped it — a regrown hole reads
		// zeros, never the truncated-away bytes).
		if err := src.buf.EnsurePartsForWrite(off, int64(len(dest))); err != nil {
			src.mu.Unlock()
			return 0, false, err
		}
		n := src.buf.ReadAt(off, dest)
		if end := src.buf.Size() - off; end < int64(len(dest)) {
			if end < 0 {
				end = 0
			}
			n = int(end)
		}
		src.mu.Unlock()
		return n, true, nil
	}
}

// readAliasStagedImage serves a reader whose own path has nothing staged
// from a hardlink alias's pending image (v1's staged alias scan): the
// truncating writer flushed on ITS pathname, and until that entry lands
// the alias's staged bytes are the inode's newest durable image.
func (w *Writeback) readAliasStagedImage(reader *bufferingHandle, off int64, dest []byte) (int, bool) {
	if w.opts.Staging == nil {
		return 0, false
	}
	ino, path := w.identityOf(reader)
	if ino == 0 {
		return 0, false
	}
	for _, alias := range w.inodeAliasPaths(ino, path) {
		if alias == path {
			continue
		}
		if !w.opts.Staging.Pending.HasPending(alias) {
			continue
		}
		meta, ok := w.pendingMeta(alias)
		if !ok || meta == nil {
			continue
		}
		if data, kok := readShadowGenerationFenced(w.opts.Staging.Shadow, alias, meta.Size); kok && data != nil {
			if off >= int64(len(data)) {
				return 0, true
			}
			return copy(dest, data[off:]), true
		}
	}
	return 0, false
}

// lockSiblingsOrdered TryLocks the sibling set in path order (v1
// tryLockFileHandlesInOrder): deterministic ordering keeps concurrent
// admissions from interleaving partial sets. Contention retries within
// the bounded window and fails closed with EAGAIN.
//
// The sort keys are snapshotted under each handle's own lock — sorting on
// a bare h.path read races a concurrent rename/retarget write and could
// order two admissions by inconsistent keys (review P1) — and every
// locked handle re-verifies its key before the set is accepted: a rename
// that moved a sibling mid-sweep restarts the sweep instead of letting an
// admission proceed over a stale ordering.
func (w *Writeback) lockSiblingsOrdered(siblings []*bufferingHandle, owner *bufferingHandle) ([]*bufferingHandle, error) {
	deadline := time.Now().Add(samePathDirtyWaitTimeout)
	locked := make([]*bufferingHandle, 0, len(siblings))
	for {
		keys := make([]siblingKey, 0, len(siblings))
		for _, sib := range siblings {
			if sib == owner {
				continue
			}
			sib.mu.Lock()
			keys = append(keys, siblingKey{h: sib, path: sib.path})
			sib.mu.Unlock()
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i].path < keys[j].path })

		locked = locked[:0]
		ok := true
		for _, k := range keys {
			if k.h.mu.TryLock() {
				locked = append(locked, k.h)
				continue
			}
			ok = false
			break
		}
		if ok && len(locked) == len(keys) {
			drifted := false
			for i, h := range locked {
				if h.path != keys[i].path {
					drifted = true
					break
				}
			}
			if !drifted {
				return locked, nil
			}
		}
		for _, l := range locked {
			l.mu.Unlock()
		}
		if time.Now().After(deadline) {
			return nil, errFtruncateBusy
		}
		time.Sleep(samePathDirtyWaitInterval)
	}
}

// siblingKey is a lock-held snapshot of one sibling's sort key.
type siblingKey struct {
	h    *bufferingHandle
	path string
}

// admitFtruncate is the fd-truncate admission path (#984 SetAttr's
// sibling contract): refuse while an independently dirty sibling exists
// (two uncommitted branches cannot be merged, and a refusal must leave
// every image byte-identical), then truncate the owner and rebase every
// clean sibling so the whole inode observes the new image at once.
func (w *Writeback) admitFtruncate(ctx context.Context, owner *bufferingHandle, ino uint64, path string, size int64) error {
	siblings := w.inodeHandles(ino, path)
	locked, err := w.lockSiblingsOrdered(siblings, owner)
	if err != nil {
		return err
	}
	defer func() {
		for i := len(locked) - 1; i >= 0; i-- {
			locked[i].mu.Unlock()
		}
	}()
	for _, sib := range locked {
		if sib.dirty && sib.buf != nil && !sib.unlinked {
			return syscall.EAGAIN
		}
	}
	if err := w.truncateOwnerUnderSiblings(ctx, owner, size); err != nil {
		return err
	}
	if ino != 0 {
		owner.mu.Lock()
		owner.ino = ino
		owner.mu.Unlock()
	}
	for _, sib := range locked {
		// The admission's path (not a bare owner.path read — the owner's
		// mutex is not held here, and rename writes the field under it).
		if sib.path == path {
			// Same-path siblings stay untouched (v1): their reads are
			// served by the visibility tiers, and a clean sibling's close
			// must publish nothing — the truncate belongs to the naming
			// descriptor that issued it.
			continue
		}
		switch {
		case sib.buf != nil && !sib.unlinked && sib.buf.Size() != size:
			// A hardlink alias's buffered handle rebases AND becomes a
			// publisher of the truncated image (v1
			// syncOpenHandlesAfterPathTruncate): its flush would otherwise
			// republish the stale pre-truncation tail at close. wgen
			// advances with the mutation — an upload that captured the
			// pre-rebase generation (a close-sync Flush or Drain in
			// flight) must not hit generation equality on completion and
			// retire this truncate's dirty state (review P1).
			if terr := sib.buf.Truncate(size); terr == nil {
				sib.buf.ResetSequentialState(size)
				sib.dirty = true
				sib.wgen++
				sib.mutSeq = w.nextMutSeq()
			}
		case sib.buf == nil && sib.pendingImage != nil:
			// The captured pending image predates the truncate; without a
			// buffer it cannot be rebased, so it must not keep serving
			// stale bytes — the visible/staged tiers own its reads now.
			sib.pendingImage = nil
		}
	}
	return nil
}

// truncateOwnerUnderSiblings runs the owner-side truncate under the
// admission's sibling locks (v1 truncateWritableHandleLocked + SetAttr's
// ordering): buffer, streaming state, dirty flag and mutation epoch move
// together, and the image is flagged as the inode's visible truncate.
func (w *Writeback) truncateOwnerUnderSiblings(ctx context.Context, owner *bufferingHandle, size int64) error {
	// Resolve identity before taking owner.mu — inoOf locks the handle.
	ino := w.inoOf(owner)
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.buf == nil {
		// The truncation base is the file's NEWEST image — a flushed
		// sibling's staged bytes included (v1: truncating a lazy handle
		// over the stale remote base would silently drop them at the
		// truncate's own commit boundary).
		if img := w.newestStagedInodeImage(ino, owner.path); len(img) > 0 {
			owner.buf = writebuffer.NewWriteBuffer(owner.path, 0, 0)
			if _, err := owner.buf.Write(0, img); err != nil {
				return err
			}
			owner.buf.ClearDirty()
			owner.baseLoaded = true
		}
	}
	if err := owner.ensureBufferLocked(ctx); err != nil {
		return err
	}
	owner.stream = nil
	owner.buf.ResetStreamingState()
	if err := owner.buf.Truncate(size); err != nil {
		return err
	}
	owner.wgen++
	owner.dirty = true
	owner.visTrunc = true
	owner.mutSeq = w.nextMutSeq()
	w.armBypass(owner.path, owner.baseRev, size, "setattr-size")
	return nil
}

// newestStagedInodeImage returns the inode's newest staged image — the
// named path's pending entry first, then any hardlink alias's (v1's staged
// alias scan). nil when nothing staged.
func (w *Writeback) newestStagedInodeImage(ino uint64, path string) []byte {
	if w.opts.Staging == nil {
		return nil
	}
	for _, p := range w.inodeAliasPaths(ino, path) {
		meta, ok := w.pendingMeta(p)
		if !ok || meta == nil || meta.Size <= 0 {
			continue
		}
		if data, kok := readShadowGenerationFenced(w.opts.Staging.Shadow, p, meta.Size); kok && data != nil {
			return data
		}
	}
	return nil
}

// liveDirtySizeForInode reports the size of the inode's newest
// uncommitted writer image (v1 dirtyHandleSize): the named path's live
// writer first, then any hardlink alias's. An unknown ino resolves through
// the facade's table (chain ops cannot be trusted with NodeId — Lookup and
// Create carry the PARENT's). ok=false when every handle is clean — the
// staged/remote tiers own the stat.
func (w *Writeback) liveDirtySizeForInode(ino uint64, path string) (int64, bool) {
	if ino == 0 && w.inodes != nil {
		if resolved, ok := w.inodes.Ino(path); ok {
			ino = resolved
		}
	}
	for _, p := range w.inodeAliasPaths(ino, path) {
		h := w.handleFor(p)
		if h == nil {
			continue
		}
		if !h.mu.TryLock() {
			// The writer is mid-mutation (its own Write holds the lock):
			// skip rather than block the stat — the next stat re-checks
			// (v1 readdir's busy row).
			continue
		}
		ok := h.dirty && h.buf != nil && !h.unlinked
		size := int64(0)
		if ok {
			size = h.buf.Size()
		}
		h.mu.Unlock()
		if ok {
			return size, true
		}
	}
	return 0, false
}
