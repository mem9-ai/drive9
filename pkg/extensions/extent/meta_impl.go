package extent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/mem9-ai/drive9/pkg/client"
)

// maxPlanBlocksPerChunk bounds continuation merging (a 64 MiB chunk of
// 4 KiB slices tops out at 16k blocks; the cap only guards a server that
// never advances next_offset). A var so tests can force the exhaustion
// path.
var maxPlanBlocksPerChunk = 1 << 15

// maxStagedPlanPages caps the pages walked by the echo-less content proof
// (one page per 64 MiB chunk). Exhausting it refuses the adoption: an image
// too large to be compared end-to-end cannot be attributed by content.
var maxStagedPlanPages = 1 << 12

const (
	fattrMode  = 1 << 0
	fattrUID   = 1 << 1
	fattrGID   = 1 << 2
	fattrSize  = 1 << 3
	fattrMtime = 1 << 5
)

// ---- lifecycle ----

func (b *Bridge) Name() string { return "drive9" }

func (b *Bridge) Init(format *meta.Format, force bool) error {
	if format != nil && format.BlockSize > 0 {
		b.format = *format
	}
	return nil
}

func (b *Bridge) Load(checkVersion bool) (*meta.Format, error) {
	f := b.format
	return &f, nil
}

func (b *Bridge) GetFormat() meta.Format {
	return b.format
}

func (b *Bridge) Shutdown() error { return nil }

func (b *Bridge) OnReload(cb func(*meta.Format)) {}

func (b *Bridge) NewSession(record bool) error {
	atomic.AddInt32(&b.sessionCount, 1)
	return nil
}

func (b *Bridge) CloseSession() error { return nil }
func (b *Bridge) FlushSession()       {}

func (b *Bridge) GetSession(sid uint64, detail bool) (*meta.Session, error) {
	return &meta.Session{}, nil
}

func (b *Bridge) ListSessions() ([]*meta.Session, error) { return nil, nil }

// ---- stat/attr helpers ----

func attrFromStat(ino meta.Ino, st *client.StatResult, mode uint32) *meta.Attr {
	a := &meta.Attr{Nlink: 1, Length: uint64(st.Size)}
	if st.IsDir {
		a.Typ = meta.TypeDirectory
		a.Nlink = 2
		if mode != 0 {
			a.Mode = uint16(mode & 0o7777)
		} else {
			a.Mode = 0o755
		}
	} else {
		a.Typ = meta.TypeFile
		if st.HasMode {
			a.Mode = uint16(st.Mode & 0o7777)
		} else if mode != 0 {
			a.Mode = uint16(mode & 0o7777)
		} else {
			a.Mode = 0o644
		}
	}
	if !st.Mtime.IsZero() {
		a.Mtime = st.Mtime.Unix()
		a.Mtimensec = uint32(st.Mtime.Nanosecond())
	}
	return a
}

func errnoOf(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	if client.IsNotFound(err) {
		return syscall.ENOENT
	}
	return syscall.EIO
}

// ---- namespace ----

func (b *Bridge) Lookup(ctx meta.Context, parent meta.Ino, name string, inode *meta.Ino, attr *meta.Attr, checkPerm bool) syscall.Errno {
	pp, ok := b.remotePath(parent)
	if !ok {
		return syscall.ESTALE
	}
	path := childPath(pp, name)
	// The live incarnation's key: a replacement's synthetic inode when one
	// owns this pathname, else the path hash.
	b.mu.Lock()
	ino := b.liveInoLocked(path)
	b.mu.Unlock()
	st, err := b.cfg.Client.StatCtx(ctxOf(ctx), path)
	if err != nil {
		// Fresh files exist only locally until their first commit.
		if client.IsNotFound(err) {
			b.mu.Lock()
			size, fresh := b.sizes[ino]
			b.mu.Unlock()
			if fresh {
				if inode != nil {
					*inode = ino
				}
				if attr != nil {
					*attr = meta.Attr{Typ: meta.TypeFile, Mode: 0o644, Nlink: 1, Length: uint64(max64(size, 0))}
				}
				return 0
			}
		}
		return errnoOf(err)
	}
	b.remember(ino, path)
	b.mu.Lock()
	b.revs[ino] = st.Revision
	if _, has := b.sizes[ino]; !has {
		b.sizes[ino] = st.Size
	}
	b.mu.Unlock()
	if inode != nil {
		*inode = ino
	}
	if attr != nil {
		*attr = *attrFromStat(ino, st, 0)
	}
	return 0
}

func (b *Bridge) GetAttr(ctx meta.Context, ino meta.Ino, attr *meta.Attr) syscall.Errno {
	path, ok := b.remotePath(ino)
	if !ok {
		return syscall.ESTALE
	}
	st, err := b.cfg.Client.StatCtx(ctxOf(ctx), path)
	if err != nil {
		if client.IsNotFound(err) {
			b.mu.Lock()
			size, fresh := b.sizes[ino]
			b.mu.Unlock()
			if fresh {
				if attr != nil {
					*attr = meta.Attr{Typ: meta.TypeFile, Mode: 0o644, Nlink: 1, Length: uint64(max64(size, 0))}
				}
				return 0
			}
		}
		return errnoOf(err)
	}
	b.mu.Lock()
	b.revs[ino] = st.Revision
	local, has := b.sizes[ino]
	b.mu.Unlock()
	if attr != nil {
		*attr = *attrFromStat(ino, st, 0)
		if has && local > st.Size {
			attr.Length = uint64(local)
		}
	}
	return 0
}

func (b *Bridge) Resolve(ctx meta.Context, parent meta.Ino, path string, inode *meta.Ino, attr *meta.Attr, force bool) syscall.Errno {
	pp, ok := b.remotePath(parent)
	if !ok {
		return syscall.ESTALE
	}
	full := childPath(pp, path)
	st, err := b.cfg.Client.StatCtx(ctxOf(ctx), full)
	if err != nil {
		return errnoOf(err)
	}
	ino := inoFor(full)
	b.remember(ino, full)
	if inode != nil {
		*inode = ino
	}
	if attr != nil {
		*attr = *attrFromStat(ino, st, 0)
	}
	return 0
}

func (b *Bridge) SetAttr(ctx meta.Context, ino meta.Ino, set uint16, sggidclearmode uint8, attr *meta.Attr) syscall.Errno {
	path, ok := b.remotePath(ino)
	if !ok {
		return syscall.ESTALE
	}
	if set&fattrSize == 0 {
		// Mode/time changes ride the classic plane; report current attrs.
		return b.GetAttr(ctx, ino, attr)
	}
	b.mu.Lock()
	old, has := b.sizes[ino]
	if !has {
		old = -1
	}
	b.mu.Unlock()
	var newSize int64 = -1
	if attr != nil && attr.Length != 0 {
		newSize = int64(attr.Length)
	} else if attr != nil {
		newSize = 0
	}
	_ = old
	b.mu.Lock()
	b.sizes[ino] = newSize
	if _, has := b.truncs[ino]; !has && newSize >= 0 {
		b.truncs[ino] = newSize
	}
	b.mu.Unlock()
	_ = path
	return b.GetAttr(ctx, ino, attr)
}

func (b *Bridge) CheckSetAttr(ctx meta.Context, ino meta.Ino, set uint16, attr meta.Attr) syscall.Errno {
	return 0
}

func (b *Bridge) Truncate(ctx meta.Context, ino meta.Ino, flags uint8, attrlength uint64, attr *meta.Attr, skipPermCheck bool) syscall.Errno {
	a := meta.Attr{Length: attrlength}
	return b.SetAttr(ctx, ino, fattrSize, 0, &a)
}

func (b *Bridge) Mkdir(ctx meta.Context, parent meta.Ino, name string, mode uint16, cumask uint16, copysgid uint8, inode *meta.Ino, attr *meta.Attr) syscall.Errno {
	pp, ok := b.remotePath(parent)
	if !ok {
		return syscall.ESTALE
	}
	path := childPath(pp, name)
	if err := b.cfg.Client.MkdirCtx(ctxOf(ctx), path, uint32(mode & ^cumask)); err != nil {
		return errnoOf(err)
	}
	ino := inoFor(path)
	b.remember(ino, path)
	if inode != nil {
		*inode = ino
	}
	if attr != nil {
		*attr = meta.Attr{Typ: meta.TypeDirectory, Mode: mode & ^cumask, Nlink: 2}
	}
	return 0
}

func (b *Bridge) Mknod(ctx meta.Context, parent meta.Ino, name string, _type uint8, mode uint16, cumask uint16, rdev uint32, path string, inode *meta.Ino, attr *meta.Attr) syscall.Errno {
	if _type != meta.TypeFile {
		return syscall.ENOTSUP
	}
	// Files materialize on the server at their first extent commit; only
	// the local mapping is created here.
	pp, ok := b.remotePath(parent)
	if !ok {
		return syscall.ESTALE
	}
	full := childPath(pp, name)
	ino := inoFor(full)
	// A held path hash (live deleted-open or renamed-away descriptor) means
	// this create is a NEW incarnation: allocate its synthetic key and
	// publish it in the same critical section, so the same incarnation's
	// later resolutions (Create's mandatory ResolvePath, repeated opens)
	// land on THIS inode and no concurrent resolution can publish another.
	b.mu.Lock()
	if held, isHeld := b.heldInoForLocked(full); isHeld && held == ino {
		ino = b.allocateHeldReplacementInoLocked()
		b.replacements[full] = ino
	}
	b.mu.Unlock()
	b.remember(ino, full)
	b.mu.Lock()
	if _, has := b.sizes[ino]; !has {
		b.sizes[ino] = 0
	}
	b.modes[ino] = uint32(mode & ^cumask)
	b.mu.Unlock()
	if inode != nil {
		*inode = ino
	}
	if attr != nil {
		*attr = meta.Attr{Typ: meta.TypeFile, Mode: mode & ^cumask, Nlink: 1}
	}
	return 0
}

func (b *Bridge) Create(ctx meta.Context, parent meta.Ino, name string, mode uint16, cumask uint16, flags uint32, inode *meta.Ino, attr *meta.Attr) syscall.Errno {
	st := b.Mknod(ctx, parent, name, meta.TypeFile, mode, cumask, 0, "", inode, attr)
	return st
}

func (b *Bridge) Unlink(ctx meta.Context, parent meta.Ino, name string, skipCheckTrash ...bool) syscall.Errno {
	return b.removeEntry(ctx, parent, name)
}

func (b *Bridge) Rmdir(ctx meta.Context, parent meta.Ino, name string, skipCheckTrash ...bool) syscall.Errno {
	return b.removeEntry(ctx, parent, name)
}

func (b *Bridge) removeEntry(ctx meta.Context, parent meta.Ino, name string) syscall.Errno {
	pp, ok := b.remotePath(parent)
	if !ok {
		return syscall.ESTALE
	}
	path := childPath(pp, name)
	// Fresh, never-committed files have no server-side name yet.
	st, err := b.cfg.Client.StatCtx(ctxOf(ctx), path)
	if err == nil || !client.IsNotFound(err) {
		if err := b.cfg.Client.RemoveAllCtx(ctxOf(ctx), path); err != nil {
			return errnoOf(err)
		}
	}
	ino := inoFor(path)
	b.mu.Lock()
	delete(b.paths, ino)
	delete(b.sizes, ino)
	delete(b.revs, ino)
	delete(b.pending, ino)
	delete(b.truncs, ino)
	delete(b.modes, ino)
	for i, e := range b.paths {
		if strings.HasPrefix(e, path+"/") {
			delete(b.paths, i)
		}
	}
	b.mu.Unlock()
	_ = st
	return 0
}

func (b *Bridge) Rename(ctx meta.Context, parentSrc meta.Ino, nameSrc string, parentDst meta.Ino, nameDst string, flags uint32, inode *meta.Ino, attr *meta.Attr) syscall.Errno {
	sp, ok := b.remotePath(parentSrc)
	if !ok {
		return syscall.ESTALE
	}
	dp, ok := b.remotePath(parentDst)
	if !ok {
		return syscall.ESTALE
	}
	src := childPath(sp, nameSrc)
	dst := childPath(dp, nameDst)
	if _, err := b.cfg.Client.StatCtx(ctxOf(ctx), src); err == nil {
		if err := b.cfg.Client.RenameCtx(ctxOf(ctx), src, dst); err != nil {
			return errnoOf(err)
		}
	}
	b.remapPath(src, dst)
	if inode != nil {
		*inode = inoFor(dst)
	}
	if attr != nil {
		_ = b.GetAttr(ctx, inoFor(dst), attr)
	}
	return 0
}

func (b *Bridge) remapPath(src, dst string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ino, p := range b.paths {
		switch {
		case p == src:
			b.paths[ino] = dst
		case strings.HasPrefix(p, src+"/"):
			b.paths[ino] = dst + p[len(src):]
		}
	}
	if s, ok := b.sizes[inoFor(src)]; ok {
		b.sizes[inoFor(dst)] = s
		delete(b.sizes, inoFor(src))
	}
}

func (b *Bridge) Readdir(ctx meta.Context, ino meta.Ino, wantattr uint8, entries *[]*meta.Entry) syscall.Errno {
	path, ok := b.remotePath(ino)
	if !ok {
		return syscall.ESTALE
	}
	infos, err := b.cfg.Client.ListCtx(ctxOf(ctx), path)
	if err != nil {
		return errnoOf(err)
	}
	b.mu.Lock()
	var local []string
	for _, p := range b.paths {
		if strings.HasPrefix(p, childPath(path, "")+"/") {
			local = append(local, p)
		}
	}
	b.mu.Unlock()
	seen := make(map[string]bool, len(infos)+len(local))
	out := make([]*meta.Entry, 0, len(infos)+len(local))
	for _, fi := range infos {
		seen[fi.Name] = true
		child := childPath(path, fi.Name)
		ci := inoFor(child)
		b.remember(ci, child)
		e := &meta.Entry{Inode: ci, Name: []byte(fi.Name)}
		if wantattr > 0 {
			st := &client.StatResult{Size: fi.Size, IsDir: fi.IsDir, Mtime: time.Unix(fi.Mtime, 0), Mode: fi.Mode, HasMode: fi.HasMode, Revision: fi.Revision}
			e.Attr = attrFromStat(ci, st, 0)
		}
		out = append(out, e)
	}
	for _, p := range local {
		name := strings.TrimPrefix(p, childPath(path, "")+"/")
		if name == "" || strings.Contains(name, "/") || seen[name] {
			continue
		}
		out = append(out, &meta.Entry{Inode: inoFor(p), Name: []byte(name), Attr: &meta.Attr{Typ: meta.TypeFile, Mode: 0o644, Nlink: 1}})
	}
	*entries = out
	return 0
}

// ---- data plane ----

func (b *Bridge) Open(ctx meta.Context, ino meta.Ino, flags uint32, attr *meta.Attr) syscall.Errno {
	return b.GetAttr(ctx, ino, attr)
}

func (b *Bridge) Close(ctx meta.Context, ino meta.Ino) syscall.Errno {
	if err := b.flushPending(ctxOf(ctx), ino); err != nil {
		return syscall.EIO
	}
	return 0
}

// Read translates one chunk of the file's block read plan into JuiceFS
// slices. The engine requires slices to tile the chunk contiguously from
// offset zero; plan blocks do exactly that. Trailing zero blocks are
// dropped (the reader zero-fills past coverage); interior holes become
// synthesized zero slices served from memory by the storage adapter.
func (b *Bridge) Read(ctx meta.Context, ino meta.Ino, indx uint32, slices *[]meta.Slice) syscall.Errno {
	path, ok := b.remotePath(ino)
	if !ok {
		return syscall.ESTALE
	}
	start := int64(indx) * meta.ChunkSize
	chunkEnd := start + meta.ChunkSize - 1
	if b.isClassicOwned(ino) {
		return b.readClassic(ctx, ino, path, start, chunkEnd, slices)
	}
	plan, err := b.cfg.Client.GetExtentReadPlan(ctxOf(ctx), path, start, chunkEnd)
	if err != nil {
		if !client.IsNotFound(err) {
			return errnoOf(err)
		}
		// No remote representation (a fresh interactive file before its
		// first commit): an empty plan — the pending overlay below still
		// serves the locally staged bytes, which must not read as zeros.
		plan = &client.ExtentReadPlan{}
	}
	// A single plan page need not cover the chunk: the server caps a plan
	// at 512 distinct block URLs and stops early on a presign failure,
	// returning next_offset for the remainder. Publishing only the first
	// page would make the engine zero-fill real data — follow every
	// continuation within this chunk's window before translating.
	blocks := plan.Blocks
	prevNext := start // the cursor position this page was requested at
	for plan.NextOffset > 0 && plan.NextOffset-1 < chunkEnd && len(blocks) < maxPlanBlocksPerChunk {
		if plan.NextOffset == prevNext {
			// The server returned a cursor that does not advance (e.g. a
			// presign failure page with empty blocks): retrying the same
			// range would spin forever.
			return syscall.EIO
		}
		prevNext = plan.NextOffset
		plan, err = b.cfg.Client.GetExtentReadPlan(ctxOf(ctx), path, plan.NextOffset, chunkEnd)
		if err != nil {
			return errnoOf(err)
		}
		blocks = append(blocks, plan.Blocks...)
	}
	if plan.NextOffset > 0 && plan.NextOffset-1 < chunkEnd {
		// The safety budget ran out with the chunk still uncovered:
		// publishing the prefix would make the engine zero-fill real data.
		return syscall.EIO
	}
	b.mu.Lock()
	b.revs[ino] = plan.Revision
	// A pending truncation is authoritative: the server's (older, larger)
	// plan size must not resurrect a shrunk tail — the truncate-only
	// flush would then see its own size back and silently drop the
	// shrink.
	if _, truncating := b.truncs[ino]; !truncating && plan.Size > b.sizes[ino] {
		b.sizes[ino] = plan.Size
	}
	b.mu.Unlock()
	// Trim trailing zero blocks.
	n := len(blocks)
	for n > 0 && blocks[n-1].Zero {
		n--
	}
	blocks = blocks[:n]

	// Deferred (interactive) writes live only in the pending list: overlay
	// them so reads serve the newest image without a remote round trip.
	// Without pending state the plan is emitted exactly as before (plan
	// gaps keep their server-authoritative meaning); with pending state a
	// fragment SHADOWS the overlapping range of any plan block (JuiceFS
	// overwrite semantics) and the merged run is re-built with explicit
	// holes.
	b.mu.Lock()
	pending := append([]pendingWrite(nil), b.pending[ino]...)
	b.mu.Unlock()
	if len(pending) == 0 {
		out := make([]meta.Slice, 0, len(blocks))
		for _, blk := range blocks {
			if blk.Zero {
				// The engine's native hole slice: readSlice zero-fills Id 0.
				out = append(out, meta.Slice{Id: 0, Len: uint32(blk.Length)})
				continue
			}
			id := b.blocks.register(blk.BlockKey, blk.URL, blk.BlockOffset+blk.Length, path, blk.FileOffset, ino)
			// JuiceFS slice semantics: Size is the backing object's size,
			// Off the start within it. Shadow-merged plans reference one
			// object with block_offset > 0, so Size must cover Off+Len for
			// the store's key derivation and reads to line up.
			out = append(out, meta.Slice{Id: id, Size: uint32(blk.BlockOffset + blk.Length), Off: uint32(blk.BlockOffset), Len: uint32(blk.Length)})
		}
		*slices = out
		return 0
	}

	type span struct {
		off, length int64
		id          uint64
		size        uint32
		inner       uint32
		seq         int
	}
	var planSpans, pend []span
	for _, blk := range blocks {
		if blk.Zero {
			planSpans = append(planSpans, span{off: blk.FileOffset, length: blk.Length})
			continue
		}
		id := b.blocks.register(blk.BlockKey, blk.URL, blk.BlockOffset+blk.Length, path, blk.FileOffset, ino)
		planSpans = append(planSpans, span{off: blk.FileOffset, length: blk.Length, id: id, size: uint32(blk.BlockOffset + blk.Length), inner: uint32(blk.BlockOffset)})
	}
	for i, p := range pending {
		off := int64(p.chunk)*meta.ChunkSize + int64(p.off)
		if off+p.size <= start || off > chunkEnd {
			continue
		}
		id := b.blocks.registerLocal(p.checksum, p.size)
		pend = append(pend, span{off: off, length: p.size, id: id, size: uint32(p.size), seq: i})
	}
	// Compact the pending fragments first: a later write shadows an
	// earlier overlapping one.
	// NOTE: no offset sort here — the compaction walks WRITE order
	// (construction order); sorting first would let an older high-offset
	// fragment shadow a newer low-offset write. The compacted result is
	// sorted by offset below.
	// Compact the pending fragments: each write's VISIBLE pieces are the
	// write minus the UNION of every later write (JuiceFS overwrite
	// semantics). Computed as pure interval subtraction against the merged
	// later-union — every emitted fragment carries its own inner offset
	// into the staged block (cur.inner is derived from cur.off, never
	// accumulated), and a write fully covered from the front contributes
	// nothing.
	var pendCompact []span
	for i := range pend {
		w := pend[i]
		union := make([]span, 0, len(pend)-i-1)
		for j := i + 1; j < len(pend); j++ {
			if pend[j].length > 0 {
				union = append(union, pend[j])
			}
		}
		sort.Slice(union, func(x, y int) bool { return union[x].off < union[y].off })
		// Merge the union into disjoint intervals.
		var merged []span
		for _, sh := range union {
			if len(merged) > 0 && sh.off <= merged[len(merged)-1].off+merged[len(merged)-1].length {
				last := &merged[len(merged)-1]
				if end := sh.off + sh.length; end > last.off+last.length {
					last.length = end - last.off
				}
				continue
			}
			merged = append(merged, sh)
		}
		// Subtract: walk [w.off, w.off+w.length) against the merged union.
		start, end := w.off, w.off+w.length
		for _, sh := range merged {
			sEnd := sh.off + sh.length
			if sEnd <= start {
				continue // shadow entirely before the remaining window
			}
			if sh.off >= end {
				break // shadows are sorted: the rest is past the window
			}
			if sh.off > start {
				frag := span{
					off:    start,
					length: sh.off - start,
					id:     w.id,
					size:   w.size,
					seq:    w.seq,
				}
				frag.inner = uint32(start - w.off) // the fragment's offset inside the staged block
				pendCompact = append(pendCompact, frag)
			}
			if sEnd >= end {
				start = end
				break
			}
			start = sEnd
		}
		if start < end {
			frag := span{off: start, length: end - start, id: w.id, size: w.size, seq: w.seq}
			frag.inner = uint32(start - w.off)
			pendCompact = append(pendCompact, frag)
		}
	}
	// Drop empties and sort the survivors by offset for the merge.
	keep := 0
	for _, sp := range pendCompact {
		if sp.length > 0 {
			pendCompact[keep] = sp
			keep++
		}
	}
	pendCompact = pendCompact[:keep]
	sort.Slice(pendCompact, func(i, j int) bool { return pendCompact[i].off < pendCompact[j].off })
	// Split the plan spans around the compacted pending fragments, then
	// merge by offset with explicit holes so the run stays consecutive.
	var merged []span
	for _, ps := range planSpans {
		cur := ps
		for _, pe := range pendCompact {
			if pe.off+pe.length <= cur.off || pe.off >= cur.off+cur.length {
				continue
			}
			if pe.off > cur.off {
				head := cur
				head.length = pe.off - cur.off
				merged = append(merged, head)
			}
			tailStart := pe.off + pe.length
			if tailStart >= cur.off+cur.length {
				cur.length = 0
				break
			}
			cut := tailStart - cur.off
			cur.inner += uint32(cut)
			cur.off = tailStart
			cur.length -= cut
		}
		if cur.length > 0 {
			merged = append(merged, cur)
		}
	}
	merged = append(merged, pendCompact...)
	sort.Slice(merged, func(i, j int) bool { return merged[i].off < merged[j].off })

	var outSlices []meta.Slice
	// The run is consecutive from the CHUNK start: a gap before the first
	// span is a leading hole, never a shift of the pending bytes.
	cursor := start
	for _, sp := range merged {
		if sp.off > cursor {
			outSlices = append(outSlices, meta.Slice{Id: 0, Len: uint32(sp.off - cursor)}) // hole
		}
		if sp.id == 0 {
			outSlices = append(outSlices, meta.Slice{Id: 0, Len: uint32(sp.length)})
		} else {
			outSlices = append(outSlices, meta.Slice{Id: sp.id, Size: sp.size, Off: sp.inner, Len: uint32(sp.length)})
		}
		cursor = sp.off + sp.length
	}
	*slices = outSlices
	return 0
}

func (b *Bridge) NewSlice(ctx meta.Context, id *uint64) syscall.Errno {
	b.mu.Lock()
	defer b.mu.Unlock()
	*id = b.nextID
	b.nextID++
	return 0
}

// Write records one uploaded slice and commits every pending slice of the
// file. The engine uploads whole-block slices (RMW merges full blocks), so
// Off is always zero; the staged object bytes were captured by the storage
// adapter's Put.
func (b *Bridge) Write(ctx meta.Context, ino meta.Ino, indx uint32, off uint32, slice meta.Slice, mtime time.Time) syscall.Errno {
	if slice.Off != 0 {
		return syscall.EIO // see package doc: engine merges RMW into whole blocks
	}
	if b.cfg.NativeWriteback {
		// The chunk store owns the block upload (JuiceFS writeback): its
		// staging is asynchronous, so the slice's bytes may not be in the
		// registry yet when the writer's Finish returns. Wait for them
		// before committing — the server verifies each referenced block,
		// so publishing early would fail the PATCH (or worse, race it).
		if !b.waitStagedSlice(ctxOf(ctx), slice.Id, int64(slice.Size)) {
			return syscall.EIO
		}
	}
	b.mu.Lock()
	sl := b.stagedSlices[slice.Id]
	if sl == nil || sl.total != int64(slice.Size) {
		b.mu.Unlock()
		return syscall.EIO
	}
	data := sl.join()
	sum := sha256Hex(data)
	delete(b.stagedSlices, slice.Id)
	// Make the bytes findable by checksum for the commit path
	// (assembleImage / blockBytes refetch), pinned against the classic
	// read LRU: a pending commit still needs them.
	b.blocks.stagePinned(sum, data, true)
	b.pending[ino] = append(b.pending[ino], pendingWrite{chunk: indx, off: off, checksum: sum, size: int64(slice.Len)})
	end := int64(indx)*meta.ChunkSize + int64(off) + int64(slice.Len)
	if end > b.sizes[ino] {
		b.sizes[ino] = end
	}
	if b.deferred.Load() {
		b.mu.Unlock()
		// Interactive durability: the bytes are staged locally and reads
		// overlay them; the remote commit lands at the flush/close
		// boundary (StagePendingLocal makes this fsync-durable).
		return 0
	}
	b.mu.Unlock()

	// The commit lifecycle (snapshot above included) is serialized per
	// file: the snapshot is re-taken under the lock so a concurrent
	// commit's result is observed, never an older size.
	unlock := b.lockCommit(ino)
	defer unlock()
	b.mu.Lock()
	pending := append([]pendingWrite(nil), b.pending[ino]...)
	size := b.sizes[ino]
	b.mu.Unlock()
	if err := b.commit(ctxOf(ctx), ino, pending, size); err != nil {
		// Put the slices back so a later flush retries them. The error is
		// NOT returned to the engine: the community writer records it
		// forever (v1.4.1 fileWriter.err is sticky), which stranded the
		// handle even after the bridge recovered — the flush/close paths
		// surface the same failure through FlushFile instead, and the
		// writer itself stays clean.
		b.mu.Lock()
		if b.commitErr[ino] == nil {
			b.commitErr[ino] = err
		}
		b.mu.Unlock()
		return 0
	}
	b.mu.Lock()
	delete(b.commitErr, ino)
	b.mu.Unlock()
	return 0
}

func (b *Bridge) InvalidateChunkCache(ctx meta.Context, ino meta.Ino, indx uint32) syscall.Errno {
	return 0
}

// commit lands the pending slices server-side: PATCH for existing extent
// files, a sequential extent session for fresh ones.
func (b *Bridge) commit(ctx context.Context, ino meta.Ino, pending []pendingWrite, size int64) error {
	if len(pending) == 0 {
		return b.flushTruncateOnly(ctx, ino, size)
	}
	path, ok := b.remotePath(ino)
	if !ok {
		return fmt.Errorf("no path for inode %d", ino)
	}
	st, err := b.cfg.Client.StatCtx(ctx, path)
	fresh := err != nil && client.IsNotFound(err)
	if err != nil && !fresh {
		return err
	}
	// An identity-verified recovery (durable replay) constrains EVERY
	// publication of this data: the occupant must still be the staged
	// object. The constraint SURVIVES failures and retries (it is only
	// consumed by a completed publication) — and when the occupant is a
	// replacement, the staged data is abandoned (mappings dropped) so no
	// later unconstrained flush can publish into the replacement.
	if v, bounded := b.peekVerifyBase(ino); bounded {
		if fresh || st.ResourceID != v.resID {
			b.abandonIno(ino)
			return fmt.Errorf("recovery target %s no longer holds the staged object", path)
		}
		st.Revision = v.rev
	}
	// A captured (echoed) landed identity constrains EVERY retry of a
	// live handle whose classification never completed — regardless of the
	// occupant's CURRENT layout: an extent replacement at the path must be
	// rejected exactly like a classic one, not published into via the
	// plain PATCH path.
	if !fresh && b.isLayoutPending(ino) && !b.isClassicOwned(ino) {
		if !b.layoutPendingMatches(ino, st.ResourceID) {
			return fmt.Errorf("target %s no longer holds our upload", path)
		}
	}
	if !fresh && b.needsFreshIdentity(ino) {
		// A locally-created descriptor whose landing never established an
		// attributable identity (no completion echo, no bound
		// classification) must not write into whoever currently holds the
		// name. An empty classic occupant is NOT an exception: publishing
		// into it is an unconditional conditional-write against a foreign
		// object (publicationIdentity is empty, so the request carries no
		// expected resource and simply succeeds), and deleting it would be
		// equally unauthorized. Refuse — the descriptor may not mutate an
		// object it was never bound to.
		return fmt.Errorf("fresh create of %s has no attributable identity for the current occupant", path)
	}
	if !fresh && st.ContentLayout != client.ContentLayoutExtent {
		if b.isClassicOwned(ino) || b.isLayoutPending(ino) {
			// A fresh session of ours degraded to a classic upload on a
			// legacy server (or its post-upload classification never
			// completed): keep the open handle usable — commit through
			// the classic surface instead of rejecting a target we own.
			// "Ours" is IDENTITY-BOUND: when the classification ever saw
			// our landed upload, the occupant must still be THAT object —
			// a foreign replacement is rejected exactly like the plain
			// non-extent case.
			if b.isLayoutPending(ino) && !b.isClassicOwned(ino) {
				if !b.layoutPendingMatches(ino, st.ResourceID) {
					return fmt.Errorf("target %s is not extent layout", path)
				}
				// The pending classification may have recovered by now.
				for _, delay := range []time.Duration{0, 200 * time.Millisecond} {
					time.Sleep(delay)
					nst, serr := b.cfg.Client.StatCtx(ctx, path)
					if serr == nil && nst.ContentLayout == client.ContentLayoutExtent && nst.ResourceID == st.ResourceID {
						b.clearLayoutPending(ino)
						return b.commitExtentExisting(ctx, ino, path, pending, size, nst.Revision)
					}
				}
				b.mu.Lock()
				b.layoutPending[ino] = false
				b.classicOwned[ino] = true // ours, and not extent: classic
				b.mu.Unlock()
			}
			return b.commitClassic(ctx, ino, path, pending, size, st.Revision)
		}
		// No ownership of this classic occupant. It must not be deleted
		// (that would destroy another client's file) and must not be
		// written through the classic surface either (that would mutate
		// it). Refuse both, whatever its size: an empty occupant is no
		// more attributable than a non-empty one, and routing it to
		// commitClassic would also materialize the whole image for a
		// sparse write.
		return fmt.Errorf("target %s is not extent layout", path)
	}

	if fresh {
		// Sparse: holes are synthesized on demand — a small write at a
		// huge offset must not materialize the whole logical image. The
		// completed upload's echoed resource identity is captured even if
		// every post-upload Stat fails: it is the ownership proof the
		// retry matches against (a replacement then rejects instead of
		// receiving the staged bytes).
		landed := ""
		upctx := client.WithCompletedResourceSink(ctx, &landed)
		ra := &sparseImageAt{bridge: b, ctx: ctx, path: path, size: size, writes: pending}
		if err := b.cfg.Client.WriteExtentStreamConditional(upctx, path, ra, size, nil, -1); err != nil {
			return err
		}
		if landed != "" {
			b.mu.Lock()
			b.layoutPendingID[ino] = landed
			b.mu.Unlock()
		}
	} else {
		blocks := make([]client.ExtentBlockWrite, 0, len(pending))
		for _, p := range pending {
			data, err := b.blockBytes(ctx, p, path)
			if err != nil {
				return err
			}
			blocks = append(blocks, client.ExtentBlockWrite{Chunk: p.chunk, Off: p.off, Data: data})
		}
		if err := b.commitWithRetry(ctx, ino, path, blocks, size, st.Revision); err != nil {
			return err
		}
	}

	if fresh {
		// Classify the landed layout BEFORE any cleanup: a classification
		// failure must leave the pending list AND the staged bytes intact
		// (retiring them first made the retry unable to re-upload). The
		// stat is retried; a failure marks the layout PENDING — proof of
		// OUR upload, distinct from a foreign replacement — so the next
		// attempt classifies instead of rejecting.
		classic := false
		classified := false
		// The echoed identity (complete response) is the fallback proof
		// when no Stat succeeds.
		b.mu.Lock()
		landedID := b.layoutPendingID[ino]
		b.mu.Unlock()
		for _, delay := range []time.Duration{0, 200 * time.Millisecond, 500 * time.Millisecond} {
			time.Sleep(delay)
			nst, serr := b.cfg.Client.StatCtx(ctx, path)
			if serr != nil {
				continue // transient; keep retrying
			}
			b.mu.Lock()
			b.revs[ino] = nst.Revision
			savedID := b.layoutPendingID[ino]
			b.mu.Unlock()
			// The classification Stat must describe the object OUR
			// completion echoed: a replacement landing between the
			// completion and this Stat presents a different identity
			// (and possibly a classic layout), and marking the handle
			// classic-owned off it would let this descriptor write into
			// a foreign file.
			if savedID != "" && nst.ResourceID != savedID {
				// The echo names OUR object and this Stat describes
				// another: a replacement, whatever its layout.
				b.mu.Lock()
				b.layoutPending[ino] = true
				b.mu.Unlock()
				return fmt.Errorf("verify landed layout of %s: occupant replaced our upload", path)
			}
			// An echo-less completion has no server-supplied identity, so
			// the occupant must be proven by CONTENT: the whole staged
			// image, not one block. A single shared checksum proves
			// nothing (a replacement whose first block happens to match
			// would be adopted and then PATCHed with our size and bytes),
			// so imageMatchesPending requires EVERY staged block at its
			// exact offset, the exact size, and no non-zero block outside
			// the staged set. Only then does the observed identity BIND.
			//
			// This is still content, not object identity: an occupant
			// carrying a byte-identical image is indistinguishable, and
			// adopting it is observationally the same object (every later
			// PATCH writes exactly the bytes this descriptor would have
			// written, onto a path CAS'd at the occupant's own revision).
			// The definitive proof is the server echoing the landed
			// resource on completion; until it does, this is the strongest
			// evidence a client can obtain — and an occupant with no
			// stat-visible identity could never be re-verified on ANY
			// later publication, so it is refused outright.
			if savedID == "" {
				if nst.ResourceID == "" || !b.imageMatchesPending(ctx, path, size, pending) {
					b.mu.Lock()
					b.layoutPending[ino] = true
					b.mu.Unlock()
					return fmt.Errorf("verify landed layout of %s: occupant not attributable to our upload", path)
				}
				b.mu.Lock()
				b.layoutPendingID[ino] = nst.ResourceID
				b.mu.Unlock()
			}
			landedID = nst.ResourceID
			classic = nst.ContentLayout != client.ContentLayoutExtent
			classified = true
			break
		}
		if !classified {
			b.mu.Lock()
			b.layoutPending[ino] = true
			// The completion path's ECHOED identity is the ownership proof
			// even when no Stat ever succeeded; never clobber a captured
			// identity with the empty stat-derived one.
			if landedID != "" {
				b.layoutPendingID[ino] = landedID
			}
			b.mu.Unlock()
			// The pending list is INTACT (cleanup runs only after
			// classification) — nothing to restore, and re-appending
			// would duplicate the snapshot.
			return fmt.Errorf("verify landed layout of %s: stat unavailable", path)
		}
		if classic {
			b.mu.Lock()
			b.classicOwned[ino] = true
			b.mu.Unlock()
		}
	} else if nst, err := b.cfg.Client.StatCtx(ctx, path); err == nil {
		b.mu.Lock()
		b.revs[ino] = nst.Revision
		b.mu.Unlock()
	}

	b.mu.Lock()
	mode := b.modes[ino]
	delete(b.commitErr, ino) // a successful landing resolves the memory
	delete(b.verifyBase, ino)
	delete(b.layoutPending, ino)
	delete(b.freshCreate, ino)
	// layoutPendingID SURVIVES the landing: it is the identity binding
	// for every later publication of this descriptor (the PATCH's
	// expected-resource), not merely the pending-classification proof.
	// It is dropped with the inode mapping itself (abandon/forget).
	b.mu.Unlock()
	b.surgicalDeletePending(ino, pending)
	b.releaseCommitted(ino, pending)
	if fresh && mode != 0 && mode != 0o644 {
		// The sequential extent create carries no mode; apply the one the
		// creator asked for.
		_ = b.cfg.Client.ChmodCtx(ctx, path, uint32(mode))
	}
	return nil
}

// commitExtentExisting lands pending blocks as an incremental PATCH
// against a known revision (the identity-verified recovery path).
func (b *Bridge) commitExtentExisting(ctx context.Context, ino meta.Ino, path string, pending []pendingWrite, size, rev int64) error {
	blocks := make([]client.ExtentBlockWrite, 0, len(pending))
	for _, p := range pending {
		data, err := b.blockBytes(ctx, p, path)
		if err != nil {
			return err
		}
		blocks = append(blocks, client.ExtentBlockWrite{Chunk: p.chunk, Off: p.off, Data: data})
	}
	if err := b.commitWithRetry(ctx, ino, path, blocks, size, rev); err != nil {
		return err
	}
	b.surgicalDeletePending(ino, pending)
	b.releaseCommitted(ino, pending)
	return nil
}

// isClassicOwned reports whether this bridge owns a file whose extent
// session degraded to a classic upload (legacy server).
func (b *Bridge) isClassicOwned(ino meta.Ino) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.classicOwned[ino]
}

// commitClassic continues a degraded fresh create: the composed image
// (current remote content with the pending fragments shadowed, truncated
// or grown to the local size) re-uploads through the classic conditional
// write. A compatibility path for legacy servers; it materializes the
// image — the engine plane is not the promotion path there.
func (b *Bridge) commitClassic(ctx context.Context, ino meta.Ino, path string, pending []pendingWrite, size, rev int64) error {
	var image []byte
	if rc, err := b.cfg.Client.ReadStream(ctx, path); err == nil {
		var rerr error
		// The body must be fully consumed before the image is trusted: a
		// truncated download with a discarded error would zero-pad the
		// missing tail and upload a damaged image under a valid revision.
		image, rerr = io.ReadAll(rc)
		_ = rc.Close()
		if rerr != nil {
			if os.Getenv("EXTENT_DBG") != "" {
				fmt.Println("DBG commitClassic base read fail:", rerr)
			}
			return fmt.Errorf("read classic base %s: %w", path, rerr)
		}
	} else if !client.IsNotFound(err) {
		return err
	}
	for _, p := range pending {
		off := int64(p.chunk)*meta.ChunkSize + int64(p.off)
		end := off + p.size
		if int64(len(image)) < end {
			image = append(image, make([]byte, end-int64(len(image)))...)
		}
		data, err := b.blockBytes(ctx, p, path)
		if err != nil {
			return err
		}
		copy(image[off:end], data)
	}
	if int64(len(image)) > size {
		image = image[:size]
	}
	for int64(len(image)) < size {
		image = append(image, 0)
	}
	// The classic publication is bound to the verified object's identity
	// when a recovery constraint is active: the request carries the
	// expected resource (header), so a replacement whose revision merely
	// equals the saved one cannot receive the staged bytes.
	// The completion echo carries the revision the server attributed to THIS
	// publication; capture it so the constraint below advances to it rather
	// than reusing the pre-write base.
	var landedRev int64
	wctx := client.WithCompletedRevisionSink(ctx, &landedRev)
	if err := b.cfg.Client.WriteStreamConditionalExpectedResource(wctx, path, bytes.NewReader(image), int64(len(image)), nil, rev, b.publicationIdentity(ino)); err != nil {
		if errors.Is(err, client.ErrResourceMismatch) {
			b.abandonIno(ino)
		}
		return err
	}
	b.mu.Lock()
	delete(b.commitErr, ino)
	b.mu.Unlock()
	b.surgicalDeletePending(ino, pending)
	b.releaseCommitted(ino, pending)
	newRev := landedRev
	if newRev <= 0 {
		// No echo: fall back to the stat, but only to ADVANCE the base. A
		// stale base would make every later publication send an obsolete
		// expected revision and fail with 409 forever — the constraint must
		// track the publication that just succeeded.
		if nst, err := b.cfg.Client.StatCtx(ctx, path); err == nil {
			newRev = nst.Revision
		}
	}
	b.mu.Lock()
	b.revs[ino] = newRev
	// A classic publication is SUCCESSFUL and bound: the identity stays (it
	// is the ownership proof for later writes) while the revision advances
	// to the one this publication produced. (The normal extent path retires
	// verifyBase because its own commit already carried the identity
	// forward; the classic path has no such carrier, so it must update.)
	if v, ok := b.verifyBase[ino]; ok && newRev > 0 {
		v.rev = newRev
		b.verifyBase[ino] = v
	}
	b.mu.Unlock()
	return nil
}

func (b *Bridge) commitWithRetry(ctx context.Context, ino meta.Ino, path string, blocks []client.ExtentBlockWrite, size, rev int64) error {
	expRes := b.publicationIdentity(ino)
	err := b.cfg.Client.CommitExtentPatchExpectedResource(ctx, path, size, blocks, rev, expRes)
	if err == nil {
		return nil
	}
	if errors.Is(err, client.ErrResourceMismatch) {
		b.abandonIno(ino)
		return err
	}
	// Revision conflict: re-base and retry once — blocks are
	// content-addressed, nothing is re-uploaded. The re-stat is
	// IDENTITY-CHECKED when a recovery constraint is active: a conflict
	// against a replacement must never publish the staged data into it.
	st, serr := b.cfg.Client.StatCtx(ctx, path)
	if serr != nil {
		return err
	}
	// The re-stat must still hold OUR object — a layout-pending upload's
	// captured identity constrains conflict retries exactly like a
	// verified recovery's.
	id2 := b.publicationIdentity(ino)
	if id2 != "" && st.ResourceID != id2 {
		b.abandonIno(ino)
		return fmt.Errorf("target %s no longer holds the staged object", path)
	}
	err2 := b.cfg.Client.CommitExtentPatchExpectedResource(ctx, path, size, blocks, st.Revision, id2)
	if errors.Is(err2, client.ErrResourceMismatch) {
		b.abandonIno(ino)
	}
	return err2
}

// flushPending commits a file's pending slices (used on Close and fsync
// paths). The snapshot -> commit -> cleanup sequence runs under the
// per-file commit lock: every retry then observes the previous commit's
// result instead of racing an earlier size snapshot.
func (b *Bridge) flushPending(ctx context.Context, ino meta.Ino) error {
	unlock := b.lockCommit(ino)
	defer unlock()
	return b.flushPendingLocked(ctx, ino)
}

// flushPendingLocked is flushPending for callers already holding the
// file's commit lock.
func (b *Bridge) flushPendingLocked(ctx context.Context, ino meta.Ino) error {
	b.mu.Lock()
	pending := b.pending[ino]
	size, has := b.sizes[ino]
	b.mu.Unlock()
	if len(pending) == 0 || !has {
		return nil
	}
	return b.commit(ctx, ino, pending, size)
}

// flushTruncateOnly handles a pure truncate whose size changed but no
// slices were written. The server accepts a truncation-only PATCH (no
// dirty blocks, including a shrink to zero). The pending marker is only
// dropped AFTER a successful commit — a failed attempt keeps it so a
// later flush retries instead of reporting success over stale data.
func (b *Bridge) flushTruncateOnly(ctx context.Context, ino meta.Ino, size int64) error {
	path, ok := b.remotePath(ino)
	if !ok {
		return nil
	}
	b.mu.Lock()
	_, tracked := b.truncs[ino]
	b.mu.Unlock()
	if !tracked {
		return nil
	}
	st, err := b.cfg.Client.StatCtx(ctx, path)
	if err != nil {
		if client.IsNotFound(err) {
			b.mu.Lock()
			delete(b.truncs, ino)
			b.mu.Unlock()
			return nil
		}
		return err
	}
	// A recovery constraint applies to metadata-only publication too: a
	// replacement at the saved path must never be truncated with the
	// staged object's size.
	if v, bounded := b.peekVerifyBase(ino); bounded {
		if st.ResourceID != v.resID {
			b.abandonIno(ino)
			return fmt.Errorf("recovery target %s no longer holds the staged object", path)
		}
		st.Revision = v.rev
	}
	if b.isClassicOwned(ino) && st.ContentLayout != client.ContentLayoutExtent {
		// A degraded (classic) target: compose the truncated image and
		// re-upload through the classic surface — a silent success would
		// resurrect the discarded tail on the next open.
		return b.commitClassic(ctx, ino, path, nil, size, st.Revision)
	}
	if st.Size == size || st.ContentLayout != client.ContentLayoutExtent {
		if st.Size == size {
			b.mu.Lock()
			delete(b.truncs, ino)
			b.mu.Unlock()
		}
		return nil
	}
	var blocks []client.ExtentBlockWrite
	switch {
	case size == 0:
		// Truncation-only PATCH: no dirty blocks, NewSize drives the
		// commit's truncation (drops the removed slices and dereferences
		// their blocks).
		blocks = nil
	case size > st.Size:
		n := int64(4096)
		if rem := size - st.Size; rem < n {
			n = rem
		}
		// A declared block never crosses a 64 MiB chunk boundary.
		if remain := client.ExtentChunkSize - (st.Size % client.ExtentChunkSize); n > remain {
			n = remain
		}
		if n <= 0 {
			return fmt.Errorf("truncate growth at a chunk boundary needs a server protocol change: %s", path)
		}
		chunk, off, _ := client.ChunkOffset(st.Size)
		blocks = []client.ExtentBlockWrite{{Chunk: chunk, Off: off, Data: make([]byte, n)}}
	default:
		// Shrink: re-declare the head block verbatim — the server already
		// holds its content (dedupe skips the upload) and the commit
		// carries the truncation.
		plan, err := b.cfg.Client.GetExtentReadPlan(ctx, path, 0, 0)
		if err != nil {
			return err
		}
		if len(plan.Blocks) == 0 {
			return fmt.Errorf("extent file %s has no blocks to re-declare for truncation", path)
		}
		blk := plan.Blocks[0]
		chunk, off, _ := client.ChunkOffset(blk.FileOffset)
		if blk.Zero {
			n := blk.Length
			if blk.FileOffset+n > size {
				n = size - blk.FileOffset
			}
			blocks = []client.ExtentBlockWrite{{Chunk: chunk, Off: off, Data: make([]byte, n)}}
		} else {
			length := blk.Length
			if blk.FileOffset+length > size {
				length = size - blk.FileOffset
			}
			rc, err := b.cfg.Client.FetchExtentBlockRange(ctx, blk.URL, blk.BlockOffset, length)
			if err != nil {
				return err
			}
			data, err := io.ReadAll(rc)
			_ = rc.Close()
			if err != nil {
				return err
			}
			blocks = []client.ExtentBlockWrite{{Chunk: chunk, Off: off, Data: data}}
		}
	}
	if err := b.commitWithRetry(ctx, ino, path, blocks, size, st.Revision); err != nil {
		return err // marker retained: the truncate has NOT landed
	}
	b.mu.Lock()
	delete(b.truncs, ino)
	b.mu.Unlock()
	return nil
}

// blockBytes returns the bytes of a pending block. The staged copy may be
// gone if the chunk store evicted it after upload; refetch via the plan.
func (b *Bridge) blockBytes(ctx context.Context, p pendingWrite, path string) ([]byte, error) {
	if data, ok := b.blocks.stagedBytes(p.checksum); ok {
		return data, nil
	}
	start := int64(p.chunk)*meta.ChunkSize + int64(p.off)
	plan, err := b.cfg.Client.GetExtentReadPlan(ctx, path, start, start+p.size-1)
	if err != nil {
		return nil, err
	}
	for _, blk := range plan.Blocks {
		if blk.BlockKey == p.checksum {
			rc, err := b.cfg.Client.FetchExtentBlockRange(ctx, blk.URL, blk.BlockOffset, blk.Length)
			if err != nil {
				return nil, err
			}
			data, err := io.ReadAll(rc)
			_ = rc.Close()
			if err != nil {
				return nil, err
			}
			b.blocks.stageBytes(p.checksum, data)
			return data, nil
		}
	}
	return nil, fmt.Errorf("block %s not found in plan for %s", p.checksum[:12], path)
}

// sparseImageAt serves a fresh file's logical image without materializing
// it: ranges covered by pending writes read their block bytes (via the
// staged cache or the plan), everything else reads as zeros.
type sparseImageAt struct {
	bridge *Bridge
	ctx    context.Context
	path   string
	size   int64
	writes []pendingWrite
}

func (s *sparseImageAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= s.size {
		return 0, io.EOF
	}
	end := off + int64(len(p))
	if end > s.size {
		end = s.size
	}
	pos := off
	for pos < end {
		// Find the pending write covering pos, if any.
		var cover *pendingWrite
		for i := range s.writes {
			w := &s.writes[i]
			wStart := int64(w.chunk)*meta.ChunkSize + int64(w.off)
			if pos >= wStart && pos < wStart+int64(len(blockData(s.bridge, s.ctx, s.path, w))) {
				cover = w
				break
			}
		}
		if cover == nil {
			// Zero gap up to the next write start or the request end.
			gapEnd := end
			for i := range s.writes {
				wStart := int64(s.writes[i].chunk)*meta.ChunkSize + int64(s.writes[i].off)
				if wStart > pos && wStart < gapEnd {
					gapEnd = wStart
				}
			}
			clear(p[pos-off : gapEnd-off])
			pos = gapEnd
			continue
		}
		data := blockData(s.bridge, s.ctx, s.path, cover)
		wStart := int64(cover.chunk)*meta.ChunkSize + int64(cover.off)
		n := copy(p[pos-off:end-off], data[pos-wStart:])
		pos += int64(n)
	}
	if end-off < int64(len(p)) {
		return int(end - off), io.EOF
	}
	return int(end - off), nil
}

// blockData returns a pending write's bytes (staged cache first).
func blockData(b *Bridge, ctx context.Context, path string, w *pendingWrite) []byte {
	if data, ok := b.blocks.stagedBytes(w.checksum); ok {
		return data
	}
	data, err := b.blockBytes(ctx, *w, path)
	if err != nil {
		return nil
	}
	return data
}

// ---- deliberately unsupported / server-owned ----

// ResolveLocal resolves a mount-local path against RemoteRoot, registering
// the path↔inode mapping. Fresh (created but not yet committed) files
// resolve through the local table.
func (b *Bridge) ResolveLocal(ctx context.Context, local string) (meta.Ino, syscall.Errno) {
	full := b.fullPath(local)
	ino := inoFor(full)
	// The registered owner of a pathname — a replacement incarnation, or a
	// renamed file at its new name — IS the live inode: every open of that
	// name converges on it (JuiceFS keys reader/writer state and write
	// invalidation by inode; a second key for one object would let a
	// reader keep stale bytes after a write through the other handle).
	// Otherwise a held path hash (live deleted-open or renamed-away
	// descriptor) means this is a NEW incarnation: allocate its synthetic
	// key and publish it in the same critical section, so concurrent first
	// resolutions converge and Create's mandatory re-resolve lands on the
	// key its Mknod allocated.
	b.mu.Lock()
	if rep, hasRep := b.replacements[full]; hasRep {
		ino = rep
	} else if held, isHeld := b.heldInoForLocked(full); isHeld && held == ino {
		ino = b.allocateHeldReplacementInoLocked()
		b.replacements[full] = ino
	}
	b.mu.Unlock()
	st, err := b.cfg.Client.StatCtx(ctx, full)
	if err != nil {
		if client.IsNotFound(err) {
			b.mu.Lock()
			_, fresh := b.sizes[ino]
			b.mu.Unlock()
			if fresh {
				b.remember(ino, full)
				return ino, 0
			}
			return 0, syscall.ENOENT
		}
		return 0, syscall.EIO
	}
	b.remember(ino, full)
	b.mu.Lock()
	b.revs[ino] = st.Revision
	if _, has := b.sizes[ino]; !has {
		b.sizes[ino] = st.Size
	}
	b.mu.Unlock()
	return ino, 0
}

func (b *Bridge) fullPath(local string) string {
	root := strings.TrimSuffix(b.cfg.RemoteRoot, "/")
	l := path.Clean("/" + strings.TrimPrefix(local, "/"))
	if root == "" || root == "/" {
		return l
	}
	if l == "/" {
		return root
	}
	return root + l
}

// LocalSizeOf reports the bridge's local size view of a mount-local path —
// the LIVE incarnation's: a replacement's synthetic inode when one owns the
// pathname (the original, renamed-away descriptor keeps its own length
// under the path hash and must not leak into the new file's attributes).
func (b *Bridge) LocalSizeOf(local string) (int64, bool) {
	full := b.fullPath(local)
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.sizes[b.liveInoLocked(full)]
	return s, ok
}

// StatLocal stats a mount-local path against RemoteRoot.
func (b *Bridge) StatLocal(ctx context.Context, local string) (*client.StatResult, error) {
	return b.cfg.Client.StatCtx(ctx, b.fullPath(local))
}

// ForgetPath drops every cached mapping for a mount-local path (the name
// changed or vanished underneath the engine, or its last handle closed).
func (b *Bridge) ForgetPath(local string) {
	full := b.fullPath(local)
	b.mu.Lock()
	defer b.mu.Unlock()
	// The registered owner of this pathname — a replacement incarnation,
	// or a renamed file at its new name — is retired with the name: its
	// last descriptor closed or the name vanished, and its key must not
	// survive to be inherited (stale length, stale revision) by the next
	// incarnation created at the same name. The old descriptor's
	// protection below never applies to the owner's own key.
	if rep, ok := b.replacements[full]; ok {
		delete(b.replacements, full)
		delete(b.moved, rep)
		b.forgetInoLocked(rep)
	}
	ino := inoFor(full)
	// A rename retarget moved this inode to a new pathname; forgetting the
	// OLD name must not destroy the live descriptor's state (its reads and
	// commits still flow through this key).
	if mapped, ok := b.paths[ino]; ok && mapped != full && !strings.HasPrefix(mapped, full+"/") {
		return
	}
	b.forgetInoLocked(ino)
	for i, p := range b.paths {
		if strings.HasPrefix(p, full+"/") {
			delete(b.paths, i)
		}
	}
}

// LocalResolve resolves a mount-local path purely from local state: the
// live incarnation's inode when the bridge holds a mapping for the name,
// without a server round trip. Fresh, not-yet-committed engine creates
// exist ONLY here until their first commit.
func (b *Bridge) LocalResolve(local string) (meta.Ino, bool) {
	full := b.fullPath(local)
	b.mu.Lock()
	defer b.mu.Unlock()
	ino := b.liveInoLocked(full)
	p, ok := b.paths[ino]
	return ino, ok && p == full
}

// LocalView reports the bridge's cached size and creation mode for an
// inode (ok=false when no size is cached).
func (b *Bridge) LocalView(ino meta.Ino) (size int64, mode uint32, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	size, ok = b.sizes[ino]
	mode = b.modes[ino]
	return
}

// stagedManifest is the durable record of one interactive fsync barrier:
// the file's pending blocks (spilled to sibling files), the size state a
// later mount needs to re-commit them, and the server identity of the
// object the bytes belong to — recovery must not adopt whichever object
// currently occupies the saved path.
type stagedManifest struct {
	Path string `json:"path"`
	// NextPaths are the OTHER names the staged object may currently hold:
	// pending rename destinations, plus every earlier destination still
	// reachable when a rename's outcome is unconfirmed. They accumulate —
	// a second rename ADDS its destination rather than replacing the
	// previous one — because until each rename is confirmed, the object may
	// be at any of them. Recovery accepts the object under Path or any
	// candidate; only a confirmed rename collapses the list.
	NextPaths []string `json:"next_paths,omitempty"`
	// NextPath is the pre-list single-destination field, read only so a
	// record written by an earlier build still recovers.
	NextPath   string `json:"next_path,omitempty"`
	Ino        uint64 `json:"ino"`
	ResourceID string `json:"resource_id,omitempty"`
	Classic    bool   `json:"classic,omitempty"` // the handle owns a degraded classic target
	Size       int64  `json:"size"`
	Trunc      int64  `json:"trunc,omitempty"`
	HasTrunc   bool   `json:"has_trunc,omitempty"`
	Blocks     []struct {
		Chunk    uint32 `json:"chunk"`
		Off      uint32 `json:"off"`
		Size     int64  `json:"size"`
		Checksum string `json:"checksum"`
		File     string `json:"file"`
	} `json:"blocks"`
}

// addNextPath appends p as a candidate destination, reporting whether the
// record changed. The saved Path is never recorded as a destination.
func (m *stagedManifest) addNextPath(p string) bool {
	if p == "" || p == m.Path {
		return false
	}
	for _, have := range m.NextPaths {
		if have == p {
			return false
		}
	}
	m.NextPaths = append(m.NextPaths, p)
	return true
}

// renamedOrigin maps the record's names through a rename of oldFull to
// newFull, reporting the first candidate that lies at or under the moved
// name.
func (m stagedManifest) renamedOrigin(oldFull, newFull string) (string, bool) {
	for _, p := range m.candidatePaths() {
		if mapped, ok := renamedUnder(p, oldFull, newFull); ok {
			return mapped, true
		}
	}
	return "", false
}

// candidatePaths lists every name the staged object may currently hold: the
// name saved at staging time followed by each recorded destination, newest
// first. Duplicates and empties are dropped, and the legacy single
// destination is folded in.
func (m stagedManifest) candidatePaths() []string {
	out := make([]string, 0, 2+len(m.NextPaths))
	add := func(p string) {
		if p == "" {
			return
		}
		for _, have := range out {
			if have == p {
				return
			}
		}
		out = append(out, p)
	}
	add(m.Path)
	for i := len(m.NextPaths) - 1; i >= 0; i-- {
		add(m.NextPaths[i])
	}
	add(m.NextPath) // legacy record
	return out
}

// resolveStagedPath decides which name currently holds the staged object.
// Every candidate is tried, not merely the most recent destination: a
// rename may have committed or not, and a second rename must not discard
// the previous destination that the object may still occupy. Returns the
// landed path and the identity stat that verified it (nil when the barrier
// carried no identity — a fresh create that had no server object yet).
func resolveStagedPath(c *client.Client, ctx context.Context, m stagedManifest) (string, *client.StatResult) {
	candidates := m.candidatePaths()
	if len(candidates) == 0 {
		return "", nil
	}
	if m.ResourceID == "" {
		// No identity to verify: the record can only trust its own names,
		// and the most recently recorded destination is the best guess.
		return candidates[0], nil
	}
	for _, p := range candidates {
		st, err := c.StatCtx(ctx, p)
		if err == nil && st.ResourceID == m.ResourceID {
			return p, st
		}
	}
	return "", nil
}

// readStagedManifest loads one staged record's manifest (ok=false when it is
// absent or unreadable).
func readStagedManifest(manifestPath string) (stagedManifest, bool) {
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		return stagedManifest{}, false
	}
	var m stagedManifest
	if json.Unmarshal(raw, &m) != nil {
		return stagedManifest{}, false
	}
	return m, true
}

// stagedGenOf extracts the generation stamp from a staging directory name
// (<ino>-<gen>).
func stagedGenOf(name string) int64 {
	i := strings.IndexByte(name, '-')
	if i < 0 {
		return 0
	}
	v, _ := strconv.ParseInt(name[i+1:], 10, 64)
	return v
}

// StagePendingLocal is the INTERACTIVE fsync barrier: everything pending
// on the file is spilled durably under the staging dir (block files + a
// manifest, fsynced) so a client crash loses nothing — the remote commit
// lands at the flush/close boundary here, or at the next mount's replay.
// Runs under the file's commit lock, and SUPERSEDES the file's earlier
// generations: pending only grows until a commit, so the newest barrier
// always carries a superset of every older one's bytes.
func (b *Bridge) StagePendingLocal(ino meta.Ino) error {
	if b.cfg.StagingDir == "" {
		return nil // no barrier armed: the mount degrades to strict fsync
	}
	unlock := b.lockCommit(ino)
	defer unlock()
	b.mu.Lock()
	path, mapped := b.paths[ino]
	size, hasSize := b.sizes[ino]
	pending := append([]pendingWrite(nil), b.pending[ino]...)
	trunc, hasTrunc := b.truncs[ino]
	b.mu.Unlock()
	if !mapped || (!hasSize && len(pending) == 0) {
		return nil
	}
	// Register this staging run for the pathname BEFORE any bytes are
	// written. A rename that settles while this run is in flight then defers
	// retiring its registration instead of erasing it, so the checks below
	// can still see the intent however the two interleave.
	b.enterStaging(path)
	defer b.exitStaging(path)
	// Bind the barrier to the object, not the name: renames and
	// replacements must not redirect recovery onto a different file. The
	// identity is MANDATORY for an existing remote object — a barrier
	// whose binding could not be established must not acknowledge.
	resID := ""
	if st, err := b.cfg.Client.StatCtx(context.Background(), path); err == nil {
		resID = st.ResourceID
	} else if !client.IsNotFound(err) {
		return fmt.Errorf("stage barrier for %s: identity unavailable: %w", path, err)
	}
	dir := filepath.Join(b.cfg.StagingDir, fmt.Sprintf("%d-%d", uint64(ino), time.Now().UnixNano()))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	clean := true
	defer func() {
		if clean {
			_ = os.RemoveAll(dir)
		}
	}()
	b.mu.Lock()
	classic := b.classicOwned[ino]
	b.mu.Unlock()
	var m stagedManifest
	m.Path, m.Ino, m.ResourceID, m.Classic = path, uint64(ino), resID, classic
	m.Size, m.Trunc, m.HasTrunc = size, trunc, hasTrunc
	ctx := context.Background()
	for i, p := range pending {
		data, has := b.blocks.stagedBytes(p.checksum)
		if !has {
			var err error
			data, err = b.blockBytes(ctx, p, path)
			if err != nil {
				return err
			}
		}
		name := fmt.Sprintf("%06d-%s", i, p.checksum)
		if err := writeFileSynced(filepath.Join(dir, name), data); err != nil {
			return err
		}
		m.Blocks = append(m.Blocks, struct {
			Chunk    uint32 `json:"chunk"`
			Off      uint32 `json:"off"`
			Size     int64  `json:"size"`
			Checksum string `json:"checksum"`
			File     string `json:"file"`
		}{Chunk: p.chunk, Off: p.off, Size: p.size, Checksum: p.checksum, File: name})
	}
	// The pause hook models a run that is still in flight; it must observe
	// the same state a real one would BEFORE taking the manifest lock (the
	// lock is what makes the publish atomic, so pausing inside it would
	// simply block the rename it is meant to interleave with).
	if pause := b.stagingPause; pause != nil {
		pause()
	}
	// A rename may be IN FLIGHT for this pathname right now. StagePendingLocal
	// does not hold the plane's path locks, so this generation can be written
	// after the rename's intent scan and before the remote rename commits —
	// in which case the older generation carrying the intent is superseded
	// here, and a record naming only the OLD path would strand these
	// acknowledged bytes if the rename then commits and the process crashes.
	// The registration is consulted AFTER the bytes are written and again
	// after the manifest lands: the writer either sees a registration whose
	// scan can no longer reach this record, or the scan sees a record that
	// already carries the intent — one of the two orderings always wins.
	m.addNextPath(b.pendingRenameFor(path))
	// The inode's CURRENT mapping wins over the snapshot: RemapLocal runs
	// when a rename commits, so a different name here means the object has
	// already moved and this barrier must say so even if the registration
	// was retired before this run resumed.
	if cur, ok := b.currentPath(ino); ok && cur != path {
		m.Path = cur
	}
	// MERGE WITH DISK before replacing: a rename writer may have settled a
	// NEWER record between this run's snapshot and this write (recording a
	// further destination, and possibly moving Path). Overwriting blindly
	// would erase it, and after a crash the object's real name would appear
	// in no candidate. Candidates are a union — strictly more knowledge is
	// never wrong — and the on-disk Path wins when it differs and the
	// current mapping has not advanced past it.
	// Test-only interleave point: a rename may complete entirely inside this
	// window. The publish then re-reads both the inode mapping and the disk
	// record under the lock, so the rename is observed either way.
	if pause := b.stagingPausePublish; pause != nil {
		pause()
	}
	// Publish under the manifest lock: the whole read-merge-write is one
	// critical section, so no rename writer can settle a newer record
	// between our read and our replacement. (A write-then-reverify, tried
	// earlier, cannot detect that: the read-back returns exactly the bytes
	// the replacement just wrote.)
	b.manifestMu.Lock()
	// Re-read the in-memory mapping INSIDE the lock: a rename that completed
	// while this run was staging may have moved the object after our earlier
	// snapshot. Lock order is manifestMu -> b.mu, consistently.
	if cur, ok := b.currentPath(ino); ok && cur != path {
		m.Path = cur
	}
	if to := b.pendingRenameFor(path); to != "" {
		m.addNextPath(to)
	}
	// A rename may have settled the record on disk before we got the lock;
	// its candidates and its (confirmed) path are newer than our snapshot.
	if prev, ok := readStagedManifest(filepath.Join(dir, "manifest.json")); ok {
		for _, cand := range prev.candidatePaths() {
			m.addNextPath(cand)
		}
		if prev.Path != m.Path {
			m.Path = prev.Path
		}
	}
	if pause := b.manifestPause; pause != nil {
		pause()
	}
	raw, merr := json.Marshal(m)
	if merr != nil {
		b.manifestMu.Unlock()
		return merr
	}
	if err := writeFileSynced(filepath.Join(dir, "manifest.json"), raw); err != nil {
		b.manifestMu.Unlock()
		return err
	}
	b.manifestMu.Unlock()
	clean = false
	// Supersede: pending is cumulative, so older generations of this file
	// are redundant once this one is durable.
	b.removeStagedGensLocked(ino, stagedGenOf(filepath.Base(dir)))
	return nil
}

// ReplayStagedLocal re-commits every staged interactive barrier that never
// landed (a crashed or interrupted mount). Each record is recovered
// all-or-nothing — one unreadable block preserves the whole record for the
// next mount instead of committing a subset — and only when the saved path
// still holds the object the bytes were staged for; a renamed, deleted or
// replaced file is never adopted. Runs at engine construction (writable
// mounts only).
func (b *Bridge) ReplayStagedLocal(ctx context.Context) {
	if b.cfg.StagingDir == "" {
		return
	}
	entries, err := os.ReadDir(b.cfg.StagingDir)
	if err != nil {
		return
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Slice(names, func(i, j int) bool { return stagedGenOf(names[i]) < stagedGenOf(names[j]) })
	for _, name := range names {
		dir := filepath.Join(b.cfg.StagingDir, name)
		raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
		if err != nil {
			continue
		}
		var m stagedManifest
		if json.Unmarshal(raw, &m) != nil || m.Ino == 0 {
			continue
		}
		ino := meta.Ino(m.Ino)
		// All-or-nothing: any unreadable block preserves the record.
		pending := make([]pendingWrite, 0, len(m.Blocks))
		blocks := make(map[string][]byte, len(m.Blocks))
		complete := true
		for _, blk := range m.Blocks {
			data, err := os.ReadFile(filepath.Join(dir, blk.File))
			if err != nil {
				complete = false
				break
			}
			blocks[blk.Checksum] = data
			pending = append(pending, pendingWrite{chunk: blk.Chunk, off: blk.Off, checksum: blk.Checksum, size: blk.Size})
		}
		if !complete {
			continue
		}
		// Identity: the bytes belong to the staged object, not to
		// whichever file occupies the saved path now. The SAME stat feeds
		// the commit's CAS base, so a replacement cannot slip between the
		// verification and the publication.
		//
		// A PENDING rename (NextPath) makes recovery accept the object at
		// EITHER name — the intent is written before the remote rename, so
		// a crash on either side of it must still find the bytes. The
		// landed name wins when both hold the object (they cannot: a rename
		// moves the inode).
		landedPath, verified := resolveStagedPath(b.cfg.Client, ctx, m)
		if landedPath == "" {
			continue // moved, deleted or replaced: leave the record
		}
		for sum, data := range blocks {
			b.blocks.stageBytes(sum, data)
		}
		unlock := b.lockCommit(ino)
		b.mu.Lock()
		// Direct assignment: remember() takes b.mu itself.
		b.paths[ino] = landedPath
		b.sizes[ino] = m.Size
		b.pending[ino] = pending
		if m.HasTrunc {
			b.truncs[ino] = m.Trunc
		}
		if m.Classic {
			// The handle owned a degraded classic target: recovery keeps
			// committing through the classic surface.
			b.classicOwned[ino] = true
		}
		b.mu.Unlock()
		if verified != nil {
			// The identity-verified stat CONSTRAINS the publication: the
			// next commit must find the same object and CAS on this
			// revision (commit's own stat only confirms the identity).
			b.setVerifyBase(ino, verified.Revision, m.ResourceID)
		}
		err = b.flushPendingLocked(ctx, ino)
		if err == nil && m.HasTrunc && len(pending) == 0 {
			err = b.flushTruncateOnly(ctx, ino, m.Size)
		}
		if err == nil && m.Size == 0 && len(pending) == 0 && !m.HasTrunc {
			// An empty-file barrier must still materialize the NAME: a
			// successful fsync acknowledged the file's existence, and
			// dropping the record without creating it would make the
			// acknowledged file disappear. No identity is recorded: the
			// create call returns none, and a follow-up Stat could describe
			// a concurrent replacement. A later promotion therefore never
			// deletes this occupant — it commits classically instead.
			if st, serr := b.StatLocal(ctx, landedPath); serr != nil && client.IsNotFound(serr) {
				_, err = b.cfg.Client.CreateFileCtx(ctx, landedPath)
			} else if serr != nil {
				err = serr
			} else {
				_ = st
			}
		}
		unlock()
		if err == nil {
			_ = os.RemoveAll(dir)
		}
		// A failed or skipped replay leaves the record for the next
		// mount; the staged bytes are also live in this bridge's
		// registry, so an open of the file can still serve them.
	}
}

// clearStagedLocal drops a file's staged barriers after a successful
// remote commit made them redundant. Safe under the file's commit lock:
// StagePendingLocal takes the same lock, so no newer barrier can exist.
func (b *Bridge) clearStagedLocal(ino meta.Ino) {
	if b.cfg.StagingDir == "" {
		return
	}
	entries, err := os.ReadDir(b.cfg.StagingDir)
	if err != nil {
		return
	}
	prefix := fmt.Sprintf("%d-", uint64(ino))
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), prefix) {
			_ = os.RemoveAll(filepath.Join(b.cfg.StagingDir, e.Name()))
		}
	}
}

// writeFileSynced writes a file and fsyncs it (the staging barrier's
// durability point).
func writeFileSynced(path string, data []byte) error {
	if err := writeFileAtomic(path, data); err != nil {
		return err
	}
	return nil
}

// writeFileAtomic replaces path with data and makes the REPLACEMENT durable:
// the temp file is fsynced, renamed over the target, and the containing
// directory is fsynced so the new name itself survives power loss. A file
// fsync alone only preserves the inode's bytes — after a crash the directory
// entry may still point at the old file.
func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(dir)
}

// syncDir fsyncs a directory so renames/creations inside it are durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

// stagedRecord is one staged barrier's directory and decoded manifest.
type stagedRecord struct {
	dir string
	m   stagedManifest
}

// scanStaged enumerates the staged barriers. A manifest that exists but
// cannot be READ is an error (the caller is about to move names, and an
// unreadable record may well name the path being moved — failing closed
// keeps it recoverable). A manifest that cannot be DECODED is skipped: it
// carries no pathname, so no rename can strand it further.
func (b *Bridge) scanStaged() ([]stagedRecord, error) {
	if b.cfg.StagingDir == "" {
		return nil, nil
	}
	entries, err := os.ReadDir(b.cfg.StagingDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []stagedRecord
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(b.cfg.StagingDir, e.Name())
		raw, rerr := os.ReadFile(filepath.Join(dir, "manifest.json"))
		if rerr != nil {
			if os.IsNotExist(rerr) {
				continue // staging in progress or already cleaned up
			}
			return nil, rerr
		}
		var m stagedManifest
		if json.Unmarshal(raw, &m) != nil {
			continue
		}
		out = append(out, stagedRecord{dir: dir, m: m})
	}
	return out, nil
}

// renamedUnder maps p through a rename of oldFull to newFull, reporting
// whether p lies at or under the moved name.
func renamedUnder(p, oldFull, newFull string) (string, bool) {
	switch {
	case p == oldFull:
		return newFull, true
	case strings.HasPrefix(p, oldFull+"/"):
		return newFull + p[len(oldFull):], true
	}
	return "", false
}

// StageRenameIntent durably records that every staged barrier at or under
// oldFull is moving to newFull. It MUST succeed BEFORE the remote rename is
// issued: afterwards recovery accepts the staged object at EITHER name, so
// a crash on either side of the remote rename cannot strand acknowledged
// bytes. A failure here refuses the rename while nothing has moved yet —
// acknowledging it would leave the barrier naming a name that no longer
// holds the object (or, worse, the reverse) forever.
//
// Two further obligations:
//
//   - A record whose SAVED name or any candidate destination lies under
//     oldFull is moving too: a previous rename may have committed with its
//     post-rename cleanup failing locally, so a candidate is the live
//     location. The new destination is ADDED to the candidates rather than
//     replacing them, because nothing is confirmed until the settle.
//   - Barriers STAGED while the rename is in flight must inherit the intent.
//     StagePendingLocal does not run under the plane's path locks, so the
//     in-flight registration is installed FIRST and StagePendingLocal
//     re-checks it after writing — either the scan below sees the new
//     manifest, or the manifest sees the registration.
func (b *Bridge) StageRenameIntent(oldFull, newFull string) error {
	if b.cfg.StagingDir == "" || oldFull == newFull {
		return nil
	}
	b.mu.Lock()
	if b.pendingRenames == nil {
		b.pendingRenames = make(map[string]string)
	}
	b.pendingRenames[oldFull] = newFull
	b.mu.Unlock()
	// Serialize with the staging publish (lock order: manifestMu outermost).
	b.manifestMu.Lock()
	defer b.manifestMu.Unlock()
	recs, err := b.scanStaged()
	if err != nil {
		b.ClearRenameIntent(oldFull, newFull)
		return fmt.Errorf("stage rename intent %s → %s: %w", oldFull, newFull, err)
	}
	for _, r := range recs {
		// Match the SAVED name or any recorded destination, and use the
		// MAPPED name: a record for a descendant of a renamed directory must
		// get its own destination (/a/file → /b/file), never the directory
		// root. Recording /b for an object that lives at /b/file leaves
		// recovery probing a directory and the missing old name.
		mapped, ok := r.m.renamedOrigin(oldFull, newFull)
		if !ok {
			continue
		}
		// ADD the destination; never replace an existing candidate, and
		// never move Path here. Until the remote rename is CONFIRMED the
		// object is still at whatever name it held, so Path and every
		// earlier candidate must stay reachable: a crash before this RPC
		// commits, or a definite failure of it, then recovers from the
		// pre-rename names instead of only the name it never reached.
		if !r.m.addNextPath(mapped) {
			continue
		}
		raw, merr := json.Marshal(r.m)
		if merr != nil {
			b.ClearRenameIntent(oldFull, newFull)
			return fmt.Errorf("stage rename intent for %s: %w", r.m.Path, merr)
		}
		if werr := writeFileAtomic(filepath.Join(r.dir, "manifest.json"), raw); werr != nil {
			b.ClearRenameIntent(oldFull, newFull)
			return fmt.Errorf("stage rename intent for %s: %w", r.m.Path, werr)
		}
	}
	return nil
}

// ClearRenameIntent retires the in-flight registration for a rename (it has
// either settled or failed). The durable NextPath entries are untouched: an
// ambiguous outcome keeps them so recovery tries both names.
func (b *Bridge) ClearRenameIntent(oldFull, newFull string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if cur, ok := b.pendingRenames[oldFull]; !ok || cur != newFull {
		return
	}
	if b.stagingInFlight[oldFull] > 0 {
		// An fsync is mid-stage for this pathname. Retiring the registration
		// now would let that run finish with only the OLD name persisted
		// and no intent, stranding its acknowledged bytes if the rename
		// already committed. Defer to the last exit instead.
		b.renameClearPending[oldFull] = true
		return
	}
	delete(b.pendingRenames, oldFull)
	delete(b.renameClearPending, oldFull)
}

// pendingRenameLocked reports the destination a in-flight rename assigns to
// full, if any. The caller holds b.mu.
func (b *Bridge) pendingRenameLocked(full string) (string, bool) {
	for from, to := range b.pendingRenames {
		if mapped, ok := renamedUnder(full, from, to); ok {
			return mapped, true
		}
	}
	return "", false
}

// enterStaging registers an in-flight staging run for a pathname.
func (b *Bridge) enterStaging(path string) {
	b.mu.Lock()
	b.stagingInFlight[path]++
	b.mu.Unlock()
}

// exitStaging retires an in-flight staging run and performs any rename
// registration retirement that was deferred while it was active.
func (b *Bridge) exitStaging(path string) {
	b.mu.Lock()
	if n := b.stagingInFlight[path]; n > 1 {
		b.stagingInFlight[path] = n - 1
		b.mu.Unlock()
		return
	}
	delete(b.stagingInFlight, path)
	if b.renameClearPending[path] {
		delete(b.renameClearPending, path)
		for from, to := range b.pendingRenames {
			if from == path {
				delete(b.pendingRenames, from)
				_ = to
			}
		}
	}
	b.mu.Unlock()
}

// currentPath reports the inode's current remote pathname.
func (b *Bridge) currentPath(ino meta.Ino) (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	p, ok := b.paths[ino]
	return p, ok
}

// pendingRenameFor reports the destination an in-flight rename assigns to
// full ("" when none).
func (b *Bridge) pendingRenameFor(full string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	to, _ := b.pendingRenameLocked(full)
	return to
}

// SettleRenameIntent adopts the destination of a rename that LANDED: every
// record at or under the old name (a directory rename strands all of its
// descendants, not only the renamed inode) follows, and the recorded
// intents are retired. It is only called for a committed rename —
// deliberately best-effort, because a leftover NextPath stays recoverable
// (replay tries both names), so a failure here can only delay cleanup.
func (b *Bridge) SettleRenameIntent(oldFull, newFull string) {
	if b.cfg.StagingDir == "" || newFull == "" {
		return
	}
	b.manifestMu.Lock()
	defer b.manifestMu.Unlock()
	recs, err := b.scanStaged()
	if err != nil {
		return
	}
	for _, r := range recs {
		changed := false
		// The record follows the rename when the SAVED name or ANY
		// candidate lies in the moved subtree — descendants included, since
		// they hold their own inodes and no per-inode retarget reaches them.
		if next, ok := r.m.renamedOrigin(oldFull, newFull); ok {
			r.m.Path, r.m.NextPaths, r.m.NextPath = next, nil, ""
			changed = true
		}
		// A recorded destination inside the new subtree is confirmed by
		// this rename: adopt it and collapse every other candidate, since
		// the object is now known to be here.
		for _, cand := range r.m.candidatePaths() {
			if cand == newFull || strings.HasPrefix(cand, newFull+"/") {
				if r.m.Path != cand {
					r.m.Path = cand
				}
				r.m.NextPaths, r.m.NextPath = nil, ""
				changed = true
				break
			}
		}
		if !changed {
			continue
		}
		raw, merr := json.Marshal(r.m)
		if merr != nil {
			continue
		}
		_ = writeFileAtomic(filepath.Join(r.dir, "manifest.json"), raw)
	}
}

// StageRenameIntentLocal is StageRenameIntent for mount-local paths.
func (b *Bridge) StageRenameIntentLocal(srcLocal, dstLocal string) error {
	return b.StageRenameIntent(b.fullPath(srcLocal), b.fullPath(dstLocal))
}

// ClearRenameIntentLocal is ClearRenameIntent for mount-local paths.
func (b *Bridge) ClearRenameIntentLocal(srcLocal, dstLocal string) {
	b.ClearRenameIntent(b.fullPath(srcLocal), b.fullPath(dstLocal))
}

// SettleRenameIntentLocal is SettleRenameIntent for mount-local paths.
func (b *Bridge) SettleRenameIntentLocal(srcLocal, dstLocal string) {
	b.SettleRenameIntent(b.fullPath(srcLocal), b.fullPath(dstLocal))
}

// stagedRetarget is one deferred durable-manifest retarget: the inode whose
// record must follow a rename, and its new pathname.
type stagedRetarget struct {
	ino  meta.Ino
	path string
}

// applyStagedRetargets performs deferred retargets under the manifest lock.
// Called AFTER b.mu is released, so the lock order stays manifestMu -> b.mu.
func (b *Bridge) applyStagedRetargets(rs []stagedRetarget) {
	if len(rs) == 0 || b.cfg.StagingDir == "" {
		return
	}
	b.manifestMu.Lock()
	defer b.manifestMu.Unlock()
	for _, r := range rs {
		b.retargetStagedGensLocked(r.ino, r.path)
	}
}

// retargetStagedGensLocked rewrites the saved pathname of every staged
// generation of ino, clearing any pending rename intent. Best-effort: the
// callers (the unlink sentinel and the event-based rename hook) cannot
// surface an error to the syscall that caused them.
func (b *Bridge) retargetStagedGensLocked(ino meta.Ino, newFull string) {
	recs, err := b.scanStaged()
	if err != nil {
		return
	}
	prefix := fmt.Sprintf("%d-", uint64(ino))
	for _, r := range recs {
		if !strings.HasPrefix(filepath.Base(r.dir), prefix) {
			continue
		}
		if r.m.Path == newFull && r.m.NextPath == "" && len(r.m.NextPaths) == 0 {
			continue
		}
		r.m.Path = newFull
		r.m.NextPath = ""
		r.m.NextPaths = nil
		raw, merr := json.Marshal(r.m)
		if merr != nil {
			continue
		}
		_ = writeFileAtomic(filepath.Join(r.dir, "manifest.json"), raw)
	}
}

// removeStagedGensLocked removes the file's staged generations strictly
// older than keepGen (the caller holds the file's commit lock, so no new
// generation can appear concurrently).
func (b *Bridge) removeStagedGensLocked(ino meta.Ino, keepGen int64) {
	entries, err := os.ReadDir(b.cfg.StagingDir)
	if err != nil {
		return
	}
	prefix := fmt.Sprintf("%d-", uint64(ino))
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), prefix) {
			continue
		}
		if gen := stagedGenOf(e.Name()); gen < keepGen {
			_ = os.RemoveAll(filepath.Join(b.cfg.StagingDir, e.Name()))
		}
	}
}

// lockCommit serializes a file's commit lifecycle (snapshot -> remote
// commit -> state cleanup) against concurrent chunk commits of the same
// file.
func (b *Bridge) lockCommit(ino meta.Ino) func() {
	b.commitMu.Lock()
	mu, ok := b.commitLocks[ino]
	if !ok {
		mu = &sync.Mutex{}
		b.commitLocks[ino] = mu
	}
	b.commitMu.Unlock()
	mu.Lock()
	return mu.Unlock
}

// readClassic serves a chunk of a file whose extent session degraded to
// a classic upload: fetch the raw range and expose it as one staged slice
// (the extent plan surface cannot describe this file).
func (b *Bridge) readClassic(ctx context.Context, ino meta.Ino, path string, start, chunkEnd int64, slices *[]meta.Slice) syscall.Errno {
	b.mu.Lock()
	size := b.sizes[ino]
	pending := append([]pendingWrite(nil), b.pending[ino]...)
	b.mu.Unlock()
	length := chunkEnd + 1 - start
	if size > 0 && start+length > size {
		length = size - start
	}
	if length <= 0 {
		*slices = nil
		return 0
	}
	rc, err := b.cfg.Client.ReadStreamRange(ctx, path, start, length)
	if err != nil {
		return errnoOf(err)
	}
	// The identity is taken from the READ's own response headers when the
	// server provides one (same-request binding); a separate later Stat
	// could describe a replacement installed in between.
	hdrID := rcHeaderResourceID(rc)
	data, rerr := io.ReadAll(rc)
	_ = rc.Close()
	if rerr != nil {
		// A partial body must never publish as a successful read.
		return syscall.EIO
	}
	// Deferred (interactive) writes overlay the remote base so
	// read-after-write holds while publication is deferred.
	for _, p := range pending {
		off := int64(p.chunk)*meta.ChunkSize + int64(p.off)
		end := off + p.size
		// Bounded by the REQUESTED range (clamped to the local size
		// above), never by the downloaded base length: appends and sparse
		// writes live past the remote EOF until publication.
		if end <= start || off >= start+length {
			continue
		}
		pdata, err := b.blockBytes(ctx, p, path)
		if err != nil {
			return syscall.EIO
		}
		if off < start {
			pdata = pdata[start-off:]
			off = start
		}
		if end > start+length {
			pdata = pdata[:start+length-off]
			end = start + length
		}
		if end > start+int64(len(data)) {
			data = append(data, make([]byte, end-start-int64(len(data)))...)
		}
		copy(data[off-start:], pdata)
	}
	sum := sha256Hex(data)
	// The refetch verifies the READ RESPONSE's identity; a stat-derived
	// fallback cannot (the redirect/206 and legacy no-echo wire paths carry
	// no echo, and a later stat may describe a replacement). Without a
	// same-read identity the entry simply never demotes — its bytes stay
	// resident and accounted instead of becoming an unreadable source.
	id := b.stageClassicChunk(sum, data, path, start, len(pending) > 0, hdrID, ino)
	*slices = []meta.Slice{{Id: id, Size: uint32(len(data)), Len: uint32(len(data))}}
	return 0
}

// classicReadCacheBytes bounds the staged bytes retained for reads of
// classic-owned (degraded) files. A var so tests can shrink it.
var classicReadCacheBytes int64 = 64 << 20

// classicReadProtect is how many of the most recently handed-out records
// are immune from retirement: the engine fetches a returned slice's bytes
// right after meta.Read (and a readahead may hold several), so the newest
// few ids must stay fetchable. Everything older is bounded by the byte
// budget ALONE — no time-based amnesty: 30s of scanning distinct 64 MiB
// chunks must not retain gigabytes.
var classicReadProtect = 4

// stageClassicChunk publishes a classic chunk read under its deterministic
// id and retires the OLDEST classic-sourced entries beyond the budget.
// Records are deduplicated by id (repeated content refreshes its record —
// retiring a duplicate would delete the id an earlier reader was just
// handed), records inside the grace window are immune (in-flight fetches),
// and pinned entries (a pending commit still references their checksum)
// are never retired.
func (b *Bridge) stageClassicChunk(sum string, data []byte, path string, start int64, overlaid bool, resID string, ino meta.Ino) uint64 {
	id := b.blocks.stagePinned(sum, data, false)
	if overlaid {
		b.blocks.markLocalOverlay(id, ino)
	}
	if resID != "" {
		b.blocks.setProvenanceIdentity(id, resID, ino)
	}
	b.mu.Lock()
	now := time.Now()
	// Refresh an existing record for this id instead of appending a
	// duplicate — and RE-PIN it: the second reader carries its own
	// outstanding lifetime.
	for i := range b.classicLRU {
		if b.classicLRU[i].id == id {
			rec := b.classicLRU[i]
			rec.at = now
			b.classicLRU = append(b.classicLRU[:i], b.classicLRU[i+1:]...)
			b.classicLRU = append(b.classicLRU, rec)
			b.mu.Unlock()
			// A refreshed handout issues a fresh handout's worth of pins
			// WITHOUT clearing existing ones: another outstanding reader
			// may still hold the id (clearing would strip its
			// protection). Stale pins age out with the entry (kept
			// accounted by the retirement re-append; a fresh staging of
			// the same id only ever ADDS readers).
			for issued := (rec.bytes + client.ExtentBlockSize - 1) / client.ExtentBlockSize; issued > 0; issued-- {
				b.blocks.issuePin(id)
			}
			return id
		}
	}
	b.classicLRU = append(b.classicLRU, classicRef{sum: sum, id: id, bytes: int64(len(data)), at: now, path: path, start: start, resID: resID, ino: ino})
	b.classicBytes += int64(len(data))
	var retire []classicRef
	for b.classicBytes > classicReadCacheBytes {
		if len(b.classicLRU) <= classicReadProtect {
			break // the newest handed-out records stay fetchable
		}
		oldest := b.classicLRU[0]
		b.classicLRU = b.classicLRU[1:]
		b.classicBytes -= oldest.bytes
		retire = append(retire, oldest)
	}
	b.mu.Unlock()
	var retained []classicRef
	for _, r := range retire {
		// DEMOTE first whenever a provenance exists (issued or not): the
		// cached bytes are dropped (freeing the budget) while the id stays
		// fetchable through the IDENTITY-VERIFIED, rename-following
		// source. Entries that cannot demote (local-only bytes, a pending
		// reference, or no recorded source identity) KEEP their bytes and
		// are re-appended so they stay ACCOUNTED and reclaimable (the
		// overlay flag self-clears at the flush).
		if b.blocks.demoteToProvenance(r.id, r.sum, r.path, r.start, r.resID, r.ino) {
			continue
		}
		if b.blocks.isIssued(r.id) {
			retained = append(retained, r)
			continue
		}
		b.blocks.releaseIfUnpinned(r.sum)
	}
	if len(retained) > 0 {
		b.mu.Lock()
		b.classicLRU = append(b.classicLRU, retained...)
		for _, r := range retained {
			b.classicBytes += r.bytes
		}
		b.mu.Unlock()
	}
	// One pin per BLOCK the reader may fetch: a 64 MiB slice is consumed
	// by sixteen separate 4 MiB Gets, and pins are NOT consumed by any
	// block fetch (a block boundary is not the end of an outstanding
	// user; clearing there would strip mid-slice and cross-reader
	// protection). The OUTSTANDING set is bounded instead: a handout
	// beyond classicReadProtect distinct issued ids releases the OLDEST
	// issued id's pins — a stalled reader behind that many newer distinct
	// chunk reads loses protection (documented residual), while ordinary
	// multi-block and concurrent-reader lifetimes stay covered.
	for issued := (int64(len(data)) + client.ExtentBlockSize - 1) / client.ExtentBlockSize; issued > 0; issued-- {
		b.blocks.issuePin(id)
	}
	b.blocks.trimIssued(classicReadProtect)
	return id
}

// surgicalDeletePending removes exactly the given snapshot entries from
// the inode's pending list: writes appended while a commit was in flight
// survive it.
func (b *Bridge) surgicalDeletePending(ino meta.Ino, pending []pendingWrite) {
	b.mu.Lock()
	if cur := b.pending[ino]; len(cur) >= len(pending) {
		keys := make(map[pendingWrite]int, len(pending))
		for _, p := range pending {
			keys[p]++
		}
		next := make([]pendingWrite, 0, len(cur)-len(pending))
		for _, p := range cur {
			if keys[p] > 0 {
				keys[p]--
				continue
			}
			next = append(next, p)
		}
		if len(next) == 0 {
			delete(b.pending, ino)
		} else {
			b.pending[ino] = next
		}
	} else {
		delete(b.pending, ino)
	}
	delete(b.truncs, ino)
	delete(b.modes, ino)
	b.mu.Unlock()
}

// isLayoutPending reports whether OUR fresh upload awaits layout
// classification (a stat outage after a successful upload).
func (b *Bridge) isLayoutPending(ino meta.Ino) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.layoutPending[ino]
}

// layoutPendingMatches reports whether the pending-classification upload
// is still the occupant. With a recorded identity the occupant must match
// it exactly. Without one — a total stat outage around OUR successful
// fresh upload — the upload PROVABLY created this path's occupant (the
// path did not exist before), so recovery adopts whichever object is
// there: refusing would strand the handle forever after a transient
// outage, and the residual risk is a replacement installed DURING a
// complete outage, which no client-side evidence can distinguish.
func (b *Bridge) layoutPendingMatches(ino meta.Ino, occupantID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.layoutPending[ino] {
		return false
	}
	// An UNKNOWN identity authorizes nothing — across retries and layouts
	// alike: without the completion echo there is no evidence tying the
	// pathname's occupant to OUR upload, so adopting it (this attempt or
	// a later one, classic or extent) would hand this descriptor a
	// replacement's bytes under its own revision. The pending state stays
	// refused until a completion supplies the identity.
	return b.layoutPendingID[ino] != "" && b.layoutPendingID[ino] == occupantID
}

// imageMatchesPending reports whether the pathname's current extent object
// holds EXACTLY the image this descriptor staged: the same size, every
// staged block represented at its exact offset over its exact byte range,
// and no non-zero block outside the staged ranges (everything else is a
// hole, which the staged image also serves as zeros).
//
// Neither a shared checksum nor FileOffset+BlockKey is enough. A plan block
// also carries Length and BlockOffset, and a block that references only a
// PREFIX of our content (same key, same start offset, shorter length)
// followed by holes would otherwise pass: the one wanted offset gets marked
// seen while the rest of the range is zeros, so a same-size replacement with
// a truncated tail could be bound as our upload. Every staged range must
// therefore be represented with its full length, read from the start of its
// object.
func (b *Bridge) imageMatchesPending(ctx context.Context, path string, size int64, pending []pendingWrite) bool {
	if size <= 0 {
		return false
	}
	type stagedBlock struct {
		length int64
		key    string
	}
	want := make(map[int64]stagedBlock, len(pending))
	for _, p := range pending {
		start := int64(p.chunk)*meta.ChunkSize + int64(p.off)
		want[start] = stagedBlock{length: p.size, key: p.checksum}
	}
	if len(want) == 0 {
		return false
	}
	seen := make(map[int64]bool, len(want))
	// Page through the plan so the whole image is observed, not only the
	// windows containing our own blocks: an EXTRA non-zero block is a
	// difference and must refuse. Plans page by chunk (NextOffset).
	off, prev := int64(0), int64(-1)
	for i := 0; i < maxStagedPlanPages && off < size; i++ {
		plan, err := b.cfg.Client.GetExtentReadPlan(ctx, path, off, -1)
		if err != nil {
			return false // transient or non-extent answer: refuse
		}
		if plan.Size != size {
			return false
		}
		for _, blk := range plan.Blocks {
			if blk.Zero || blk.BlockKey == "" {
				continue // a hole agrees with our zero-filled gaps
			}
			exp, ours := want[blk.FileOffset]
			// The block must represent OUR bytes: same content key, the
			// full staged length, and read from the beginning of that
			// object. A partial reference to the same content is a
			// different image (holes where our bytes should be).
			if !ours || exp.key != blk.BlockKey ||
				exp.length != blk.Length || blk.BlockOffset != 0 {
				return false
			}
			seen[blk.FileOffset] = true
		}
		if plan.NextOffset <= prev || plan.NextOffset <= off {
			break // no further pages
		}
		prev, off = plan.NextOffset, plan.NextOffset
	}
	if len(seen) != len(want) {
		return false // some staged block is not in the object
	}
	return true
}

// rcHeaderResourceID extracts an identity echoed by a read response, when
// the server provides one on the same request that supplied the bytes.
func rcHeaderResourceID(rc io.ReadCloser) string {
	type headerer interface{ Header() http.Header }
	if h, ok := rc.(headerer); ok {
		return h.Header().Get("X-Dat9-Resource-ID")
	}
	return ""
}

// publicationIdentity returns the resource identity this inode's next
// publication must be bound to: a verified recovery's constraint, or a
// still-unclassified upload's captured (echoed) identity. Empty when
// neither applies.
func (b *Bridge) publicationIdentity(ino meta.Ino) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if v, ok := b.verifyBase[ino]; ok {
		return v.resID
	}
	return b.layoutPendingID[ino]
}

// peekVerifyBase reads the recovery constraint without consuming it.
func (b *Bridge) peekVerifyBase(ino meta.Ino) (verifyBase, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	v, ok := b.verifyBase[ino]
	return v, ok
}

// abandonIno drops every mapping for an inode whose recovery target was
// replaced: the staged data must never reach a later unconstrained
// publication.
func (b *Bridge) abandonIno(ino meta.Ino) {
	b.mu.Lock()
	delete(b.freshCreate, ino)
	b.mu.Unlock()

	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.verifyBase, ino)
	delete(b.layoutPending, ino)
	delete(b.layoutPendingID, ino)
	delete(b.pending, ino)
	delete(b.truncs, ino)
	delete(b.sizes, ino)
	delete(b.paths, ino)
	delete(b.replacements, "")
	delete(b.classicOwned, ino)
}

// clearLayoutPending drops the pending-classification marker.
func (b *Bridge) clearLayoutPending(ino meta.Ino) {
	b.mu.Lock()
	delete(b.layoutPending, ino)
	b.mu.Unlock()
}

// setVerifyBase arms an identity-verified recovery's publication
// constraint: the next commit must find the same object and CAS against
// the verified revision.
func (b *Bridge) setVerifyBase(ino meta.Ino, rev int64, resID string) {
	b.mu.Lock()
	b.verifyBase[ino] = verifyBase{rev: rev, resID: resID}
	b.mu.Unlock()
}

// HasPending reports whether un-committed writes remain for the inode.
func (b *Bridge) HasPending(ino meta.Ino) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.pending[ino]) > 0
}

// EnableDeferredCommits arms interactive durability: writer flushes stage
// slices locally (reads overlay them) and the remote commit defers to the
// flush/close boundary or the next mount's replay of a staged barrier.
func (b *Bridge) EnableDeferredCommits() { b.deferred.Store(true) }

// MarkFreshCreate records that this inode is a descriptor WE created: its
// first publication must establish an attributable identity (completion
// echo, or the content-addressed classification proof) before writing any
// pathname occupant.
func (b *Bridge) MarkFreshCreate(ino meta.Ino) {
	b.mu.Lock()
	b.freshCreate[ino] = true
	b.mu.Unlock()
}

// waitStagedSlice blocks until the store has delivered all blocks of the
// slice (or the bounded wait expires). JuiceFS writeback stages blocks to
// its disk cache and signals the writer before the object PUT completes,
// so the meta side must not assume the bytes are present.
func (b *Bridge) waitStagedSlice(ctx context.Context, id uint64, size int64) bool {
	wait := b.cfg.WritebackWait
	if wait <= 0 {
		wait = 60 * time.Second
	}
	deadline := time.Now().Add(wait)
	for {
		b.mu.Lock()
		sl := b.stagedSlices[id]
		ready := sl != nil && sl.total == size
		b.mu.Unlock()
		if ready {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// needsFreshIdentity reports whether this inode is a locally-created
// descriptor whose identity was never established (no verifyBase, no
// bound landing identity, not classic-owned, no recorded placeholder).
func (b *Bridge) needsFreshIdentity(ino meta.Ino) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.freshCreate[ino] && b.verifyBase[ino].resID == "" &&
		b.layoutPendingID[ino] == "" &&
		!b.classicOwned[ino]
}

// ModeOf reports the mode recorded for the inode at creation (Mknod).
func (b *Bridge) ModeOf(ino meta.Ino) (uint32, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	m, ok := b.modes[ino]
	return m, ok
}

// forgetInoLocked drops one inode's cached state; the caller holds b.mu.
func (b *Bridge) forgetInoLocked(ino meta.Ino) {
	delete(b.layoutPendingID, ino)
	delete(b.layoutPending, ino)
	// freshCreate describes THIS incarnation's create. Inodes are derived
	// from pathnames, so a closed file's marker must not survive into a
	// later, unrelated file that reuses the same path: it would tell the
	// new descriptor's Sync that it created an occupant it never created.
	delete(b.freshCreate, ino)
	delete(b.verifyBase, ino)
	delete(b.commitErr, ino)
	delete(b.classicOwned, ino)
	delete(b.paths, ino)
	delete(b.sizes, ino)
	delete(b.revs, ino)
	delete(b.pending, ino)
	delete(b.truncs, ino)
	delete(b.modes, ino)
}

// TruncateLocal records a size change on a mount-local path (committed
// with the next flush).
func (b *Bridge) TruncateLocal(ctx context.Context, local string, size int64) error {
	ino, errno := b.ResolveLocal(ctx, local)
	if errno != 0 {
		return errno
	}
	b.mu.Lock()
	b.sizes[ino] = size
	if _, has := b.truncs[ino]; !has {
		b.truncs[ino] = size
	}
	b.mu.Unlock()
	return nil
}

// releaseCommitted drops the staged bytes of a successful commit — but
// only for checksums no OTHER pending commit still references (identical
// content staged for two files shares one registry entry; an eager
// release cleared the sibling's bytes and a fresh sibling silently
// uploaded zeros for them).
func (b *Bridge) releaseCommitted(ino meta.Ino, pending []pendingWrite) {
	// The reference snapshot, the overlay-resolution decision and the
	// retirement share ONE critical section with Bridge.Write's staging
	// (b.mu): a snapshot-then-unlock let a sibling stage the same checksum
	// between the two and lose its bytes to the stale snapshot, and a
	// separately-sampled "no pending writes" could clear an inode's
	// overlay flags while a write staged in between was still
	// un-published. Lock order is b.mu then registry.mu — Bridge.Write
	// stages under b.mu before touching the registry, so no inversion
	// exists.
	b.mu.Lock()
	defer b.mu.Unlock()
	// Whole-chunk overlay images from this inode become remotely
	// reproducible only when NOTHING survives un-published: the surgical
	// deletion preserved writes appended during the commit, and their
	// bytes exist only inside those images. Resolving by commit snapshot
	// (per checksum) left such images demotable with Y still pending.
	if len(b.pending[ino]) == 0 {
		defer b.blocks.resolveOverlaysFor(ino)
	}
	refs := make(map[string]int, len(pending))
	// The surgical deletion has already removed THIS inode's committed
	// snapshot, so its remaining list holds only writes that SURVIVED the
	// commit (appended while it was in flight) — they reference their
	// checksums exactly like another file's would.
	for _, list := range b.pending {
		for _, w := range list {
			refs[w.checksum]++
		}
	}
	for _, p := range pending {
		// EVERY completed reference unpins: skipping the decrement while
		// another file still referenced the checksum left a permanent pin
		// after the last reference completed, and pinned entries are never
		// retired by the classic-read LRU.
		b.blocks.unpin(p.checksum)
		if refs[p.checksum] > 0 {
			continue
		}
		b.blocks.release(p.checksum)
	}
}

// PathsOf reports the remote path currently mapped for a mount-local
// path (introspection/test helper).
func (b *Bridge) PathsOf(local string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.paths[inoFor(b.fullPath(local))]
}

// RemapLocal retargets cached mappings after a remote rename of a
// mount-local path (prefix-aware for directory moves). The incarnation
// renamed away from oldPath is whichever key currently OWNS that pathname —
// its registered owner if one exists, else the path hash — never an older
// inode that merely still maps through it after its own move. The moved
// inode is registered as the owner of its NEW name, so every later open of
// that name converges on the descriptor's inode (one JuiceFS identity per
// object), and its old name's hash is recorded as moved so a recreation
// there gets a synthetic key.
func (b *Bridge) RemapLocal(oldPath, newPath string) {
	oldFull := b.fullPath(oldPath)
	newFull := b.fullPath(newPath)
	if oldFull == newFull {
		return
	}
	// Durable retargets are collected HERE but applied after b.mu is
	// released: the manifest lock is always outermost (see
	// Bridge.manifestMu), and a staging publish takes it before reading any
	// in-memory mapping. Taking them in the other order is what deadlocked
	// the suite earlier.
	var retargets []stagedRetarget
	b.mu.Lock()
	defer func() {
		b.mu.Unlock()
		b.applyStagedRetargets(retargets)
	}()
	newIno := inoFor(newFull)
	if rep, ok := b.replacements[oldFull]; ok {
		// A registered owner (replacement incarnation, or a file renamed
		// before) holds oldPath: follow IT and carry its registration. The
		// hashed key belongs to an earlier, already-moved generation whose
		// open descriptor reads and commits through its own target —
		// retargeting it here would fuse two live incarnations onto one
		// object.
		b.paths[rep] = newFull
		b.moved[rep] = newFull
		b.replacements[newFull] = rep
		delete(b.replacements, oldFull)
		retargets = append(retargets, stagedRetarget{ino: rep, path: newFull})
	} else if ino := inoFor(oldFull); b.paths[ino] == oldFull {
		b.paths[ino] = newFull
		// Record the move so a recreated OLD pathname gets a synthetic key
		// instead of overwriting the live mapping, and register the moved
		// inode as the owner of its new name so every later open of that
		// name converges on the descriptor's inode (one JuiceFS identity
		// per object: a second key would split cache invalidation).
		b.moved[ino] = newFull
		b.replacements[newFull] = ino
		// The durable barrier record must follow the inode: a crash before
		// the next landing otherwise leaves the manifest naming a path that
		// no longer holds the object, stranding acknowledged bytes.
		retargets = append(retargets, stagedRetarget{ino: ino, path: newFull})
		// The destination's hash key may still be HELD by a live descriptor
		// that merely vacated this name, or is retained under a sentinel
		// alias while the name is replaced (its descriptor keeps using that
		// key): installing the mirror would hijack its mapping and length,
		// and its next commit would PATCH the newly arrived file. The owner
		// registration above already resolves the new name; only mirror the
		// natural key when no live descriptor holds it.
		if _, held := b.heldInoForLocked(newFull); !held {
			b.paths[newIno] = newFull
			b.sizes[newIno] = b.sizes[ino]
		}
	}
	// Descendants (a directory move arrives as ONE event for the
	// directory) follow with their bookkeeping: each child's key is
	// recorded as moved — a recreated old child pathname must get a
	// synthetic key, never the open descendant's inode — and a child
	// without a registered owner (its inode is the hash of its OLD path)
	// becomes the owner of its new name. Registered child owners are
	// re-keyed under the new subtree below. Every child's DURABLE barrier
	// record follows its new name too: a descendant holds its own inode, so
	// retargeting only the renamed directory's inode would leave an open
	// /d/a's manifest naming /d/a while the object now lives at /e/a, and a
	// crash before Flush would strand the fsync-acknowledged bytes.
	prefix := oldFull + "/"
	for i, p := range b.paths {
		switch {
		case p == oldFull:
			b.paths[i] = newFull
			b.moved[i] = newFull
			retargets = append(retargets, stagedRetarget{ino: i, path: newFull})
		case strings.HasPrefix(p, prefix):
			np := newFull + p[len(oldFull):]
			b.paths[i] = np
			b.moved[i] = np
			retargets = append(retargets, stagedRetarget{ino: i, path: np})
			if i == inoFor(p) {
				if _, owned := b.replacements[p]; !owned {
					b.replacements[np] = i
				}
			}
		}
	}
	var rekey []string
	for k := range b.replacements {
		if strings.HasPrefix(k, prefix) {
			rekey = append(rekey, k)
		}
	}
	for _, k := range rekey {
		rep := b.replacements[k]
		delete(b.replacements, k)
		b.replacements[newFull+k[len(oldFull):]] = rep
	}
}

// sentinelSeq makes every sentinel name unique per retention: seeded from
// the clock so names also differ across remounts, so a retention can never
// target a name that already holds a retained object (a leftover from an
// interrupted session, or a still-live one).
var sentinelSeq = uint64(time.Now().UnixNano())

// deletedOpenSentinel builds the hidden keep-alive name for a file with
// live engine handles that is losing its pathname (the silly-rename
// trick): same parent, dot-prefixed, stamped with the RETAINED
// INCARNATION's inode and a unique sequence — two simultaneously retained
// generations of one pathname, or a later retention of the same name,
// never collide (the server's rename replaces an existing target).
func deletedOpenSentinel(local string, live meta.Ino) string {
	parent, name := SplitParent(local)
	seq := atomic.AddUint64(&sentinelSeq, 1)
	return childPath(parent, "."+name+".d9open-"+fmt.Sprintf("%x-%x", uint64(live), seq))
}

// KeepDeletedOpen moves an unlinked-but-open extent file to its hidden
// sentinel name and retargets the live incarnation's mapping (the engine's
// VFS keeps using that ino). The sentinel is REGISTERED: only registered
// names are ever deleted by DropDeletedOpen — a user file that merely
// contains ".d9open-" in its name is untouchable. Each generation retains
// its OWN sentinel (name and registration keyed by its live inode — the
// replacement's synthetic key when one owns the path), so every retained
// incarnation stays independently readable until its own last close; the
// path hash stays marked held so a replacement created at the same path
// gets a synthetic key instead of colliding with a live descriptor.
func (b *Bridge) KeepDeletedOpen(local string) (string, error) {
	full := b.fullPath(local)
	// Resolve the live inode BEFORE RemapLocal carries a replacement
	// registration away from this pathname.
	b.mu.Lock()
	live := b.liveInoLocked(full)
	b.mu.Unlock()
	sentinel := deletedOpenSentinel(local, live)
	if _, err := b.cfg.Client.StatCtx(context.Background(), full); err != nil {
		if client.IsNotFound(err) {
			return "", nil // already gone remotely: nothing to keep
		}
		return "", err
	}
	if err := b.cfg.Client.RenameCtx(context.Background(), full, b.fullPath(sentinel)); err != nil {
		return "", err
	}
	b.RemapLocal(local, sentinel)
	b.mu.Lock()
	b.sentinels[sentinel] = live
	b.mu.Unlock()
	return sentinel, nil
}

// RetainOpenAlias retains an OPEN extent file that a rename is about to
// replace WITHOUT vacating its name: a hardlink is added under a fresh
// sentinel name (non-replacing — the server refuses an existing
// destination) and the live incarnation's mapping is pointed at the alias,
// so its descriptors read and commit through a name nothing else touches.
// The retained name itself stops resolving to the inode: a resolution of
// the name re-points the inode's mapping at it (remember), which would
// drag the retained descriptor back onto the replaceable name mid-rename.
// The alias is one extra reference in every history — dropping it at the
// retained file's last close is always correct (an extra link while the
// file still holds its name, its last reference once the name was
// replaced) — so no outcome ever needs a rollback, and nothing is ever
// put back, deleted or decided from an unreserved observation. "" when
// the file is already gone remotely.
func (b *Bridge) RetainOpenAlias(local string) (string, error) {
	full := b.fullPath(local)
	b.mu.Lock()
	live := b.liveInoLocked(full)
	b.mu.Unlock()
	sentinel := deletedOpenSentinel(local, live)
	if _, err := b.cfg.Client.StatCtx(context.Background(), full); err != nil {
		if client.IsNotFound(err) {
			return "", nil
		}
		return "", err
	}
	sentinelFull := b.fullPath(sentinel)
	if err := b.cfg.Client.HardlinkCtx(context.Background(), full, sentinelFull); err != nil {
		return "", err
	}
	b.mu.Lock()
	b.paths[live] = sentinelFull
	b.replacements[sentinelFull] = live
	if b.replacements[full] == live {
		delete(b.replacements, full)
	}
	b.sentinels[sentinel] = live
	b.mu.Unlock()
	return sentinel, nil
}

// DropDeletedOpen removes a REGISTERED sentinel after the last handle
// closes. Unregistered paths (anything this bridge did not create) are
// never touched. The held-inode marker clears with it.
func (b *Bridge) DropDeletedOpen(sentinel string) {
	b.mu.Lock()
	heldIno, ours := b.sentinels[sentinel]
	if !ours {
		b.mu.Unlock()
		return
	}
	delete(b.sentinels, sentinel)
	delete(b.replacements, b.fullPath(sentinel))
	delete(b.moved, heldIno)
	b.forgetInoLocked(heldIno)
	b.mu.Unlock()
	_ = b.cfg.Client.RemoveAllCtx(context.Background(), b.fullPath(sentinel))
}

// heldInoForLocked reports the held (moved or sentinel-bound) path-hash
// inode for a full path, if any; the caller holds b.mu. Mknod/ResolveLocal
// consult this so a replacement incarnation gets a synthetic key instead
// of colliding with the live descriptor.
func (b *Bridge) heldInoForLocked(full string) (meta.Ino, bool) {
	ino := inoFor(full)
	if _, moved := b.moved[ino]; moved {
		return ino, true
	}
	for _, held := range b.sentinels {
		if held == ino {
			return ino, true
		}
	}
	return 0, false
}

// liveInoLocked resolves the LIVE incarnation's inode for a full path; the
// caller holds b.mu. A replacement's synthetic key wins when one owns the
// pathname, else the path hash — so a replacement and the renamed-away or
// deleted-open original it displaced never alias through a lookup.
func (b *Bridge) liveInoLocked(full string) meta.Ino {
	if rep, ok := b.replacements[full]; ok {
		return rep
	}
	return inoFor(full)
}

// allocateHeldReplacementInoLocked hands out a synthetic inode key for a
// NEW incarnation at a path whose original key is held; the caller holds
// b.mu so the allocation and its publication are one atomic step.
func (b *Bridge) allocateHeldReplacementInoLocked() meta.Ino {
	b.nextHeldIno++
	return b.nextHeldIno
}

// FlushFile commits everything pending on one file: slices and any
// truncate-only growth. Empty fresh files (touch) materialize as classic
// empties — the extent protocol has no zero-size session.
func (b *Bridge) FlushFile(ctx context.Context, ino meta.Ino) error {
	return b.flushFile(ctx, ino, false)
}

// FlushFileFinal is FlushFile for the LAST close of a descriptor: it may
// additionally materialize a still-empty file's NAME. Intermediate flushes
// deliberately do not: an empty classic object cannot be attributed back to
// this descriptor later (the create returns no resource identity), so
// materializing it while the handle can still be written would leave the
// next write with an occupant it must neither delete nor publish into. A
// name created at the last close is never written through this descriptor
// again — a later open resolves it as an ordinary classic file.
func (b *Bridge) FlushFileFinal(ctx context.Context, ino meta.Ino) error {
	return b.flushFile(ctx, ino, true)
}

func (b *Bridge) flushFile(ctx context.Context, ino meta.Ino, finalize bool) error {
	// One commit lifecycle at a time per file: concurrent chunk commits
	// each snapshot the size independently, and a retry that only re-bases
	// its revision could then send an older, smaller size over a newly
	// committed tail. Serializing the snapshot->commit->cleanup sequence
	// makes every retry observe the previous commit's result.
	unlockCommit := b.lockCommit(ino)
	defer unlockCommit()
	if err := b.flushPendingLocked(ctx, ino); err != nil {
		return err
	}
	b.mu.Lock()
	size, has := b.sizes[ino]
	path, mapped := b.paths[ino]
	b.mu.Unlock()
	if finalize && has && mapped && size == 0 {
		if _, err := b.StatLocal(ctx, path); err != nil && client.IsNotFound(err) {
			if _, err := b.cfg.Client.CreateFileCtx(ctx, path); err != nil {
				return err
			}
			// No placeholder identity is recorded: CreateFileCtx returns a
			// revision and no resource id, and a follow-up Stat is a
			// separate request whose answer may describe another client's
			// replacement. Binding that answer would authorize deleting a
			// foreign object, so the identity stays unknown and a later
			// promotion commits through the classic surface instead.
			// Keep the inode mapping: the engine handle is still open, and
			// a later write through the same descriptor commits via this
			// path (the size-0 classic target is replaced with a fresh
			// extent create in commit). Forgetting the mapping turned that
			// write into EIO.
		}
	}
	if !has {
		return nil
	}
	if err := b.flushTruncateOnly(ctx, ino, size); err != nil {
		return err
	}
	// Everything landed: staged interactive barriers are redundant now.
	// (A remembered landing failure was cleared by the successful commit
	// itself — Bridge.Write's sticky memory exists only until the next
	// successful landing, which just happened.)
	//
	// An EMPTY file whose name was never materialized has nothing staged
	// for this inode to make redundant: clearing here deleted the barrier
	// an earlier interactive fsync wrote, leaving a durability-acknowledged
	// empty file with neither a remote name nor a local record. Keep it for
	// the replay (or the final close, which materializes the name and then
	// clears it once the name exists).
	if !finalize && size == 0 && !b.nameMaterialized(ctx, path) {
		return nil
	}
	b.clearStagedLocal(ino)
	return nil
}

// isFreshCreate reports whether this bridge created the inode locally (so its
// first publication must establish an attributable identity).
func (b *Bridge) isFreshCreate(ino meta.Ino) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.freshCreate[ino]
}

// nameMaterialized reports whether the file's name currently exists
// server-side (a fresh empty engine file may have none yet).
func (b *Bridge) nameMaterialized(ctx context.Context, path string) bool {
	_, err := b.StatLocal(ctx, path)
	return err == nil
}

// MaterializeEmpty gives an EMPTY fresh file a remote name, which is what a
// remotely-durable fsync boundary means for a file with no content: strict
// fsync must not report success while the name exists only locally (or, with
// no staging directory, nowhere at all).
//
// The name is created with an absence-CAS create, and its identity is
// captured immediately afterward as the publication binding (verifyBase):
// every later commit this descriptor makes CASes on that identity, so a
// replacement taking the name is detected and the write is abandoned instead
// of mutating a foreign object.
//
// Residual: the create and the recording Stat are separate requests, so a
// replacement landing between them would be bound as ours. Closing that
// window needs the server to echo the created object's resource identity —
// no client-side observation can attribute an anonymous create.
func (b *Bridge) MaterializeEmpty(ctx context.Context, ino meta.Ino) error {
	b.mu.Lock()
	size, has := b.sizes[ino]
	path, mapped := b.paths[ino]
	pending := len(b.pending[ino])
	b.mu.Unlock()
	if !has || !mapped || pending > 0 || size != 0 {
		return nil
	}
	// Already named remotely. What this means depends on whether THIS
	// descriptor created the file:
	//
	//   - an ordinary open of an existing empty file: nothing to materialize
	//     and nothing to attribute — the object predates this handle, and its
	//     later writes go through the normal commit path with their own
	//     revision CAS. Synchronizing it must simply succeed.
	//   - a descriptor THIS bridge created locally, whose remote create has
	//     not been attributed: the occupant may be another client's object
	//     (Mknod never touches the server, so two clients can race the same
	//     path). Binding it would make every later write publish against a
	//     stranger's object, so refuse rather than adopt.
	if b.nameMaterialized(ctx, path) {
		if b.publicationIdentity(ino) != "" || !b.isFreshCreate(ino) {
			return nil
		}
		return fmt.Errorf("materialize empty file %s: name occupied by an object this descriptor did not create", path)
	}
	rev, createdID, err := b.cfg.Client.CreateFileCreatedCtx(ctx, path)
	if err != nil {
		return fmt.Errorf("materialize empty file %s: %w", path, err)
	}
	if createdID != "" {
		// The server echoed the identity of the object IT created. Bind
		// that — never a follow-up stat, which a concurrent replacement
		// can turn into a stranger's identity.
		b.setVerifyBase(ino, rev, createdID)
		b.mu.Lock()
		b.classicOwned[ino] = true
		b.mu.Unlock()
		b.clearStagedLocal(ino)
		return nil
	}
	// The create landed but the server did not echo what it created, so this
	// descriptor still has no attributable identity. A follow-up Stat cannot
	// supply one — a replacement landing in that window would be adopted as
	// ours — so strict durability cannot be honoured here and the call must
	// fail rather than acknowledge an object we cannot prove we created.
	return fmt.Errorf("materialize empty file %s: server did not echo the created object's identity", path)
}

func (b *Bridge) CompactAll(ctx meta.Context, threads int, bar *utils.Bar) syscall.Errno { return 0 }
func (b *Bridge) Compact(ctx meta.Context, ino meta.Ino, concurrency int, pre, post func()) syscall.Errno {
	return 0
}
func (b *Bridge) CleanupSlices(ctx meta.Context) syscall.Errno { return 0 }

func (b *Bridge) Flock(ctx meta.Context, ino meta.Ino, owner uint64, ltype uint32, block bool) syscall.Errno {
	return syscall.ENOLCK
}
func (b *Bridge) Getlk(ctx meta.Context, ino meta.Ino, owner uint64, ltype *uint32, start, end *uint64, pid *uint32) syscall.Errno {
	return syscall.ENOLCK
}
func (b *Bridge) Setlk(ctx meta.Context, ino meta.Ino, owner uint64, block bool, ltype uint32, start, end uint64, pid uint32) syscall.Errno {
	return syscall.ENOLCK
}

func (b *Bridge) GetXattr(ctx meta.Context, ino meta.Ino, name string, vbuff *[]byte) syscall.Errno {
	return meta.ENOATTR
}
func (b *Bridge) ListXattr(ctx meta.Context, ino meta.Ino, dbuff *[]byte) syscall.Errno { return 0 }
func (b *Bridge) SetXattr(ctx meta.Context, ino meta.Ino, name string, value []byte, flags uint32) syscall.Errno {
	return syscall.ENOTSUP
}
func (b *Bridge) RemoveXattr(ctx meta.Context, ino meta.Ino, name string) syscall.Errno {
	return syscall.ENOTSUP
}

func (b *Bridge) Symlink(ctx meta.Context, parent meta.Ino, name string, path string, inode *meta.Ino, attr *meta.Attr) syscall.Errno {
	return syscall.ENOTSUP
}
func (b *Bridge) ReadLink(ctx meta.Context, ino meta.Ino, path *[]byte) syscall.Errno {
	return syscall.ENOTSUP
}
func (b *Bridge) Link(ctx meta.Context, inodeSrc, parent meta.Ino, name string, attr *meta.Attr) syscall.Errno {
	return syscall.ENOTSUP
}

func (b *Bridge) Access(ctx meta.Context, ino meta.Ino, modemask uint8, attr *meta.Attr) syscall.Errno {
	return 0
}

func (b *Bridge) StatFS(ctx meta.Context, ino meta.Ino, totalspace, availspace, iused, iavail *uint64) syscall.Errno {
	if totalspace != nil {
		*totalspace = 1 << 40
	}
	if availspace != nil {
		*availspace = 1 << 39
	}
	if iused != nil {
		*iused = 1 << 16
	}
	if iavail != nil {
		*iavail = 1 << 30
	}
	return 0
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// NativeWritebackForTest reports the bridge's writeback mode (test hook).
func (b *Bridge) NativeWritebackForTest() bool { return b.cfg.NativeWriteback }
