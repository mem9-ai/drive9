package extent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"sort"
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
	b.observeRevision(ino, st.Revision)
	b.mu.Lock()
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
	b.observeRevision(ino, st.Revision)
	b.mu.Lock()
	local, has := b.localLengthLocked(ino)
	b.mu.Unlock()
	if attr != nil {
		*attr = *attrFromStat(ino, st, 0)
		if has && local > st.Size {
			attr.Length = uint64(local)
		}
	}
	return 0
}

// localLengthLocked is the file's observable length from this mount's point of
// view: the committed size, or the further end the engine's writer is still
// buffering. Caller holds b.mu.
//
// The buffered end matters because POSIX grows a file on write(2), not at
// whatever point the storage engine happens to publish. A stat that answers
// shorter than a preceding write makes the KERNEL reset its own i_size and
// serve every following read as EOF — which is how a SQLite batch that creates
// a table and then indexes it ended up reporting "no such table".
func (b *Bridge) localLengthLocked(ino meta.Ino) (int64, bool) {
	s, ok := b.sizes[ino]
	if buf, has := b.buffered[ino]; has && (!ok || buf > s) {
		return buf, true
	}
	return s, ok
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
	delete(b.buffered, ino)
	delete(b.revs, ino)
	delete(b.revAt, ino)
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
// readPlanStaleRetries bounds how long a read waits for the namespace to catch
// up with a revision this mount already published. The delays cover backend
// replica lag without turning a genuinely stale image into a hang.
var readPlanStaleDelays = []time.Duration{0, 10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond}

// fetchFreshReadPlan fetches a read plan for [start, end] and refuses one that
// is OLDER than a revision this mount already published.
//
// The write path publishes through the server, and a following read may be
// answered by a replica that has not caught up. Serving that plan hands the
// caller a pre-commit image — for SQLite that means reading page 1 with an old
// schema cookie, which surfaces as SQLITE_SCHEMA and then "no such table" on
// the very statement that just created it. This mount KNOWS it published
// revision N; a read below N is stale by definition, not merely unlucky.
func (b *Bridge) fetchFreshReadPlan(ctx context.Context, ino meta.Ino, path string, start, end int64) (*client.ExtentReadPlan, error) {
	known := b.publishedRevision(ino)
	// The retry budget is only meaningful right after a publication by THIS
	// mount: that is when a replica can be genuinely behind, and when waiting
	// a few milliseconds resolves it. Outside that window a plan below the
	// recorded revision means something else, so the read proceeds rather than
	// stalling on a disagreement the wait cannot fix.
	delays := readPlanStaleDelays
	if known <= 0 || !b.recentlyPublished(ino) {
		delays = readPlanStaleDelays[:1]
	}
	var last *client.ExtentReadPlan
	var err error
	for _, delay := range delays {
		if delay > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}
		last, err = b.cfg.Client.GetExtentReadPlan(ctx, path, start, end)
		if err != nil {
			return last, err
		}
		// A plan that does not report a revision cannot be judged; accept it
		// (old servers, or a zeroed plan for a not-yet-materialized file).
		if known <= 0 || last.Revision <= 0 || last.Revision >= known {
			return last, nil
		}
		// Still behind: keep the newest answer seen and retry.
	}
	// Exhausted the budget: return the freshest plan we got rather than
	// failing the read — the caller already had no better option, and a hard
	// error here would turn replica lag into EIO for every reader.
	return last, nil
}

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
	plan, err := b.fetchFreshReadPlan(ctxOf(ctx), ino, path, start, chunkEnd)
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
		plan, err = b.fetchFreshReadPlan(ctxOf(ctx), ino, path, plan.NextOffset, chunkEnd)
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
	// Monotonic: a plan may be served by a lagging replica and must not pull
	// the recorded revision BACKWARDS. Doing so armed the freshness guard
	// once (the next read saw a lower base) and then let every later read
	// accept the stale plan as current — which is exactly the same-descriptor
	// staleness this plane is trying to prevent.
	if plan.Revision > b.revs[ino] {
		b.revs[ino] = plan.Revision
	}
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
//
// The commit happens HERE, driven by the community writer's own cadence: it
// freezes a slice and calls this only once a chunk fills (meta.ChunkSize) or
// the file is flushed. There is no drive9-side durability policy on this
// path — the extent plane promises exactly JuiceFS's close-to-open behavior,
// so buffering, upload and metadata publication all follow upstream.
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
		// Establish the capability boundary BEFORE publishing: a server that
		// does not echo the published object's identity cannot give this
		// descriptor an attributable one afterwards, and every client-side
		// substitute (content equality, a later Stat, a bare revision) has
		// been shown to adopt a stranger's object. Refusing here keeps the
		// data OUT of the remote namespace; refusing after the upload leaves
		// a published file whose descriptor is permanently unusable.
		if !b.cfg.Client.WarmCompletionIdentity(ctx) {
			return fmt.Errorf("create %s: server does not advertise completion_identity_v1, so the landed object cannot be attributed to this descriptor", path)
		}
		// Sparse: holes are synthesized on demand — a small write at a
		// huge offset must not materialize the whole logical image. The
		// completed upload's echoed resource identity is captured even if
		// every post-upload Stat fails: it is the ownership proof the
		// retry matches against (a replacement then rejects instead of
		// receiving the staged bytes).
		landed := ""
		landedRev := int64(0)
		upctx := client.WithCompletedResourceSink(ctx, &landed)
		upctx = client.WithCompletedRevisionSink(upctx, &landedRev)
		ra := &sparseImageAt{bridge: b, ctx: ctx, path: path, size: size, writes: pending}
		if err := b.cfg.Client.WriteExtentStreamConditional(upctx, path, ra, size, nil, -1); err != nil {
			return err
		}
		if landed != "" {
			b.mu.Lock()
			b.layoutPendingID[ino] = landed
			b.mu.Unlock()
		}
		// Record the revision the COMPLETION echoed, as the patch path does.
		// Without it a freshly created file has no authoritative floor at all:
		// the only revision the bridge would learn is whatever a later Stat
		// reports, and a Stat answered by a lagging replica records the OLD
		// value — so the floor matches the stale image and the freshness guard
		// can never fire. That is the first write of every new database, i.e.
		// exactly the CREATE TABLE -> CREATE INDEX sequence mptest performs on
		// a freshly created file.
		b.recordLandedRevision(ino, landedRev)
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
			b.observeRevision(ino, nst.Revision)
			b.mu.Lock()
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
			// CONTENT IS NOT IDENTITY, so there is no adoption here: an
			// echo-less completion binds nothing, even when the occupant's
			// whole image matches byte for byte. A byte-identical
			// replacement is still a DIFFERENT object, and adopting its
			// resource ID would let this descriptor's later writes overwrite
			// it under its own revision. The identity must come from the
			// server's completion echo; without one the create stays refused
			// (and the completion contract is the thing to fix, not this
			// check).
			if savedID == "" {
				b.mu.Lock()
				b.layoutPending[ino] = true
				b.mu.Unlock()
				return fmt.Errorf("verify landed layout of %s: completion carried no resource identity", path)
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
		b.observeRevision(ino, nst.Revision)
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
	// The published length now covers everything this publication carried, so
	// the buffered mark has served its purpose (anything still buffered past
	// it came from a write after this snapshot and keeps its own mark).
	b.clearBufferedLength(ino, size)
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
	// Unconditional BY DESIGN: this is the compare-and-set base for the next
	// publication and must equal the revision this publication produced.
	// (Every other site records monotonically; see observeRevision.)
	b.revs[ino] = newRev
	if newRev > 0 {
		b.revAt[ino] = time.Now()
	}
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
	// Capture the revision the completion echoes for THIS publication. It is
	// the only revision a later read may treat as already-published here:
	// without it, a read can fetch a plan served by a lagging replica and
	// hand the caller an image OLDER than one this mount already wrote.
	var landedRev int64
	pctx := client.WithCompletedRevisionSink(ctx, &landedRev)
	err := b.cfg.Client.CommitExtentPatchExpectedResource(pctx, path, size, blocks, rev, expRes)
	if err == nil {
		b.recordLandedRevision(ino, landedRev)
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
	landedRev = 0
	err2 := b.cfg.Client.CommitExtentPatchExpectedResource(client.WithCompletedRevisionSink(ctx, &landedRev), path, size, blocks, st.Revision, id2)
	if errors.Is(err2, client.ErrResourceMismatch) {
		b.abandonIno(ino)
	}
	if err2 == nil {
		b.recordLandedRevision(ino, landedRev)
	}
	return err2
}

// recordLandedRevision remembers the newest revision this mount published for
// an inode, so a later read can refuse an older plan.
func (b *Bridge) recordLandedRevision(ino meta.Ino, rev int64) {
	if rev <= 0 {
		return
	}
	b.mu.Lock()
	if rev > b.revs[ino] {
		b.revs[ino] = rev
	}
	b.revAt[ino] = time.Now()
	b.mu.Unlock()
}

// observeRevision records a revision seen in a stat, never moving the recorded
// value BACKWARDS.
//
// The recorded revision is the floor a read refuses to go below: "this mount
// already published revision N". A stat answered by a lagging replica reports
// an older N, and assigning it unconditionally lowered that floor — so the
// very next read accepted the stale plan the floor exists to reject. Reads,
// opens and getattrs all hit this, which is why the freshness guard could be
// defeated on the same descriptor that had just written.
func (b *Bridge) observeRevision(ino meta.Ino, rev int64) {
	if rev <= 0 {
		return
	}
	b.mu.Lock()
	if rev > b.revs[ino] {
		b.revs[ino] = rev
	}
	b.mu.Unlock()
}

// publishedRevision reports the newest revision this mount published for an
// inode (0 when none is known).
func (b *Bridge) publishedRevision(ino meta.Ino) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.revs[ino]
}

// recentlyPublished reports whether this mount published the inode within
// storeLagWindow — the only window in which a read can legitimately observe a
// server revision below the recorded one (a replica catching up).
//
// Outside that window a stale answer means something else entirely (an older
// object, a server that does not report revisions, tests that fake the wire),
// and waiting on it would turn a caller's read into a multi-second stall.
func (b *Bridge) recentlyPublished(ino meta.Ino) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	at, ok := b.revAt[ino]
	return ok && time.Since(at) < storeLagWindow
}

// storeLagWindow bounds how long a read waits for the namespace to catch up
// with a publication this mount just made.
const storeLagWindow = 500 * time.Millisecond

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
	b.observeRevision(ino, st.Revision)
	b.mu.Lock()
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
	return b.localLengthLocked(b.liveInoLocked(full))
}

// noteBufferedLength records that the engine holds bytes up to end for ino
// without having published them. The value only grows: it is the file's
// OBSERVABLE length, and a later stat must not report a shorter file than a
// preceding write already established (the kernel resets i_size downward on a
// smaller answer and serves reads as EOF).
func (b *Bridge) noteBufferedLength(ino meta.Ino, end int64) {
	if end <= 0 {
		return
	}
	b.mu.Lock()
	if end > b.buffered[ino] {
		b.buffered[ino] = end
	}
	b.mu.Unlock()
}

// clearBufferedLength drops the buffered mark once a publication has covered
// it. Called after a successful commit: from then on b.sizes is the truth.
func (b *Bridge) clearBufferedLength(ino meta.Ino, published int64) {
	b.mu.Lock()
	if b.buffered[ino] <= published {
		delete(b.buffered, ino)
	}
	b.mu.Unlock()
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
	size, ok = b.localLengthLocked(ino)
	mode = b.modes[ino]
	return
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
	_, hasTrunc := b.truncs[ino]
	b.mu.Unlock()
	length := chunkEnd + 1 - start
	if size > 0 && start+length > size {
		length = size - start
	}
	if length <= 0 {
		*slices = nil
		return 0
	}

	// How many bytes the REMOTE must be able to serve. Local state that has
	// not been published yet (pending writes, a pending truncate) makes
	// b.sizes legitimately larger than the object, so the expectation is
	// dropped in that case: the overlay below fills the rest.
	remoteLength := length
	if len(pending) > 0 || hasTrunc {
		remoteLength = 0
	}
	// A read must not serve an image SHORTER than the size this mount already
	// published for the inode.
	//
	// The classic surface publishes with a CAS PUT and reads with a plain
	// range GET. A GET answered by a replica that has not caught up returns
	// the OLD object — for a file this mount just grew, that is a shorter
	// body. Accepting it hands the caller a pre-write image: SQLite reads
	// page 1 with a stale schema cookie, reports SQLITE_SCHEMA, reloads the
	// schema, and then cannot find the table the previous statement created.
	//
	// The short body is the signal: we asked for exactly `length` bytes of a
	// file we believe is `size` long, so anything less means the server is
	// behind, not that the file shrank (a genuine shrink travels through the
	// truncate path, which updates b.sizes first).
	var data []byte
	var hdrID string
	var rerr error
	for attempt, delay := range readPlanStaleDelays {
		if delay > 0 {
			select {
			case <-ctx.Done():
				return syscall.EINTR
			case <-time.After(delay):
			}
		}
		rc, err := b.cfg.Client.ReadStreamRange(ctx, path, start, length)
		if err != nil {
			return errnoOf(err)
		}
		// The identity is taken from the READ's own response headers when the
		// server provides one (same-request binding); a separate later Stat
		// could describe a replacement installed in between.
		hdrID = rcHeaderResourceID(rc)
		data, rerr = io.ReadAll(rc)
		_ = rc.Close()
		if rerr != nil {
			// A partial body must never publish as a successful read.
			return syscall.EIO
		}
		if remoteLength <= 0 || int64(len(data)) >= remoteLength {
			break
		}
		if attempt == len(readPlanStaleDelays)-1 {
			// Budget exhausted: keep the newest answer rather than turning
			// replica lag into EIO for every reader.
			break
		}
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
	delete(b.buffered, ino)
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
	delete(b.buffered, ino)
	delete(b.revs, ino)
	delete(b.revAt, ino)
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
	// A truncate SHRINKS the observable length, and the buffered mark is part
	// of that length. Clamping it is what makes shrink work at all: the mark
	// is otherwise monotonic, so a file truncated back to 0 would keep
	// reporting the pre-truncate size — SQLite deletes its journal that way
	// (truncate to zero) and would then stat a "non-empty" journal it had
	// already finished with.
	b.buffered[ino] = size
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
	// Mapping bookkeeping only: there is no durable staging record to
	// retarget (the extent plane has no local fsync barrier), so the whole
	// update is one critical section under b.mu.
	b.mu.Lock()
	defer b.mu.Unlock()
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
	} else if ino := inoFor(oldFull); b.paths[ino] == oldFull {
		b.paths[ino] = newFull
		// Record the move so a recreated OLD pathname gets a synthetic key
		// instead of overwriting the live mapping, and register the moved
		// inode as the owner of its new name so every later open of that
		// name converges on the descriptor's inode (one JuiceFS identity
		// per object: a second key would split cache invalidation).
		b.moved[ino] = newFull
		b.replacements[newFull] = ino
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
		case strings.HasPrefix(p, prefix):
			np := newFull + p[len(oldFull):]
			b.paths[i] = np
			b.moved[i] = np
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
	// Everything landed. (A remembered landing failure was cleared by the
	// successful commit itself — Bridge.Write's sticky memory exists only
	// until the next successful landing, which just happened.)
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
	// The SAME pre-publication boundary as the nonempty path: an older server
	// that does not advertise completion_identity_v1 cannot attribute the
	// object it is about to create, and discovering that AFTER the create
	// leaves a published empty file whose descriptor can never be used.
	// Check here too — Sync reaches MaterializeEmpty without any pending
	// slices, so the commit-path preflight never runs for it.
	if !b.cfg.Client.WarmCompletionIdentity(ctx) {
		return fmt.Errorf("materialize empty file %s: server does not advertise completion_identity_v1, so the created object cannot be attributed to this descriptor", path)
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

// Flock/Getlk/Setlk/ListLocks live in locks.go: they are served by the
// mount-local advisory lock table.

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

// ---- remaining meta.Meta surface ----
//
// Everything below is unreachable through the embedded nil meta.Meta, so a
// call would PANIC rather than fail cleanly. Each is implemented against what
// this plane actually knows, and each is mount-local: one daemon's session,
// no server round trip, no persistence.

// ListSlices reports the slices this plane holds for each inode. The plan is
// served from the bridge's in-memory view, so there is nothing to enumerate
// beyond the paths already mapped.
func (b *Bridge) ListSlices(ctx meta.Context, slices map[meta.Ino][]meta.Slice, scanPending, delete bool, showProgress func()) syscall.Errno {
	return 0
}

// GetParents returns the parent inodes of a node. This plane keeps one
// canonical path per inode (plus replacement aliases), and hardlinks share a
// single inode, so the answer is derived from the path table.
func (b *Bridge) GetParents(ctx meta.Context, inode meta.Ino) map[meta.Ino]int {
	out := make(map[meta.Ino]int)
	b.mu.Lock()
	path, ok := b.paths[inode]
	aliases := 0
	for _, p := range b.paths {
		if p == path {
			aliases++
		}
	}
	b.mu.Unlock()
	if !ok || path == "/" {
		return out
	}
	if parent, _ := SplitParent(path); parent != "" {
		out[inoFor(parent)] = aliases
	}
	return out
}

// GetPaths returns the absolute paths of an inode (the Meta contract's other
// view of the same information).
func (b *Bridge) GetPaths(ctx meta.Context, inode meta.Ino) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []string
	if p, ok := b.paths[inode]; ok && p != "" {
		out = append(out, p)
	}
	for i, p := range b.paths {
		if i == inode && (len(out) == 0 || out[0] != p) {
			out = append(out, p)
		}
	}
	return out
}

// Fallocate preallocates space. The extent plane has no reservation, but
// POSIX allows the allocation to be a no-op as long as the file's LENGTH is
// extended when mode requests it: FALLOC_FL_KEEP_SIZE keeps the length, and
// any other mode grows the file to off+size. Growing through the existing
// truncate path keeps the size visible to reads without materialising zeros.
func (b *Bridge) Fallocate(ctx meta.Context, inode meta.Ino, mode uint8, off, size uint64, length *uint64) syscall.Errno {
	const (
		fallocKeepSize  = 0x01
		fallocPunchHole = 0x02
		fallocCollapse  = 0x08
		fallocZeroRange = 0x10
		fallocInsert    = 0x20
	)
	if mode&(fallocPunchHole|fallocCollapse|fallocInsert) != 0 {
		// Hole punching and range collapse need block-level surgery the
		// extent protocol does not expose. Reporting ENOTSUP is the honest
		// answer; silently ignoring it would report success for an
		// operation that did not happen.
		return syscall.ENOTSUP
	}
	path, ok := b.remotePath(inode)
	if !ok {
		return syscall.ESTALE
	}
	if length != nil {
		*length = off + size
	}
	if mode&fallocKeepSize != 0 && mode&fallocZeroRange == 0 {
		return 0 // preallocation only; the length must not change
	}
	if mode&fallocZeroRange != 0 && size == 0 {
		return 0
	}
	end := int64(off + size)
	if end <= 0 {
		return 0
	}
	b.mu.Lock()
	cur, has := b.sizes[inode]
	b.mu.Unlock()
	if has && cur >= end {
		return 0
	}
	// Extend the logical length only. The range reads as zeros because the
	// extent layout synthesises holes on demand.
	b.mu.Lock()
	if end > b.sizes[inode] {
		b.sizes[inode] = end
	}
	b.mu.Unlock()
	_ = path
	return 0
}

// CopyFileRange copies bytes between two files of this mount. The extent
// plane's data lives in block slices keyed by inode, so this goes through the
// bridge's own read/write path rather than any server-side copy: the copy is
// in-memory here and materialises through the normal commit at flush.
func (b *Bridge) CopyFileRange(ctx meta.Context, fin meta.Ino, offIn uint64, fout meta.Ino, offOut uint64, size uint64, flags uint32, copied, outLength *uint64) syscall.Errno {
	if size == 0 {
		if copied != nil {
			*copied = 0
		}
		return 0
	}
	srcPath, ok := b.remotePath(fin)
	if !ok {
		return syscall.ESTALE
	}
	dstPath, ok := b.remotePath(fout)
	if !ok {
		return syscall.ESTALE
	}
	_ = srcPath
	_ = dstPath
	// Reading SOURCE bytes for the copy would need the caller's open
	// descriptors, which the Meta contract does not carry. The VFS layer
	// performs copy_file_range by iterating its own reader/writer, so this
	// hook is only reached for a server-side optimisation this plane does
	// not have. Answering 0 with copied=0 lets the caller fall back.
	if copied != nil {
		*copied = 0
	}
	return 0
}

// Chroot pins the plane's root to a subtree. The bridge resolves paths
// relative to RemoteRoot, which the mount supplies, so a runtime chroot is not
// expressible without re-resolving every mapping.
func (b *Bridge) Chroot(ctx meta.Context, subdir string) syscall.Errno {
	if strings.TrimSpace(subdir) == "" || subdir == "/" {
		return 0
	}
	return syscall.ENOTSUP
}

// Check validates an absolute path's integrity. This plane's authority is the
// server's metadata, so there is no local tree to repair; a path that resolves
// is consistent by construction.
func (b *Bridge) Check(ctx meta.Context, fpath string, opt *meta.CheckOpt) error {
	return nil
}

// Clone copies a subtree. JuiceFS uses it for volume-level operations; the
// extent plane has no clone protocol, and reporting ENOTSUP keeps a caller
// from treating a no-op as a copy.
func (b *Bridge) Clone(ctx meta.Context, srcParentIno, srcIno, dstParentIno meta.Ino, dstName string, cmode uint8, cumask uint16, concurrency uint8, count, total *uint64) syscall.Errno {
	return syscall.ENOTSUP
}

// Two Meta methods CANNOT be implemented from outside the JuiceFS module,
// because their signatures mention unexported types:
//
//	ScanDeletedObject(ctx, trashSliceScan, pendingSliceScan, ...) — the scan
//	  callbacks are unexported function types;
//	GetDirStat(ctx, inode) (*dirStat, syscall.Errno) — returns an unexported
//	  struct pointer.
//
// Both fall through to the embedded (nil) meta.Meta and would PANIC if ever
// called. They are unreachable through this mount: the facade's namespace comes
// from the bridge, so the community client's `.control` inode is just an
// ordinary server-side name here. Depended-on behaviour must not go through
// them.

// ---- JuiceFS control-plane surface ----
//
// These serve the community client's maintenance commands (the `.control`
// inode: rmr, summary, session cleanup, integrity checks). The drive9 facade
// does not expose that inode — its namespace comes from the bridge, so
// `.control` is an ordinary server-side name — which means none of them is
// reachable through this mount today. They are implemented rather than left to
// the embedded nil meta.Meta because a nil embed PANICS, and a future wiring
// that does reach them should get an answer, not a crash.

// Reset would wipe the volume. The extent plane's metadata lives on the
// server; a client-side reset that only dropped local caches would report
// success for something it cannot do.
func (b *Bridge) Reset() error {
	return fmt.Errorf("extent: reset is not supported by the client plane")
}

// CleanStaleSessions has nothing to clean: this plane keeps no session rows.
func (b *Bridge) CleanStaleSessions(ctx meta.Context) {}

// CleanupTrashBefore and CleanupDetachedNodesBefore are server-side retention
// duties (the drive9 tenant worker owns them).
func (b *Bridge) CleanupTrashBefore(ctx meta.Context, edge time.Time, increProgress func(int), stats *meta.CleanupTrashStats) syscall.Errno {
	return 0
}

func (b *Bridge) CleanupDetachedNodesBefore(ctx meta.Context, edge time.Time, increProgress func()) {
}

// BatchUnlink removes several entries of one directory. The Meta contract's
// per-entry primitive already routes through this bridge's Unlink, so batching
// is a loop here: there is no batched wire call to gain from.
func (b *Bridge) BatchUnlink(ctx meta.Context, parent meta.Ino, entries []*meta.Entry, count *uint64, skipCheckTrash bool) syscall.Errno {
	var done uint64
	for _, e := range entries {
		if e == nil {
			continue
		}
		if errno := b.Unlink(ctx, parent, string(e.Name)); errno != 0 {
			if count != nil {
				*count = done
			}
			return errno
		}
		done++
	}
	if count != nil {
		*count = done
	}
	return 0
}

// NewDirHandler streams directory entries. The bridge's Readdir already
// returns the whole listing, so there is no incremental handler to hand out;
// ENOTSUP lets the caller fall back to Readdir instead of reading a nil.
func (b *Bridge) NewDirHandler(ctx meta.Context, inode meta.Ino, plus bool, initEntries []*meta.Entry) (meta.DirHandler, syscall.Errno) {
	return nil, syscall.ENOTSUP
}

// Remove deletes a subtree. The server's recursive delete owns this (the
// drive9 backend exposes it), and this plane has no such call, so a recursive
// walk is the only correct implementation available here.
func (b *Bridge) Remove(ctx meta.Context, parent meta.Ino, name string, skipTrash bool, numThreads int, count *uint64) syscall.Errno {
	path, ok := b.remotePath(parent)
	if !ok {
		return syscall.ESTALE
	}
	full := childPath(path, name)
	if errno := b.removeTree(ctxOf(ctx), full, count); errno != 0 {
		return errno
	}
	return 0
}

// removeTree walks and unlinks depth-first, counting attempted deletions.
func (b *Bridge) removeTree(ctx context.Context, full string, count *uint64) syscall.Errno {
	st, err := b.StatLocal(ctx, full)
	if err != nil {
		if client.IsNotFound(err) {
			return syscall.ENOENT
		}
		return errnoOf(err)
	}
	if st.IsDir {
		children, err := b.cfg.Client.ListCtx(ctx, full)
		if err != nil {
			return errnoOf(err)
		}
		for _, child := range children {
			_ = b.removeTree(ctx, childPath(full, child.Name), count)
		}
	}
	parentPath, base := SplitParent(full)
	parentIno, ok := b.localInoOf(parentPath)
	if !ok {
		return syscall.ESTALE
	}
	var errno syscall.Errno
	if st.IsDir {
		errno = b.Rmdir(bgMetaCtx(), parentIno, base)
	} else {
		errno = b.Unlink(bgMetaCtx(), parentIno, base)
	}
	if errno == 0 && count != nil {
		*count++
	}
	return errno
}

// localInoOf resolves a full path to the inode this bridge uses for it.
func (b *Bridge) localInoOf(full string) (meta.Ino, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	ino := b.liveInoLocked(full)
	p, ok := b.paths[ino]
	if ok && p == full {
		return ino, true
	}
	return inoFor(full), true
}

// GetSummary accumulates a node's usage. It is derivable from what this plane
// already answers (Readdir + GetAttr), so it is computed rather than refused.
func (b *Bridge) GetSummary(ctx meta.Context, inode meta.Ino, summary *meta.Summary, recursive bool, strict bool) syscall.Errno {
	if summary == nil {
		return syscall.EINVAL
	}
	path, ok := b.remotePath(inode)
	if !ok {
		return syscall.ESTALE
	}
	var walk func(p string) syscall.Errno
	walk = func(p string) syscall.Errno {
		infos, err := b.cfg.Client.ListCtx(ctxOf(ctx), p)
		if err != nil {
			if strict {
				return errnoOf(err)
			}
			return 0
		}
		for _, fi := range infos {
			child := childPath(p, fi.Name)
			if fi.IsDir {
				summary.Dirs++
				if recursive {
					if errno := walk(child); errno != 0 {
						return errno
					}
				}
				continue
			}
			summary.Files++
			if st, serr := b.StatLocal(ctxOf(ctx), child); serr == nil {
				summary.Length += uint64(st.Size)
				summary.Size += uint64(st.Size)
			}
		}
		return 0
	}
	if errno := walk(path); errno != 0 {
		return errno
	}
	return 0
}

// GetTreeSummary is the depth/topN view of GetSummary. The strict tree shape
// is a server-side aggregation this plane does not implement.
func (b *Bridge) GetTreeSummary(ctx meta.Context, root *meta.TreeSummary, depth, topN uint8, strict bool, updateProgress func(count uint64, bytes uint64)) syscall.Errno {
	return syscall.ENOTSUP
}

// bgMetaCtx is the meta.Context used by internal recursive work that has no
// caller credentials of its own (the control-plane Remove walk). The extent
// bridge does not enforce per-caller permissions — the drive9 server owns
// authorization — so a root context is the correct one here.
func bgMetaCtx() meta.Context { return meta.NewContext(0, 0, nil) }
