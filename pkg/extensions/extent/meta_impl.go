package extent

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"
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

func (b *Bridge) OnReload(cb func(*meta.Format) error) {}

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
	plan, err := b.cfg.Client.GetExtentReadPlan(ctxOf(ctx), path, start, chunkEnd)
	if err != nil {
		if client.IsNotFound(err) {
			*slices = nil
			return 0
		}
		return errnoOf(err)
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
	if plan.Size > b.sizes[ino] {
		b.sizes[ino] = plan.Size
	}
	b.mu.Unlock()
	// Trim trailing zero blocks.
	n := len(blocks)
	for n > 0 && blocks[n-1].Zero {
		n--
	}
	out := make([]meta.Slice, 0, n)
	for _, blk := range blocks[:n] {
		if blk.Zero {
			// The engine's native hole slice: readSlice zero-fills Id 0.
			out = append(out, meta.Slice{Id: 0, Len: uint32(blk.Length)})
			continue
		}
		id := b.blocks.register(blk.BlockKey, blk.URL, blk.BlockOffset+blk.Length, path, blk.FileOffset)
		// JuiceFS slice semantics: Size is the backing object's size, Off
		// the start within it. Shadow-merged plans reference one object
		// with block_offset > 0, so Size must cover Off+Len for the store's
		// key derivation and reads to line up.
		out = append(out, meta.Slice{Id: id, Size: uint32(blk.BlockOffset + blk.Length), Off: uint32(blk.BlockOffset), Len: uint32(blk.Length)})
	}
	*slices = out
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
	// (assembleImage / blockBytes refetch).
	b.blocks.stageBytes(sum, data)
	b.pending[ino] = append(b.pending[ino], pendingWrite{chunk: indx, off: off, checksum: sum, size: int64(slice.Len)})
	end := int64(indx)*meta.ChunkSize + int64(off) + int64(slice.Len)
	if end > b.sizes[ino] {
		b.sizes[ino] = end
	}
	pending := append([]pendingWrite(nil), b.pending[ino]...)
	size := b.sizes[ino]
	b.mu.Unlock()

	if err := b.commit(ctxOf(ctx), ino, pending, size); err != nil {
		// Put the slices back so a later flush retries them.
		b.mu.Lock()
		b.pending[ino] = append(pending, b.pending[ino]...)
		b.stagedSlices[slice.Id] = sl
		b.mu.Unlock()
		return syscall.EIO
	}
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
	if !fresh && st.ContentLayout != client.ContentLayoutExtent {
		if st.Size == 0 {
			// An empty file materialized as classic (empty extent files
			// cannot exist server-side yet); replace it and treat this as
			// the fresh create it semantically is.
			if err := b.cfg.Client.RemoveAllCtx(ctx, path); err != nil {
				return err
			}
			fresh = true
		} else {
			return fmt.Errorf("target %s is not extent layout", path)
		}
	}

	if fresh {
		// Sparse: holes are synthesized on demand — a small write at a
		// huge offset must not materialize the whole logical image.
		ra := &sparseImageAt{bridge: b, ctx: ctx, path: path, size: size, writes: pending}
		if err := b.cfg.Client.WriteExtentStreamConditional(ctx, path, ra, size, nil, -1); err != nil {
			return err
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
		if err := b.commitWithRetry(ctx, path, blocks, size, st.Revision); err != nil {
			return err
		}
	}

	b.mu.Lock()
	mode := b.modes[ino]
	delete(b.pending, ino)
	delete(b.truncs, ino)
	delete(b.modes, ino)
	b.mu.Unlock()
	b.releaseCommitted(ino, pending)
	if fresh && mode != 0 && mode != 0o644 {
		// The sequential extent create carries no mode; apply the one the
		// creator asked for.
		_ = b.cfg.Client.ChmodCtx(ctx, path, uint32(mode))
	}
	// Refresh the revision so the next commit CASes against reality.
	if nst, err := b.cfg.Client.StatCtx(ctx, path); err == nil {
		b.mu.Lock()
		b.revs[ino] = nst.Revision
		b.mu.Unlock()
	}
	return nil
}

func (b *Bridge) commitWithRetry(ctx context.Context, path string, blocks []client.ExtentBlockWrite, size, rev int64) error {
	err := b.cfg.Client.CommitExtentPatch(ctx, path, size, blocks, rev)
	if err == nil {
		return nil
	}
	// Revision conflict: re-base and retry once — blocks are
	// content-addressed, nothing is re-uploaded.
	st, serr := b.cfg.Client.StatCtx(ctx, path)
	if serr != nil {
		return err
	}
	return b.cfg.Client.CommitExtentPatch(ctx, path, size, blocks, st.Revision)
}

// flushPending commits a file's pending slices (used on Close and fsync
// paths).
func (b *Bridge) flushPending(ctx context.Context, ino meta.Ino) error {
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
	if err := b.commitWithRetry(ctx, path, blocks, size, st.Revision); err != nil {
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

// forgetInoLocked drops one inode's cached state; the caller holds b.mu.
func (b *Bridge) forgetInoLocked(ino meta.Ino) {
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
	// The reference snapshot and the retirement must share ONE critical
	// section with Bridge.Write's staging (b.mu): a snapshot-then-unlock
	// let a sibling stage the same checksum between the two and lose its
	// bytes to the stale snapshot. Lock order is b.mu then registry.mu —
	// Bridge.Write stages under b.mu before touching the registry, so no
	// inversion exists.
	b.mu.Lock()
	defer b.mu.Unlock()
	refs := make(map[string]int, len(pending))
	for ino2, list := range b.pending {
		if ino2 == ino {
			continue
		}
		for _, w := range list {
			refs[w.checksum]++
		}
	}
	for _, p := range pending {
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
		b.paths[newIno] = newFull
		// The engine descriptor keeps USING the original inode: mirror the
		// length under both keys. Deleting the original made a later 4 KiB
		// overwrite PATCH new_size=4096 and truncate the untouched tail.
		b.sizes[newIno] = b.sizes[ino]
		b.moved[ino] = newFull
		b.replacements[newFull] = ino
	}
	// Descendants (a directory move arrives as ONE event for the
	// directory) follow with their bookkeeping: each child's key is
	// recorded as moved — a recreated old child pathname must get a
	// synthetic key, never the open descendant's inode — and a child
	// without a registered owner (its inode is the hash of its OLD path)
	// becomes the owner of its new name. Registered child owners are
	// re-keyed under the new subtree below.
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

// deletedOpenSentinel builds the hidden keep-alive name for an unlinked
// path with live engine handles (the silly-rename trick): same parent,
// dot-prefixed, stamped with the RETAINED INCARNATION's inode — not the
// pathname's hash — so two simultaneously open, unlinked generations of
// one pathname land on distinct objects (the server's rename replaces an
// existing target; a shared name would overwrite the first retained file
// while it is still open).
func deletedOpenSentinel(local string, live meta.Ino) string {
	parent, name := SplitParent(local)
	return childPath(parent, "."+name+".d9open-"+fmt.Sprintf("%x", uint64(live)))
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
	if err := b.flushPending(ctx, ino); err != nil {
		return err
	}
	b.mu.Lock()
	size, has := b.sizes[ino]
	path, mapped := b.paths[ino]
	b.mu.Unlock()
	if has && mapped && size == 0 {
		if _, err := b.StatLocal(ctx, path); err != nil && client.IsNotFound(err) {
			if _, err := b.cfg.Client.CreateFileCtx(ctx, path); err != nil {
				return err
			}
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
	return b.flushTruncateOnly(ctx, ino, size)
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
