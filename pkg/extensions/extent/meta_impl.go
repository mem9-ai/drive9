package extent

import (
	"context"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/mem9-ai/drive9/pkg/client"
)

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
		a.Mtime = st.Mtime.UnixMilli()
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
	st, err := b.cfg.Client.StatCtx(ctxOf(ctx), path)
	if err != nil {
		// Fresh files exist only locally until their first commit.
		if client.IsNotFound(err) {
			b.mu.Lock()
			_, fresh := b.sizes[inoFor(path)]
			b.mu.Unlock()
			if fresh {
				if inode != nil {
					*inode = inoFor(path)
				}
				if attr != nil {
					b.mu.Lock()
					size := b.sizes[inoFor(path)]
					b.mu.Unlock()
					*attr = meta.Attr{Typ: meta.TypeFile, Mode: 0o644, Nlink: 1, Length: uint64(max64(size, 0))}
				}
				return 0
			}
		}
		return errnoOf(err)
	}
	ino := inoFor(path)
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
	b.remember(ino, full)
	b.mu.Lock()
	if _, has := b.sizes[ino]; !has {
		b.sizes[ino] = 0
	}
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
	plan, err := b.cfg.Client.GetExtentReadPlan(ctxOf(ctx), path, start, start+meta.ChunkSize-1)
	if err != nil {
		if client.IsNotFound(err) {
			*slices = nil
			return 0
		}
		return errnoOf(err)
	}
	b.mu.Lock()
	b.revs[ino] = plan.Revision
	if plan.Size > b.sizes[ino] {
		b.sizes[ino] = plan.Size
	}
	b.mu.Unlock()
	// Trim trailing zero blocks.
	n := len(plan.Blocks)
	for n > 0 && plan.Blocks[n-1].Zero {
		n--
	}
	out := make([]meta.Slice, 0, n)
	for _, blk := range plan.Blocks[:n] {
		if blk.Zero {
			// The engine's native hole slice: readSlice zero-fills Id 0.
			out = append(out, meta.Slice{Id: 0, Len: uint32(blk.Length)})
			continue
		}
		id := b.blocks.register(blk.BlockKey, blk.URL, blk.BlockOffset+blk.Length, path)
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
		return fmt.Errorf("target %s is not extent layout", path)
	}

	if fresh {
		image, err := b.assembleImage(ctx, ino, pending, size, path)
		if err != nil {
			return err
		}
		if err := b.cfg.Client.WriteExtentStreamConditional(ctx, path, bytesAt(image), size, nil, -1); err != nil {
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
	delete(b.pending, ino)
	delete(b.truncs, ino)
	b.mu.Unlock()
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
// slices were written: PATCH needs at least one dirty block. Growth pads a
// zero block at the old end (content addressing dedupes it); shrink
// re-declares the head block verbatim — the server already holds its
// content (dedupe skips the upload) and the commit carries the truncation.
// Truncate-to-zero is a known server-side protocol gap (new_size must be
// positive) and fails explicitly.
func (b *Bridge) flushTruncateOnly(ctx context.Context, ino meta.Ino, size int64) error {
	path, ok := b.remotePath(ino)
	if !ok {
		return nil
	}
	b.mu.Lock()
	_, tracked := b.truncs[ino]
	delete(b.truncs, ino)
	b.mu.Unlock()
	if !tracked {
		return nil
	}
	st, err := b.cfg.Client.StatCtx(ctx, path)
	if err != nil {
		if client.IsNotFound(err) {
			return nil
		}
		return err
	}
	if st.Size == size || st.ContentLayout != client.ContentLayoutExtent {
		return nil
	}
	var block client.ExtentBlockWrite
	switch {
	case size <= 0:
		return fmt.Errorf("truncate-to-zero on extent files needs a server protocol change (new_size must be positive): %s", path)
	case size > st.Size:
		n := int64(4096)
		if rem := size - st.Size; rem < n {
			n = rem
		}
		chunk, off, _ := client.ChunkOffset(st.Size)
		block = client.ExtentBlockWrite{Chunk: chunk, Off: off, Data: make([]byte, n)}
	default:
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
			block = client.ExtentBlockWrite{Chunk: chunk, Off: off, Data: make([]byte, n)}
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
			block = client.ExtentBlockWrite{Chunk: chunk, Off: off, Data: data}
		}
	}
	return b.commitWithRetry(ctx, path, []client.ExtentBlockWrite{block}, size, st.Revision)
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

// assembleImage materializes a fresh file's full image from its pending
// slices (holes zero-filled). Fresh files can only be created through a
// sequential extent session.
func (b *Bridge) assembleImage(ctx context.Context, ino meta.Ino, pending []pendingWrite, size int64, path string) ([]byte, error) {
	image := make([]byte, size)
	sorted := append([]pendingWrite(nil), pending...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].chunk != sorted[j].chunk {
			return sorted[i].chunk < sorted[j].chunk
		}
		return sorted[i].off < sorted[j].off
	})
	for _, p := range sorted {
		data, err := b.blockBytes(ctx, p, path)
		if err != nil {
			return nil, err
		}
		start := int64(p.chunk)*meta.ChunkSize + int64(p.off)
		copy(image[start:], data)
	}
	return image, nil
}

// ResolveLocal resolves a mount-local path against RemoteRoot, registering
// the path↔inode mapping. Fresh (created but not yet committed) files
// resolve through the local table.
func (b *Bridge) ResolveLocal(ctx context.Context, local string) (meta.Ino, syscall.Errno) {
	full := b.fullPath(local)
	ino := inoFor(full)
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

// LocalSizeOf reports the bridge's local size view of a mount-local path.
func (b *Bridge) LocalSizeOf(local string) (int64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.sizes[inoFor(b.fullPath(local))]
	return s, ok
}

// FlushFile commits everything pending on one file: slices and any
// truncate-only growth.
func (b *Bridge) FlushFile(ctx context.Context, ino meta.Ino) error {
	if err := b.flushPending(ctx, ino); err != nil {
		return err
	}
	b.mu.Lock()
	size, has := b.sizes[ino]
	b.mu.Unlock()
	if !has {
		return nil
	}
	return b.flushTruncateOnly(ctx, ino, size)
}

// StatLocal stats a mount-local path against RemoteRoot.
func (b *Bridge) StatLocal(ctx context.Context, local string) (*client.StatResult, error) {
	return b.cfg.Client.StatCtx(ctx, b.fullPath(local))
}

// ForgetPath drops every cached mapping for a mount-local path (the name
// changed or vanished underneath the engine).
func (b *Bridge) ForgetPath(local string) {
	full := b.fullPath(local)
	ino := inoFor(full)
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.paths, ino)
	delete(b.sizes, ino)
	delete(b.revs, ino)
	delete(b.pending, ino)
	delete(b.truncs, ino)
	for i, p := range b.paths {
		if strings.HasPrefix(p, full+"/") {
			delete(b.paths, i)
		}
	}
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

// ---- deliberately unsupported / server-owned ----

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
