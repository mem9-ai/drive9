package fuse

import (
	"context"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/pkg/client"
)

const (
	fallocKeepSize  = 0x01
	fallocPunchHole = 0x02
	fallocCollapse  = 0x08
	fallocZeroRange = 0x10
	fallocInsert    = 0x20
	fallocUnshare   = 0x40
)

// Fallocate implements JuiceFS-style fallocate for extent files: punch-hole
// and zero-range become hole slices. Grow without those flags only raises
// inode length (JuiceFS doFallocate), and does not append hole slices.
func (fs *Dat9FS) Fallocate(cancel <-chan struct{}, input *gofuse.FallocateIn) (status gofuse.Status) {
	if fs.opts != nil && fs.opts.ReadOnly {
		return gofuse.EROFS
	}
	fh, ok := fs.fileHandles.Get(input.Fh)
	if !ok {
		return gofuse.ENOENT
	}
	ctx, cf := fuseCtx(cancel)
	defer cf()
	fh.Lock()
	defer fh.Unlock()
	if !fh.isExtent() {
		return gofuse.ENOSYS
	}
	if input.Mode&(fallocCollapse|fallocInsert|fallocUnshare) != 0 {
		return gofuse.Status(syscall.ENOTSUP)
	}
	// JuiceFS VFS.Fallocate: Flush then Meta.Fallocate.
	if fh.extentWriter != nil && fh.extentWriter.bound() {
		if _, st := fs.flushExtentHandle(ctx, fh); st != gofuse.OK {
			return st
		}
	}
	if fh.Dirty == nil {
		fh.Dirty = fs.newWriteBuffer(fh.Path, 0, 0)
	}
	off := int64(input.Offset)
	length := int64(input.Length)
	if off < 0 || length <= 0 {
		return gofuse.Status(syscall.EINVAL)
	}
	end := off + length
	size := fh.Dirty.Size()
	if fh.extentOpen != nil && fh.extentOpen.size > size {
		size = fh.extentOpen.size
	}
	keepSize := input.Mode&fallocKeepSize != 0
	punchHole := input.Mode&fallocPunchHole != 0
	zeroRange := input.Mode&fallocZeroRange != 0
	if punchHole && !keepSize {
		return gofuse.Status(syscall.EINVAL)
	}
	var truncateTo *int64
	if zeroRange && !keepSize && end > size {
		if err := fh.Dirty.SetSizeOnly(end); err != nil {
			return gofuse.Status(syscall.EFBIG)
		}
		if fh.extentOpen != nil {
			fh.extentOpen.setSize(end)
		}
		if fs.inodes != nil {
			fs.inodes.UpdateSize(fh.Ino, end)
		}
		size = end
		to := end
		truncateTo = &to
	}
	if punchHole || zeroRange {
		if off >= size {
			return gofuse.OK
		}
		if end > size {
			end = size
			length = end - off
		}
		if fh.extentWriter != nil {
			fh.extentWriter.punch(off, end)
		}
		if fh.extentDirty != nil {
			fh.extentDirty.punch(off, end)
		}
		fh.DirtySeq = fs.markDirtySize(fh.Ino, fh.Dirty.Size())
		ops := holeOpsForRange(off, length)
		if len(ops) == 0 && truncateTo == nil {
			return gofuse.OK
		}
		return fs.commitExtentHoleOpsLocked(ctx, fh, ops, truncateTo)
	}
	if end > size && input.Mode&fallocKeepSize == 0 {
		if err := fh.Dirty.SetSizeOnly(end); err != nil {
			return gofuse.Status(syscall.EFBIG)
		}
		if fh.extentOpen != nil {
			fh.extentOpen.setSize(end)
		}
		fh.DirtySeq = fs.markDirtySize(fh.Ino, end)
		fs.inodes.UpdateSize(fh.Ino, end)
		return fs.commitExtentGrowLengthLocked(ctx, fh, end)
	}
	return gofuse.OK
}

func holeOpsForRange(off, length int64) []client.SliceOp {
	var out []client.SliceOp
	end := off + length
	for off < end {
		chunkEnd := (off &^ (client.ExtentChunkSize - 1)) + client.ExtentChunkSize
		next := end
		if chunkEnd < next {
			next = chunkEnd
		}
		out = append(out, client.SliceOp{FileOff: off, Len: next - off, Kind: "hole"})
		off = next
	}
	return out
}

func (fs *Dat9FS) commitExtentGrowLengthLocked(ctx context.Context, fh *FileHandle, newSize int64) gofuse.Status {
	if fh == nil || fs == nil || fs.client == nil {
		return gofuse.OK
	}
	remotePath := fs.remotePath(fh.Path)
	baseRev := fh.BaseRev
	gen := fh.SliceGeneration
	parkedCommit := fh.RemoteCommitUnlock
	fh.RemoteCommitUnlock = nil
	fh.Unlock()
	if parkedCommit != nil {
		parkedCommit()
	}
	result, err := fs.client.GrowExtentLength(ctx, remotePath, client.NewExtentOpID(), baseRev, gen, newSize)
	fh.Lock()
	if parkedCommit != nil {
		fh.RemoteCommitUnlock = fs.lockWritableRemoteCommitPath(fh.Path)
	}
	if err != nil {
		return httpToFuseStatus(err)
	}
	fh.BaseRev = result.Revision
	fh.SliceGeneration = result.Generation
	fh.OrigSize = result.SizeBytes
	fh.extentNeedTruncate = false
	if fs.extentCache != nil {
		fs.extentCache.invalidatePath(remotePath)
	}
	if fh.extentOpen != nil {
		fh.extentOpen.setSize(newSize)
		fh.extentOpen.noteCommit(result.Revision, result.Generation, result.SizeBytes)
	}
	fs.refreshExtentHandlesAfterCommit(fh.Path, fh, result.Revision, result.Generation, result.SizeBytes)
	fs.inodes.UpdateSize(fh.Ino, newSize)
	return gofuse.OK
}

func (fs *Dat9FS) commitExtentHoleOpsLocked(ctx context.Context, fh *FileHandle, ops []client.SliceOp, truncateTo *int64) gofuse.Status {
	if len(ops) == 0 && truncateTo == nil {
		return gofuse.OK
	}
	remotePath := fs.remotePath(fh.Path)
	baseRev := fh.BaseRev
	gen := fh.SliceGeneration
	localSize := extentHandleLogicalSize(fh)
	zeroFrom := extentOpenZeroFrom(fh)
	sentHoles := len(ops) > 0
	parkedCommit := fh.RemoteCommitUnlock
	fh.RemoteCommitUnlock = nil
	fh.Unlock()
	if parkedCommit != nil {
		parkedCommit()
	}
	result, err := fs.client.CommitSlicesStaged(ctx, remotePath, client.NewExtentOpID(), baseRev, gen, ops, truncateTo, true, true)
	fh.Lock()
	if parkedCommit != nil {
		fh.RemoteCommitUnlock = fs.lockWritableRemoteCommitPath(fh.Path)
	}
	if err != nil {
		return httpToFuseStatus(err)
	}
	fh.BaseRev = result.Revision
	fh.SliceGeneration = result.Generation
	fh.OrigSize = result.SizeBytes
	logical := extentHandleLogicalSize(fh)
	if !(fh.extentNeedTruncate && logical != result.SizeBytes) {
		fh.extentNeedTruncate = false
	}
	if fs.extentCache != nil {
		fs.extentCache.invalidatePath(remotePath)
	}
	if fh.extentOpen != nil {
		if logical < result.SizeBytes {
			fh.extentOpen.clipSize(logical)
		} else {
			fh.extentOpen.setSize(logical)
		}
		fh.extentOpen.noteCommit(result.Revision, result.Generation, result.SizeBytes)
		if truncateTo != nil {
			fs.applyExtentZeroFromAfterCommit(fh, zeroFrom, localSize, result.SizeBytes, sentHoles)
		}
	}
	fs.refreshExtentHandlesAfterCommit(fh.Path, fh, result.Revision, result.Generation, result.SizeBytes)
	visible := logical
	if !fh.extentNeedTruncate && result.SizeBytes > visible {
		visible = result.SizeBytes
	}
	fs.inodes.UpdateSize(fh.Ino, visible)
	return gofuse.OK
}

// CopyFileRange clones extent slices by sharing block keys (JuiceFS
// copy_file_range). Dirty handles are flushed first. Mixed-layout copies
// fall back to a userspace read-then-write of the requested range.
func (fs *Dat9FS) CopyFileRange(cancel <-chan struct{}, input *gofuse.CopyFileRangeIn) (written uint32, status gofuse.Status) {
	if fs.opts != nil && fs.opts.ReadOnly {
		return 0, gofuse.EROFS
	}
	src, ok := fs.fileHandles.Get(input.FhIn)
	if !ok {
		return 0, gofuse.ENOENT
	}
	dst, ok := fs.fileHandles.Get(input.FhOut)
	if !ok {
		return 0, gofuse.ENOENT
	}
	if !src.isExtent() || !dst.isExtent() {
		return fs.copyFileRangeUserspace(cancel, input, src, dst)
	}
	if input.Flags != 0 {
		return 0, gofuse.Status(syscall.EINVAL)
	}
	ctx, cf := fuseCtx(cancel)
	defer cf()
	srcOff := int64(input.OffIn)
	dstOff := int64(input.OffOut)
	length := int64(input.Len)
	if src.Ino == dst.Ino && length > 0 &&
		((srcOff <= dstOff && dstOff < srcOff+length) || (dstOff <= srcOff && srcOff < dstOff+length)) {
		return 0, gofuse.Status(syscall.EINVAL)
	}

	// JuiceFS copy_file_range always writer.Flush(src) and Flush(dst)
	// before cloning slice keys, even when the handle looks clean.
	src.Lock()
	srcWriter := src.extentWriter
	srcPath := fs.remotePath(src.Path)
	src.Unlock()
	if srcWriter != nil && srcWriter.bound() {
		if st := srcWriter.flush(ctx); st != gofuse.OK {
			return 0, st
		}
	}

	dst.Lock()
	dstWriter := dst.extentWriter
	dstPath := fs.remotePath(dst.Path)
	rev := dst.BaseRev
	gen := dst.SliceGeneration
	dst.Unlock()
	if dstWriter != nil && dstWriter.bound() {
		if st := dstWriter.flush(ctx); st != gofuse.OK {
			return 0, st
		}
	}

	if length <= 0 {
		return 0, gofuse.OK
	}
	result, err := fs.client.CopyFileRange(ctx, srcPath, dstPath, int64(input.OffIn), dstOff, length, rev, gen)
	if err != nil {
		return 0, httpToFuseStatus(err)
	}
	dst.Lock()
	dst.BaseRev = result.Revision
	dst.SliceGeneration = result.Generation
	dst.OrigSize = result.SizeBytes
	if dst.Dirty != nil {
		_ = dst.Dirty.SetSizeOnly(result.SizeBytes)
		if dst.Dirty.Size() > result.SizeBytes {
			_ = dst.Dirty.Truncate(result.SizeBytes)
		}
	}
	if dst.extentOpen != nil {
		dst.extentOpen.invalidateSlices()
		dst.extentOpen.clipSize(result.SizeBytes)
	}
	if fs.extentCache != nil {
		fs.extentCache.invalidatePath(dstPath)
	}
	fs.inodes.UpdateSize(dst.Ino, result.SizeBytes)
	fs.cacheFileForPath(dst.Path, result.SizeBytes, time.Now(), result.Revision)
	dst.Unlock()
	if length > int64(^uint32(0)) {
		length = int64(^uint32(0))
	}
	return uint32(length), gofuse.OK
}

func (fs *Dat9FS) copyFileRangeUserspace(cancel <-chan struct{}, input *gofuse.CopyFileRangeIn, src, dst *FileHandle) (written uint32, status gofuse.Status) {
	if input.Flags != 0 {
		return 0, gofuse.Status(syscall.EINVAL)
	}
	srcOff := int64(input.OffIn)
	dstOff := int64(input.OffOut)
	length := int64(input.Len)
	if length <= 0 {
		return 0, gofuse.OK
	}
	if src.Ino == dst.Ino &&
		((srcOff <= dstOff && dstOff < srcOff+length) || (dstOff <= srcOff && srcOff < dstOff+length)) {
		return 0, gofuse.Status(syscall.EINVAL)
	}
	ctx, cf := fuseCtx(cancel)
	defer cf()

	buf, err := fs.readHandleRange(ctx, src, srcOff, length)
	if err != nil {
		return 0, httpToFuseStatus(err)
	}
	n := len(buf)
	if n <= 0 {
		return 0, gofuse.OK
	}

	dst.Lock()
	defer dst.Unlock()
	if dst.Dirty == nil {
		dst.Dirty = fs.newWriteBuffer(dst.Path, 0, 0)
	}
	if dst.isExtent() {
		if dst.extentWriter == nil {
			fs.attachExtentWriter(dst)
		}
		if dst.extentDirty == nil {
			dst.extentDirty = &extentDirtySet{}
		}
		dst.extentWriter.writeAt(dstOff, buf)
		if err := dst.Dirty.SetSizeOnly(dstOff + int64(n)); err != nil {
			return 0, gofuse.Status(syscall.EFBIG)
		}
		dst.extentDirty.add(dstOff, dstOff+int64(n))
		if dst.extentOpen != nil {
			dst.extentOpen.setSize(dstOff + int64(n))
		}
	} else {
		wn, err := dst.Dirty.Write(dstOff, buf)
		if err != nil {
			return 0, gofuse.Status(syscall.EFBIG)
		}
		n = int(wn)
	}
	if fs.dirtyInodes != nil {
		dst.DirtySeq = fs.markDirtySize(dst.Ino, dst.Dirty.Size())
	}
	if fs.inodes != nil {
		fs.inodes.UpdateSize(dst.Ino, dst.Dirty.Size())
	}
	return uint32(n), gofuse.OK
}

// readHandleRange reads [offset, offset+size) the same way Dat9FS.Read
// serves a handle: extent files go through readExtentRange; dirty buffers
// EnsureLoaded then ReadAt; O_RDONLY and unloaded lazy parts fall through
// to ReadStreamRange. Caller must not hold fh.mu.
func (fs *Dat9FS) readHandleRange(ctx context.Context, fh *FileHandle, offset, size int64) ([]byte, error) {
	if fh == nil || size <= 0 {
		return nil, nil
	}
	if fh.isExtent() {
		return fs.readExtentRange(ctx, fh, offset, size)
	}

	fh.Lock()
	if data, handled, err := fs.readDirtyRangeLocked(fh, offset, size); handled || err != nil {
		fh.Unlock()
		return data, err
	}
	path := fh.Path
	fh.Unlock()

	if fs.client == nil {
		return nil, syscall.EIO
	}
	release, err := fs.acquireRemoteReadSlot(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	data, n, err := fs.readStreamRangeWithRetry(ctx, path, fh, offset, size)
	if err != nil {
		return nil, err
	}
	if n < 0 {
		n = 0
	}
	if n > len(data) {
		n = len(data)
	}
	return data[:n], nil
}

func (fs *Dat9FS) readDirtyRangeLocked(fh *FileHandle, offset, size int64) ([]byte, bool, error) {
	if fh == nil || fh.Dirty == nil {
		return nil, false, nil
	}
	wb := fh.Dirty
	fileSize := wb.Size()
	if offset >= fileSize {
		return nil, true, nil
	}
	end := offset + size
	if end > fileSize {
		end = fileSize
	}
	if end <= offset {
		return nil, true, nil
	}

	if evicted := wb.StreamedPartIndices(); len(evicted) > 0 {
		ps := wb.PartSize()
		firstPart := int(offset / ps)
		lastPart := int((end - 1) / ps)
		for p := firstPart; p <= lastPart; p++ {
			if evicted[p] && !wb.IsPartLoaded(p) {
				if wb.remoteSize == 0 {
					return nil, true, syscall.EIO
				}
				return nil, false, nil
			}
		}
	}

	ps := wb.PartSize()
	firstPart := int(offset / ps)
	lastPart := int((end - 1) / ps)
	if wb.HasDirtyParts() {
		for p := firstPart; p <= lastPart; p++ {
			if !wb.IsPartLoaded(p) {
				if err := wb.EnsureLoaded(p); err != nil {
					return nil, true, err
				}
			}
		}
		out := make([]byte, end-offset)
		n := wb.ReadAt(offset, out)
		return out[:n], true, nil
	}
	if fileSize > 0 {
		fullyLoaded := true
		for p := firstPart; p <= lastPart; p++ {
			if !wb.IsPartLoaded(p) {
				fullyLoaded = false
				break
			}
		}
		if fullyLoaded {
			out := make([]byte, end-offset)
			n := wb.ReadAt(offset, out)
			return out[:n], true, nil
		}
	}
	return nil, false, nil
}
