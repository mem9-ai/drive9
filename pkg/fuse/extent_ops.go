package fuse

import (
	"context"
	"strings"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extent"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

func (fs *Dat9FS) extentStatIno(ctx context.Context, p string) (uint64, bool) {
	if fs.client == nil {
		return 0, false
	}
	stat, err := fs.client.StatCtx(ctx, fs.remotePath(p))
	if err != nil || stat == nil {
		return 0, false
	}
	if stat.ContentLayout == client.ContentLayoutExtent && stat.ExtentIno != 0 {
		return stat.ExtentIno, true
	}
	return 0, false
}

func (fs *Dat9FS) isExtentFile(ctx context.Context, p string) bool {
	_, ok := fs.existingExtentIno(ctx, p)
	return ok
}

// existingExtentIno reports a live extent file from stat ContentLayout, not
// the create-time glob. Directories are never extent objects.
func (fs *Dat9FS) existingExtentIno(ctx context.Context, p string) (uint64, bool) {
	if p == "" || strings.HasSuffix(p, "/") || fs.pathIsDir(p) {
		return 0, false
	}
	return fs.extentStatIno(ctx, p)
}

func (fs *Dat9FS) pathIsDir(p string) bool {
	if strings.HasSuffix(p, "/") {
		return true
	}
	if fs.inodes == nil {
		return false
	}
	ino, ok := fs.inodes.GetInode(p)
	if !ok {
		return false
	}
	entry, ok := fs.inodes.GetEntry(ino)
	return ok && entry.IsDir
}

func (fs *Dat9FS) extentVFSFh(fuseFh uint64) uint64 {
	if fuseFh == 0 {
		return 0
	}
	fh, ok := fs.fileHandles.Get(fuseFh)
	if !ok || fh == nil {
		return 0
	}
	return fh.extentFh
}

func (fs *Dat9FS) cachedExtentIno(nodeID uint64) (uint64, bool) {
	if fs.openHandles != nil {
		for _, fh := range fs.openHandles.SnapshotInode(nodeID) {
			if fh != nil && fh.extentIno != 0 {
				return uint64(fh.extentIno), true
			}
		}
	}
	if fs.inodes != nil {
		if entry, ok := fs.inodes.GetEntry(nodeID); ok && !entry.IsDir && entry.ExtentIno != 0 {
			return entry.ExtentIno, true
		}
	}
	return 0, false
}

// extentApplyWriterLength is JuiceFS replyAttr's UpdateLength: in-memory
// writer length wins over the attr we already have. No meta RPC.
//
// JuiceFS only calls UpdateLength with Attr from GetAttr/Readdir/Lookup.
// Passing Length=0 when the writer is already closed truncates the JuiceFS
// data reader to 0 (VFS.UpdateLength always reader.Truncate's to the result),
// so a later FUSE read of a flushed 8MiB file returns EOF. Skip the
// synthetic-zero case; overlay/GetAttr pass a real juicefs Attr.
func (fs *Dat9FS) extentApplyWriterLength(entry *InodeEntry) {
	if fs == nil || entry == nil || entry.IsDir || entry.ExtentIno == 0 {
		return
	}
	if entry.Size <= 0 {
		return
	}
	v := fs.extentVFS()
	if v == nil {
		return
	}
	attr := &jfsmeta.Attr{Full: true, Typ: jfsmeta.TypeFile, Length: uint64(entry.Size)}
	v.UpdateLength(jfsmeta.Ino(entry.ExtentIno), attr)
	entry.Size = int64(attr.Length)
	if fs.inodes != nil {
		fs.inodes.UpdateSize(entry.Ino, entry.Size)
	}
}

func (fs *Dat9FS) applyJuiceFSAttr(entry *InodeEntry, attr *jfsmeta.Attr) {
	if fs == nil || entry == nil || attr == nil {
		return
	}
	entry.Size = int64(attr.Length)
	entry.Uid = attr.Uid
	entry.Gid = attr.Gid
	entry.HasUID = true
	entry.HasGID = true
	entry.Rdev = attr.Rdev
	entry.Mode = juiceTypeToStatMode(attr.Typ, attr.Mode)
	entry.HasMode = true
	// Do not clobber Dat9FS nlink with JuiceFS GetAttr: after hardlink the
	// VFS attr cache can still say 1 while AddAlias already stored 2
	// (pjdfstest link/00). Remaining-link nlink after rename-replace is
	// finishLocalRename + InodeNotify, not this path.
	if entry.Nlink == 0 && attr.Nlink > 0 {
		entry.Nlink = attr.Nlink
		entry.Unlinked = false
		if fs.inodes != nil && entry.Ino != 0 {
			fs.inodes.UpdateLinkCount(entry.Ino, attr.Nlink)
		}
	}
	if attr.Mtime != 0 {
		entry.Mtime = time.Unix(attr.Mtime, int64(attr.Mtimensec))
	}
	if attr.Atime != 0 {
		entry.Atime = time.Unix(attr.Atime, int64(attr.Atimensec))
	}
	if attr.Ctime != 0 {
		entry.Ctime = time.Unix(attr.Ctime, int64(attr.Ctimensec))
	}
	if fs.inodes != nil && entry.Ino != 0 {
		fs.inodes.UpdateSize(entry.Ino, entry.Size)
		fs.inodes.UpdateOwner(entry.Ino, attr.Uid, attr.Gid, true, true)
		fs.inodes.UpdateMode(entry.Ino, entry.Mode)
		fs.inodes.UpdateRdev(entry.Ino, attr.Rdev)
		if attr.Mtime != 0 {
			fs.inodes.UpdateMtime(entry.Ino, entry.Mtime)
		}
		if attr.Atime != 0 {
			fs.inodes.UpdateAtime(entry.Ino, entry.Atime)
		}
		if attr.Ctime != 0 {
			fs.inodes.UpdateCtime(entry.Ino, entry.Ctime)
		}
	}
}

// extentRefreshFromVFS copies JuiceFS GetAttr (length, uid/gid, mode, nlink)
// onto the Dat9FS inode. file_nodes size is often 0 after a remount.
// Directories stay on the Dat9FS mkdir path: juicefs parents are created
// lazily as 0755 and must not overwrite sticky/owner from FUSE Mkdir.
func (fs *Dat9FS) extentRefreshFromVFS(nodeID uint64, p string, fuseFh uint64) bool {
	if fs == nil {
		return false
	}
	if entry, ok := fs.inodes.GetEntry(nodeID); ok && entry.IsDir {
		return false
	}
	inoNum, ok := fs.cachedExtentIno(nodeID)
	if !ok {
		if err := fs.ensureExtentRuntime(); err != nil {
			return false
		}
		if ino, found := fs.extentLookupChild(fs.jfsCtx(0, 0, 0), p); found {
			inoNum = uint64(ino)
			ok = true
			if fs.inodes != nil && nodeID != 0 {
				fs.inodes.SetExtentIno(nodeID, inoNum)
			}
		}
	}
	if !ok {
		return false
	}
	if err := fs.ensureExtentRuntime(); err != nil {
		return false
	}
	v := fs.extentVFS()
	if v == nil {
		return false
	}
	opened := uint8(0)
	if fuseFh != 0 {
		opened = 1
	}
	ctx := fs.jfsCtx(0, 0, 0)
	jentry, errn := v.GetAttr(ctx, jfsmeta.Ino(inoNum), opened)
	if errn != 0 || jentry == nil || jentry.Attr == nil {
		return false
	}
	v.UpdateLength(jfsmeta.Ino(inoNum), jentry.Attr)
	entry, ok := fs.inodes.GetEntry(nodeID)
	if !ok {
		return false
	}
	fs.applyJuiceFSAttr(entry, jentry.Attr)
	return true
}

// extentAttrLength is JuiceFS fuse GetAttr: VFS.GetAttr then UpdateLength.
// Uses the FUSE inode's juicefs Ino mapping only — no extra HEAD.
func (fs *Dat9FS) extentAttrLength(nodeID uint64, p string, fuseFh uint64) (int64, bool) {
	if !fs.extentRefreshFromVFS(nodeID, p, fuseFh) {
		return 0, false
	}
	entry, ok := fs.inodes.GetEntry(nodeID)
	if !ok {
		return 0, false
	}
	return entry.Size, true
}

func (fs *Dat9FS) extentSetAttr(cancel <-chan struct{}, input *gofuse.SetAttrIn, entry *InodeEntry, out *gofuse.AttrOut) gofuse.Status {
	if err := fs.ensureExtentRuntime(); err != nil {
		return gofuse.EIO
	}
	v := fs.extentVFS()
	if v == nil || entry == nil {
		return gofuse.EIO
	}
	inoNum, ok := fs.resolveJuiceIno(input.NodeId, entry.Path)
	if !ok {
		return gofuse.ENOENT
	}
	select {
	case <-cancel:
		return gofuse.EINTR
	default:
	}
	set := 0
	var mode, uid, gid uint32
	var atime, mtime int64
	var atimensec, mtimensec uint32
	var size uint64
	if input.Valid&gofuse.FATTR_MODE != 0 {
		perm, st := fs.setAttrModeForCaller(input, entry)
		if st != gofuse.OK {
			return st
		}
		set |= int(jfsmeta.SetAttrMode)
		mode = perm
	}
	ownerUID, hasUID, ownerGID, hasGID := resolveSetAttrOwner(input)
	if hasUID || hasGID {
		if st := fs.checkSetAttrOwnerForCaller(input, entry, ownerUID, hasUID, ownerGID, hasGID); st != gofuse.OK {
			return st
		}
		if hasUID {
			set |= int(jfsmeta.SetAttrUID)
			uid = ownerUID
		}
		if hasGID {
			set |= int(jfsmeta.SetAttrGID)
			gid = ownerGID
		}
	}
	if input.Valid&gofuse.FATTR_ATIME_NOW != 0 {
		set |= int(jfsmeta.SetAttrAtimeNow)
	} else if t, ok := input.GetATime(); ok {
		set |= int(jfsmeta.SetAttrAtime)
		atime = t.Unix()
		atimensec = uint32(t.Nanosecond())
	}
	if input.Valid&gofuse.FATTR_MTIME_NOW != 0 {
		set |= int(jfsmeta.SetAttrMtimeNow)
	} else if t, ok := input.GetMTime(); ok {
		set |= int(jfsmeta.SetAttrMtime)
		mtime = t.Unix()
		mtimensec = uint32(t.Nanosecond())
	}
	if input.Valid&gofuse.FATTR_SIZE != 0 {
		if input.Size > uint64(1<<63-1) {
			return gofuse.Status(syscall.EFBIG)
		}
		set |= int(jfsmeta.SetAttrSize)
		size = input.Size
	}
	if set == 0 {
		fs.fillAttr(entry, &out.Attr)
		out.SetTimeout(extentAttrTimeout(entry))
		return gofuse.OK
	}
	ctx := fs.jfsCtx(input.Pid, input.Uid, input.Gid)
	jentry, errn := v.SetAttr(ctx, jfsmeta.Ino(inoNum), set, fs.extentVFSFh(input.Fh), mode, uid, gid, atime, mtime, atimensec, mtimensec, size)
	if errn != 0 {
		return gofuse.Status(errn)
	}
	if jentry != nil && jentry.Attr != nil {
		v.UpdateLength(jfsmeta.Ino(inoNum), jentry.Attr)
		if e, ok := fs.inodes.GetEntry(input.NodeId); ok {
			fs.applyJuiceFSAttr(e, jentry.Attr)
			entry = e
		}
	}
	// JuiceFS NoAtime GetAttr/SetAttr replies can zero atime. Keep the
	// FUSE request times as kernel DAC/utimensat source of truth.
	if t, ok := input.GetATime(); ok {
		entry.Atime = t
		fs.inodes.UpdateAtime(input.NodeId, t)
	}
	if t, ok := input.GetMTime(); ok {
		entry.Mtime = t
		fs.inodes.UpdateMtime(input.NodeId, t)
	}
	if input.Valid&gofuse.FATTR_MODE != 0 {
		kind := entry.Mode & fileKindModeMask
		if kind == 0 {
			kind = uint32(syscall.S_IFREG)
		}
		entry.Mode = kind | (mode & posixPermissionModeMask)
		entry.HasMode = true
		fs.inodes.UpdateMode(input.NodeId, entry.Mode)
	}
	if input.Valid&gofuse.FATTR_SIZE != 0 {
		fs.extentKeepOpenWALIndexSize(entry)
	}
	fs.fillAttr(entry, &out.Attr)
	out.SetTimeout(extentAttrTimeout(entry))
	if input.Valid&gofuse.FATTR_SIZE != 0 {
		// JuiceFS fuse Open notifies when !KeepCache. After ftruncate of a
		// MAP_SHARED WAL shm (crash01 _exit while other processes still
		// mmap), leftover dirty pages past i_size make close() return EIO
		// (SQLITE_IOERR_CLOSE) without a FUSE WRITE. Drop the kernel cache.
		fs.notifyInode(input.NodeId)
	}
	return gofuse.OK
}

func (fs *Dat9FS) extentMirrorDirAttr(input *gofuse.SetAttrIn, entry *InodeEntry) {
	if fs == nil || input == nil || entry == nil || !entry.IsDir {
		return
	}
	if fs.extentRT == nil && !fs.extentEnabled() {
		return
	}
	if err := fs.ensureExtentRuntime(); err != nil {
		return
	}
	v := fs.extentVFS()
	if v == nil {
		return
	}
	inoNum, ok := fs.resolveJuiceIno(input.NodeId, entry.Path)
	if !ok {
		return
	}
	set := 0
	var mode, uid, gid uint32
	var atime, mtime int64
	var atimensec, mtimensec uint32
	if input.Valid&gofuse.FATTR_MODE != 0 {
		set |= int(jfsmeta.SetAttrMode)
		mode = entry.Mode & posixPermissionModeMask
	}
	ownerUID, hasUID, ownerGID, hasGID := resolveSetAttrOwner(input)
	if hasUID {
		set |= int(jfsmeta.SetAttrUID)
		uid = ownerUID
	}
	if hasGID {
		set |= int(jfsmeta.SetAttrGID)
		gid = ownerGID
	}
	if input.Valid&gofuse.FATTR_ATIME_NOW != 0 {
		set |= int(jfsmeta.SetAttrAtimeNow)
	} else if t, ok := input.GetATime(); ok {
		set |= int(jfsmeta.SetAttrAtime)
		atime = t.Unix()
		atimensec = uint32(t.Nanosecond())
	}
	if input.Valid&gofuse.FATTR_MTIME_NOW != 0 {
		set |= int(jfsmeta.SetAttrMtimeNow)
	} else if t, ok := input.GetMTime(); ok {
		set |= int(jfsmeta.SetAttrMtime)
		mtime = t.Unix()
		mtimensec = uint32(t.Nanosecond())
	}
	if set == 0 {
		return
	}
	ctx := fs.jfsCtx(input.Pid, input.Uid, input.Gid)
	_, _ = v.SetAttr(ctx, jfsmeta.Ino(inoNum), set, 0, mode, uid, gid, atime, mtime, atimensec, mtimensec, 0)
}

// resolveExtentIno finds the JuiceFS inode without depending on a FUSE
// request context. Open handles and the FUSE inode cache are preferred so
// SetAttr(FATTR_SIZE) still truncates after a cancelled HEAD or a hardlink.
func (fs *Dat9FS) resolveJuiceIno(nodeID uint64, p string) (uint64, bool) {
	if fs.inodes != nil {
		if entry, ok := fs.inodes.GetEntry(nodeID); ok && entry.ExtentIno != 0 {
			return entry.ExtentIno, true
		}
	}
	if ino, ok := fs.cachedExtentIno(nodeID); ok {
		return ino, true
	}
	if p != "" && (fs.extentRT != nil || fs.extentEnabled()) {
		if err := fs.ensureExtentRuntime(); err == nil {
			if ino, ok := fs.extentLookupChild(fs.jfsCtx(0, 0, 0), p); ok {
				if fs.inodes != nil && nodeID != 0 {
					fs.inodes.SetExtentIno(nodeID, uint64(ino))
				}
				return uint64(ino), true
			}
		}
	}
	return fs.resolveExtentIno(nodeID, p)
}

func (fs *Dat9FS) resolveExtentIno(nodeID uint64, p string) (uint64, bool) {
	if ino, ok := fs.cachedExtentIno(nodeID); ok {
		return ino, true
	}
	std, cf := context.WithTimeout(context.Background(), fuseTimeout)
	defer cf()
	ino, ok := fs.extentStatIno(std, p)
	if ok && fs.inodes != nil && nodeID != 0 {
		fs.inodes.SetExtentIno(nodeID, ino)
	}
	return ino, ok
}

func (fs *Dat9FS) shouldExtentTruncate(nodeID uint64, p string) bool {
	_, ok := fs.resolveExtentIno(nodeID, p)
	return ok
}

func (fs *Dat9FS) extentTruncate(cancel <-chan struct{}, nodeID uint64, p string, fuseFh, size uint64) gofuse.Status {
	if err := fs.ensureExtentRuntime(); err != nil {
		return gofuse.EIO
	}
	select {
	case <-cancel:
		return gofuse.EINTR
	default:
	}
	v := fs.extentVFS()
	if v == nil {
		return gofuse.EIO
	}
	inoNum, ok := fs.resolveExtentIno(nodeID, p)
	if !ok {
		return gofuse.ENOENT
	}
	ctx := fs.jfsCtx(0, 0, 0)
	entry, errn := v.SetAttr(ctx, jfsmeta.Ino(inoNum), int(jfsmeta.SetAttrSize), fs.extentVFSFh(fuseFh), 0, 0, 0, 0, 0, 0, 0, size)
	if errn != 0 {
		return gofuse.Status(errn)
	}
	if entry != nil && entry.Attr != nil {
		fs.inodes.UpdateSize(nodeID, int64(entry.Attr.Length))
	} else {
		fs.inodes.UpdateSize(nodeID, int64(size))
	}
	return gofuse.OK
}

func (fs *Dat9FS) extentRename(cancel <-chan struct{}, input *gofuse.RenameIn, oldP, newP, oldName, newName string) gofuse.Status {
	if err := fs.ensureExtentRuntime(); err != nil {
		return gofuse.EIO
	}
	std, cf := fuseCtx(cancel)
	defer cf()
	_, newInfo, pre := fs.renamePreflight(std, input, oldP, newP)
	if pre != gofuse.OK {
		return pre
	}
	if newInfo.special {
		if rst := fs.removeRenameSpecialTarget(std, newInfo); rst != gofuse.OK {
			return rst
		}
	}
	ctx := fs.jfsCtx(input.Pid, input.Uid, input.Gid)
	srcParent, errn := fs.juiceParentForOp(ctx, input.NodeId, pathutil.ParentPath(oldP))
	if errn != 0 {
		return gofuse.Status(errn)
	}
	dstParent, errn := fs.juiceParentForOp(ctx, input.Newdir, pathutil.ParentPath(newP))
	if errn != 0 {
		return gofuse.Status(errn)
	}
	var resp struct {
		Errno int `json:"errno"`
	}
	call := fs.extentRT.rt.Transport.Call(ctx, jfsmeta.Drive9OpRename, map[string]any{
		"src_parent": srcParent,
		"src_name":   oldName,
		"dst_parent": dstParent,
		"dst_name":   newName,
		"flags":      input.Flags,
		"src_path":   fs.remotePath(oldP),
		"dst_path":   fs.remotePath(newP),
	}, &resp)
	if call != 0 {
		return gofuse.Status(call)
	}
	if resp.Errno != 0 {
		eno := syscall.Errno(resp.Errno)
		if eno == syscall.ENOTDIR {
			if e, ok := fs.inodes.GetEntry(input.Newdir); ok && entryIsSymlink(e) {
				return gofuse.Status(syscall.ELOOP)
			}
			if e, ok := fs.inodes.GetEntry(input.NodeId); ok && entryIsSymlink(e) {
				return gofuse.Status(syscall.ELOOP)
			}
		}
		return gofuse.Status(eno)
	}
	replacedIno, _ := fs.inodes.GetInode(newP)
	fs.finishLocalRename(input, oldP, newP)
	if replacedIno != 0 {
		if srcIno, ok := fs.inodes.GetInode(newP); !ok || srcIno != replacedIno {
			fs.notifyInode(replacedIno)
		}
	}
	return gofuse.OK
}

// extentRenameDirEdge moves the JuiceFS directory edge after drive9 has
// already done the projection subtree UPDATE. Empty src/dst paths skip the
// single-row file_nodes rewrite.
func (fs *Dat9FS) extentRenameDirEdge(input *gofuse.RenameIn, oldP, newP, oldName, newName string) {
	if fs.extentRT == nil {
		if fs.opts == nil || len(fs.opts.ExtentPaths) == 0 {
			return
		}
		if err := fs.ensureExtentRuntime(); err != nil {
			return
		}
	}
	ctx := fs.jfsCtx(input.Pid, input.Uid, input.Gid)
	srcParent, st := fs.juiceParentForOp(ctx, input.NodeId, pathutil.ParentPath(strings.TrimSuffix(oldP, "/")))
	if st != 0 {
		return
	}
	dstParent, st := fs.juiceParentForOp(ctx, input.Newdir, pathutil.ParentPath(strings.TrimSuffix(newP, "/")))
	if st != 0 {
		return
	}
	var resp struct {
		Errno int `json:"errno"`
	}
	_ = fs.extentRT.rt.Transport.Call(ctx, jfsmeta.Drive9OpRename, map[string]any{
		"src_parent": srcParent,
		"src_name":   oldName,
		"dst_parent": dstParent,
		"dst_name":   newName,
	}, &resp)
}

func (fs *Dat9FS) extentSetLk(cancel <-chan struct{}, input *gofuse.LkIn, blocking bool) gofuse.Status {
	fh, ok := fs.fileHandles.Get(input.Fh)
	if !ok || fh == nil || fh.extentIno == 0 || fh.extentFh == 0 {
		return gofuse.ENOENT
	}
	if err := fs.ensureExtentRuntime(); err != nil {
		return gofuse.EIO
	}
	v := fs.extentVFS()
	if v == nil {
		return gofuse.EIO
	}
	ctx, stop := fs.jfsCtxCancelLock(cancel, input.Pid, 0, 0, blocking)
	defer stop()
	// JuiceFS fuse setLk/Flock pass in.Owner as-is. fuseLockOwner (pid/fh
	// fallback) is only for Dat9FS's in-memory table; using it here made
	// Setlk owner≠Flush LockOwner so POSIX unlocks missed and WAL blocked.
	if input.LkFlags&gofuse.FUSE_LK_FLOCK != 0 {
		return gofuse.Status(v.Flock(ctx, fh.extentIno, fh.extentFh, input.Owner, input.Lk.Typ, blocking))
	}
	return gofuse.Status(v.Setlk(ctx, fh.extentIno, fh.extentFh, input.Owner, input.Lk.Start, input.Lk.End, input.Lk.Typ, input.Pid, blocking))
}

func (fs *Dat9FS) extentGetLk(cancel <-chan struct{}, input *gofuse.LkIn, out *gofuse.LkOut) gofuse.Status {
	fh, ok := fs.fileHandles.Get(input.Fh)
	if !ok || fh == nil || fh.extentIno == 0 || fh.extentFh == 0 {
		return gofuse.ENOENT
	}
	if err := fs.ensureExtentRuntime(); err != nil {
		return gofuse.EIO
	}
	v := fs.extentVFS()
	if v == nil {
		return gofuse.EIO
	}
	ctx, stop := fs.jfsCtxCancel(cancel, input.Pid, 0, 0)
	defer stop()
	ltype := input.Lk.Typ
	start, end := input.Lk.Start, input.Lk.End
	pid := input.Pid
	if errn := v.Getlk(ctx, fh.extentIno, fh.extentFh, input.Owner, &start, &end, &ltype, &pid); errn != 0 {
		return gofuse.Status(errn)
	}
	out.Lk = gofuse.FileLock{Start: start, End: end, Typ: ltype, Pid: pid}
	return gofuse.OK
}

func (fs *Dat9FS) invalidateExtentReaders(p string) {
	// JuiceFS close-to-open: VFS owns reader invalidation on Write.
	// Remote SSE already notifyInode; do not keep a parallel Dat9FS cache.
}

func (fs *Dat9FS) extentCompactLoop(ctx context.Context) {
	// JuiceFS NoBGJob: compact is Write-triggered, not a tight scanner.
	// A 200ms idle still claimed leftover fat chunks in a loop and the
	// HTTP CAS contended with sqlite --finish on the same mount.
	idle := 2 * time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		if fs.extentRT == nil || fs.extentRT.rt == nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(idle):
			}
			continue
		}
		ino, indx, taskID, err := extent.ClaimNextCompact(fs.extentRT.rt.Transport)
		if err != nil || taskID == "" || ino == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(idle):
			}
			continue
		}
		if err := extent.ExecuteCompact(fs.extentRT.rt, ino, indx); err != nil {
			_ = extent.RequeueCompact(fs.extentRT.rt.Transport, taskID)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(idle):
		}
	}
}
