package fuse

import (
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// viewRequestFS gates complete foreground callbacks, including local handle
// and cache publication, rather than only their remote requests. Cleanup notifications are admitted even while paused, but their side effects
// are counted until they return. Lifecycle notifications always pass through. This adapter
// alone is NOT a kernel reply/cache barrier or a view identity router.
//
// Deliberately do not embed RawFileSystem: adding a kernel callback must force
// an explicit decision here about ordinary admission versus cleanup.
type viewRequestFS struct {
	inner     gofuse.RawFileSystem
	admission viewAdmission
}

var _ gofuse.RawFileSystem = (*viewRequestFS)(nil)

func (v *viewRequestFS) String() string {
	return v.inner.String()
}

func (v *viewRequestFS) SetDebug(debug bool) {
	v.inner.SetDebug(debug)
}

func (v *viewRequestFS) Lookup(cancel <-chan struct{}, header *gofuse.InHeader, name string, out *gofuse.EntryOut) (status gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.Lookup(cancel, header, name, out)
}

func (v *viewRequestFS) Forget(nodeid, nlookup uint64) {
	leave := v.admission.enterCleanup()
	defer leave()
	v.inner.Forget(nodeid, nlookup)
}

func (v *viewRequestFS) GetAttr(cancel <-chan struct{}, input *gofuse.GetAttrIn, out *gofuse.AttrOut) (code gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.GetAttr(cancel, input, out)
}

func (v *viewRequestFS) SetAttr(cancel <-chan struct{}, input *gofuse.SetAttrIn, out *gofuse.AttrOut) (code gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.SetAttr(cancel, input, out)
}

func (v *viewRequestFS) Mknod(cancel <-chan struct{}, input *gofuse.MknodIn, name string, out *gofuse.EntryOut) (code gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.Mknod(cancel, input, name, out)
}

func (v *viewRequestFS) Mkdir(cancel <-chan struct{}, input *gofuse.MkdirIn, name string, out *gofuse.EntryOut) (code gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.Mkdir(cancel, input, name, out)
}

func (v *viewRequestFS) Unlink(cancel <-chan struct{}, header *gofuse.InHeader, name string) (code gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.Unlink(cancel, header, name)
}

func (v *viewRequestFS) Rmdir(cancel <-chan struct{}, header *gofuse.InHeader, name string) (code gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.Rmdir(cancel, header, name)
}

func (v *viewRequestFS) Rename(cancel <-chan struct{}, input *gofuse.RenameIn, oldName string, newName string) (code gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.Rename(cancel, input, oldName, newName)
}

func (v *viewRequestFS) Link(cancel <-chan struct{}, input *gofuse.LinkIn, filename string, out *gofuse.EntryOut) (code gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.Link(cancel, input, filename, out)
}

func (v *viewRequestFS) Symlink(cancel <-chan struct{}, header *gofuse.InHeader, pointedTo string, linkName string, out *gofuse.EntryOut) (code gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.Symlink(cancel, header, pointedTo, linkName, out)
}

func (v *viewRequestFS) Readlink(cancel <-chan struct{}, header *gofuse.InHeader) (out []byte, code gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return nil, gofuse.EAGAIN
	}
	defer leave()
	return v.inner.Readlink(cancel, header)
}

func (v *viewRequestFS) Access(cancel <-chan struct{}, input *gofuse.AccessIn) (code gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.Access(cancel, input)
}

func (v *viewRequestFS) GetXAttr(cancel <-chan struct{}, header *gofuse.InHeader, attr string, dest []byte) (sz uint32, code gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return 0, gofuse.EAGAIN
	}
	defer leave()
	return v.inner.GetXAttr(cancel, header, attr, dest)
}

func (v *viewRequestFS) ListXAttr(cancel <-chan struct{}, header *gofuse.InHeader, dest []byte) (uint32, gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return 0, gofuse.EAGAIN
	}
	defer leave()
	return v.inner.ListXAttr(cancel, header, dest)
}

func (v *viewRequestFS) SetXAttr(cancel <-chan struct{}, input *gofuse.SetXAttrIn, attr string, data []byte) gofuse.Status {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.SetXAttr(cancel, input, attr, data)
}

func (v *viewRequestFS) RemoveXAttr(cancel <-chan struct{}, header *gofuse.InHeader, attr string) (code gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.RemoveXAttr(cancel, header, attr)
}

func (v *viewRequestFS) Create(cancel <-chan struct{}, input *gofuse.CreateIn, name string, out *gofuse.CreateOut) (code gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.Create(cancel, input, name, out)
}

func (v *viewRequestFS) Open(cancel <-chan struct{}, input *gofuse.OpenIn, out *gofuse.OpenOut) (status gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.Open(cancel, input, out)
}

func (v *viewRequestFS) Read(cancel <-chan struct{}, input *gofuse.ReadIn, buf []byte) (gofuse.ReadResult, gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return nil, gofuse.EAGAIN
	}
	defer leave()
	return v.inner.Read(cancel, input, buf)
}

func (v *viewRequestFS) Lseek(cancel <-chan struct{}, in *gofuse.LseekIn, out *gofuse.LseekOut) gofuse.Status {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.Lseek(cancel, in, out)
}

func (v *viewRequestFS) GetLk(cancel <-chan struct{}, input *gofuse.LkIn, out *gofuse.LkOut) (code gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.GetLk(cancel, input, out)
}

func (v *viewRequestFS) SetLk(cancel <-chan struct{}, input *gofuse.LkIn) (code gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.SetLk(cancel, input)
}

func (v *viewRequestFS) SetLkw(cancel <-chan struct{}, input *gofuse.LkIn) (code gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.SetLkw(cancel, input)
}

func (v *viewRequestFS) Release(cancel <-chan struct{}, input *gofuse.ReleaseIn) {
	leave := v.admission.enterCleanup()
	defer leave()
	v.inner.Release(cancel, input)
}

func (v *viewRequestFS) Write(cancel <-chan struct{}, input *gofuse.WriteIn, data []byte) (written uint32, code gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return 0, gofuse.EAGAIN
	}
	defer leave()
	return v.inner.Write(cancel, input, data)
}

func (v *viewRequestFS) CopyFileRange(cancel <-chan struct{}, input *gofuse.CopyFileRangeIn) (written uint32, code gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return 0, gofuse.EAGAIN
	}
	defer leave()
	return v.inner.CopyFileRange(cancel, input)
}

func (v *viewRequestFS) Ioctl(cancel <-chan struct{}, input *gofuse.IoctlIn, inbuf []byte, output *gofuse.IoctlOut, outbuf []byte) (code gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.Ioctl(cancel, input, inbuf, output, outbuf)
}

func (v *viewRequestFS) Flush(cancel <-chan struct{}, input *gofuse.FlushIn) gofuse.Status {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.Flush(cancel, input)
}

func (v *viewRequestFS) Fsync(cancel <-chan struct{}, input *gofuse.FsyncIn) (code gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.Fsync(cancel, input)
}

func (v *viewRequestFS) Fallocate(cancel <-chan struct{}, input *gofuse.FallocateIn) (code gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.Fallocate(cancel, input)
}

func (v *viewRequestFS) OpenDir(cancel <-chan struct{}, input *gofuse.OpenIn, out *gofuse.OpenOut) (status gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.OpenDir(cancel, input, out)
}

func (v *viewRequestFS) ReadDir(cancel <-chan struct{}, input *gofuse.ReadIn, out *gofuse.DirEntryList) gofuse.Status {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.ReadDir(cancel, input, out)
}

func (v *viewRequestFS) ReadDirPlus(cancel <-chan struct{}, input *gofuse.ReadIn, out *gofuse.DirEntryList) gofuse.Status {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.ReadDirPlus(cancel, input, out)
}

func (v *viewRequestFS) ReleaseDir(input *gofuse.ReleaseIn) {
	leave := v.admission.enterCleanup()
	defer leave()
	v.inner.ReleaseDir(input)
}

func (v *viewRequestFS) FsyncDir(cancel <-chan struct{}, input *gofuse.FsyncIn) (code gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.FsyncDir(cancel, input)
}

func (v *viewRequestFS) StatFs(cancel <-chan struct{}, input *gofuse.InHeader, out *gofuse.StatfsOut) (code gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.StatFs(cancel, input, out)
}

func (v *viewRequestFS) SyncFs(cancel <-chan struct{}, input *gofuse.InHeader) (code gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.SyncFs(cancel, input)
}

func (v *viewRequestFS) Statx(cancel <-chan struct{}, input *gofuse.StatxIn, out *gofuse.StatxOut) (code gofuse.Status) {
	leave, err := v.admission.enter()
	if err != nil {
		return gofuse.EAGAIN
	}
	defer leave()
	return v.inner.Statx(cancel, input, out)
}

func (v *viewRequestFS) Init(server *gofuse.Server) {
	v.inner.Init(server)
}

func (v *viewRequestFS) OnUnmount() {
	v.inner.OnUnmount()
}
