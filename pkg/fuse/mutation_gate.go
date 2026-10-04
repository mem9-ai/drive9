package fuse

import (
	"context"
	"sync"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

type workspaceMutationGate struct {
	mu        sync.Mutex
	quiescing bool
	active    int
	changed   chan struct{}
}

func newWorkspaceMutationGate() *workspaceMutationGate {
	return &workspaceMutationGate{changed: make(chan struct{})}
}

func (g *workspaceMutationGate) enter(cancel <-chan struct{}) bool {
	if g == nil {
		return true
	}
	for {
		if canceled(cancel) {
			return false
		}
		g.mu.Lock()
		if !g.quiescing {
			g.active++
			g.mu.Unlock()
			return true
		}
		changed := g.changed
		g.mu.Unlock()
		if cancel == nil {
			<-changed
			continue
		}
		select {
		case <-changed:
		case <-cancel:
			return false
		}
	}
}

func (g *workspaceMutationGate) leave() {
	if g == nil {
		return
	}
	g.mu.Lock()
	if g.active == 0 {
		g.mu.Unlock()
		panic("workspace mutation gate leave without enter")
	}
	g.active--
	g.signalLocked()
	g.mu.Unlock()
}

func (g *workspaceMutationGate) quiesce(ctx context.Context) (func(), error) {
	if g == nil {
		return func() {}, nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		g.mu.Lock()
		if !g.quiescing {
			g.quiescing = true
			g.signalLocked()
			g.mu.Unlock()
			break
		}
		changed := g.changed
		g.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	for {
		g.mu.Lock()
		if g.active == 0 {
			g.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() {
					g.mu.Lock()
					g.quiescing = false
					g.signalLocked()
					g.mu.Unlock()
				})
			}, nil
		}
		changed := g.changed
		g.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			g.mu.Lock()
			g.quiescing = false
			g.signalLocked()
			g.mu.Unlock()
			return nil, ctx.Err()
		}
	}
}

func (g *workspaceMutationGate) signalLocked() {
	close(g.changed)
	g.changed = make(chan struct{})
}

func canceled(cancel <-chan struct{}) bool {
	if cancel == nil {
		return false
	}
	select {
	case <-cancel:
		return true
	default:
		return false
	}
}

type gatedRawFileSystem struct {
	gofuse.RawFileSystem
	gate *workspaceMutationGate
}

func newGatedRawFileSystem(fs gofuse.RawFileSystem, gate *workspaceMutationGate) gofuse.RawFileSystem {
	return &gatedRawFileSystem{RawFileSystem: fs, gate: gate}
}

func (fs *gatedRawFileSystem) enter(cancel <-chan struct{}) bool {
	return fs.gate.enter(cancel)
}

func (fs *gatedRawFileSystem) leave() {
	fs.gate.leave()
}

func (fs *gatedRawFileSystem) SetAttr(cancel <-chan struct{}, input *gofuse.SetAttrIn, out *gofuse.AttrOut) gofuse.Status {
	if !fs.enter(cancel) {
		return gofuse.Status(syscall.EINTR)
	}
	defer fs.leave()
	return fs.RawFileSystem.SetAttr(cancel, input, out)
}

func (fs *gatedRawFileSystem) Mknod(cancel <-chan struct{}, input *gofuse.MknodIn, name string, out *gofuse.EntryOut) gofuse.Status {
	if !fs.enter(cancel) {
		return gofuse.Status(syscall.EINTR)
	}
	defer fs.leave()
	return fs.RawFileSystem.Mknod(cancel, input, name, out)
}

func (fs *gatedRawFileSystem) Mkdir(cancel <-chan struct{}, input *gofuse.MkdirIn, name string, out *gofuse.EntryOut) gofuse.Status {
	if !fs.enter(cancel) {
		return gofuse.Status(syscall.EINTR)
	}
	defer fs.leave()
	return fs.RawFileSystem.Mkdir(cancel, input, name, out)
}

func (fs *gatedRawFileSystem) Unlink(cancel <-chan struct{}, header *gofuse.InHeader, name string) gofuse.Status {
	if !fs.enter(cancel) {
		return gofuse.Status(syscall.EINTR)
	}
	defer fs.leave()
	return fs.RawFileSystem.Unlink(cancel, header, name)
}

func (fs *gatedRawFileSystem) Rmdir(cancel <-chan struct{}, header *gofuse.InHeader, name string) gofuse.Status {
	if !fs.enter(cancel) {
		return gofuse.Status(syscall.EINTR)
	}
	defer fs.leave()
	return fs.RawFileSystem.Rmdir(cancel, header, name)
}

func (fs *gatedRawFileSystem) Rename(cancel <-chan struct{}, input *gofuse.RenameIn, oldName string, newName string) gofuse.Status {
	if !fs.enter(cancel) {
		return gofuse.Status(syscall.EINTR)
	}
	defer fs.leave()
	return fs.RawFileSystem.Rename(cancel, input, oldName, newName)
}

func (fs *gatedRawFileSystem) Link(cancel <-chan struct{}, input *gofuse.LinkIn, filename string, out *gofuse.EntryOut) gofuse.Status {
	if !fs.enter(cancel) {
		return gofuse.Status(syscall.EINTR)
	}
	defer fs.leave()
	return fs.RawFileSystem.Link(cancel, input, filename, out)
}

func (fs *gatedRawFileSystem) Symlink(cancel <-chan struct{}, header *gofuse.InHeader, pointedTo string, linkName string, out *gofuse.EntryOut) gofuse.Status {
	if !fs.enter(cancel) {
		return gofuse.Status(syscall.EINTR)
	}
	defer fs.leave()
	return fs.RawFileSystem.Symlink(cancel, header, pointedTo, linkName, out)
}

func (fs *gatedRawFileSystem) SetXAttr(cancel <-chan struct{}, input *gofuse.SetXAttrIn, attr string, data []byte) gofuse.Status {
	if !fs.enter(cancel) {
		return gofuse.Status(syscall.EINTR)
	}
	defer fs.leave()
	return fs.RawFileSystem.SetXAttr(cancel, input, attr, data)
}

func (fs *gatedRawFileSystem) RemoveXAttr(cancel <-chan struct{}, header *gofuse.InHeader, attr string) gofuse.Status {
	if !fs.enter(cancel) {
		return gofuse.Status(syscall.EINTR)
	}
	defer fs.leave()
	return fs.RawFileSystem.RemoveXAttr(cancel, header, attr)
}

func (fs *gatedRawFileSystem) Create(cancel <-chan struct{}, input *gofuse.CreateIn, name string, out *gofuse.CreateOut) gofuse.Status {
	if !fs.enter(cancel) {
		return gofuse.Status(syscall.EINTR)
	}
	defer fs.leave()
	return fs.RawFileSystem.Create(cancel, input, name, out)
}

func (fs *gatedRawFileSystem) Open(cancel <-chan struct{}, input *gofuse.OpenIn, out *gofuse.OpenOut) gofuse.Status {
	if !fs.enter(cancel) {
		return gofuse.Status(syscall.EINTR)
	}
	defer fs.leave()
	return fs.RawFileSystem.Open(cancel, input, out)
}

func (fs *gatedRawFileSystem) Release(cancel <-chan struct{}, input *gofuse.ReleaseIn) {
	// Release has no status result, so dropping it on cancellation would leak
	// the handle and could skip its final flush. It must wait for the barrier.
	_ = fs.enter(nil)
	defer fs.leave()
	fs.RawFileSystem.Release(cancel, input)
}

func (fs *gatedRawFileSystem) Write(cancel <-chan struct{}, input *gofuse.WriteIn, data []byte) (uint32, gofuse.Status) {
	if !fs.enter(cancel) {
		return 0, gofuse.Status(syscall.EINTR)
	}
	defer fs.leave()
	return fs.RawFileSystem.Write(cancel, input, data)
}

func (fs *gatedRawFileSystem) CopyFileRange(cancel <-chan struct{}, input *gofuse.CopyFileRangeIn) (uint32, gofuse.Status) {
	if !fs.enter(cancel) {
		return 0, gofuse.Status(syscall.EINTR)
	}
	defer fs.leave()
	return fs.RawFileSystem.CopyFileRange(cancel, input)
}

func (fs *gatedRawFileSystem) Ioctl(cancel <-chan struct{}, input *gofuse.IoctlIn, inbuf []byte, output *gofuse.IoctlOut, outbuf []byte) gofuse.Status {
	if !fs.enter(cancel) {
		return gofuse.Status(syscall.EINTR)
	}
	defer fs.leave()
	return fs.RawFileSystem.Ioctl(cancel, input, inbuf, output, outbuf)
}

func (fs *gatedRawFileSystem) Flush(cancel <-chan struct{}, input *gofuse.FlushIn) gofuse.Status {
	if !fs.enter(cancel) {
		return gofuse.Status(syscall.EINTR)
	}
	defer fs.leave()
	return fs.RawFileSystem.Flush(cancel, input)
}

func (fs *gatedRawFileSystem) Fsync(cancel <-chan struct{}, input *gofuse.FsyncIn) gofuse.Status {
	if !fs.enter(cancel) {
		return gofuse.Status(syscall.EINTR)
	}
	defer fs.leave()
	return fs.RawFileSystem.Fsync(cancel, input)
}

func (fs *gatedRawFileSystem) Fallocate(cancel <-chan struct{}, input *gofuse.FallocateIn) gofuse.Status {
	if !fs.enter(cancel) {
		return gofuse.Status(syscall.EINTR)
	}
	defer fs.leave()
	return fs.RawFileSystem.Fallocate(cancel, input)
}

func (fs *gatedRawFileSystem) FsyncDir(cancel <-chan struct{}, input *gofuse.FsyncIn) gofuse.Status {
	if !fs.enter(cancel) {
		return gofuse.Status(syscall.EINTR)
	}
	defer fs.leave()
	return fs.RawFileSystem.FsyncDir(cancel, input)
}

func (fs *gatedRawFileSystem) SyncFs(cancel <-chan struct{}, input *gofuse.InHeader) gofuse.Status {
	if !fs.enter(cancel) {
		return gofuse.Status(syscall.EINTR)
	}
	defer fs.leave()
	return fs.RawFileSystem.SyncFs(cancel, input)
}
