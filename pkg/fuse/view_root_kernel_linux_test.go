//go:build linux

package fuse

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
)

// This is deliberately opt-in: it creates a local kernel mount, requires
// CAP_SYS_ADMIN and /dev/fuse, and does not connect to a Drive9 server/database.
// It covers SETATTR reply ordering only, not complete mount switching.
func TestViewRootFenceKernelDelayedSetAttr(t *testing.T) {
	if os.Getenv("DRIVE9_RUN_FUSE_KERNEL_TEST") != "1" {
		t.Skip("manual Linux kernel test: set DRIVE9_RUN_FUSE_KERNEL_TEST=1")
	}
	var kernel unix.Utsname
	if err := unix.Uname(&kernel); err != nil {
		t.Fatal(err)
	}
	t.Logf("kernel=%s", bytes.TrimRight(kernel.Release[:], "\x00"))

	delay := &rootReplyDelay{held: make(chan struct{}), release: make(chan struct{})}
	raw := &rootReplyFS{RawFileSystem: gofuse.NewDefaultRawFileSystem(), delay: delay}
	raw.mode.Store(0755)
	adapter := &viewRequestFS{inner: raw}
	srv, mountpoint, notifyFD := mountRootReplyTest(t, adapter, delay)
	// Registered after mount cleanup, so a failed assertion first releases
	// the pending kernel request before unmounting and joining Serve.
	t.Cleanup(delay.unblock)
	chmodDone := make(chan error, 1)
	go func() { chmodDone <- os.Chmod(mountpoint, 0700) }()
	select {
	case <-delay.held:
	case err := <-chmodDone:
		t.Fatalf("chmod returned before reply delay: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("SETATTR reply did not reach the delay")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pause, err := adapter.admission.pause()
	if err != nil {
		t.Fatal(err)
	}
	if err := pause.wait(ctx); err != nil {
		t.Fatal(err)
	}
	// The adapter has finished its callback, but the kernel still waits for
	// its reply. A counter-only switch would incorrectly declare this idle.
	raw.mode.Store(0755)
	probe := &rootFDNotifier{fd: notifyFD, entered: make(chan struct{})}
	fenceDone := make(chan error, 1)
	go func() { fenceDone <- fenceViewRootUpdates(ctx, probe) }()
	select {
	case <-probe.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-fenceDone:
		t.Fatalf("root fence passed a still-held SETATTR reply: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	delay.unblock()
	select {
	case err := <-chmodDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("chmod did not finish after reply release")
	}
	select {
	case err := <-fenceDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("root fence did not finish after SETATTR completion")
	}
	if status := srv.InodeNotify(gofuse.FUSE_ROOT_ID, 0, 0); status != gofuse.OK {
		t.Fatalf("root invalidation: %v", status)
	}
	if err := pause.resume(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(mountpoint)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0755 {
		t.Fatalf("new root mode=%o, want 755", got)
	}
}

// go-fuse logs tx after RawFileSystem returns and before writing to /dev/fuse.
// Matching the captured request Unique keeps other requests out of the delay.
// This uses the dependency's test seam, without a production delay hook.
type rootReplyDelay struct {
	unique  atomic.Uint64
	held    chan struct{}
	release chan struct{}
	once    sync.Once
}

func (d *rootReplyDelay) unblock() { d.once.Do(func() { close(d.release) }) }
func (d *rootReplyDelay) Write(p []byte) (int, error) {
	if id := d.unique.Load(); id != 0 && bytes.HasPrefix(p, []byte(fmt.Sprintf("tx %d:", id))) {
		close(d.held)
		<-d.release
	}
	return len(p), nil
}

type rootReplyFS struct {
	gofuse.RawFileSystem
	delay *rootReplyDelay
	mode  atomic.Uint32
}

func (r *rootReplyFS) GetAttr(_ <-chan struct{}, in *gofuse.GetAttrIn, out *gofuse.AttrOut) gofuse.Status {
	if in.NodeId != gofuse.FUSE_ROOT_ID {
		return gofuse.ENOENT
	}
	r.fillAttr(out, r.mode.Load())
	return gofuse.OK
}
func (r *rootReplyFS) SetAttr(_ <-chan struct{}, in *gofuse.SetAttrIn, out *gofuse.AttrOut) gofuse.Status {
	mode, ok := in.GetMode()
	if !ok || in.NodeId != gofuse.FUSE_ROOT_ID {
		return gofuse.EINVAL
	}
	r.mode.Store(mode & 0777)
	r.fillAttr(out, mode&0777)
	r.delay.unique.Store(in.Unique)
	return gofuse.OK
}
func (r *rootReplyFS) fillAttr(out *gofuse.AttrOut, mode uint32) {
	out.Attr = gofuse.Attr{Ino: gofuse.FUSE_ROOT_ID, Mode: unix.S_IFDIR | mode, Nlink: 2,
		Owner: gofuse.Owner{Uid: uint32(os.Getuid()), Gid: uint32(os.Getgid())}}
}

// The debug log mutex is held by the delayed reply. Calling srv.EntryNotify
// would also wait on that mutex and give a false pass without testing the
// kernel lock. Send the same FUSE notification through a dup of its device FD.
type rootFDNotifier struct {
	fd      int
	entered chan struct{}
}

func (n *rootFDNotifier) EntryNotify(parent uint64, name string) gofuse.Status {
	packet := make([]byte, 16+16+len(name)+1) // out_header, inval_entry, NUL name
	binary.NativeEndian.PutUint32(packet[0:4], uint32(len(packet)))
	binary.NativeEndian.PutUint32(packet[4:8], uint32(-gofuse.NOTIFY_INVAL_ENTRY))
	binary.NativeEndian.PutUint64(packet[16:24], parent)
	binary.NativeEndian.PutUint32(packet[24:28], uint32(len(name)))
	copy(packet[32:], name)
	close(n.entered)
	count, err := unix.Write(n.fd, packet)
	if err != nil {
		return gofuse.ToStatus(err)
	}
	if count != len(packet) {
		return gofuse.EIO
	}
	return gofuse.OK
}

func mountRootReplyTest(t *testing.T, fs gofuse.RawFileSystem, delay *rootReplyDelay) (*gofuse.Server, string, int) {
	t.Helper()
	mountpoint := t.TempDir()
	fd, err := unix.Open("/dev/fuse", unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	options := fmt.Sprintf("fd=%d,rootmode=40000,user_id=%d,group_id=%d", fd, os.Getuid(), os.Getgid())
	if err := unix.Mount("drive9-root-fence-test", mountpoint, "fuse", unix.MS_NOSUID|unix.MS_NODEV, options); err != nil {
		_ = unix.Close(fd)
		t.Fatal(err)
	}
	var srv *gofuse.Server
	served := make(chan struct{})
	t.Cleanup(func() {
		if err := unix.Unmount(mountpoint, unix.MNT_DETACH); err != nil {
			t.Errorf("unmount: %v", err)
		}
		if srv == nil {
			return // NewServer closes its fd on initialization failure.
		}
		select {
		case <-served:
		case <-time.After(5 * time.Second):
			t.Error("FUSE server did not stop after unmount")
		}
	})
	// Duplicate before NewServer takes ownership. F_DUPFD_CLOEXEC also keeps
	// the notification FD out of subprocesses on the test machine.
	notifyFD, err := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		_ = unix.Close(fd)
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Close(notifyFD) })
	srv, err = gofuse.NewServer(fs, fmt.Sprintf("/dev/fd/%d", fd), &gofuse.MountOptions{
		Debug: true, Logger: log.New(delay, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { srv.Serve(); close(served) }()
	return srv, mountpoint, notifyFD
}
