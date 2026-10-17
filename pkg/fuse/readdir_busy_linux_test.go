package fuse

import (
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestReadDirPlusBusyNameKernel(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		if os.Getenv("DRIVE9_REQUIRE_KERNEL_FUSE") == "1" {
			t.Fatal(err)
		}
		t.Skipf("Linux FUSE unavailable: %v", err)
	}
	fs, ino, _ := truncateHandoffFS(t, []byte("hello world"))
	id := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
	truncateHandoffTruncate(t, fs, ino, id, 5)
	// A rolled-back newer marker requires consulting the locked live owner.
	later := fs.markDirtySize(ino, 77)
	fs.clearDirtySize(ino, later)
	entry, _ := fs.inodes.GetEntry(ino)
	fs.dirCache = NewDirCache(time.Hour)
	fs.dirCache.Put("/", []CachedFileInfo{cachedInfoFromEntry("file.bin", entry)})
	fs.opts.MountPoint = t.TempDir()
	fs.opts.DirectMountStrict = os.Geteuid() == 0
	kernelFS := &readdirPendingKernelFS{Dat9FS: fs}
	server, err := gofuse.NewServer(kernelFS, fs.opts.MountPoint, newGoFuseMountOptions(fs.opts))
	if err != nil {
		if os.Getenv("DRIVE9_REQUIRE_KERNEL_FUSE") == "1" {
			t.Fatal(err)
		}
		t.Skipf("cannot mount Linux FUSE: %v", err)
	}
	go server.Serve()
	t.Cleanup(func() {
		fs.notifyWg.Wait()
		if err := server.Unmount(); err != nil {
			t.Error(err)
		}
	})
	if err := server.WaitMount(); err != nil {
		t.Fatal(err)
	}
	dir, err := os.Open(fs.opts.MountPoint)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dir.Close() }()
	owner, _ := fs.fileHandles.Get(id)
	owner.Lock()
	unlock := sync.OnceFunc(owner.Unlock)
	defer unlock()
	if _, _, busy := fs.dirtyHandleSize(ino); !busy {
		t.Fatal("busy metadata prerequisite missing")
	}
	var names []string
	buf := make([]byte, 4096)
	for {
		n, err := syscall.ReadDirent(int(dir.Fd()), buf)
		if err != nil {
			t.Fatalf("busy getdents64: %v", err)
		}
		if n == 0 {
			break
		}
		_, _, names = syscall.ParseDirent(buf[:n], -1, names)
	}
	if kernelFS.plusCalls.Load() == 0 || len(names) != 1 || names[0] != "file.bin" {
		t.Fatalf("READDIRPLUS calls=%d names=%v", kernelFS.plusCalls.Load(), names)
	}
	after, _ := fs.inodes.GetEntry(ino)
	if after.Nlookup != entry.Nlookup {
		t.Fatal("name-only response acquired a lookup reference")
	}
}
