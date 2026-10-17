package fuse

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

type retiredReadKernelFS struct {
	*Dat9FS
	reads atomic.Int32
}

func (f *retiredReadKernelFS) Open(c <-chan struct{}, in *gofuse.OpenIn, out *gofuse.OpenOut) gofuse.Status {
	st := f.Dat9FS.Open(c, in, out)
	if st == gofuse.OK {
		out.OpenFlags |= gofuse.FOPEN_DIRECT_IO
	}
	return st
}

func (f *retiredReadKernelFS) Read(c <-chan struct{}, in *gofuse.ReadIn, buf []byte) (gofuse.ReadResult, gofuse.Status) {
	if h, ok := f.fileHandles.Get(in.Fh); ok {
		h.Lock()
		if hasRetiredRead(h) && h.DirtySeq != 0 {
			f.reads.Add(1)
		}
		h.Unlock()
	}
	return f.Dat9FS.Read(c, in, buf)
}

func TestRetiredFtruncateKernel(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		if os.Getenv("DRIVE9_REQUIRE_KERNEL_FUSE") == "1" {
			t.Fatal(err)
		}
		t.Skip(err)
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"direct", "cache"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, remote := truncateHandoffFS(t, bytes.Repeat([]byte("p"), (2<<20)+4096))
			fs.opts.MountPoint = t.TempDir()
			fs.opts.DirectMountStrict = os.Geteuid() == 0
			fs.inodes.UpdateMode(ino, 0600)
			fs.inodes.UpdateOwner(ino, uint32(os.Getuid()), uint32(os.Getgid()), true, true)
			fs.cacheFileForPath("/file.bin", (2<<20)+4096, time.Now(), 1)
			raw := &retiredReadKernelFS{Dat9FS: fs}
			server, err := gofuse.NewServer(raw, fs.opts.MountPoint, newGoFuseMountOptions(fs.opts))
			if err != nil {
				if os.Getenv("DRIVE9_REQUIRE_KERNEL_FUSE") == "1" {
					t.Fatal(err)
				}
				t.Skip(err)
			}
			go server.Serve()
			t.Cleanup(func() {
				if err := server.Unmount(); err != nil {
					t.Error(err)
				}
				fs.commitQueue.DrainAll()
				fs.notifyWg.Wait()
			})
			if err := server.WaitMount(); err != nil {
				t.Fatal(err)
			}
			script := `import os,sys
p,mode=sys.argv[1:]
a=os.open(p,os.O_RDWR);b=os.open(p,os.O_RDWR);r=os.open(p,os.O_RDONLY)
try:
 os.ftruncate(a,2<<20)
 os.fsync(a)
 assert os.pwrite(b,b'H',0)==1
 assert os.pread(b,5,0)==b'Hpppp'
 if mode=='cache':
  duplicate=os.dup(b)
  os.close(duplicate)
  assert os.pread(b,5,0)==b'Hpppp'
 os.fsync(b)
 assert os.pwrite(b,b'J',1)==1
 assert os.pread(b,5,0)==b'HJppp'
 assert os.pread(b,8,2<<20)==b''
 os.fsync(b)
 print('RETIRED_SELF_READ_OK',mode,flush=True)
finally:
 os.close(r);os.close(b);os.close(a)
`
			out, err := exec.Command(python, "-c", script, filepath.Join(fs.opts.MountPoint, "file.bin"), mode).CombinedOutput()
			t.Logf("userspace: %s", out)
			if err != nil {
				t.Fatal(err)
			}
			if raw.reads.Load() < 2 {
				t.Fatal("kernel reads did not exercise the retired buffer")
			}
			fs.commitQueue.DrainAll()
			data, _, _ := remote.snapshot()
			want := bytes.Repeat([]byte("p"), 2<<20)
			copy(want, "HJ")
			if !bytes.Equal(data, want) {
				t.Fatal("kernel close/commit lost the accepted content")
			}
		})
	}
}
