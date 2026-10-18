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

type materializedPathKernelFS struct {
	*Dat9FS
	fdTruncates, pathTruncates, reads atomic.Int32
}

func (f *materializedPathKernelFS) SetAttr(c <-chan struct{}, in *gofuse.SetAttrIn, out *gofuse.AttrOut) gofuse.Status {
	if in.Valid&gofuse.FATTR_SIZE != 0 {
		if in.Valid&gofuse.FATTR_FH != 0 {
			f.fdTruncates.Add(1)
		} else {
			f.pathTruncates.Add(1)
		}
	}
	return f.Dat9FS.SetAttr(c, in, out)
}

func (f *materializedPathKernelFS) Read(c <-chan struct{}, in *gofuse.ReadIn, buf []byte) (gofuse.ReadResult, gofuse.Status) {
	f.reads.Add(1)
	return f.Dat9FS.Read(c, in, buf)
}

func TestFtruncatePathCommitKernel(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		if os.Getenv("DRIVE9_REQUIRE_KERNEL_FUSE") == "1" {
			t.Fatal(err)
		}
		t.Skip(err)
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		if os.Getenv("DRIVE9_REQUIRE_KERNEL_FUSE") == "1" {
			t.Fatal(err)
		}
		t.Skip(err)
	}
	for _, mode := range []string{"lazy-proof", "path-shrink", "path-grow"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, remote := truncateHandoffFS(t, []byte("hello world"))
			if mode == "lazy-proof" {
				addTruncateHandoffAlias(t, fs, ino, remote)
			}
			fs.opts.MountPoint = t.TempDir()
			fs.opts.DirectMountStrict = os.Geteuid() == 0
			fs.inodes.UpdateMode(ino, 0600)
			fs.inodes.UpdateOwner(ino, uint32(os.Getuid()), uint32(os.Getgid()), true, true)
			fs.cacheFileForPath("/file.bin", 11, time.Now(), 1)
			raw := &materializedPathKernelFS{Dat9FS: fs}
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
				fs.notifyWg.Wait()
			})
			if err := server.WaitMount(); err != nil {
				t.Fatal(err)
			}
			script := `import os,subprocess,sys
root,mode=sys.argv[1:]
p=os.path.join(root,'file.bin')
a=os.open(p,os.O_RDWR)
b=os.open(os.path.join(root,'alias.bin') if mode=='lazy-proof' else p,os.O_RDWR)
r=os.open(p,os.O_RDONLY)
try:
 os.ftruncate(a,11 if mode=='lazy-proof' else 5)
 os.fsync(a)
 if mode=='lazy-proof':
  os.close(a);a=-1
  want=b'hello world'
 else:
  size=3 if mode=='path-shrink' else 8
  subprocess.run([sys.executable,'-c','import os,sys;os.truncate(sys.argv[1],int(sys.argv[2]))',p,str(size)],check=True)
  want=b'hel' if size==3 else b'hello\0\0\0'
 for fd in ([b,r] if a<0 else [a,b,r]):
  got=os.pread(fd,64,0)
  assert got==want,(mode,fd,got,want)
  assert os.pread(fd,64,len(want))==b''
 print('KERNEL_HANDOFF_OK',mode,want.hex(),flush=True)
finally:
 os.close(r);os.close(b)
 if a>=0:os.close(a)
`
			result, err := exec.Command(python, "-c", script, filepath.Clean(fs.opts.MountPoint), mode).CombinedOutput()
			t.Logf("userspace %s", result)
			if err != nil {
				t.Fatal(err)
			}
			if raw.fdTruncates.Load() == 0 || raw.reads.Load() == 0 || (mode != "lazy-proof" && raw.pathTruncates.Load() == 0) {
				t.Fatalf("missing kernel callbacks: fd=%d path=%d reads=%d", raw.fdTruncates.Load(), raw.pathTruncates.Load(), raw.reads.Load())
			}
			data, _, _ := remote.snapshot()
			want := []byte("hello world")
			switch mode {
			case "path-shrink":
				want = []byte("hel")
			case "path-grow":
				want = []byte("hello\x00\x00\x00")
			}
			if !bytes.Equal(data, want) {
				t.Fatalf("kernel fixture remote=%q want%q", data, want)
			}
		})
	}
}
