package fuse

import (
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// Direct I/O ensures pread reaches the FUSE Read implementation instead of
// succeeding solely from the kernel's copy of the preceding write.
type inheritedSelfReadKernelFS struct {
	*Dat9FS
	reads atomic.Int32
}

func (f *inheritedSelfReadKernelFS) Open(c <-chan struct{}, in *gofuse.OpenIn, out *gofuse.OpenOut) gofuse.Status {
	st := f.Dat9FS.Open(c, in, out)
	if st == gofuse.OK {
		out.OpenFlags |= gofuse.FOPEN_DIRECT_IO
	}
	return st
}
func (f *inheritedSelfReadKernelFS) Read(c <-chan struct{}, in *gofuse.ReadIn, buf []byte) (gofuse.ReadResult, gofuse.Status) {
	if h, ok := f.fileHandles.Get(in.Fh); ok {
		h.Lock()
		event := h.pendingFtruncate.Load()
		dirty := h.Dirty != nil && event != nil && h.DirtySeq > event.seq && !h.Dirty.visibleTruncate &&
			h.Flags&syscall.O_ACCMODE != syscall.O_RDONLY &&
			!h.coversFtruncateWrites(int64(in.Offset), min(int64(in.Offset)+int64(in.Size), h.Dirty.Size()), max(event.seq, f.ftruncateCommittedSeq(h.Ino)))
		h.Unlock()
		if dirty {
			f.reads.Add(1)
		}
	}
	return f.Dat9FS.Read(c, in, buf)
}
func TestFtruncateInheritedSelfReadKernel(t *testing.T) {
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
	for _, mode := range []string{"pending", "parent-committed", "own-committed", "nonfinal-flush"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, a, b, remote := newFtruncateCommitFS(t)
			// This fixture provides only backend state; all test descriptors below are
			// opened by the kernel, so remove its two setup descriptors normally.
			for _, id := range []uint64{a, b} {
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id})
			}
			interceptFtruncateHTTP(t, fs, func(w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Path == "/v1/status" {
					_ = json.NewEncoder(w).Encode(map[string]any{"inline_threshold": 1 << 20})
					return true
				}
				if r.URL.Path == "/v1/fs/" && r.Method == http.MethodHead {
					w.Header().Set("X-Dat9-IsDir", "true")
					w.Header().Set("Content-Length", "0")
					w.Header().Set("X-Dat9-Revision", "1")
					return true
				}
				return false
			})
			fs.opts.MountPoint = t.TempDir()
			fs.opts.DirectMountStrict = os.Geteuid() == 0
			fs.inodes.UpdateMode(ino, 0600)
			fs.inodes.UpdateOwner(ino, uint32(os.Getuid()), uint32(os.Getgid()), true, true)
			fs.cacheFileForPath("/file.bin", 11, time.Now(), 1)
			raw := &inheritedSelfReadKernelFS{Dat9FS: fs}
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
 os.ftruncate(a,5)
 if mode=='nonfinal-flush':
  duplicate=os.dup(a)
  os.close(duplicate)
  assert os.pread(b,5,0)==b'hello'
  assert os.pread(r,5,0)==b'hello'
 if mode=='parent-committed': os.fsync(a)
 assert os.pwrite(b,b'H',0)==1
 want=b'Hello'
 if mode=='own-committed':
  os.fsync(b)
  assert os.pwrite(b,b'X',1)==1
  want=b'HXllo'
 assert os.pread(b,5,0)==want
 assert os.pread(b,4096,0)==want
 assert os.pread(b,4,1)==want[1:]
 assert os.pread(b,5,5)==b''
 assert os.pread(r,4096,0)==want
 os.fsync(b)
 print('SELF_READ_OK',mode,want.decode(),flush=True)
finally:
 os.close(r);os.close(b);os.close(a)
`
			result, err := exec.Command(python, "-c", script, filepath.Join(fs.opts.MountPoint, "file.bin"), mode).CombinedOutput()
			t.Logf("userspace %s", result)
			if err != nil {
				t.Fatal(err)
			}
			if raw.reads.Load() == 0 {
				t.Fatal("pread never reached an inherited range requiring the new self-read proof")
			}
			fs.commitQueue.DrainAll()
			want := "Hello"
			if mode == "own-committed" {
				want = "HXllo"
			}
			if remote() != want {
				t.Fatalf("remote=%q want=%q", remote(), want)
			}
		})
	}
}
