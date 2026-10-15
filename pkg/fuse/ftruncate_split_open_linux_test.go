package fuse

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

type splitOpenKernelFS struct {
	*Dat9FS
	mu       sync.Mutex
	requests []string
}

func (fs *splitOpenKernelFS) Open(c <-chan struct{}, in *gofuse.OpenIn, out *gofuse.OpenOut) gofuse.Status {
	fs.mu.Lock()
	fs.requests = append(fs.requests, fmt.Sprintf("Open pid=%d flags=%#x", in.Pid, in.Flags))
	fs.mu.Unlock()
	return fs.Dat9FS.Open(c, in, out)
}

func (fs *splitOpenKernelFS) SetAttr(c <-chan struct{}, in *gofuse.SetAttrIn, out *gofuse.AttrOut) gofuse.Status {
	fs.mu.Lock()
	fs.requests = append(fs.requests, fmt.Sprintf("SetAttr pid=%d valid=%#x size=%d", in.Pid, in.Valid, in.Size))
	fs.mu.Unlock()
	return fs.Dat9FS.SetAttr(c, in, out)
}

func (fs *splitOpenKernelFS) Flush(c <-chan struct{}, in *gofuse.FlushIn) gofuse.Status {
	st := fs.Dat9FS.Flush(c, in)
	held := false
	if fh, ok := fs.fileHandles.Get(in.Fh); ok {
		fh.Lock()
		held = fh.RemoteCommitUnlock != nil
		fh.Unlock()
	}
	fs.mu.Lock()
	fs.requests = append(fs.requests, fmt.Sprintf("Flush pid=%d fh=%d status=%v fence=%t", in.Pid, in.Fh, st, held))
	fs.mu.Unlock()
	return st
}

func TestFtruncateSplitOpenKernel(t *testing.T) {
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
	for _, order := range []string{"child_fsync_parent_close", "parent_close_child_fsync"} {
		t.Run(order, func(t *testing.T) {
			var zeroPuts atomic.Int32
			fs, ino, a, b, remote := newFtruncateCommitFS(t, func(w http.ResponseWriter, r *http.Request) bool {
				if r.ContentLength == 0 {
					zeroPuts.Add(1)
				}
				return false
			})
			// Match mount.go's asynchronous commit and supersession callbacks.
			fs.commitQueue.OnDiscard = fs.onCommitQueueDiscard
			fs.commitQueue.IsSuperseded = fs.commitEntrySuperseded
			fs.commitQueue.DurableWatermark = fs.latestCommittedRevision
			fs.uploader = &WriteBackUploader{client: fs.client, cache: fs.writeBack, remoteRoot: "/", uploadCh: make(chan string, 8)}
			fs.uploader.OnDataCommitted, fs.uploader.OnSuccess = fs.onWriteBackDataCommitted, fs.onWriteBackUploadSuccess
			fs.uploader.SnapshotStagingGens = fs.snapshotStagingGens
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
					return true
				}
				return false
			})
			fs.uploader.client = fs.client
			fs.opts.MountPoint = t.TempDir()
			fs.opts.DirectMountStrict = os.Geteuid() == 0
			fs.inodes.UpdateMode(ino, 0600)
			fs.inodes.UpdateOwner(ino, uint32(os.Getuid()), uint32(os.Getgid()), true, true)
			fs.cacheFileForPath("/file.bin", 11, time.Now(), 1)
			raw := &splitOpenKernelFS{Dat9FS: fs}
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
			script := `import os, subprocess, sys
p, order = sys.argv[1:]
child_code = '''import os, sys
p, order = sys.argv[1:]
assert sys.stdin.readline().strip() == 'open'
b = os.open(p, os.O_RDWR | os.O_TRUNC)
assert os.pwrite(b, b'X', 0) == 1
if order == 'child_fsync_parent_close': os.fsync(b)
print('ready', flush=True)
assert sys.stdin.readline().strip() == 'continue'
os.fsync(b)
os.close(b)
'''
# Start the independent process before opening a, so exec does not close an
# inherited duplicate of a and introduce a separate nonfinal-Flush schedule.
child = subprocess.Popen([sys.executable, '-c', child_code, p, order], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
a = -1
try:
 a = os.open(p, os.O_RDWR)
 os.ftruncate(a, 5)
 child.stdin.write('open\n'); child.stdin.flush()
 assert child.stdout.readline().strip() == 'ready'
 os.close(a)
 a = -1
 child.stdin.write('continue\n'); child.stdin.flush()
 assert child.wait(timeout=10) == 0
 print('SPLIT_OPEN_OK', order, flush=True)
finally:
 if a >= 0: os.close(a)
 if child.poll() is None: child.kill(); child.wait()
`
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			result, err := exec.CommandContext(ctx, python, "-c", script, filepath.Join(fs.opts.MountPoint, "file.bin"), order).CombinedOutput()
			t.Logf("userspace %s", result)
			raw.mu.Lock()
			t.Logf("kernel requests: %v", raw.requests)
			raw.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			fs.commitQueue.DrainAll()
			if zeroPuts.Load() != 1 || remote() != "X" {
				t.Fatalf("zero PUTs=%d remote=%q", zeroPuts.Load(), remote())
			}
			if meta, ok := fs.pendingIndex.GetMeta("/file.bin"); ok && meta.Kind == PendingConflict {
				t.Fatalf("retained conflict: %+v", meta)
			}
		})
	}
}
