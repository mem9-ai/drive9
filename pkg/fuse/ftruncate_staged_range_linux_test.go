package fuse

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// Hold Release until the observer registers, then delay the queued upload.
// This deterministically exposes the live-to-staged handoff without altering
// the production buffers, lineage, or generation checks.
type stagedRangeKernelFS struct {
	*Dat9FS
	releaseAllowed chan struct{}
	releaseDone    chan struct{}
	allowOnce      sync.Once
	releaseOnce    sync.Once
	writer         atomic.Uint64
	truncates      atomic.Int32
	t              *testing.T
}

func (f *stagedRangeKernelFS) Create(c <-chan struct{}, in *gofuse.CreateIn, name string, out *gofuse.CreateOut) gofuse.Status {
	st := f.Dat9FS.Create(c, in, name, out)
	if st == gofuse.OK {
		f.writer.Store(out.Fh)
	}
	return st
}
func (f *stagedRangeKernelFS) SetAttr(c <-chan struct{}, in *gofuse.SetAttrIn, out *gofuse.AttrOut) gofuse.Status {
	if in.Valid&gofuse.FATTR_SIZE != 0 {
		f.truncates.Add(1)
		f.t.Logf("KERNEL setattr valid=%x size=%d fh=%d", in.Valid, in.Size, in.Fh)
	}
	return f.Dat9FS.SetAttr(c, in, out)
}
func (f *stagedRangeKernelFS) Open(c <-chan struct{}, in *gofuse.OpenIn, out *gofuse.OpenOut) gofuse.Status {
	st := f.Dat9FS.Open(c, in, out)
	if st == gofuse.OK && in.Flags&syscall.O_ACCMODE == syscall.O_RDONLY {
		h, _ := f.fileHandles.Get(out.Fh)
		f.t.Logf("KERNEL reader registered inherited=%t shadow_pin=%t", h.pendingFtruncate.Load() != nil, h.ShadowPinned)
		f.allowOnce.Do(func() { close(f.releaseAllowed) })
		select {
		case <-f.releaseDone:
		case <-time.After(5 * time.Second):
			f.t.Error("writer Release did not finish")
		}
	}
	return st
}
func (f *stagedRangeKernelFS) Release(c <-chan struct{}, in *gofuse.ReleaseIn) {
	if in.Fh == f.writer.Load() {
		<-f.releaseAllowed
		f.Dat9FS.Release(c, in)
		f.releaseOnce.Do(func() { close(f.releaseDone) })
		return
	}
	f.Dat9FS.Release(c, in)
}
func TestFtruncateStagedRangeKernel(t *testing.T) {
	if _, e := os.Stat("/dev/fuse"); e != nil {
		if os.Getenv("DRIVE9_REQUIRE_KERNEL_FUSE") == "1" {
			t.Fatal(e)
		}
		t.Skipf("Linux FUSE unavailable: %v", e)
	}
	nodeBin := os.Getenv("DRIVE9_TEST_NODE_BIN")
	if nodeBin == "" {
		nodeBin, _ = exec.LookPath("node")
	}
	if _, e := exec.LookPath("python3"); e != nil && nodeBin == "" {
		if os.Getenv("DRIVE9_REQUIRE_KERNEL_FUSE") == "1" {
			t.Fatal(e)
		}
		t.Skip("python3 unavailable")
	}
	for _, tc := range []struct {
		name, kind string
		size       int
	}{
		{"copy-large", "copy", 9 << 20}, {"copy-small", "copy", 512 << 10}, {"plain-write-large", "write", 9 << 20}, {"exclusive-copy-large", "exclusive", 9 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			remote := map[string][]byte{}
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				p := strings.TrimPrefix(r.URL.Path, "/v1/fs")
				switch {
				case r.URL.Path == "/v1/status":
					_ = json.NewEncoder(w).Encode(map[string]any{"inline_threshold": 1 << 20})
				case r.Method == http.MethodHead:
					mu.Lock()
					data, found := remote[p]
					mu.Unlock()
					if p == "/" {
						w.Header().Set("X-Dat9-IsDir", "true")
					} else if !found {
						w.WriteHeader(404)
						return
					}
					w.Header().Set("Content-Length", strconv.Itoa(len(data)))
					w.Header().Set("X-Dat9-Revision", "1")
				case r.Method == http.MethodPut:
					data, e := io.ReadAll(r.Body)
					if e != nil {
						w.WriteHeader(500)
						return
					}
					mu.Lock()
					remote[p] = data
					mu.Unlock()
					w.Header().Set("X-Dat9-Revision", "1")
					_ = json.NewEncoder(w).Encode(map[string]any{"revision": 1})
				case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
					_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
				case r.Method == http.MethodGet:
					mu.Lock()
					data, found := remote[p]
					mu.Unlock()
					if !found {
						w.WriteHeader(404)
						return
					}
					http.ServeContent(w, r, p, time.Now(), bytes.NewReader(data))
				case r.Method == http.MethodPost && (r.URL.Query().Has("chmod") || r.URL.Query().Has("chown") || r.URL.Query().Has("utimes")):
					w.WriteHeader(200)
				default:
					t.Logf("HTTP %s %s", r.Method, r.URL)
					w.WriteHeader(404)
				}
			}))
			t.Cleanup(ts.Close)
			opts := &MountOptions{Profile: MountProfileNone, LocalRoot: t.TempDir(), MountPoint: t.TempDir(), SyncMode: SyncStrict, WritePolicy: WritePolicyWriteBack, DirTTL: time.Second, AttrTTL: time.Second, EntryTTL: time.Second, Debug: false, DirectMountStrict: os.Geteuid() == 0}
			opts.setDefaults()
			fs := NewDat9FS(newTestClient(ts.URL), opts)
			fs.client.SetSmallFileThresholdForTests(1 << 20)
			var e error
			fs.shadowStore, e = NewShadowStore(t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(fs.shadowStore.Close)
			fs.pendingIndex, e = NewPendingIndex(t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			fs.writeBack, e = NewWriteBackCache(t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			fs.uploader = NewWriteBackUploader(fs.client, fs.writeBack, 1)
			t.Cleanup(fs.uploader.DrainAll)
			gate := make(chan struct{})
			var gateOnce sync.Once
			allowUpload := func() {
				gateOnce.Do(func() {
					// Observations use the production-sized inline threshold.
					// Only cleanup uses PUT so this fixture needs no multipart server.
					fs.client.SetSmallFileThresholdForTests(32 << 20)
					close(gate)
				})
			}
			cq := NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 16)
			cq.PathLock = func(p string) func() { <-gate; return fs.lockRemoteCommitPath(p) }
			cq.OnUploaded = fs.onCommitQueueUploaded
			cq.OnSuccess = fs.onCommitQueueSuccess
			cq.OnCleanup = fs.onCommitQueueCleanup
			fs.commitQueue = cq
			t.Cleanup(func() { allowUpload(); cq.DrainAll() })
			fs.dirCache.Put("/", []CachedFileInfo{})
			raw := &stagedRangeKernelFS{Dat9FS: fs, releaseAllowed: make(chan struct{}), releaseDone: make(chan struct{}), t: t}
			server, e := gofuse.NewServer(raw, opts.MountPoint, newGoFuseMountOptions(opts))
			if e != nil {
				if os.Getenv("DRIVE9_REQUIRE_KERNEL_FUSE") == "1" {
					t.Fatal(e)
				}
				t.Skipf("cannot mount Linux FUSE: %v", e)
			}
			go server.Serve()
			t.Cleanup(func() {
				raw.allowOnce.Do(func() { close(raw.releaseAllowed) })
				allowUpload()
				cq.DrainAll()
				fs.notifyWg.Wait()
				if e := server.Unmount(); e != nil {
					t.Error(e)
				}
			})
			if e = server.WaitMount(); e != nil {
				t.Fatal(e)
			}
			src := filepath.Join(t.TempDir(), "source")
			payload := bytes.Repeat([]byte("C"), tc.size)
			if e = os.WriteFile(src, payload, 0600); e != nil {
				t.Fatal(e)
			}
			dst := filepath.Join(opts.MountPoint, "target.bin")
			script := `import os,sys
src,dst,kind=sys.argv[1:]
data=open(src,'rb').read()
s=os.open(src,os.O_RDONLY)
d=os.open(dst,os.O_WRONLY|os.O_CREAT|(os.O_EXCL if kind=='exclusive' else 0),0o600)
if kind=='copy': os.ftruncate(d,0)
if kind=='write':
 n=0
 while n<len(data): n+=os.write(d,data[n:n+65536])
else:
 n=0
 while n<len(data): n+=os.sendfile(d,s,n,len(data)-n)
os.close(d);os.close(s)
try:
 fd=os.open(dst,os.O_RDONLY)
 try:
  chunks=[]
  while True:
   part=os.read(fd,512*1024)
   if not part: break
   chunks.append(part)
  got=b''.join(chunks)
 finally: os.close(fd)
 print('READ_OK',len(got),'MATCH',got==data,flush=True)
except OSError as e:
 print('READ_ERROR',e.errno,e.strerror,flush=True)
`
			command := exec.Command("python3", "-c", script, src, dst, tc.kind)
			if nodeBin != "" {
				nodeScript := `const fs = require('fs/promises');
const constants = require('fs').constants;
const [src, dst, kind] = process.argv.slice(1);
(async () => {
  if (kind === 'write') await fs.writeFile(dst, await fs.readFile(src));
  else await fs.copyFile(src, dst, kind === 'exclusive' ? constants.COPYFILE_EXCL : 0);
  try {
    const got = await fs.readFile(dst);
    console.log('READ_OK', got.length, 'MATCH', got.equals(await fs.readFile(src)) ? 'True' : 'False');
  } catch (error) {
    console.log('READ_ERROR', Math.abs(error.errno), error.code);
  }
})().catch(error => { console.error(error); process.exitCode = 1; });`
				command = exec.Command(nodeBin, "-e", nodeScript, src, dst, tc.kind)
			}
			result, err := command.CombinedOutput()
			t.Logf("USERSPACE %s err=%v", result, err)
			if err != nil {
				t.Fatal(err)
			}
			meta, exists := fs.pendingIndex.GetMeta("/target.bin")
			if exists {
				t.Logf("PENDING size=%d seq=%d owned=%t shadow=%d", meta.Size, meta.MutationSeq, meta.ownedStagingKnown, meta.ownedStagingGens.ShadowGen)
			}
			t.Logf("RESULT case=%s explicit_size_changes=%d output=%s", tc.name, raw.truncates.Load(), result)
			if !bytes.Contains(result, []byte(fmt.Sprintf("READ_OK %d MATCH True", tc.size))) {
				t.Errorf("read before upload failed: %s", result)
			}
		})
	}
}
