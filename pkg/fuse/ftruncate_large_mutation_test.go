package fuse

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestFtruncateLargeSettledAliasMutation(t *testing.T) {
	for _, queued := range []bool{true, false} {
		for _, appendMode := range []bool{false, true} {
			for _, holdSibling := range []bool{false, true} {
				for _, closeSource := range []bool{false, true} {
					t.Run(fmt.Sprintf("queue=%t/append=%t/hold=%t/closed=%t", queued, appendMode, holdSibling, closeSource), func(t *testing.T) {
						target := 2 << 20
						initial := bytes.Repeat([]byte("P"), target+4096)
						copy(initial, "hello world")
						fs, ino, remote := truncateHandoffFS(t, initial)
						queue := fs.commitQueue
						if !queued {
							fs.commitQueue = nil
							t.Cleanup(func() { fs.commitQueue = queue })
						}
						a := truncateHandoffOpen(t, fs, ino, syscall.O_RDWR)
						addTruncateHandoffAlias(t, fs, ino, remote)
						flags := syscall.O_RDWR
						if appendMode {
							flags |= syscall.O_APPEND
						}
						b := truncateHandoffOpen(t, fs, ino, flags)
						observer := truncateHandoffOpen(t, fs, ino, syscall.O_RDONLY)
						parent, _ := fs.fileHandles.Get(a)
						reader, _ := fs.fileHandles.Get(b)
						if parent.Path == reader.Path {
							t.Fatal("PREMISE expected different hardlink paths")
						}
						entered, allow := make(chan struct{}), make(chan struct{})
						var once sync.Once
						unblock := sync.OnceFunc(func() { close(allow) })
						remoteURL, err := url.Parse(fs.client.BaseURL())
						if err != nil {
							t.Fatal(err)
						}
						proxy := httputil.NewSingleHostReverseProxy(remoteURL)
						server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							if r.Method == http.MethodPut && r.URL.Path == "/v1/fs"+parent.Path {
								once.Do(func() { close(entered) })
								<-allow
							}
							proxy.ServeHTTP(w, r)
						}))
						t.Cleanup(server.Close)
						t.Cleanup(unblock)
						fs.client = newTestClient(server.URL)
						fs.client.SetSmallFileThresholdForTests(8 << 20)
						queue.client = fs.client
						truncateHandoffTruncate(t, fs, ino, a, target)
						finished := make(chan gofuse.Status, 1)
						go func() {
							finished <- fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
						}()
						select {
						case <-entered:
						case <-time.After(10 * time.Second):
							t.Fatal("source did not reach its data upload")
						}
						if holdSibling {
							reader.Lock()
						}
						unblock()
						select {
						case st := <-finished:
							if holdSibling {
								reader.Unlock()
							}
							if st != gofuse.OK {
								t.Fatalf("source Fsync=%v", st)
							}
						case <-time.After(10 * time.Second):
							if holdSibling {
								reader.Unlock()
							}
							t.Fatal("source finalization blocked on a clean sibling")
						}
						if closeSource {
							fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
						}
						queue.WaitPath(parent.Path)
						data, rev, _ := remote.snapshot()
						if len(data) != target || rev != 2 || !bytes.Equal(data, initial[:target]) {
							t.Fatalf("PREMISE actual remote size=%d rev=%d", len(data), rev)
						}
						if reader.DirtySeq != 0 || reader.Dirty.HasDirtyParts() {
							t.Fatal("PREMISE sibling is already dirty")
						}
						entry, _ := fs.inodes.GetEntry(ino)
						for path := range entry.Paths {
							if _, pending := fs.pendingIndex.GetMeta(path); pending || queue.HasPath(path) {
								t.Fatal("PREMISE unresolved staged or queued owner")
							}
						}
						for _, id := range []uint64{b, observer} {
							for _, offset := range []int{0, target - 8, target, target + 1} {
								got, st := truncateHandoffRead(fs, ino, id, int64(offset), 16)
								var want []byte
								if offset < target {
									want = data[offset:min(offset+16, target)]
								}
								if st != gofuse.OK || !bytes.Equal(got, want) {
									t.Fatalf("committed reader%d at%d=%x/%v want=%x", id, offset, got, st, want)
								}
							}
						}
						if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("H")); n != 1 || st != gofuse.OK {
							t.Fatalf("next Write=%d/%v", n, st)
						}
						if reader.pendingFtruncate.Load() != nil || reader.LineageTrusted || reader.ftruncateInherited != "" {
							t.Fatal("settled large baseline retained writable truncate ancestry")
						}
						hardlinkSync(t, fs, ino, b)
						want := bytes.Clone(initial[:target])
						if appendMode {
							want = append(want, 'H')
						} else {
							want[0] = 'H'
						}
						if data, rev, _ := remote.snapshot(); rev != 3 || !bytes.Equal(data, want) {
							t.Fatalf("next actual remote size=%d rev=%d want size=%d rev3", len(data), rev, len(want))
						}
						got, st := truncateHandoffRead(fs, ino, observer, 0, 16)
						if st != gofuse.OK || !bytes.Equal(got, want[:16]) {
							t.Fatalf("old observer after child commit=%x/%v want=%x", got, st, want[:16])
						}
						if !closeSource {
							fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
							queue.WaitPath(parent.Path)
							if data, _, _ := remote.snapshot(); !bytes.Equal(data, want) {
								t.Fatal("later source release overwrote accepted child")
							}
						}
					})
				}
			}
		}
	}
}
