package fuse

import (
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestFtruncateReadOnlyRetiredPin(t *testing.T) {
	fs, ino, a, _, _ := newFtruncateCommitFS(t)
	if err := fs.shadowStore.WriteFull("/file.bin", []byte("hello world"), 1); err != nil {
		t.Fatal(err)
	}
	var ro gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDONLY)}, &ro); st != gofuse.OK {
		t.Fatal(st)
	}
	reader, _ := fs.fileHandles.Get(ro.Fh)
	if !reader.ShadowPinned {
		t.Fatal("reader must pin old shadow")
	}
	oldPin := reader.ShadowGen
	fs.shadowStore.Remove("/file.bin") // Retire the original reader generation.
	reviewFtruncate(t, fs, ino, a, 5)
	hardlinkSync(t, fs, ino, a)
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
	got, st, err := readDat9FSTestRange(fs, ino, ro.Fh, 0, 11)
	t.Logf("readonly_after_committed_truncate=%q status=%v err=%v inherited=%t expected=hello", got, st, err, reader.pendingFtruncate.Load() != nil)
	if err != nil || st != gofuse.OK || string(got) != "hello" {
		t.Fatal("readonly returned pre-truncate bytes")
	}
	if reader.ShadowPinned || reader.ShadowGen != 0 || fs.shadowStore.SizeGen(oldPin) >= 0 {
		t.Fatal("obsolete reader pin not released")
	}
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ro.Fh})
}
func TestFtruncateDirectOpenTruncVisible(t *testing.T) {
	fs, ino, a, b, _ := newFtruncateCommitFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	var c gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR | syscall.O_TRUNC)}, &c); st != gofuse.OK {
		t.Fatal(st)
	}
	successor, _ := fs.fileHandles.Get(c.Fh)
	got, st, err := readDat9FSTestRange(fs, ino, b, 0, 11)
	t.Logf("O_TRUNC_sibling_read=%q status=%v err=%v visible=%t expected=EOF/OK", got, st, err, successor.Dirty.visibleTruncate)
	if err != nil || st != gofuse.OK || len(got) != 0 || !successor.Dirty.visibleTruncate {
		t.Fatal("truncate successor must be immediately visible")
	}
}

func openFtruncateObserver(t *testing.T, fs *Dat9FS, ino uint64) uint64 {
	t.Helper()
	var out gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDONLY)}, &out); st != gofuse.OK {
		t.Fatal(st)
	}
	return out.Fh
}
func TestFtruncateObserversSuccessorLifecycle(t *testing.T) {
	for _, alias := range []bool{false, true} {
		for _, mode := range []string{"live", "queued", "failed", "committed"} {
			name := mode
			if alias {
				name += "-alias"
			}
			t.Run(name, func(t *testing.T) {
				var reject atomic.Bool
				fs, ino, a, b, _ := newFtruncateCommitFS(t, func(w http.ResponseWriter, _ *http.Request) bool {
					if reject.Load() {
						w.WriteHeader(http.StatusConflict)
						return true
					}
					return false
				})
				if alias {
					fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b})
					fs.inodes.AddAlias(ino, "/alias.bin", "shared", 2, false, 11, time.Now())
					var aliasOpen gofuse.OpenOut
					if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR)}, &aliasOpen); st != gofuse.OK {
						t.Fatal(st)
					}
					b = aliasOpen.Fh
				}
				ro := openFtruncateObserver(t, fs, ino)
				reviewFtruncate(t, fs, ino, a, 5)
				var c gofuse.OpenOut
				if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR | syscall.O_TRUNC)}, &c); st != gofuse.OK {
					t.Fatal(st)
				}
				for _, id := range []uint64{b, ro} {
					readHardlinkWant(t, fs, ino, id, "")
				}
				hardlinkWrite(t, fs, ino, c.Fh, "new")
				gate := make(chan struct{})
				var once sync.Once
				t.Cleanup(func() { once.Do(func() { close(gate) }) })
				switch mode {
				case "queued":
					original := fs.commitQueue.PathLock
					fs.commitQueue.PathLock = func(p string) func() { <-gate; return original(p) }
					fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: c.Fh})
				case "failed":
					reject.Store(true)
					if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: c.Fh}); st == gofuse.OK {
						t.Fatal("expected conflict")
					}
					fs.commitQueue.DrainAll()
					fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: c.Fh})
				case "committed":
					hardlinkSync(t, fs, ino, c.Fh)
					fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: c.Fh})
				}
				before := fs.shadowStore.ActiveGeneration("/file.bin")
				for _, id := range []uint64{b, ro} {
					readHardlinkWant(t, fs, ino, id, "new")
				}
				reader, _ := fs.fileHandles.Get(ro)
				if reader.Dirty != nil || reader.DirtySeq != 0 || reader.PendingIndexGen != 0 || reader.ShadowStageGen != 0 || reader.WriteBackGen != 0 {
					t.Fatal("reader gained write authority")
				}
				if fs.shadowStore.ActiveGeneration("/file.bin") != before {
					t.Fatal("reader cleaned writer staging")
				}
				once.Do(func() { close(gate) })
			})
		}
	}
}
func TestFtruncateObserverWarmCommittedReads(t *testing.T) {
	fs, ino, a, b, _ := newFtruncateCommitFS(t)
	ro := openFtruncateObserver(t, fs, ino)
	target, err := url.Parse(fs.client.BaseURL())
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead || r.Method == http.MethodGet {
			requests.Add(1)
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	fs.client = newTestClient(server.URL)
	fs.commitQueue.client = fs.client
	reviewFtruncate(t, fs, ino, a, 5)
	hardlinkSync(t, fs, ino, a)
	for _, want := range []string{"hello", "Hello", "Jello"} {
		if want != "hello" {
			hardlinkWrite(t, fs, ino, b, want[:1])
			hardlinkSync(t, fs, ino, b)
		}
		readHardlinkWant(t, fs, ino, ro, want)
		n := requests.Load()
		for i := 0; i < 20; i++ {
			readHardlinkWant(t, fs, ino, ro, want)
		}
		if got := requests.Load(); got != n {
			t.Fatalf("warm reads made %d HTTP calls", got-n)
		}
		t.Logf("%s: 20 warm reads, no additional HTTP", want)
	}
	// An observer alone cannot seed a fresh writable truncate epoch.
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b})
	if event := fs.openHandles.ftruncateInheritance(ino, "/file.bin"); event != nil {
		t.Fatal("readonly seeded writer inheritance")
	}
}

func TestFtruncateObserverLargeCommittedRange(t *testing.T) {
	fs, ino, a, _, _ := newFtruncateCommitFS(t)
	fs.client.SetSmallFileThresholdForTests(8 << 20)
	ro := openFtruncateObserver(t, fs, ino)
	reviewFtruncate(t, fs, ino, a, (2<<20)+7)
	// Large live views keep the existing lazy/range source.
	got, st, err := readDat9FSTestRange(fs, ino, ro, 0, 5)
	if err != nil || st != gofuse.OK || string(got) != "hello" {
		t.Fatalf("live range %q/%v/%v", got, st, err)
	}
	hardlinkSync(t, fs, ino, a)
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
	got, st, err = readDat9FSTestRange(fs, ino, ro, 2<<20, 10)
	if err != nil || st != gofuse.OK || string(got) != string(make([]byte, 7)) {
		t.Fatalf("large committed suffix %q/%v/%v", got, st, err)
	}
}

func TestFtruncateDirectOpenRejectionPublishesNothing(t *testing.T) {
	fs, ino, a, b, _ := newFtruncateCommitFS(t)
	reviewFtruncate(t, fs, ino, a, 5)
	hardlinkWrite(t, fs, ino, b, "H")
	before := len(fs.openHandles.SnapshotInode(ino))
	seq, _ := fs.pendingFtruncateSeq(ino)
	var c gofuse.OpenOut
	st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR | syscall.O_TRUNC)}, &c)
	if st != gofuse.EAGAIN || c.Fh != 0 || len(fs.openHandles.SnapshotInode(ino)) != before {
		t.Fatalf("rejected Open published handle: %v/%d", st, c.Fh)
	}
	after, _ := fs.pendingFtruncateSeq(ino)
	if after != seq {
		t.Fatal("rejected Open changed visible marker")
	}
}
