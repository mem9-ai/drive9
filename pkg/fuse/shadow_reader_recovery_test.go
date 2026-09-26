package fuse

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestReadOnlyShadowNewFileReplacesOrphan(t *testing.T) {
	for _, name := range []string{"plain.bin", "new.db-wal"} {
		for _, resident := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/resident=%t", name, resident), func(t *testing.T) {
				fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
					t.Errorf("new staging must read locally, got %s", r.Method)
					w.WriteHeader(http.StatusNotFound)
				})
				fs.appendLogMatcher = NewAppendLogMatcher([]string{"**/*-wal"})
				path := "/" + name
				if err := os.WriteFile(fs.shadowStore.shadowPath(path), []byte("torn old shadow"), 0o600); err != nil {
					t.Fatal(err)
				}
				if resident {
					fs.shadowStore.EnsureActiveGeneration(path)
				}
				var created gofuse.CreateOut
				if st := fs.Create(nil, &gofuse.CreateIn{InHeader: gofuse.InHeader{NodeId: 1}, Mode: 0o644}, name, &created); st != gofuse.OK {
					t.Fatal(st)
				}
				writer, _ := fs.fileHandles.Get(created.Fh)
				header := gofuse.InHeader{NodeId: created.NodeId}
				var out gofuse.OpenOut
				if st := fs.Open(nil, &gofuse.OpenIn{InHeader: header}, &out); st != gofuse.OK {
					t.Fatal(st)
				}
				defer fs.Release(nil, &gofuse.ReleaseIn{Fh: out.Fh})
				// Do not let the dirty sibling mask rejection of the actual
				// staging source. No remote commit has occurred yet.
				fs.openHandles.Remove(writer)
				got, st, err := readDat9FSTestRange(fs, writer.Ino, out.Fh, 0, 30)
				if err != nil || st != gofuse.OK || len(got) != 0 {
					t.Fatalf("read new empty file=%q/%v/%v", got, st, err)
				}
				data := []byte("close-sync content")
				if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: header, Fh: created.Fh}, data); st != gofuse.OK || int(n) != len(data) {
					t.Fatalf("write=%d/%v", n, st)
				}
				got, st, err = readDat9FSTestRange(fs, writer.Ino, out.Fh, 0, 30)
				if err != nil || st != gofuse.OK || string(got) != "close-sync content" {
					t.Fatalf("read new file=%q/%v/%v", got, st, err)
				}
			})
		}
	}
}

func TestReadOnlyShadowPlainPathRejectsUnverifiedRecovery(t *testing.T) {
	for _, resident := range []bool{false, true} {
		for _, revision := range []int64{0, 7} {
			t.Run(fmt.Sprintf("resident=%t/revision=%d", resident, revision), func(t *testing.T) {
				fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.WriteString(w, "remote image")
				})
				const path = "/plain.bin"
				ino := fs.inodes.Lookup(path, false, 12, time.Now())
				fs.inodes.UpdateRevision(ino, revision)
				if err := os.WriteFile(fs.shadowStore.shadowPath(path), []byte("torn"), 0o600); err != nil {
					t.Fatal(err)
				}
				if resident {
					fs.shadowStore.EnsureActiveGeneration(path)
				}
				var out gofuse.OpenOut
				if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out); st != gofuse.OK {
					t.Fatal(st)
				}
				defer fs.Release(nil, &gofuse.ReleaseIn{Fh: out.Fh})
				got, st, err := readDat9FSTestRange(fs, ino, out.Fh, 0, 30)
				if err != nil || st != gofuse.OK || string(got) != "remote image" {
					t.Fatalf("read recovered orphan=%q/%v/%v", got, st, err)
				}
			})
		}
	}
}

func TestReadOnlyShadowAfterCloseSyncAckAndRestart(t *testing.T) {
	var mu sync.Mutex
	var durable []byte
	handler := func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			durable, _ = io.ReadAll(r.Body)
			_, _ = io.WriteString(w, `{"revision":1}`)
		case http.MethodGet:
			_, _ = w.Write(durable)
		default:
			t.Errorf("unexpected request %s", r.Method)
			w.WriteHeader(http.StatusBadRequest)
		}
	}
	fs := newCloseSyncShadowTestFS(t, handler)
	writer, flush := createCloseSyncShadowTestFile(t, fs, "acked.bin", 0o644)
	if st := fs.Flush(nil, flush); st != gofuse.OK {
		t.Fatal(st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{Fh: flush.Fh})
	// Model a crash after the remote acknowledgement but before local
	// cleanup persists: only a truncated shadow survives, with no pending
	// metadata or current-process committed-revision history.
	dir := fs.shadowStore.dir
	fs.shadowStore.Close()
	if err := os.WriteFile(fs.shadowStore.shadowPath(writer.Path), []byte("close"), 0o600); err != nil {
		t.Fatal(err)
	}
	restarted := newCloseSyncShadowTestFS(t, handler)
	var err error
	restarted.shadowStore, err = NewShadowStoreWithQuota(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(restarted.shadowStore.Close)
	ino := restarted.inodes.Lookup(writer.Path, false, 18, time.Now())
	restarted.inodes.UpdateRevision(ino, 1)
	var out gofuse.OpenOut
	if st := restarted.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out); st != gofuse.OK {
		t.Fatal(st)
	}
	defer restarted.Release(nil, &gofuse.ReleaseIn{Fh: out.Fh})
	got, st, err := readDat9FSTestRange(restarted, ino, out.Fh, 0, 30)
	if err != nil || st != gofuse.OK || string(got) != "close-sync content" {
		t.Fatalf("read acknowledged bytes after restart=%q/%v/%v", got, st, err)
	}
}

func TestReadOnlyShadowPendingDiskRecovery(t *testing.T) {
	for _, afterOpen := range []bool{false, true} {
		t.Run(fmt.Sprintf("afterOpen=%t", afterOpen), func(t *testing.T) {
			fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, "remote")
			})
			const path = "/pending.bin"
			ino := fs.inodes.Lookup(path, false, 6, time.Now())
			fs.inodes.UpdateRevision(ino, 7)
			stage := func() {
				if err := os.WriteFile(fs.shadowStore.shadowPath(path), []byte("staged"), 0o600); err != nil {
					t.Fatal(err)
				}
				gen, err := fs.pendingIndex.PutShadowSpill(path, 6, PendingOverwrite, 6)
				if err != nil {
					t.Fatal(err)
				}
				fs.pendingIndex.recoverShadowSource(path, gen, fs.shadowStore)
			}
			if !afterOpen {
				stage()
			}
			var out gofuse.OpenOut
			if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out); st != gofuse.OK {
				t.Fatal(st)
			}
			defer fs.Release(nil, &gofuse.ReleaseIn{Fh: out.Fh})
			if afterOpen {
				stage()
			}
			got, st, err := readDat9FSTestRange(fs, ino, out.Fh, 0, 30)
			if err != nil || st != gofuse.OK || string(got) != "staged" {
				t.Fatalf("pending read=%q/%v/%v", got, st, err)
			}
			// Recovery authority can end without best-effort shadow cleanup.
			// The same reader must then reject the still-resident unknown pin.
			fs.pendingIndex.Remove(path)
			got, st, err = readDat9FSTestRange(fs, ino, out.Fh, 0, 30)
			if err != nil || st != gofuse.OK || string(got) != "remote" {
				t.Fatalf("read after pending removal=%q/%v/%v", got, st, err)
			}
		})
	}
}

func TestReadOnlyShadowFollowsInodeRevisionWithoutLocalCommit(t *testing.T) {
	fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "new")
	})
	const path = "/inode.bin"
	ino := fs.inodes.Lookup(path, false, 3, time.Now())
	fs.inodes.UpdateRevision(ino, 1)
	if err := fs.shadowStore.WriteFull(path, []byte("old"), 1); err != nil {
		t.Fatal(err)
	}
	var out gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out); st != gofuse.OK {
		t.Fatal(st)
	}
	defer fs.Release(nil, &gofuse.ReleaseIn{Fh: out.Fh})
	got, st, err := readDat9FSTestRange(fs, ino, out.Fh, 0, 3)
	if err != nil || st != gofuse.OK || string(got) != "old" {
		t.Fatalf("initial read=%q/%v/%v", got, st, err)
	}
	// Stat/revalidation can learn a peer's new revision without recording a
	// commit by this mount. The already-open reader must reject its old pin.
	fs.inodes.UpdateRevision(ino, 2)
	got, st, err = readDat9FSTestRange(fs, ino, out.Fh, 0, 3)
	if err != nil || st != gofuse.OK || string(got) != "new" {
		t.Fatalf("read after inode revision advanced=%q/%v/%v", got, st, err)
	}
}

func BenchmarkReadOnlyShadowRefresh(b *testing.B) {
	for _, resident := range []bool{false, true} {
		b.Run(fmt.Sprintf("resident=%t", resident), func(b *testing.B) {
			store, err := NewShadowStoreWithQuota(b.TempDir(), 0, 0)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(store.Close)
			pending, err := NewPendingIndex(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			writeBack, err := NewWriteBackCache(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			fs := &Dat9FS{shadowStore: store, pendingIndex: pending, writeBack: writeBack, inodes: NewInodeToPath()}
			const path = "/bench.bin"
			ino := fs.inodes.Lookup(path, false, 4, time.Now())
			fs.inodes.UpdateRevision(ino, 1)
			fh := &FileHandle{Path: path, Ino: ino, BaseRev: 1}
			if resident {
				if err := store.WriteFull(path, []byte("data"), 1); err != nil {
					b.Fatal(err)
				}
			}
			fs.openReadOnlyShadowLocked(fh)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				fs.refreshReadOnlyShadowLocked(fh)
			}
		})
	}
}
