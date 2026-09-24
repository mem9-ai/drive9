package fuse

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestReadOnlyShadowFollowsOrdinaryCleanup(t *testing.T) {
	for _, configured := range []bool{false, true} {
		name := "ordinary"
		if configured {
			name = "configured"
		}
		t.Run(name, func(t *testing.T) {
			fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
				t.Errorf("fresh shadow must serve the read, got remote %s", r.Method)
				w.WriteHeader(http.StatusInternalServerError)
			})
			const path = "/file-wal"
			if configured {
				fs.appendLogMatcher = NewAppendLogMatcher([]string{"**/*-wal"})
			}
			ino := fs.inodes.Lookup(path, false, 5, time.Now())
			fs.inodes.UpdateRevision(ino, 1)
			if err := fs.shadowStore.WriteFull(path, []byte("first"), 1); err != nil {
				t.Fatal(err)
			}
			var out gofuse.OpenOut
			if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out); st != gofuse.OK {
				t.Fatal(st)
			}
			defer fs.Release(nil, &gofuse.ReleaseIn{Fh: out.Fh})
			reader, _ := fs.fileHandles.Get(out.Fh)
			if !reader.ShadowPinned {
				t.Fatal("reader must pin before commit")
			}
			oldGen := reader.ShadowGen
			fs.removeShadowPendingStagingGenerationLocked(nil, path, fs.shadowStore.ActiveGeneration(path), 0)
			fs.recordCommittedRevisionWithSize(path, 2, 5)
			if fs.shadowStore.SizeGen(oldGen) != 5 {
				t.Fatal("cleanup must retire the pinned generation")
			}
			// A later ordinary commit changes the path while the original
			// reader still holds the generation retired by the first commit.
			fs.recordCommittedRevisionWithSize(path, 3, 6)
			fs.inodes.UpdateRevision(ino, 3)
			fs.inodes.UpdateSize(ino, 6)
			if err := fs.shadowStore.WriteFull(path, []byte("second"), 3); err != nil {
				t.Fatal(err)
			}
			got, st, err := readDat9FSTestRange(fs, ino, out.Fh, 0, 20)
			if err != nil || st != gofuse.OK || string(got) != "second" {
				t.Fatalf("read=%q/%v/%v, want second", got, st, err)
			}
			if fs.shadowStore.SizeGen(oldGen) >= 0 {
				t.Fatal("obsolete retired pin leaked")
			}
		})
	}
}

func TestReadOnlyShadowAcrossTwoOrdinaryFlushes(t *testing.T) {
	var mu sync.Mutex
	var content []byte
	var revision int64
	fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			content, _ = io.ReadAll(r.Body)
			revision++
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": revision})
		case http.MethodGet:
			_, _ = w.Write(content)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	writer, flush := createCloseSyncShadowTestFile(t, fs, "file.bin", 0o644)
	var out gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: flush.InHeader}, &out); st != gofuse.OK {
		t.Fatal(st)
	}
	defer fs.Release(nil, &gofuse.ReleaseIn{Fh: out.Fh})
	reader, _ := fs.fileHandles.Get(out.Fh)
	if !reader.ShadowPinned {
		t.Fatal("reader must pin before first Flush")
	}
	for _, want := range [][]byte{[]byte("close-sync content"), bytes.Repeat([]byte("x"), 18)} {
		if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: flush.InHeader, Fh: flush.Fh}, want); st != gofuse.OK {
			t.Fatal(st)
		}
		if st := fs.Flush(nil, flush); st != gofuse.OK {
			t.Fatal(st)
		}
		fs.readCache.Invalidate(writer.Path)
		got, st, err := readDat9FSTestRange(fs, writer.Ino, out.Fh, 0, 30)
		if err != nil || st != gofuse.OK || !bytes.Equal(got, want) {
			t.Fatalf("read=%q/%v/%v want %q", got, st, err, want)
		}
	}
	if revision != 2 {
		t.Fatalf("commits=%d, want 2", revision)
	}
}

func TestReadOnlyShadowPinsNewWALBeforeCommit(t *testing.T) {
	fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("uncommitted WAL must read locally, got %s", r.Method)
		w.WriteHeader(http.StatusNotFound)
	})
	fs.appendLogMatcher = NewAppendLogMatcher([]string{"**/*-wal"})
	writer, flush := createCloseSyncShadowTestFile(t, fs, "new.db-wal", 0o644)
	var out gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: flush.InHeader}, &out); st != gofuse.OK {
		t.Fatal(st)
	}
	defer fs.Release(nil, &gofuse.ReleaseIn{Fh: out.Fh})
	reader, _ := fs.fileHandles.Get(out.Fh)
	if !reader.ShadowPinned {
		t.Fatal("new WAL staging was rejected as an unverified recovered cache")
	}
	// Also require the staged generation itself to work without a sibling
	// dirty-handle fallback hiding a rejected pin.
	fs.openHandles.Remove(writer)
	got, st, err := readDat9FSTestRange(fs, writer.Ino, out.Fh, 0, 30)
	if err != nil || st != gofuse.OK || string(got) != "close-sync content" {
		t.Fatalf("read=%q/%v/%v", got, st, err)
	}
}

func TestReadOnlyShadowMissDoesNotTouchDiskOrPathLock(t *testing.T) {
	fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("remote"))
	})
	const path = "/missing-wal"
	fs.appendLogMatcher = NewAppendLogMatcher([]string{"**/*-wal"})
	ino := fs.inodes.Lookup(path, false, 6, time.Now())
	fs.inodes.UpdateRevision(ino, 1)
	fs.recordCommittedRevisionWithSize(path, 1, 6)
	var out gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out); st != gofuse.OK {
		t.Fatal(st)
	}
	defer fs.Release(nil, &gofuse.ReleaseIn{Fh: out.Fh})
	// An orphan appearing after Open must not be opened or unlinked by Read.
	if err := os.WriteFile(fs.shadowStore.shadowPath(path), []byte("orphan"), 0o600); err != nil {
		t.Fatal(err)
	}
	pl := fs.shadowStore.acquirePathLock(path)
	unlock := sync.OnceFunc(func() { fs.shadowStore.releasePathLock(path, pl) })
	defer unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 3 {
			got, st, err := readDat9FSTestRange(fs, ino, out.Fh, 0, 6)
			if err != nil || st != gofuse.OK || string(got) != "remote" {
				t.Errorf("read=%q/%v/%v", got, st, err)
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		unlock()
		<-done
		t.Fatal("cache miss waited for a disk mutation's path lock")
	}
	if got, err := os.ReadFile(fs.shadowStore.shadowPath(path)); err != nil || string(got) != "orphan" {
		t.Fatalf("Read changed the disk orphan: %q/%v", got, err)
	}
}

func TestReadOnlyShadowPendingMetadataOwnsCurrentGeneration(t *testing.T) {
	fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("pending current image must serve the read locally")
		w.WriteHeader(http.StatusInternalServerError)
	})
	const path = "/pending-wal"
	fs.appendLogMatcher = NewAppendLogMatcher([]string{"**/*-wal"})
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
	reader, _ := fs.fileHandles.Get(out.Fh)
	oldGen := reader.ShadowGen
	fs.shadowStore.Remove(path)
	fs.recordCommittedRevisionWithSize(path, 3, 3)
	fs.inodes.UpdateRevision(ino, 3)
	if _, err := fs.pendingIndex.Put(path, 6, PendingOverwrite); err != nil {
		t.Fatal(err)
	}
	if err := fs.shadowStore.WriteFull(path, []byte("staged"), 1); err != nil {
		t.Fatal(err)
	}
	got, st, err := readDat9FSTestRange(fs, ino, out.Fh, 0, 20)
	if err != nil || st != gofuse.OK || string(got) != "staged" {
		t.Fatalf("pending read=%q/%v/%v", got, st, err)
	}
	if reader.ShadowGen == oldGen || fs.shadowStore.SizeGen(oldGen) >= 0 {
		t.Fatal("pending metadata revived the retired pin")
	}
}

func TestReadOnlyShadowRetirementInvalidatesPrefetchWithoutCommittedSize(t *testing.T) {
	fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("new"))
	})
	const path = "/prefetched.bin"
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
	reader, _ := fs.fileHandles.Get(out.Fh)
	prefetch := NewPrefetcher(fs.client, path, 3)
	ready := make(chan struct{})
	close(ready)
	prefetch.cache[0] = &prefetchBlock{data: []byte("old"), ready: ready}
	reader.Prefetch = prefetch
	fs.removeShadowPendingStagingGenerationLocked(nil, path, fs.shadowStore.ActiveGeneration(path), 0)
	// Ordinary commit publication need not include a size in committedSize.
	fs.recordCommittedRevision(path, 2)
	fs.inodes.UpdateRevision(ino, 2)
	got, st, err := readDat9FSTestRange(fs, ino, out.Fh, 0, 3)
	if err != nil || st != gofuse.OK || string(got) != "new" {
		t.Fatalf("read after invalidating shadow=%q/%v/%v, want new", got, st, err)
	}
}
