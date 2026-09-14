package fuse

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDirectorySnapshotAtomicInstallAfterCommit(t *testing.T) {
	for _, revision := range []int64{0, 2} {
		t.Run(fmt.Sprint(revision), func(t *testing.T) {
			opts := &MountOptions{}
			opts.setDefaults()
			fs := NewDat9FS(newTestClient("http://localhost"), opts)
			ino := fs.inodes.Lookup("/file.dat", false, 0, time.Now())
			local := fs.snapshotDirLocal("/")
			live, _ := fs.inodes.GetEntry(ino)
			candidate, preserve := fs.localDirEntryInfo("/file.dat", CachedFileInfo{Name: "file.dat"}, live, local)
			if preserve {
				t.Fatal("expected a clean remote candidate before commit")
			}
			fs.onCommitQueueSuccess(&CommitEntry{Path: "/file.dat", Inode: ino, Size: 4096}, revision)
			installed := fs.inodes.EnsureDirEntry("/file.dat", candidate, preserve)
			if installed.Size != 4096 || installed.Revision != revision {
				t.Fatalf("stale candidate overwrote commit: %+v", installed)
			}
		})
	}
}

func TestDirectorySnapshotPreservesNewInodeAndUnlink(t *testing.T) {
	m := NewInodeToPath()
	stale := CachedFileInfo{Name: "file.dat", observedVersion: m.AttrVersion()}
	ino := m.LookupWithIdentity("/file.dat", "resource-1", 2, false, 4096, time.Now())
	if got := m.EnsureDirEntry("/file.dat", stale, false); got.Ino != ino || got.Size != 4096 {
		t.Fatalf("listing predating inode creation overwrote it: %+v", got)
	}
	m.LookupWithIdentity("/alias.dat", "resource-1", 2, false, 4096, time.Now())
	live, _ := m.GetEntry(ino)
	stale = cachedInfoFromEntry("file.dat", live)
	m.RemoveLink("/alias.dat")
	if got := m.EnsureDirEntry("/file.dat", stale, false); got.Nlink != 1 {
		t.Fatalf("listing predating unlink restored stale link count: %+v", got)
	}
}

func TestReadDirPlusUnknownRevisionInFlightListAndFreshRemoteChange(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	var calls atomic.Int32
	var remoteSize atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		size := remoteSize.Load()
		switch {
		case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
			if calls.Add(1) == 1 {
				close(entered)
				<-release
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{map[string]any{"name": "file.dat", "size": size}}})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-stat":
			_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{map[string]any{"path": "/file.dat", "status": 200, "size": size}}})
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(ts.Close)
	t.Cleanup(unblock)
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.Lookup("/file.dat", false, 0, time.Now())
	done := make(chan uint64, 1)
	go func() { done <- readDirPlusAttrs(t, fs, 1, "/")["file.dat"].Size }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("remote listing did not reach the gate")
	}
	fs.onCommitQueueSuccess(&CommitEntry{Path: "/file.dat", Inode: ino, Size: 4096}, 0)
	unblock()
	select {
	case size := <-done:
		if size != 4096 {
			t.Fatalf("in-flight zero-revision listing published size %d", size)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("listing did not complete")
	}
	if got := fs.dirCache.Lookup("/", "file.dat").item.Size; got != 4096 {
		t.Fatalf("stale listing poisoned directory cache: size %d", got)
	}
	// A subsequent remote observation must still see legitimate external changes.
	remoteSize.Store(128)
	fs.dirCache.Invalidate("/")
	if got := readDirPlusAttrs(t, fs, 1, "/")["file.dat"].Size; got != 128 {
		t.Fatalf("fresh remote truncation size = %d, want 128", got)
	}
}

func TestReadDirPlusLoadedHandleRefreshesPendingGeneration(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	var err error
	fs.pendingIndex, err = NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.dirCache.Put("/", []CachedFileInfo{{Name: "file.dat"}})
	dh := &DirHandle{Ino: 1, Path: "/"}
	for _, size := range []int64{4096, 8192, 0} {
		gen, err := fs.pendingIndex.PutWithBaseRev("/file.dat", size, PendingOverwrite, 1)
		if err != nil {
			t.Fatal(err)
		}
		if got := readDirPlusHandleAttrs(t, fs, dh)["file.dat"].Size; got != uint64(size) {
			t.Fatalf("loaded handle size = %d, want latest pending size %d", got, size)
		}
		if fs.pendingIndex.Generation("/file.dat") != gen {
			t.Fatal("READDIRPLUS consumed pending generation")
		}
	}
}

func TestReadDirPlusColdPendingMetadata(t *testing.T) {
	for _, alias := range []bool{false, true} {
		t.Run(fmt.Sprint("alias=", alias), func(t *testing.T) {
			opts := &MountOptions{}
			opts.setDefaults()
			fs := NewDat9FS(newTestClient("http://localhost"), opts)
			pending, err := NewPendingIndex(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			fs.pendingIndex = pending
			if _, err := pending.PutWithBaseRevAndMode("/file.dat", 4096, PendingOverwrite, 3, 0o640, true); err != nil {
				t.Fatal(err)
			}
			var other uint64
			if alias {
				other = fs.inodes.LookupWithIdentity("/other.dat", "resource-1", 2, false, 4096, time.Now())
				fs.inodes.UpdateOwner(other, 123, 456, true, true)
				fs.inodes.UpdateRevision(other, 3)
				fs.inodes.UpdateMode(other, 0o640)
			}
			fs.dirCache.Put("/", []CachedFileInfo{{Name: "file.dat", ResourceID: "resource-1", Nlink: 2,
				Size: 0, Revision: 3, Uid: 123, Gid: 456, HasUID: true, HasGID: true, Mode: 0o640, HasMode: true}})
			attr := readDirPlusAttrs(t, fs, 1, "/")["file.dat"]
			if attr.Size != 4096 || attr.Uid != 123 || attr.Gid != 456 || attr.Nlink != 2 || attr.Mode&0o777 != 0o640 {
				t.Errorf("cold pending attributes = %+v", attr)
			}
			entry, ok := fs.inodes.GetEntry(attr.NodeId)
			if !ok || entry.ResourceID != "resource-1" || entry.Revision != 3 {
				t.Errorf("cold pending inode = %+v", entry)
			}
			if alias && attr.NodeId != other {
				t.Errorf("hardlink inode = %d, want %d", attr.NodeId, other)
			}
		})
	}
}

func TestDirectorySnapshotAfterCommitWithoutRevision(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	ino := fs.inodes.Lookup("/file.dat", false, 0, time.Now())
	fs.dirCache.Put("/", []CachedFileInfo{{Name: "file.dat", Size: 0}})
	stale, _ := fs.dirCache.Get("/")
	fs.onCommitQueueSuccess(&CommitEntry{Path: "/file.dat", Inode: ino, Size: 4096}, 0)
	entries := fs.cachedToDirEntries("/", stale)
	entry, _ := fs.inodes.GetEntry(ino)
	if len(entries) != 1 || entries[0].Size != 4096 || entry.Size != 4096 {
		t.Fatalf("stale zero-revision listing overwrote commit: entries=%+v inode=%+v", entries, entry)
	}
}

func TestDirectorySnapshotCommitDuringMetadataProbe(t *testing.T) {
	for _, revision := range []int64{0, 2} {
		t.Run(fmt.Sprint("revision=", revision), func(t *testing.T) {
			opts := &MountOptions{}
			opts.setDefaults()
			fs := NewDat9FS(newTestClient("http://localhost"), opts)
			wb, err := NewWriteBackCache(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			fs.writeBack = wb
			ino := fs.inodes.Lookup("/file.dat", false, 0, time.Now())
			pl := wb.acquirePathLock("/file.dat")
			released := false
			t.Cleanup(func() {
				if !released {
					wb.releasePathLock("/file.dat", pl)
				}
			})
			done := make(chan []DirEntry, 1)
			go func() { done <- fs.cachedToDirEntries("/", []CachedFileInfo{{Name: "file.dat"}}) }()
			deadline := time.Now().Add(5 * time.Second)
			for {
				wb.mu.Lock()
				waiting := pl.waiters > 1
				wb.mu.Unlock()
				if waiting {
					// Old code has already copied the inode before waiting here.
					fs.onCommitQueueSuccess(&CommitEntry{Path: "/file.dat", Inode: ino, Size: 4096}, revision)
					wb.releasePathLock("/file.dat", pl)
					released = true
					break
				}
				select {
				case <-done:
					// A nonblocking snapshot avoids this read/probe race entirely.
					return
				default:
				}
				if time.Now().After(deadline) {
					t.Fatal("metadata probe neither completed nor reached the held lock")
				}
				runtime.Gosched()
			}
			select {
			case entries := <-done:
				live, _ := fs.inodes.GetEntry(ino)
				if live.Size != 4096 || entries[0].Size != 4096 {
					t.Fatalf("commit lost during metadata probe: inode=%+v entries=%+v", live, entries)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("listing did not finish")
			}
		})
	}
}

func TestReadDirPlusDoesNotWaitForUnpublishedWriteBack(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	wb, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.writeBack = wb
	fs.dirCache.Put("/", []CachedFileInfo{{Name: "file.dat", Size: 4096}})
	pl := wb.acquirePathLock("/file.dat")
	done := make(chan struct{})
	go func() { defer close(done); readDirPlusAttrs(t, fs, 1, "/") }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Error("READDIRPLUS waits on a per-path write-back lock for a remote entry")
	}
	wb.releasePathLock("/file.dat", pl)
	<-done
}
