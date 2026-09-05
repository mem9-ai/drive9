package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestAppendLogTailInvalidatesPrefetchedEOF(t *testing.T) {
	fs, fh, cleanup := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte("pretail"))
			return
		}
		_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 6, Size: 7})
	})
	defer cleanup()
	prepareAppendLogFinalizeTest(t, fs, fh, "append")
	prefetch := NewPrefetcher(nil, fh.Path, 3)
	defer prefetch.Close()
	ready := make(chan struct{})
	close(ready)
	prefetch.cache[0] = &prefetchBlock{data: []byte("pre"), ready: ready}
	reader := &FileHandle{Ino: fh.Ino, Path: fh.Path, Prefetch: prefetch}
	id := fs.allocateFileHandle(reader)
	defer fs.deleteFileHandle(id, reader)
	fh.Lock()
	result := fs.tryAppendLogLocked(context.Background(), fh)
	fh.Unlock()
	if result.status != gofuse.OK {
		t.Fatalf("append = %+v", result)
	}
	got, status, err := readDat9FSTestRange(fs, fh.Ino, id, 0, 7)
	if err != nil || status != gofuse.OK || string(got) != "pretail" {
		t.Fatalf("Read = %q/%d/%v, want pretail", got, status, err)
	}
	if prefetch.fileSize != 7 {
		t.Fatalf("prefetch size = %d, want 7", prefetch.fileSize)
	}
}

func TestAppendLogOrdinaryCommitDetachesRetiredReader(t *testing.T) {
	for _, route := range []string{"append", "rewrite"} {
		t.Run(route, func(t *testing.T) {
			want := []byte("pretail")
			if route == "rewrite" {
				want = []byte("rewrite")
			}
			fs, fh, cleanup := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					_, _ = w.Write(want)
					return
				}
				_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 6, Size: 7})
			})
			defer cleanup()
			prepareAppendLogFinalizeTest(t, fs, fh, route)
			shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer shadow.Close()
			fs.shadowStore = shadow
			if err := shadow.WriteFull(fh.Path, []byte("pre"), 5); err != nil {
				t.Fatal(err)
			}
			var out gofuse.OpenOut
			if status := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: fh.Ino}}, &out); status != gofuse.OK {
				t.Fatal(status)
			}
			reader, _ := fs.fileHandles.Get(out.Fh)
			defer fs.deleteFileHandle(out.Fh, reader)
			if !reader.ShadowPinned {
				t.Fatal("fixture reader did not pin shadow")
			}
			oldGen := reader.ShadowGen
			// A busy reader cannot be repaired by the publisher's TryLock loop.
			reader.Lock()
			fh.Lock()
			handled, status, _ := fs.routeAppendLogLocked(context.Background(), fh)
			fh.Unlock()
			reader.Unlock()
			if !handled || status != gofuse.OK {
				t.Fatalf("commit = %t/%d", handled, status)
			}
			var reads sync.WaitGroup
			for range 8 {
				reads.Add(1)
				go func() {
					defer reads.Done()
					got, status, err := readDat9FSTestRange(fs, fh.Ino, out.Fh, 0, 7)
					if err != nil || status != gofuse.OK || !bytes.Equal(got, want) {
						t.Errorf("Read after %s = %q/%d/%v, want %q", route, got, status, err, want)
					}
				}()
			}
			reads.Wait()
			if reader.ShadowPinned || reader.ShadowGen != 0 || shadow.SizeGen(oldGen) >= 0 {
				t.Error("obsolete reader pin was not released")
			}
		})
	}
}

func TestAppendLogCommitUpdatesInodeMtime(t *testing.T) {
	for _, route := range []string{"append", "rewrite", "reset"} {
		t.Run(route, func(t *testing.T) {
			fs, fh, cleanup := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 6, Size: 7})
			})
			defer cleanup()
			prepareAppendLogFinalizeTest(t, fs, fh, route)
			old := time.Unix(1, 0)
			fs.inodes.UpdateMtime(fh.Ino, old)
			fh.Lock()
			handled, status, _ := fs.routeAppendLogLocked(context.Background(), fh)
			fh.Unlock()
			if !handled || status != gofuse.OK {
				t.Fatal("commit failed")
			}
			entry, _ := fs.inodes.GetEntry(fh.Ino)
			cached := fs.dirCache.Lookup("/", "configured-log")
			if !entry.Mtime.After(old) || !entry.Mtime.Equal(cached.item.Mtime) {
				t.Errorf("inode/cache mtime = %v/%v, want same advanced timestamp", entry.Mtime, cached.item.Mtime)
			}
		})
	}
}

func TestAppendLogOrdinaryCommitPreservesEarlierResetSnapshot(t *testing.T) {
	fs, owner, cleanup := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
		case http.MethodPost:
			_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 7, Size: 36})
		default:
			t.Errorf("snapshot must not read remote: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	defer cleanup()
	h1 := prepareAppendLogFinalizeTest(t, fs, owner, "reset")
	oldImage := append([]byte(nil), owner.Dirty.Bytes()...)
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	fs.shadowStore = shadow
	if err := shadow.WriteFull(owner.Path, oldImage, owner.BaseRev); err != nil {
		t.Fatal(err)
	}
	var out gofuse.OpenOut
	if status := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: owner.Ino}}, &out); status != gofuse.OK {
		t.Fatal(status)
	}
	snapshotReader, _ := fs.fileHandles.Get(out.Fh)
	defer fs.deleteFileHandle(out.Fh, snapshotReader)
	oldGen := snapshotReader.ShadowGen
	owner.Lock()
	reset := fs.tryAppendLogGenerationResetLocked(context.Background(), owner)
	owner.Unlock()
	if reset.status != gofuse.OK {
		t.Fatalf("reset = %+v", reset)
	}
	writer := &FileHandle{Ino: owner.Ino, Path: owner.Path, BaseRev: 6, OrigSize: 32, DirtySeq: 2, Dirty: NewWriteBuffer(owner.Path, 1024, 0)}
	if _, err := writer.Dirty.Write(0, append(append([]byte(nil), h1...), []byte("next")...)); err != nil {
		t.Fatal(err)
	}
	writer.appendLogRecordUserWrite(32, 32, 4)
	writer.Lock()
	result := fs.tryAppendLogLocked(context.Background(), writer)
	writer.Unlock()
	if result.status != gofuse.OK {
		t.Fatalf("successor append = %+v", result)
	}
	got, status, err := readDat9FSTestRange(fs, owner.Ino, out.Fh, 0, len(oldImage))
	if err != nil || status != gofuse.OK || !bytes.Equal(got, oldImage) || snapshotReader.ShadowGen != oldGen {
		t.Fatalf("pre-reset snapshot changed: read=%x/%d/%v gen=%d", got, status, err, snapshotReader.ShadowGen)
	}
	fs.releaseHandleShadowPin(snapshotReader)
	if shadow.SizeGen(oldGen) >= 0 {
		t.Error("released snapshot generation leaked")
	}
}

func TestAppendLogRetiredPinPreservesUnlinkedSnapshot(t *testing.T) {
	fs, fh, cleanup := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unlinked snapshot must not access remote: %s", r.Method)
		w.WriteHeader(http.StatusInternalServerError)
	})
	defer cleanup()
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	fs.shadowStore = shadow
	if err := shadow.WriteFull(fh.Path, []byte("anonymous"), 5); err != nil {
		t.Fatal(err)
	}
	gen := shadow.Pin(fh.Path)
	reader := &FileHandle{Ino: fh.Ino, Path: fh.Path, ShadowPinned: true, ShadowGen: gen,
		Unlinked: true, UnlinkedSnapshot: true, UnlinkedData: []byte("anonymous")}
	id := fs.allocateFileHandle(reader)
	defer fs.deleteFileHandle(id, reader)
	shadow.removeAfterAppendLogCommit(fh.Path, 6)
	got, status, err := readDat9FSTestRange(fs, fh.Ino, id, 0, 9)
	if err != nil || status != gofuse.OK || string(got) != "anonymous" || reader.ShadowGen != gen {
		t.Fatalf("unlinked snapshot changed: %q/%d/%v gen=%d", got, status, err, reader.ShadowGen)
	}
	fs.releaseHandleShadowPin(reader)
}

func TestAppendLogLateFinalizerPreservesNewerCommit(t *testing.T) {
	for _, route := range []string{"append", "rewrite"} {
		t.Run(route, func(t *testing.T) {
			started := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			fs, fh, cleanup := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("X-Dat9-Expected-Revision") == "5" {
					close(started)
					<-release
					_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 6, Size: 7})
					return
				}
				if r.Method != http.MethodPost || r.Header.Get("X-Dat9-Expected-Revision") != "6" {
					t.Errorf("unexpected successor request %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 7, Size: 11})
			})
			defer cleanup()
			defer once.Do(func() { close(release) })
			want := append(prepareAppendLogFinalizeTest(t, fs, fh, route), []byte("next")...)
			done := make(chan gofuse.Status, 1)
			go func() {
				fh.Lock()
				_, status, _ := fs.routeAppendLogLocked(context.Background(), fh)
				fh.Unlock()
				done <- status
			}()
			<-started
			// Hold the existing handle lock across the predecessor's response,
			// so its local finalizer waits while a successor commits on the wire.
			fh.Lock()
			once.Do(func() { close(release) })
			unlockPath, ok := fs.lockRemoteCommitPathTimeout(fh.Path, time.Second)
			if !ok {
				fh.Unlock()
				t.Fatal("predecessor did not release path lock")
			}
			successor, err := fs.client.AppendLog(context.Background(), fh.Path, bytes.NewReader([]byte("next")), 4, 6, 7)
			if err != nil {
				unlockPath()
				fh.Unlock()
				t.Fatal(err)
			}
			fs.recordCommittedRevisionWithSize(fh.Path, successor.Revision, successor.Size)
			fh.BaseRev, fh.OrigSize = successor.Revision, successor.Size
			fh.appendLogMarkAppendSuccess(successor.Revision, successor.Size)
			_, _ = fh.Dirty.Write(7, []byte("next"))
			fh.Dirty.ClearDirty()
			fh.DirtySeq = 0
			fs.inodes.UpdateRevision(fh.Ino, successor.Revision)
			fs.inodes.UpdateSize(fh.Ino, successor.Size)
			stamp := time.Now()
			fs.inodes.UpdateMtime(fh.Ino, stamp)
			fs.cacheFileForPath(fh.Path, successor.Size, stamp, successor.Revision)
			fs.readCache.Put(fh.Path, want, successor.Revision)
			unlockPath()
			fh.Unlock()
			if status := <-done; status != gofuse.OK {
				t.Fatalf("predecessor status = %d", status)
			}
			entry, _ := fs.inodes.GetEntry(fh.Ino)
			cached := fs.dirCache.Lookup("/", "configured-log")
			if fh.BaseRev != 7 || fh.OrigSize != 11 || entry.Revision != 7 || cached.item.Revision != 7 || !cached.item.Mtime.Equal(stamp) {
				t.Errorf("superseded finalizer published stale state: handle=%d/%d inode=%d cache=%+v", fh.BaseRev, fh.OrigSize, entry.Revision, cached.item)
			}
			if got, ok := fs.readCache.Get(fh.Path, 7); !ok || !bytes.Equal(got, want) {
				t.Errorf("superseded finalizer replaced current read cache: %q/%t", got, ok)
			}
		})
	}
}
