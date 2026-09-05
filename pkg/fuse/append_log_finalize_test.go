package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

func prepareAppendLogFinalizeTest(t *testing.T, fs *Dat9FS, fh *FileHandle, route string) []byte {
	t.Helper()
	fs.appendLogMatcher = NewAppendLogMatcher([]string{"**/configured-log"})
	fh.Path = "/configured-log" // Exercise caches bypassed by the special -wal suffix.
	want := []byte("pretail")
	switch route {
	case "rewrite":
		want = []byte("rewrite")
		setAppendLogRewriteDirty(t, fh, string(want))
		fh.appendLogObserveLayout(client.ContentLayoutAppendLog, 5, 3)
	case "reset":
		h0, ok := parseSQLiteWALHeader(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2))
		if !ok {
			t.Fatal("invalid fixture H0")
		}
		want = makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
		setGenerationResetDirty(t, fh, h0, want, 128)
	}
	fh.Ino = fs.inodes.Lookup(fh.Path, false, fh.OrigSize, time.Now())
	fs.inodes.UpdateRevision(fh.Ino, fh.BaseRev)
	fs.markStatCacheVerified()
	return want
}

func TestAppendLogReplacementInvalidatesSiblingPrefetch(t *testing.T) {
	for _, route := range []string{"rewrite", "reset"} {
		for _, locked := range []bool{false, true} {
			name := route + "/unlocked"
			if locked {
				name = route + "/locked"
			}
			t.Run(name, func(t *testing.T) {
				fs, fh, cleanup := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
					if r.Method != http.MethodPut {
						t.Errorf("unexpected request %s", r.Method)
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
				})
				defer cleanup()
				want := prepareAppendLogFinalizeTest(t, fs, fh, route)
				prefetch := NewPrefetcher(nil, fs.remotePath(fh.Path), 2<<20)
				defer prefetch.Close()
				ready := make(chan struct{})
				close(ready)
				prefetch.cache[0] = &prefetchBlock{data: bytes.Repeat([]byte("x"), len(want)), ready: ready}
				prefetch.cache[int64(len(want))] = &prefetchBlock{offset: int64(len(want)), data: []byte("old tail"), ready: ready}
				oldCtx := prefetch.ctx
				reader := &FileHandle{Ino: fh.Ino, Path: fh.Path, BaseRev: 5, Prefetch: prefetch}
				readerID := fs.allocateFileHandle(reader)
				defer fs.deleteFileHandle(readerID, reader)
				if locked {
					reader.Lock()
				}
				fh.Lock()
				handled, status, _ := fs.routeAppendLogLocked(context.Background(), fh)
				fh.Unlock()
				if locked {
					reader.Unlock()
				}
				if !handled || status != gofuse.OK {
					t.Fatalf("commit = %t/%d", handled, status)
				}
				got, status, err := readDat9FSTestRange(fs, fh.Ino, readerID, 0, len(want))
				if err != nil || status != gofuse.OK || !bytes.Equal(got, want) {
					t.Errorf("post-commit Read = %q/%d/%v, want %q", got, status, err, want)
				}
				got, status, err = readDat9FSTestRange(fs, fh.Ino, readerID, int64(len(want)), 8)
				if err != nil || status != gofuse.OK || len(got) != 0 {
					t.Errorf("post-commit EOF = %q/%d/%v", got, status, err)
				}
				if prefetch.fileSize != int64(len(want)) || oldCtx.Err() == nil {
					t.Errorf("prefetch size/context = %d/%v, want %d/canceled", prefetch.fileSize, oldCtx.Err(), len(want))
				}
			})
		}
	}
}

func TestAppendLogReplacementCancelsInflightSiblingPrefetch(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	fs, fh, cleanup := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			close(started)
			<-release
			_, _ = w.Write(bytes.Repeat([]byte("x"), 64))
			return
		}
		if r.Method != http.MethodPut {
			t.Errorf("unexpected request %s", r.Method)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
	})
	defer cleanup()
	defer releaseOnce.Do(func() { close(release) })
	prepareAppendLogFinalizeTest(t, fs, fh, "reset")
	prefetch := NewPrefetcher(fs.client, fs.remotePath(fh.Path), 128)
	defer prefetch.Close()
	reader := &FileHandle{Ino: fh.Ino, Path: fh.Path, Prefetch: prefetch}
	id := fs.allocateFileHandle(reader)
	defer fs.deleteFileHandle(id, reader)
	prefetch.mu.Lock()
	prefetch.readSize = 64
	prefetch.startPrefetch(0, 64)
	oldBlock := prefetch.cache[0]
	oldCtx := prefetch.ctx
	prefetch.mu.Unlock()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("prefetch request did not start")
	}
	fh.Lock()
	result := fs.tryAppendLogGenerationResetLocked(context.Background(), fh)
	fh.Unlock()
	if result.status != gofuse.OK {
		t.Fatalf("reset = %+v", result)
	}
	if oldCtx.Err() == nil {
		t.Error("old-generation fetch was not canceled")
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-oldBlock.ready:
	case <-time.After(time.Second):
		t.Fatal("old fetch did not drain")
	}
	if got, ok := prefetch.Get(0, 32); ok {
		t.Errorf("late old prefetch was served: %q", got)
	}
}

func TestAppendLogReleaseRetriesOnlyPendingMode(t *testing.T) {
	for _, route := range []string{"append", "rewrite", "reset"} {
		for _, persistentFailure := range []bool{false, true} {
			name := route + "/transient"
			if persistentFailure {
				name = route + "/persistent"
			}
			t.Run(name, func(t *testing.T) {
				contentCalls, modeCalls := 0, 0
				failMode := true
				fs, fh, cleanup := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
					switch {
					case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
						modeCalls++
						var request struct{ Mode uint32 }
						if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Mode != 0o600 {
							t.Errorf("chmod payload = %+v/%v, want mode 600", request, err)
						}
						if failMode {
							w.WriteHeader(http.StatusInternalServerError)
							if !persistentFailure {
								failMode = false
							}
						}
					case r.Method == http.MethodPost && r.URL.Query().Has("append-log"):
						contentCalls++
						_ = json.NewEncoder(w).Encode(client.AppendLogResult{Revision: 6, Size: 7})
					case r.Method == http.MethodPut:
						contentCalls++
						_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
					default:
						t.Errorf("unexpected request %s %s", r.Method, r.URL)
						w.WriteHeader(http.StatusInternalServerError)
					}
				})
				defer cleanup()
				want := prepareAppendLogFinalizeTest(t, fs, fh, route)
				fs.inodes.UpdateMode(fh.Ino, 0o644)
				fs.setPendingModeLocked(fh, 0o600, 1)
				// Preserve retry ownership on a still-open same-inode handle.
				sibling := &FileHandle{Ino: fh.Ino, Path: fh.Path, BaseRev: 5}
				fs.setPendingModeLocked(sibling, 0o600, fh.PendingModeGen)
				siblingID := fs.allocateFileHandle(sibling)
				defer fs.deleteFileHandle(siblingID, sibling)
				id := fs.allocateFileHandle(fh)
				fs.Release(nil, &gofuse.ReleaseIn{Fh: id, InHeader: gofuse.InHeader{NodeId: fh.Ino}})
				if contentCalls != 1 || fh.BaseRev != 6 || fh.DirtySeq != 0 || !bytes.Equal(fh.Dirty.Bytes(), want) {
					t.Fatalf("content changed during mode failure: calls=%d rev=%d seq=%d", contentCalls, fh.BaseRev, fh.DirtySeq)
				}
				if modeCalls != 2 {
					t.Errorf("chmod calls = %d, want initial attempt and Release retry", modeCalls)
				}
				if fh.HasPendingMode != persistentFailure || sibling.HasPendingMode != persistentFailure {
					t.Errorf("pending mode owner/sibling = %t/%t, want %t", fh.HasPendingMode, sibling.HasPendingMode, persistentFailure)
				}
				if _, ok := fs.fileHandles.Get(id); ok {
					t.Error("released handle still registered")
				}
				if persistentFailure {
					failMode = false
					sibling.Lock()
					status := fs.syncHandleToRemoteLocked(context.Background(), sibling)
					sibling.Unlock()
					if status != gofuse.OK || sibling.HasPendingMode || modeCalls != 3 || contentCalls != 1 {
						t.Errorf("sibling mode retry = %d pending=%t chmod=%d content=%d", status, sibling.HasPendingMode, modeCalls, contentCalls)
					}
				}
			})
		}
	}
}

func TestAppendLogGenerationResetNotifiesKernelAfterCommit(t *testing.T) {
	for _, test := range []struct {
		name     string
		failPut  bool
		failMode bool
	}{
		{name: "success"},
		{name: "put-failure", failPut: true},
		{name: "post-commit-mode-failure", failMode: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fs, fh, cleanup := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
				if test.failMode && r.Method == http.MethodPost && r.URL.Query().Has("chmod") {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				if r.Method != http.MethodPut {
					t.Errorf("unexpected request %s", r.Method)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				_, _ = io.Copy(io.Discard, r.Body)
				if test.failPut {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]int64{"revision": 6})
			})
			defer cleanup()
			prepareAppendLogFinalizeTest(t, fs, fh, "reset")
			if test.failMode {
				fs.setPendingModeLocked(fh, 0o600, 1)
			}
			before := fs.notifyCount.Load()
			fh.Lock()
			result := fs.tryAppendLogGenerationResetLocked(context.Background(), fh)
			fh.Unlock()
			wantNotify := int64(1)
			if test.failPut {
				wantNotify = 0
				if result.status == gofuse.OK || fh.DirtySeq == 0 {
					t.Fatalf("failed reset lost dirty state: %+v seq=%d", result, fh.DirtySeq)
				}
			} else if (result.status == gofuse.OK) == test.failMode || fh.OrigSize != sqliteWALHeaderSize {
				t.Fatalf("reset = %+v size=%d", result, fh.OrigSize)
			}
			if got := fs.notifyCount.Load() - before; got != wantNotify {
				t.Errorf("kernel notifications = %d, want %d", got, wantNotify)
			}
		})
	}
}

func TestAppendLogReleaseDoesNotRetryModeAfterContentFailure(t *testing.T) {
	for _, route := range []string{"append", "rewrite", "reset"} {
		t.Run(route, func(t *testing.T) {
			contentCalls, modeCalls := 0, 0
			fs, fh, cleanup := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Has("chmod") {
					modeCalls++
				} else {
					contentCalls++
				}
				w.WriteHeader(http.StatusInternalServerError)
			})
			defer cleanup()
			prepareAppendLogFinalizeTest(t, fs, fh, route)
			fs.setPendingModeLocked(fh, 0o600, 1)
			id := fs.allocateFileHandle(fh)
			fs.Release(nil, &gofuse.ReleaseIn{Fh: id})
			if contentCalls != 1 || modeCalls != 0 || fh.DirtySeq == 0 || fh.BaseRev != 5 {
				t.Fatalf("failed content: content=%d chmod=%d seq=%d rev=%d", contentCalls, modeCalls, fh.DirtySeq, fh.BaseRev)
			}
		})
	}
}
