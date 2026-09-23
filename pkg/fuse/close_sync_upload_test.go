package fuse

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func newCloseSyncShadowTestFS(t *testing.T, handler http.HandlerFunc) *Dat9FS {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	opts := &MountOptions{SyncMode: SyncStrict, WritePolicy: WritePolicyCloseSync}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(server.URL), opts)
	var err error
	fs.shadowStore, err = NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fs.shadowStore.Close)
	fs.pendingIndex, err = NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return fs
}

func createCloseSyncShadowTestFile(t *testing.T, fs *Dat9FS, name string, mode uint32) (*FileHandle, *gofuse.FlushIn) {
	t.Helper()
	var out gofuse.CreateOut
	if st := fs.Create(nil, &gofuse.CreateIn{InHeader: gofuse.InHeader{NodeId: 1}, Mode: mode}, name, &out); st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}
	data := []byte("close-sync content")
	if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh}, data); st != gofuse.OK || int(n) != len(data) {
		t.Fatalf("Write = %d, %v", n, st)
	}
	fh, ok := fs.fileHandles.Get(out.Fh)
	if !ok || !fh.ShadowSpill || fh.ShadowStageGen == 0 {
		t.Fatal("expected a generation-bound shadow handle")
	}
	return fh, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh}
}

func TestCloseSyncShadowUploadSkipsLocalSyncOnlyForCloseSync(t *testing.T) {
	for _, tc := range []struct {
		name       string
		policy     WritePolicy
		generation bool
	}{
		{"close-sync", WritePolicyCloseSync, true},
		{"write-sync", WritePolicyWriteSync, true},
		{"writeback", WritePolicyWriteBack, true},
		{"legacy-close-sync", WritePolicyCloseSync, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var puts atomic.Int32
			fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPut {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				puts.Add(1)
				body, _ := io.ReadAll(r.Body)
				if string(body) != "close-sync content" || r.Header.Get("X-Dat9-Expected-Revision") != "0" {
					t.Errorf("upload = %q, revision = %q", body, r.Header.Get("X-Dat9-Expected-Revision"))
				}
				_, _ = io.WriteString(w, `{"revision":1}`)
			})
			fh, _ := createCloseSyncShadowTestFile(t, fs, "sync.txt", 0o644)
			// Make Sync fail without damaging the on-disk bytes. Upload opens
			// its own fd, so this distinguishes local sync from remote commit.
			fs.shadowStore.mu.Lock()
			err := fs.shadowStore.files[fh.Path].fd.Close()
			fs.shadowStore.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			fh.Lock()
			fh.WritePolicy = tc.policy
			if !tc.generation {
				fh.ShadowStageGen = 0
			}
			st := fs.syncHandleToRemoteWithoutAppendLogLocked(context.Background(), fh)
			dirty := fh.Dirty.HasDirtyParts()
			fh.Unlock()
			if tc.policy == WritePolicyCloseSync && tc.generation {
				if st != gofuse.OK || puts.Load() != 1 || dirty {
					t.Fatalf("close-sync: status=%v puts=%d dirty=%t", st, puts.Load(), dirty)
				}
			} else if st == gofuse.OK || puts.Load() != 0 || !dirty {
				t.Fatalf("local sync failure: status=%v puts=%d dirty=%t", st, puts.Load(), dirty)
			}
		})
	}
}

func TestCloseSyncShadowUploadPinsGenerationUntilRemoteAck(t *testing.T) {
	started, allow := make(chan struct{}), make(chan struct{})
	var release sync.Once
	var fs *Dat9FS
	fs = newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.Method != http.MethodPut || string(body) != "close-sync content" {
			t.Errorf("unexpected upload: %s %q", r.Method, body)
		}
		fs.shadowStore.mu.RLock()
		pl := fs.shadowStore.pathLocks["/pinned.txt"]
		fs.shadowStore.mu.RUnlock()
		if pl == nil {
			t.Error("shadow path lock missing during upload")
		} else if pl.mu.TryLock() {
			pl.mu.Unlock()
			t.Error("shadow path lock released before remote acknowledgement")
		}
		close(started)
		<-allow
		_, _ = io.WriteString(w, `{"revision":1}`)
	})
	t.Cleanup(func() { release.Do(func() { close(allow) }) })
	fh, input := createCloseSyncShadowTestFile(t, fs, "pinned.txt", 0o644)
	done := make(chan gofuse.Status, 1)
	go func() { done <- fs.Flush(nil, input) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start")
	}
	select {
	case st := <-done:
		t.Fatalf("Flush returned before remote acknowledgement: %v", st)
	default:
	}
	release.Do(func() { close(allow) })
	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatalf("Flush: %v", st)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Flush did not finish")
	}
	if fh.Dirty.HasDirtyParts() || fh.BaseRev != 1 || fs.shadowStore.Has(fh.Path) {
		t.Fatal("successful upload did not finalize the committed generation")
	}
}

func TestCloseSyncShadowUploadFailureRetainsRetryableData(t *testing.T) {
	var puts atomic.Int32
	fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) {
		if puts.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = io.WriteString(w, `{"revision":1}`)
	})
	fh, input := createCloseSyncShadowTestFile(t, fs, "retry.txt", 0o644)
	gen := fs.shadowStore.ActiveGeneration(fh.Path)
	if st := fs.Flush(nil, input); st == gofuse.OK {
		t.Fatal("failed upload returned success")
	}
	if !fh.Dirty.HasDirtyParts() || fs.shadowStore.ActiveGeneration(fh.Path) != gen || !fh.IsNew {
		t.Fatal("failed upload changed dirty state or shadow ownership")
	}
	data, err := fs.shadowStore.ReadAll(fh.Path)
	if err != nil || string(data) != "close-sync content" {
		t.Fatalf("retry data = %q, %v", data, err)
	}
	if st := fs.Flush(nil, input); st != gofuse.OK || puts.Load() != 2 {
		t.Fatalf("retry: status=%v puts=%d", st, puts.Load())
	}
}

func TestShadowUploadWithoutLocalSyncRejectsStaleGeneration(t *testing.T) {
	var calls atomic.Int32
	fs := newCloseSyncShadowTestFS(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1) })
	fh, _ := createCloseSyncShadowTestFile(t, fs, "stale.txt", 0o644)
	gen := fs.shadowStore.ActiveGeneration(fh.Path)
	if _, err := fs.shadowStore.WriteAt(fh.Path, 0, []byte("new"), 0); err != nil {
		t.Fatal(err)
	}
	_, err := uploadFromShadowRemoteWithoutLocalSync(context.Background(), fs.client, fs.shadowStore, fh.Path, fh.Path, 0, gen)
	if !errors.Is(err, errCommitPayloadStale) || calls.Load() != 0 {
		t.Fatalf("stale upload: err=%v remote calls=%d", err, calls.Load())
	}
}
