package fuse

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestLayerReleaseWaitsForContendedCommitAndSurvivesRemount(t *testing.T) {
	for _, policy := range []WritePolicy{WritePolicyCloseSync, WritePolicyWriteSync} {
		t.Run(string(policy), func(t *testing.T) {
			var mu sync.Mutex
			var uploaded []byte
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					mu.Lock()
					data := append([]byte(nil), uploaded...)
					mu.Unlock()
					entry := client.FSLayerEntry{LayerID: "layer-1", Path: "/a", Op: "upsert", Kind: "file", Content: data, SizeBytes: int64(len(data)), EntrySeq: 1}
					w.Header().Set("Content-Type", "application/json")
					switch r.URL.Path {
					case "/v1/layers/layer-1/diff":
						_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FSLayerEntry{entry}})
					case "/v1/layers/layer-1/entries":
						_ = json.NewEncoder(w).Encode(entry)
					default:
						http.NotFound(w, r)
					}
					return
				}
				var data []byte
				switch r.URL.Path {
				case "/v1/layers/layer-1/entries":
					var req client.FSLayerEntryRequest
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Error(err)
						w.WriteHeader(400)
						return
					}
					data = req.Content
				case "/v1/layers/layer-1/objects":
					var err error
					data, err = io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
						w.WriteHeader(400)
						return
					}
				default:
					http.NotFound(w, r)
					return
				}
				if data != nil {
					mu.Lock()
					uploaded = append([]byte(nil), data...)
					mu.Unlock()
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"layer_id":"layer-1","entry_seq":1}`))
			}))
			defer ts.Close()
			idx, shadows := newLayerShutdownState(t)
			fs := NewDat9FS(newTestClient(ts.URL), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/", WritePolicy: policy, RemoteCommitWaitTimeout: time.Millisecond})
			fs.pendingIndex, fs.shadowStore = idx, shadows
			fs.commitQueue = NewCommitQueue(fs.client, shadows, idx, nil, 1, 8)
			fs.commitQueue.SetLayerRef("layer-1")
			fs.commitQueue.PathLock = fs.lockRemoteCommitPath
			t.Cleanup(fs.commitQueue.DrainAll)
			var created gofuse.CreateOut
			if st := fs.Create(nil, &gofuse.CreateIn{InHeader: gofuse.InHeader{NodeId: 1}, Flags: syscall.O_RDWR, Mode: 0600}, "a", &created); st != gofuse.OK {
				t.Fatal(st)
			}
			fh, _ := fs.fileHandles.Get(created.Fh)
			fh.Lock()
			fs.releaseHandleRemoteCommitPathLocked(fh)
			fh.Unlock()
			unlock := fs.lockRemoteCommitPath("/a")
			var once sync.Once
			unlockPath := func() { once.Do(unlock) }
			defer unlockPath()
			if policy == WritePolicyWriteSync {
				// Seed an existing dirty buffer to exercise Release's final retry.
				// A failed write-sync Write normally rolls back its unaccepted bytes.
				fh.Lock()
				_, err := fh.Dirty.Write(0, []byte("keep me"))
				fh.DirtySeq = fs.markDirtySize(fh.Ino, fh.Dirty.Size())
				fh.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			} else if _, st := fs.Write(nil, &gofuse.WriteIn{Fh: created.Fh, InHeader: gofuse.InHeader{NodeId: created.NodeId}}, []byte("keep me")); st != gofuse.OK {
				t.Fatalf("Write=%v", st)
			}
			if st := fs.Flush(nil, &gofuse.FlushIn{Fh: created.Fh}); st != gofuse.Status(syscall.EAGAIN) {
				t.Fatalf("Flush=%v, want EAGAIN", st)
			}
			done := make(chan struct{})
			go func() {
				fs.Release(nil, &gofuse.ReleaseIn{Fh: created.Fh})
				close(done)
			}()
			select {
			case <-done:
				t.Fatal("Release discarded the dirty handle while its path was still locked")
			case <-time.After(20 * time.Millisecond):
			}
			// A commit completion may need fh.mu before releasing the path lock.
			// Release must not invert that order while waiting for the path.
			callback := make(chan int64, 1)
			go func() {
				fh.Lock()
				size := int64(-1)
				if fh.Dirty != nil {
					size = fh.Dirty.Size()
				}
				fh.Unlock()
				callback <- size
			}()
			select {
			case size := <-callback:
				if size != 7 {
					t.Fatalf("waiting Release lost its dirty buffer: %d bytes", size)
				}
			case <-time.After(time.Second):
				unlockPath()
				t.Fatal("Release held the handle lock while waiting for the commit lock")
			}
			unlockPath()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("Release did not complete after the path lock was released")
			}
			mu.Lock()
			got := string(uploaded)
			mu.Unlock()
			if got != "keep me" {
				t.Fatalf("remote bytes=%q", got)
			}
			shadows.Close()
			cold, err := NewShadowStoreWithQuota(shadows.dir, 0, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(cold.Close)
			recovered, err := NewPendingIndex(idx.dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := recovered.RecoverFromDisk(); err != nil {
				t.Fatal(err)
			}
			coldFS := NewDat9FS(fs.client, fs.opts)
			coldFS.pendingIndex, coldFS.shadowStore = recovered, cold
			recovered.setShadowStore(cold)
			if err := restoreLayerEntries(context.Background(), fs.client, fs.opts, cold, recovered, coldFS); err != nil {
				t.Fatal(err)
			}
			meta, ok := recovered.GetMeta("/a")
			if !ok || !meta.LayerClean || recovered.recoverShadowSource("/a", meta.Generation, cold) == 0 {
				t.Fatalf("release cache not recoverable after restart: %+v", meta)
			}
			data, err := cold.ReadAll("/a")
			if err != nil || string(data) != "keep me" {
				t.Fatalf("recovered bytes=%q, err=%v", data, err)
			}
		})
	}
}
