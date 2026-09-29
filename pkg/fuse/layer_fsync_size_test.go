package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestLayerFsyncPublishesCurrentStagedSize(t *testing.T) {
	for _, scenario := range []string{"metadata", "journal", "publication_failure", "upload_failure", "path_lock_timeout"} {
		t.Run(scenario, func(t *testing.T) {
			var mu sync.Mutex
			var uploaded []byte
			var posts int
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodPost && r.URL.Path == "/v1/layers/layer-1/objects" {
					data, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					mu.Lock()
					posts++
					if scenario != "upload_failure" {
						uploaded = data
					}
					mu.Unlock()
					if scenario == "upload_failure" {
						http.Error(w, "injected upload failure", http.StatusInternalServerError)
						return
					}
					_ = json.NewEncoder(w).Encode(client.FSLayerEntry{LayerID: "layer-1", EntrySeq: 1})
					return
				}
				mu.Lock()
				data := bytes.Clone(uploaded)
				mu.Unlock()
				entry := client.FSLayerEntry{LayerID: "layer-1", Path: "/a", Op: "upsert", Kind: "file", SizeBytes: int64(len(data)), Content: data, EntrySeq: 1}
				switch r.URL.Path {
				case "/v1/layers/layer-1/diff":
					_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FSLayerEntry{entry}})
				case "/v1/layers/layer-1/entries":
					_ = json.NewEncoder(w).Encode(entry)
				default:
					http.NotFound(w, r)
				}
			}))
			defer ts.Close()
			shadows, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(shadows.Close)
			idx, err := NewPendingIndex(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			idx.setShadowStore(shadows)
			opts := &MountOptions{LayerRef: "layer-1", RemoteRoot: "/", WritePolicy: WritePolicyWriteBack}
			opts.setDefaults()
			fs := NewDat9FS(newTestClient(ts.URL), opts)
			fs.syncMode = SyncStrict
			fs.pendingIndex, fs.shadowStore = idx, shadows
			var journal *Journal
			if scenario != "metadata" {
				var err error
				journal, err = NewJournal(filepath.Join(t.TempDir(), "journal.wal"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = journal.Close() })
				idx.SetJournal(journal)
			}
			fs.commitQueue = NewCommitQueue(fs.client, shadows, idx, journal, 1, 8)
			fs.commitQueue.SetLayerRef("layer-1")
			fs.commitQueue.PathLock = fs.lockRemoteCommitPath
			t.Cleanup(fs.commitQueue.DrainAll)
			var created gofuse.CreateOut
			if st := fs.Create(nil, &gofuse.CreateIn{InHeader: gofuse.InHeader{NodeId: 1}, Flags: syscall.O_RDWR, Mode: 0600}, "a", &created); st != gofuse.OK {
				t.Fatal(st)
			}
			fh, _ := fs.fileHandles.Get(created.Fh)
			t.Cleanup(func() {
				fh.Lock()
				fs.releaseHandleRemoteCommitPathLocked(fh)
				fh.Unlock()
			})
			prefix := bytes.Repeat([]byte("p"), writeBackThreshold)
			tail := []byte("appended after closing a duplicate descriptor")
			want := append(bytes.Clone(prefix), tail...)
			write := func(offset uint64, data []byte) {
				t.Helper()
				if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh, Offset: offset}, data); st != gofuse.OK || int(n) != len(data) {
					t.Fatalf("Write: n=%d status=%v", n, st)
				}
			}
			write(0, prefix)
			// dup(2) shares one FUSE handle. Closing one descriptor issues Flush
			// without Release; the surviving descriptor can still append/fsync.
			if st := fs.Flush(nil, &gofuse.FlushIn{Fh: created.Fh}); st != gofuse.OK {
				t.Fatal(st)
			}
			staged, ok := idx.GetMeta("/a")
			if !ok || staged.Size != int64(len(prefix)) || !staged.ShadowSpill {
				t.Fatalf("duplicate close did not stage the initial image: %+v", staged)
			}
			write(uint64(len(prefix)), tail)
			if scenario == "publication_failure" {
				if err := journal.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "path_lock_timeout" {
				fh.Lock()
				fs.releaseHandleRemoteCommitPathLocked(fh)
				fh.Unlock()
				unlock := fs.lockRemoteCommitPath("/a")
				defer unlock()
				fs.opts.RemoteCommitWaitTimeout = time.Millisecond
			}
			st := fs.Fsync(nil, &gofuse.FsyncIn{Fh: created.Fh, InHeader: gofuse.InHeader{NodeId: created.NodeId}})
			mu.Lock()
			got, attempts := bytes.Clone(uploaded), posts
			mu.Unlock()
			meta, _ := idx.GetMeta("/a")
			if scenario == "publication_failure" || scenario == "upload_failure" || scenario == "path_lock_timeout" {
				if st == gofuse.OK || !fh.Dirty.HasDirtyParts() || meta == nil || meta.LayerClean {
					t.Fatalf("failed fsync lost dirty ownership: status=%v meta=%+v", st, meta)
				}
				if scenario == "publication_failure" && attempts != 0 {
					t.Errorf("uploaded %d times before publishing current recovery metadata", attempts)
				}
				if scenario == "path_lock_timeout" && (st != gofuse.Status(syscall.EAGAIN) || attempts != 0 || meta.Generation != staged.Generation) {
					t.Errorf("unfenced fsync published state: status=%v uploads=%d meta=%+v", st, attempts, meta)
				}
				if scenario == "upload_failure" && meta.Size != int64(len(want)) {
					t.Errorf("failed upload recovery size=%d, want %d", meta.Size, len(want))
				}
				return
			}
			if st != gofuse.OK || !bytes.Equal(got, want) {
				t.Fatalf("Fsync status=%v remote length=%d, want %d", st, len(got), len(want))
			}
			if meta == nil || meta.Size != int64(len(want)) || !meta.LayerClean || meta.Generation == staged.Generation || meta.LayerID != "layer-1" || meta.LayerEntrySeq != 1 || meta.shadowSource.generation != shadows.ActiveGeneration("/a") {
				t.Errorf("fsync acknowledged stale metadata: %+v; want size=%d and a new clean generation", meta, len(want))
			}
			if journal != nil {
				if err := journal.Close(); err != nil {
					t.Fatal(err)
				}
			}
			shadows.Close()
			cold, err := NewShadowStoreWithQuota(shadows.dir, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(cold.Close)
			recovered, err := NewPendingIndex(idx.dir)
			if err != nil {
				t.Fatal(err)
			}
			recovered.setShadowStore(cold)
			if err := recovered.RecoverFromDisk(); err != nil {
				t.Fatal(err)
			}
			if journal != nil {
				replayed, err := NewJournal(journal.path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = replayed.Close() })
				if err := replayJournalIntoPending(replayed, recovered, cold); err != nil {
					t.Fatal(err)
				}
			}
			coldFS := NewDat9FS(fs.client, opts)
			coldFS.pendingIndex, coldFS.shadowStore = recovered, cold
			coldFS.writeBack, err = NewWriteBackCache(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := restoreLayerEntries(context.Background(), fs.client, opts, cold, recovered, coldFS); err != nil {
				t.Fatal(err)
			}
			var node gofuse.EntryOut
			if st := coldFS.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "a", &node); st != gofuse.OK {
				t.Fatal(st)
			}
			var attr gofuse.AttrOut
			if st := coldFS.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: node.NodeId}}, &attr); st != gofuse.OK || attr.Size != uint64(len(want)) {
				t.Errorf("remount stat: status=%v size=%d, want %d", st, attr.Size, len(want))
			}
			var opened gofuse.OpenOut
			if st := coldFS.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: node.NodeId}}, &opened); st != gofuse.OK {
				t.Fatal(st)
			}
			defer coldFS.Release(nil, &gofuse.ReleaseIn{Fh: opened.Fh})
			data, status, err := readDat9FSTestRange(coldFS, node.NodeId, opened.Fh, 0, len(want))
			if err != nil || status != gofuse.OK || !bytes.Equal(data, want) {
				t.Errorf("remount read: status=%v err=%v bytes=%d, want %d", status, err, len(data), len(want))
			}
		})
	}
}
