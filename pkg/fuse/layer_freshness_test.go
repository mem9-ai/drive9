package fuse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestLayerRefreshReadsExternalMutation(t *testing.T) {
	for _, scenario := range []string{"update", "delete", "dirty_update", "dirty_delete", "different_layer_same_sequence"} {
		t.Run(scenario, func(t *testing.T) {
			var mu sync.Mutex
			entries := []client.FSLayerEntry{{LayerID: "layer-1", Path: "/a", Op: "upsert", Kind: "file", Content: []byte("old"), SizeBytes: 3, EntrySeq: 1}}
			fetches := 0
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/v1/layers/layer-1/diff":
					_ = json.NewEncoder(w).Encode(map[string]any{"entries": entries})
				case "/v1/layers/layer-1/entries":
					fetches++
					seq, _ := strconv.ParseInt(r.URL.Query().Get("max_seq"), 10, 64)
					for i := len(entries) - 1; i >= 0; i-- {
						if entries[i].EntrySeq <= seq {
							_ = json.NewEncoder(w).Encode(entries[i])
							return
						}
					}
					t.Errorf("unrecognized replay sequence %d", seq)
				case "/v1/layers/layer-1/events":
					_ = json.NewEncoder(w).Encode(map[string]any{"events": []client.FSLayerEvent{{LayerID: "layer-1", Seq: 2, Op: "upsert", Path: "/a"}}})
				default:
					http.NotFound(w, r)
				}
			}))
			defer ts.Close()
			idx, shadows := newLayerShutdownState(t)
			fs := NewDat9FS(newTestClient(ts.URL), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/", CacheSize: 1 << 20})
			fs.pendingIndex, fs.shadowStore = idx, shadows
			if err := restoreLayerEntries(context.Background(), fs.client, fs.opts, shadows, idx, fs); err != nil {
				t.Fatal(err)
			}
			var node gofuse.EntryOut
			if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "a", &node); st != gofuse.OK {
				t.Fatal(st)
			}
			var opened gofuse.OpenOut
			if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: node.NodeId}}, &opened); st != gofuse.OK {
				t.Fatal(st)
			}
			defer fs.Release(nil, &gofuse.ReleaseIn{Fh: opened.Fh})
			if got, st, err := readDat9FSTestRange(fs, node.NodeId, opened.Fh, 0, 3); err != nil || st != gofuse.OK || string(got) != "old" {
				t.Fatalf("initial read=%q %v %v", got, st, err)
			}
			dirty := scenario == "dirty_update" || scenario == "dirty_delete"
			if dirty {
				if err := shadows.WriteFull("/a", []byte("own"), 0); err != nil {
					t.Fatal(err)
				}
				if _, err := idx.Put("/a", 3, PendingOverwrite); err != nil {
					t.Fatal(err)
				}
			}
			mu.Lock()
			next := client.FSLayerEntry{LayerID: "layer-1", Path: "/a", Op: "upsert", Kind: "file", Content: []byte("new"), SizeBytes: 3, EntrySeq: 2}
			if scenario == "delete" || scenario == "dirty_delete" {
				next.Op = "whiteout"
			}
			if scenario == "different_layer_same_sequence" {
				next.LayerID, next.EntrySeq = "child", 1
				entries = nil
			}
			entries = append(entries, next)
			mu.Unlock()
			if _, err := refreshLayerEvents(context.Background(), fs.client, fs.opts, shadows, idx, fs, 1); err != nil {
				t.Fatal(err)
			}
			if scenario == "delete" {
				if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "a", &node); st != gofuse.ENOENT {
					t.Fatalf("external delete remained visible: %v", st)
				}
				if idx.HasPending("/a") || shadows.Has("/a") {
					t.Fatal("external delete retained a clean cache")
				}
				return
			}
			want := "new"
			if dirty {
				want = "own"
				if m, _ := idx.GetMeta("/a"); m == nil || m.LayerClean {
					t.Fatalf("dirty publication lost: %+v", m)
				}
			}
			if got, st, err := readDat9FSTestRange(fs, node.NodeId, opened.Fh, 0, 3); err != nil || st != gofuse.OK || string(got) != want {
				t.Fatalf("refreshed open-handle read=%q %v %v, want %q", got, st, err, want)
			}
			mu.Lock()
			before := fetches
			mu.Unlock()
			if err := restoreLayerEntries(context.Background(), fs.client, fs.opts, shadows, idx, fs); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			after := fetches
			mu.Unlock()
			if before != after {
				t.Fatalf("unchanged replay downloaded content again: %d -> %d", before, after)
			}
		})
	}
}

func TestLayerFlushRequiresCommitLock(t *testing.T) {
	for _, inherited := range []bool{false, true} {
		t.Run(strconv.FormatBool(inherited), func(t *testing.T) {
			var mu sync.Mutex
			posts := 0
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					http.NotFound(w, r)
					return
				}
				mu.Lock()
				posts++
				mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"layer_id":"layer-1","entry_seq":1}`))
			}))
			defer ts.Close()
			idx, shadows := newLayerShutdownState(t)
			fs := NewDat9FS(newTestClient(ts.URL), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/", WritePolicy: WritePolicyWriteSync, RemoteCommitWaitTimeout: time.Millisecond})
			fs.pendingIndex, fs.shadowStore = idx, shadows
			var created gofuse.CreateOut
			if st := fs.Create(nil, &gofuse.CreateIn{InHeader: gofuse.InHeader{NodeId: 1}, Flags: syscall.O_RDWR, Mode: 0600}, "a", &created); st != gofuse.OK {
				t.Fatal(st)
			}

			fh, _ := fs.fileHandles.Get(created.Fh)
			fh.Lock()
			fs.releaseHandleRemoteCommitPathLocked(fh)
			if inherited {
				fh.RemoteCommitUnlock = func() {} // inherited best-effort timeout escape
			}
			fh.Unlock()
			unlock := fs.lockRemoteCommitPath("/a")
			_, st := fs.Write(nil, &gofuse.WriteIn{Fh: created.Fh, InHeader: gofuse.InHeader{NodeId: created.NodeId}}, []byte("data"))
			unlock()
			if st != gofuse.Status(syscall.EAGAIN) {
				t.Errorf("flush without lock = %v, want EAGAIN", st)
			}
			mu.Lock()
			attempted := posts
			mu.Unlock()
			if attempted != 0 || idx.HasPending("/a") || shadows.Has("/a") {
				t.Fatal("unlocked flush uploaded or published a clean cache")
			}
			if st := fs.Flush(nil, &gofuse.FlushIn{Fh: created.Fh}); st != gofuse.OK {
				t.Fatalf("retry after lock release: %v", st)
			}
			fs.Release(nil, &gofuse.ReleaseIn{Fh: created.Fh})
			if m, _ := idx.GetMeta("/a"); m == nil || !m.LayerClean {
				t.Fatalf("retry did not publish durable data: %+v", m)
			}
		})
	}
}

func TestLayerRestoreCacheIdentityBoundaries(t *testing.T) {
	for _, scenario := range []string{"same", "legacy", "newer_remote", "newer_local", "checkpoint", "ancestor_pin", "historical_whiteout"} {
		t.Run(scenario, func(t *testing.T) {
			id := LayerCacheIdentity{LayerID: "layer-1", EntrySeq: 1}
			entry := client.FSLayerEntry{LayerID: "layer-1", Path: "/a", Op: "upsert", Kind: "file", EntrySeq: 1, Content: []byte("newer"), SizeBytes: 5}
			opts := &MountOptions{LayerRef: "layer-1", RemoteRoot: "/", CacheSize: 1 << 20}
			reuse := scenario == "same" || scenario == "newer_local" || scenario == "historical_whiteout"
			switch scenario {
			case "legacy":
				id = LayerCacheIdentity{}
			case "newer_remote":
				entry.EntrySeq = 2
			case "newer_local":
				id.EntrySeq = 2
			case "checkpoint":
				id.EntrySeq = 2
				opts.CheckpointRef, opts.ReadOnly = "cp1", true
			case "ancestor_pin":
				id.LayerID, id.EntrySeq, entry.LayerID = "parent", 9, "parent"
			case "historical_whiteout":
				id.EntrySeq, entry.EntrySeq = 3, 3
			}
			idx, shadows := newLayerShutdownState(t)
			if err := shadows.WriteFull("/a", []byte("cache"), 0); err != nil {
				t.Fatal(err)
			}
			if _, err := idx.PutLayerCache("/a", 5, 0, 0600, true, id); err != nil {
				t.Fatal(err)
			}
			// Reopen both stores to verify persisted identity and cold read binding.
			recovered, err := NewPendingIndex(idx.dir)
			if err != nil {
				t.Fatal(err)
			}
			shadows.Close()
			shadows, err = NewShadowStoreWithQuota(shadows.dir, 0, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(shadows.Close)
			recovered.setShadowStore(shadows)
			if err := recovered.RecoverFromDisk(); err != nil {
				t.Fatal(err)
			}
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/v1/layer-checkpoints/cp1":
					_ = json.NewEncoder(w).Encode(client.FSLayerCheckpoint{LayerID: "layer-1", CheckpointID: "cp1", DurableSeq: 1})
				case "/v1/layers/layer-1/diff":
					entries := []client.FSLayerEntry{entry}
					if scenario == "historical_whiteout" {
						entries = append([]client.FSLayerEntry{
							{LayerID: "layer-1", Path: "/a", Op: "upsert", Kind: "file", EntrySeq: 1},
							{LayerID: "layer-1", Path: "/a", Op: "whiteout", Kind: "file", EntrySeq: 2},
						}, entry)
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"entries": entries})
				case "/v1/layers/layer-1/entries":
					if reuse {
						t.Errorf("valid cache downloaded again: %s", r.URL)
					}
					_ = json.NewEncoder(w).Encode(entry)
				default:
					t.Errorf("unexpected request: %s", r.URL)
					http.NotFound(w, r)
				}
			}))
			defer ts.Close()
			fs := NewDat9FS(newTestClient(ts.URL), opts)
			fs.pendingIndex, fs.shadowStore = recovered, shadows
			if err := restoreLayerEntries(context.Background(), fs.client, opts, shadows, recovered, fs); err != nil {
				t.Fatal(err)
			}
			var node gofuse.EntryOut
			if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "a", &node); st != gofuse.OK {
				t.Fatal(st)
			}
			var opened gofuse.OpenOut
			if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: node.NodeId}}, &opened); st != gofuse.OK {
				t.Fatal(st)
			}
			defer fs.Release(nil, &gofuse.ReleaseIn{Fh: opened.Fh})
			want := "newer"
			if reuse {
				want = "cache"
			}
			if got, st, err := readDat9FSTestRange(fs, node.NodeId, opened.Fh, 0, 5); err != nil || st != gofuse.OK || string(got) != want {
				t.Fatalf("restored read=%q %v %v, want %q", got, st, err, want)
			}
		})
	}
}
