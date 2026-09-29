package fuse

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestLayerRefreshWritableHandle(t *testing.T) {
	for _, scenario := range []string{"update", "empty", "delete", "dirty_update", "dirty_delete", "staged_update", "busy_handle"} {
		t.Run(scenario, func(t *testing.T) {
			var mu sync.Mutex
			entries := []client.FSLayerEntry{{LayerID: "layer-1", Path: "/a", Op: "upsert", Kind: "file", Content: []byte("old"), SizeBytes: 3, EntrySeq: 1}}
			posts := 0
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/v1/layers/layer-1/diff":
					_ = json.NewEncoder(w).Encode(map[string]any{"entries": entries})
				case "/v1/layers/layer-1/events":
					_ = json.NewEncoder(w).Encode(map[string]any{"events": []client.FSLayerEvent{{Seq: 2, Path: "/a", Op: "upsert"}}})
				case "/v1/layers/layer-1/entries":
					if r.Method == http.MethodPost {
						var req client.FSLayerEntryRequest
						if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
							t.Error(err)
							w.WriteHeader(400)
							return
						}
						posts++
						if req.BaseRevision != 0 {
							t.Errorf("Layer-local rewrite claimed base revision %d", req.BaseRevision)
						}
						entry := client.FSLayerEntry{LayerID: "layer-1", Path: req.Path, Op: req.Op, Kind: req.Kind, Content: req.Content, SizeBytes: req.SizeBytes, EntrySeq: int64(len(entries) + 1)}
						entries = append(entries, entry)
						_ = json.NewEncoder(w).Encode(entry)
						return
					}
					seq, _ := strconv.ParseInt(r.URL.Query().Get("max_seq"), 10, 64)
					for i := len(entries) - 1; i >= 0; i-- {
						if entries[i].EntrySeq <= seq {
							_ = json.NewEncoder(w).Encode(entries[i])
							return
						}
					}
					w.WriteHeader(404)
				default:
					http.NotFound(w, r)
				}
			}))
			defer ts.Close()
			idx, shadows := newLayerShutdownState(t)
			fs := NewDat9FS(newTestClient(ts.URL), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/", CacheSize: 1 << 20, SyncMode: SyncStrict})
			fs.pendingIndex, fs.shadowStore = idx, shadows
			var err error
			fs.writeBack, err = NewWriteBackCache(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := restoreLayerEntries(context.Background(), fs.client, fs.opts, shadows, idx, fs); err != nil {
				t.Fatal(err)
			}
			var node gofuse.EntryOut
			if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "a", &node); st != gofuse.OK {
				t.Fatal(st)
			}
			var opened gofuse.OpenOut
			if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: node.NodeId}, Flags: syscall.O_RDWR}, &opened); st != gofuse.OK {
				t.Fatal(st)
			}
			fh, _ := fs.fileHandles.Get(opened.Fh)
			defer fs.Release(nil, &gofuse.ReleaseIn{Fh: opened.Fh})
			dirty := scenario == "dirty_update" || scenario == "dirty_delete" || scenario == "staged_update"
			if dirty {
				if _, st := fs.Write(nil, &gofuse.WriteIn{Fh: opened.Fh, InHeader: gofuse.InHeader{NodeId: node.NodeId}}, []byte("own")); st != gofuse.OK {
					t.Fatal(st)
				}
				if scenario == "staged_update" {
					fh.Lock()
					err := fs.stageShadowLocked(fh, true)
					fh.Unlock()
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			want := "external longer"
			next := client.FSLayerEntry{LayerID: "layer-1", Path: "/a", Op: "upsert", Kind: "file", Content: []byte(want), SizeBytes: int64(len(want)), EntrySeq: 2}
			if scenario == "empty" {
				want, next.Content, next.SizeBytes = "", nil, 0
			}
			deleted := scenario == "delete" || scenario == "dirty_delete"
			if deleted {
				next.Op = "whiteout"
			}
			mu.Lock()
			entries = append(entries, next)
			mu.Unlock()
			refresh := func() (int64, error) {
				return refreshLayerEvents(context.Background(), fs.client, fs.opts, shadows, idx, fs, 1)
			}
			if scenario == "busy_handle" {
				fh.Lock()
				seq, err := refresh()
				fh.Unlock()
				if !errors.Is(err, errLayerReplayBusy) || seq != 1 {
					t.Fatalf("busy refresh advanced cursor: seq=%d err=%v", seq, err)
				}
			}
			if _, err := refresh(); err != nil {
				t.Fatal(err)
			}
			if dirty {
				want = "own"
			}
			got, st, err := readDat9FSTestRange(fs, node.NodeId, opened.Fh, 0, 64)
			if deleted && !dirty {
				if st != gofuse.ENOENT {
					t.Fatalf("deleted handle read=%q status=%v", got, st)
				}
				if _, st := fs.Write(nil, &gofuse.WriteIn{Fh: opened.Fh}, []byte("x")); st != gofuse.ENOENT {
					t.Fatalf("deleted handle write=%v", st)
				}
				return
			}
			if err != nil || st != gofuse.OK || string(got) != want {
				t.Fatalf("refreshed writable handle=%q %v %v; want %q", got, st, err, want)
			}
			if dirty {
				if st := fs.Fsync(nil, &gofuse.FsyncIn{Fh: opened.Fh}); st != gofuse.Status(syscall.EAGAIN) {
					t.Fatalf("conflicting fsync=%v, want EAGAIN", st)
				}
				if st := fs.Flush(nil, &gofuse.FlushIn{Fh: opened.Fh}); st != gofuse.Status(syscall.EAGAIN) {
					t.Fatalf("conflicting close=%v, want EAGAIN", st)
				}
				fs.Release(nil, &gofuse.ReleaseIn{Fh: opened.Fh})
				recovered, err := NewPendingIndex(idx.dir)
				if err != nil {
					t.Fatal(err)
				}
				if err := recovered.RecoverFromDisk(); err != nil {
					t.Fatal(err)
				}
				meta, _ := recovered.GetMeta("/a")
				if meta == nil || meta.Kind != PendingConflict {
					t.Fatalf("conflict lost on restart: %+v", meta)
				}
				data, err := shadows.ReadAll("/a")
				if err != nil || string(data) != "own" {
					t.Fatalf("local recovery data=%q %v", data, err)
				}
				mu.Lock()
				defer mu.Unlock()
				if posts != 0 {
					t.Fatalf("conflicting refresh uploaded %d times", posts)
				}
				return
			}
			if _, st := fs.Write(nil, &gofuse.WriteIn{Fh: opened.Fh, InHeader: gofuse.InHeader{NodeId: node.NodeId}}, []byte("X")); st != gofuse.OK {
				t.Fatal(st)
			}
			if st := fs.Fsync(nil, &gofuse.FsyncIn{Fh: opened.Fh}); st != gofuse.OK {
				t.Fatal(st)
			}
			if len(want) == 0 {
				want = "X"
			} else {
				want = "X" + want[1:]
			}
			mu.Lock()
			defer mu.Unlock()
			if body := string(entries[len(entries)-1].Content); body != want {
				t.Fatalf("write reverted remote body: %q want %q", body, want)
			}
		})
	}
}
