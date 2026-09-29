package fuse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestLayerChmodPreservesObjectBackedContentWithoutShadow(t *testing.T) {
	for _, storage := range []string{"reference_only", "s3", "inline"} {
		t.Run(storage, func(t *testing.T) {
			entry := client.FSLayerEntry{LayerID: "layer-1", Path: "/a", Op: "upsert", Kind: "file", SizeBytes: 7, EntrySeq: 1}
			if storage == "reference_only" {
				entry.StorageRef = "s3://bucket/object"
			}
			if storage == "s3" {
				entry.StorageType = "s3"
			}
			if storage == "inline" {
				entry.Content = []byte("payload")
			}
			var posted client.FSLayerEntryRequest
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/layers/layer-1/entries" {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodPost {
					if err := json.NewDecoder(r.Body).Decode(&posted); err != nil {
						t.Error(err)
						w.WriteHeader(400)
						return
					}
				}
				_ = json.NewEncoder(w).Encode(entry)
			}))
			defer ts.Close()
			fs := NewDat9FS(newTestClient(ts.URL), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/"})
			if err := fs.upsertLayerChmod(context.Background(), "/a", 0600); err != nil {
				t.Fatal(err)
			}
			if posted.Path != "/a" || posted.Mode != 0600 {
				t.Fatalf("wrong permission update: %+v", posted)
			}
			if storage == "inline" {
				if posted.Op != "upsert" || string(posted.Content) != "payload" {
					t.Fatalf("inline content lost: %+v", posted)
				}
			} else if posted.Op != "chmod" || len(posted.Content) != 0 {
				t.Fatalf("object-backed chmod replaced remote content: %+v", posted)
			}
		})
	}
}

func TestLayerChmodDoesNotDeferToReleasedHandle(t *testing.T) {
	var posted client.FSLayerEntryRequest
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/layers/layer-1/entries" {
			if err := json.NewDecoder(r.Body).Decode(&posted); err != nil {
				t.Error(err)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(client.FSLayerEntry{LayerID: "layer-1", Path: "/a", EntrySeq: 2, Mode: posted.Mode})
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()
	idx, shadows := newLayerShutdownState(t)
	if err := shadows.WriteFull("/a", []byte("body"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := idx.PutLayerCache("/a", 4, 0, 0644, true, LayerCacheIdentity{"layer-1", 1}); err != nil {
		t.Fatal(err)
	}
	fs := NewDat9FS(newTestClient(ts.URL), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/"})
	fs.pendingIndex, fs.shadowStore = idx, shadows
	fs.markLayerFileMode("/a", 0644)
	var node gofuse.EntryOut
	if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "a", &node); st != gofuse.OK {
		t.Fatal(st)
	}
	// Release has finished its pending-mode check but has not removed the
	// handle from the index yet. Chmod must not assign work to that handle.
	fh := &FileHandle{Path: "/a", Ino: node.NodeId, Flags: syscall.O_RDWR, Dirty: fs.newWriteBuffer("/a", 0, 0), releasing: true}
	id := fs.allocateFileHandle(fh)
	defer fs.deleteFileHandle(id, fh)
	in := &gofuse.SetAttrIn{}
	in.NodeId, in.Valid, in.Mode = node.NodeId, gofuse.FATTR_MODE, 0600
	if st := fs.SetAttr(nil, in, &gofuse.AttrOut{}); st != gofuse.OK {
		t.Fatal(st)
	}
	if posted.Mode != 0600 || string(posted.Content) != "body" {
		t.Fatalf("chmod was lost behind Release: %+v", posted)
	}
	if meta, _ := idx.GetMeta("/a"); meta == nil || !meta.LayerClean || meta.Mode != 0600 {
		t.Fatalf("chmod was not acknowledged: %+v", meta)
	}
}

func TestLayerRewriteKeepsCommittedMode(t *testing.T) {
	for _, mode := range []uint32{0600, 0755} {
		t.Run(strconv.FormatUint(uint64(mode), 8), func(t *testing.T) {
			var requests []client.FSLayerEntryRequest
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					http.NotFound(w, r)
					return
				}
				var req client.FSLayerEntryRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
				}
				requests = append(requests, req)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(client.FSLayerEntry{LayerID: "layer-1", Path: req.Path, EntrySeq: int64(len(requests)), Mode: req.Mode})
			}))
			defer ts.Close()
			idx, shadows := newLayerShutdownState(t)
			fs := NewDat9FS(newTestClient(ts.URL), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/", WritePolicy: WritePolicyWriteSync})
			fs.pendingIndex, fs.shadowStore = idx, shadows
			var created gofuse.CreateOut
			if st := fs.Create(nil, &gofuse.CreateIn{InHeader: gofuse.InHeader{NodeId: 1}, Flags: syscall.O_RDWR, Mode: mode}, "a", &created); st != gofuse.OK {
				t.Fatal(st)
			}
			defer fs.Release(nil, &gofuse.ReleaseIn{Fh: created.Fh})
			// Shell redirection closes a duplicate descriptor before printf.
			if st := fs.Flush(nil, &gofuse.FlushIn{Fh: created.Fh}); st != gofuse.OK {
				t.Fatal(st)
			}
			for i := 0; i < 2; i++ {
				if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh, Offset: uint64(i)}, []byte("x")); st != gofuse.OK {
					t.Fatal(st)
				}
			}
			if len(requests) < 2 {
				t.Fatalf("only %d publications", len(requests))
			}
			for i, req := range requests {
				if req.Mode != mode {
					t.Fatalf("publication %d dropped mode: got %o want %o", i, req.Mode, mode)
				}
			}
			if meta, _ := idx.GetMeta("/a"); meta == nil || !meta.HasMode || meta.Mode != mode {
				t.Fatalf("cached mode lost: %+v", meta)
			}
		})
	}
}

func TestLayerRenameReadsObjectWithoutShadow(t *testing.T) {
	for _, storage := range []string{"reference_only", "s3", "inline_empty"} {
		t.Run(storage, func(t *testing.T) {
			want := "layer object"
			entry := client.FSLayerEntry{LayerID: "layer-1", Path: "/a", Op: "upsert", Kind: "file", SizeBytes: int64(len(want)), EntrySeq: 7, Mode: 0600}
			if storage == "reference_only" {
				entry.StorageRef = "s3://bucket/object"
			}
			if storage == "s3" {
				entry.StorageType = "s3"
			}
			if storage == "inline_empty" {
				want, entry.SizeBytes = "", 0
			}
			var posted []client.FSLayerEntryRequest
			objectReads, baseReads := 0, 0
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/layers/layer-1/entries":
					if r.Method == http.MethodPost {
						var req client.FSLayerEntryRequest
						if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
							t.Error(err)
						}
						posted = append(posted, req)
					}
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(entry)
				case "/v1/layers/layer-1/objects":
					objectReads++
					if r.URL.Query().Get("max_seq") != strconv.FormatInt(entry.EntrySeq, 10) {
						t.Error("object read was not pinned")
					}
					_, _ = w.Write([]byte(want))
				case "/v1/fs/a":
					baseReads++
					if r.Method == http.MethodHead {
						w.Header().Set("Content-Length", "4")
						w.Header().Set("X-Dat9-IsDir", "false")
						return
					}
					_, _ = w.Write([]byte("base"))
				default:
					http.NotFound(w, r)
				}
			}))
			defer ts.Close()
			idx, shadows := newLayerShutdownState(t)
			if _, err := idx.PutLayerCache("/a", entry.SizeBytes, 0, 0600, true, layerCacheIdentity(&entry, "layer-1")); err != nil {
				t.Fatal(err)
			}
			fs := NewDat9FS(newTestClient(ts.URL), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/"})
			fs.pendingIndex, fs.shadowStore = idx, shadows
			if err := fs.upsertLayerRename(context.Background(), "/a", "/b"); err != nil {
				t.Fatal(err)
			}
			if len(posted) != 2 || posted[0].Path != "/b" || string(posted[0].Content) != want || posted[0].Mode != 0600 || posted[1].Op != "whiteout" {
				t.Fatalf("rename entries: %+v", posted)
			}
			if baseReads != 0 || (storage != "inline_empty" && objectReads != 1) {
				t.Fatalf("base reads=%d object reads=%d", baseReads, objectReads)
			}
			if data, err := shadows.ReadAll("/b"); err != nil || string(data) != want {
				t.Fatalf("renamed shadow=%q %v", data, err)
			}
		})
	}
}
