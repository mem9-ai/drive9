package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mem9-ai/dat9/pkg/client"
)

func TestRestoreLayerEntriesHonorsCheckpointSeq(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs-layer-checkpoints/cp1":
			_ = json.NewEncoder(w).Encode(client.FSLayerCheckpoint{
				CheckpointID: "cp1",
				LayerID:      "layer-1",
				DurableSeq:   1,
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs-layers/layer-1/diff":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"entries": []client.FSLayerEntry{
					{LayerID: "layer-1", Path: "/repo/a.txt", Op: "upsert", Kind: "file", BaseRevision: 0, SizeBytes: 1, EntrySeq: 1},
					{LayerID: "layer-1", Path: "/repo/old.txt", Op: "whiteout", Kind: "file", EntrySeq: 1},
					{LayerID: "layer-1", Path: "/repo/newdir/", Op: "mkdir", Kind: "dir", Mode: 0o755, EntrySeq: 1},
					{LayerID: "layer-1", Path: "/repo/renamed-from.txt", Op: "rename", Kind: "file", ContentText: "/repo/renamed-to.txt", Mode: 0o644, EntrySeq: 1},
					{LayerID: "layer-1", Path: "/repo/b.txt", Op: "upsert", Kind: "file", BaseRevision: 0, SizeBytes: 1, EntrySeq: 2},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs-layers/layer-1/entries":
			if r.URL.Query().Get("path") != "/repo/a.txt" {
				t.Errorf("unexpected entry path query: %q", r.URL.RawQuery)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
				LayerID:      "layer-1",
				Path:         "/repo/a.txt",
				Op:           "upsert",
				Kind:         "file",
				Content:      []byte("a"),
				BaseRevision: 0,
				SizeBytes:    1,
				EntrySeq:     1,
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/repo/renamed-from.txt":
			_, _ = w.Write([]byte("renamed"))
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs := NewDat9FS(client.New(ts.URL, ""), &MountOptions{
		LayerRef:   "layer-1",
		RemoteRoot: "/repo",
	})

	err = restoreLayerEntries(context.Background(), client.New(ts.URL, ""), &MountOptions{
		LayerRef:      "layer-1",
		CheckpointRef: "cp1",
		RemoteRoot:    "/repo",
	}, shadow, pending, fs)
	if err != nil {
		t.Fatalf("restoreLayerEntries: %v", err)
	}
	data, err := shadow.ReadAll("/a.txt")
	if err != nil {
		t.Fatalf("ReadAll restored a.txt: %v", err)
	}
	if !bytes.Equal(data, []byte("a")) {
		t.Fatalf("restored data = %q, want a", data)
	}
	if !pending.HasPending("/a.txt") {
		t.Fatal("a.txt pending metadata missing")
	}
	if !fs.isLayerWhiteout("/old.txt") {
		t.Fatal("old.txt whiteout missing")
	}
	if mode, ok := fs.layerDirMode("/newdir"); !ok || mode != 0o755 {
		t.Fatalf("newdir layer dir = (%#o, %t), want 0755 true", mode, ok)
	}
	renamed, err := shadow.ReadAll("/renamed-to.txt")
	if err != nil {
		t.Fatalf("ReadAll restored renamed target: %v", err)
	}
	if !bytes.Equal(renamed, []byte("renamed")) {
		t.Fatalf("restored renamed data = %q, want renamed", renamed)
	}
	if !fs.isLayerWhiteout("/renamed-from.txt") {
		t.Fatal("renamed source whiteout missing")
	}
	if !pending.HasPending("/renamed-to.txt") {
		t.Fatal("renamed target pending metadata missing")
	}
	if pending.HasPending("/b.txt") || shadow.Has("/b.txt") {
		t.Fatal("b.txt should not be restored past checkpoint seq")
	}
}

func TestLayerRenameCopyUpMaterializesTarget(t *testing.T) {
	var requests []clientLayerEntryRequest
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/repo/old.txt":
			w.Header().Set("Content-Length", "8")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "7")
			w.Header().Set("X-Dat9-Mode", "420")
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/repo/new.txt":
			http.NotFound(w, r)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/repo/old.txt":
			_, _ = w.Write([]byte("old-data"))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs-layers/layer-1/entries":
			var req clientLayerEntryRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode layer entry: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			requests = append(requests, req)
			_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
				LayerID:  "layer-1",
				Path:     req.Path,
				Op:       req.Op,
				Kind:     req.Kind,
				EntrySeq: int64(len(requests)),
			})
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs := NewDat9FS(client.New(ts.URL, ""), &MountOptions{
		LayerRef:              "layer-1",
		RemoteRoot:            "/repo",
		CacheSize:             1 << 20,
		ReadCacheMaxFileBytes: 1 << 20,
	})
	fs.shadowStore = shadow
	fs.pendingIndex = pending

	if err := fs.upsertLayerRename(context.Background(), "/old.txt", "/new.txt"); err != nil {
		t.Fatalf("upsertLayerRename: %v", err)
	}
	if len(requests) != 2 {
		t.Fatalf("layer requests len = %d, want 2: %+v", len(requests), requests)
	}
	if requests[0].Path != "/repo/new.txt" || requests[0].Op != "upsert" || !bytes.Equal(requests[0].Content, []byte("old-data")) {
		t.Fatalf("upsert request = %+v", requests[0])
	}
	if requests[1].Path != "/repo/old.txt" || requests[1].Op != "whiteout" {
		t.Fatalf("whiteout request = %+v", requests[1])
	}
	data, err := shadow.ReadAll("/new.txt")
	if err != nil {
		t.Fatalf("ReadAll new shadow: %v", err)
	}
	if !bytes.Equal(data, []byte("old-data")) {
		t.Fatalf("new shadow = %q, want old-data", data)
	}
	if !pending.HasPending("/new.txt") {
		t.Fatal("new pending metadata missing")
	}
	if !fs.isLayerWhiteout("/old.txt") {
		t.Fatal("old path whiteout missing")
	}
}

func TestLayerRenameCopyUpUsesExistingLayerEntry(t *testing.T) {
	var requests []clientLayerEntryRequest
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs-layers/layer-1/entries":
			if r.URL.Query().Get("path") != "/repo/old.txt" {
				t.Errorf("unexpected layer entry query: %q", r.URL.RawQuery)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
				LayerID: "layer-1",
				Path:    "/repo/old.txt",
				Op:      "upsert",
				Kind:    "file",
				Content: []byte("layer-data"),
				Mode:    0o600,
			})
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/repo/new.txt":
			http.NotFound(w, r)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/repo/old.txt":
			t.Error("rename read base old path; want existing layer entry")
			w.WriteHeader(http.StatusInternalServerError)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs-layers/layer-1/entries":
			var req clientLayerEntryRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode layer entry: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			requests = append(requests, req)
			_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
				LayerID:  "layer-1",
				Path:     req.Path,
				Op:       req.Op,
				Kind:     req.Kind,
				EntrySeq: int64(len(requests)),
			})
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRevAndMode("/old.txt", 10, PendingOverwrite, 7, 0o600, true); err != nil {
		t.Fatal(err)
	}
	fs := NewDat9FS(client.New(ts.URL, ""), &MountOptions{
		LayerRef:              "layer-1",
		RemoteRoot:            "/repo",
		CacheSize:             1 << 20,
		ReadCacheMaxFileBytes: 1 << 20,
	})
	fs.shadowStore = shadow
	fs.pendingIndex = pending

	if err := fs.upsertLayerRename(context.Background(), "/old.txt", "/new.txt"); err != nil {
		t.Fatalf("upsertLayerRename: %v", err)
	}
	if len(requests) != 2 {
		t.Fatalf("layer requests len = %d, want 2: %+v", len(requests), requests)
	}
	if requests[0].Path != "/repo/new.txt" || requests[0].Op != "upsert" || !bytes.Equal(requests[0].Content, []byte("layer-data")) {
		t.Fatalf("upsert request = %+v", requests[0])
	}
	if requests[0].Mode != 0o600 {
		t.Fatalf("upsert mode = %#o, want 0600", requests[0].Mode)
	}
	if requests[1].Path != "/repo/old.txt" || requests[1].Op != "whiteout" {
		t.Fatalf("whiteout request = %+v", requests[1])
	}
}
