package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/dat9/pkg/client"
)

func TestRestoreLayerEntriesHonorsCheckpointSeq(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layer-checkpoints/cp1":
			_ = json.NewEncoder(w).Encode(client.FSLayerCheckpoint{
				CheckpointID: "cp1",
				LayerID:      "layer-1",
				DurableSeq:   1,
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/diff":
			if r.URL.Query().Get("max_seq") != "1" {
				t.Errorf("diff max_seq = %q, want 1", r.URL.Query().Get("max_seq"))
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if r.URL.Query().Get("replay") != "1" {
				t.Errorf("diff replay = %q, want 1", r.URL.Query().Get("replay"))
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"entries": []client.FSLayerEntry{
					{LayerID: "layer-1", Path: "/repo/a.txt", Op: "upsert", Kind: "file", BaseRevision: 0, SizeBytes: 1, EntrySeq: 1},
					{LayerID: "layer-1", Path: "/repo/a.txt", Op: "chmod", Kind: "file", Mode: 0o600, EntrySeq: 1},
					{LayerID: "layer-1", Path: "/repo/old.txt", Op: "whiteout", Kind: "file", EntrySeq: 1},
					{LayerID: "layer-1", Path: "/repo/newdir/", Op: "mkdir", Kind: "dir", Mode: 0o755, EntrySeq: 1},
					{LayerID: "layer-1", Path: "/repo/newdir/", Op: "chmod", Kind: "dir", Mode: 0o700, EntrySeq: 1},
					{LayerID: "layer-1", Path: "/repo/link", Op: "symlink", Kind: "symlink", ContentText: "target.txt", Mode: 0o777, SizeBytes: 10, EntrySeq: 1},
					{LayerID: "layer-1", Path: "/repo/link", Op: "chmod", Kind: "symlink", Mode: 0o600, EntrySeq: 1},
					{LayerID: "layer-1", Path: "/repo/renamed-from.txt", Op: "rename", Kind: "file", ContentText: "/repo/renamed-to.txt", Mode: 0o644, EntrySeq: 1},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/entries":
			if r.URL.Query().Get("path") != "/repo/a.txt" {
				t.Errorf("unexpected entry path query: %q", r.URL.RawQuery)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if r.URL.Query().Get("max_seq") != "1" {
				t.Errorf("entry max_seq = %q, want 1", r.URL.Query().Get("max_seq"))
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
	if meta, ok := pending.GetMeta("/a.txt"); !ok || meta.Kind != PendingOverwrite {
		t.Fatalf("a.txt pending meta = %+v, want PendingOverwrite", meta)
	}
	if meta, ok := pending.GetMeta("/a.txt"); !ok || !meta.HasMode || meta.Mode != 0o600 {
		t.Fatalf("a.txt pending mode = %+v, want 0600", meta)
	}
	if mode, ok := fs.layerFileMode("/a.txt"); !ok || mode != 0o600 {
		t.Fatalf("a.txt layer file mode = (%#o, %t), want 0600 true", mode, ok)
	}
	if !fs.isLayerWhiteout("/old.txt") {
		t.Fatal("old.txt whiteout missing")
	}
	if mode, ok := fs.layerDirMode("/newdir"); !ok || mode != 0o700 {
		t.Fatalf("newdir layer dir = (%#o, %t), want 0700 true", mode, ok)
	}
	if target, mode, ok := fs.layerSymlink("/link"); !ok || target != "target.txt" || mode&0o777 != 0o600 {
		t.Fatalf("layer symlink = (%q, %#o, %t), want target.txt 0600 true", target, mode, ok)
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
	if meta, ok := pending.GetMeta("/renamed-to.txt"); !ok || meta.Kind != PendingOverwrite {
		t.Fatalf("renamed target pending meta = %+v, want PendingOverwrite", meta)
	}
	if pending.HasPending("/b.txt") || shadow.Has("/b.txt") {
		t.Fatal("b.txt should not be restored past checkpoint seq")
	}
}

func TestRestoreLayerEntriesReplaysSamePathUpsertOverwrite(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/diff":
			if r.URL.Query().Get("replay") != "1" {
				t.Errorf("diff replay = %q, want 1", r.URL.Query().Get("replay"))
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"entries": []client.FSLayerEntry{
					{LayerID: "layer-1", Path: "/repo/new.txt", Op: "upsert", Kind: "file", SizeBytes: 0, Mode: 0o644, EntrySeq: 1},
					{LayerID: "layer-1", Path: "/repo/new.txt", Op: "upsert", Kind: "file", SizeBytes: 4, Mode: 0o600, EntrySeq: 2},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/entries":
			if r.URL.Query().Get("path") != "/repo/new.txt" {
				t.Errorf("entry path query: %q, want /repo/new.txt", r.URL.Query().Get("path"))
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			switch r.URL.Query().Get("max_seq") {
			case "1":
				_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
					LayerID:   "layer-1",
					Path:      "/repo/new.txt",
					Op:        "upsert",
					Kind:      "file",
					SizeBytes: 0,
					Mode:      0o644,
					EntrySeq:  1,
				})
			case "2":
				_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
					LayerID:   "layer-1",
					Path:      "/repo/new.txt",
					Op:        "upsert",
					Kind:      "file",
					Content:   []byte("data"),
					SizeBytes: 4,
					Mode:      0o600,
					EntrySeq:  2,
				})
			default:
				t.Errorf("entry max_seq = %q, want 1 or 2", r.URL.Query().Get("max_seq"))
				w.WriteHeader(http.StatusInternalServerError)
			}
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
	if err := restoreLayerEntries(context.Background(), client.New(ts.URL, ""), &MountOptions{
		LayerRef:   "layer-1",
		RemoteRoot: "/repo",
	}, shadow, pending, fs); err != nil {
		t.Fatalf("restoreLayerEntries: %v", err)
	}
	got, err := shadow.ReadAll("/new.txt")
	if err != nil {
		t.Fatalf("ReadAll restored new.txt: %v", err)
	}
	if !bytes.Equal(got, []byte("data")) {
		t.Fatalf("restored data = %q, want data", got)
	}
	meta, ok := pending.GetMeta("/new.txt")
	if !ok {
		t.Fatal("new.txt pending metadata missing")
	}
	if meta.Size != 4 || !meta.HasMode || meta.Mode != 0o600 {
		t.Fatalf("pending meta = %+v, want size=4 mode=0600", meta)
	}
	if mode, ok := fs.layerFileMode("/new.txt"); !ok || mode != 0o600 {
		t.Fatalf("layer file mode = (%#o, %t), want 0600 true", mode, ok)
	}
}

func TestRestoreLayerEntriesStreamsObjectBackedFile(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 128*1024+13)
	objectReads := 0
	entry := client.FSLayerEntry{
		LayerID:     "layer-1",
		Path:        "/repo/large.bin",
		Op:          "upsert",
		Kind:        "file",
		StorageType: "s3",
		StorageRef:  "layers/layer-1/object",
		SizeBytes:   int64(len(payload)),
		Mode:        0o640,
		EntrySeq:    1,
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/diff":
			if r.URL.Query().Get("replay") != "1" {
				t.Errorf("diff replay = %q, want 1", r.URL.Query().Get("replay"))
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FSLayerEntry{entry}})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/entries":
			if r.URL.Query().Get("path") != "/repo/large.bin" {
				t.Errorf("unexpected entry query: %q", r.URL.RawQuery)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if r.URL.Query().Get("max_seq") != "1" {
				t.Errorf("entry max_seq = %q, want 1", r.URL.Query().Get("max_seq"))
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(entry)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/objects":
			if r.URL.Query().Get("path") != "/repo/large.bin" {
				t.Errorf("unexpected object query: %q", r.URL.RawQuery)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if r.URL.Query().Get("max_seq") != "1" {
				t.Errorf("object max_seq = %q, want 1", r.URL.Query().Get("max_seq"))
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			objectReads++
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(payload)
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
	if err := restoreLayerEntries(context.Background(), client.New(ts.URL, ""), &MountOptions{
		LayerRef:   "layer-1",
		RemoteRoot: "/repo",
	}, shadow, pending, nil); err != nil {
		t.Fatalf("restoreLayerEntries: %v", err)
	}
	if objectReads != 1 {
		t.Fatalf("object reads = %d, want 1", objectReads)
	}
	got, err := shadow.ReadAll("/large.bin")
	if err != nil {
		t.Fatalf("ReadAll restored large.bin: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("restored object payload mismatch: got %d bytes want %d", len(got), len(payload))
	}
	if meta, ok := pending.GetMeta("/large.bin"); !ok || meta.Size != int64(len(payload)) || !meta.HasMode || meta.Mode != 0o640 {
		t.Fatalf("pending meta = %+v, want size %d mode 0640", meta, len(payload))
	}
}

func TestRestoreLayerChmodOnlyFileRecordsModeOverlay(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/diff":
			if r.URL.Query().Get("replay") != "1" {
				t.Errorf("diff replay = %q, want 1", r.URL.Query().Get("replay"))
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"entries": []client.FSLayerEntry{
					{LayerID: "layer-1", Path: "/repo/base.txt", Op: "chmod", Kind: "file", Mode: 0o600, EntrySeq: 1},
				},
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
		LayerRef:   "layer-1",
		RemoteRoot: "/repo",
	})

	if err := restoreLayerEntries(context.Background(), client.New(ts.URL, ""), &MountOptions{
		LayerRef:   "layer-1",
		RemoteRoot: "/repo",
	}, shadow, pending, fs); err != nil {
		t.Fatalf("restoreLayerEntries: %v", err)
	}
	if pending.HasPending("/base.txt") || shadow.Has("/base.txt") {
		t.Fatal("chmod-only restore created pending data, want mode overlay only")
	}
	if mode, ok := fs.layerFileMode("/base.txt"); !ok || mode != 0o600 {
		t.Errorf("layer file mode = (%#o, %t), want 0600 true", mode, ok)
	}
	ino := fs.inodes.EnsureInode("/base.txt", false, 5, time.Now())
	entries := fs.mergeLayerNamespaceEntries("/", []DirEntry{{
		Name:        "base.txt",
		Ino:         ino,
		Mode:        dirEntryMode(false, true, 0o644),
		AttrMode:    0o644,
		HasMode:     true,
		HasMetadata: true,
	}})
	if len(entries) != 1 || entries[0].AttrMode != 0o600 || entries[0].Mode&0o777 != 0o600 {
		t.Errorf("merged chmod-only entry = %+v, want mode 0600", entries)
	}
}

func TestLayerSymlinkReadlinkUsesLayerTarget(t *testing.T) {
	fs := NewDat9FS(client.New("http://127.0.0.1", ""), &MountOptions{
		LayerRef:              "layer-1",
		RemoteRoot:            "/repo",
		CacheSize:             1 << 20,
		ReadCacheMaxFileBytes: 1 << 20,
	})
	fs.markLayerSymlink("/link", "target.txt", symlinkMode())
	ino := fs.inodes.Lookup("/link", false, int64(len("target.txt")), time.Now())
	fs.inodes.UpdateMode(ino, symlinkMode())
	got, st := fs.Readlink(nil, &gofuse.InHeader{NodeId: ino})
	if st != gofuse.OK {
		t.Fatalf("Readlink status = %v, want OK", st)
	}
	if !bytes.Equal(got, []byte("target.txt")) {
		t.Fatalf("Readlink target = %q, want target.txt", got)
	}
}

func TestLayerSymlinkLookupAndReadlinkPreferLayerOverLocalPolicy(t *testing.T) {
	fs := NewDat9FS(client.New("http://127.0.0.1", ""), &MountOptions{
		LayerRef:              "layer-1",
		RemoteRoot:            "/repo",
		LocalRoot:             t.TempDir(),
		LocalOnlyPatterns:     []string{"**/link"},
		CacheSize:             1 << 20,
		ReadCacheMaxFileBytes: 1 << 20,
	})
	if got := fs.observePathPolicy("/link"); got != PathLayerLocalOnly {
		t.Fatalf("policy for /link = %s, want local-only", got)
	}
	fs.markLayerSymlink("/link", "base.txt", symlinkMode())

	var out gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "link", &out)
	if st != gofuse.OK {
		t.Fatalf("Lookup status = %v, want OK", st)
	}
	if out.Mode&uint32(syscall.S_IFMT) != uint32(syscall.S_IFLNK) {
		t.Fatalf("Lookup mode type = %#o, want S_IFLNK", out.Mode&uint32(syscall.S_IFMT))
	}
	var attr gofuse.AttrOut
	st = fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: out.NodeId}}, &attr)
	if st != gofuse.OK {
		t.Fatalf("GetAttr status = %v, want OK", st)
	}
	if attr.Mode&uint32(syscall.S_IFMT) != uint32(syscall.S_IFLNK) {
		t.Fatalf("GetAttr mode type = %#o, want S_IFLNK", attr.Mode&uint32(syscall.S_IFMT))
	}
	if attr.Size != uint64(len("base.txt")) {
		t.Fatalf("GetAttr size = %d, want %d", attr.Size, len("base.txt"))
	}
	got, st := fs.Readlink(nil, &gofuse.InHeader{NodeId: out.NodeId})
	if st != gofuse.OK {
		t.Fatalf("Readlink status = %v, want OK", st)
	}
	if !bytes.Equal(got, []byte("base.txt")) {
		t.Fatalf("Readlink target = %q, want base.txt", got)
	}
}

func TestLayerChmodPreservesSymlinkModeAcrossRelistLookupReadlink(t *testing.T) {
	var got clientLayerEntryRequest
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/layers/layer-1/entries" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode layer entry: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
			LayerID: "layer-1",
			Path:    got.Path,
			Op:      got.Op,
			Kind:    got.Kind,
			Mode:    got.Mode,
		})
	}))
	defer ts.Close()

	fs := NewDat9FS(client.New(ts.URL, ""), &MountOptions{
		LayerRef:   "layer-1",
		RemoteRoot: "/repo",
	})
	fs.markLayerSymlink("/link", "target.txt", symlinkMode())

	if err := fs.upsertLayerChmod(context.Background(), "/link", 0o600); err != nil {
		t.Fatalf("upsertLayerChmod: %v", err)
	}
	if got.Path != "/repo/link" || got.Op != "chmod" || got.Kind != "symlink" || got.Mode != 0o600 {
		t.Fatalf("chmod request = %+v, want symlink chmod", got)
	}
	target, mode, ok := fs.layerSymlink("/link")
	if !ok || target != "target.txt" {
		t.Fatalf("layer symlink = (%q, %t), want target.txt true", target, ok)
	}
	if mode&uint32(syscall.S_IFMT) != uint32(syscall.S_IFLNK) {
		t.Fatalf("layer symlink mode type = %#o, want S_IFLNK", mode&uint32(syscall.S_IFMT))
	}
	if mode&0o777 != 0o600 {
		t.Fatalf("layer symlink mode perms = %#o, want 0600", mode&0o777)
	}

	entries := fs.mergeLayerNamespaceEntries("/", nil)
	if len(entries) != 1 || entries[0].Name != "link" {
		t.Fatalf("merged entries = %+v, want single link entry", entries)
	}
	if entries[0].Mode&uint32(syscall.S_IFMT) != uint32(syscall.S_IFLNK) {
		t.Fatalf("merged link type = %#o, want S_IFLNK", entries[0].Mode&uint32(syscall.S_IFMT))
	}

	var out gofuse.EntryOut
	st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "link", &out)
	if st != gofuse.OK {
		t.Fatalf("Lookup status = %v, want OK", st)
	}
	if out.Mode&uint32(syscall.S_IFMT) != uint32(syscall.S_IFLNK) {
		t.Fatalf("Lookup mode type = %#o, want S_IFLNK", out.Mode&uint32(syscall.S_IFMT))
	}
	gotTarget, st := fs.Readlink(nil, &gofuse.InHeader{NodeId: out.NodeId})
	if st != gofuse.OK {
		t.Fatalf("Readlink status = %v, want OK", st)
	}
	if !bytes.Equal(gotTarget, []byte("target.txt")) {
		t.Fatalf("Readlink target = %q, want target.txt", gotTarget)
	}
}

func TestLayerChmodUsesDirectoryKind(t *testing.T) {
	var got clientLayerEntryRequest
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/layers/layer-1/entries" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode layer entry: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
			LayerID: "layer-1",
			Path:    got.Path,
			Op:      got.Op,
			Kind:    got.Kind,
			Mode:    got.Mode,
		})
	}))
	defer ts.Close()

	fs := NewDat9FS(client.New(ts.URL, ""), &MountOptions{
		LayerRef:   "layer-1",
		RemoteRoot: "/repo",
	})
	fs.markLayerDir("/dir", 0o755)
	if err := fs.upsertLayerChmod(context.Background(), "/dir", 0o700); err != nil {
		t.Fatalf("upsertLayerChmod: %v", err)
	}
	if got.Path != "/repo/dir" || got.Op != "chmod" || got.Kind != "dir" || got.Mode != 0o700 {
		t.Fatalf("chmod request = %+v, want directory chmod", got)
	}
	if mode, ok := fs.layerDirMode("/dir"); !ok || mode != 0o700 {
		t.Fatalf("layer dir mode = (%#o, %t), want 0700 true", mode, ok)
	}
}

func TestLayerChmodCoalescesPendingFileContent(t *testing.T) {
	var got clientLayerEntryRequest
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/layers/layer-1/entries" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode layer entry: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
			LayerID: "layer-1",
			Path:    got.Path,
			Op:      got.Op,
			Kind:    got.Kind,
			Mode:    got.Mode,
			Content: got.Content,
		})
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	if err := shadow.WriteFull("/new.txt", []byte("data"), 7); err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRevAndMode("/new.txt", 4, PendingOverwrite, 7, 0o644, true); err != nil {
		t.Fatal(err)
	}
	fs := NewDat9FS(client.New(ts.URL, ""), &MountOptions{
		LayerRef:   "layer-1",
		RemoteRoot: "/repo",
	})
	fs.shadowStore = shadow
	fs.pendingIndex = pending

	if err := fs.upsertLayerChmod(context.Background(), "/new.txt", 0o600); err != nil {
		t.Fatalf("upsertLayerChmod: %v", err)
	}
	if got.Path != "/repo/new.txt" || got.Op != "upsert" || got.Kind != "file" {
		t.Fatalf("chmod coalesced request = %+v, want file upsert", got)
	}
	if !bytes.Equal(got.Content, []byte("data")) {
		t.Fatalf("coalesced content = %q, want data", got.Content)
	}
	if got.BaseRevision != 7 {
		t.Fatalf("base revision = %d, want 7", got.BaseRevision)
	}
	if got.Mode != 0o600 {
		t.Fatalf("mode = %#o, want 0600", got.Mode)
	}
	meta, ok := pending.GetMeta("/new.txt")
	if !ok || !meta.HasMode || meta.Mode != 0o600 {
		t.Fatalf("pending mode = %+v, want 0600", meta)
	}
	if mode, ok := fs.layerFileMode("/new.txt"); !ok || mode != 0o600 {
		t.Fatalf("layer file mode = (%#o, %t), want 0600 true", mode, ok)
	}
}

func TestLayerSetAttrModeCoalescesPendingFileContent(t *testing.T) {
	var got clientLayerEntryRequest
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/layers/layer-1/entries" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode layer entry: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
			LayerID: "layer-1",
			Path:    got.Path,
			Op:      got.Op,
			Kind:    got.Kind,
			Mode:    got.Mode,
			Content: got.Content,
		})
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	if err := shadow.WriteFull("/new.txt", []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRevAndMode("/new.txt", 4, PendingNew, 0, 0o644, true); err != nil {
		t.Fatal(err)
	}
	fs := NewDat9FS(client.New(ts.URL, ""), &MountOptions{
		LayerRef:   "layer-1",
		RemoteRoot: "/repo",
	})
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	ino := fs.inodes.Lookup("/new.txt", false, 4, time.Now())
	fs.inodes.UpdateMode(ino, 0o644)

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_MODE,
			Mode:     0o600,
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}
	if got.Path != "/repo/new.txt" || got.Op != "upsert" || got.Kind != "file" {
		t.Fatalf("chmod coalesced request = %+v, want file upsert", got)
	}
	if !bytes.Equal(got.Content, []byte("data")) {
		t.Fatalf("coalesced content = %q, want data", got.Content)
	}
	if got.Mode != 0o600 {
		t.Fatalf("mode = %#o, want 0600", got.Mode)
	}
	meta, ok := pending.GetMeta("/new.txt")
	if !ok || meta.Kind != PendingNew || !meta.HasMode || meta.Mode != 0o600 {
		t.Fatalf("pending mode = %+v, want PendingNew 0600", meta)
	}
	if mode, ok := fs.layerFileMode("/new.txt"); !ok || mode != 0o600 {
		t.Fatalf("layer file mode = (%#o, %t), want 0600 true", mode, ok)
	}
	if got, want := out.Mode&0o777, uint32(0o600); got != want {
		t.Fatalf("SetAttr output mode = %o, want %o", got, want)
	}
}

func TestLayerChmodCoalescesShadowFileWithoutPendingMeta(t *testing.T) {
	var got clientLayerEntryRequest
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/repo/new.txt":
			w.WriteHeader(http.StatusNotFound)
			return
		case r.Method == http.MethodPost && r.URL.Path == "/v1/layers/layer-1/entries":
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode layer entry: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
			LayerID:      "layer-1",
			Path:         got.Path,
			Op:           got.Op,
			Kind:         got.Kind,
			Mode:         got.Mode,
			Content:      got.Content,
			BaseRevision: got.BaseRevision,
		})
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	if err := shadow.WriteFull("/new.txt", []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs := NewDat9FS(client.New(ts.URL, ""), &MountOptions{
		LayerRef:   "layer-1",
		RemoteRoot: "/repo",
	})
	fs.shadowStore = shadow
	fs.pendingIndex = pending

	if err := fs.upsertLayerChmod(context.Background(), "/new.txt", 0o600); err != nil {
		t.Fatalf("upsertLayerChmod: %v", err)
	}
	if got.Path != "/repo/new.txt" || got.Op != "upsert" || got.Kind != "file" {
		t.Fatalf("chmod coalesced request = %+v, want file upsert", got)
	}
	if !bytes.Equal(got.Content, []byte("data")) {
		t.Fatalf("coalesced content = %q, want data", got.Content)
	}
	if got.BaseRevision != 0 {
		t.Fatalf("base revision = %d, want 0", got.BaseRevision)
	}
	if got.Mode != 0o600 {
		t.Fatalf("mode = %#o, want 0600", got.Mode)
	}
	meta, ok := pending.GetMeta("/new.txt")
	if !ok || meta.Kind != PendingNew || !meta.HasMode || meta.Mode != 0o600 {
		t.Fatalf("pending mode = %+v, want PendingNew 0600", meta)
	}
	if mode, ok := fs.layerFileMode("/new.txt"); !ok || mode != 0o600 {
		t.Fatalf("layer file mode = (%#o, %t), want 0600 true", mode, ok)
	}
}

func TestLayerChmodCoalescesExistingLayerUpsertContent(t *testing.T) {
	var got clientLayerEntryRequest
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/entries":
			if r.URL.Query().Get("path") != "/repo/new.txt" {
				t.Errorf("entry path query = %q, want /repo/new.txt", r.URL.Query().Get("path"))
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
				LayerID:      "layer-1",
				Path:         "/repo/new.txt",
				Op:           "upsert",
				Kind:         "file",
				BaseRevision: 0,
				Content:      []byte("data"),
				Mode:         0o644,
			})
			return
		case r.Method == http.MethodPost && r.URL.Path == "/v1/layers/layer-1/entries":
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/repo/new.txt":
			w.WriteHeader(http.StatusNotFound)
			return
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode layer entry: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
			LayerID:      "layer-1",
			Path:         got.Path,
			Op:           got.Op,
			Kind:         got.Kind,
			Mode:         got.Mode,
			Content:      got.Content,
			BaseRevision: got.BaseRevision,
		})
	}))
	defer ts.Close()

	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs := NewDat9FS(client.New(ts.URL, ""), &MountOptions{
		LayerRef:   "layer-1",
		RemoteRoot: "/repo",
	})
	fs.pendingIndex = pending

	if err := fs.upsertLayerChmod(context.Background(), "/new.txt", 0o600); err != nil {
		t.Fatalf("upsertLayerChmod: %v", err)
	}
	if got.Path != "/repo/new.txt" || got.Op != "upsert" || got.Kind != "file" {
		t.Fatalf("chmod coalesced request = %+v, want file upsert", got)
	}
	if !bytes.Equal(got.Content, []byte("data")) {
		t.Fatalf("coalesced content = %q, want data", got.Content)
	}
	if got.BaseRevision != 0 {
		t.Fatalf("base revision = %d, want 0", got.BaseRevision)
	}
	if got.Mode != 0o600 {
		t.Fatalf("mode = %#o, want 0600", got.Mode)
	}
	meta, ok := pending.GetMeta("/new.txt")
	if !ok || meta.Kind != PendingNew || !meta.HasMode || meta.Mode != 0o600 {
		t.Fatalf("pending mode = %+v, want PendingNew 0600", meta)
	}
}

func TestLayerFileUpsertClearsStaleNamespaceState(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/layers/layer-1/entries" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var req clientLayerEntryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode layer entry: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(client.FSLayerEntry{LayerID: "layer-1", Path: req.Path, Op: req.Op, Kind: req.Kind})
	}))
	defer ts.Close()

	fs := NewDat9FS(client.New(ts.URL, ""), &MountOptions{
		LayerRef:   "layer-1",
		RemoteRoot: "/repo",
	})
	fs.markLayerSymlink("/file.txt", "old-target", symlinkMode())
	if err := fs.upsertLayerFile(context.Background(), "/file.txt", []byte("file"), 0, 0o644, true); err != nil {
		t.Fatalf("upsertLayerFile over symlink: %v", err)
	}
	if _, _, ok := fs.layerSymlink("/file.txt"); ok {
		t.Fatal("file upsert left stale layer symlink")
	}
	fs.markLayerWhiteout("/file.txt")
	if err := fs.upsertLayerFile(context.Background(), "/file.txt", []byte("file2"), 0, 0o644, true); err != nil {
		t.Fatalf("upsertLayerFile over whiteout: %v", err)
	}
	if fs.isLayerWhiteout("/file.txt") {
		t.Fatal("file upsert left stale layer whiteout")
	}
}

func TestLayerSetAttrTruncateWritesLayerEntryNotBase(t *testing.T) {
	var (
		got         clientLayerEntryRequest
		basePutSeen bool
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/layers/layer-1/entries":
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
				t.Errorf("decode layer entry: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
				LayerID:      "layer-1",
				Path:         got.Path,
				Op:           got.Op,
				Kind:         got.Kind,
				BaseRevision: got.BaseRevision,
				SizeBytes:    got.SizeBytes,
				Mode:         got.Mode,
			})
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs/repo/base.txt":
			basePutSeen = true
			t.Errorf("unexpected base PUT for layer truncate")
			w.WriteHeader(http.StatusInternalServerError)
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
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	ino := fs.inodes.Lookup("/base.txt", false, 12, time.Now())
	fs.inodes.UpdateRevision(ino, 7)
	fs.inodes.SetModeState(ino, 0o640, true)

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_SIZE,
			Size:     0,
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}
	if basePutSeen {
		t.Fatal("layer truncate wrote base filesystem")
	}
	if got.Path != "/repo/base.txt" || got.Op != "upsert" || got.Kind != "file" {
		t.Fatalf("layer truncate request = %+v, want file upsert", got)
	}
	if got.BaseRevision != 7 || got.SizeBytes != 0 || len(got.Content) != 0 {
		t.Fatalf("layer truncate content metadata = %+v, want base rev 7 empty content", got)
	}
	if got.Mode != 0o640 {
		t.Fatalf("layer truncate mode = %#o, want 0640", got.Mode)
	}
	if !shadow.Has("/base.txt") || shadow.Size("/base.txt") != 0 {
		t.Fatalf("shadow empty file not staged; has=%t size=%d", shadow.Has("/base.txt"), shadow.Size("/base.txt"))
	}
	meta, ok := pending.GetMeta("/base.txt")
	if !ok || meta.Size != 0 || meta.BaseRev != 7 || !meta.HasMode || meta.Mode != 0o640 {
		t.Fatalf("pending meta = %+v, want empty base rev 7 mode 0640", meta)
	}
}

func TestLayerSetAttrLargeTruncateUploadsLayerObjectFromShadow(t *testing.T) {
	const (
		remotePath = "/repo/base.txt"
		localPath  = "/base.txt"
		baseRev    = int64(7)
		mode       = uint32(0o640)
		newSize    = maxPathTruncateInMemoryBytes + 1
	)
	seed := []byte("seed")

	var (
		uploadCalls int
		basePutSeen bool
		baseGetSeen bool
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/layers/layer-1/objects":
			uploadCalls++
			if got := r.URL.Query().Get("path"); got != remotePath {
				t.Errorf("layer object path = %q, want %q", got, remotePath)
			}
			if want := strconv.FormatInt(newSize, 10); r.URL.Query().Get("size") != want {
				t.Errorf("layer object size = %q, want %s", r.URL.Query().Get("size"), want)
			}
			if want := strconv.FormatInt(baseRev, 10); r.URL.Query().Get("base_revision") != want {
				t.Errorf("layer object base_revision = %q, want %s", r.URL.Query().Get("base_revision"), want)
			}
			if want := strconv.FormatUint(uint64(mode), 8); r.URL.Query().Get("mode") != want {
				t.Errorf("layer object mode = %q, want %s", r.URL.Query().Get("mode"), want)
			}
			if r.ContentLength != newSize {
				t.Errorf("layer object content length = %d, want %d", r.ContentLength, newSize)
			}
			prefix := make([]byte, len(seed))
			if _, err := io.ReadFull(r.Body, prefix); err != nil {
				t.Errorf("read layer object prefix: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if !bytes.Equal(prefix, seed) {
				t.Errorf("layer object prefix = %q, want %q", prefix, seed)
			}
			n, err := io.Copy(io.Discard, r.Body)
			if err != nil {
				t.Errorf("read layer object body: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if got := n + int64(len(prefix)); got != newSize {
				t.Errorf("layer object body bytes = %d, want %d", got, newSize)
			}
			_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
				LayerID:      "layer-1",
				Path:         remotePath,
				Op:           "upsert",
				Kind:         "file",
				BaseRevision: baseRev,
				SizeBytes:    newSize,
				Mode:         mode,
			})
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs/repo/base.txt":
			basePutSeen = true
			t.Errorf("unexpected base PUT for layer large truncate")
			w.WriteHeader(http.StatusInternalServerError)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/repo/base.txt":
			baseGetSeen = true
			t.Errorf("unexpected base GET for layer large truncate with local shadow")
			w.WriteHeader(http.StatusInternalServerError)
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
	if err := shadow.WriteFull(localPath, seed, baseRev); err != nil {
		t.Fatalf("WriteFull: %v", err)
	}
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs := NewDat9FS(client.New(ts.URL, ""), &MountOptions{
		LayerRef:   "layer-1",
		RemoteRoot: "/repo",
	})
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	ino := fs.inodes.Lookup(localPath, false, int64(len(seed)), time.Now())
	fs.inodes.UpdateRevision(ino, baseRev)
	fs.inodes.SetModeState(ino, mode, true)

	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_SIZE,
			Size:     uint64(newSize),
		},
	}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}
	if uploadCalls != 1 {
		t.Fatalf("layer object upload calls = %d, want 1", uploadCalls)
	}
	if basePutSeen {
		t.Fatal("layer large truncate wrote base filesystem")
	}
	if baseGetSeen {
		t.Fatal("layer large truncate fetched base filesystem despite local shadow")
	}
	if got := shadow.Size(localPath); got != newSize {
		t.Fatalf("shadow size = %d, want %d", got, newSize)
	}
	if out.Size != uint64(newSize) {
		t.Fatalf("out.Size = %d, want %d", out.Size, newSize)
	}
	meta, ok := pending.GetMeta(localPath)
	if !ok {
		t.Fatal("missing pending meta for layer large truncate")
	}
	if meta.Size != newSize || meta.BaseRev != baseRev || meta.Kind != PendingOverwrite || !meta.ShadowSpill || !meta.HasMode || meta.Mode != mode {
		t.Fatalf("pending meta = %+v, want shadow spill overwrite size %d base rev %d mode %#o", meta, newSize, baseRev, mode)
	}
}

func TestLayerRmdirRejectsOverlayOnlyChildren(t *testing.T) {
	fs := NewDat9FS(client.New("http://127.0.0.1", ""), &MountOptions{
		LayerRef:   "layer-1",
		RemoteRoot: "/repo",
	})
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.pendingIndex = pending
	if _, err := pending.PutWithBaseRev("/dir/file.txt", 1, PendingNew, 0); err != nil {
		t.Fatal(err)
	}
	fs.inodes.Lookup("/dir", true, 0, time.Now())
	st := fs.Rmdir(nil, &gofuse.InHeader{NodeId: 1}, "dir")
	if st != gofuse.Status(syscall.ENOTEMPTY) {
		t.Fatalf("Rmdir status = %v, want ENOTEMPTY", st)
	}
	if !pending.HasPending("/dir/file.txt") {
		t.Fatal("pending child was removed despite failed rmdir")
	}
}

func TestLayerRmdirRejectsOpenHandleChild(t *testing.T) {
	fs := NewDat9FS(client.New("http://127.0.0.1", ""), &MountOptions{
		LayerRef:   "layer-1",
		RemoteRoot: "/repo",
	})
	fs.inodes.Lookup("/dir", true, 0, time.Now())
	childIno := fs.inodes.Lookup("/dir/file.txt", false, 0, time.Now())
	fhID := fs.fileHandles.Allocate(&FileHandle{
		Ino:   childIno,
		Path:  "/dir/file.txt",
		Dirty: fs.newWriteBuffer("/dir/file.txt", maxPreloadSize, 0),
		IsNew: true,
	})
	defer fs.fileHandles.Delete(fhID)

	st := fs.Rmdir(nil, &gofuse.InHeader{NodeId: 1}, "dir")
	if st != gofuse.Status(syscall.ENOTEMPTY) {
		t.Fatalf("Rmdir status = %v, want ENOTEMPTY", st)
	}
	if fs.isLayerWhiteout("/dir") {
		t.Fatal("rmdir created a layer whiteout despite live open child")
	}
}

func TestLayerRmdirRejectsNonEmptyBaseDir(t *testing.T) {
	var layerWrites int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/repo/dir" && r.URL.Query().Get("list") == "1":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"entries": []client.FileInfo{{Name: "child.txt", Size: 1, IsDir: false}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-stat":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results": []client.BatchStatResult{{Path: "/repo/dir/child.txt", Status: http.StatusOK, Size: 1}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/layers/layer-1/entries":
			layerWrites++
			w.WriteHeader(http.StatusInternalServerError)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	fs := NewDat9FS(client.New(ts.URL, ""), &MountOptions{
		LayerRef:   "layer-1",
		RemoteRoot: "/repo",
	})
	fs.inodes.Lookup("/dir", true, 0, time.Now())

	st := fs.Rmdir(nil, &gofuse.InHeader{NodeId: 1}, "dir")
	if st != gofuse.Status(syscall.ENOTEMPTY) {
		t.Errorf("Rmdir status = %v, want ENOTEMPTY", st)
	}
	if layerWrites != 0 {
		t.Errorf("layer whiteout writes = %d, want 0", layerWrites)
	}
	if fs.isLayerWhiteout("/dir") {
		t.Error("rmdir created a layer whiteout for non-empty base dir")
	}
}

func TestLayerSymlinkHonorsLocalOnlyOverlay(t *testing.T) {
	var layerWrites int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/layers/layer-1/entries" {
			layerWrites++
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	fs := NewDat9FS(client.New(ts.URL, ""), &MountOptions{
		LayerRef:          "layer-1",
		RemoteRoot:        "/repo",
		LocalRoot:         t.TempDir(),
		LocalOnlyPatterns: []string{"**/local-link"},
	})
	if err := fs.localOverlay.EnsureRoot(); err != nil {
		t.Fatal(err)
	}

	var out gofuse.EntryOut
	st := fs.Symlink(nil, &gofuse.InHeader{NodeId: 1}, "target.txt", "local-link", &out)
	if st != gofuse.OK {
		t.Errorf("Symlink status = %v, want OK", st)
	}
	if layerWrites != 0 {
		t.Errorf("layer writes = %d, want 0", layerWrites)
	}
	target, err := fs.localOverlay.Readlink("/local-link")
	if err != nil {
		t.Errorf("local overlay readlink: %v", err)
	}
	if target != "target.txt" {
		t.Errorf("local overlay symlink target = %q, want target.txt", target)
	}
}

func TestLayerRenamePreservesLayerSymlink(t *testing.T) {
	var requests []clientLayerEntryRequest
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/layers/layer-1/entries" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var req clientLayerEntryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode layer entry: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		requests = append(requests, req)
		_ = json.NewEncoder(w).Encode(client.FSLayerEntry{LayerID: "layer-1", Path: req.Path, Op: req.Op, Kind: req.Kind})
	}))
	defer ts.Close()

	fs := NewDat9FS(client.New(ts.URL, ""), &MountOptions{
		LayerRef:   "layer-1",
		RemoteRoot: "/repo",
	})
	fs.markLayerSymlink("/link", "target.txt", symlinkMode())
	if err := fs.upsertLayerRename(context.Background(), "/link", "/moved"); err != nil {
		t.Fatalf("upsertLayerRename symlink: %v", err)
	}
	if len(requests) != 2 {
		t.Fatalf("requests len = %d, want 2: %+v", len(requests), requests)
	}
	if requests[0].Path != "/repo/moved" || requests[0].Op != "symlink" || requests[0].Kind != "symlink" || requests[0].ContentText != "target.txt" {
		t.Fatalf("symlink rename request = %+v", requests[0])
	}
	if requests[1].Path != "/repo/link" || requests[1].Op != "whiteout" {
		t.Fatalf("symlink source whiteout request = %+v", requests[1])
	}
	if target, _, ok := fs.layerSymlink("/moved"); !ok || target != "target.txt" {
		t.Fatalf("moved symlink = (%q, %t), want target.txt true", target, ok)
	}
	if _, _, ok := fs.layerSymlink("/link"); ok {
		t.Fatal("old symlink state still present")
	}
	if !fs.isLayerWhiteout("/link") {
		t.Fatal("old symlink source whiteout missing")
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
		case r.Method == http.MethodPost && r.URL.Path == "/v1/layers/layer-1/entries":
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
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/entries":
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
		case r.Method == http.MethodPost && r.URL.Path == "/v1/layers/layer-1/entries":
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
