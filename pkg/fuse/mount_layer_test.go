package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
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

	shadowDir := t.TempDir()
	shadow, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pendingDir := t.TempDir()
	pending, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	pending.setShadowStore(shadow)
	journal, err := NewJournal(filepath.Join(t.TempDir(), "restore.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = journal.Close() }()
	pending.SetJournal(journal)
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
	if meta, ok := pending.GetMeta("/a.txt"); !ok || meta.Kind != PendingOverwrite || !meta.LayerCommitted {
		t.Fatalf("a.txt pending meta = %+v, want committed PendingOverwrite", meta)
	}
	pendingShadowGen := pending.shadowReadGeneration("/a.txt", shadow)
	if pendingShadowGen == 0 {
		t.Fatal("restored a.txt pending metadata is not bound to its resident shadow")
	}
	reader := &FileHandle{Path: "/a.txt"}
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	fs.openReadOnlyShadowLocked(reader)
	if !reader.ShadowPinned || !shadow.canReadGeneration(reader.ShadowGen, 0, pendingShadowGen) {
		t.Fatalf("restored a.txt is not readable from shadow: pinned=%t gen=%d pending_gen=%d", reader.ShadowPinned, reader.ShadowGen, pendingShadowGen)
	}
	shadow.Unpin(reader.ShadowGen)
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
	if meta, ok := pending.GetMeta("/renamed-to.txt"); !ok || meta.Kind != PendingOverwrite || !meta.LayerCommitted {
		t.Fatalf("renamed target pending meta = %+v, want committed PendingOverwrite", meta)
	}
	renamedPendingShadowGen := pending.shadowReadGeneration("/renamed-to.txt", shadow)
	if renamedPendingShadowGen == 0 {
		t.Fatal("restored renamed target pending metadata is not bound to its resident shadow")
	}
	renamedReader := &FileHandle{Path: "/renamed-to.txt"}
	fs.openReadOnlyShadowLocked(renamedReader)
	if !renamedReader.ShadowPinned || !shadow.canReadGeneration(renamedReader.ShadowGen, 0, renamedPendingShadowGen) {
		t.Fatalf("restored renamed target is not readable from shadow: pinned=%t gen=%d pending_gen=%d", renamedReader.ShadowPinned, renamedReader.ShadowGen, renamedPendingShadowGen)
	}
	shadow.Unpin(renamedReader.ShadowGen)
	if pending.HasPending("/b.txt") || shadow.Has("/b.txt") {
		t.Fatal("b.txt should not be restored past checkpoint seq")
	}

	// A restored overlay is already durable in the remote layer. Graceful
	// late-drain and crash/restart recovery must not append it again.
	cq := NewCommitQueue(client.New(ts.URL, ""), shadow, pending, nil, 1, 8, "/repo")
	cq.SetLayerRef("layer-1")
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	fs.commitQueue = cq
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	fs.drainLatePendingEntries(ctx)
	if got := journal.UncommittedFrameCount(); got != 0 {
		t.Fatalf("restore journal uncommitted frames = %d, want 0", got)
	}

	recovered, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	if err := replayJournalIntoPending(journal, recovered, shadow); err != nil {
		t.Fatal(err)
	}
	recoveredQueue := NewCommitQueue(client.New(ts.URL, ""), shadow, recovered, nil, 1, 8, "/repo")
	recoveredQueue.SetLayerRef("layer-1")
	if got := recoveredQueue.RecoverPendingSync(context.Background()); got != 0 {
		t.Fatalf("restart recovered committed layer entries = %d, want 0", got)
	}
}

func TestRestoreLayerReplayDoesNotOverwriteNewerNamespaceMutation(t *testing.T) {
	tests := []struct {
		name            string
		mountedLayerID  string
		mutationLayerID string
		mutationSeq     int64
		replay          client.FSLayerEntry
		seed            func(*Dat9FS)
		mutate          func(*Dat9FS) gofuse.Status
		assertFinal     func(*testing.T, *Dat9FS)
	}{
		{
			name:   "mkdir-wins-over-stale-whiteout",
			replay: client.FSLayerEntry{LayerID: "layer-1", Path: "/repo/node", Op: "whiteout", Kind: "dir", EntrySeq: 1},
			mutate: func(fs *Dat9FS) gofuse.Status {
				return fs.Mkdir(nil, &gofuse.MkdirIn{InHeader: gofuse.InHeader{NodeId: 1}, Mode: 0o750}, "node", &gofuse.EntryOut{})
			},
			assertFinal: func(t *testing.T, fs *Dat9FS) {
				if fs.isLayerWhiteout("/node") {
					t.Fatal("stale replay whiteout replaced newer mkdir")
				}
				if mode, ok := fs.layerDirMode("/node"); !ok || mode != 0o750 {
					t.Fatalf("final layer directory = (%#o, %t), want 0750 true", mode, ok)
				}
			},
		},
		{
			name:   "rmdir-wins-over-stale-mkdir",
			replay: client.FSLayerEntry{LayerID: "layer-1", Path: "/repo/node", Op: "mkdir", Kind: "dir", Mode: 0o755, EntrySeq: 1},
			seed: func(fs *Dat9FS) {
				fs.markLayerDir("/node", 0o755)
				fs.inodes.Lookup("/node", true, 0, time.Now())
			},
			mutate: func(fs *Dat9FS) gofuse.Status {
				return fs.Rmdir(nil, &gofuse.InHeader{NodeId: 1}, "node")
			},
			assertFinal: func(t *testing.T, fs *Dat9FS) {
				if !fs.isLayerWhiteout("/node") {
					t.Fatal("stale replay mkdir replaced newer rmdir whiteout")
				}
				if _, ok := fs.layerDirMode("/node"); ok {
					t.Fatal("stale replay mkdir remained visible after newer rmdir")
				}
			},
		},
		{
			name:            "child-first-mkdir-wins-over-high-seq-parent-whiteout",
			mountedLayerID:  "child-1",
			mutationLayerID: "child-1",
			mutationSeq:     1,
			replay:          client.FSLayerEntry{LayerID: "parent-1", Path: "/repo/node", Op: "whiteout", Kind: "dir", EntrySeq: 100},
			mutate: func(fs *Dat9FS) gofuse.Status {
				return fs.Mkdir(nil, &gofuse.MkdirIn{InHeader: gofuse.InHeader{NodeId: 1}, Mode: 0o750}, "node", &gofuse.EntryOut{})
			},
			assertFinal: func(t *testing.T, fs *Dat9FS) {
				if fs.isLayerWhiteout("/node") {
					t.Fatal("high-sequence parent replay replaced first child-layer mkdir")
				}
				if mode, ok := fs.layerDirMode("/node"); !ok || mode != 0o750 {
					t.Fatalf("final child-layer directory = (%#o, %t), want 0750 true", mode, ok)
				}
			},
		},
		{
			name:            "child-first-symlink-wins-over-high-seq-parent-whiteout",
			mountedLayerID:  "child-1",
			mutationLayerID: "child-1",
			mutationSeq:     1,
			replay:          client.FSLayerEntry{LayerID: "parent-1", Path: "/repo/node", Op: "whiteout", Kind: "symlink", EntrySeq: 100},
			mutate: func(fs *Dat9FS) gofuse.Status {
				return fs.Symlink(nil, &gofuse.InHeader{NodeId: 1}, "target.txt", "node", &gofuse.EntryOut{})
			},
			assertFinal: func(t *testing.T, fs *Dat9FS) {
				if fs.isLayerWhiteout("/node") {
					t.Fatal("high-sequence parent replay replaced first child-layer symlink")
				}
				if target, _, ok := fs.layerSymlink("/node"); !ok || target != "target.txt" {
					t.Fatalf("final child-layer symlink = (%q, %t), want target.txt true", target, ok)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			replayFetched := make(chan struct{})
			allowReplay := make(chan struct{})
			testHookAfterLayerReplayFetched = func() {
				close(replayFetched)
				<-allowReplay
			}
			t.Cleanup(func() { testHookAfterLayerReplayFetched = nil })

			mountedLayerID := tc.mountedLayerID
			if mountedLayerID == "" {
				mountedLayerID = "layer-1"
			}
			mutationLayerID := tc.mutationLayerID
			if mutationLayerID == "" {
				mutationLayerID = mountedLayerID
			}
			mutationSeq := tc.mutationSeq
			if mutationSeq == 0 {
				mutationSeq = 2
			}
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/"+mountedLayerID+"/diff":
					_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FSLayerEntry{tc.replay}})
				case r.Method == http.MethodPost && r.URL.Path == "/v1/layers/"+mountedLayerID+"/entries":
					var req client.FSLayerEntryRequest
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Errorf("decode mutation: %v", err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					_ = json.NewEncoder(w).Encode(client.FSLayerEntry{LayerID: mutationLayerID, Path: req.Path, Op: req.Op, Kind: req.Kind, Mode: req.Mode, EntrySeq: mutationSeq})
				case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/repo/node" && r.URL.Query().Get("list") == "1":
					_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FileInfo{}})
				case r.Method == http.MethodHead:
					http.NotFound(w, r)
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
			opts := &MountOptions{LayerRef: mountedLayerID, RemoteRoot: "/repo"}
			fs := NewDat9FS(client.New(ts.URL, ""), opts)
			if tc.seed != nil {
				tc.seed(fs)
			}
			restoreDone := make(chan error, 1)
			go func() {
				restoreDone <- restoreLayerEntries(context.Background(), client.New(ts.URL, ""), opts, shadow, pending, fs)
			}()
			select {
			case <-replayFetched:
			case <-time.After(5 * time.Second):
				t.Fatal("restore did not fetch replay snapshot")
			}
			if st := tc.mutate(fs); st != gofuse.OK {
				t.Fatalf("newer local mutation status = %v, want OK", st)
			}
			close(allowReplay)
			if err := <-restoreDone; err != nil {
				t.Fatalf("restore stale replay: %v", err)
			}
			tc.assertFinal(t, fs)
		})
	}
}

func TestRestoreLayerReplayDoesNotOverwriteCommittedChildWriteback(t *testing.T) {
	for _, shadowSpill := range []bool{false, true} {
		name := "inline"
		if shadowSpill {
			name = "object"
		}
		t.Run(name, func(t *testing.T) {
			const (
				localPath  = "/node.txt"
				remotePath = "/repo/node.txt"
			)
			oldData := []byte("parent")
			newData := []byte("child")
			replayFetched := make(chan struct{})
			allowReplay := make(chan struct{})
			testHookAfterLayerReplayFetched = func() {
				close(replayFetched)
				<-allowReplay
			}
			t.Cleanup(func() { testHookAfterLayerReplayFetched = nil })

			var uploadCalls atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/child-1/diff":
					_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FSLayerEntry{{
						LayerID: "parent-1", Path: remotePath, Op: "upsert", Kind: "file",
						SizeBytes: int64(len(oldData)), EntrySeq: 100,
					}}})
				case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/parent-1/entries":
					if got := r.URL.Query().Get("max_seq"); got != "100" {
						t.Errorf("parent entry max_seq = %q, want 100", got)
					}
					_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
						LayerID: "parent-1", Path: remotePath, Op: "upsert", Kind: "file",
						Content: oldData, SizeBytes: int64(len(oldData)), EntrySeq: 100,
					})
				case r.Method == http.MethodPost && (r.URL.Path == "/v1/layers/child-1/entries" || r.URL.Path == "/v1/layers/child-1/objects"):
					if shadowSpill && r.URL.Path != "/v1/layers/child-1/objects" {
						t.Errorf("object writeback used %s", r.URL.Path)
					}
					if !shadowSpill && r.URL.Path != "/v1/layers/child-1/entries" {
						t.Errorf("inline writeback used %s", r.URL.Path)
					}
					uploadCalls.Add(1)
					_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
						LayerID: "child-1", Path: remotePath, Op: "upsert", Kind: "file",
						SizeBytes: int64(len(newData)), EntrySeq: 1,
					})
				case r.Method == http.MethodPost && r.URL.Path == "/v1/layers/child-1/uploads/initiate" && shadowSpill:
					// Exercise the object response path while keeping this test's
					// server minimal: 404 is the specified legacy-object fallback.
					http.NotFound(w, r)
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
			if err := shadow.WriteFull(localPath, newData, 0); err != nil {
				t.Fatal(err)
			}
			pendingGen, err := pending.PutWithBaseRev(localPath, int64(len(newData)), PendingOverwrite, 0)
			if err != nil {
				t.Fatal(err)
			}
			shadowGen := shadow.ActiveGeneration(localPath)

			opts := &MountOptions{LayerRef: "child-1", RemoteRoot: "/repo"}
			opts.setDefaults()
			c := client.New(ts.URL, "")
			fs := NewDat9FS(c, opts)
			fs.shadowStore = shadow
			fs.pendingIndex = pending
			cq := NewCommitQueue(c, shadow, pending, nil, 1, 8, opts.RemoteRoot)
			cq.SetLayerRef(opts.LayerRef)
			cq.OnUploaded = fs.onCommitQueueUploaded
			cq.OnSuccess = fs.onCommitQueueSuccess
			fs.commitQueue = cq
			defer cq.DrainAll()

			restoreDone := make(chan error, 1)
			go func() {
				restoreDone <- restoreLayerEntries(context.Background(), c, opts, shadow, pending, fs)
			}()
			select {
			case <-replayFetched:
			case <-time.After(5 * time.Second):
				t.Fatal("restore did not fetch parent replay")
			}

			if err := cq.Enqueue(&CommitEntry{
				Path: localPath, BaseRev: 0, Size: int64(len(newData)), Kind: PendingOverwrite,
				ShadowSpill: shadowSpill, ShadowGen: shadowGen, PendingIndexGen: pendingGen,
			}); err != nil {
				t.Fatal(err)
			}
			cq.WaitPath(localPath)
			if got := uploadCalls.Load(); got != 1 {
				t.Fatalf("child-layer uploads = %d, want 1", got)
			}
			if meta, ok := pending.GetMeta(localPath); !ok || !meta.LayerCommitted {
				t.Fatalf("child writeback pending meta = %+v, want committed", meta)
			}

			close(allowReplay)
			if err := <-restoreDone; err != nil {
				t.Fatalf("restore stale parent replay: %v", err)
			}
			got, err := shadow.ReadAll(localPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, newData) {
				t.Fatalf("shadow after stale parent replay = %q, want %q", got, newData)
			}
			fs.layerMu.RLock()
			version := fs.layerEntryVersion[localPath]
			fs.layerMu.RUnlock()
			if version != (layerEntryVersion{LayerID: "child-1", EntrySeq: 1}) {
				t.Fatalf("child writeback version = %+v, want child-1/1", version)
			}
		})
	}
}

func TestRestoreLayerDirectoryRenameProjectsSubtreeWithoutFileState(t *testing.T) {
	renameEntry := client.FSLayerEntry{
		LayerID: "layer-1", Path: "/repo/old/", Op: "rename", Kind: "dir",
		ContentText: "/repo/new/", Mode: 0o750, EntrySeq: 7,
	}
	childEntry := client.FSLayerEntry{
		LayerID: "layer-1", Path: "/repo/old/layer-child.txt", Op: "upsert", Kind: "file",
		Mode: 0o640, SizeBytes: 5, EntrySeq: 6,
	}
	var postCount int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/diff":
			if r.URL.Query().Get("replay") != "1" {
				t.Errorf("layer replay query = %q, want 1", r.URL.RawQuery)
			}
			// replay=1 is the causal effective log, not a path-sorted folded
			// projection: the file body is installed at the source prefix and
			// the later directory rename must carry it to the target.
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FSLayerEntry{childEntry, renameEntry}})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/entries" && r.URL.Query().Get("path") == "/repo/old/layer-child.txt":
			if got := r.URL.Query().Get("max_seq"); got != "6" {
				t.Errorf("layer entry max_seq = %q, want 6", got)
			}
			full := childEntry
			full.Content = []byte("layer")
			_ = json.NewEncoder(w).Encode(full)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/repo/new/main-child.txt":
			w.Header().Set("Content-Length", "5")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "11")
			w.Header().Set("X-Dat9-Mode", "420")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost:
			postCount++
			t.Errorf("committed restored state must not be replayed: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		default:
			t.Errorf("unexpected request (directory rename must not use file restore): %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	shadowDir := t.TempDir()
	pendingDir := t.TempDir()
	shadow, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.WriteFull("/old/retained.txt", []byte("retained"), 3); err != nil {
		t.Fatal(err)
	}
	retainedGen, err := pending.PutWithBaseRevAndMode("/old/retained.txt", int64(len("retained")), PendingOverwrite, 3, 0o600, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pending.MarkLayerCommitted("/old/retained.txt", retainedGen, 0); err != nil {
		t.Fatal(err)
	}
	opts := &MountOptions{LayerRef: "layer-1", RemoteRoot: "/repo"}
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	fs.markLayerDir("/old", 0o755)
	fs.markLayerFileMode("/old/retained.txt", 0o600)
	fs.inodes.Lookup("/old", true, 0, time.Now())
	fs.inodes.Lookup("/old/main-child.txt", false, 5, time.Now())

	if err := restoreLayerEntries(context.Background(), client.New(ts.URL, ""), opts, shadow, pending, fs); err != nil {
		t.Fatalf("restore directory rename: %v", err)
	}
	if !fs.isLayerWhiteout("/old") {
		t.Fatal("directory rename source whiteout missing")
	}
	if mode, ok := fs.layerDirMode("/new"); !ok || mode != 0o750 {
		t.Fatalf("directory rename target = (%#o, %t), want 0750 true", mode, ok)
	}
	if mode, ok := fs.layerFileMode("/new/retained.txt"); !ok || mode != 0o600 {
		t.Fatalf("retained descendant = (%#o, %t), want moved 0600 true", mode, ok)
	}
	if got, err := shadow.ReadAll("/new/retained.txt"); err != nil || string(got) != "retained" {
		t.Fatalf("retained descendant shadow = %q, %v; want retained", got, err)
	}
	if meta, ok := pending.GetMeta("/new/retained.txt"); !ok || !meta.LayerCommitted || meta.Path != "/new/retained.txt" {
		t.Fatalf("retained descendant pending meta = %+v, want committed target", meta)
	}
	if shadow.Has("/old/retained.txt") || pending.HasPending("/old/retained.txt") {
		t.Fatal("retained descendant cache remained under directory rename source")
	}
	if _, ok := fs.layerFileMode("/old/retained.txt"); ok {
		t.Fatal("retained descendant remained under rename source")
	}
	if mode, ok := fs.layerFileMode("/new/layer-child.txt"); !ok || mode != 0o640 {
		t.Fatalf("effective-log renamed child = (%#o, %t), want restored 0640 true", mode, ok)
	}
	if meta, ok := pending.GetMeta("/new/layer-child.txt"); !ok || !meta.LayerCommitted || meta.Generation == 0 {
		t.Fatalf("renamed child pending meta = %+v, want committed generation", meta)
	}
	if got, err := shadow.ReadAll("/new/layer-child.txt"); err != nil || string(got) != "layer" {
		t.Fatalf("renamed child shadow = %q, %v; want layer", got, err)
	}
	if pending.HasPending("/old") || pending.HasPending("/new") || shadow.Has("/old") || shadow.Has("/new") {
		t.Fatal("directory rename manufactured file shadow/pending state")
	}

	var oldOut gofuse.EntryOut
	if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "old", &oldOut); st != gofuse.ENOENT {
		t.Fatalf("lookup rename source = %v, want ENOENT", st)
	}
	var targetOut gofuse.EntryOut
	if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "new", &targetOut); st != gofuse.OK || targetOut.NodeId == 0 {
		t.Fatalf("lookup rename target = %v node=%d, want directory", st, targetOut.NodeId)
	}
	var childOut gofuse.EntryOut
	if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: targetOut.NodeId}, "main-child.txt", &childOut); st != gofuse.OK {
		t.Fatalf("lookup renamed main-backed child = %v, want OK", st)
	}

	// The directory rename must persist the committed descendant move, not
	// merely retarget the current process's in-memory layer projection.
	shadow.Close()
	reopenedShadow, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedShadow.Close()
	if err := reopenedShadow.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	reopenedPending, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopenedPending.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	if got, err := reopenedShadow.ReadAll("/new/layer-child.txt"); err != nil || string(got) != "layer" {
		t.Fatalf("reopened renamed child shadow = %q, %v; want layer", got, err)
	}
	if meta, ok := reopenedPending.GetMeta("/new/layer-child.txt"); !ok || !meta.LayerCommitted {
		t.Fatalf("reopened renamed child meta = %+v, want committed", meta)
	}
	for _, oldPath := range []string{"/old/layer-child.txt", "/old/retained.txt"} {
		if reopenedShadow.Has(oldPath) || reopenedPending.HasPending(oldPath) {
			t.Fatalf("reopened old prefix retained %s", oldPath)
		}
	}
	reopenedFS := NewDat9FS(client.New(ts.URL, ""), opts)
	reopenedCQ := NewCommitQueue(reopenedFS.client, reopenedShadow, reopenedPending, nil, 1, 8, opts.RemoteRoot)
	defer reopenedCQ.DrainAll()
	reopenedCQ.SetLayerRef(opts.LayerRef)
	reopenedFS.shadowStore = reopenedShadow
	reopenedFS.pendingIndex = reopenedPending
	reopenedFS.commitQueue = reopenedCQ
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	reopenedFS.drainLatePendingEntries(ctx)
	if postCount != 0 {
		t.Fatalf("late drain replay posts = %d, want 0", postCount)
	}
}

func TestRestoreLayerDirectoryRenamePreservesForkReplayOrderAndAncestorObject(t *testing.T) {
	objectData := []byte("ancestor object body")
	upsert := client.FSLayerEntry{
		LayerID: "parent-1", Path: "/repo/a/x.bin", Op: "upsert", Kind: "file",
		StorageType: "s3", StorageRef: "objects/parent-x", SizeBytes: int64(len(objectData)),
		Mode: 0o640, EntrySeq: 99,
	}
	parentRename := client.FSLayerEntry{
		LayerID: "parent-1", Path: "/repo/a/", Op: "rename", Kind: "dir",
		ContentText: "/repo/b/", Mode: 0o750, EntrySeq: 100,
	}
	childRename := client.FSLayerEntry{
		LayerID: "child-1", Path: "/repo/b/", Op: "rename", Kind: "dir",
		ContentText: "/repo/c/", Mode: 0o700, EntrySeq: 1,
	}
	var postCount int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/child-1/diff":
			if r.URL.Query().Get("replay") != "1" {
				t.Errorf("layer replay query = %q, want replay=1", r.URL.RawQuery)
			}
			// This is the real root-to-tip effective-log shape. Parent EntrySeq
			// 100 precedes child EntrySeq 1 because sequence domains are local
			// to each layer.
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FSLayerEntry{
				upsert, parentRename, childRename,
			}})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/parent-1/entries":
			if got := r.URL.Query().Get("path"); got != upsert.Path {
				t.Errorf("ancestor entry path = %q, want %q", got, upsert.Path)
			}
			if got := r.URL.Query().Get("max_seq"); got != "99" {
				t.Errorf("ancestor entry max_seq = %q, want 99", got)
			}
			_ = json.NewEncoder(w).Encode(upsert)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/parent-1/objects":
			if got := r.URL.Query().Get("path"); got != upsert.Path {
				t.Errorf("ancestor object path = %q, want %q", got, upsert.Path)
			}
			if got := r.URL.Query().Get("max_seq"); got != "99" {
				t.Errorf("ancestor object max_seq = %q, want 99", got)
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(objectData)
		case r.Method == http.MethodPost:
			postCount++
			t.Errorf("committed fork replay must not POST: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	shadowDir := t.TempDir()
	pendingDir := t.TempDir()
	shadow, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	opts := &MountOptions{LayerRef: "child-1", RemoteRoot: "/repo"}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	fs.markLayerDir("/a", 0o755)
	fs.inodes.Lookup("/a", true, 0, time.Now())
	fs.inodes.Lookup("/a/x.bin", false, int64(len(objectData)), time.Now())

	if err := restoreLayerEntries(context.Background(), fs.client, opts, shadow, pending, fs); err != nil {
		t.Fatalf("restore fork rename chain: %v", err)
	}
	for _, oldPath := range []string{"/a", "/b"} {
		if !fs.isLayerWhiteout(oldPath) {
			t.Fatalf("rename chain source %s is not hidden", oldPath)
		}
	}
	if mode, ok := fs.layerDirMode("/c"); !ok || mode != 0o700 {
		t.Fatalf("rename chain target = (%#o, %t), want 0700 true", mode, ok)
	}
	if mode, ok := fs.layerFileMode("/c/x.bin"); !ok || mode != 0o640 {
		t.Fatalf("rename chain child = (%#o, %t), want 0640 true", mode, ok)
	}
	if got, err := shadow.ReadAll("/c/x.bin"); err != nil || !bytes.Equal(got, objectData) {
		t.Fatalf("rename chain object = %q, %v; want %q", got, err, objectData)
	}
	if meta, ok := pending.GetMeta("/c/x.bin"); !ok || !meta.LayerCommitted || meta.Path != "/c/x.bin" {
		t.Fatalf("rename chain pending meta = %+v, want committed /c/x.bin", meta)
	}
	for _, oldPath := range []string{"/a/x.bin", "/b/x.bin"} {
		if shadow.Has(oldPath) || pending.HasPending(oldPath) {
			t.Fatalf("rename chain retained old persistent path %s", oldPath)
		}
	}
	if _, ok := fs.inodes.GetInode("/c/x.bin"); !ok {
		t.Fatal("rename chain did not retarget child inode")
	}

	shadow.Close()
	reopenedShadow, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedShadow.Close()
	if err := reopenedShadow.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	reopenedPending, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopenedPending.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	if got, err := reopenedShadow.ReadAll("/c/x.bin"); err != nil || !bytes.Equal(got, objectData) {
		t.Fatalf("reopened rename chain object = %q, %v; want %q", got, err, objectData)
	}
	if meta, ok := reopenedPending.GetMeta("/c/x.bin"); !ok || !meta.LayerCommitted {
		t.Fatalf("reopened rename chain meta = %+v, want committed", meta)
	}
	for _, oldPath := range []string{"/a/x.bin", "/b/x.bin"} {
		if reopenedShadow.Has(oldPath) || reopenedPending.HasPending(oldPath) {
			t.Fatalf("reopened rename chain retained old path %s", oldPath)
		}
	}
	reopenedFS := NewDat9FS(client.New(ts.URL, ""), opts)
	reopenedCQ := NewCommitQueue(reopenedFS.client, reopenedShadow, reopenedPending, nil, 1, 8, opts.RemoteRoot)
	defer reopenedCQ.DrainAll()
	reopenedCQ.SetLayerRef(opts.LayerRef)
	reopenedFS.shadowStore = reopenedShadow
	reopenedFS.pendingIndex = reopenedPending
	reopenedFS.commitQueue = reopenedCQ
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	reopenedFS.drainLatePendingEntries(ctx)
	if postCount != 0 {
		t.Fatalf("late drain replay posts = %d, want 0", postCount)
	}
}

func TestRestoreLayerDirectoryRenamePreservesDirtyDescendant(t *testing.T) {
	entry := client.FSLayerEntry{
		LayerID: "layer-1", Path: "/repo/from", Op: "rename", Kind: "dir",
		ContentText: "/repo/to", Mode: 0o750, EntrySeq: 7,
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/diff" {
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FSLayerEntry{entry}})
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
		w.WriteHeader(http.StatusInternalServerError)
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
	if err := shadow.WriteFull("/from/dirty.txt", []byte("local"), 3); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRevAndMode("/from/dirty.txt", 5, PendingOverwrite, 3, 0o600, true); err != nil {
		t.Fatal(err)
	}
	opts := &MountOptions{LayerRef: "layer-1", RemoteRoot: "/repo"}
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	fs.markLayerDir("/from", 0o755)
	if err := restoreLayerEntries(context.Background(), client.New(ts.URL, ""), opts, shadow, pending, fs); err != nil {
		t.Fatalf("restore directory rename: %v", err)
	}
	if fs.isLayerWhiteout("/from") {
		t.Fatal("peer directory rename hid a local unfinished descendant")
	}
	if _, ok := fs.layerDirMode("/to"); ok {
		t.Fatal("peer directory rename published target over a local unfinished descendant")
	}
	if got, err := shadow.ReadAll("/from/dirty.txt"); err != nil || string(got) != "local" {
		t.Fatalf("dirty descendant = %q, %v; want local", got, err)
	}
}

func TestRestoreLayerDirectoryRenameRetargetsOpenDirtyChild(t *testing.T) {
	entry := client.FSLayerEntry{
		LayerID: "layer-1", Path: "/repo/old", Op: "rename", Kind: "dir",
		ContentText: "/repo/new", Mode: 0o750, EntrySeq: 7,
	}
	payload := []byte("dirty child")
	var (
		mu       sync.Mutex
		requests []clientLayerEntryRequest
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/diff":
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FSLayerEntry{entry}})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/layers/layer-1/entries":
			var req clientLayerEntryRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode layer child flush: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			mu.Lock()
			requests = append(requests, req)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
				LayerID: "layer-1", Path: req.Path, Op: req.Op, Kind: req.Kind,
				Content: req.Content, SizeBytes: req.SizeBytes, EntrySeq: 8,
			})
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{LayerRef: "layer-1", RemoteRoot: "/repo", WritePolicy: WritePolicyCloseSync}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.markLayerDir("/old", 0o755)
	fs.inodes.Lookup("/old", true, 0, time.Now())
	ino := fs.inodes.Lookup("/old/child.txt", false, int64(len(payload)), time.Now())
	dirty := NewWriteBuffer("/old/child.txt", maxPreloadSize, 0)
	if _, err := dirty.Write(0, payload); err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{
		Ino: ino, Path: "/old/child.txt", Dirty: dirty,
		DirtySeq: fs.markDirtySize(ino, int64(len(payload))), IsNew: true,
		WritePolicy: WritePolicyCloseSync,
	}
	fhID := fs.allocateFileHandle(fh)
	defer fs.deleteFileHandle(fhID, fh)

	if err := restoreLayerEntries(context.Background(), fs.client, opts, shadow, pending, fs); err != nil {
		t.Fatalf("restore peer directory rename: %v", err)
	}
	if got, ok := fs.inodes.GetPath(ino); !ok || got != "/new/child.txt" {
		t.Fatalf("open child inode path = (%q, %t), want /new/child.txt", got, ok)
	}
	fh.Lock()
	gotHandlePath := fh.Path
	gotDirtyPath := fh.Dirty.path
	fh.Unlock()
	if gotHandlePath != "/new/child.txt" || gotDirtyPath != "/new/child.txt" {
		t.Fatalf("retargeted handle paths = (%q, %q), want /new/child.txt", gotHandlePath, gotDirtyPath)
	}

	if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: fhID}); st != gofuse.OK {
		t.Fatalf("Flush renamed dirty child = %v, want OK", st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: fhID})

	mu.Lock()
	got := append([]clientLayerEntryRequest(nil), requests...)
	mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("layer child flushes = %+v, want one", got)
	}
	if got[0].Path != "/repo/new/child.txt" || !bytes.Equal(got[0].Content, payload) {
		t.Fatalf("renamed child request = %+v, want /repo/new/child.txt with %q", got[0], payload)
	}
	if got[0].Path == "/repo/old/child.txt" {
		t.Fatal("peer directory rename allowed open handle to recreate the old path")
	}
}

func TestRestoreLayerEntriesRejectsCheckpointLayerMismatch(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layer-checkpoints/cp-mismatch":
			_ = json.NewEncoder(w).Encode(client.FSLayerCheckpoint{
				CheckpointID: "cp-mismatch",
				LayerID:      "layer-2",
				DurableSeq:   7,
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
	err = restoreLayerEntries(context.Background(), client.New(ts.URL, ""), &MountOptions{
		LayerRef:      "layer-1",
		CheckpointRef: "cp-mismatch",
		RemoteRoot:    "/repo",
	}, shadow, pending, nil)
	if err == nil {
		t.Fatal("restoreLayerEntries error = nil, want checkpoint layer mismatch")
	}
	if !strings.Contains(err.Error(), "checkpoint cp-mismatch belongs to layer layer-2, want layer-1") {
		t.Fatalf("restoreLayerEntries error = %v", err)
	}
}

func TestMountRejectsCheckpointLayerMismatchBeforeFuseServer(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/repo":
			w.Header().Set("X-Dat9-IsDir", "true")
			w.Header().Set("X-Dat9-Revision", "1")
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/fix-auth-bug":
			_ = json.NewEncoder(w).Encode(client.FSLayer{
				LayerID:        "lyr_abc",
				BaseRootPath:   "/repo",
				Name:           "fix-auth-bug",
				State:          "active",
				DurabilityMode: "restore-safe",
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layer-checkpoints/cp_before_refactor":
			_ = json.NewEncoder(w).Encode(client.FSLayerCheckpoint{
				CheckpointID: "cp_before_refactor",
				LayerID:      "lyr_other",
				DurableSeq:   3,
			})
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	err := Mount(&MountOptions{
		Server:        ts.URL,
		APIKey:        "sk-test",
		MountPoint:    t.TempDir(),
		RemoteRoot:    "/repo",
		CacheDir:      t.TempDir(),
		LayerRef:      "fix-auth-bug",
		CheckpointRef: "cp_before_refactor",
	})
	if err == nil {
		t.Fatal("Mount error = nil, want checkpoint layer mismatch")
	}
	want := "mount: restore fs layer entries: checkpoint cp_before_refactor belongs to layer lyr_other, want lyr_abc"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("Mount error = %v, want to contain %q", err, want)
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
	if meta.Size != 4 || !meta.HasMode || meta.Mode != 0o600 || !meta.LayerCommitted {
		t.Fatalf("pending meta = %+v, want committed size=4 mode=0600", meta)
	}
	if mode, ok := fs.layerFileMode("/new.txt"); !ok || mode != 0o600 {
		t.Fatalf("layer file mode = (%#o, %t), want 0600 true", mode, ok)
	}
}

func TestRestoreLayerEntriesRefreshesCommittedOverlayButPreservesUncommittedGeneration(t *testing.T) {
	const localPath = "/shared.txt"
	var entryFetches int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/diff":
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FSLayerEntry{{
				LayerID: "layer-1", Path: "/repo/shared.txt", Op: "upsert", Kind: "file",
				SizeBytes: 2, BaseRevision: 9, Mode: 0o640, EntrySeq: 2,
			}}})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/entries":
			entryFetches++
			_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
				LayerID: "layer-1", Path: "/repo/shared.txt", Op: "upsert", Kind: "file",
				Content: []byte("v2"), SizeBytes: 2, BaseRevision: 9, Mode: 0o640, EntrySeq: 2,
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
	if err := shadow.WriteFull(localPath, []byte("v1"), 8); err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gen, err := pending.PutWithBaseRevAndMode(localPath, 2, PendingOverwrite, 8, 0o644, true)
	if err != nil {
		t.Fatal(err)
	}
	if marked, err := pending.MarkLayerCommitted(localPath, gen, 8); err != nil || !marked {
		t.Fatalf("mark v1 committed = (%t, %v), want true, nil", marked, err)
	}
	opts := &MountOptions{LayerRef: "layer-1", RemoteRoot: "/repo"}
	if err := restoreLayerEntries(context.Background(), client.New(ts.URL, ""), opts, shadow, pending, nil); err != nil {
		t.Fatalf("refresh committed overlay: %v", err)
	}
	if got, err := shadow.ReadAll(localPath); err != nil || !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("refreshed committed overlay = %q, %v; want v2", got, err)
	}
	if meta, ok := pending.GetMeta(localPath); !ok || !meta.LayerCommitted || meta.BaseRev != 9 || meta.Mode != 0o640 {
		t.Fatalf("refreshed committed metadata = %+v", meta)
	}

	// A later unfinished local generation owns the path and must fence the
	// same replay result instead of being overwritten by server cache refresh.
	if err := shadow.WriteFull(localPath, []byte("local"), 9); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRevAndMode(localPath, 5, PendingOverwrite, 9, 0o600, true); err != nil {
		t.Fatal(err)
	}
	if err := restoreLayerEntries(context.Background(), client.New(ts.URL, ""), opts, shadow, pending, nil); err != nil {
		t.Fatalf("refresh with unfinished local generation: %v", err)
	}
	if got, err := shadow.ReadAll(localPath); err != nil || !bytes.Equal(got, []byte("local")) {
		t.Fatalf("unfinished local generation after refresh = %q, %v; want local", got, err)
	}
	if meta, ok := pending.GetMeta(localPath); !ok || meta.LayerCommitted || meta.Size != 5 || meta.Mode != 0o600 {
		t.Fatalf("unfinished local metadata after refresh = %+v", meta)
	}
	if entryFetches != 1 {
		t.Fatalf("layer entry fetches = %d, want 1 (committed refresh only)", entryFetches)
	}
}

func TestRestoreLayerEntriesDoesNotOverwriteConcurrentUncommittedGeneration(t *testing.T) {
	const localPath = "/shared.txt"
	fetchStarted := make(chan struct{})
	allowResponse := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/diff":
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FSLayerEntry{{
				LayerID: "layer-1", Path: "/repo/shared.txt", Op: "upsert", Kind: "file",
				SizeBytes: 2, BaseRevision: 9, Mode: 0o640, EntrySeq: 2,
			}}})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/entries":
			close(fetchStarted)
			<-allowResponse
			_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
				LayerID: "layer-1", Path: "/repo/shared.txt", Op: "upsert", Kind: "file",
				Content: []byte("v2"), SizeBytes: 2, BaseRevision: 9, Mode: 0o640, EntrySeq: 2,
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
	if err := shadow.WriteFull(localPath, []byte("v1"), 8); err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gen, err := pending.PutWithBaseRevAndMode(localPath, 2, PendingOverwrite, 8, 0o644, true)
	if err != nil {
		t.Fatal(err)
	}
	if marked, err := pending.MarkLayerCommitted(localPath, gen, 8); err != nil || !marked {
		t.Fatalf("mark v1 committed = (%t, %v), want true, nil", marked, err)
	}

	opts := &MountOptions{LayerRef: "layer-1", RemoteRoot: "/repo"}
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	restoreDone := make(chan error, 1)
	go func() {
		restoreDone <- restoreLayerEntries(context.Background(), client.New(ts.URL, ""), opts, shadow, pending, fs)
	}()
	select {
	case <-fetchStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("restore did not begin fetching the refreshed layer entry")
	}

	// Model the normal writer lock discipline while the network fetch is in
	// flight. Restore must recheck ownership after that fetch, not overwrite
	// this newer unfinished generation using its stale pre-fetch observation.
	unlock := fs.lockRemoteCommitPath(localPath)
	if err := shadow.WriteFull(localPath, []byte("local"), 8); err != nil {
		unlock()
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRevAndMode(localPath, 5, PendingOverwrite, 8, 0o600, true); err != nil {
		unlock()
		t.Fatal(err)
	}
	unlock()
	close(allowResponse)
	if err := <-restoreDone; err != nil {
		t.Fatalf("restore with concurrent local generation: %v", err)
	}
	if got, err := shadow.ReadAll(localPath); err != nil || !bytes.Equal(got, []byte("local")) {
		t.Fatalf("concurrent local generation after refresh = %q, %v; want local", got, err)
	}
	if meta, ok := pending.GetMeta(localPath); !ok || meta.LayerCommitted || meta.Size != 5 || meta.Mode != 0o600 {
		t.Fatalf("concurrent local metadata after refresh = %+v", meta)
	}
}

func TestRestoreLayerEntriesSlowObjectDoesNotOverwriteLockTimeoutWriter(t *testing.T) {
	const localPath = "/shared.bin"
	streamStarted := make(chan struct{})
	allowStream := make(chan struct{})
	var allowStreamOnce sync.Once
	releaseStream := func() { allowStreamOnce.Do(func() { close(allowStream) }) }
	defer releaseStream()
	entry := client.FSLayerEntry{
		LayerID: "layer-1", Path: "/repo/shared.bin", Op: "upsert", Kind: "file",
		StorageType: "s3", StorageRef: "layers/layer-1/shared", SizeBytes: 6,
		BaseRevision: 9, Mode: 0o640, EntrySeq: 2,
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/diff":
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FSLayerEntry{entry}})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/entries":
			_ = json.NewEncoder(w).Encode(entry)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/objects":
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(http.StatusOK)
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			close(streamStarted)
			<-allowStream
			_, _ = w.Write([]byte("remote"))
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
	if err := shadow.WriteFull(localPath, []byte("v1"), 8); err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gen, err := pending.PutWithBaseRevAndMode(localPath, 2, PendingOverwrite, 8, 0o644, true)
	if err != nil {
		t.Fatal(err)
	}
	if marked, err := pending.MarkLayerCommitted(localPath, gen, 8); err != nil || !marked {
		t.Fatalf("mark v1 committed = (%t, %v), want true, nil", marked, err)
	}

	opts := &MountOptions{
		LayerRef:                "layer-1",
		RemoteRoot:              "/repo",
		RemoteCommitWaitTimeout: 20 * time.Millisecond,
	}
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	restoreDone := make(chan error, 1)
	go func() {
		restoreDone <- restoreLayerEntries(context.Background(), client.New(ts.URL, ""), opts, shadow, pending, fs)
	}()
	select {
	case <-streamStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("restore did not begin the slow object stream")
	}

	writerDone := make(chan error, 1)
	go func() {
		unlockMutation, ok := fs.lockPromotionMutation()
		if !ok {
			writerDone <- syscall.EIO
			return
		}
		defer unlockMutation()
		unlock := fs.lockWritableRemoteCommitPath(localPath)
		defer unlock()
		if err := shadow.WriteFull(localPath, []byte("local"), 8); err != nil {
			writerDone <- err
			return
		}
		_, err := pending.PutWithBaseRevAndMode(localPath, 5, PendingOverwrite, 8, 0o600, true)
		writerDone <- err
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		got, readErr := shadow.ReadAll(localPath)
		if readErr == nil && bytes.Equal(got, []byte("local")) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lock-timeout writer did not bypass replay lock; shadow=%q err=%v", got, readErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	releaseStream()
	if err := <-restoreDone; err != nil {
		t.Fatalf("restore with lock-timeout writer: %v", err)
	}
	if err := <-writerDone; err != nil {
		t.Fatalf("lock-timeout writer: %v", err)
	}
	if got, err := shadow.ReadAll(localPath); err != nil || !bytes.Equal(got, []byte("local")) {
		t.Fatalf("lock-timeout local generation after restore = %q, %v; want local", got, err)
	}
	if meta, ok := pending.GetMeta(localPath); !ok || meta.LayerCommitted || meta.Size != 5 || meta.Mode != 0o600 {
		t.Fatalf("lock-timeout local metadata after restore = %+v", meta)
	}
}

func TestRestoreLayerChmodPublishesViewBeforeQueuedWriter(t *testing.T) {
	const localPath = "/shared.txt"
	entry := client.FSLayerEntry{
		LayerID: "layer-1", Path: "/repo/shared.txt", Op: "chmod", Kind: "file",
		Mode: 0o640, EntrySeq: 2,
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet || r.URL.Path != "/v1/layers/layer-1/diff" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FSLayerEntry{entry}})
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
	gen, err := pending.PutWithBaseRevAndMode(localPath, 1, PendingOverwrite, 8, 0o600, true)
	if err != nil {
		t.Fatal(err)
	}
	if marked, err := pending.MarkLayerCommitted(localPath, gen, 8); err != nil || !marked {
		t.Fatalf("mark committed = (%t, %v)", marked, err)
	}

	opts := &MountOptions{LayerRef: "layer-1", RemoteRoot: "/repo"}
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	fs.markLayerFileMode(localPath, 0o600)
	publishReached := make(chan struct{})
	allowPublish := make(chan struct{})
	testHookBeforeLayerRestoreViewPublish = func(paths []string) {
		if len(paths) == 1 && paths[0] == localPath {
			close(publishReached)
			<-allowPublish
		}
	}
	t.Cleanup(func() { testHookBeforeLayerRestoreViewPublish = nil })

	restoreDone := make(chan error, 1)
	go func() {
		restoreDone <- restoreLayerEntries(context.Background(), client.New(ts.URL, ""), opts, shadow, pending, fs)
	}()
	select {
	case <-publishReached:
	case <-time.After(5 * time.Second):
		t.Fatal("peer chmod did not reach view publication")
	}

	writerStarted := make(chan struct{})
	writerAcquired := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		close(writerStarted)
		unlock, ok := fs.lockPromotionMutation()
		if !ok {
			return
		}
		close(writerAcquired)
		fs.markLayerFileMode(localPath, 0o660)
		fs.readCache.Put(localPath, []byte("newer"), 9)
		unlock()
		close(writerDone)
	}()
	<-writerStarted
	select {
	case <-writerAcquired:
		t.Fatal("queued chmod writer passed lifecycle barrier before peer view publication")
	case <-time.After(50 * time.Millisecond):
	}
	close(allowPublish)
	if err := <-restoreDone; err != nil {
		t.Fatalf("restore peer chmod: %v", err)
	}
	select {
	case <-writerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("queued chmod writer did not resume")
	}
	if mode, ok := fs.layerFileMode(localPath); !ok || mode != 0o660 {
		t.Fatalf("visible mode after queued chmod = (%#o, %t), want writer mode 0660", mode, ok)
	}
	if got, ok := fs.readCache.Get(localPath, 9); !ok || string(got) != "newer" {
		t.Fatalf("writer cache after peer chmod = %q, %t; peer invalidation ran after writer", got, ok)
	}
}

func TestRestoreLayerRenamePublishesBothPathsBeforeQueuedWriter(t *testing.T) {
	entry := client.FSLayerEntry{
		LayerID: "layer-1", Path: "/repo/from.txt", Op: "rename", Kind: "file",
		ContentText: "/repo/to.txt", Mode: 0o640, EntrySeq: 2,
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet || r.URL.Path != "/v1/layers/layer-1/diff" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FSLayerEntry{entry}})
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	if err := shadow.WriteFull("/from.txt", []byte("peer"), 8); err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gen, err := pending.PutWithBaseRevAndMode("/from.txt", 4, PendingOverwrite, 8, 0o600, true)
	if err != nil {
		t.Fatal(err)
	}
	if marked, err := pending.MarkLayerCommitted("/from.txt", gen, 8); err != nil || !marked {
		t.Fatalf("mark committed = (%t, %v)", marked, err)
	}

	opts := &MountOptions{LayerRef: "layer-1", RemoteRoot: "/repo"}
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	fs.markLayerFileMode("/from.txt", 0o600)
	publishReached := make(chan struct{})
	allowPublish := make(chan struct{})
	testHookBeforeLayerRestoreViewPublish = func(paths []string) {
		if len(paths) == 2 && paths[0] == "/from.txt" && paths[1] == "/to.txt" {
			close(publishReached)
			<-allowPublish
		}
	}
	t.Cleanup(func() { testHookBeforeLayerRestoreViewPublish = nil })

	restoreDone := make(chan error, 1)
	go func() {
		restoreDone <- restoreLayerEntries(context.Background(), client.New(ts.URL, ""), opts, shadow, pending, fs)
	}()
	select {
	case <-publishReached:
	case <-time.After(5 * time.Second):
		t.Fatal("peer rename did not reach view publication")
	}

	writerStarted := make(chan struct{})
	writerAcquired := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		close(writerStarted)
		unlock, ok := fs.lockPromotionMutation()
		if !ok {
			writerDone <- syscall.EIO
			return
		}
		defer unlock()
		close(writerAcquired)
		if err := shadow.WriteFull("/from.txt", []byte("newer"), 9); err != nil {
			writerDone <- err
			return
		}
		if _, err := pending.PutWithBaseRevAndMode("/from.txt", 5, PendingOverwrite, 9, 0o660, true); err != nil {
			writerDone <- err
			return
		}
		fs.markLayerFileMode("/from.txt", 0o660)
		fs.markLayerWhiteout("/to.txt")
		fs.readCache.Put("/from.txt", []byte("newer"), 9)
		writerDone <- nil
	}()
	<-writerStarted
	select {
	case <-writerAcquired:
		t.Fatal("queued rename writer passed lifecycle barrier before peer source/target publication")
	case <-time.After(50 * time.Millisecond):
	}
	close(allowPublish)
	if err := <-restoreDone; err != nil {
		t.Fatalf("restore peer rename: %v", err)
	}
	if err := <-writerDone; err != nil {
		t.Fatalf("queued rename writer: %v", err)
	}
	if fs.isLayerWhiteout("/from.txt") {
		t.Fatal("peer rename whiteout overwrote later local source generation")
	}
	if !fs.isLayerWhiteout("/to.txt") {
		t.Fatal("later local target whiteout was overwritten by peer rename")
	}
	if mode, ok := fs.layerFileMode("/from.txt"); !ok || mode != 0o660 {
		t.Fatalf("visible source mode after queued rename = (%#o, %t), want 0660", mode, ok)
	}
	if got, ok := fs.readCache.Get("/from.txt", 9); !ok || string(got) != "newer" {
		t.Fatalf("writer cache after peer rename = %q, %t; peer invalidation ran after writer", got, ok)
	}
}

func TestRestoreLayerChmodPreservesUncommittedGeneration(t *testing.T) {
	const localPath = "/shared.txt"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/diff":
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FSLayerEntry{{
				LayerID: "layer-1", Path: "/repo/shared.txt", Op: "chmod", Kind: "file", Mode: 0o640, EntrySeq: 2,
			}}})
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
	if err := shadow.WriteFull(localPath, []byte("local"), 8); err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gen, err := pending.PutWithBaseRevAndMode(localPath, 5, PendingOverwrite, 8, 0o600, true)
	if err != nil {
		t.Fatal(err)
	}
	fs := NewDat9FS(client.New(ts.URL, ""), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/repo"})
	fs.markLayerFileMode(localPath, 0o600)
	if err := restoreLayerEntries(context.Background(), client.New(ts.URL, ""), &MountOptions{
		LayerRef: "layer-1", RemoteRoot: "/repo",
	}, shadow, pending, fs); err != nil {
		t.Fatalf("restore peer chmod: %v", err)
	}
	meta, ok := pending.GetMeta(localPath)
	if !ok || meta.Generation != gen || meta.LayerCommitted || meta.Mode != 0o600 {
		t.Fatalf("unfinished generation after peer chmod = %+v, want gen=%d uncommitted mode=0600", meta, gen)
	}
	if mode, ok := fs.layerFileMode(localPath); !ok || mode != 0o600 {
		t.Fatalf("visible layer mode after skipped peer chmod = (%#o, %t), want 0600 true", mode, ok)
	}
}

func TestRestoreLayerRenamePreservesDirtySourceAndTarget(t *testing.T) {
	for _, dirtyPath := range []string{"source", "target"} {
		t.Run(dirtyPath, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/diff":
					_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FSLayerEntry{{
						LayerID: "layer-1", Path: "/repo/from.txt", Op: "rename", Kind: "file",
						ContentText: "/repo/to.txt", Mode: 0o644, EntrySeq: 2,
					}}})
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
			seed := func(path string, data string, committed bool) {
				t.Helper()
				if err := shadow.WriteFull(path, []byte(data), 8); err != nil {
					t.Fatal(err)
				}
				gen, err := pending.PutWithBaseRevAndMode(path, int64(len(data)), PendingOverwrite, 8, 0o600, true)
				if err != nil {
					t.Fatal(err)
				}
				if committed {
					if marked, err := pending.MarkLayerCommitted(path, gen, 8); err != nil || !marked {
						t.Fatalf("mark %s committed = (%t, %v)", path, marked, err)
					}
				}
			}
			seed("/from.txt", "source", dirtyPath != "source")
			seed("/to.txt", "target", dirtyPath != "target")
			fs := NewDat9FS(client.New(ts.URL, ""), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/repo"})
			if err := restoreLayerEntries(context.Background(), client.New(ts.URL, ""), &MountOptions{
				LayerRef: "layer-1", RemoteRoot: "/repo",
			}, shadow, pending, fs); err != nil {
				t.Fatalf("restore peer rename: %v", err)
			}
			for path, want := range map[string]string{"/from.txt": "source", "/to.txt": "target"} {
				got, err := shadow.ReadAll(path)
				if err != nil || string(got) != want {
					t.Fatalf("%s after peer rename = %q, %v; want %q", path, got, err, want)
				}
			}
			if fs.isLayerWhiteout("/from.txt") {
				t.Fatal("skipped peer rename installed source whiteout over local generation")
			}
			dirtyLocalPath := "/from.txt"
			if dirtyPath == "target" {
				dirtyLocalPath = "/to.txt"
			}
			if meta, ok := pending.GetMeta(dirtyLocalPath); !ok || meta.LayerCommitted {
				t.Fatalf("dirty %s metadata after peer rename = %+v, want unfinished", dirtyPath, meta)
			}
		})
	}
}

func TestRestoreLayerRenameDoesNotOverwriteTargetWrittenDuringEntryFetch(t *testing.T) {
	fetchStarted := make(chan struct{})
	allowFetch := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/diff":
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FSLayerEntry{{
				LayerID: "layer-1", Path: "/repo/from.txt", Op: "rename", Kind: "file", EntrySeq: 2,
			}}})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/entries":
			close(fetchStarted)
			<-allowFetch
			_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
				LayerID: "layer-1", Path: "/repo/from.txt", Op: "rename", Kind: "file",
				ContentText: "/repo/to.txt", Mode: 0o644, EntrySeq: 2,
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
	if err := shadow.WriteFull("/from.txt", []byte("source"), 8); err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gen, err := pending.PutWithBaseRevAndMode("/from.txt", 6, PendingOverwrite, 8, 0o644, true)
	if err != nil {
		t.Fatal(err)
	}
	if marked, err := pending.MarkLayerCommitted("/from.txt", gen, 8); err != nil || !marked {
		t.Fatalf("mark source committed = (%t, %v)", marked, err)
	}
	opts := &MountOptions{LayerRef: "layer-1", RemoteRoot: "/repo"}
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	restoreDone := make(chan error, 1)
	go func() {
		restoreDone <- restoreLayerEntries(context.Background(), client.New(ts.URL, ""), opts, shadow, pending, fs)
	}()
	select {
	case <-fetchStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("rename restore did not begin fetching the authoritative tuple")
	}
	if err := shadow.WriteFull("/to.txt", []byte("local-target"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRevAndMode("/to.txt", 12, PendingNew, 0, 0o600, true); err != nil {
		t.Fatal(err)
	}
	close(allowFetch)
	if err := <-restoreDone; err != nil {
		t.Fatalf("restore peer rename: %v", err)
	}
	if got, err := shadow.ReadAll("/to.txt"); err != nil || string(got) != "local-target" {
		t.Fatalf("concurrent target after peer rename = %q, %v; want local-target", got, err)
	}
	if meta, ok := pending.GetMeta("/to.txt"); !ok || meta.LayerCommitted || meta.Size != 12 {
		t.Fatalf("concurrent target metadata after peer rename = %+v", meta)
	}
	if fs.isLayerWhiteout("/from.txt") {
		t.Fatal("skipped concurrent peer rename installed source whiteout")
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
	if meta, ok := pending.GetMeta("/large.bin"); !ok || meta.Size != int64(len(payload)) || !meta.HasMode || meta.Mode != 0o640 || !meta.LayerCommitted {
		t.Fatalf("pending meta = %+v, want committed size %d mode 0640", meta, len(payload))
	}
}

func TestRestoreLayerEntriesUsesAncestorVersionDomainUnderChildCheckpoint(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layer-checkpoints/cp-child":
			_ = json.NewEncoder(w).Encode(client.FSLayerCheckpoint{
				CheckpointID: "cp-child",
				LayerID:      "child-1",
				DurableSeq:   0,
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/child-1/diff":
			if r.URL.Query().Get("max_seq") != "0" {
				t.Errorf("diff max_seq = %q, want 0", r.URL.Query().Get("max_seq"))
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"entries": []client.FSLayerEntry{
					{LayerID: "parent-1", Path: "/repo/a.txt", Op: "upsert", Kind: "file", SizeBytes: 2, EntrySeq: 10},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/parent-1/entries":
			if r.URL.Query().Get("path") != "/repo/a.txt" {
				t.Errorf("unexpected entry path: %q", r.URL.Query().Get("path"))
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if r.URL.Query().Get("max_seq") != "10" {
				t.Errorf("entry max_seq = %q, want ancestor operation seq 10", r.URL.Query().Get("max_seq"))
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
				LayerID:   "parent-1",
				Path:      "/repo/a.txt",
				Op:        "upsert",
				Kind:      "file",
				Content:   []byte("v1"),
				SizeBytes: 2,
				EntrySeq:  10,
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
	if err := restoreLayerEntries(context.Background(), client.New(ts.URL, ""), &MountOptions{
		LayerRef:      "child-1",
		CheckpointRef: "cp-child",
		RemoteRoot:    "/repo",
	}, shadow, pending, nil); err != nil {
		t.Fatalf("restoreLayerEntries: %v", err)
	}
	got, err := shadow.ReadAll("/a.txt")
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("restored = %q, want v1", got)
	}
}

func TestFSLayerMountStateAllowed(t *testing.T) {
	if !fsLayerMountStateAllowed("active", false) {
		t.Fatal("writable mount should allow active")
	}
	if fsLayerMountStateAllowed("sealed", false) {
		t.Fatal("writable mount should reject sealed")
	}
	if fsLayerMountStateAllowed("committed", false) {
		t.Fatal("writable mount should reject committed")
	}
	if !fsLayerMountStateAllowed("sealed", true) || !fsLayerMountStateAllowed("committed", true) {
		t.Fatal("checkpoint mount should allow sealed and committed")
	}
	if fsLayerMountStateAllowed("abandoned", true) || fsLayerMountStateAllowed("committing", true) {
		t.Fatal("checkpoint mount should reject abandoned and committing")
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
	if !ok || meta.Kind != PendingOverwrite || !meta.LayerCommitted || !meta.HasMode || meta.Mode != 0o600 {
		t.Fatalf("pending mode = %+v, want committed PendingOverwrite 0600", meta)
	}
	if mode, ok := fs.layerFileMode("/new.txt"); !ok || mode != 0o600 {
		t.Fatalf("layer file mode = (%#o, %t), want 0600 true", mode, ok)
	}
	if got, want := out.Mode&0o777, uint32(0o600); got != want {
		t.Fatalf("SetAttr output mode = %o, want %o", got, want)
	}
}

func TestLayerSetAttrModeFailureLeavesNewGenerationRecoverable(t *testing.T) {
	const (
		localPath  = "/committed.txt"
		remotePath = "/repo/committed.txt"
	)
	data := []byte("committed layer data")
	var (
		mu       sync.Mutex
		requests []clientLayerEntryRequest
		failPost = true
	)
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
		mu.Lock()
		requests = append(requests, req)
		shouldFail := failPost
		seq := len(requests)
		mu.Unlock()
		if shouldFail {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
			LayerID:      "layer-1",
			Path:         req.Path,
			Op:           req.Op,
			Kind:         req.Kind,
			BaseRevision: req.BaseRevision,
			Mode:         req.Mode,
			Content:      req.Content,
			SizeBytes:    req.SizeBytes,
			EntrySeq:     int64(seq),
		})
	}))
	defer ts.Close()

	shadowDir := t.TempDir()
	pendingDir := t.TempDir()
	shadow, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	if err := shadow.WriteFull(localPath, data, 8); err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	gen, err := pending.PutWithBaseRevAndMode(localPath, int64(len(data)), PendingOverwrite, 8, 0o644, true)
	if err != nil {
		t.Fatal(err)
	}
	marked, err := pending.MarkLayerCommitted(localPath, gen, 8)
	if err != nil || !marked {
		t.Fatalf("mark initial layer generation committed = (%t, %v), want true, nil", marked, err)
	}
	committedMeta, _ := pending.GetMeta(localPath)

	fs := NewDat9FS(client.New(ts.URL, ""), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/repo"})
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	ino := fs.inodes.Lookup(localPath, false, int64(len(data)), time.Now())
	fs.inodes.UpdateRevision(ino, 8)
	fs.inodes.UpdateMode(ino, 0o644)
	var out gofuse.AttrOut
	st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
		InHeader: gofuse.InHeader{NodeId: ino},
		Valid:    gofuse.FATTR_MODE,
		Mode:     0o600,
	}}, &out)
	if st == gofuse.OK {
		t.Fatal("layer chmod with rejected POST unexpectedly succeeded")
	}
	meta, ok := pending.GetMeta(localPath)
	if !ok || meta.LayerCommitted || meta.Generation <= committedMeta.Generation || !meta.HasMode || meta.Mode != 0o600 {
		t.Fatalf("failed chmod metadata = %+v, want newer recoverable mode generation", meta)
	}

	// Simulate restart after the remote rejection. The new metadata generation
	// must survive and be uploaded; the preceding committed overlay must not be
	// the generation replayed.
	recovered, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	failPost = false
	mu.Unlock()
	cq := NewCommitQueue(client.New(ts.URL, ""), shadow, recovered, nil, 1, 8, "/repo")
	cq.SetLayerRef("layer-1")
	if got := cq.RecoverPendingSync(context.Background()); got != 1 {
		t.Fatalf("recovered chmod commits = %d, want 1", got)
	}
	mu.Lock()
	gotRequests := append([]clientLayerEntryRequest(nil), requests...)
	mu.Unlock()
	if len(gotRequests) != 2 || gotRequests[1].Path != remotePath || gotRequests[1].Mode != 0o600 || !bytes.Equal(gotRequests[1].Content, data) {
		t.Fatalf("recovered chmod requests = %+v, want failed then recovered content upsert", gotRequests)
	}
	meta, ok = recovered.GetMeta(localPath)
	if !ok || !meta.LayerCommitted {
		t.Fatalf("recovered chmod metadata = %+v, want committed retained overlay", meta)
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
	if !ok || meta.Kind != PendingOverwrite || !meta.LayerCommitted || !meta.HasMode || meta.Mode != 0o600 {
		t.Fatalf("pending mode = %+v, want committed PendingOverwrite 0600", meta)
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
	if !ok || meta.Kind != PendingOverwrite || !meta.LayerCommitted || !meta.HasMode || meta.Mode != 0o600 {
		t.Fatalf("pending mode = %+v, want committed PendingOverwrite 0600", meta)
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

func TestLayerSetAttrTruncateDoesNotGiveDelayedCleanHandleAnEmptyGeneration(t *testing.T) {
	const (
		localPath    = "/base.txt"
		remotePath   = "/repo/base.txt"
		baseRevision = int64(7)
		oldPID       = uint32(4101)
		writerPID    = uint32(4102)
	)
	base := []byte("base content")
	updated := []byte("edited in layer")

	var (
		mu       sync.Mutex
		requests []clientLayerEntryRequest
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs"+remotePath:
			w.Header().Set("Content-Length", strconv.Itoa(len(base)))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(baseRevision, 10))
			w.Header().Set("X-Dat9-Mode", strconv.FormatUint(uint64(0o644), 10))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs"+remotePath:
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(baseRevision, 10))
			_, _ = w.Write(base)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/layers/layer-1/entries":
			var req clientLayerEntryRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode layer entry: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			mu.Lock()
			requests = append(requests, req)
			seq := len(requests)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
				LayerID:      "layer-1",
				Path:         req.Path,
				Op:           req.Op,
				Kind:         req.Kind,
				BaseRevision: req.BaseRevision,
				Content:      req.Content,
				SizeBytes:    req.SizeBytes,
				EntrySeq:     int64(seq),
			})
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	opts := &MountOptions{
		LayerRef:      "layer-1",
		RemoteRoot:    "/repo",
		SyncMode:      SyncStrict,
		WritePolicy:   WritePolicyWriteSync,
		FlushDebounce: 0,
	}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	ino := fs.inodes.Lookup(localPath, false, int64(len(base)), time.Now())
	fs.inodes.UpdateRevision(ino, baseRevision)

	openWriter := func(pid uint32) gofuse.OpenOut {
		t.Helper()
		var out gofuse.OpenOut
		if st := fs.Open(nil, &gofuse.OpenIn{
			InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: pid}},
			Flags:    uint32(syscall.O_WRONLY),
		}, &out); st != gofuse.OK {
			t.Fatalf("Open(pid=%d) status = %v, want OK", pid, st)
		}
		return out
	}

	// Keep a clean handle from an earlier process registered while a later
	// writer performs the OPEN -> SETATTR(O_TRUNC) sequence. Layer truncate is
	// already durable after the first upsert; neither handle may acquire a
	// second dirty zero generation whose delayed Release can become the latest
	// layer entry after the writer's content.
	old := openWriter(oldPID)
	writer := openWriter(writerPID)
	var attrOut gofuse.AttrOut
	if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
		InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: writerPID}},
		Valid:    gofuse.FATTR_SIZE,
		Size:     0,
	}}, &attrOut); st != gofuse.OK {
		t.Fatalf("SetAttr truncate status = %v, want OK", st)
	}
	for name, id := range map[string]uint64{"old": old.Fh, "writer": writer.Fh} {
		fh, ok := fs.fileHandles.Get(id)
		if !ok {
			t.Fatalf("%s handle missing after SetAttr", name)
		}
		fh.Lock()
		dirtySeq := fh.DirtySeq
		dirty := fh.Dirty != nil && fh.Dirty.HasDirtyParts()
		size := int64(-1)
		if fh.Dirty != nil {
			size = fh.Dirty.Size()
		}
		fh.Unlock()
		if dirtySeq != 0 || dirty || size != 0 {
			t.Fatalf("%s handle after durable layer truncate: seq=%d dirty=%t size=%d, want clean size 0", name, dirtySeq, dirty, size)
		}
	}

	mu.Lock()
	if len(requests) != 1 || requests[0].SizeBytes != 0 || len(requests[0].Content) != 0 {
		got := append([]clientLayerEntryRequest(nil), requests...)
		mu.Unlock()
		t.Fatalf("layer requests after truncate = %+v, want one empty upsert", got)
	}
	mu.Unlock()

	if n, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: writerPID}},
		Fh:       writer.Fh,
		Offset:   0,
		Size:     uint32(len(updated)),
	}, updated); st != gofuse.OK || n != uint32(len(updated)) {
		t.Fatalf("Write = (%d, %v), want (%d, OK)", n, st, len(updated))
	}
	if st := fs.Flush(nil, &gofuse.FlushIn{
		InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: writerPID}},
		Fh:       writer.Fh,
	}); st != gofuse.OK {
		t.Fatalf("Flush status = %v, want OK", st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{
		InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: writerPID}},
		Fh:       writer.Fh,
	})

	// Deliver the older handle's Release only after the current writer has
	// committed its content. This is the real kernel ordering which previously
	// let a hidden empty generation become the final layer entry.
	fs.Release(nil, &gofuse.ReleaseIn{
		InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: oldPID}},
		Fh:       old.Fh,
	})

	mu.Lock()
	got := append([]clientLayerEntryRequest(nil), requests...)
	mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("layer requests = %+v, want truncate then content", got)
	}
	if !bytes.Equal(got[1].Content, updated) || got[1].SizeBytes != int64(len(updated)) {
		t.Fatalf("final layer entry = %+v, want content %q", got[1], updated)
	}
}

func TestLayerLatePendingDrainDoesNotReplayRetainedOverlay(t *testing.T) {
	const localPath = "/base.txt"
	data := []byte("edited in layer")

	var (
		mu       sync.Mutex
		requests []clientLayerEntryRequest
	)
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
		mu.Lock()
		requests = append(requests, req)
		seq := len(requests)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
			LayerID:      "layer-1",
			Path:         req.Path,
			Op:           req.Op,
			Kind:         req.Kind,
			BaseRevision: req.BaseRevision,
			Content:      req.Content,
			SizeBytes:    req.SizeBytes,
			EntrySeq:     int64(seq),
		})
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
	if err := shadow.WriteFull(localPath, data, 7); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev(localPath, int64(len(data)), PendingOverwrite, 7); err != nil {
		t.Fatal(err)
	}

	opts := &MountOptions{LayerRef: "layer-1", RemoteRoot: "/repo"}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	cq := NewCommitQueue(fs.client, shadow, pending, nil, 1, 8, opts.RemoteRoot)
	cq.SetLayerRef(opts.LayerRef)
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	fs.commitQueue = cq
	if err := cq.Enqueue(attachTestStagingGens(shadow, pending, &CommitEntry{
		Path:    localPath,
		BaseRev: 7,
		Size:    int64(len(data)),
		Kind:    PendingOverwrite,
	})); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if !pending.HasPending(localPath) || !shadow.Has(localPath) {
		t.Fatal("successful layer upload must retain its pending metadata and shadow")
	}
	meta, ok := pending.GetMeta(localPath)
	if !ok || !meta.LayerCommitted {
		t.Fatalf("retained layer metadata = %+v, want committed overlay marker", meta)
	}
	mu.Lock()
	before := append([]clientLayerEntryRequest(nil), requests...)
	mu.Unlock()
	if len(before) != 1 || !bytes.Equal(before[0].Content, data) {
		t.Fatalf("initial layer requests = %+v, want one content upsert", before)
	}

	// A graceful unmount's late-pending rescue is only for unfinished base
	// writeback. The retained layer overlay above is already committed; replaying
	// it can append a stale shadow after a newer layer entry and make stale data
	// authoritative. Bound the call so deleting the layer guard fails quickly.
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	fs.drainLatePendingEntries(ctx)

	mu.Lock()
	after := append([]clientLayerEntryRequest(nil), requests...)
	mu.Unlock()
	if len(after) != 1 {
		t.Fatalf("late pending drain replayed committed layer overlay: %+v", after)
	}
}

func TestLayerLateReleaseAfterQueueStopIsRecoveredOnce(t *testing.T) {
	const (
		localPath  = "/late.txt"
		remotePath = "/repo/late.txt"
		baseRev    = int64(0)
	)
	data := []byte("late release data")

	var (
		mu       sync.Mutex
		requests []clientLayerEntryRequest
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
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
		mu.Lock()
		requests = append(requests, req)
		seq := len(requests)
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
			LayerID:      "layer-1",
			Path:         req.Path,
			Op:           req.Op,
			Kind:         req.Kind,
			BaseRevision: req.BaseRevision,
			Content:      req.Content,
			SizeBytes:    req.SizeBytes,
			EntrySeq:     int64(seq),
		})
	}))
	defer ts.Close()

	opts := &MountOptions{LayerRef: "layer-1", RemoteRoot: "/repo"}
	opts.setDefaults()
	fs := NewDat9FS(client.New(ts.URL, ""), opts)
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cq := NewCommitQueue(fs.client, shadow, pending, nil, 1, 8, opts.RemoteRoot)
	cq.SetLayerRef(opts.LayerRef)
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	fs.commitQueue = cq
	fs.syncMode = SyncInteractive
	writeBack, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	uploader := NewWriteBackUploader(fs.client, writeBack, 1, opts.RemoteRoot)
	defer uploader.DrainAll()
	fs.SetWriteBack(writeBack, uploader)
	// Unmount has already stopped the async queue when this delayed Release
	// arrives from the kernel. The handle has not staged anything yet: its late
	// Flush must durably create the shadow and pending generation, then Release
	// observes the stopped queue while transferring ownership.
	cq.DrainAll()
	if shadow.Has(localPath) || pending.HasPending(localPath) {
		t.Fatal("test setup unexpectedly staged the late handle before queue stop")
	}

	ino := fs.inodes.Lookup(localPath, false, int64(len(data)), time.Now())
	fs.inodes.UpdateRevision(ino, baseRev)
	dirty := NewWriteBuffer(localPath, maxPreloadSize, 0)
	if _, err := dirty.Write(0, data); err != nil {
		t.Fatal(err)
	}
	dirtySeq := fs.markDirtySize(ino, int64(len(data)))
	fh := &FileHandle{
		Ino:         ino,
		Path:        localPath,
		Dirty:       dirty,
		DirtySeq:    dirtySeq,
		BaseRev:     baseRev,
		OrigSize:    int64(len(data)),
		WritePolicy: WritePolicyWriteBack,
	}
	fhID := fs.fileHandles.Allocate(fh)
	fs.openHandles.Add(fh)
	if st := fs.Flush(nil, &gofuse.FlushIn{
		InHeader: gofuse.InHeader{NodeId: ino},
		Fh:       fhID,
	}); st != gofuse.OK {
		t.Fatalf("late Flush after queue stop = %v, want OK after durable local staging", st)
	}
	if !shadow.Has(localPath) || !pending.HasPending(localPath) {
		t.Fatal("late Flush did not durably stage shadow and pending metadata after queue stop")
	}
	fs.Release(nil, &gofuse.ReleaseIn{Fh: fhID})

	mu.Lock()
	before := len(requests)
	mu.Unlock()
	if before != 0 {
		t.Fatalf("late Release uploaded through stopped queue: %d requests", before)
	}
	meta, ok := pending.GetMeta(localPath)
	if !ok || meta.LayerCommitted {
		t.Fatalf("late Release metadata = %+v, want recoverable uncommitted generation", meta)
	}
	if meta.Kind != PendingOverwrite || meta.BaseRev != 0 {
		t.Fatalf("late Release metadata = %+v, want layer overwrite at absent base revision", meta)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fs.drainLatePendingEntries(ctx)

	mu.Lock()
	after := append([]clientLayerEntryRequest(nil), requests...)
	mu.Unlock()
	if len(after) != 1 || after[0].Path != remotePath || !bytes.Equal(after[0].Content, data) {
		t.Fatalf("rescued layer requests = %+v, want one %s content upsert", after, remotePath)
	}
	meta, ok = pending.GetMeta(localPath)
	if !ok || !meta.LayerCommitted {
		t.Fatalf("rescued layer metadata = %+v, want committed retained overlay", meta)
	}
	if got := fs.commitQueue.pendingRecoveryCount(); got != 0 {
		t.Fatalf("recoverable pending entries after rescue = %d, want 0", got)
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
		case r.Method == http.MethodPost && r.URL.Path == "/v1/layers/layer-1/uploads/initiate":
			http.NotFound(w, r)
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
