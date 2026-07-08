package fuse

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

// TestApplyLayerRollbackClearsOverlay verifies that applyLayerRollback
// clears the in-memory overlay, sets the abandoned flag, and marks pending
// entries as conflict without deleting shadow blobs.
func TestApplyLayerRollbackClearsOverlay(t *testing.T) {
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	fs := NewDat9FS(client.New("http://localhost:0", ""), &MountOptions{
		LayerRef:   "layer-rollback-test",
		RemoteRoot: "/repo",
	})

	// Populate the overlay with a whiteout, file, dir, and symlink.
	fs.markLayerWhiteout("/old.txt")
	fs.markLayerFileMode("/a.txt", 0o644)
	fs.markLayerDir("/newdir", 0o755)
	fs.markLayerSymlink("/link", "target.txt", 0o777|syscall.S_IFLNK)

	// Stage a pending write (shadow + pending index) that should be
	// preserved as PendingConflict after rollback.
	if err := shadow.WriteFull("/a.txt", []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/a.txt", 4, PendingOverwrite, 0); err != nil {
		t.Fatal(err)
	}
	fs.commitQueue = NewCommitQueue(client.New("http://localhost:0", ""), shadow, pending, nil, 1, 8)
	fs.commitQueue.SetLayerRef("layer-rollback-test")

	// Sanity: overlay is populated.
	if !fs.isLayerWhiteout("/old.txt") {
		t.Fatal("precondition: old.txt should be whiteout")
	}
	if mode, ok := fs.layerFileMode("/a.txt"); !ok || mode != 0o644 {
		t.Fatalf("precondition: a.txt file mode = (%#o, %t), want 0o644 true", mode, ok)
	}
	if !fs.layerEnabled() {
		t.Fatal("precondition: layer should be enabled")
	}
	if fs.isLayerAbandoned() {
		t.Fatal("precondition: layer should not be abandoned")
	}

	// Act: apply the rollback.
	fs.applyLayerRollback(shadow, pending)

	// 1. Abandoned flag is set.
	if !fs.isLayerAbandoned() {
		t.Fatal("after rollback: layer should be abandoned")
	}

	// 2. Overlay is cleared.
	if fs.isLayerWhiteout("/old.txt") {
		t.Fatal("after rollback: old.txt whiteout should be cleared")
	}
	if _, ok := fs.layerFileMode("/a.txt"); ok {
		t.Fatal("after rollback: a.txt overlay file should be cleared")
	}
	if _, ok := fs.layerDirMode("/newdir"); ok {
		t.Fatal("after rollback: newdir overlay dir should be cleared")
	}
	if _, _, ok := fs.layerSymlink("/link"); ok {
		t.Fatal("after rollback: link overlay symlink should be cleared")
	}

	// 3. Commit queue is stopped + abandoned (Enqueue returns sentinel).
	err = fs.commitQueue.Enqueue(&CommitEntry{Path: "/b.txt", Size: 0})
	if !errors.Is(err, errLayerRolledBack) {
		t.Fatalf("after rollback: Enqueue err = %v, want errLayerRolledBack", err)
	}

	// 4. Pending entry preserved as conflict, shadow blob not deleted.
	if !pending.HasPending("/a.txt") {
		t.Fatal("after rollback: a.txt pending should still exist (preserved)")
	}
	if meta, ok := pending.GetMeta("/a.txt"); !ok || meta.Kind != PendingConflict {
		t.Fatalf("after rollback: a.txt pending kind = %v, want PendingConflict", meta.Kind)
	}
	if !shadow.Has("/a.txt") {
		t.Fatal("after rollback: a.txt shadow blob should be preserved for manual recovery")
	}
}

// TestUpsertLayerEntryReturnsErrLayerRolledBack verifies that after the
// abandoned flag is set, upsertLayerEntry short-circuits with errLayerRolledBack.
func TestUpsertLayerEntryReturnsErrLayerRolledBack(t *testing.T) {
	fs := NewDat9FS(client.New("http://localhost:0", ""), &MountOptions{
		LayerRef:   "layer-rollback-test",
		RemoteRoot: "/repo",
	})
	fs.setLayerAbandoned()

	err := fs.upsertLayerEntry(context.Background(), client.FSLayerEntryRequest{
		Path:      "/repo/a.txt",
		Op:        "upsert",
		Kind:      "file",
		Content:   []byte("data"),
		SizeBytes: 4,
	}, 4)
	if !errors.Is(err, errLayerRolledBack) {
		t.Fatalf("upsertLayerEntry after abandoned: err = %v, want errLayerRolledBack", err)
	}
}

// TestHttpToFuseStatusMapsErrLayerRolledBackToESTALE verifies the error
// mapper returns the "stale file handle" errno for the rollback sentinel.
func TestHttpToFuseStatusMapsErrLayerRolledBackToESTALE(t *testing.T) {
	got := httpToFuseStatus(errLayerRolledBack)
	if got != gofuse.Status(syscall.ESTALE) {
		t.Fatalf("httpToFuseStatus(errLayerRolledBack) = %v, want ESTALE", got)
	}
}

// TestRefreshLayerEventsDetectsRollback verifies that refreshLayerEvents,
// when it sees a rollback event, calls applyLayerRollback instead of
// restoreLayerEntries.
func TestRefreshLayerEventsDetectsRollback(t *testing.T) {
	var diffCalled bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-rb/events":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"events": []client.FSLayerEvent{
					{EventID: "layer-rb:1", LayerID: "layer-rb", Seq: 1, Op: "upsert", Path: "/repo/a.txt"},
					{EventID: "layer-rb:rollback:2", LayerID: "layer-rb", Seq: 2, Op: "rollback", Path: "/"},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-rb/diff":
			diffCalled = true
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FSLayerEntry{}})
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

	c := client.New(ts.URL, "")
	fs := NewDat9FS(c, &MountOptions{
		LayerRef:   "layer-rb",
		RemoteRoot: "/repo",
	})
	fs.commitQueue = NewCommitQueue(c, shadow, pending, nil, 1, 8)
	fs.commitQueue.SetLayerRef("layer-rb")

	opts := &MountOptions{LayerRef: "layer-rb", RemoteRoot: "/repo"}
	nextSeq, err := refreshLayerEvents(context.Background(), c, opts, shadow, pending, fs, 0)
	if err != nil {
		t.Fatalf("refreshLayerEvents: %v", err)
	}
	if nextSeq != 2 {
		t.Fatalf("nextSeq = %d, want 2", nextSeq)
	}

	// restoreLayerEntries (the /diff endpoint) must NOT be called for a
	// rollback event.
	if diffCalled {
		t.Fatal("restoreLayerEntries /diff was called; should be skipped for rollback")
	}

	// applyLayerRollback should have run.
	if !fs.isLayerAbandoned() {
		t.Fatal("layer should be abandoned after refreshLayerEvents saw rollback")
	}
}

// TestRefreshLayerEventsSkipsRollbackWhenNoRollbackEvent verifies the normal
// (non-rollback) path still calls restoreLayerEntries.
func TestRefreshLayerEventsSkipsRollbackWhenNoRollbackEvent(t *testing.T) {
	var diffCalled bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-norm/events":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"events": []client.FSLayerEvent{
					{EventID: "layer-norm:1", LayerID: "layer-norm", Seq: 1, Op: "upsert", Path: "/repo/a.txt"},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-norm/diff":
			diffCalled = true
			_ = json.NewEncoder(w).Encode(map[string]any{
				"entries": []client.FSLayerEntry{
					{LayerID: "layer-norm", Path: "/repo/a.txt", Op: "upsert", Kind: "file", SizeBytes: 1, EntrySeq: 1},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-norm/entries":
			_ = json.NewEncoder(w).Encode(client.FSLayerEntry{
				LayerID: "layer-norm", Path: "/repo/a.txt", Op: "upsert", Kind: "file",
				Content: []byte("a"), SizeBytes: 1, EntrySeq: 1,
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

	c := client.New(ts.URL, "")
	fs := NewDat9FS(c, &MountOptions{LayerRef: "layer-norm", RemoteRoot: "/repo"})
	fs.commitQueue = NewCommitQueue(c, shadow, pending, nil, 1, 8)
	fs.commitQueue.SetLayerRef("layer-norm")

	opts := &MountOptions{LayerRef: "layer-norm", RemoteRoot: "/repo"}
	nextSeq, err := refreshLayerEvents(context.Background(), c, opts, shadow, pending, fs, 0)
	if err != nil {
		t.Fatalf("refreshLayerEvents: %v", err)
	}
	if nextSeq != 1 {
		t.Fatalf("nextSeq = %d, want 1", nextSeq)
	}
	if !diffCalled {
		t.Fatal("restoreLayerEntries /diff should be called for non-rollback events")
	}
	if fs.isLayerAbandoned() {
		t.Fatal("layer should NOT be abandoned for non-rollback events")
	}
}