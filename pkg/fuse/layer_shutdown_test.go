package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

func newLayerShutdownState(t *testing.T) (*PendingIndex, *ShadowStore) {
	t.Helper()
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shadow.Close)
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	pending.setShadowStore(shadow)
	return pending, shadow
}

func TestLayerShutdownCleanOverlay(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		base                      int64
		checkpoint, upload, empty bool
	}{
		{name: "empty"}, {name: "restored_without_IO"}, {name: "readonly_checkpoint", checkpoint: true},
		{name: "uploaded", upload: true}, {name: "restored_positive_base", base: 5},
	} {
		tc.empty = tc.name == "empty"
		t.Run(tc.name, func(t *testing.T) {
			var posts atomic.Int64
			data := []byte("probe text\n")
			entry := client.FSLayerEntry{LayerID: "layer-1", Path: "/hello.txt", Op: "upsert", Kind: "file", Content: data, BaseRevision: tc.base, SizeBytes: int64(len(data)), EntrySeq: 1}
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/v1/layer-checkpoints/cp1":
					_ = json.NewEncoder(w).Encode(client.FSLayerCheckpoint{CheckpointID: "cp1", LayerID: "layer-1", DurableSeq: 1})
				case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/diff":
					entries := []client.FSLayerEntry{}
					if !tc.empty {
						entries = append(entries, entry)
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"entries": entries})
				case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/entries":
					_ = json.NewEncoder(w).Encode(entry)
				case r.Method == http.MethodPost && r.URL.Path == "/v1/layers/layer-1/entries":
					posts.Add(1)
					_ = json.NewEncoder(w).Encode(entry)
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					w.WriteHeader(500)
				}
			}))
			defer ts.Close()
			pending, shadow := newLayerShutdownState(t)
			c := newTestClient(ts.URL)
			opts := &MountOptions{LayerRef: "layer-1", RemoteRoot: "/", ReadOnly: tc.checkpoint}
			if tc.checkpoint {
				opts.CheckpointRef = "cp1"
			}
			fs := newTestDrainFS()
			fs.opts, fs.pendingIndex, fs.shadowStore = opts, pending, shadow
			if !tc.checkpoint {
				fs.commitQueue = NewCommitQueue(c, shadow, pending, nil, 1, 8)
				fs.commitQueue.SetLayerRef("layer-1")
				defer fs.commitQueue.DrainAll()
			}
			if tc.upload {
				if err := shadow.WriteFull(entry.Path, data, 0); err != nil {
					t.Fatal(err)
				}
				if _, err := pending.Put(entry.Path, entry.SizeBytes, PendingNew); err != nil {
					t.Fatal(err)
				}
				if err := fs.commitQueue.CommitNow(context.Background(), attachTestStagingGens(shadow, pending, &CommitEntry{Path: entry.Path, Size: entry.SizeBytes, Kind: PendingNew})); err != nil {
					t.Fatal(err)
				}
			} else if err := restoreLayerEntries(context.Background(), c, opts, shadow, pending, nil); err != nil {
				t.Fatal(err)
			}
			if resp := fs.Drain(context.Background()); !resp.OK {
				t.Fatalf("Drain: %+v", resp)
			}
			if fs.commitQueue != nil {
				fs.commitQueue.DrainAll()
			}
			before := posts.Load()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := fs.drainLatePendingEntries(ctx); err != nil {
				t.Fatal(err)
			}
			if posts.Load() != before {
				t.Fatal("shutdown uploaded a clean overlay")
			}
			if !tc.empty {
				meta, ok := pending.GetMeta(entry.Path)
				if !ok || !meta.LayerClean {
					t.Fatalf("retained metadata = %+v", meta)
				}
				got, err := shadow.ReadAll(entry.Path)
				if err != nil || !bytes.Equal(got, data) {
					t.Fatalf("shadow = %q, %v", got, err)
				}
			}
		})
	}
}

func TestLateDrainRecoversUncommittedWrites(t *testing.T) {
	for _, tc := range []struct {
		name  string
		layer bool
		kind  PendingKind
		base  int64
	}{
		{"layer_new", true, PendingNew, 0}, {"layer_overwrite_zero", true, PendingOverwrite, 0}, {"layer_overwrite_existing", true, PendingOverwrite, 5},
		{"base_new", false, PendingNew, 0}, {"base_overwrite", false, PendingOverwrite, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int64
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				if tc.layer {
					if r.URL.Path != "/v1/layers/layer-1/entries" || r.Method != http.MethodPost {
						t.Errorf("unexpected %s %s", r.Method, r.URL)
					}
					_, _ = w.Write([]byte(`{"layer_id":"layer-1","path":"/a","op":"upsert","kind":"file","entry_seq":1}`))
				} else {
					if r.URL.Path != "/v1/fs/a" || r.Method != http.MethodPut {
						t.Errorf("unexpected %s %s", r.Method, r.URL)
					}
					_, _ = w.Write([]byte(`{"status":"ok","revision":6}`))
				}
			}))
			defer ts.Close()
			pending, shadow := newLayerShutdownState(t)
			if err := shadow.WriteFull("/a", []byte("data"), tc.base); err != nil {
				t.Fatal(err)
			}
			if _, err := pending.PutWithBaseRev("/a", 4, tc.kind, tc.base); err != nil {
				t.Fatal(err)
			}
			cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
			if tc.layer {
				cq.SetLayerRef("layer-1")
			}
			cq.DrainAll()
			fs := &Dat9FS{pendingIndex: pending, commitQueue: cq}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := fs.drainLatePendingEntries(ctx); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 1 {
				t.Fatalf("uploads=%d, want 1", calls.Load())
			}
			if tc.layer {
				if m, ok := pending.GetMeta("/a"); !ok || !m.LayerClean {
					t.Fatalf("meta=%+v", m)
				}
			} else if pending.HasPending("/a") || shadow.Has("/a") {
				t.Fatal("base staging not removed")
			}
		})
	}
}

func TestLateDrainPreservesFailures(t *testing.T) {
	for _, name := range []string{"no_queue", "conflict", "canceled", "upload_failure"} {
		t.Run(name, func(t *testing.T) {
			pending, shadow := newLayerShutdownState(t)
			if err := shadow.WriteFull("/a", []byte("data"), 0); err != nil {
				t.Fatal(err)
			}
			kind := PendingNew
			if name == "conflict" {
				kind = PendingConflict
			}
			if _, err := pending.Put("/a", 4, kind); err != nil {
				t.Fatal(err)
			}
			fs := &Dat9FS{pendingIndex: pending}
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }))
			defer ts.Close()
			if name != "no_queue" {
				fs.commitQueue = NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
				fs.commitQueue.SetLayerRef("layer-1")
				fs.commitQueue.DrainAll()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			if name == "canceled" {
				cancel()
			}
			err := fs.drainLatePendingEntries(ctx)
			if err == nil {
				t.Fatal("incomplete drain reported success")
			}
			if name == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("error=%v", err)
			}
			if !pending.HasPending("/a") || !shadow.Has("/a") {
				t.Fatal("failure discarded recovery state")
			}
		})
	}
}

func TestLayerCleanGenerationAndRecovery(t *testing.T) {
	for _, wal := range []bool{false, true} {
		t.Run(map[bool]string{false: "disk", true: "wal"}[wal], func(t *testing.T) {
			pending, shadow := newLayerShutdownState(t)
			var journal *Journal
			if wal {
				var err error
				journal, err = NewJournal(filepath.Join(t.TempDir(), "journal"))
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = journal.Close() }()
				pending.SetJournal(journal)
			}
			if err := shadow.WriteFull("/a", []byte("old"), 0); err != nil {
				t.Fatal(err)
			}
			gen, err := pending.Put("/a", 3, PendingNew)
			if err != nil {
				t.Fatal(err)
			}
			if err := pending.MarkLayerCommittedIfGeneration("/a", gen, 0, 0, false, LayerCacheIdentity{}); err != nil {
				t.Fatal(err)
			}
			if len(pending.ListUncommittedPaths()) != 0 {
				t.Fatal("clean overlay counted as pending")
			}
			recoverIndex := func() *PendingIndex {
				t.Helper()
				idx, err := NewPendingIndex(pending.dir)
				if err != nil {
					t.Fatal(err)
				}
				if err := idx.RecoverFromDisk(); err != nil {
					t.Fatal(err)
				}
				if wal {
					if err := replayJournalIntoPending(journal, idx, shadow); err != nil {
						t.Fatal(err)
					}
				}
				return idx
			}
			if m, _ := recoverIndex().GetMeta("/a"); m == nil || !m.LayerClean {
				t.Fatalf("clean flag lost on recovery: %+v", m)
			}
			if err := pending.UpdateMode("/a", 0600); err != nil {
				t.Fatal(err)
			}
			if m, _ := pending.GetMeta("/a"); m.LayerClean {
				t.Fatal("unsent chmod marked clean")
			}
			if m, _ := recoverIndex().GetMeta("/a"); m == nil || m.LayerClean || m.Mode != 0600 {
				t.Fatalf("unsent chmod lost on recovery: %+v", m)
			}
			if err := shadow.WriteFull("/a", []byte("new"), 0); err != nil {
				t.Fatal(err)
			}
			next, err := pending.PutWithBaseRev("/a", 3, PendingOverwrite, 0)
			if err != nil {
				t.Fatal(err)
			}
			if err := pending.MarkLayerCommittedIfGeneration("/a", gen, 0, 0, false, LayerCacheIdentity{}); err != nil {
				t.Fatal(err)
			}
			for _, idx := range []*PendingIndex{pending, recoverIndex()} {
				m, _ := idx.GetMeta("/a")
				if m == nil || m.LayerClean || m.Generation != next {
					t.Fatalf("stale completion hid newer write: %+v", m)
				}
			}
		})
	}
}

func TestLayerCompletionDoesNotHideConcurrentPublication(t *testing.T) {
	pending, shadow := newLayerShutdownState(t)
	journal, err := NewJournal(filepath.Join(t.TempDir(), "journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = journal.Close() }()
	pending.SetJournal(journal)
	if err := shadow.WriteFull("/a", []byte("old"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.Put("/a", 3, PendingNew); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"layer_id":"layer-1","entry_seq":1}`))
	}))
	defer ts.Close()
	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, journal, 1, 8)
	cq.SetLayerRef("layer-1")
	defer cq.DrainAll()
	entry := attachTestStagingGens(shadow, pending, &CommitEntry{Path: "/a", Size: 3, Kind: PendingNew})
	// Publish the next edit exactly after the old remote upload completes,
	// before its acknowledgement cleans local metadata. No scheduling sleeps.
	cq.OnSuccess = func(_ *CommitEntry, _ int64) {
		if err := shadow.WriteFull("/a", []byte("new"), 0); err != nil {
			t.Error(err)
		}
		if _, err := pending.PutWithBaseRev("/a", 3, PendingOverwrite, 0); err != nil {
			t.Error(err)
		}
	}
	if err := cq.CommitNow(context.Background(), entry); err != nil {
		t.Fatal(err)
	}
	recovered, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := replayJournalIntoPending(journal, recovered, shadow); err != nil {
		t.Fatal(err)
	}
	for _, idx := range []*PendingIndex{pending, recovered} {
		m, ok := idx.GetMeta("/a")
		if !ok || m.LayerClean || m.Generation == entry.PendingIndexGen {
			t.Fatalf("new write lost: %+v", m)
		}
	}
}

func TestLayerStartupRecoverySkipsCleanAndUploadsDirty(t *testing.T) {
	for _, clean := range []bool{false, true} {
		t.Run(map[bool]string{false: "dirty_zero_base", true: "clean_positive_base"}[clean], func(t *testing.T) {
			pending, shadow := newLayerShutdownState(t)
			if err := shadow.WriteFull("/a", []byte("data"), 0); err != nil {
				t.Fatal(err)
			}
			if clean {
				if _, err := pending.PutLayerCache("/a", 4, 5, 0, false, LayerCacheIdentity{}); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := pending.PutWithBaseRev("/a", 4, PendingOverwrite, 0); err != nil {
					t.Fatal(err)
				}
			}
			var calls atomic.Int64
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"layer_id":"layer-1","entry_seq":1}`))
			}))
			defer ts.Close()
			cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
			cq.SetLayerRef("layer-1")
			cq.RecoverPending()
			cq.DrainAll()
			want := int64(1)
			if clean {
				want = 0
			}
			if calls.Load() != want {
				t.Fatalf("uploads=%d, want %d", calls.Load(), want)
			}
			if m, _ := pending.GetMeta("/a"); m == nil || !m.LayerClean {
				t.Fatalf("overlay not retained clean: %+v", m)
			}
		})
	}
}

func TestRestoreLayerPreservesUncommittedContent(t *testing.T) {
	pending, shadow := newLayerShutdownState(t)
	if err := shadow.WriteFull("/a", []byte("local edit"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/a", 10, PendingOverwrite, 0); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/layers/layer-1/diff" {
			t.Errorf("must not fetch over uncommitted shadow: %s", r.URL)
			w.WriteHeader(500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FSLayerEntry{{Path: "/a", Op: "upsert", Kind: "file", EntrySeq: 1}}})
	}))
	defer ts.Close()
	if err := restoreLayerEntries(context.Background(), newTestClient(ts.URL), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/"}, shadow, pending, nil); err != nil {
		t.Fatal(err)
	}
	if got, err := shadow.ReadAll("/a"); err != nil || string(got) != "local edit" {
		t.Fatalf("content=%q, err=%v", got, err)
	}
	if m, _ := pending.GetMeta("/a"); m.LayerClean {
		t.Fatal("restoration acknowledged uncommitted write")
	}
}

func TestLayerRollbackOnlyConflictsUncommittedWrites(t *testing.T) {
	for _, dirty := range []bool{false, true} {
		name := "clean_overlay"
		if dirty {
			name = "unsent_write"
		}
		t.Run(name, func(t *testing.T) {
			pending, shadow := newLayerShutdownState(t)
			if err := shadow.WriteFull("/a", []byte("data"), 0); err != nil {
				t.Fatal(err)
			}
			if _, err := pending.PutLayerCache("/a", 4, 0, 0, false, LayerCacheIdentity{}); err != nil {
				t.Fatal(err)
			}
			if dirty {
				if _, err := pending.Put("/a", 4, PendingOverwrite); err != nil {
					t.Fatal(err)
				}
			}
			fs := newTestDrainFS()
			fs.opts = &MountOptions{LayerRef: "layer-1"}
			fs.pendingIndex, fs.shadowStore = pending, shadow
			fs.commitQueue = NewCommitQueue(newTestClient("http://localhost:0"), shadow, pending, nil, 1, 8)
			fs.commitQueue.SetLayerRef("layer-1")
			defer fs.commitQueue.DrainAll()
			fs.applyLayerRollback(shadow, pending)
			m, ok := pending.GetMeta("/a")
			if ok != dirty || (dirty && m.Kind != PendingConflict) {
				t.Fatalf("rollback metadata = %+v, dirty = %v", m, dirty)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := fs.drainLatePendingEntries(ctx); (err != nil) != dirty {
				t.Fatalf("drain error = %v, dirty = %v", err, dirty)
			}
			if dirty {
				if got, err := shadow.ReadAll("/a"); err != nil || string(got) != "data" {
					t.Fatalf("preserved data = %q, %v", got, err)
				}
			} else if shadow.Has("/a") {
				t.Fatal("clean abandoned shadow still exists")
			}
		})
	}
}
