package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

// Exercise namespace operations through the FUSE entry points, then restore
// the remote replay into a fresh mount to catch files resurrecting on remount.
func TestLayerDurableNamespaceMutation(t *testing.T) {
	for _, op := range []string{"unlink", "rename"} {
		t.Run(op, func(t *testing.T) {
			var mu sync.Mutex
			entries := map[string]client.FSLayerEntry{"/a": {LayerID: "layer-1", Path: "/a", Op: "upsert", Kind: "file", Content: []byte("data"), SizeBytes: 4}}
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodPost && r.URL.Path == "/v1/layers/layer-1/entries":
					var req client.FSLayerEntryRequest
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Error(err)
						w.WriteHeader(500)
						return
					}
					e := client.FSLayerEntry{LayerID: "layer-1", Path: req.Path, Op: req.Op, Kind: req.Kind, Content: req.Content, SizeBytes: req.SizeBytes, Mode: req.Mode}
					entries[e.Path] = e
					_ = json.NewEncoder(w).Encode(e)
				case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/diff":
					var all []client.FSLayerEntry
					for _, e := range entries {
						all = append(all, e)
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"entries": all})
				case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/layer-1/entries":
					e, ok := entries[r.URL.Query().Get("path")]
					if !ok {
						http.NotFound(w, r)
						return
					}
					_ = json.NewEncoder(w).Encode(e)
				default:
					http.NotFound(w, r)
				}
			}))
			defer ts.Close()
			idx, shadow := newLayerShutdownState(t)
			if err := shadow.WriteFull("/a", []byte("data"), 0); err != nil {
				t.Fatal(err)
			}
			if _, err := idx.PutLayerCache("/a", 4, 0, 0600, true); err != nil {
				t.Fatal(err)
			}
			fs := NewDat9FS(newTestClient(ts.URL), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/"})
			fs.pendingIndex, fs.shadowStore = idx, shadow
			fs.inodes.Lookup("/a", false, 4, time.Now())
			var st gofuse.Status
			if op == "unlink" {
				st = fs.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "a")
			} else {
				st = fs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "a", "b")
			}
			if st != gofuse.OK {
				t.Fatalf("%s: %v", op, st)
			}
			mu.Lock()
			old := entries["/a"]
			moved := entries["/b"]
			mu.Unlock()
			if old.Op != "whiteout" {
				t.Errorf("durable source was not whiteouted: %+v", old)
			}
			if op == "rename" && (moved.Op != "upsert" || !bytes.Equal(moved.Content, []byte("data"))) {
				t.Errorf("rename target = %+v", moved)
			}
			restored, restoredShadow := newLayerShutdownState(t)
			next := NewDat9FS(newTestClient(ts.URL), fs.opts)
			next.pendingIndex, next.shadowStore = restored, restoredShadow
			if err := restoreLayerEntries(context.Background(), next.client, next.opts, restoredShadow, restored, next); err != nil {
				t.Fatal(err)
			}
			var out gofuse.EntryOut
			if st := next.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "a", &out); st != gofuse.ENOENT {
				t.Fatalf("deleted source resurrected: %v", st)
			}
			if op == "rename" {
				if st := next.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "b", &out); st != gofuse.OK {
					t.Fatalf("renamed target missing: %v", st)
				}
			}
		})
	}
}

func TestLayerRollbackDiscardsDurableView(t *testing.T) {
	for _, wal := range []bool{false, true} {
		t.Run(map[bool]string{false: "disk", true: "wal"}[wal], func(t *testing.T) {
			idx, shadow := newLayerShutdownState(t)
			var journal *Journal
			if wal {
				var err error
				journal, err = NewJournal(filepath.Join(t.TempDir(), "journal"))
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = journal.Close() }()
				idx.SetJournal(journal)
			}
			for _, p := range []string{"/clean", "/dirty"} {
				if err := shadow.WriteFull(p, []byte("data"), 0); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := idx.PutLayerCache("/clean", 4, 0, 0, false); err != nil {
				t.Fatal(err)
			}
			if _, err := idx.Put("/dirty", 4, PendingNew); err != nil {
				t.Fatal(err)
			}
			ts := httptest.NewServer(http.NotFoundHandler())
			defer ts.Close()
			fs := NewDat9FS(newTestClient(ts.URL), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/"})
			fs.pendingIndex, fs.shadowStore = idx, shadow
			fs.applyLayerRollback(shadow, idx)
			var out gofuse.EntryOut
			if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "clean", &out); st != gofuse.ENOENT {
				t.Errorf("rollback Lookup = %v", st)
			}
			for _, e := range fs.mergePendingDirEntries("/", nil) {
				if e.Name == "clean" {
					t.Error("rollback readdir still exposes abandoned clean file")
				}
			}
			if idx.HasPending("/clean") || shadow.Has("/clean") {
				t.Error("clean cache survived rollback")
			}
			m, ok := idx.GetMeta("/dirty")
			if !ok || m.Kind != PendingConflict || !shadow.Has("/dirty") {
				t.Fatalf("dirty recovery data lost: %+v", m)
			}
			recovered, err := NewPendingIndex(idx.dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := recovered.RecoverFromDisk(); err != nil {
				t.Fatal(err)
			}
			if wal {
				if err := replayJournalIntoPending(journal, recovered, shadow); err != nil {
					t.Fatal(err)
				}
			}
			if recovered.HasPending("/clean") {
				t.Error("abandoned cache resurrected from disk/WAL")
			}
			m, ok = recovered.GetMeta("/dirty")
			if !ok || m.Kind != PendingConflict {
				t.Fatalf("conflict lost on recovery: %+v", m)
			}
		})
	}
}

func TestRestoreLayerPrunesOrphansBeforeReplay(t *testing.T) {
	for _, op := range []string{"upsert", "whiteout"} {
		t.Run(op, func(t *testing.T) {
			idx, shadow := newLayerShutdownState(t)
			if _, err := idx.Put("/a", 4, PendingNew); err != nil {
				t.Fatal(err)
			}
			e := client.FSLayerEntry{LayerID: "layer-1", Path: "/a", Op: op, Kind: "file", Content: []byte("data"), SizeBytes: 4}
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/v1/layers/layer-1/diff" {
					_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FSLayerEntry{e}})
				} else {
					_ = json.NewEncoder(w).Encode(e)
				}
			}))
			defer ts.Close()
			fs := NewDat9FS(newTestClient(ts.URL), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/"})
			fs.pendingIndex, fs.shadowStore = idx, shadow
			if err := restoreLayerEntries(context.Background(), fs.client, fs.opts, shadow, idx, fs); err != nil {
				t.Fatal(err)
			}
			if op == "whiteout" {
				if !fs.isLayerWhiteout("/a") || idx.HasPending("/a") {
					t.Fatal("orphan hid authoritative whiteout")
				}
			} else {
				m, ok := idx.GetMeta("/a")
				data, err := shadow.ReadAll("/a")
				if !ok || !m.LayerClean || err != nil || string(data) != "data" {
					t.Fatalf("orphan hid durable file: %+v %q %v", m, data, err)
				}
			}
		})
	}
}

func TestLateDrainMakesProgressBesideConflict(t *testing.T) {
	idx, shadow := newLayerShutdownState(t)
	for _, p := range []string{"/bad", "/good"} {
		if err := shadow.WriteFull(p, []byte("data"), 0); err != nil {
			t.Fatal(err)
		}
		kind := PendingNew
		if p == "/bad" {
			kind = PendingConflict
		}
		if _, err := idx.Put(p, 4, kind); err != nil {
			t.Fatal(err)
		}
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req client.FSLayerEntryRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Path != "/good" {
			t.Errorf("attempted conflicted upload: %s", req.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"entry_seq":1}`))
	}))
	defer ts.Close()
	cq := NewCommitQueue(newTestClient(ts.URL), shadow, idx, nil, 1, 8)
	cq.SetLayerRef("layer-1")
	cq.DrainAll()
	fs := &Dat9FS{pendingIndex: idx, commitQueue: cq}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := fs.drainLatePendingEntries(ctx); err == nil {
		t.Fatal("conflict not reported")
	}
	if m, _ := idx.GetMeta("/good"); m == nil || !m.LayerClean {
		t.Fatalf("healthy late write not committed: %+v", m)
	}
	if m, _ := idx.GetMeta("/bad"); m == nil || m.Kind != PendingConflict || !shadow.Has("/bad") {
		t.Fatal("conflict data not preserved")
	}
}

func TestLateDrainStopsWhenUnrecoverable(t *testing.T) {
	for _, kind := range []string{"short_shadow", "unknown_base"} {
		t.Run(kind, func(t *testing.T) {
			idx, shadow := newLayerShutdownState(t)
			if err := shadow.WriteFull("/a", []byte("x"), 0); err != nil {
				t.Fatal(err)
			}
			size := int64(1)
			if kind == "short_shadow" {
				size = 4
			}
			if _, err := idx.PutWithBaseRev("/a", size, PendingOverwrite, 0); err != nil {
				t.Fatal(err)
			}
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Errorf("unrecoverable payload uploaded: %s", r.URL)
				w.WriteHeader(500)
			}))
			defer ts.Close()
			cq := NewCommitQueue(newTestClient(ts.URL), shadow, idx, nil, 1, 8)
			if kind == "short_shadow" {
				cq.SetLayerRef("layer-1")
			}
			cq.DrainAll()
			fs := &Dat9FS{pendingIndex: idx, commitQueue: cq}
			ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
			defer cancel()
			err := fs.drainLatePendingEntries(ctx)
			if err == nil || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("drain must diagnose before deadline: %v", err)
			}
			if !strings.Contains(err.Error(), "preserved for recovery") {
				t.Fatalf("missing recovery diagnosis: %v", err)
			}
			if !idx.HasPending("/a") || !shadow.Has("/a") {
				t.Fatal("unrecoverable data discarded")
			}
		})
	}
}

// Publish between reading bytes and acknowledging the upload. This is the
// window allowed when the writable same-path lock times out.
func TestLayerChmodDoesNotAcknowledgeNewPublication(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "initially_absent", true: "existing_metadata"}[existing], func(t *testing.T) {
			idx, shadow := newLayerShutdownState(t)
			if err := shadow.WriteFull("/a", []byte("old"), 0); err != nil {
				t.Fatal(err)
			}
			if existing {
				if _, err := idx.Put("/a", 3, PendingNew); err != nil {
					t.Fatal(err)
				}
			}
			var uploaded []byte
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					var req client.FSLayerEntryRequest
					_ = json.NewDecoder(r.Body).Decode(&req)
					uploaded = append([]byte(nil), req.Content...)
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"entry_seq":1}`))
					return
				}
				http.NotFound(w, r)
			}))
			defer ts.Close()
			fs := NewDat9FS(newTestClient(ts.URL), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/"})
			fs.pendingIndex, fs.shadowStore = idx, shadow
			var next uint64
			testHookAfterLayerChmodShadowRead = func(p string) {
				if p != "/a" {
					return
				}
				if err := shadow.WriteFull(p, []byte("new"), 0); err != nil {
					t.Fatal(err)
				}
				var err error
				next, err = idx.Put(p, 3, PendingNew)
				if err != nil {
					t.Fatal(err)
				}
			}
			t.Cleanup(func() { testHookAfterLayerChmodShadowRead = nil })
			if err := fs.upsertLayerChmod(context.Background(), "/a", 0600); err != nil {
				t.Fatal(err)
			}
			if string(uploaded) != "old" {
				t.Fatalf("upload=%q", uploaded)
			}
			m, ok := idx.GetMeta("/a")
			if !ok || m.Generation != next || m.LayerClean {
				t.Fatalf("new unuploaded bytes acknowledged: %+v", m)
			}
			if got, err := shadow.ReadAll("/a"); err != nil || string(got) != "new" {
				t.Fatalf("local edit lost: %q %v", got, err)
			}
		})
	}
}

func TestRestoreLayerRebindsCleanReadCache(t *testing.T) {
	idx, shadow := newLayerShutdownState(t)
	if err := shadow.WriteFull("/a", []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := idx.PutLayerCache("/a", 4, 0, 0, false); err != nil {
		t.Fatal(err)
	}
	recovered, err := NewPendingIndex(idx.dir)
	if err != nil {
		t.Fatal(err)
	}
	shadow.Close()
	shadow, err = NewShadowStoreWithQuota(shadow.dir, 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shadow.Close)
	recovered.setShadowStore(shadow)
	if err := recovered.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/layers/layer-1/diff" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FSLayerEntry{{Path: "/a", Op: "upsert", Kind: "file", Content: []byte("data"), SizeBytes: 4}}})
			return
		}
		t.Errorf("durable local cache must serve read: %s %s", r.Method, r.URL)
		http.NotFound(w, r)
	}))
	defer ts.Close()
	fs := NewDat9FS(newTestClient(ts.URL), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/", CacheSize: 1 << 20})
	fs.pendingIndex, fs.shadowStore = recovered, shadow
	if err := restoreLayerEntries(context.Background(), fs.client, fs.opts, shadow, recovered, fs); err != nil {
		t.Fatal(err)
	}
	var entry gofuse.EntryOut
	if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "a", &entry); st != gofuse.OK {
		t.Fatal(st)
	}
	var opened gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: entry.NodeId}}, &opened); st != gofuse.OK {
		t.Fatal(st)
	}
	defer fs.Release(nil, &gofuse.ReleaseIn{Fh: opened.Fh})
	got, st, err := readDat9FSTestRange(fs, entry.NodeId, opened.Fh, 0, 4)
	if err != nil || st != gofuse.OK || string(got) != "data" {
		t.Fatalf("restored read=%q %v %v", got, st, err)
	}
}

func TestLayerWriteSyncReopensWithoutBaseFile(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"entry_seq":1}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()
	idx, shadow := newLayerShutdownState(t)
	fs := NewDat9FS(newTestClient(ts.URL), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/", CacheSize: 1 << 20, WritePolicy: WritePolicyWriteSync})
	fs.pendingIndex, fs.shadowStore = idx, shadow
	var created gofuse.CreateOut
	if st := fs.Create(nil, &gofuse.CreateIn{InHeader: gofuse.InHeader{NodeId: 1}, Flags: syscall.O_RDWR, Mode: 0600}, "a", &created); st != gofuse.OK {
		t.Fatal(st)
	}
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh}, []byte("data")); st != gofuse.OK {
		t.Fatal(st)
	}
	if st := fs.Flush(nil, &gofuse.FlushIn{Fh: created.Fh}); st != gofuse.OK {
		t.Fatal(st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{Fh: created.Fh})
	var opened gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: created.NodeId}, Flags: syscall.O_RDWR}, &opened); st != gofuse.OK {
		t.Fatalf("reopen durable layer file: %v", st)
	}
	defer fs.Release(nil, &gofuse.ReleaseIn{Fh: opened.Fh})
	if st := fs.Fsync(nil, &gofuse.FsyncIn{Fh: opened.Fh}); st != gofuse.OK {
		t.Fatal(st)
	}
	got, st, err := readDat9FSTestRange(fs, created.NodeId, opened.Fh, 0, 4)
	if err != nil || st != gofuse.OK || string(got) != "data" {
		t.Fatalf("reopened read=%q %v %v", got, st, err)
	}
}

func TestLayerRollbackFencesLatePublication(t *testing.T) {
	idx, shadow := newLayerShutdownState(t)
	if err := shadow.WriteFull("/a", []byte("old"), 0); err != nil {
		t.Fatal(err)
	}
	gen, err := idx.PutLayerCache("/a", 3, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	// A writer has replaced the shadow but not yet published its metadata.
	if err := shadow.WriteFull("/a", []byte("new"), 0); err != nil {
		t.Fatal(err)
	}
	fs := newTestDrainFS()
	fs.pendingIndex, fs.shadowStore = idx, shadow
	fs.applyLayerRollback(shadow, idx)
	if got, err := shadow.ReadAll("/a"); err != nil || string(got) != "new" {
		t.Fatalf("rollback deleted newer shadow: %q %v", got, err)
	}
	if _, err := idx.PutLayerCache("/a", 3, 0, 0, false); !errors.Is(err, errLayerRolledBack) {
		t.Fatalf("late clean publication accepted: %v", err)
	}
	next, err := idx.Put("/a", 3, PendingNew)
	if err != nil {
		t.Fatal(err)
	}
	for _, generation := range []uint64{gen, next} {
		if err := idx.MarkLayerCommittedIfGeneration("/a", generation, 0, 0, false); err != nil {
			t.Fatal(err)
		}
	}
	if m, _ := idx.GetMeta("/a"); m == nil || m.Kind != PendingConflict || m.LayerClean {
		t.Fatalf("late acknowledgement revived rolled-back cache: %+v", m)
	}
}
