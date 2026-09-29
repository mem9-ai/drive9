package fuse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestLayerRefreshConflictsIdentitylessPending(t *testing.T) {
	for _, wal := range []bool{false, true} {
		for _, base := range []int64{0, 7} {
			for _, op := range []string{"upsert", "whiteout"} {
				name := "wal=" + strconv.FormatBool(wal) + "/base=" + strconv.FormatInt(base, 10) + "/" + op
				t.Run(name, func(t *testing.T) {
					idx, shadows := newLayerShutdownState(t)
					var journal *Journal
					if wal {
						var err error
						journal, err = NewJournal(filepath.Join(t.TempDir(), "journal"))
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { _ = journal.Close() })
						idx.SetJournal(journal)
					}
					kind := PendingNew
					if base > 0 {
						kind = PendingOverwrite
					}
					if err := shadows.WriteFull("/a", []byte("local"), base); err != nil {
						t.Fatal(err)
					}
					if _, err := idx.PutWithBaseRev("/a", 5, kind, base); err != nil {
						t.Fatal(err)
					}
					// Reconstruct the closed writer's publication without any live
					// handles, exactly as startup does before replaying the Layer.
					idx = recoverLayerPendingTestIndex(t, idx.dir, shadows, journal)
					var posts atomic.Int64
					tip := client.FSLayerEntry{LayerID: "layer-1", Path: "/a", Op: op, Kind: "file", Content: []byte("external"), SizeBytes: 8, EntrySeq: 1}
					ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.Header().Set("Content-Type", "application/json")
						if r.Method == http.MethodPost {
							posts.Add(1)
							_ = json.NewEncoder(w).Encode(tip)
							return
						}
						switch r.URL.Path {
						case "/v1/layers/layer-1/events":
							_ = json.NewEncoder(w).Encode(map[string]any{"events": []client.FSLayerEvent{{Seq: 1, Path: "/a", Op: op}}})
						case "/v1/layers/layer-1/diff":
							_ = json.NewEncoder(w).Encode(map[string]any{"entries": []client.FSLayerEntry{tip}})
						default:
							http.NotFound(w, r)
						}
					}))
					defer ts.Close()
					fs := NewDat9FS(newTestClient(ts.URL), &MountOptions{LayerRef: "layer-1", RemoteRoot: "/", SyncMode: SyncStrict})
					fs.pendingIndex, fs.shadowStore = idx, shadows
					cq := NewCommitQueue(fs.client, shadows, idx, nil, 1, 8)
					cq.SetLayerRef("layer-1")
					defer cq.DrainAll()
					fs.commitQueue = cq
					queued := cq.recoveredEntry("/a")
					if queued == nil {
						t.Fatal("fixture is not a recoverable first write")
					}
					seq, err := refreshLayerEvents(context.Background(), fs.client, fs.opts, shadows, idx, fs, 0)
					if err != nil || seq != 1 {
						t.Fatalf("refresh: seq=%d err=%v", seq, err)
					}
					if meta, _ := idx.GetMeta("/a"); meta == nil || meta.Kind != PendingConflict {
						t.Errorf("observed tip did not conflict first local write: %+v", meta)
					}
					if n := cq.RecoverPendingSync(context.Background()); n != 0 {
						t.Errorf("recovery uploaded %d conflicting publications", n)
					}
					// A deferred upload may have captured its entry before refresh.
					// Its generation still matches; the upload itself must recheck.
					if err := cq.CommitNow(context.Background(), queued); err == nil {
						t.Error("previously captured upload ignored observed conflict")
					}
					if !queued.DisableAutoResolveLWW {
						t.Error("observed Layer conflict still allows automatic overwrite")
					}
					// The worker must also stop before a streamed upload, without
					// trying base-revision auto-resolution or removing local bytes.
					queued.ShadowSpill = true
					cq.commitOne(queued)
					idx = recoverLayerPendingTestIndex(t, idx.dir, shadows, journal)
					if meta, _ := idx.GetMeta("/a"); meta == nil || meta.Kind != PendingConflict || meta.BaseRev != base {
						t.Fatalf("conflict lost on restart: %+v", meta)
					}
					fs.pendingIndex = idx
					var node gofuse.EntryOut
					if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "a", &node); st != gofuse.OK {
						t.Fatal(st)
					}
					// chmod also publishes shadow content in Layer mode, even with
					// no open writer. It must not clear the recovery conflict.
					var attr gofuse.AttrOut
					if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
						InHeader: gofuse.InHeader{NodeId: node.NodeId}, Valid: gofuse.FATTR_MODE, Mode: 0o600,
					}}, &attr); st != gofuse.EAGAIN {
						t.Errorf("conflicted chmod=%v, want EAGAIN", st)
					}
					var opened gofuse.OpenOut
					if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: node.NodeId}, Flags: syscall.O_RDWR}, &opened); st != gofuse.OK {
						t.Fatal(st)
					}
					if st := fs.Flush(nil, &gofuse.FlushIn{Fh: opened.Fh}); st != gofuse.EAGAIN {
						t.Errorf("reopened conflict Flush=%v, want EAGAIN", st)
					}
					fs.Release(nil, &gofuse.ReleaseIn{Fh: opened.Fh})
					if data, err := shadows.ReadAll("/a"); err != nil || string(data) != "local" {
						t.Errorf("local recovery payload=%q err=%v", data, err)
					}
					if got := posts.Load(); got != 0 {
						t.Errorf("conflict sent %d uploads, want zero", got)
					}
				})
			}
		}
	}
}

func TestPendingRestagingRetainsIdentitylessConflict(t *testing.T) {
	for _, wal := range []bool{false, true} {
		t.Run("wal="+strconv.FormatBool(wal), func(t *testing.T) {
			idx, shadows := newLayerShutdownState(t)
			var journal *Journal
			if wal {
				var err error
				journal, err = NewJournal(filepath.Join(t.TempDir(), "journal"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = journal.Close() })
				idx.SetJournal(journal)
			}
			if err := shadows.WriteFull("/a", []byte("local"), 0); err != nil {
				t.Fatal(err)
			}
			if _, err := idx.Put("/a", 5, PendingNew); err != nil {
				t.Fatal(err)
			}
			if err := idx.MarkConflict("/a"); err != nil {
				t.Fatal(err)
			}
			for _, spill := range []bool{false, true} {
				var err error
				if spill {
					_, err = idx.PutShadowSpill("/a", 5, PendingNew, 0)
				} else {
					_, err = idx.Put("/a", 5, PendingNew)
				}
				if err != nil {
					t.Fatal(err)
				}
				for _, state := range []*PendingIndex{idx, recoverLayerPendingTestIndex(t, idx.dir, shadows, journal)} {
					if meta, _ := state.GetMeta("/a"); meta == nil || meta.Kind != PendingConflict || meta.LayerEntrySeq != 0 {
						t.Errorf("restaging erased identity-less conflict (spill=%v): %+v", spill, meta)
					}
				}
			}
		})
	}
}

func recoverLayerPendingTestIndex(t *testing.T, dir string, shadows *ShadowStore, journal *Journal) *PendingIndex {
	t.Helper()
	idx, err := NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	idx.setShadowStore(shadows)
	if err := idx.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	if err := replayJournalIntoPending(journal, idx, shadows); err != nil {
		t.Fatal(err)
	}
	if journal != nil {
		idx.SetJournal(journal)
	}
	return idx
}
