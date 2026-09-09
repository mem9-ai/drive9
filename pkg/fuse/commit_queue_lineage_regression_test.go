package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

func newLineageQueue(t *testing.T, url string) *CommitQueue {
	t.Helper()
	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shadow.Close() })
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := newTestClient(url)
	c.SetSmallFileThresholdForTests(1 << 20)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	t.Cleanup(cq.DrainAll)
	return cq
}

func stageLineageEntry(t *testing.T, cq *CommitQueue, path string, data []byte, id, parent string, spill bool) *CommitEntry {
	t.Helper()
	if err := cq.shadows.WriteFull(path, data, 0); err != nil {
		t.Fatal(err)
	}
	var gen uint64
	var err error
	if spill {
		gen, err = cq.index.PutShadowSpillWithModeAndLineage(path, int64(len(data)), PendingNew, 0, 0, false, id, parent, true)
	} else {
		gen, err = cq.index.PutWithBaseRevAndModeAndLineage(path, int64(len(data)), PendingNew, 0, 0, false, id, parent, true)
	}
	if err != nil {
		t.Fatal(err)
	}
	return &CommitEntry{Path: path, Size: int64(len(data)), Kind: PendingNew, PayloadBaseRevSet: true,
		ShadowSpill: spill, ShadowGen: cq.shadows.ActiveGeneration(path), PendingIndexGen: gen,
		SnapshotID: id, ParentSnapshotID: parent, liveLineageProof: true}
}

// Reproduce the original spill window precisely: upload has returned and
// released its generation lock, but success has not yet recorded a landmark.
func TestCommitQueueSpillLandedProofSurvivesReplacementBeforeSuccess(t *testing.T) {
	const path = "/spill-tail.db"
	server, ts := newCASFileServer(t, path, 0, nil)
	t.Cleanup(ts.Close)
	cq := newLineageQueue(t, ts.URL)
	parent := stageLineageEntry(t, cq, path, []byte("parent"), "A", "", true)
	rev, err := cq.uploadEntry(context.Background(), parent)
	if err != nil {
		t.Fatal(err)
	}
	child := stageLineageEntry(t, cq, path, []byte("parent-grown"), "B", "A", true)
	if err := cq.onCommitSuccess(parent, 0, rev); err != nil {
		t.Fatal(err)
	}
	proof := cq.landedCommit(path)
	if proof.snapshotID != "A" || proof.checksum != payloadChecksum([]byte("parent")) {
		t.Fatalf("uploaded parent proof = %+v", proof)
	}
	if meta, ok := cq.index.GetMeta(path); !ok || meta.Generation != child.PendingIndexGen {
		t.Fatal("parent cleanup removed the child generation")
	}
	child.DurableWatermarkRev = 1
	if err := cq.CommitNow(context.Background(), child); err != nil {
		t.Fatal(err)
	}
	if rev, body, _ := server.snapshot(); rev != 2 || string(body) != "parent-grown" {
		t.Fatalf("rev=%d body=%q", rev, body)
	}
}

func TestCommitQueueSpillConflictReadsHonorCancellation(t *testing.T) {
	for _, phase := range []string{"stat", "growth-read", "equal-read"} {
		t.Run(phase, func(t *testing.T) {
			const path = "/cancel-spill.db"
			payload := []byte("parent-grown")
			parentSize := len("parent")
			if phase == "equal-read" {
				parentSize = len(payload)
			}
			started := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPut {
					_, _ = io.Copy(io.Discard, r.Body)
					w.WriteHeader(http.StatusConflict)
					return
				}
				if (phase == "stat" && r.Method == http.MethodHead) || (phase != "stat" && r.Method == http.MethodGet) {
					once.Do(func() { close(started) })
					select {
					case <-r.Context().Done():
					case <-release:
					}
					return
				}
				if r.Method == http.MethodHead {
					w.Header().Set("Content-Length", strconv.Itoa(parentSize))
					w.Header().Set("X-Dat9-Revision", "1")
					w.WriteHeader(http.StatusOK)
					return
				}
				http.NotFound(w, r)
			}))
			t.Cleanup(ts.Close)
			cq := newLineageQueue(t, ts.URL)
			// Cleanup must unblock the handler before draining, including on assertion failure.
			t.Cleanup(func() { close(release); cq.CancelPathPreserveLocal(path) })
			entry := stageLineageEntry(t, cq, path, payload, "B", "A", true)
			// No watermark: the actual initial PUT hits 409 and enters spill resolution.
			if err := cq.Enqueue(entry); err != nil {
				t.Fatal(err)
			}
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("conflict read did not start")
			}
			cq.CancelPathPreserveLocal(path)
			drained := make(chan struct{})
			go func() { cq.WaitPath(path); close(drained) }()
			select {
			case <-drained:
			case <-time.After(2 * time.Second):
				t.Fatal("canceled spill read did not drain")
			}
			meta, ok := cq.index.GetMeta(path)
			if !ok || meta.Kind == PendingConflict {
				t.Fatalf("canceled staging=%+v exists=%t", meta, ok)
			}
		})
	}
}

func TestCommitQueueLayerGrowthPreservesMainClaim(t *testing.T) {
	for _, spill := range []bool{false, true} {
		for _, mainParent := range []bool{false, true} {
			t.Run(fmt.Sprintf("spill=%t/main-parent=%t", spill, mainParent), func(t *testing.T) {
				const path = "/layer-growth.db"
				parentBody := []byte("header")
				childBody := []byte("header-grown")
				main := &casFileServer{t: t, path: path}
				type claim struct {
					rev  int64
					body []byte
				}
				claims := make(chan claim, 2)
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/v1/fs"+path {
						main.serveHTTP(w, r)
						return
					}
					var got claim
					switch r.URL.Path {
					case "/v1/layers/L/entries":
						var req client.FSLayerEntryRequest
						if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
							http.Error(w, err.Error(), 400)
							return
						}
						got = claim{req.BaseRevision, req.Content}
					case "/v1/layers/L/objects":
						got.rev, _ = strconv.ParseInt(r.URL.Query().Get("base_revision"), 10, 64)
						got.body, _ = io.ReadAll(r.Body)
					default:
						http.NotFound(w, r)
						return // SDK fallback to /objects
					}
					claims <- got
					_ = json.NewEncoder(w).Encode(map[string]any{"layer_id": "L", "path": path, "op": "upsert", "kind": "file", "size_bytes": len(got.body)})
				}))
				t.Cleanup(ts.Close)
				cq := newLineageQueue(t, ts.URL)
				if !mainParent {
					cq.SetLayerRef("L")
				}
				parent := stageLineageEntry(t, cq, path, parentBody, "A", "", spill)
				if err := cq.CommitNow(context.Background(), parent); err != nil {
					t.Fatal(err)
				}
				if !mainParent {
					got := <-claims
					if got.rev != 0 || !bytes.Equal(got.body, parentBody) {
						t.Fatalf("parent claim=%+v", got)
					}
					if proof := cq.landedCommit(path); proof.snapshotID != "" {
						t.Fatalf("overlay invented main landmark: %+v", proof)
					}
				}
				// An existing main proof exercises post-validation CAS wiring; this
				// is not a claim that overlay uploads increment main revisions.
				cq.SetLayerRef("L")
				child := stageLineageEntry(t, cq, path, childBody, "B", "A", spill)
				wantRev := int64(0)
				if mainParent {
					wantRev = 1
					child.DurableWatermarkRev = 1
				}
				if err := cq.CommitNow(context.Background(), child); err != nil {
					t.Fatal(err)
				}
				got := <-claims
				if got.rev != wantRev || !bytes.Equal(got.body, childBody) {
					t.Fatalf("child claim rev=%d body=%q, want %d/%q", got.rev, got.body, wantRev, childBody)
				}
				if child.PayloadBaseRev != 0 {
					t.Fatal("payload audit base changed")
				}
			})
		}
	}
}

func TestCommitQueueCoalescePreservesModeClaim(t *testing.T) {
	for _, mode := range []struct {
		has  bool
		mode uint32
	}{{false, 0}, {true, 0o600}} {
		cq := newLineageQueue(t, "http://127.0.0.1")
		parent := &CommitEntry{Path: "/mode", Kind: PendingNew, PayloadBaseRevSet: true, SnapshotID: "A", liveLineageProof: true, HasMode: true, Mode: 0o640}
		child := stageLineageEntry(t, cq, "/mode", []byte("grown"), "B", "A", false)
		child.HasMode, child.Mode = mode.has, mode.mode
		cq.mu.Lock()
		cq.queue = []*CommitEntry{parent, child}
		cq.rebuildQueuedIndexLocked()
		cq.coalesceDirectParentQueuedLocked(child)
		canceled := parent.canceled
		cq.queue = nil
		cq.rebuildQueuedIndexLocked()
		cq.mu.Unlock()
		if canceled || child.ParentSnapshotID != "A" {
			t.Fatal("coalesced across a different mode claim")
		}
	}
}

func TestCommitQueueSmallSpillMultipartRetainsUploadedProof(t *testing.T) {
	const path = "/multipart-parent.db"
	payload := []byte("small-parent")
	expected := int64(0)
	rec := newMultipartUploadRecorder(t, path, int64(len(payload)), &expected)
	t.Cleanup(rec.server.Close)
	cq := newLineageQueue(t, rec.server.URL)
	cq.client.SetSmallFileThresholdForTests(1) // force multipart despite the bounded image
	entry := stageLineageEntry(t, cq, path, payload, "A", "", true)
	rev, err := cq.uploadEntry(context.Background(), entry)
	if err != nil {
		t.Fatal(err)
	}
	stageLineageEntry(t, cq, path, []byte("small-parent-grown"), "B", "A", true)
	if err := cq.onCommitSuccess(entry, 0, rev); err != nil {
		t.Fatal(err)
	}
	proof := cq.landedCommit(path)
	if proof.checksum != payloadChecksum(payload) || proof.snapshotID != "A" {
		t.Fatalf("proof=%+v", proof)
	}
	if rec.completeCalls.Load() != 1 || rec.directFilePuts.Load() != 0 {
		t.Fatal("test did not use multipart completion")
	}
}

func TestCommitQueueSmallSpillChecksActualSizeBeforeBinding(t *testing.T) {
	cq := newLineageQueue(t, "http://127.0.0.1:1")
	entry := stageLineageEntry(t, cq, "/oversize", make([]byte, maxLandedPayloadBytes+1), "A", "", true)
	entry.Size = 8 // stale/mismatched queue metadata must not permit an unbounded allocation
	if _, err := cq.uploadEntry(context.Background(), entry); err == nil {
		t.Fatal("accepted mismatched spill size")
	}
	if entry.payloadBound {
		t.Fatal("bound an oversized spill image")
	}
}

func TestCommitQueueRenamedSnapshotCommitsThenAuthorizesGrowth(t *testing.T) {
	const oldPath = "/old.db"
	const newPath = "/new.db"
	server, ts := newCASFileServer(t, newPath, 0, nil)
	t.Cleanup(ts.Close)
	cq := newLineageQueue(t, ts.URL)
	parent := stageLineageEntry(t, cq, oldPath, []byte("header"), "A", "", true)
	prepared, err := cq.index.PrepareRename(oldPath, newPath)
	if err != nil || prepared == nil {
		t.Fatalf("prepare=%+v err=%v", prepared, err)
	}
	if !cq.shadows.Rename(oldPath, newPath) {
		t.Fatal("shadow rename failed")
	}
	cq.index.CommitRename(oldPath, prepared)
	parent.Path = newPath
	parent.ShadowGen = cq.shadows.ActiveGeneration(newPath)
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(cq.client, opts)
	fs.bindCommitEntryToPreparedPendingMeta(parent, prepared)
	if err := cq.CommitNow(context.Background(), parent); err != nil {
		t.Fatal(err)
	}
	if proof := cq.landedCommit(oldPath); proof.snapshotID != "" {
		t.Fatal("landmark remained at old path")
	}
	child := stageLineageEntry(t, cq, newPath, []byte("header-grown"), "B", "A", true)
	child.DurableWatermarkRev = 1
	if err := cq.CommitNow(context.Background(), child); err != nil {
		t.Fatal(err)
	}
	if rev, body, puts := server.snapshot(); rev != 2 || string(body) != "header-grown" || len(puts) != 2 {
		t.Fatalf("renamed growth rev=%d body=%q puts=%d", rev, body, len(puts))
	}
}

func TestCommitQueueCanceledIdempotentSuccessPreservesStaging(t *testing.T) {
	for _, spill := range []bool{false, true} {
		for _, cancelContext := range []bool{false, true} {
			t.Run(fmt.Sprintf("spill=%t/context=%t", spill, cancelContext), func(t *testing.T) {
				cq := newLineageQueue(t, "http://127.0.0.1:1")
				entry := stageLineageEntry(t, cq, "/canceled", []byte("equal"), "A", "", spill)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if cancelContext {
					cancel()
				} else {
					entry.canceled = true
				}
				published := false
				cq.OnUploaded = func(*CommitEntry, int64) { published = true }
				// The equality read has succeeded, but cancellation won before its
				// shared buffered/spill success tail begins.
				cq.finishIdempotentCommit(ctx, entry, 1)
				if published {
					t.Fatal("published canceled equality result")
				}
				if meta, ok := cq.index.GetMeta(entry.Path); !ok || meta.Kind == PendingConflict {
					t.Fatalf("staging=%+v exists=%t", meta, ok)
				}
				if !cq.shadows.Has(entry.Path) {
					t.Fatal("removed canceled shadow")
				}
			})
		}
	}
}
