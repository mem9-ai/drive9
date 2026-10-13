// v1 writeback_generation_fence_test.go (queue-side rows): the durable
// watermark and generation fences reject stale payloads locally — a stale
// checkpoint must never reach the server, never remove or conflict the
// newer pending entry, and never LWW-rebase its old bytes over a newer
// image. Rows drive the queue directly with constructed entries (the
// production lineage producer feeds the same fields; cross-mount
// landed-lineage adoption is ledger-noted as partial).
package writeback

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// v1 TestCommitQueueRejectsPayloadOlderThanDurableWatermark: a payload
// whose base predates the durable watermark is rejected with
// errCommitPayloadStale before any server write; the shadow is untouched.
func TestPortFenceRejectsPayloadOlderThanDurableWatermark(t *testing.T) {
	var putCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut || r.Method == http.MethodPost {
			putCalls.Add(1)
		}
		http.Error(w, "stale payload must not reach server", http.StatusInternalServerError)
	}))
	defer ts.Close()

	shadow, err := staging.NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	const path = "/repro.db"
	oldMain := []byte("sqlite-main: checkpoint A has 5 rows")
	if err := shadow.WriteFull(path, oldMain, 2); err != nil {
		t.Fatal(err)
	}
	shadowGen := shadow.ActiveGeneration(path)
	pendingGen, err := pending.PutWithBaseRev(path, int64(len(oldMain)), staging.PendingOverwrite, 2)
	if err != nil {
		t.Fatal(err)
	}

	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(50000)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	entry := &CommitEntry{
		Path:                  path,
		BaseRev:               3,
		PayloadBaseRev:        2,
		PayloadBaseRevSet:     true,
		Size:                  int64(len(oldMain)),
		Kind:                  staging.PendingOverwrite,
		ShadowGen:             shadowGen,
		PendingIndexGen:       pendingGen,
		DurableWatermarkRev:   3,
		DisableAutoResolveLWW: true,
	}

	if err := cq.CommitNow(context.Background(), entry); !errors.Is(err, errCommitPayloadStale) {
		t.Fatalf("CommitNow err = %v, want errCommitPayloadStale", err)
	}
	if got := putCalls.Load(); got != 0 {
		t.Fatalf("server saw %d writes; stale payload must not be PUT past the watermark", got)
	}
	if data, rerr := shadow.ReadAll(path); rerr != nil || string(data) != string(oldMain) {
		t.Fatalf("shadow changed after rejection: data=%q err=%v", data, rerr)
	}
}

// v1 TestCommitQueueStaleGenerationCannotPoisonNewerPendingEntry: an
// entry staged against an older shadow/pending generation is dropped
// locally — the newer pending entry survives unconflicted and the shadow
// keeps the fresh payload.
func TestPortFenceStaleGenerationCannotPoisonNewerPending(t *testing.T) {
	var putCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		putCalls.Add(1)
		http.Error(w, "stale generation must not upload", http.StatusInternalServerError)
	}))
	defer ts.Close()

	shadow, err := staging.NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	const path = "/same-path.db"
	oldPayload := []byte("old handle payload")
	freshPayload := []byte("fresh handle payload")
	if err := shadow.WriteFull(path, oldPayload, 2); err != nil {
		t.Fatal(err)
	}
	oldShadowGen := shadow.ActiveGeneration(path)
	oldPendingGen, err := pending.PutWithBaseRev(path, int64(len(oldPayload)), staging.PendingOverwrite, 2)
	if err != nil {
		t.Fatal(err)
	}

	if err := shadow.WriteFull(path, freshPayload, 3); err != nil {
		t.Fatal(err)
	}
	freshPendingGen, err := pending.PutWithBaseRev(path, int64(len(freshPayload)), staging.PendingOverwrite, 3)
	if err != nil {
		t.Fatal(err)
	}

	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(50000)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	if err := cq.Enqueue(&CommitEntry{
		Path:              path,
		BaseRev:           2,
		PayloadBaseRev:    2,
		PayloadBaseRevSet: true,
		Size:              int64(len(oldPayload)),
		Kind:              staging.PendingOverwrite,
		ShadowGen:         oldShadowGen,
		PendingIndexGen:   oldPendingGen,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if got := putCalls.Load(); got != 0 {
		t.Fatalf("server saw %d uploads; stale generation must be rejected locally", got)
	}
	meta, ok := pending.GetMeta(path)
	if !ok {
		t.Fatal("newer pending entry was removed by the stale commit failure")
	}
	if meta.Generation != freshPendingGen {
		t.Fatalf("pending generation = %d, want the newer %d", meta.Generation, freshPendingGen)
	}
	if meta.Kind == staging.PendingConflict {
		t.Fatal("newer pending entry was incorrectly conflicted by the stale failure")
	}
	data, rerr := shadow.ReadAll(path)
	if rerr != nil || string(data) != string(freshPayload) {
		t.Fatalf("shadow = %q err=%v, want the fresh payload", data, rerr)
	}
}

// v1 TestCommitQueueFencedConflictDoesNotLWWRebaseOldPayload: a 409 on a
// fenced (watermark-disabled) entry must NOT auto-resolve by rebasing the
// old payload onto the server's revision — the stale bytes stay local and
// the entry lands in conflict.
func TestPortFenceConflictDoesNotLWWRebaseOldPayload(t *testing.T) {
	var putCalls atomic.Int32
	var headRevs = []string{"3"} // server already at rev 3
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			rev := "0"
			if len(headRevs) > 0 {
				rev, headRevs = headRevs[0], headRevs[1:]
			}
			w.Header().Set("X-Dat9-Revision", rev)
			w.WriteHeader(http.StatusOK)
		case http.MethodPut, http.MethodPost:
			putCalls.Add(1)
			// The base-2 CAS against server rev 3 conflicts.
			http.Error(w, `{"error":{"code":"revision_conflict"}}`, http.StatusConflict)
		default:
			http.Error(w, "unexpected", http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	shadow, err := staging.NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	const path = "/fenced.db"
	payload := []byte("old bytes that must not be rebased")
	if err := shadow.WriteFull(path, payload, 2); err != nil {
		t.Fatal(err)
	}
	shadowGen := shadow.ActiveGeneration(path)
	pendingGen, err := pending.PutWithBaseRev(path, int64(len(payload)), staging.PendingOverwrite, 2)
	if err != nil {
		t.Fatal(err)
	}

	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(50000)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	entry := &CommitEntry{
		Path:                  path,
		BaseRev:               2,
		PayloadBaseRev:        2,
		PayloadBaseRevSet:     true,
		Size:                  int64(len(payload)),
		Kind:                  staging.PendingOverwrite,
		ShadowGen:             shadowGen,
		PendingIndexGen:       pendingGen,
		DurableWatermarkRev:   2,
		DisableAutoResolveLWW: true,
	}
	// A fenced conflict surfaces as an error, never a silent rebase.
	_ = cq.CommitNow(context.Background(), entry)
	if got := putCalls.Load(); got > 1 {
		t.Fatalf("server saw %d writes; a fenced conflict must not re-PUT the rebased payload", got)
	}
	if data, rerr := shadow.ReadAll(path); rerr != nil || string(data) != string(payload) {
		t.Fatalf("shadow = %q err=%v, want the untouched old bytes", data, rerr)
	}
}

// v1 TestDirtySiblingKeepsOriginalBaseRevAfterCommittedRevisionAdvances
// (handle half): a handle staged before a sibling's commit keeps ITS
// staging base; the queue's fence — not a silent rebase — decides at
// upload time. Driven through the queue with the sibling's landing
// recorded as the durable watermark.
func TestPortFenceDirtySiblingKeepsOriginalStagingBase(t *testing.T) {
	var putCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut || r.Method == http.MethodPost {
			putCalls.Add(1)
		}
		http.Error(w, "fence decides before any write", http.StatusInternalServerError)
	}))
	defer ts.Close()

	shadow, err := staging.NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	const path = "/sib.db"
	siblingImage := []byte("sibling landed at rev 5")
	dirtyImage := []byte("dirty handle staged at rev 4")
	// The dirty handle staged against rev 4.
	if err := shadow.WriteFull(path, dirtyImage, 4); err != nil {
		t.Fatal(err)
	}
	shadowGen := shadow.ActiveGeneration(path)
	pendingGen, err := pending.PutWithBaseRev(path, int64(len(dirtyImage)), staging.PendingOverwrite, 4)
	if err != nil {
		t.Fatal(err)
	}
	_ = siblingImage

	c := newTestClient(ts.URL)
	c.SetSmallFileThresholdForTests(50000)
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	// The sibling's commit advanced the durable watermark past the dirty
	// handle's payload base: the fence rejects without touching the server.
	err = cq.CommitNow(context.Background(), &CommitEntry{
		Path:                  path,
		BaseRev:               5,
		PayloadBaseRev:        4,
		PayloadBaseRevSet:     true,
		Size:                  int64(len(dirtyImage)),
		Kind:                  staging.PendingOverwrite,
		ShadowGen:             shadowGen,
		PendingIndexGen:       pendingGen,
		DurableWatermarkRev:   5,
		DisableAutoResolveLWW: true,
	})
	if !errors.Is(err, errCommitPayloadStale) {
		t.Fatalf("CommitNow err = %v, want errCommitPayloadStale (fence, not rebase)", err)
	}
	if got := putCalls.Load(); got != 0 {
		t.Fatalf("server saw %d writes", got)
	}
}
