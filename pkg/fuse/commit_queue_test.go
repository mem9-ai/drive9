package fuse

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mem9-ai/dat9/pkg/client"
)

func TestCommitQueueConditionalCommitSuccess(t *testing.T) {
	var gotExpected string
	var gotCommitted int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotExpected = r.Header.Get("X-Dat9-Expected-Revision")
		w.WriteHeader(http.StatusOK)
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

	if err := shadow.WriteFull("/ok.txt", []byte("data"), 7); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/ok.txt", 4, PendingOverwrite, 7); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(client.New(ts.URL, ""), shadow, pending, nil, 1, 8)
	cq.SetSuccessCallback(func(_ string, committedRevision int64) {
		gotCommitted = committedRevision
	})
	if err := cq.Enqueue(&CommitEntry{
		Path:    "/ok.txt",
		BaseRev: 7,
		Size:    4,
		Kind:    PendingOverwrite,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if gotExpected != "7" {
		t.Fatalf("expected revision header = %q, want 7", gotExpected)
	}
	if gotCommitted != 8 {
		t.Fatalf("committed revision callback = %d, want 8", gotCommitted)
	}
	if pending.HasPending("/ok.txt") {
		t.Fatal("pending entry should be removed after successful commit")
	}
	if shadow.Has("/ok.txt") {
		t.Fatal("shadow should be removed after successful commit")
	}
}

func TestCommitQueueConflictKeepsPendingState(t *testing.T) {
	var calls int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
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

	if err := shadow.WriteFull("/conflict.txt", []byte("data"), 3); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/conflict.txt", 4, PendingOverwrite, 3); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(client.New(ts.URL, ""), shadow, pending, nil, 1, 8)
	if err := cq.Enqueue(&CommitEntry{
		Path:    "/conflict.txt",
		BaseRev: 3,
		Size:    4,
		Kind:    PendingOverwrite,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if calls != 1 {
		t.Fatalf("conflict should stop retries after first attempt, got %d calls", calls)
	}
	// Terminal failure preserves shadow and pending data for manual recovery,
	// but marks the entry as PendingConflict so RecoverPending skips it.
	if !pending.HasPending("/conflict.txt") {
		t.Fatal("pending entry should be preserved after terminal conflict")
	}
	meta, ok := pending.GetMeta("/conflict.txt")
	if !ok || meta.Kind != PendingConflict {
		t.Fatalf("pending entry should be marked as PendingConflict, got kind=%v ok=%v", meta.Kind, ok)
	}
	if !shadow.Has("/conflict.txt") {
		t.Fatal("shadow should be preserved after terminal conflict")
	}
	if got := cq.Pending(); got != 0 {
		t.Fatalf("queue pending count = %d, want 0 after terminal conflict", got)
	}
}

func TestCommitQueueRecoverPendingSkipsLegacyOverwriteWithoutBaseRev(t *testing.T) {
	var calls int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusOK)
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

	if err := shadow.WriteFull("/legacy.txt", []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.Put("/legacy.txt", 4, PendingOverwrite); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(client.New(ts.URL, ""), shadow, pending, nil, 1, 8)
	cq.RecoverPending()
	cq.DrainAll()

	if calls != 0 {
		t.Fatalf("legacy overwrite should not be auto-committed, got %d requests", calls)
	}
	if !pending.HasPending("/legacy.txt") {
		t.Fatal("legacy pending entry should remain for explicit recovery")
	}
	if !shadow.Has("/legacy.txt") {
		t.Fatal("legacy shadow should remain after skipped recovery")
	}
}
