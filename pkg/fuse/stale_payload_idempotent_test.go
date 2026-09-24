package fuse

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// A queued entry can be superseded by a sibling upload of the SAME bytes: the
// Release direct-PUT path commits the file first and advances the path's
// durable watermark, after which the queued entry fails the base-revision
// preflight. That is a redundant upload, not a conflict, and must be resolved
// as already-landed instead of marking a fully durable file PendingConflict.
func TestCommitQueueStalePayloadIdenticalToDurableResolvesIdempotent(t *testing.T) {
	const path = "/dup"
	payload := []byte("same-bytes-already-durable")
	server, ts := newCASFileServer(t, path, 1, payload)
	defer ts.Close()

	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.WriteFull(path, payload, 0); err != nil {
		t.Fatal(err)
	}
	gen, err := pending.PutWithBaseRev(path, int64(len(payload)), PendingNew, 0)
	if err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	defer cq.DrainAll()
	entry := &CommitEntry{
		Path: path, BaseRev: 0, Size: int64(len(payload)), Kind: PendingNew,
		ShadowGen: shadow.ActiveGeneration(path), PendingIndexGen: gen,
		// The watermark advanced past the staged base: the preflight will
		// reject this entry before any PUT.
		DurableWatermarkRev: 1,
	}

	if err := cq.CommitNow(context.Background(), entry); err != nil {
		t.Fatalf("duplicate payload commit err=%v, want idempotent success", err)
	}

	if rev, body, puts := server.snapshot(); rev != 1 || !bytes.Equal(body, payload) || len(puts) != 0 {
		t.Fatalf("remote mutated by the rejected duplicate: rev=%d body=%q puts=%d", rev, body, len(puts))
	}
	if _, ok := pending.GetMeta(path); ok {
		t.Fatal("pending entry survived the idempotent resolution")
	}
	if shadow.Has(path) {
		t.Fatal("shadow staging survived the idempotent resolution")
	}
	if count, _, _ := pending.ConflictSummary(); count != 0 {
		t.Fatalf("conflicts=%d, want 0 for a byte-identical duplicate", count)
	}
}

// The same preflight rejection with DIFFERENT bytes is a real divergence and
// must stay terminal: the fix must not silently accept a payload the server
// never stored.
func TestCommitQueueStalePayloadDifferentFromDurableStaysTerminal(t *testing.T) {
	const path = "/diverged"
	durable := []byte("durable-image")
	staged := []byte("different-staged-bytes")
	server, ts := newCASFileServer(t, path, 1, durable)
	defer ts.Close()

	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.WriteFull(path, staged, 0); err != nil {
		t.Fatal(err)
	}
	gen, err := pending.PutWithBaseRev(path, int64(len(staged)), PendingNew, 0)
	if err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	defer cq.DrainAll()
	entry := &CommitEntry{
		Path: path, BaseRev: 0, Size: int64(len(staged)), Kind: PendingNew,
		ShadowGen: shadow.ActiveGeneration(path), PendingIndexGen: gen,
		DurableWatermarkRev: 1,
	}

	if err := cq.CommitNow(context.Background(), entry); !errors.Is(err, errCommitPayloadStale) {
		t.Fatalf("diverged payload err=%v, want errCommitPayloadStale", err)
	}
	// Callers of the synchronous path convert a stale rejection into the
	// terminal outcome; do the same here to assert what the user observes.
	cq.onCommitTerminalFailure(entry, errCommitPayloadStale)

	if rev, body, puts := server.snapshot(); rev != 1 || !bytes.Equal(body, durable) || len(puts) != 0 {
		t.Fatalf("terminal path must not upload: rev=%d body=%q puts=%d", rev, body, len(puts))
	}
	meta, ok := pending.GetMeta(path)
	if !ok {
		t.Fatal("local pending data must be preserved for recovery")
	}
	if meta.Kind != PendingConflict {
		t.Fatalf("pending kind=%v, want PendingConflict", meta.Kind)
	}
	if shadow.ActiveGeneration(path) == 0 {
		t.Fatal("shadow staging must be preserved for a real divergence")
	}
}

// Layer mounts must not use the duplicate proof: the remote read would target
// the base FS namespace, where the layer entry's bytes legitimately may not
// appear. The rejection stays terminal, as in tryAutoResolveConflict.
func TestCommitQueueStalePayloadLayerMountStaysTerminal(t *testing.T) {
	const path = "/layered"
	payload := []byte("layer-staged-bytes")
	server, ts := newCASFileServer(t, path, 1, payload)
	defer ts.Close()

	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.WriteFull(path, payload, 0); err != nil {
		t.Fatal(err)
	}
	gen, err := pending.PutWithBaseRev(path, int64(len(payload)), PendingNew, 0)
	if err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	defer cq.DrainAll()
	cq.SetLayerRef("layer-1")
	entry := &CommitEntry{
		Path: path, BaseRev: 0, Size: int64(len(payload)), Kind: PendingNew,
		ShadowGen: shadow.ActiveGeneration(path), PendingIndexGen: gen,
		DurableWatermarkRev: 1,
	}

	if err := cq.CommitNow(context.Background(), entry); err == nil {
		t.Fatal("layer mount must keep the stale rejection terminal")
	}
	if rev, body, puts := server.snapshot(); rev != 1 || !bytes.Equal(body, payload) || len(puts) != 0 {
		t.Fatalf("layer path must not upload through the base namespace: rev=%d body=%q puts=%d", rev, body, len(puts))
	}
}

// A remote read failure must fail closed: without proof of equality the entry
// keeps its terminal outcome rather than being acknowledged.
func TestCommitQueueStalePayloadFailsClosedWithoutRemoteProof(t *testing.T) {
	const path = "/unreachable"
	payload := []byte("payload")

	shadow, err := NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.WriteFull(path, payload, 0); err != nil {
		t.Fatal(err)
	}
	gen, err := pending.PutWithBaseRev(path, int64(len(payload)), PendingNew, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Closed listener: Stat/Read cannot complete.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	closedURL := ts.URL
	ts.Close()
	cq := NewCommitQueue(newTestClient(closedURL), shadow, pending, nil, 1, 8)
	defer cq.DrainAll()
	entry := &CommitEntry{
		Path: path, BaseRev: 0, Size: int64(len(payload)), Kind: PendingNew,
		ShadowGen: shadow.ActiveGeneration(path), PendingIndexGen: gen,
		DurableWatermarkRev: 1,
	}

	if err := cq.CommitNow(context.Background(), entry); !errors.Is(err, errCommitPayloadStale) {
		t.Fatalf("unprovable duplicate err=%v, want errCommitPayloadStale (fail closed)", err)
	}
	if _, ok := pending.GetMeta(path); !ok {
		t.Fatal("pending data lost when the remote proof could not be obtained")
	}
	if shadow.ActiveGeneration(path) == 0 {
		t.Fatal("shadow staging lost when the remote proof could not be obtained")
	}
}
