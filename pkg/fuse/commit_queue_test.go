package fuse

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mem9-ai/dat9/pkg/client"
)

func TestCommitQueueConditionalCommitSuccess(t *testing.T) {
	var gotExpected string
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

	// After the initial 409, auto-resolve attempts a Stat (HEAD) which also
	// hits the blanket 409 handler, causing it to fall back to terminal failure.
	// We expect 2 calls: the original upload + the Stat attempt.
	if calls != 2 {
		t.Fatalf("expected 2 calls (upload + auto-resolve stat), got %d", calls)
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

// --- Auto-resolve tests (LWW MVP) ---

// Test axis 1: 409 → fetch → LWW re-upload succeeds.
func TestCommitQueueAutoResolveLWW(t *testing.T) {
	var uploadCalls, statCalls, readCalls int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			// Stat: return current revision.
			statCalls++
			w.Header().Set("X-Dat9-Revision", "10")
			w.Header().Set("Content-Length", "12")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			// Read: return different content (triggers LWW).
			readCalls++
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write([]byte("server-data!"))
		default:
			// PUT/POST: upload path.
			uploadCalls++
			rev := r.Header.Get("X-Dat9-Expected-Revision")
			if rev == "5" {
				// First upload: 409 conflict.
				http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
				return
			}
			if rev == "10" {
				// LWW re-upload with new revision: success.
				w.WriteHeader(http.StatusOK)
				return
			}
			http.Error(w, `{"error":"unexpected revision"}`, http.StatusBadRequest)
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

	if err := shadow.WriteFull("/lww.txt", []byte("local-data!!"), 5); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/lww.txt", 12, PendingOverwrite, 5); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(client.New(ts.URL, ""), shadow, pending, nil, 1, 8)
	if err := cq.Enqueue(&CommitEntry{
		Path:    "/lww.txt",
		BaseRev: 5,
		Size:    12,
		Kind:    PendingOverwrite,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if uploadCalls != 2 {
		t.Fatalf("upload calls = %d, want 2 (initial 409 + LWW retry)", uploadCalls)
	}
	if statCalls != 1 {
		t.Fatalf("stat calls = %d, want 1", statCalls)
	}
	if readCalls != 1 {
		t.Fatalf("read calls = %d, want 1", readCalls)
	}
	// Success: shadow and pending should be cleaned up.
	if pending.HasPending("/lww.txt") {
		t.Fatal("pending entry should be removed after successful LWW")
	}
	if shadow.Has("/lww.txt") {
		t.Fatal("shadow should be removed after successful LWW")
	}
}

// Test axis 2: 409 → fetch → content matches → idempotent success.
func TestCommitQueueAutoResolveIdempotent(t *testing.T) {
	var uploadCalls int
	sameContent := []byte("identical!")
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("X-Dat9-Revision", "8")
			w.Header().Set("Content-Length", "10")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(sameContent)
		default:
			// First upload: 409.
			uploadCalls++
			http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
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

	if err := shadow.WriteFull("/idem.txt", sameContent, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/idem.txt", int64(len(sameContent)), PendingOverwrite, 5); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(client.New(ts.URL, ""), shadow, pending, nil, 1, 8)
	if err := cq.Enqueue(&CommitEntry{
		Path:    "/idem.txt",
		BaseRev: 5,
		Size:    int64(len(sameContent)),
		Kind:    PendingOverwrite,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	// Only the initial upload should have been attempted (the 409 that triggered auto-resolve).
	// No LWW re-upload because content matched.
	if uploadCalls != 1 {
		t.Fatalf("upload calls = %d, want 1 (initial 409 only, no LWW re-upload)", uploadCalls)
	}
	// Idempotent: should be cleaned up without a second upload.
	if pending.HasPending("/idem.txt") {
		t.Fatal("pending entry should be removed after idempotent resolve")
	}
	if shadow.Has("/idem.txt") {
		t.Fatal("shadow should be removed after idempotent resolve")
	}
}

// Test axis 3: 409 → fetch → LWW re-upload → second 409 → PendingConflict fallback.
func TestCommitQueueAutoResolveLWWSecond409Fallback(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("X-Dat9-Revision", "10")
			w.Header().Set("Content-Length", "12")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write([]byte("server-data!"))
		default:
			// ALL uploads return 409 (simulates rapid concurrent writes).
			http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
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

	if err := shadow.WriteFull("/double409.txt", []byte("local-data!!"), 5); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/double409.txt", 12, PendingOverwrite, 5); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(client.New(ts.URL, ""), shadow, pending, nil, 1, 8)
	if err := cq.Enqueue(&CommitEntry{
		Path:    "/double409.txt",
		BaseRev: 5,
		Size:    12,
		Kind:    PendingOverwrite,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	// Should fall back to PendingConflict — no regression from current behavior.
	if !pending.HasPending("/double409.txt") {
		t.Fatal("pending entry should be preserved after double-409 fallback")
	}
	meta, ok := pending.GetMeta("/double409.txt")
	if !ok || meta.Kind != PendingConflict {
		t.Fatalf("pending entry should be PendingConflict, got kind=%v ok=%v", meta.Kind, ok)
	}
	if !shadow.Has("/double409.txt") {
		t.Fatal("shadow should be preserved after double-409 fallback")
	}
}

// Test axis 4: Non-conflict errors (500) still follow existing retry + terminal failure.
func TestCommitQueueNonConflictErrorUnchanged(t *testing.T) {
	var calls int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.Error(w, `{"error":"internal"}`, http.StatusInternalServerError)
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

	if err := shadow.WriteFull("/500.txt", []byte("data"), 5); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/500.txt", 4, PendingOverwrite, 5); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(client.New(ts.URL, ""), shadow, pending, nil, 1, 8)
	if err := cq.Enqueue(&CommitEntry{
		Path:    "/500.txt",
		BaseRev: 5,
		Size:    4,
		Kind:    PendingOverwrite,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	// 500 errors should retry (maxRetries=5), not trigger auto-resolve.
	if calls != 5 {
		t.Fatalf("500 error should retry 5 times, got %d calls", calls)
	}
	// Terminal failure: PendingConflict marker.
	if !pending.HasPending("/500.txt") {
		t.Fatal("pending entry should be preserved after 500 terminal failure")
	}
	meta, ok := pending.GetMeta("/500.txt")
	if !ok || meta.Kind != PendingConflict {
		t.Fatalf("pending entry should be PendingConflict, got kind=%v ok=%v", meta.Kind, ok)
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

// --- ShadowSpill tests ---

// TestCommitQueueShadowSpillUpload verifies that ShadowSpill entries are
// uploaded via streaming (uploadFromShadow) rather than ReadAll, and that
// the server receives the correct data and expected revision.
func TestCommitQueueShadowSpillUpload(t *testing.T) {
	var gotExpected string
	var gotBody []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotExpected = r.Header.Get("X-Dat9-Expected-Revision")
		body, _ := io.ReadAll(r.Body)
		gotBody = body
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

	// Write data to shadow file (simulates what Write() + ShadowSpill does).
	data := bytes.Repeat([]byte("shadowspill-data-"), 100) // ~1700 bytes
	if err := shadow.WriteFull("/big.bin", data, 12); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/big.bin", int64(len(data)), PendingOverwrite, 12); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(client.New(ts.URL, ""), shadow, pending, nil, 1, 8)
	if err := cq.Enqueue(&CommitEntry{
		Path:        "/big.bin",
		BaseRev:     12,
		Size:        int64(len(data)),
		Kind:        PendingOverwrite,
		ShadowSpill: true,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if gotExpected != "12" {
		t.Fatalf("expected revision header = %q, want 12", gotExpected)
	}
	if !bytes.Equal(gotBody, data) {
		t.Fatalf("server received %d bytes, want %d", len(gotBody), len(data))
	}
	if pending.HasPending("/big.bin") {
		t.Fatal("pending entry should be removed after successful ShadowSpill commit")
	}
	if shadow.Has("/big.bin") {
		t.Fatal("shadow should be removed after successful ShadowSpill commit")
	}
}

// TestCommitQueueShadowSpillConflictTerminal verifies that ShadowSpill entries
// skip auto-resolve (which would OOM) and go straight to terminal failure.
func TestCommitQueueShadowSpillConflictTerminal(t *testing.T) {
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

	if err := shadow.WriteFull("/big-conflict.bin", []byte("data"), 5); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/big-conflict.bin", 4, PendingOverwrite, 5); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(client.New(ts.URL, ""), shadow, pending, nil, 1, 8)
	if err := cq.Enqueue(&CommitEntry{
		Path:        "/big-conflict.bin",
		BaseRev:     5,
		Size:        4,
		Kind:        PendingOverwrite,
		ShadowSpill: true,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	// ShadowSpill: 409 → skip auto-resolve → terminal failure immediately.
	// Only 1 call: the initial upload (no Stat/Read for auto-resolve).
	if calls != 1 {
		t.Fatalf("expected 1 call (upload only, no auto-resolve), got %d", calls)
	}
	if !pending.HasPending("/big-conflict.bin") {
		t.Fatal("pending entry should be preserved after ShadowSpill terminal conflict")
	}
	meta, ok := pending.GetMeta("/big-conflict.bin")
	if !ok || meta.Kind != PendingConflict {
		t.Fatalf("pending entry should be PendingConflict, got kind=%v ok=%v", meta.Kind, ok)
	}
	if !shadow.Has("/big-conflict.bin") {
		t.Fatal("shadow should be preserved after ShadowSpill terminal conflict")
	}
}

// TestCommitQueueRecoverPendingShadowSpill verifies that crash recovery
// preserves the ShadowSpill flag so recovered entries use streaming upload
// (not ReadAll which would OOM for large files).
func TestCommitQueueRecoverPendingShadowSpill(t *testing.T) {
	var gotBody []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		gotBody = body
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	shadowDir := t.TempDir()
	pendingDir := t.TempDir()

	// Phase 1: create pending entry with ShadowSpill=true, then "crash" (close everything).
	shadow1, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatal(err)
	}
	pending1, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("recover-"), 200)
	if err := shadow1.WriteFull("/recover.bin", data, 8); err != nil {
		t.Fatal(err)
	}
	if _, err := pending1.PutShadowSpill("/recover.bin", int64(len(data)), PendingOverwrite, 8); err != nil {
		t.Fatal(err)
	}
	shadow1.Close()

	// Phase 2: "restart" — reopen stores and recover.
	shadow2, err := NewShadowStore(shadowDir)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow2.Close()
	if err := shadow2.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}
	pending2, err := NewPendingIndex(pendingDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := pending2.RecoverFromDisk(); err != nil {
		t.Fatal(err)
	}

	// Verify the recovered meta has ShadowSpill=true.
	meta, ok := pending2.GetMeta("/recover.bin")
	if !ok {
		t.Fatal("pending entry should survive restart")
	}
	if !meta.ShadowSpill {
		t.Fatal("ShadowSpill flag must be persisted and recovered from disk")
	}

	// RecoverPending should reconstruct CommitEntry with ShadowSpill=true,
	// causing uploadEntry to use streaming (uploadFromShadow) not ReadAll.
	cq := NewCommitQueue(client.New(ts.URL, ""), shadow2, pending2, nil, 1, 8)
	cq.RecoverPending()
	cq.DrainAll()

	// Verify data arrived correctly at the server (streaming upload worked).
	if !bytes.Equal(gotBody, data) {
		t.Fatalf("server received %d bytes, want %d", len(gotBody), len(data))
	}
	if pending2.HasPending("/recover.bin") {
		t.Fatal("pending entry should be removed after successful recovery upload")
	}
	if shadow2.Has("/recover.bin") {
		t.Fatal("shadow should be removed after successful recovery upload")
	}
}
