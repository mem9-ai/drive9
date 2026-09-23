package fuse

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// modeAwareServer serves one path with a revision, a body, and a permission
// mode, and supports the read-only probes the duplicate-payload proof uses
// (HEAD for Stat, range GET for the body).
type modeAwareServer struct {
	mu       sync.Mutex
	revision int64
	body     []byte
	mode     uint32
	hasMode  bool
	// bumps counts every HEAD so a test can change the revision mid-proof.
	headCalls int
	onHead    func(s *modeAwareServer)
	// mutations records accepted non-read requests (a chmod, for instance).
	mutations []string
}

func newModeAwareServer(t *testing.T, path string, revision int64, body []byte, mode uint32, hasMode bool) (*modeAwareServer, *httptest.Server) {
	t.Helper()
	s := &modeAwareServer{revision: revision, body: append([]byte(nil), body...), mode: mode, hasMode: hasMode}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/fs"+path {
			http.NotFound(w, r)
			return
		}
		switch r.Method {
		case http.MethodHead:
			s.mu.Lock()
			s.headCalls++
			if s.onHead != nil {
				s.onHead(s)
			}
			rev, size, mode, hasMode := s.revision, len(s.body), s.mode, s.hasMode
			s.mu.Unlock()
			w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", fmt.Sprintf("%d", rev))
			if hasMode {
				w.Header().Set("X-Dat9-Mode", fmt.Sprintf("%d", mode))
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			s.mu.Lock()
			body := append([]byte(nil), s.body...)
			s.mu.Unlock()
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
			_, _ = w.Write(body)
		default:
			// Accept a chmod (or any other mutation) and record it, so a test
			// can assert that the proof did not apply a stale mode over a
			// newer writer's permissions.
			s.mu.Lock()
			s.mutations = append(s.mutations, r.Method)
			s.mu.Unlock()
			w.WriteHeader(http.StatusOK)
		}
	}))
	return s, ts
}

// mutationsSeen reports how many mutating requests the server accepted.
func (s *modeAwareServer) mutationsSeen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.mutations...)
}

func newStaleEntryFixture(t *testing.T, path string, payload []byte, mode uint32, hasMode bool) (*CommitQueue, *ShadowStore, *PendingIndex, *CommitEntry) {
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
	if err := shadow.WriteFull(path, payload, 0); err != nil {
		t.Fatal(err)
	}
	gen, err := pending.PutWithBaseRev(path, int64(len(payload)), PendingNew, 0)
	if err != nil {
		t.Fatal(err)
	}
	return nil, shadow, pending, &CommitEntry{
		Path: path, BaseRev: 0, Size: int64(len(payload)), Kind: PendingNew,
		Mode: mode, HasMode: hasMode,
		ShadowGen: shadow.ActiveGeneration(path), PendingIndexGen: gen,
		DurableWatermarkRev: 1,
	}
}

// A pending mode must be part of the proof: identical bytes with different
// durable permissions are another writer's outcome, and consuming this entry
// would chmod the newer file back to the stale mode. 0600 needs applying (a
// PendingNew at the default 0644 does not, so it could not exercise this).
func TestCommitQueueStalePayloadModeMismatchStaysTerminal(t *testing.T) {
	const path = "/mode-mismatch"
	payload := []byte("identical-bytes-different-mode")
	server, ts := newModeAwareServer(t, path, 1, payload, 0o644, true)
	defer ts.Close()

	_, shadow, pending, entry := newStaleEntryFixture(t, path, payload, 0o600, true)
	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	defer cq.DrainAll()

	if err := cq.CommitNow(context.Background(), entry); err == nil {
		t.Fatal("mode mismatch must not resolve as an idempotent success")
	}
	// The stale entry must not have reached the server at all: applying its
	// 0600 would overwrite the newer writer's 0644 on the durable file.
	if got := server.mutationsSeen(); len(got) != 0 {
		t.Fatalf("stale entry mutated the durable file: %v", got)
	}
	// CommitNow returns the rejection; its callers convert it into the
	// terminal outcome. Do the same to assert what the user observes.
	cq.onCommitTerminalFailure(entry, errCommitPayloadStale)
	if meta, ok := pending.GetMeta(path); !ok || meta.Kind != PendingConflict {
		t.Fatalf("pending kind=%v ok=%v, want a preserved conflict", meta.Kind, ok)
	}
}

// Matching bytes AND mode is a true duplicate: it resolves without issuing a
// redundant chmod.
func TestCommitQueueStalePayloadMatchingModeResolvesIdempotent(t *testing.T) {
	const path = "/mode-match"
	payload := []byte("identical-bytes-identical-mode")
	_, ts := newModeAwareServer(t, path, 1, payload, 0o600, true)
	defer ts.Close()

	_, shadow, pending, entry := newStaleEntryFixture(t, path, payload, 0o600, true)
	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	defer cq.DrainAll()

	if err := cq.CommitNow(context.Background(), entry); err != nil {
		t.Fatalf("matching duplicate err=%v, want idempotent success", err)
	}
	if _, ok := pending.GetMeta(path); ok {
		t.Fatal("pending entry survived the idempotent resolution")
	}
}

// A same-size write that lands between the Stat and the body read would let
// this entry record the older revision for newer bytes: the proof must fail.
func TestCommitQueueStalePayloadRevisionMovedDuringReadStaysTerminal(t *testing.T) {
	const path = "/rev-moved"
	payload := []byte("same-size-payload-here")
	server, ts := newModeAwareServer(t, path, 1, payload, 0o644, true)
	defer ts.Close()
	// The proof's confirmation Stat sees a newer revision than the read did.
	server.onHead = func(s *modeAwareServer) {
		if s.headCalls == 2 {
			s.revision = 2
		}
	}

	_, shadow, pending, entry := newStaleEntryFixture(t, path, payload, 0, false)
	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	defer cq.DrainAll()

	if err := cq.CommitNow(context.Background(), entry); err == nil {
		t.Fatal("a revision that moved during the proof must not resolve as idempotent")
	}
	if _, ok := pending.GetMeta(path); !ok {
		t.Fatal("pending data must be preserved when the proof fails")
	}
}

// The compared bytes must belong to the entry's own staging generation: a
// bound payload from an earlier attempt cannot speak for a superseded entry.
func TestCommitQueueStalePayloadReplacedShadowStaysTerminal(t *testing.T) {
	const path = "/replaced-gen"
	payload := []byte("first-generation-bytes")
	server, ts := newModeAwareServer(t, path, 1, payload, 0o644, true)
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
	staleGen := shadow.ActiveGeneration(path)
	gen, err := pending.PutWithBaseRev(path, int64(len(payload)), PendingNew, 0)
	if err != nil {
		t.Fatal(err)
	}
	// A newer writer replaces the staged content, then the entry's payload is
	// bound (as a retry would leave it) from the still-readable old bytes.
	entry := &CommitEntry{
		Path: path, BaseRev: 0, Size: int64(len(payload)), Kind: PendingNew,
		ShadowGen: staleGen, PendingIndexGen: gen, DurableWatermarkRev: 1,
	}
	entry.bindPayload(payload)
	if err := shadow.WriteFull(path, []byte("newer-generation-bytes"), 0); err != nil {
		t.Fatal(err)
	}
	_ = server

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	defer cq.DrainAll()

	if err := cq.CommitNow(context.Background(), entry); err == nil {
		t.Fatal("a superseded staging generation must not resolve as idempotent")
	}
	if _, ok := pending.GetMeta(path); !ok {
		t.Fatal("newer pending data must be preserved")
	}
}

// The resolver must not acknowledge a payload the server holds different
// bytes for, even when the sizes match.
func TestCommitQueueStalePayloadSameSizeDifferentBytesStaysTerminal(t *testing.T) {
	const path = "/same-size-diff"
	durable := []byte("AAAAAAAAAAAAAAAAAAAA")
	staged := []byte("BBBBBBBBBBBBBBBBBBBB")
	_, ts := newModeAwareServer(t, path, 1, durable, 0o644, true)
	defer ts.Close()

	_, shadow, pending, entry := newStaleEntryFixture(t, path, staged, 0, false)
	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	defer cq.DrainAll()

	if err := cq.CommitNow(context.Background(), entry); err == nil {
		t.Fatal("same-size different bytes must not resolve as idempotent")
	}
	if bytes.Equal(durable, staged) {
		t.Fatal("fixture bug: payloads must differ")
	}
	if _, ok := pending.GetMeta(path); !ok {
		t.Fatal("pending data must be preserved")
	}
}
