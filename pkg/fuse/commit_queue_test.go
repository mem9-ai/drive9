package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCommitQueueConditionalCommitSuccess(t *testing.T) {
	var gotExpected string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotExpected = r.Header.Get("X-Dat9-Expected-Revision")
		// Return committed revision in JSON (direct PUT response format).
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","revision":8}`))
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

	var successPath string
	var successRev int64

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	cq.OnSuccess = func(entry *CommitEntry, committedRev int64) {
		successPath = entry.Path
		successRev = committedRev
	}
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
	if successPath != "/ok.txt" {
		t.Fatalf("OnSuccess path = %q, want /ok.txt", successPath)
	}
	if successRev != 8 {
		t.Fatalf("OnSuccess committedRev = %d, want 8", successRev)
	}
	if pending.HasPending("/ok.txt") {
		t.Fatal("pending entry should be removed after successful commit")
	}
	if shadow.Has("/ok.txt") {
		t.Fatal("shadow should be removed after successful commit")
	}
}

func TestCommitQueueBeginInFlightSerializesSamePath(t *testing.T) {
	cq := &CommitQueue{inFlight: make(map[string]*CommitEntry)}
	first := &CommitEntry{Path: "/same.txt"}
	second := &CommitEntry{Path: "/same.txt"}
	other := &CommitEntry{Path: "/other.txt"}

	if !cq.beginInFlight(first) {
		t.Fatal("begin first in-flight returned false")
	}
	if !cq.beginInFlight(other) {
		t.Fatal("begin other-path in-flight returned false")
	}
	cq.endInFlight(other)

	done := make(chan struct{})
	go func() {
		if !cq.beginInFlight(second) {
			t.Errorf("begin second in-flight returned false")
		}
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("second same-path entry became in-flight before first ended")
	case <-time.After(25 * time.Millisecond):
	}

	cq.endInFlight(first)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("second same-path entry did not become in-flight after first ended")
	}
	cq.endInFlight(second)
}

func TestCommitQueueBeginInFlightPreservesSamePathFIFO(t *testing.T) {
	first := &CommitEntry{Path: "/same.txt"}
	second := &CommitEntry{Path: "/same.txt"}
	cq := &CommitQueue{
		queue:        []*CommitEntry{first, second},
		queuedByPath: make(map[string]map[*CommitEntry]struct{}),
		inFlight:     make(map[string]*CommitEntry),
	}
	cq.rebuildQueuedIndexLocked()

	secondDone := make(chan struct{})
	go func() {
		if !cq.beginInFlight(second) {
			t.Errorf("begin second in-flight returned false")
		}
		close(secondDone)
	}()

	select {
	case <-secondDone:
		t.Fatal("second same-path entry became in-flight before older queued entry")
	case <-time.After(25 * time.Millisecond):
	}

	if !cq.beginInFlight(first) {
		t.Fatal("begin first in-flight returned false")
	}

	select {
	case <-secondDone:
		t.Fatal("second same-path entry became in-flight while first was in-flight")
	case <-time.After(25 * time.Millisecond):
	}

	cq.endInFlight(first)
	cq.removeFromQueue(first)

	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("second same-path entry did not become in-flight after older entry completed")
	}
	cq.endInFlight(second)
	cq.removeFromQueue(second)
}

func TestCommitQueueCommitNowHoldsPathLockThroughSuccessCleanup(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","revision":9}`))
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	if err := shadow.WriteFull("/locked.txt", []byte("data"), 8); err != nil {
		t.Fatal(err)
	}
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/locked.txt", 4, PendingOverwrite, 8); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	locked := false
	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	defer cq.DrainAll()
	cq.PathLock = func(path string) func() {
		mu.Lock()
		if path != "/locked.txt" {
			t.Errorf("PathLock path = %q, want /locked.txt", path)
		}
		locked = true
		mu.Unlock()
		return func() {
			mu.Lock()
			locked = false
			mu.Unlock()
		}
	}
	cq.OnSuccess = func(entry *CommitEntry, committedRev int64) {
		mu.Lock()
		defer mu.Unlock()
		if !locked {
			t.Error("PathLock was not held during OnSuccess cleanup")
		}
	}

	if err := cq.CommitNow(context.Background(), &CommitEntry{
		Path:    "/locked.txt",
		BaseRev: 8,
		Size:    4,
		Kind:    PendingOverwrite,
	}); err != nil {
		t.Fatalf("CommitNow: %v", err)
	}
}

func TestCommitQueueAppliesModeAfterUpload(t *testing.T) {
	var putCalls atomic.Int32
	var chmodCalls atomic.Int32
	var chmodBody []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			putCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ok","revision":9}`))
		case r.Method == http.MethodPost && r.URL.RawQuery == "chmod":
			chmodCalls.Add(1)
			chmodBody, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
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
	if err := shadow.WriteFull("/exec.sh", []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRevAndMode("/exec.sh", 4, PendingNew, 0, 0o755, true); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	if err := cq.Enqueue(&CommitEntry{
		Path:    "/exec.sh",
		Size:    4,
		Kind:    PendingNew,
		Mode:    0o755,
		HasMode: true,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if got := putCalls.Load(); got != 1 {
		t.Fatalf("PUT calls = %d, want 1", got)
	}
	if got := chmodCalls.Load(); got != 1 {
		t.Fatalf("chmod calls = %d, want 1", got)
	}
	if !bytes.Contains(chmodBody, []byte("493")) {
		t.Fatalf("chmod body = %s, want decimal mode 493", chmodBody)
	}
}

func TestCommitQueueSkipsDefaultModeForPendingNew(t *testing.T) {
	var putCalls atomic.Int32
	var chmodCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			putCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ok","revision":9}`))
		case r.Method == http.MethodPost && r.URL.RawQuery == "chmod":
			chmodCalls.Add(1)
			http.Error(w, "unexpected chmod", http.StatusInternalServerError)
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
	if err := shadow.WriteFull("/plain.txt", []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRevAndMode("/plain.txt", 4, PendingNew, 0, defaultRegularFileMode, true); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	if err := cq.Enqueue(&CommitEntry{
		Path:    "/plain.txt",
		Size:    4,
		Kind:    PendingNew,
		Mode:    defaultRegularFileMode,
		HasMode: true,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if got := putCalls.Load(); got != 1 {
		t.Fatalf("PUT calls = %d, want 1", got)
	}
	if got := chmodCalls.Load(); got != 0 {
		t.Fatalf("chmod calls = %d, want 0", got)
	}
	if pending.HasPending("/plain.txt") {
		t.Fatal("pending entry should be removed after successful default-mode commit")
	}
	if shadow.Has("/plain.txt") {
		t.Fatal("shadow should be removed after successful default-mode commit")
	}
}

func TestCommitQueueRetriesPostUploadChmodNotFound(t *testing.T) {
	var putCalls atomic.Int32
	var chmodCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			putCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ok","revision":9}`))
		case r.Method == http.MethodPost && r.URL.RawQuery == "chmod":
			if chmodCalls.Add(1) == 1 {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
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
	if err := shadow.WriteFull("/exec-retry.sh", []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRevAndMode("/exec-retry.sh", 4, PendingNew, 0, 0o755, true); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	if err := cq.Enqueue(&CommitEntry{
		Path:    "/exec-retry.sh",
		Size:    4,
		Kind:    PendingNew,
		Mode:    0o755,
		HasMode: true,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	if got := putCalls.Load(); got != 1 {
		t.Fatalf("PUT calls = %d, want 1", got)
	}
	if got := chmodCalls.Load(); got != 2 {
		t.Fatalf("chmod calls = %d, want 2", got)
	}
	if pending.HasPending("/exec-retry.sh") {
		t.Fatal("pending entry should be removed after chmod retry succeeds")
	}
	if shadow.Has("/exec-retry.sh") {
		t.Fatal("shadow should be removed after chmod retry succeeds")
	}
}

func TestCommitQueueChmodFailureKeepsPendingState(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ok","revision":9}`))
		case r.Method == http.MethodPost && r.URL.RawQuery == "chmod":
			http.Error(w, "chmod failed", http.StatusInternalServerError)
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
	if err := shadow.WriteFull("/exec-fail.sh", []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRevAndMode("/exec-fail.sh", 4, PendingNew, 0, 0o755, true); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	cq.DrainAll()
	err = cq.CommitNow(context.Background(), &CommitEntry{
		Path:    "/exec-fail.sh",
		Size:    4,
		Kind:    PendingNew,
		Mode:    0o755,
		HasMode: true,
	})
	if err == nil {
		t.Fatal("CommitNow should fail when chmod fails")
	}
	if !pending.HasPending("/exec-fail.sh") {
		t.Fatal("pending entry should be retained when chmod fails")
	}
	if !shadow.Has("/exec-fail.sh") {
		t.Fatal("shadow should be retained when chmod fails")
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

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
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

func TestCommitQueueCancelPathDoesNotPoisonFutureSamePath(t *testing.T) {
	const path = "/repo/.git/config.lock"
	newData := []byte("[remote \"origin\"]\n\turl = https://github.com/mem9-ai/drive9.git\n")

	var puts [][]byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusOK)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read PUT body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		puts = append(puts, body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","revision":9}`))
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

	oldEntry := &CommitEntry{Path: path, Size: 3, Kind: PendingNew}
	cq := &CommitQueue{
		client:     newTestClient(ts.URL),
		shadows:    shadow,
		index:      pending,
		inFlight:   make(map[string]*CommitEntry),
		maxPending: 8,
		workCh:     make(chan *CommitEntry, 8),
	}
	cq.queue = append(cq.queue, oldEntry)
	cq.workCh <- oldEntry

	cq.CancelPath(path)
	if !cq.isEntryCanceled(oldEntry) {
		t.Fatal("old queued entry should be canceled")
	}

	if err := shadow.WriteFull(path, newData, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev(path, int64(len(newData)), PendingNew, 0); err != nil {
		t.Fatal(err)
	}
	newEntry := &CommitEntry{Path: path, Size: int64(len(newData)), Kind: PendingNew}
	if cq.isEntryCanceled(newEntry) {
		t.Fatal("new entry for same path must not inherit old cancellation")
	}
	if err := cq.Enqueue(newEntry); err != nil {
		t.Fatal(err)
	}

	close(cq.workCh)
	cq.wg.Add(1)
	go cq.worker()
	cq.wg.Wait()

	if len(puts) != 1 {
		t.Fatalf("PUT count = %d, want 1", len(puts))
	}
	if !bytes.Equal(puts[0], newData) {
		t.Fatalf("PUT body = %q, want %q", puts[0], newData)
	}
	if pending.HasPending(path) {
		t.Fatal("pending entry should be removed after new entry commits")
	}
	if shadow.Has(path) {
		t.Fatal("shadow should be removed after new entry commits")
	}
}

func TestCommitQueueCancelPathCancelsRetryBackoff(t *testing.T) {
	const path = "/retry.txt"
	var calls atomic.Int32
	firstAttemptDone := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			call := calls.Add(1)
			http.Error(w, `{"error":"temporary"}`, http.StatusServiceUnavailable)
			if call == 1 {
				close(firstAttemptDone)
			}
			return
		}
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
	if err := shadow.WriteFull(path, []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev(path, 4, PendingNew, 0); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	defer cq.DrainAll()
	if err := cq.Enqueue(&CommitEntry{Path: path, Size: 4, Kind: PendingNew}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstAttemptDone:
	case <-time.After(2 * time.Second):
		t.Fatal("first upload attempt did not finish")
	}

	start := time.Now()
	cq.CancelPath(path)
	done := make(chan struct{})
	go func() {
		cq.WaitPath(path)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(150 * time.Millisecond):
		t.Fatal("WaitPath did not unblock during retry backoff cancellation")
	}
	if elapsed := time.Since(start); elapsed >= 150*time.Millisecond {
		t.Fatalf("cancel during backoff took %s, want <150ms", elapsed)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("upload attempts after cancellation = %d, want 1", got)
	}
}

func TestCommitQueueQueuedPathIndexTracksCancelAndRemove(t *testing.T) {
	path := "/repo/file.txt"
	otherPath := "/repo/other.txt"
	e1 := &CommitEntry{Path: path}
	e2 := &CommitEntry{Path: path}
	other := &CommitEntry{Path: otherPath}
	cq := &CommitQueue{
		queue:        []*CommitEntry{e1, other, e2},
		queuedByPath: make(map[string]map[*CommitEntry]struct{}),
		inFlight:     make(map[string]*CommitEntry),
	}
	cq.addQueuedLocked(e1)
	cq.addQueuedLocked(other)
	cq.addQueuedLocked(e2)

	if !cq.HasPath(path) {
		t.Fatal("HasPath should find queued entries through index")
	}
	cq.CancelPath(path)
	if cq.HasPath(path) {
		t.Fatal("CancelPath should remove all queued entries for path from index")
	}
	if !cq.isEntryCanceled(e1) || !cq.isEntryCanceled(e2) {
		t.Fatal("CancelPath should cancel all queued entries for path")
	}
	if !cq.HasPath(otherPath) {
		t.Fatal("CancelPath should preserve unrelated queued paths")
	}

	cq.removeFromQueue(other)
	if cq.HasPath(otherPath) {
		t.Fatal("removeFromQueue should remove queued path index entry")
	}
}

// TestCommitQueueDirectPutRouting verifies that files under
// commitQueueDirectPutThreshold use direct PUT (WriteCtxConditionalWithRevision)
// which sends raw body, while files at or above the threshold use multipart
// upload (uploadBufferedRemoteFile → WriteStreamConditional).
func TestCommitQueueDirectPutRouting(t *testing.T) {
	var usedDirectPut bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Direct PUT sends Content-Type: application/octet-stream with raw body.
		// Multipart upload sends to /upload/initiate first.
		if r.Method == http.MethodPut && r.Header.Get("Content-Type") == "application/octet-stream" {
			usedDirectPut = true
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ok","revision":10}`))
			return
		}
		// Fallback: accept anything else (multipart endpoints, etc.)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	t.Run("small file uses direct PUT", func(t *testing.T) {
		usedDirectPut = false
		shadow, err := NewShadowStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer shadow.Close()
		pending, err := NewPendingIndex(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}

		data := make([]byte, 30000) // 30KB — under 50KB server threshold
		if err := shadow.WriteFull("/small.bin", data, 5); err != nil {
			t.Fatal(err)
		}
		if _, err := pending.PutWithBaseRev("/small.bin", int64(len(data)), PendingOverwrite, 5); err != nil {
			t.Fatal(err)
		}

		cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
		if err := cq.Enqueue(&CommitEntry{
			Path:    "/small.bin",
			BaseRev: 5,
			Size:    int64(len(data)),
			Kind:    PendingOverwrite,
		}); err != nil {
			t.Fatal(err)
		}
		cq.DrainAll()

		if !usedDirectPut {
			t.Fatal("30KB file should use direct PUT, not multipart")
		}
		if shadow.Has("/small.bin") {
			t.Fatal("shadow should be removed after commit")
		}
	})
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

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
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

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
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

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
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

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
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

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
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
	data := bytes.Repeat([]byte("shadowspill-data-"), 100) // ~1700 bytes
	var gotExpected int64
	var gotBody []byte
	var statCalls atomic.Int32
	var successRev int64
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/big.bin":
			statCalls.Add(1)
			w.Header().Set("X-Dat9-Revision", "13")
			w.Header().Set("Content-Length", "1700")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			var req struct {
				Path             string `json:"path"`
				TotalSize        int64  `json:"total_size"`
				ExpectedRevision *int64 `json:"expected_revision"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode initiate request: %v", err)
			}
			if req.Path != "/big.bin" {
				t.Fatalf("initiate path = %q, want /big.bin", req.Path)
			}
			if req.TotalSize != int64(len(data)) {
				t.Fatalf("initiate total_size = %d, want %d", req.TotalSize, len(data))
			}
			if req.ExpectedRevision == nil {
				t.Fatal("initiate expected_revision missing")
			}
			gotExpected = *req.ExpectedRevision
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"upload_id":   "u1",
				"key":         "object-key",
				"part_size":   int64(len(data)),
				"total_parts": 1,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/u1/presign-batch":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"parts": []map[string]any{{
					"number": 1,
					"url":    ts.URL + "/s3/u1/1",
					"size":   int64(len(data)),
				}},
			})
		case r.Method == http.MethodPut && r.URL.Path == "/s3/u1/1":
			body, _ := io.ReadAll(r.Body)
			gotBody = body
			w.Header().Set("ETag", "etag-1")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/u1/complete":
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
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

	if err := shadow.WriteFull("/big.bin", data, 12); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/big.bin", int64(len(data)), PendingOverwrite, 12); err != nil {
		t.Fatal(err)
	}

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	cq.OnSuccess = func(entry *CommitEntry, committedRev int64) {
		if entry.Path != "/big.bin" {
			t.Fatalf("OnSuccess path = %q, want /big.bin", entry.Path)
		}
		successRev = committedRev
	}
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

	if gotExpected != 12 {
		t.Fatalf("expected revision = %d, want 12", gotExpected)
	}
	if !bytes.Equal(gotBody, data) {
		t.Fatalf("server received %d bytes, want %d", len(gotBody), len(data))
	}
	if statCalls.Load() != 0 {
		t.Fatalf("stat calls = %d, want 0", statCalls.Load())
	}
	if successRev != 0 {
		t.Fatalf("OnSuccess committedRev = %d, want 0 for multipart stream", successRev)
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

	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
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
	data := bytes.Repeat([]byte("recover-"), 200)
	var gotBody []byte
	var statCalls atomic.Int32
	var successRev int64
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/recover.bin":
			statCalls.Add(1)
			w.Header().Set("X-Dat9-Revision", "9")
			w.Header().Set("Content-Length", "1600")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			var req struct {
				Path             string `json:"path"`
				TotalSize        int64  `json:"total_size"`
				ExpectedRevision *int64 `json:"expected_revision"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode initiate request: %v", err)
			}
			if req.Path != "/recover.bin" {
				t.Fatalf("initiate path = %q, want /recover.bin", req.Path)
			}
			if req.TotalSize != int64(len(data)) {
				t.Fatalf("initiate total_size = %d, want %d", req.TotalSize, len(data))
			}
			if req.ExpectedRevision == nil || *req.ExpectedRevision != 8 {
				t.Fatalf("initiate expected_revision = %v, want 8", req.ExpectedRevision)
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"upload_id":   "u1",
				"key":         "object-key",
				"part_size":   int64(len(data)),
				"total_parts": 1,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/u1/presign-batch":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"parts": []map[string]any{{
					"number": 1,
					"url":    ts.URL + "/s3/u1/1",
					"size":   int64(len(data)),
				}},
			})
		case r.Method == http.MethodPut && r.URL.Path == "/s3/u1/1":
			body, _ := io.ReadAll(r.Body)
			gotBody = body
			w.Header().Set("ETag", "etag-1")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/u1/complete":
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
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
	cq := NewCommitQueue(newTestClient(ts.URL), shadow2, pending2, nil, 1, 8)
	cq.OnSuccess = func(entry *CommitEntry, committedRev int64) {
		if entry.Path != "/recover.bin" {
			t.Fatalf("OnSuccess path = %q, want /recover.bin", entry.Path)
		}
		successRev = committedRev
	}
	cq.RecoverPending()
	cq.DrainAll()

	// Verify data arrived correctly at the server (streaming upload worked).
	if !bytes.Equal(gotBody, data) {
		t.Fatalf("server received %d bytes, want %d", len(gotBody), len(data))
	}
	if statCalls.Load() != 0 {
		t.Fatalf("stat calls = %d, want 0", statCalls.Load())
	}
	if successRev != 0 {
		t.Fatalf("OnSuccess committedRev = %d, want 0 for multipart stream", successRev)
	}
	if pending2.HasPending("/recover.bin") {
		t.Fatal("pending entry should be removed after successful recovery upload")
	}
	if shadow2.Has("/recover.bin") {
		t.Fatal("shadow should be removed after successful recovery upload")
	}
}
