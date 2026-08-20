package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testtidb"
	"github.com/mem9-ai/drive9/pkg/datastore"
)

// sseEvent represents a parsed SSE event.
type sseEvent struct {
	Event string
	Data  string
}

// readSSEEvent reads one SSE event from the scanner.
func readSSEEvent(scanner *bufio.Scanner) (sseEvent, bool) {
	var ev sseEvent
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if ev.Event != "" || ev.Data != "" {
				return ev, true
			}
			continue
		}
		if strings.HasPrefix(line, "event: ") {
			ev.Event = strings.TrimPrefix(line, "event: ")
		} else if strings.HasPrefix(line, "data: ") {
			ev.Data = strings.TrimPrefix(line, "data: ")
		}
	}
	return ev, false
}

func newTestStoreForSSE(t *testing.T) *datastore.Store {
	t.Helper()
	store, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	testtidb.ResetDB(t, store.DB())
	if _, err := store.DB().Exec(`CREATE TABLE IF NOT EXISTS fs_events (
		seq        BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		path       TEXT NOT NULL,
		op         VARCHAR(64) NOT NULL,
		actor      VARCHAR(255),
		ts         BIGINT NOT NULL,
		created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`CREATE INDEX idx_fs_events_created ON fs_events(created_at)`); err != nil && !strings.Contains(err.Error(), "Duplicate key") {
		t.Fatal(err)
	}
	// Reset AUTO_INCREMENT so seq starts at 1 for deterministic test assertions.
	if _, err := store.DB().Exec(`ALTER TABLE fs_events AUTO_INCREMENT = 1`); err != nil {
		t.Fatal(err)
	}
	return store
}

func newSSETestServer(t *testing.T) (*Server, *datastore.Store) {
	t.Helper()
	store := newTestStoreForSSE(t)
	srv := &Server{events: newEventBuses()}
	srv.events.get("", store)
	return srv, store
}

func publishTestEvent(t *testing.T, store *datastore.Store, bus *EventBus, path, op, actor string) {
	t.Helper()
	if _, err := store.InsertFSEvent(context.Background(), path, op, actor, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	bus.Publish()
}

func TestSSEEndpointSince0SendsReset(t *testing.T) {
	srv, store := newSSETestServer(t)
	bus := srv.events.get("", store)
	publishTestEvent(t, store, bus, "/existing.txt", "write", "")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Inject fallback scope context.
		ctx := context.WithValue(r.Context(), tenantScopeKey, &TenantScope{TenantID: ""})
		srv.handleEvents(w, r.WithContext(ctx))
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"?since=0", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type=%q, want text/event-stream", ct)
	}

	scanner := bufio.NewScanner(resp.Body)
	ev, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected at least one SSE event")
	}
	if ev.Event != "reset" {
		t.Fatalf("first event=%q, want 'reset'", ev.Event)
	}

	var data map[string]interface{}
	if err := json.Unmarshal([]byte(ev.Data), &data); err != nil {
		t.Fatalf("unmarshal reset data: %v", err)
	}
	if data["reason"] != "initial_sync" {
		t.Errorf("reset reason=%v, want initial_sync", data["reason"])
	}
	if _, ok := data["actor"]; ok {
		t.Errorf("initial_sync reset should not include actor: %+v", data)
	}
	if _, ok := data["path"]; ok {
		t.Errorf("initial_sync reset should not include path: %+v", data)
	}
	if _, ok := data["op"]; ok {
		t.Errorf("initial_sync reset should not include op: %+v", data)
	}
}

func TestSSEEndpointReplay(t *testing.T) {
	srv, store := newSSETestServer(t)
	bus := srv.events.get("", store)

	publishTestEvent(t, store, bus, "/a.txt", "write", "actor1")
	publishTestEvent(t, store, bus, "/b.txt", "write", "actor2")
	publishTestEvent(t, store, bus, "/c.txt", "write", "actor1")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), tenantScopeKey, &TenantScope{TenantID: ""})
		srv.handleEvents(w, r.WithContext(ctx))
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"?since=1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	scanner := bufio.NewScanner(resp.Body)

	// Should get 2 events (seq=2 and seq=3).
	ev1, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected first replayed event")
	}
	if ev1.Event != "file_changed" {
		t.Fatalf("event1=%q, want file_changed", ev1.Event)
	}
	var data1 ChangeEvent
	if err := json.Unmarshal([]byte(ev1.Data), &data1); err != nil {
		t.Fatalf("unmarshal event1: %v", err)
	}
	if data1.Path != "/b.txt" || data1.Op != "write" {
		t.Errorf("event1 data: %+v", data1)
	}

	ev2, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected second replayed event")
	}
	var data2 ChangeEvent
	if err := json.Unmarshal([]byte(ev2.Data), &data2); err != nil {
		t.Fatalf("unmarshal event2: %v", err)
	}
	if data2.Path != "/c.txt" || data2.Op != "write" {
		t.Errorf("event2 data: %+v", data2)
	}
}

func TestSSEEndpointLiveEvent(t *testing.T) {
	srv, store := newSSETestServer(t)
	bus := srv.events.get("", store)

	publishTestEvent(t, store, bus, "/existing.txt", "write", "")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), tenantScopeKey, &TenantScope{TenantID: ""})
		srv.handleEvents(w, r.WithContext(ctx))
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Connect at current head (since=1), so no replay.
	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"?since=1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	scanner := bufio.NewScanner(resp.Body)

	current, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected initial stream-current heartbeat")
	}
	if current.Event != "heartbeat" {
		t.Fatalf("initial event=%q, want heartbeat", current.Event)
	}

	publishTestEvent(t, store, bus, "/new.txt", "write", "remote-actor")

	ev, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected live event")
	}
	if ev.Event != "file_changed" {
		t.Fatalf("live event=%q, want file_changed", ev.Event)
	}
	var data ChangeEvent
	if err := json.Unmarshal([]byte(ev.Data), &data); err != nil {
		t.Fatalf("unmarshal live event: %v", err)
	}
	if data.Path != "/new.txt" || data.Actor != "remote-actor" {
		t.Errorf("live event data: %+v", data)
	}
}

func TestSSEStructuralOpEmitsReset(t *testing.T) {
	srv, store := newSSETestServer(t)
	bus := srv.events.get("", store)

	publishTestEvent(t, store, bus, "/a.txt", "write", "actor1")
	publishTestEvent(t, store, bus, "/old.txt", "rename", "actor1")
	publishTestEvent(t, store, bus, "/dir", "mkdir", "actor1")
	publishTestEvent(t, store, bus, "/gone.txt", "delete", "actor1")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), tenantScopeKey, &TenantScope{TenantID: ""})
		srv.handleEvents(w, r.WithContext(ctx))
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Replay from seq=0 → initial reset, then connect at head for live.
	// Instead, replay from seq=1 to get events 2,3,4.
	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"?since=1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	scanner := bufio.NewScanner(resp.Body)

	// Event 2: rename → should be reset with reason structural_change.
	ev1, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected first event")
	}
	if ev1.Event != "reset" {
		t.Fatalf("rename op: event=%q, want reset", ev1.Event)
	}
	var reset1 map[string]interface{}
	if err := json.Unmarshal([]byte(ev1.Data), &reset1); err != nil {
		t.Fatalf("unmarshal reset1: %v", err)
	}
	if reset1["reason"] != "structural_change" {
		t.Errorf("rename reset reason=%v, want structural_change", reset1["reason"])
	}
	if reset1["actor"] != "actor1" {
		t.Errorf("rename reset actor=%v, want actor1", reset1["actor"])
	}
	if reset1["path"] != "/old.txt" {
		t.Errorf("rename reset path=%v, want /old.txt", reset1["path"])
	}
	if reset1["op"] != "rename" {
		t.Errorf("rename reset op=%v, want rename", reset1["op"])
	}

	// Event 3: mkdir → also reset.
	ev2, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected second event")
	}
	if ev2.Event != "reset" {
		t.Fatalf("mkdir op: event=%q, want reset", ev2.Event)
	}
	var reset2 map[string]interface{}
	if err := json.Unmarshal([]byte(ev2.Data), &reset2); err != nil {
		t.Fatalf("unmarshal reset2: %v", err)
	}
	if reset2["reason"] != "structural_change" {
		t.Errorf("mkdir reset reason=%v, want structural_change", reset2["reason"])
	}
	if reset2["actor"] != "actor1" {
		t.Errorf("mkdir reset actor=%v, want actor1", reset2["actor"])
	}
	if reset2["path"] != "/dir" {
		t.Errorf("mkdir reset path=%v, want /dir", reset2["path"])
	}
	if reset2["op"] != "mkdir" {
		t.Errorf("mkdir reset op=%v, want mkdir", reset2["op"])
	}

	// Event 4: delete → also reset.
	ev3, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected third event")
	}
	if ev3.Event != "reset" {
		t.Fatalf("delete op: event=%q, want reset", ev3.Event)
	}
	var reset3 map[string]interface{}
	if err := json.Unmarshal([]byte(ev3.Data), &reset3); err != nil {
		t.Fatalf("unmarshal reset3: %v", err)
	}
	if reset3["reason"] != "structural_change" {
		t.Errorf("delete reset reason=%v, want structural_change", reset3["reason"])
	}
	if reset3["actor"] != "actor1" {
		t.Errorf("delete reset actor=%v, want actor1", reset3["actor"])
	}
	if reset3["path"] != "/gone.txt" {
		t.Errorf("delete reset path=%v, want /gone.txt", reset3["path"])
	}
	if reset3["op"] != "delete" {
		t.Errorf("delete reset op=%v, want delete", reset3["op"])
	}
}

func TestSSEStructuralOpLiveEmitsReset(t *testing.T) {
	srv, store := newSSETestServer(t)
	bus := srv.events.get("", store)

	publishTestEvent(t, store, bus, "/existing.txt", "write", "")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), tenantScopeKey, &TenantScope{TenantID: ""})
		srv.handleEvents(w, r.WithContext(ctx))
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"?since=1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	scanner := bufio.NewScanner(resp.Body)

	current, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected initial stream-current heartbeat")
	}
	if current.Event != "heartbeat" {
		t.Fatalf("initial event=%q, want heartbeat", current.Event)
	}

	publishTestEvent(t, store, bus, "/old", "rename", "remote-actor")

	ev, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected live event")
	}
	if ev.Event != "reset" {
		t.Fatalf("live rename: event=%q, want reset", ev.Event)
	}
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(ev.Data), &data); err != nil {
		t.Fatalf("unmarshal live reset: %v", err)
	}
	if data["reason"] != "structural_change" {
		t.Errorf("live rename reason=%v, want structural_change", data["reason"])
	}
	if data["actor"] != "remote-actor" {
		t.Errorf("live rename actor=%v, want remote-actor", data["actor"])
	}
	if data["path"] != "/old" {
		t.Errorf("live rename path=%v, want /old", data["path"])
	}
	if data["op"] != "rename" {
		t.Errorf("live rename op=%v, want rename", data["op"])
	}
}

func TestSSEEndpointMethodNotAllowed(t *testing.T) {
	srv := &Server{events: newEventBuses()}
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/v1/events", nil)
	srv.handleEvents(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d, want 405", w.Code)
	}
}

func TestSSEEndpointBadSince(t *testing.T) {
	srv := &Server{events: newEventBuses()}
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/v1/events?since=abc", nil)
	srv.handleEvents(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400", w.Code)
	}
}

func TestSSEBufferedWriterBatchFlush(t *testing.T) {
	rec := httptest.NewRecorder()
	bw := newSSEBufferedWriter(rec, rec)

	// Write 9 events (below batch size of 10).
	for i := 0; i < 9; i++ {
		sendSSEHeartbeat(bw, uint64(i))
	}

	// Before flush, response body should be empty (buffered).
	if rec.Body.Len() != 0 {
		t.Fatalf("expected empty body before flush, got %d bytes", rec.Body.Len())
	}

	// shouldFlush should return false since count < 10.
	if bw.shouldFlush() {
		t.Fatal("shouldFlush should be false below batch size")
	}

	// 10th event reaches batch size.
	sendSSEHeartbeat(bw, 9)

	// shouldFlush should now return true.
	if !bw.shouldFlush() {
		t.Fatal("shouldFlush should be true at batch size")
	}

	// Explicit flush simulates what handleEvents does when shouldFlush is true.
	if err := bw.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// After batch flush, all 10 events should be in the body.
	body := rec.Body.String()
	count := strings.Count(body, "event: heartbeat")
	if count != 10 {
		t.Fatalf("expected 10 heartbeat events after batch flush, got %d", count)
	}
}

func TestSSEBufferedWriterMaxDelayFlush(t *testing.T) {
	rec := httptest.NewRecorder()
	bw := newSSEBufferedWriter(rec, rec)

	// Write 1 event.
	sendSSEHeartbeat(bw, 1)

	// Before max delay expires, body should be empty and shouldFlush false.
	if rec.Body.Len() != 0 {
		t.Fatalf("expected empty body before max delay, got %d bytes", rec.Body.Len())
	}
	if bw.shouldFlush() {
		t.Fatal("shouldFlush should be false before max delay")
	}

	// Wait for max delay (1ms) plus a small margin.
	time.Sleep(sseFlushMaxDelay + 5*time.Millisecond)

	// After max delay, shouldFlush should become true.
	if !bw.shouldFlush() {
		t.Fatal("shouldFlush should be true after max delay")
	}

	// Flush and verify.
	if err := bw.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "event: heartbeat") {
		t.Fatal("expected heartbeat event after max delay flush")
	}
}

func TestSSEBufferedWriterExplicitFlush(t *testing.T) {
	rec := httptest.NewRecorder()
	bw := newSSEBufferedWriter(rec, rec)

	sendSSEEvent(bw, ChangeEvent{Seq: 1, Path: "/a.txt", Op: "write"})

	if rec.Body.Len() != 0 {
		t.Fatalf("expected empty body before explicit flush, got %d bytes", rec.Body.Len())
	}

	if err := bw.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "file_changed") {
		t.Fatal("expected file_changed event after explicit flush")
	}
	if !strings.Contains(body, "/a.txt") {
		t.Fatal("expected event data to contain path")
	}
}

// TestSSEBurstFlush verifies that a burst of 3 events arriving in a single
// notify wakeup are flushed within sseFlushMaxDelay, not buffered until the
// next heartbeat (30s).
func TestSSEBurstFlush(t *testing.T) {
	srv, store := newSSETestServer(t)
	bus := srv.events.get("", store)

	publishTestEvent(t, store, bus, "/existing.txt", "write", "")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), tenantScopeKey, &TenantScope{TenantID: ""})
		srv.handleEvents(w, r.WithContext(ctx))
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"?since=1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	scanner := bufio.NewScanner(resp.Body)

	// Initial replay is empty for since=1, but the server must still emit a
	// current heartbeat immediately so clients can clear reconnect-unverified
	// cache state without waiting for the periodic heartbeat.
	ev, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected initial current heartbeat")
	}
	if ev.Event != "heartbeat" {
		t.Fatalf("expected initial heartbeat, got %q", ev.Event)
	}

	// Publish a burst of 3 events.
	go func() {
		time.Sleep(50 * time.Millisecond)
		publishTestEvent(t, store, bus, "/a.txt", "write", "actor1")
		publishTestEvent(t, store, bus, "/b.txt", "write", "actor2")
		publishTestEvent(t, store, bus, "/c.txt", "write", "actor3")
	}()

	// Read first event with a timeout well under heartbeat (30s).
	done := make(chan struct{})
	go func() {
		ev, ok = readSSEEvent(scanner)
		close(done)
	}()

	select {
	case <-done:
		if !ok {
			t.Fatal("expected first event from burst")
		}
		if ev.Event != "file_changed" {
			t.Fatalf("expected file_changed, got %q", ev.Event)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("burst events not flushed within 200ms (timer broken?)")
	}

	// Read remaining 2 events quickly.
	for i := 0; i < 2; i++ {
		ev, ok = readSSEEvent(scanner)
		if !ok {
			t.Fatalf("expected event %d from burst", i+2)
		}
		if ev.Event != "file_changed" {
			t.Fatalf("expected file_changed for event %d, got %q", i+2, ev.Event)
		}
	}
}

// TestSSEResetFlushWhenSeqTooOld verifies that a reset caused by a stale since
// (events were pruned by cleanup) is flushed immediately.
func TestSSEResetFlushWhenSeqTooOld(t *testing.T) {
	srv, store := newSSETestServer(t)
	bus := srv.events.get("", store)

	// Insert one event.
	publishTestEvent(t, store, bus, "/a.txt", "write", "actor1")

	// Delete all events to simulate cleanup pruning.
	if _, _, err := store.DeleteFSEventsBefore(context.Background(), time.Now().Add(time.Hour), 1000, 10); err != nil {
		t.Fatal(err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), tenantScopeKey, &TenantScope{TenantID: ""})
		srv.handleEvents(w, r.WithContext(ctx))
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Connect with since=1 (the event was pruned → empty table → reset).
	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"?since=1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	scanner := bufio.NewScanner(resp.Body)

	ev, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected reset event immediately")
	}
	if ev.Event != "reset" {
		t.Fatalf("expected reset, got %q", ev.Event)
	}
	// A fully-pruned table (headSeq == 0) must be labeled seq_too_old — the
	// cursor is behind the retained window, not ahead of a non-empty head.
	var data sseResetPayload
	if err := json.Unmarshal([]byte(ev.Data), &data); err != nil {
		t.Fatalf("unmarshal reset: %v", err)
	}
	if data.Reason != "seq_too_old" {
		t.Fatalf("reset reason = %q, want seq_too_old (fully-pruned table)", data.Reason)
	}
}

// TestSSEResetWhenPartialPruning verifies that a client whose cursor falls
// in the pruned gap (events between cursor and oldest retained were deleted)
// gets a reset, not a silent gap in the replay.
func TestSSEResetWhenPartialPruning(t *testing.T) {
	srv, store := newSSETestServer(t)
	bus := srv.events.get("", store)

	// Insert events seq=1,2,3.
	publishTestEvent(t, store, bus, "/a.txt", "write", "actor1")
	publishTestEvent(t, store, bus, "/b.txt", "write", "actor2")
	publishTestEvent(t, store, bus, "/c.txt", "write", "actor3")

	// Delete events 1 and 2 (simulate partial cleanup pruning).
	if _, err := store.DB().Exec(`DELETE FROM fs_events WHERE seq <= 2`); err != nil {
		t.Fatal(err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), tenantScopeKey, &TenantScope{TenantID: ""})
		srv.handleEvents(w, r.WithContext(ctx))
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Client at since=1: events 2 was pruned (oldestSeq=3 > since+1=2) → reset.
	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"?since=1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	scanner := bufio.NewScanner(resp.Body)
	ev, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected event")
	}
	if ev.Event != "reset" {
		t.Fatalf("expected reset for partially-pruned cursor, got %q", ev.Event)
	}
}

// TestSSEResetWhenFutureCursor verifies that a client whose since is ahead of
// the current head (server restarted / cursor stale) gets a reset.
func TestSSEResetWhenFutureCursor(t *testing.T) {
	srv, store := newSSETestServer(t)
	bus := srv.events.get("", store)

	// Insert one event (seq=1).
	publishTestEvent(t, store, bus, "/a.txt", "write", "actor1")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), tenantScopeKey, &TenantScope{TenantID: ""})
		srv.handleEvents(w, r.WithContext(ctx))
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Client at since=999: ahead of head (1) → reset (server_restart).
	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"?since=999", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	scanner := bufio.NewScanner(resp.Body)
	ev, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected event")
	}
	// With no new events and since > head, the handler sends a heartbeat first
	// (from the empty EventsSince result), then the poll ticker or the next
	// EventsSince detects since > head and sends reset. But actually on the
	// initial connection, EventsSince(999) returns ok=false → reset immediately.
	if ev.Event != "reset" {
		t.Fatalf("expected reset for future cursor, got %q", ev.Event)
	}
}

// bulkInsertFSEvents inserts n fs_events rows directly (bypassing the bus) in
// multi-row batches for speed. Returns nothing; seqs are assigned by the
// table's AUTO_INCREMENT (reset to 1 by newTestStoreForSSE).
func bulkInsertFSEvents(t *testing.T, store *datastore.Store, n int) {
	t.Helper()
	ctx := context.Background()
	for start := 0; start < n; start += 200 {
		batch := min(200, n-start)
		var sb strings.Builder
		sb.WriteString("INSERT INTO fs_events (path, op, actor, ts) VALUES ")
		args := make([]any, 0, batch*2)
		for i := 0; i < batch; i++ {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString("(?, 'write', 'bulk', ?)")
			args = append(args, fmt.Sprintf("/bulk/%d.txt", start+i), int64(start+i))
		}
		if _, err := store.DB().ExecContext(ctx, sb.String(), args...); err != nil {
			t.Fatalf("bulk insert at %d: %v", start, err)
		}
	}
}

// TestSSEEndpointPhase1DrainsMultiPageBacklog verifies that a reconnect with a
// backlog larger than one EventsSince page streams the entire backlog during
// Phase 1 (loop until a short page), ending with a heartbeat at the head.
func TestSSEEndpointPhase1DrainsMultiPageBacklog(t *testing.T) {
	srv, store := newSSETestServer(t)

	total := eventPageSize + 50
	bulkInsertFSEvents(t, store, total)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), tenantScopeKey, &TenantScope{TenantID: ""})
		srv.handleEvents(w, r.WithContext(ctx))
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// since=1 → replay seq 2..total (more than one eventPageSize page).
	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"?since=1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	scanner := bufio.NewScanner(resp.Body)
	events := 0
	for {
		ev, ok := readSSEEvent(scanner)
		if !ok {
			t.Fatalf("stream ended after %d events before the end-of-replay heartbeat", events)
		}
		switch ev.Event {
		case "file_changed":
			events++
		case "heartbeat":
			if events != total-1 {
				t.Fatalf("replayed %d events before heartbeat, want %d (multi-page drain)", events, total-1)
			}
			var data map[string]uint64
			if err := json.Unmarshal([]byte(ev.Data), &data); err != nil {
				t.Fatalf("unmarshal heartbeat: %v", err)
			}
			if data["seq"] != uint64(total) {
				t.Fatalf("heartbeat seq = %d, want %d", data["seq"], total)
			}
			return
		}
	}
}

// TestSSEEndpointPhase2EventDrivenDrain verifies that a single notify wake
// drains a backlog larger than one page: pollAndSend re-polls immediately on
// a full page instead of waiting for the next notify signal.
func TestSSEEndpointPhase2EventDrivenDrain(t *testing.T) {
	srv, store := newSSETestServer(t)
	bus := srv.events.get("", store)

	// One existing event so the client can connect at head with no replay.
	publishTestEvent(t, store, bus, "/existing.txt", "write", "")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), tenantScopeKey, &TenantScope{TenantID: ""})
		srv.handleEvents(w, r.WithContext(ctx))
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"?since=1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	scanner := bufio.NewScanner(resp.Body)

	// First event must be the end-of-Phase-1 heartbeat (caught up at head).
	ev, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected initial heartbeat")
	}
	if ev.Event != "heartbeat" {
		t.Fatalf("first event = %q, want heartbeat", ev.Event)
	}

	// Commit a > 1 page backlog directly, then send ONE notify signal. The
	// event-driven drain must deliver all of them on that single wake.
	backlog := eventPageSize + 10
	bulkInsertFSEvents(t, store, backlog)
	bus.Publish()

	for i := 0; i < backlog; i++ {
		ev, ok := readSSEEvent(scanner)
		if !ok {
			t.Fatalf("stream ended after %d/%d drained events", i, backlog)
		}
		if ev.Event != "file_changed" {
			t.Fatalf("event %d = %q, want file_changed", i, ev.Event)
		}
	}
}

// TestSSEEndpointPhase2CatchupTimerDrainsBurst verifies that a burst larger
// than sseMaxFullPagesPerWake pages does NOT stall after the per-wake cap:
// the catch-up timer re-arms and the tail is delivered with no further
// notify signal (the outbox coalescer emits only one signal per tenant per
// 200ms window, so without the timer the tail would wait for the next write).
func TestSSEEndpointPhase2CatchupTimerDrainsBurst(t *testing.T) {
	oldPage, oldCap := eventPageSize, sseMaxFullPagesPerWake
	eventPageSize, sseMaxFullPagesPerWake = 50, 3
	t.Cleanup(func() { eventPageSize, sseMaxFullPagesPerWake = oldPage, oldCap })

	srv, store := newSSETestServer(t)
	bus := srv.events.get("", store)

	// One existing event so the client can connect at head with no replay.
	publishTestEvent(t, store, bus, "/existing.txt", "write", "")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), tenantScopeKey, &TenantScope{TenantID: ""})
		srv.handleEvents(w, r.WithContext(ctx))
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"?since=1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	scanner := bufio.NewScanner(resp.Body)

	// First event must be the end-of-Phase-1 heartbeat (caught up at head).
	ev, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected initial heartbeat")
	}
	if ev.Event != "heartbeat" {
		t.Fatalf("first event = %q, want heartbeat", ev.Event)
	}

	// Commit a 5-page burst (page=50, cap=3): the first wake drains 3 pages
	// and re-arms the catch-up timer; the remaining 2 pages must arrive on
	// timer fires with NO further Publish.
	const backlog = 250
	bulkInsertFSEvents(t, store, backlog)
	bus.Publish()

	for i := 0; i < backlog; i++ {
		ev, ok := readSSEEvent(scanner)
		if !ok {
			t.Fatalf("stream ended after %d/%d events (catch-up timer failed to resume the drain)", i, backlog)
		}
		if ev.Event != "file_changed" {
			t.Fatalf("event %d = %q, want file_changed", i, ev.Event)
		}
	}
}

// TestSSEEndpointPhase1MidDrainErrorTerminates verifies that a DB query error
// in the middle of the Phase-1 backlog drain TERMINATES the stream instead of
// sending a false caught-up heartbeat: the client must reconnect and resume
// from its durable cursor rather than believe it is current.
func TestSSEEndpointPhase1MidDrainErrorTerminates(t *testing.T) {
	oldPage := eventPageSize
	eventPageSize = 3
	t.Cleanup(func() { eventPageSize = oldPage })

	srv, store := newSSETestServer(t)

	// 4 real events; with page size 3 and since=1 the first (real, tolerated)
	// Phase-1 call returns a full page (seqs 2,3,4).
	bulkInsertFSEvents(t, store, 4)

	// Stub the mid-drain event source to report a DB error deterministically.
	// The FIRST Phase-1 call delegates to the real implementation (it must
	// return the full page from the real rows); later calls fail.
	realEventsSinceE := eventsSinceE
	var calls atomic.Int32
	eventsSinceE = func(eb *EventBus, ctx context.Context, since uint64) ([]ChangeEvent, uint64, bool, error) {
		if calls.Add(1) == 1 {
			return realEventsSinceE(eb, ctx, since)
		}
		return nil, since, true, fmt.Errorf("injected mid-drain db error")
	}
	t.Cleanup(func() { eventsSinceE = realEventsSinceE })

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), tenantScopeKey, &TenantScope{TenantID: ""})
		srv.handleEvents(w, r.WithContext(ctx))
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"?since=1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	scanner := bufio.NewScanner(resp.Body)

	// The client receives the first page (3 events) and then the stream must
	// END: no heartbeat, no reset — EOF. Without termination the next read
	// would block until the 5s ctx timeout and fail this test.
	for i, wantSeq := range []uint64{2, 3, 4} {
		ev, ok := readSSEEvent(scanner)
		if !ok {
			t.Fatalf("stream ended before page-1 event %d", i)
		}
		if ev.Event != "file_changed" {
			t.Fatalf("event %d = %q, want file_changed", i, ev.Event)
		}
		var data ChangeEvent
		if err := json.Unmarshal([]byte(ev.Data), &data); err != nil {
			t.Fatalf("unmarshal event %d: %v", i, err)
		}
		if data.Seq != wantSeq {
			t.Fatalf("event %d seq = %d, want %d", i, data.Seq, wantSeq)
		}
	}
	if ev, ok := readSSEEvent(scanner); ok {
		t.Fatalf("expected stream termination after mid-drain error, got extra event %q (false caught-up marker?)", ev.Event)
	}
	if calls.Load() < 1 {
		t.Fatalf("eventsSinceE calls = %d, want >= 1 (mid-drain call)", calls.Load())
	}
}

// TestSSEEndpointPhase2QueryErrorKeepsCatchupArmed verifies that a DB query
// error mid-burst does NOT kill the event-driven drain: the catch-up timer
// must stay armed so the tail is delivered once the error clears, with no
// further notify signal. (The companion guarantee — the failed poll not
// stamping lastSuccessfulPoll — is enforced in pollAndSend by the early
// qErr return before the stamp; a timing-based assertion of that internal
// clock would be flaky by construction, so it is covered by review of that
// branch plus the liveness-poll path in handleEvents.)
func TestSSEEndpointPhase2QueryErrorKeepsCatchupArmed(t *testing.T) {
	oldPage, oldCap := eventPageSize, sseMaxFullPagesPerWake
	eventPageSize, sseMaxFullPagesPerWake = 50, 3
	t.Cleanup(func() { eventPageSize, sseMaxFullPagesPerWake = oldPage, oldCap })

	srv, store := newSSETestServer(t)
	bus := srv.events.get("", store)

	// One existing event so the client can connect at head with no replay.
	publishTestEvent(t, store, bus, "/existing.txt", "write", "")

	// Inject exactly ONE query error: the 4th poll (the first catch-up fire
	// after the initial 3-page wake) fails; every other call goes through.
	realEventsSinceE := eventsSinceE
	var pollCalls atomic.Int32
	eventsSinceE = func(eb *EventBus, ctx context.Context, since uint64) ([]ChangeEvent, uint64, bool, error) {
		if pollCalls.Add(1) == 4 {
			return nil, since, true, fmt.Errorf("injected mid-burst db error")
		}
		return realEventsSinceE(eb, ctx, since)
	}
	t.Cleanup(func() { eventsSinceE = realEventsSinceE })

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), tenantScopeKey, &TenantScope{TenantID: ""})
		srv.handleEvents(w, r.WithContext(ctx))
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"?since=1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	scanner := bufio.NewScanner(resp.Body)

	// First event must be the end-of-Phase-1 heartbeat (caught up at head).
	ev, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected initial heartbeat")
	}
	if ev.Event != "heartbeat" {
		t.Fatalf("first event = %q, want heartbeat", ev.Event)
	}

	// Commit a 5-page burst (page=50, cap=3) and send ONE notify signal.
	// Wake 1 drains 3 pages and arms the catch-up timer. The first catch-up
	// fire hits the injected error: with the bug it would disarm and the tail
	// would stall until the next write (test would time out); post-fix the
	// timer stays armed and the remaining 2 pages arrive on later fires.
	const backlog = 250
	bulkInsertFSEvents(t, store, backlog)
	bus.Publish()

	for i := 0; i < backlog; i++ {
		ev, ok := readSSEEvent(scanner)
		if !ok {
			t.Fatalf("stream ended after %d/%d events (query error killed the catch-up drain)", i, backlog)
		}
		if ev.Event != "file_changed" {
			t.Fatalf("event %d = %q, want file_changed", i, ev.Event)
		}
	}
	if got := pollCalls.Load(); got < 7 {
		t.Fatalf("poll calls = %d, want >= 7 (3 initial + 1 error + 3 drain)", got)
	}
}

// TestSSECatchupBackoff asserts the catch-up re-arm backoff curve as pure
// state (no sleeping): 10ms doubling per consecutive error, capped at 1s.
func TestSSECatchupBackoff(t *testing.T) {
	for _, tc := range []struct {
		errs int
		want time.Duration
	}{
		{0, 10 * time.Millisecond}, // defensive floor
		{1, 10 * time.Millisecond},
		{2, 20 * time.Millisecond},
		{3, 40 * time.Millisecond},
		{6, 320 * time.Millisecond},
		{7, 640 * time.Millisecond},
		{8, sseCatchupMaxPollDelay}, // 10ms << 7 = 1.28s → capped
		{100, sseCatchupMaxPollDelay},
	} {
		if got := sseCatchupBackoff(tc.errs); got != tc.want {
			t.Fatalf("sseCatchupBackoff(%d) = %v, want %v", tc.errs, got, tc.want)
		}
	}
}

// TestSSEEndpointPhase2QueryErrorBackoffResumesDrain verifies that a burst
// drain survives MULTIPLE consecutive query errors on the catch-up timer: the
// re-arm backs off (10ms → 20ms → 40ms) instead of giving up, and once the
// error clears the tail is delivered with no further notify signal.
func TestSSEEndpointPhase2QueryErrorBackoffResumesDrain(t *testing.T) {
	oldPage, oldCap := eventPageSize, sseMaxFullPagesPerWake
	eventPageSize, sseMaxFullPagesPerWake = 50, 3
	t.Cleanup(func() { eventPageSize, sseMaxFullPagesPerWake = oldPage, oldCap })

	srv, store := newSSETestServer(t)
	bus := srv.events.get("", store)

	// One existing event so the client can connect at head with no replay.
	publishTestEvent(t, store, bus, "/existing.txt", "write", "")

	// Inject THREE consecutive query errors: catch-up fires 1-3 (poll calls
	// 4-6) fail; every other call goes through.
	realEventsSinceE := eventsSinceE
	var pollCalls atomic.Int32
	eventsSinceE = func(eb *EventBus, ctx context.Context, since uint64) ([]ChangeEvent, uint64, bool, error) {
		if n := pollCalls.Add(1); n >= 4 && n <= 6 {
			return nil, since, true, fmt.Errorf("injected persistent db error")
		}
		return realEventsSinceE(eb, ctx, since)
	}
	t.Cleanup(func() { eventsSinceE = realEventsSinceE })

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), tenantScopeKey, &TenantScope{TenantID: ""})
		srv.handleEvents(w, r.WithContext(ctx))
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"?since=1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	scanner := bufio.NewScanner(resp.Body)

	// First event must be the end-of-Phase-1 heartbeat (caught up at head).
	ev, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected initial heartbeat")
	}
	if ev.Event != "heartbeat" {
		t.Fatalf("first event = %q, want heartbeat", ev.Event)
	}

	// Commit a 5-page burst (page=50, cap=3) and send ONE notify signal.
	// Wake 1 drains 3 pages and arms the catch-up timer; fires 1-3 hit the
	// injected errors (backoff 10ms → 20ms → 40ms); fire 4 succeeds and
	// drains the remaining 2 pages at full speed.
	const backlog = 250
	bulkInsertFSEvents(t, store, backlog)
	bus.Publish()

	for i := 0; i < backlog; i++ {
		ev, ok := readSSEEvent(scanner)
		if !ok {
			t.Fatalf("stream ended after %d/%d events (backoff did not resume the drain)", i, backlog)
		}
		if ev.Event != "file_changed" {
			t.Fatalf("event %d = %q, want file_changed", i, ev.Event)
		}
	}
	if got := pollCalls.Load(); got < 9 {
		t.Fatalf("poll calls = %d, want >= 9 (3 initial + 3 errors + 3 drain)", got)
	}
}

// TestSSEEndpointPhase2NotifyWakeQueryErrorStartsCatchup verifies the H1
// path: a NOTIFY wake delivers a full page and its immediate inner-loop
// re-poll fails — the backlog definitively exists (a full page was just
// delivered), so the catch-up timer must arm even though the wake did not
// come from the timer. With no further notify, the tail must still drain
// once the error clears.
func TestSSEEndpointPhase2NotifyWakeQueryErrorStartsCatchup(t *testing.T) {
	oldPage, oldCap := eventPageSize, sseMaxFullPagesPerWake
	eventPageSize, sseMaxFullPagesPerWake = 50, 3
	t.Cleanup(func() { eventPageSize, sseMaxFullPagesPerWake = oldPage, oldCap })

	srv, store := newSSETestServer(t)
	bus := srv.events.get("", store)

	// One existing event so the client can connect at head with no replay.
	publishTestEvent(t, store, bus, "/existing.txt", "write", "")

	// Inject exactly ONE query error on the 2nd poll: the notify wake's first
	// poll returns a full page, its inner-loop re-poll fails.
	realEventsSinceE := eventsSinceE
	var pollCalls atomic.Int32
	eventsSinceE = func(eb *EventBus, ctx context.Context, since uint64) ([]ChangeEvent, uint64, bool, error) {
		if pollCalls.Add(1) == 2 {
			return nil, since, true, fmt.Errorf("injected mid-wake db error")
		}
		return realEventsSinceE(eb, ctx, since)
	}
	t.Cleanup(func() { eventsSinceE = realEventsSinceE })

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), tenantScopeKey, &TenantScope{TenantID: ""})
		srv.handleEvents(w, r.WithContext(ctx))
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"?since=1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	scanner := bufio.NewScanner(resp.Body)

	// First event must be the end-of-Phase-1 heartbeat (caught up at head).
	ev, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected initial heartbeat")
	}
	if ev.Event != "heartbeat" {
		t.Fatalf("first event = %q, want heartbeat", ev.Event)
	}

	// Commit a 5-page burst (page=50, cap=3) and send ONE notify signal.
	// The wake delivers page 1, fails on the page-2 poll, and must arm the
	// catch-up timer; the remaining 4 pages arrive on timer fires with NO
	// further Publish.
	const backlog = 250
	bulkInsertFSEvents(t, store, backlog)
	bus.Publish()

	for i := 0; i < backlog; i++ {
		ev, ok := readSSEEvent(scanner)
		if !ok {
			t.Fatalf("stream ended after %d/%d events (notify-wake query error did not arm the catch-up timer)", i, backlog)
		}
		if ev.Event != "file_changed" {
			t.Fatalf("event %d = %q, want file_changed", i, ev.Event)
		}
	}
	if got := pollCalls.Load(); got < 7 {
		t.Fatalf("poll calls = %d, want >= 7 (1 page + 1 error + 5 drain)", got)
	}
}

// TestSSEEndpointPhase2NotifyFirstPollErrorStartsCatchup verifies the Q4
// path: a NOTIFY wake whose FIRST poll fails (no full page delivered) still
// arms the catch-up timer, because a notify is a real signal that a durable
// row exists — unlike a liveness tick. With no further notify, the tail must
// drain via the timer once the error clears.
func TestSSEEndpointPhase2NotifyFirstPollErrorStartsCatchup(t *testing.T) {
	oldPage, oldCap := eventPageSize, sseMaxFullPagesPerWake
	eventPageSize, sseMaxFullPagesPerWake = 50, 3
	t.Cleanup(func() { eventPageSize, sseMaxFullPagesPerWake = oldPage, oldCap })

	srv, store := newSSETestServer(t)
	bus := srv.events.get("", store)

	// One existing event so the client can connect at head with no replay.
	publishTestEvent(t, store, bus, "/existing.txt", "write", "")

	// Inject exactly ONE query error on the FIRST Phase-2 poll after the
	// notify (call 1 is Phase 1, which must succeed so the connection
	// reaches the live phase).
	realEventsSinceE := eventsSinceE
	var pollCalls atomic.Int32
	eventsSinceE = func(eb *EventBus, ctx context.Context, since uint64) ([]ChangeEvent, uint64, bool, error) {
		if pollCalls.Add(1) == 2 {
			return nil, since, true, fmt.Errorf("injected first-poll db error")
		}
		return realEventsSinceE(eb, ctx, since)
	}
	t.Cleanup(func() { eventsSinceE = realEventsSinceE })

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), tenantScopeKey, &TenantScope{TenantID: ""})
		srv.handleEvents(w, r.WithContext(ctx))
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"?since=1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	scanner := bufio.NewScanner(resp.Body)

	// First event must be the end-of-Phase-1 heartbeat (caught up at head).
	ev, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected initial heartbeat")
	}
	if ev.Event != "heartbeat" {
		t.Fatalf("first event = %q, want heartbeat", ev.Event)
	}

	// Commit a 5-page burst and send ONE notify signal. The wake's first poll
	// fails; the catch-up timer must arm and drain all 5 pages with NO
	// further Publish.
	const backlog = 250
	bulkInsertFSEvents(t, store, backlog)
	bus.Publish()

	for i := 0; i < backlog; i++ {
		ev, ok := readSSEEvent(scanner)
		if !ok {
			t.Fatalf("stream ended after %d/%d events (notify first-poll error did not arm the catch-up timer)", i, backlog)
		}
		if ev.Event != "file_changed" {
			t.Fatalf("event %d = %q, want file_changed", i, ev.Event)
		}
	}
	if got := pollCalls.Load(); got < 7 {
		t.Fatalf("poll calls = %d, want >= 7 (phase1 + 1 error + 5 pages + 1 short)", got)
	}
}

// TestSSEEndpointPhase1InitialQueryErrorTerminates verifies that a transient
// (non-missing-table) query error on the FIRST Phase-1 call terminates the
// stream WITHOUT a caught-up heartbeat: the client must reconnect and resume
// from its durable cursor rather than believe it is current.
func TestSSEEndpointPhase1InitialQueryErrorTerminates(t *testing.T) {
	realEventsSinceE := eventsSinceE
	var calls atomic.Int32
	eventsSinceE = func(eb *EventBus, _ context.Context, since uint64) ([]ChangeEvent, uint64, bool, error) {
		calls.Add(1)
		return nil, since, true, fmt.Errorf("injected initial transient db error")
	}
	t.Cleanup(func() { eventsSinceE = realEventsSinceE })

	srv, store := newSSETestServer(t)
	bulkInsertFSEvents(t, store, 3) // a backlog exists; the error must not mask it

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), tenantScopeKey, &TenantScope{TenantID: ""})
		srv.handleEvents(w, r.WithContext(ctx))
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"?since=1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	// The stream must END immediately: no replay events, no reset, and above
	// all no caught-up heartbeat. Without termination the read would block
	// until the 5s ctx timeout.
	scanner := bufio.NewScanner(resp.Body)
	if ev, ok := readSSEEvent(scanner); ok {
		t.Fatalf("expected immediate termination after initial query error, got event %q (false caught-up marker?)", ev.Event)
	}
	if calls.Load() < 1 {
		t.Fatalf("eventsSinceE calls = %d, want >= 1", calls.Load())
	}
}

// TestSSEEndpointPhase1MissingTableTolerated verifies the asymmetry: a
// MISSING fs_events table (pre-migration tenant) keeps the tolerated
// caught-up fallback — the client gets its heartbeat instead of a
// reconnect-loop.
func TestSSEEndpointPhase1MissingTableTolerated(t *testing.T) {
	srv, store := newSSETestServer(t)
	if _, err := store.DB().ExecContext(context.Background(), `DROP TABLE fs_events`); err != nil {
		t.Fatal(err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), tenantScopeKey, &TenantScope{TenantID: ""})
		srv.handleEvents(w, r.WithContext(ctx))
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"?since=1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	scanner := bufio.NewScanner(resp.Body)
	ev, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected heartbeat for the missing-table (pre-migration) case")
	}
	if ev.Event != "heartbeat" {
		t.Fatalf("event = %q, want heartbeat (missing table must be tolerated)", ev.Event)
	}
}
