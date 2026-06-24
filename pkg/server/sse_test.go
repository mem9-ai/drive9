package server

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testmysql"
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
	testmysql.ResetDB(t, store.DB())
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
	if _, err := store.DeleteFSEventsBefore(context.Background(), time.Now().Add(time.Hour)); err != nil {
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
