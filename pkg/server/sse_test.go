package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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

func TestSSEEndpointSince0SendsReset(t *testing.T) {
	srv := &Server{events: newEventBuses()}
	bus := srv.events.get("")

	bus.Publish("/existing.txt", "write", "")

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
	srv := &Server{events: newEventBuses()}
	bus := srv.events.get("")

	bus.Publish("/a.txt", "write", "actor1")
	bus.Publish("/b.txt", "write", "actor2")
	bus.Publish("/c.txt", "write", "actor1")

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

func TestSSEEndpointPersistentReplayAfterMemoryBusReset(t *testing.T) {
	srv := newTestServer(t)
	store := srv.fallback.Store()
	ctx := context.Background()
	ev1, err := store.InsertFSEvent(ctx, "/persist-a.txt", "write", "actor1")
	if err != nil {
		t.Fatal(err)
	}
	ev2, err := store.InsertFSEvent(ctx, "/persist-b.txt", "chmod", "actor2")
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a server restart: persistent events remain, memory bus is empty.
	srv.events = newEventBuses()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scope := &TenantScope{TenantID: "", Backend: srv.fallback}
		srv.handleEvents(w, r.WithContext(withScope(r.Context(), scope)))
	}))
	defer ts.Close()

	reqCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(reqCtx, "GET", fmt.Sprintf("%s?since=%d", ts.URL, ev1.Seq), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	scanner := bufio.NewScanner(resp.Body)
	got, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected replayed persistent event")
	}
	if got.Event != "file_changed" {
		t.Fatalf("event=%q, want file_changed", got.Event)
	}
	var data ChangeEvent
	if err := json.Unmarshal([]byte(got.Data), &data); err != nil {
		t.Fatal(err)
	}
	if data.Seq != ev2.Seq || data.Path != "/persist-b.txt" || data.Op != "chmod" || data.Actor != "actor2" {
		t.Fatalf("persistent replay event = %+v, want chmod seq %d", data, ev2.Seq)
	}
}

func TestSSERetentionSweeperPrunesOutsideWindow(t *testing.T) {
	srv := newTestServer(t)
	store := srv.fallback.Store()
	ctx := context.Background()

	ev1, err := store.InsertFSEvent(ctx, "/old.txt", "write", "")
	if err != nil {
		t.Fatal(err)
	}
	ev2, err := store.InsertFSEvent(ctx, "/keep-a.txt", "write", "")
	if err != nil {
		t.Fatal(err)
	}
	ev3, err := store.InsertFSEvent(ctx, "/keep-b.txt", "chmod", "")
	if err != nil {
		t.Fatal(err)
	}

	sweeper := newSSERetentionSweeper()
	sweeper.retention = 2
	sweeper.sweepHead(ctx, store, ev3.Seq)

	events, err := store.ListFSEventsSince(ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("events len=%d, want 2: %+v", len(events), events)
	}
	if events[0].Seq != ev2.Seq || events[1].Seq != ev3.Seq {
		t.Fatalf("kept seqs = [%d %d], want [%d %d]", events[0].Seq, events[1].Seq, ev2.Seq, ev3.Seq)
	}
	for _, ev := range events {
		if ev.Seq == ev1.Seq {
			t.Fatalf("old event was not pruned: %+v", ev)
		}
	}
}

func TestHandleChmodPublishesPersistentSSEEvent(t *testing.T) {
	srv := newTestServer(t)
	ctx := context.Background()
	if err := srv.fallback.CreateCtx(ctx, "/chmod.txt"); err != nil {
		t.Fatal(err)
	}

	body := bytes.NewBufferString(`{"mode":384}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/fs/chmod.txt?chmod=1", body)
	req = req.WithContext(withScope(req.Context(), &TenantScope{TenantID: "", Backend: srv.fallback}))
	rr := httptest.NewRecorder()
	srv.handleChmod(rr, req, "/chmod.txt")
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}

	events, err := srv.fallback.Store().ListFSEventsSince(ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("events len=%d, want 1: %+v", len(events), events)
	}
	if got := events[0]; got.Path != "/chmod.txt" || got.Op != "chmod" {
		t.Fatalf("event=%+v, want chmod event for /chmod.txt", got)
	}
	nf, err := srv.fallback.Store().Stat(ctx, "/chmod.txt")
	if err != nil {
		t.Fatal(err)
	}
	if nf.File == nil || nf.File.Revision != 2 {
		t.Fatalf("revision=%v, want 2 after chmod", nf.File)
	}
}

func TestSSEEndpointLiveEvent(t *testing.T) {
	srv := &Server{events: newEventBuses()}
	bus := srv.events.get("")

	// Pre-publish one event so since=1 is valid.
	bus.Publish("/existing.txt", "write", "")

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

	bus.Publish("/new.txt", "write", "remote-actor")

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
	srv := &Server{events: newEventBuses()}
	bus := srv.events.get("")

	// Publish a mix: write (file_changed) then rename (structural → reset).
	bus.Publish("/a.txt", "write", "actor1")
	bus.Publish("/old.txt", "rename", "actor1")
	bus.Publish("/dir", "mkdir", "actor1")
	bus.Publish("/gone.txt", "delete", "actor1")

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
	srv := &Server{events: newEventBuses()}
	bus := srv.events.get("")

	bus.Publish("/existing.txt", "write", "")

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

	bus.Publish("/old", "rename", "remote-actor")

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

func TestSSEForceResetSignalLiveEmitsReset(t *testing.T) {
	srv := &Server{events: newEventBuses()}
	bus := srv.events.get("")
	bus.PublishEvent(ChangeEvent{Seq: 5, Path: "/seed.txt", Op: "write", Ts: 1})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), tenantScopeKey, &TenantScope{TenantID: ""})
		srv.handleEvents(w, r.WithContext(ctx))
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"?since=5", nil)
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

	bus.PublishEvent(ChangeEvent{Seq: 7, Path: "/gap.txt", Op: "write", Ts: 1})

	ev, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected live reset event")
	}
	if ev.Event != "reset" {
		t.Fatalf("event=%q, want reset", ev.Event)
	}
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(ev.Data), &data); err != nil {
		t.Fatalf("unmarshal force reset: %v", err)
	}
	if data["reason"] != "seq_too_old" {
		t.Fatalf("reason=%v, want seq_too_old", data["reason"])
	}
	if data["seq"] != float64(6) {
		t.Fatalf("seq=%v, want 6", data["seq"])
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
	srv := &Server{events: newEventBuses()}
	bus := srv.events.get("")

	// Pre-publish one event so since=1 is valid.
	bus.Publish("/existing.txt", "write", "")

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

	// Publish a burst of 3 events concurrently.
	go func() {
		time.Sleep(50 * time.Millisecond)
		bus.Publish("/a.txt", "write", "actor1")
		bus.Publish("/b.txt", "write", "actor2")
		bus.Publish("/c.txt", "write", "actor3")
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

// TestSSEResetFlushWhenSeqTooOld verifies that a reset caused by seq_too_old
// is flushed immediately and not buffered until the next heartbeat.
func TestSSEResetFlushWhenSeqTooOld(t *testing.T) {
	srv := &Server{events: newEventBuses()}
	bus := srv.events.get("")

	// Publish one event so the ring has content.
	bus.Publish("/a.txt", "write", "actor1")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), tenantScopeKey, &TenantScope{TenantID: ""})
		srv.handleEvents(w, r.WithContext(ctx))
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Publish enough events to wrap the ring buffer (eventBusRingSize = 10000).
	// We publish 10005 events, then request since=5. The ring will have wrapped
	// and seq=5 is too old, triggering seq_too_old.
	for i := 2; i <= 10005; i++ {
		bus.Publish(fmt.Sprintf("/file%d.txt", i), "write", "actor1")
	}

	// Connect with since=4 (older than the oldest retained event).
	// After publishing 10005 events, oldestSeq = 6, so since+1 = 5 < 6 triggers seq_too_old.
	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"?since=4", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	scanner := bufio.NewScanner(resp.Body)

	// First event must be the reset (not buffered).
	ev, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected reset event immediately")
	}
	if ev.Event != "reset" {
		t.Fatalf("expected reset, got %q", ev.Event)
	}

	var data map[string]interface{}
	if err := json.Unmarshal([]byte(ev.Data), &data); err != nil {
		t.Fatalf("unmarshal reset: %v", err)
	}
	if data["reason"] != "seq_too_old" {
		t.Fatalf("expected reason seq_too_old, got %v", data["reason"])
	}
}
