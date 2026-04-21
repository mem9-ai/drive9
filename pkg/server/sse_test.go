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

func TestSSECreateReplayEmitsFileChanged(t *testing.T) {
	srv := &Server{events: newEventBuses()}
	bus := srv.events.get("")

	bus.Publish("/existing.txt", "write", "actor1")
	bus.Publish("/new.txt", "create", "actor2")

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
	ev, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected replayed create event")
	}
	if ev.Event != "file_changed" {
		t.Fatalf("event=%q, want file_changed", ev.Event)
	}
	var data ChangeEvent
	if err := json.Unmarshal([]byte(ev.Data), &data); err != nil {
		t.Fatalf("unmarshal create event: %v", err)
	}
	if data.Path != "/new.txt" || data.Op != "create" {
		t.Fatalf("unexpected create replay payload: %+v", data)
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

	// Publish a new event after connection is established.
	go func() {
		time.Sleep(100 * time.Millisecond)
		bus.Publish("/new.txt", "write", "remote-actor")
	}()

	scanner := bufio.NewScanner(resp.Body)
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

func TestSSECreateLiveEmitsFileChanged(t *testing.T) {
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

	go func() {
		time.Sleep(100 * time.Millisecond)
		bus.Publish("/brand-new.txt", "create", "remote-actor")
	}()

	scanner := bufio.NewScanner(resp.Body)
	ev, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected live create event")
	}
	if ev.Event != "file_changed" {
		t.Fatalf("event=%q, want file_changed", ev.Event)
	}
	var data ChangeEvent
	if err := json.Unmarshal([]byte(ev.Data), &data); err != nil {
		t.Fatalf("unmarshal live create event: %v", err)
	}
	if data.Path != "/brand-new.txt" || data.Op != "create" || data.Actor != "remote-actor" {
		t.Fatalf("unexpected live create payload: %+v", data)
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

	// Event 3: mkdir → also reset.
	ev2, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected second event")
	}
	if ev2.Event != "reset" {
		t.Fatalf("mkdir op: event=%q, want reset", ev2.Event)
	}

	// Event 4: delete → also reset.
	ev3, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected third event")
	}
	if ev3.Event != "reset" {
		t.Fatalf("delete op: event=%q, want reset", ev3.Event)
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

	// Publish a structural op live.
	go func() {
		time.Sleep(100 * time.Millisecond)
		bus.Publish("/old", "rename", "remote-actor")
	}()

	scanner := bufio.NewScanner(resp.Body)
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
