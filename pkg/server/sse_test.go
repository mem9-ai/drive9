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
	defer resp.Body.Close()

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
	bus.Publish("/b.txt", "delete", "actor2")
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
	defer resp.Body.Close()

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
	json.Unmarshal([]byte(ev1.Data), &data1)
	if data1.Path != "/b.txt" || data1.Op != "delete" {
		t.Errorf("event1 data: %+v", data1)
	}

	ev2, ok := readSSEEvent(scanner)
	if !ok {
		t.Fatal("expected second replayed event")
	}
	var data2 ChangeEvent
	json.Unmarshal([]byte(ev2.Data), &data2)
	if data2.Path != "/c.txt" || data2.Op != "write" {
		t.Errorf("event2 data: %+v", data2)
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
	defer resp.Body.Close()

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
	json.Unmarshal([]byte(ev.Data), &data)
	if data.Path != "/new.txt" || data.Actor != "remote-actor" {
		t.Errorf("live event data: %+v", data)
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
