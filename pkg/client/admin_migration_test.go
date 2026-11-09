package client

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAdminCreateMigrationBatchSendsCredentialsAndPayload(t *testing.T) {
	var gotPath, gotMethod, gotBody, gotPublic, gotPrivate string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod = r.URL.Path, r.Method
		gotPublic = r.Header.Get("X-TiDBCloud-Public-Key")
		gotPrivate = r.Header.Get("X-TiDBCloud-Private-Key")
		raw, _ := io.ReadAll(r.Body)
		gotBody = string(raw)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"batch_id":"b1","state":"prepared"}`))
	}))
	defer ts.Close()

	raw, err := New(ts.URL, "").AdminCreateMigrationBatch(context.Background(), AdminMigrationBatchCreateRequest{
		Credentials: AdminMigrationCredentials{PublicKey: "pub", PrivateKey: "priv"},
		Name:        "customer-a",
		TenantIDs:   []string{"t1", "t2"},
	})
	if err != nil {
		t.Fatalf("create batch: %v", err)
	}
	if gotMethod != http.MethodPost || gotPath != "/v1/admin/migration/batches" {
		t.Fatalf("request = %s %s", gotMethod, gotPath)
	}
	if gotPublic != "pub" || gotPrivate != "priv" {
		t.Fatalf("credentials = %q/%q", gotPublic, gotPrivate)
	}
	if !strings.Contains(gotBody, `"name":"customer-a"`) || !strings.Contains(gotBody, `"tenant_ids":["t1","t2"]`) {
		t.Fatalf("body = %s", gotBody)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil || out["batch_id"] != "b1" {
		t.Fatalf("response = %s err=%v", raw, err)
	}
}

func TestAdminListMigrationTenantsSendsOnlyKnownFilters(t *testing.T) {
	var gotQuery string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		_, _ = w.Write([]byte(`{"tenants":[]}`))
	}))
	defer ts.Close()

	_, err := New(ts.URL, "").AdminListMigrationTenants(context.Background(), AdminMigrationTenantListRequest{
		Credentials: AdminMigrationCredentials{PublicKey: "pub", PrivateKey: "priv"},
		BatchID:     "b1",
		State:       "switched",
		TenantIDs:   []string{"t1", "t2"},
		Page:        2,
		PageSize:    10,
	})
	if err != nil {
		t.Fatalf("list tenants: %v", err)
	}
	for _, want := range []string{"batch_id=b1", "state=switched", "tenant_ids=t1%2Ct2", "page=2", "page_size=10"} {
		if !strings.Contains(gotQuery, want) {
			t.Fatalf("query %q missing %q", gotQuery, want)
		}
	}
}

func TestAdminRollbackMigrationKeepsTenantInQueryAndBody(t *testing.T) {
	var gotQuery, gotBody string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		raw, _ := io.ReadAll(r.Body)
		gotBody = string(raw)
		_, _ = w.Write([]byte(`{"tenant_id":"t1","state":"rolledback"}`))
	}))
	defer ts.Close()

	_, err := New(ts.URL, "").AdminRollbackMigration(context.Background(), AdminMigrationRollbackRequest{
		Credentials: AdminMigrationCredentials{PublicKey: "pub", PrivateKey: "priv"},
		TenantID:    "t1",
	})
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if gotQuery != "tenant_id=t1" {
		t.Fatalf("query = %q", gotQuery)
	}
	if !strings.Contains(gotBody, `"tenant_id":"t1"`) {
		t.Fatalf("body = %s", gotBody)
	}
}

func TestWatchAdminMigrationEventsParsesSSEAndStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		_, _ = io.WriteString(w, "event: state_changed\n")
		_, _ = io.WriteString(w, "data: {\"tenant_id\":\"t1\",\"state\":\"switched\"}\n\n")
		flusher.Flush()
		<-r.Context().Done()
	}))
	defer ts.Close()

	var events []AdminMigrationEvent
	err := New(ts.URL, "").WatchAdminMigrationEvents(ctx, AdminMigrationWatchRequest{
		Credentials: AdminMigrationCredentials{PublicKey: "pub", PrivateKey: "priv"},
		BatchID:     "b1",
	}, func(event AdminMigrationEvent) error {
		events = append(events, event)
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("watch error = %v, want context.Canceled", err)
	}
	if len(events) != 1 || events[0].Type != "state_changed" {
		t.Fatalf("events = %+v", events)
	}
	if !strings.Contains(string(events[0].Data), `"state":"switched"`) {
		t.Fatalf("event data = %s", events[0].Data)
	}
}

func TestAdminCreateMigrationBatchOmitsEmptyName(t *testing.T) {
	var gotBody string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		gotBody = string(raw)
		_, _ = w.Write([]byte(`{"batch_id":"b1"}`))
	}))
	defer ts.Close()

	if _, err := New(ts.URL, "").AdminCreateMigrationBatch(context.Background(), AdminMigrationBatchCreateRequest{
		Credentials: AdminMigrationCredentials{PublicKey: "pub", PrivateKey: "priv"},
		TenantIDs:   []string{"t1"},
	}); err != nil {
		t.Fatalf("create batch: %v", err)
	}
	if strings.Contains(gotBody, `"name"`) {
		t.Fatalf("body unexpectedly contains name: %s", gotBody)
	}
	if !strings.Contains(gotBody, `"tenant_ids":["t1"]`) {
		t.Fatalf("body = %s", gotBody)
	}
}

func TestWatchAdminMigrationEventsResumesCRLFWithLastEventID(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	calls := 0
	var lastIDs []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		call := calls
		lastIDs = append(lastIDs, r.Header.Get("Last-Event-ID"))
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		if call == 1 {
			_, _ = io.WriteString(w, "id: 7\r\nevent: state_changed\r\ndata: {\"tenant_id\":\"t1\",\"state\":\"switched\"}\r\n\r\n")
			return
		}
		_, _ = io.WriteString(w, "id: 8\r\nevent: migrated\r\ndata: {\"tenant_id\":\"t1\",\"state\":\"migrated\"}\r\n\r\n")
	}))
	defer ts.Close()
	var events []AdminMigrationEvent
	err := New(ts.URL, "").WatchAdminMigrationEvents(ctx, AdminMigrationWatchRequest{
		Credentials: AdminMigrationCredentials{PublicKey: "pub", PrivateKey: "priv"},
	}, func(event AdminMigrationEvent) error {
		events = append(events, event)
		if len(events) == 2 {
			cancel()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("watch error = %v, want context.Canceled", err)
	}
	if len(events) != 2 || events[0].Type != "state_changed" || events[1].Type != "migrated" {
		t.Fatalf("events = %+v", events)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(lastIDs) < 2 || lastIDs[1] != "7" {
		t.Fatalf("Last-Event-ID headers = %v", lastIDs)
	}
}

func TestMigrationStreamPermanentErrorsAreNotRetried(t *testing.T) {
	if isRetryableMigrationStreamError(bufio.ErrTooLong) {
		t.Fatal("oversized SSE event must be permanent")
	}
	if !isRetryableMigrationStreamError(io.ErrUnexpectedEOF) {
		t.Fatal("unexpected EOF must be retryable")
	}
}

func TestWatchAdminMigrationEventsDoesNotRetryMalformedBaseURL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := New("http://[::1", "").WatchAdminMigrationEvents(ctx, AdminMigrationWatchRequest{}, func(AdminMigrationEvent) error {
		return nil
	})
	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("watch error = %v, want permanent request error", err)
	}
}
