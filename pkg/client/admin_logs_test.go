package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAdminGetTenantLogsSendsSelectorAndDecodesEntries(t *testing.T) {
	var gotPath, gotKeyword, gotTimeRange, gotLimit, gotPublic, gotPrivate string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotKeyword = r.URL.Query().Get("keyword")
		gotTimeRange = r.URL.Query().Get("time_range")
		gotLimit = r.URL.Query().Get("limit")
		gotPublic = r.Header.Get("X-TiDBCloud-Public-Key")
		gotPrivate = r.Header.Get("X-TiDBCloud-Private-Key")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"tenant_id": "tenant-1",
			"keyword": "boom",
			"time_range": "20m0s",
			"start": "2026-10-21T10:00:00Z",
			"end": "2026-10-21T10:20:00Z",
			"limit": 5,
			"count": 1,
			"entries": [{"timestamp": "2026-10-21T10:19:58.348755792Z", "line": "hello"}]
		}`))
	}))
	defer ts.Close()

	c := New(ts.URL, "")
	out, err := c.AdminGetTenantLogs(context.Background(), AdminTenantLogsRequest{
		TenantID:   "tenant-1",
		PublicKey:  "public-1",
		PrivateKey: "private-1",
		Keyword:    "boom",
		TimeRange:  "20m",
		Limit:      5,
	})
	if err != nil {
		t.Fatalf("AdminGetTenantLogs: %v", err)
	}
	if want := "/v1/admin/tenants/tenant-1/logs"; gotPath != want {
		t.Fatalf("path = %q, want %q", gotPath, want)
	}
	if gotKeyword != "boom" || gotTimeRange != "20m" || gotLimit != "5" {
		t.Fatalf("query = keyword %q time_range %q limit %q", gotKeyword, gotTimeRange, gotLimit)
	}
	if gotPublic != "public-1" || gotPrivate != "private-1" {
		t.Fatalf("headers = %q/%q", gotPublic, gotPrivate)
	}
	if out.Count != 1 || len(out.Entries) != 1 {
		t.Fatalf("count = %d entries = %d", out.Count, len(out.Entries))
	}
	if out.Entries[0].Line != "hello" || out.Entries[0].Timestamp.IsZero() {
		t.Fatalf("entry = %+v", out.Entries[0])
	}
}

func TestAdminGetTenantLogsOmitsEmptyOptionalParams(t *testing.T) {
	var rawQuery string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rawQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"count":0,"entries":[]}`))
	}))
	defer ts.Close()

	c := New(ts.URL, "")
	if _, err := c.AdminGetTenantLogs(context.Background(), AdminTenantLogsRequest{TenantID: "tenant-1"}); err != nil {
		t.Fatalf("AdminGetTenantLogs: %v", err)
	}
	if rawQuery != "" {
		t.Fatalf("expected no query parameters, got %q", rawQuery)
	}
}

func TestAdminGetTenantLogsReturnsStatusError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "time_range must be at most 30m", "code": "invalid_request"})
	}))
	defer ts.Close()

	c := New(ts.URL, "")
	_, err := c.AdminGetTenantLogs(context.Background(), AdminTenantLogsRequest{TenantID: "tenant-1", TimeRange: "31m"})
	if err == nil {
		t.Fatal("expected an error")
	}
	var statusErr *StatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("error type = %T, want *StatusError", err)
	}
	if statusErr.StatusCode != http.StatusBadRequest || statusErr.Message != "time_range must be at most 30m" {
		t.Fatalf("status error = %+v", statusErr)
	}
}

func TestAdminGetTenantLogsRequiresTenantID(t *testing.T) {
	c := New("http://127.0.0.1:1", "")
	if _, err := c.AdminGetTenantLogs(context.Background(), AdminTenantLogsRequest{}); err == nil {
		t.Fatal("expected an error for a missing tenant id")
	}
}
