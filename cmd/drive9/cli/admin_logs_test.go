package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAdminTenantLogsHelpDocumentsCommandAndFlags(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	stdout, err := captureStdoutE(t, func() error {
		return Admin([]string{"--help"})
	})
	if err != nil {
		t.Fatalf("Admin help: %v", err)
	}
	if !strings.Contains(stdout, "tenant logs --tenant-id ID") {
		t.Fatalf("admin help missing the logs command:\n%s", stdout)
	}

	stdout, err = captureStdoutE(t, func() error {
		return Admin([]string{"tenant", "logs", "--help"})
	})
	if err != nil {
		t.Fatalf("logs help: %v", err)
	}
	for _, want := range []string{
		"usage: drive9 admin tenant logs",
		"--tenant-id ID",
		"--keyword TEXT",
		"--time-range DURATION",
		"--limit N",
		"--json",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("logs help missing %q:\n%s", want, stdout)
		}
	}
}

func TestAdminTenantLogsPrintsEntries(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	var gotKeyword, gotTimeRange, gotLimit, gotPublic, gotPrivate, gotPath string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotKeyword = r.URL.Query().Get("keyword")
		gotTimeRange = r.URL.Query().Get("time_range")
		gotLimit = r.URL.Query().Get("limit")
		gotPublic = r.Header.Get("X-TiDBCloud-Public-Key")
		gotPrivate = r.Header.Get("X-TiDBCloud-Private-Key")
		_, _ = w.Write([]byte(`{
			"tenant_id": "tenant-1",
			"keyword": "boom",
			"time_range": "20m0s",
			"start": "2026-10-21T10:00:00Z",
			"end": "2026-10-21T10:20:00Z",
			"limit": 5,
			"count": 2,
			"entries": [
				{"timestamp": "2026-10-21T10:19:58.348755792Z", "line": "first line"},
				{"timestamp": "2026-10-21T10:19:50Z", "line": "second line"}
			]
		}`))
	}))
	defer ts.Close()

	stdout, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"tenant", "logs",
			"--server", ts.URL,
			"--tenant-id", "tenant-1",
			"--keyword", "boom",
			"--time-range", "20m",
			"--limit", "5",
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
		})
	})
	if err != nil {
		t.Fatalf("Admin tenant logs: %v", err)
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
	for _, want := range []string{"TIMESTAMP", "LINE", "first line", "second line", "2026-10-21T10:19:58.348755792Z"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout)
		}
	}
}

func TestAdminTenantLogsJSONOutput(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"tenant_id":"tenant-1","time_range":"5m0s","count":1,"entries":[{"timestamp":"2026-10-21T10:19:58Z","line":"hello"}]}`))
	}))
	defer ts.Close()

	stdout, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"tenant", "logs",
			"--server", ts.URL,
			"--tenant-id", "tenant-1",
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
			"--json",
		})
	})
	if err != nil {
		t.Fatalf("Admin tenant logs --json: %v", err)
	}
	var out struct {
		TenantID string `json:"tenant_id"`
		Count    int    `json:"count"`
		Entries  []struct {
			Line string `json:"line"`
		} `json:"entries"`
	}
	if err := json.Unmarshal([]byte(stdout), &out); err != nil {
		t.Fatalf("decode JSON output: %v\n%s", err, stdout)
	}
	if out.TenantID != "tenant-1" || out.Count != 1 || len(out.Entries) != 1 || out.Entries[0].Line != "hello" {
		t.Fatalf("unexpected JSON output: %+v", out)
	}
}

func TestAdminTenantLogsEmptyResultPrintsNothing(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"tenant_id":"tenant-1","time_range":"5m0s","count":0,"entries":[]}`))
	}))
	defer ts.Close()

	stdout, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"tenant", "logs",
			"--server", ts.URL,
			"--tenant-id", "tenant-1",
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
		})
	})
	if err != nil {
		t.Fatalf("Admin tenant logs: %v", err)
	}
	if strings.TrimSpace(stdout) != "" {
		t.Fatalf("stdout = %q, want empty", stdout)
	}
}

func TestAdminTenantLogsSurfacesAPIError(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "tenant not found", "code": "not_found"})
	}))
	defer ts.Close()

	_, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"tenant", "logs",
			"--server", ts.URL,
			"--tenant-id", "tenant-1",
			"--time-range", "10m",
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
		})
	})
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"HTTP 404", "tenant not found"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %q, want it to contain %q", err, want)
		}
	}
}

func TestAdminTenantLogsRejectsBadFlags(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	for _, tc := range []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "zero limit", args: []string{"--tenant-id", "t", "--limit", "0"}, wantErr: "--limit must be a positive integer"},
		{name: "oversized limit", args: []string{"--tenant-id", "t", "--limit", "1001"}, wantErr: "--limit must be at most 1000"},
		{name: "bad duration", args: []string{"--tenant-id", "t", "--time-range", "20"}, wantErr: "--time-range"},
		{name: "zero duration", args: []string{"--tenant-id", "t", "--time-range", "0s"}, wantErr: "--time-range must be positive"},
		{name: "negative duration", args: []string{"--tenant-id", "t", "--time-range", "-5m"}, wantErr: "--time-range must be positive"},
		{name: "oversized window", args: []string{"--tenant-id", "t", "--time-range", "31m"}, wantErr: "--time-range must be at most 30m0s"},
		{name: "unknown flag", args: []string{"--tenant-id", "t", "--nope"}, wantErr: "unknown flag"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := captureStdoutE(t, func() error {
				return Admin(append([]string{"tenant", "logs"}, tc.args...))
			})
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}
