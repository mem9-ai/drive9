package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAdminTenantListPrintsTable(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/admin/tenants" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.URL.Query().Get("page") != "2" || r.URL.Query().Get("page_size") != "5" {
			t.Fatalf("query = %s, want page=2&page_size=5", r.URL.RawQuery)
		}
		if r.URL.Query().Get("include_quota") != "true" {
			t.Fatalf("query = %s, want include_quota=true for text output", r.URL.RawQuery)
		}
		if r.Header.Get("X-TiDBCloud-Public-Key") != "public-1" || r.Header.Get("X-TiDBCloud-Private-Key") != "private-1" {
			t.Fatalf("missing tidbcloud headers")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tenants": []map[string]any{
				{"tenant_id": "tenant-1", "status": "active", "kind": "live"},
				{"tenant_id": "tenant-2", "status": "provisioning", "kind": "live"},
			},
			"page":      2,
			"page_size": 5,
			"next_page": 3,
		})
	}))
	defer ts.Close()

	stdout, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"tenant", "list",
			"--server", ts.URL,
			"--page", "2",
			"--page-size", "5",
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
		})
	})
	if err != nil {
		t.Fatalf("Admin tenant list: %v", err)
	}
	for _, want := range []string{
		"TENANT_ID", "STATUS", "KIND", "MAX_STORAGE", "MAX_FILE_SIZE", "MAX_FILE_COUNT", "SPENDING_LIMIT", "STORAGE_USED", "RESERVED", "FILE_COUNT",
		"tenant-1", "active", "live",
		"tenant-2", "provisioning",
		"next_page=3", "page=2", "page_size=5",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout)
		}
	}
}

func TestAdminQuotaListPrintsQuotaTable(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)
	spendingLimit := int64(10000)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/admin/tenants" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.URL.Query().Get("include_quota") != "true" {
			t.Fatalf("query = %s, want include_quota=true", r.URL.RawQuery)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tenants": []map[string]any{
				{
					"tenant_id": "tenant-1",
					"status":    "active",
					"kind":      "live",
					"quota": map[string]any{
						"config": map[string]any{
							"max_storage_size":         102400,
							"max_file_size":            1024,
							"max_file_count":           0,
							"tidbcloud_spending_limit": spendingLimit,
						},
						"usage": map[string]any{
							"storage_bytes":  123,
							"reserved_bytes": 2048,
							"file_count":     4,
						},
					},
				},
			},
			"page":      1,
			"page_size": 10,
		})
	}))
	defer ts.Close()

	stdout, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"quota", "list",
			"--server", ts.URL,
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
		})
	})
	if err != nil {
		t.Fatalf("Admin quota list: %v", err)
	}
	for _, want := range []string{
		"TENANT_ID", "MAX_STORAGE", "MAX_FILE_SIZE", "MAX_FILE_COUNT", "SPENDING_LIMIT", "STORAGE_USED", "RESERVED", "FILE_COUNT",
		"tenant-1", "102400 Mi", "1024 Mi", "unlimited", "10000", "123 B", "2.0 KiB", "4",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout)
		}
	}
}

func TestAdminQuotaGetIsNotACommand(t *testing.T) {
	err := Admin([]string{"quota", "get", "--tenant-id", "tenant-1"})
	if err == nil {
		t.Fatal("Admin quota get error = nil, want unknown command")
	}
	if !strings.Contains(err.Error(), `unknown admin quota command "get"`) {
		t.Fatalf("error = %q", err)
	}
}

func TestAdminTenantGetPrintsQuota(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/admin/tenants/tenant-1" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tenant_id": "tenant-1",
			"status":    "active",
			"kind":      "live",
			"quota": map[string]any{
				"config": map[string]any{
					"max_storage_size":         102400,
					"max_file_size":            1024,
					"max_file_count":           10,
					"tidbcloud_spending_limit": 10000,
				},
				"usage": map[string]any{
					"storage_bytes":  123,
					"reserved_bytes": 0,
					"file_count":     4,
				},
			},
		})
	}))
	defer ts.Close()

	stdout, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"tenant", "get",
			"--server", ts.URL,
			"--tenant-id", "tenant-1",
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
		})
	})
	if err != nil {
		t.Fatalf("Admin tenant get: %v", err)
	}
	for _, want := range []string{
		"TENANT_ID", "STATUS", "KIND", "MAX_STORAGE", "MAX_FILE_SIZE", "MAX_FILE_COUNT", "SPENDING_LIMIT", "STORAGE_USED", "RESERVED", "FILE_COUNT",
		"tenant-1", "active", "live", "102400 Mi", "1024 Mi", "10", "10000", "123 B", "0 B", "4",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout)
		}
	}
	for _, banned := range []string{"tenant_id:", "MaxStorage:", "StorageUsed:"} {
		if strings.Contains(stdout, banned) {
			t.Fatalf("stdout should use table output, found %q:\n%s", banned, stdout)
		}
	}
}
