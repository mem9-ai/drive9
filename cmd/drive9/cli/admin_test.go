package cli

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAdminTenantHelpIncludesCommandsFlagsAndExamples(t *testing.T) {
	stdout, err := captureStdoutE(t, func() error {
		return Admin([]string{"tenant", "--help"})
	})
	if err != nil {
		t.Fatalf("Admin tenant help: %v", err)
	}
	for _, want := range []string{
		"usage: drive9 admin tenant <command> [arguments]",
		"commands:",
		"create [flags]",
		"list [flags]",
		"get --tenant-id ID",
		"delete --tenant-id ID",
		"set-quota --tenant-id ID",
		"--server URL",
		"--region-code CODE",
		"--tidbcloud-public-key KEY",
		"--tidbcloud-private-key KEY",
		"examples:",
		"drive9 admin tenant create",
		"drive9 admin tenant delete",
		"drive9 admin tenant set-quota",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout)
		}
	}
}

func TestAdminTenantCreatePrintsTable(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)
	spendingLimit := int64(10000)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/admin/tenants" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["public_key"] != "public-1" || body["private_key"] != "private-1" {
			t.Fatalf("body credentials = %#v", body)
		}
		if body["max_storage_size"] != float64(102400) || body["tidbcloud_spending_limit"] != float64(spendingLimit) {
			t.Fatalf("body quota = %#v", body)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tenant_id":      "tenant-1",
			"api_key":        "drive9_owner_key",
			"status":         "provisioning",
			"cloud_provider": "aws",
			"region":         "ap-southeast-1",
		})
	}))
	defer ts.Close()

	stdout, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"tenant", "create",
			"--server", ts.URL,
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
			"--max-storage-size", "102400",
			"--tidbcloud-spending-limit", "10000",
		})
	})
	if err != nil {
		t.Fatalf("Admin tenant create: %v", err)
	}
	for _, want := range []string{
		"TENANT_ID", "STATUS", "CLOUD_PROVIDER", "REGION", "API_KEY",
		"tenant-1", "provisioning", "aws", "ap-southeast-1", "drive9_owner_key",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout)
		}
	}
	for _, banned := range []string{"tenant_id:", "api_key:"} {
		if strings.Contains(stdout, banned) {
			t.Fatalf("stdout should use table output, found %q:\n%s", banned, stdout)
		}
	}
}

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

func TestAdminTenantListIncludeQuotaPrintsQuotaTable(t *testing.T) {
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
			"tenant", "list",
			"--server", ts.URL,
			"--include-quota",
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
		})
	})
	if err != nil {
		t.Fatalf("Admin tenant list --include-quota: %v", err)
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

func TestAdminQuotaNamespaceIsNotACommand(t *testing.T) {
	err := Admin([]string{"quota", "get", "--tenant-id", "tenant-1"})
	if err == nil {
		t.Fatal("Admin quota error = nil, want unknown command")
	}
	if !strings.Contains(err.Error(), `unknown admin command "quota"`) {
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

func TestAdminTenantDeletePrintsTable(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/v1/admin/tenants/tenant-1" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(raw), `"public_key":"public-1"`) || !strings.Contains(string(raw), `"private_key":"private-1"`) {
			t.Fatalf("body = %s, want tidbcloud credentials", raw)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tenant_id": "tenant-1",
			"status":    "deleting",
		})
	}))
	defer ts.Close()

	stdout, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"tenant", "delete",
			"--server", ts.URL,
			"--tenant-id", "tenant-1",
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
		})
	})
	if err != nil {
		t.Fatalf("Admin tenant delete: %v", err)
	}
	for _, want := range []string{"TENANT_ID", "STATUS", "tenant-1", "deleting"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout)
		}
	}
	if strings.Contains(stdout, "delete status:") {
		t.Fatalf("stdout should use table output:\n%s", stdout)
	}
}
