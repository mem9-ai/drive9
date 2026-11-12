package cli

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAdminMigrationHelpDocumentsCommands(t *testing.T) {
	stdout, err := captureStdoutE(t, func() error {
		return Admin([]string{"migration", "--help"})
	})
	if err != nil {
		t.Fatalf("migration help: %v", err)
	}
	for _, want := range []string{"batch create", "start BATCH_ID", "status TENANT_ID", "list [flags]", "watch [flags]", "rollback TENANT_ID"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("migration help missing %q:\n%s", want, stdout)
		}
	}
}

func TestAdminMigrationBatchCreateReadsFileAndSendsPayload(t *testing.T) {
	home := withIsolatedHome(t)
	clearProvisionEnv(t)
	tenantFile := filepath.Join(home, "tenants.list")
	if err := os.WriteFile(tenantFile, []byte("# batch\n tnt_a \ntnt_b\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var gotMethod, gotPath, gotBody, gotPublic, gotPrivate string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		gotPublic = r.Header.Get("X-TiDBCloud-Public-Key")
		gotPrivate = r.Header.Get("X-TiDBCloud-Private-Key")
		raw, _ := io.ReadAll(r.Body)
		gotBody = string(raw)
		_, _ = w.Write([]byte(`{"batch_id":"b1","name":"customer-a","state":"prepared","tenant_count":2}`))
	}))
	defer ts.Close()

	stdout, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"migration", "batch", "create",
			"--file", tenantFile,
			"--server", ts.URL,
			"--tidbcloud-public-key", "pub",
			"--tidbcloud-private-key", "priv",
		})
	})
	if err != nil {
		t.Fatalf("batch create: %v", err)
	}
	if gotMethod != http.MethodPost || gotPath != "/v1/admin/migration/batches" {
		t.Fatalf("request = %s %s", gotMethod, gotPath)
	}
	if gotPublic != "pub" || gotPrivate != "priv" {
		t.Fatalf("credentials = %q/%q", gotPublic, gotPrivate)
	}
	if strings.Contains(gotBody, `"name"`) || !strings.Contains(gotBody, `"tenant_ids":["tnt_a","tnt_b"]`) {
		t.Fatalf("body = %s", gotBody)
	}
	for _, want := range []string{"BATCH_ID", "b1", "prepared"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout)
		}
	}
}

func TestAdminMigrationBatchCreateRejectsDuplicateTenantIDs(t *testing.T) {
	home := withIsolatedHome(t)
	clearProvisionEnv(t)
	tenantFile := filepath.Join(home, "tenants.list")
	if err := os.WriteFile(tenantFile, []byte("tnt_a\ntnt_a\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"migration", "batch", "create",
			"--file", tenantFile,
			"--server", "http://127.0.0.1:1",
			"--tidbcloud-public-key", "pub",
			"--tidbcloud-private-key", "priv",
		})
	})
	if err == nil || !strings.Contains(err.Error(), "appears more than once") {
		t.Fatalf("error = %v, want duplicate tenant error", err)
	}
}

func TestAdminMigrationStartSendsBatchInBody(t *testing.T) {
	withIsolatedHome(t)
	clearProvisionEnv(t)
	var gotPath, gotBody string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		raw, _ := io.ReadAll(r.Body)
		gotBody = string(raw)
		_, _ = w.Write([]byte(`{"batch_id":"b1","state":"syncing"}`))
	}))
	defer ts.Close()

	stdout, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"migration", "start", "b1",
			"--server", ts.URL,
			"--tidbcloud-public-key", "pub",
			"--tidbcloud-private-key", "priv",
		})
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if gotPath != "/v1/admin/migration/start" {
		t.Fatalf("path = %q", gotPath)
	}
	if !strings.Contains(gotBody, `"batch_id":"b1"`) {
		t.Fatalf("body = %s", gotBody)
	}
	if !strings.Contains(stdout, "syncing") {
		t.Fatalf("stdout = %s", stdout)
	}
}

func TestAdminMigrationStatusJSONPreservesControllerFields(t *testing.T) {
	withIsolatedHome(t)
	clearProvisionEnv(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"tenant_id":"t1","state":"switched","sync_complete":true,"blocking_reason":"UMOUNT_REQUIRED","a_mode":"read_write","b_mode":"read_only","progress":{"lag_seconds":1.5,"rows_copied":"42","objects_copied":7},"future_field":"kept"}`))
	}))
	defer ts.Close()

	stdout, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"migration", "status", "t1",
			"--server", ts.URL,
			"--tidbcloud-public-key", "pub",
			"--tidbcloud-private-key", "priv",
			"--json",
		})
	})
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(stdout), &out); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout)
	}
	if out["future_field"] != "kept" || out["state"] != "switched" {
		t.Fatalf("out = %#v", out)
	}
}

func TestAdminMigrationListSendsFiltersAndPrintsTable(t *testing.T) {
	withIsolatedHome(t)
	clearProvisionEnv(t)
	var gotQuery string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		_, _ = w.Write([]byte(`{"tenants":[{"tenant_id":"t1","batch_id":"b1","state":"switched","sync_complete":true,"a_mode":"read_only","b_mode":"read_write","progress":{"lag_seconds":0,"rows_copied":42,"objects_copied":7}}]}`))
	}))
	defer ts.Close()

	stdout, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"migration", "list",
			"--batch", "b1",
			"--state", "switched",
			"--tenant-ids", "t1",
			"--page", "2",
			"--page-size", "10",
			"--server", ts.URL,
			"--tidbcloud-public-key", "pub",
			"--tidbcloud-private-key", "priv",
		})
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	for _, want := range []string{"batch_id=b1", "state=switched", "tenant_ids=t1", "page=2", "page_size=10"} {
		if !strings.Contains(gotQuery, want) {
			t.Fatalf("query %q missing %q", gotQuery, want)
		}
	}
	for _, want := range []string{"TENANT_ID", "t1", "switched", "42", "7"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout)
		}
	}
}

func TestAdminMigrationRollbackSendsTenantInQueryAndBody(t *testing.T) {
	withIsolatedHome(t)
	clearProvisionEnv(t)
	var gotQuery, gotBody string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		raw, _ := io.ReadAll(r.Body)
		gotBody = string(raw)
		_, _ = w.Write([]byte(`{"tenant_id":"t1","state":"rolledback"}`))
	}))
	defer ts.Close()

	stdout, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"migration", "rollback", "t1",
			"--server", ts.URL,
			"--tidbcloud-public-key", "pub",
			"--tidbcloud-private-key", "priv",
		})
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
	if !strings.Contains(stdout, "rolledback") {
		t.Fatalf("stdout = %s", stdout)
	}
}

func TestAdminMigrationErrorKeepsControllerCode(t *testing.T) {
	withIsolatedHome(t)
	clearProvisionEnv(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"no active migration plan","code":"MIGRATION_NOT_ENABLED"}`))
	}))
	defer ts.Close()

	_, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"migration", "status", "t1",
			"--server", ts.URL,
			"--tidbcloud-public-key", "pub",
			"--tidbcloud-private-key", "priv",
		})
	})
	if err == nil || !strings.Contains(err.Error(), "MIGRATION_NOT_ENABLED") {
		t.Fatalf("error = %v, want controller code", err)
	}
}
