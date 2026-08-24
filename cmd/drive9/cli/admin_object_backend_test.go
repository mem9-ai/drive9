package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAdminObjectBackendHelp(t *testing.T) {
	help, err := captureStdoutE(t, func() error {
		return Admin([]string{"--help"})
	})
	if err != nil {
		t.Fatalf("admin help: %v", err)
	}
	if !strings.Contains(help, "object-backend <add|ls|rm>") {
		t.Fatalf("admin help missing object-backend:\n%s", help)
	}
	if !strings.Contains(help, "object-namespace <get|set|clear>") {
		t.Fatalf("admin help missing object-namespace:\n%s", help)
	}

	stdout, err := captureStdoutE(t, func() error {
		return Admin([]string{"object-backend", "--help"})
	})
	if err != nil {
		t.Fatalf("help: %v", err)
	}
	for _, want := range []string{
		"usage: drive9 admin object-backend <add|ls|rm>",
		"--scheme",
		"--bucket",
		"--credential-kind",
		"Secrets are never printed",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("help missing %q:\n%s", want, stdout)
		}
	}
}

func TestAdminTenantObjectNamespaceHelp(t *testing.T) {
	stdout, err := captureStdoutE(t, func() error {
		return Admin([]string{"tenant", "object-namespace", "--help"})
	})
	if err != nil {
		t.Fatalf("help: %v", err)
	}
	if !strings.Contains(stdout, "usage: drive9 admin tenant object-namespace") {
		t.Fatalf("help:\n%s", stdout)
	}
}

func TestAdminObjectBackendListSendsHeaders(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	clearProvisionEnv(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/admin/object-backends" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("X-TiDBCloud-Public-Key") != "public-1" || r.Header.Get("X-TiDBCloud-Private-Key") != "private-1" {
			t.Errorf("missing TiDB Cloud headers")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"backends": []map[string]any{{"id": "obb_1", "scheme": "s3", "bucket": "b", "credential_kind": "static"}},
		})
	}))
	defer srv.Close()

	stdout, err := captureStdoutE(t, func() error {
		return Admin([]string{
			"object-backend", "ls",
			"--server", srv.URL,
			"--tidbcloud-public-key", "public-1",
			"--tidbcloud-private-key", "private-1",
		})
	})
	if err != nil {
		t.Fatalf("ls: %v", err)
	}
	if !strings.Contains(stdout, "obb_1") || !strings.Contains(stdout, "s3") {
		t.Fatalf("stdout=%s", stdout)
	}
}

func TestAdminTenantObjectNamespaceSetRejectsSlash(t *testing.T) {
	err := Admin([]string{
		"tenant", "object-namespace", "set",
		"--tenant-id", "t1",
		"--namespace-id", "a/b",
		"--tidbcloud-public-key", "public-1",
		"--tidbcloud-private-key", "private-1",
		"--server", "http://127.0.0.1:1",
	})
	if err == nil || !strings.Contains(err.Error(), "slashes") || !strings.Contains(err.Error(), "parent-directory") {
		t.Fatalf("err=%v", err)
	}
}
