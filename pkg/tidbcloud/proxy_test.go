package tidbcloud

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCreateServiceUserViaProxy_Success(t *testing.T) {
	var gotReq proxyExecuteRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method=%s, want POST", r.Method)
		}
		if r.URL.Path != "/v1beta2/execute" {
			t.Fatalf("path=%s, want /v1beta2/execute", r.URL.Path)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Fatalf("content-type=%s, want application/json", ct)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotReq); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(proxyExecuteResponse{ErrNumber: 0})
	}))
	defer srv.Close()

	err := CreateServiceUserViaProxy(context.Background(), srv.URL, 12345,
		"pfx.cloud_admin", "admin-pass", "pfx.fs_admin", "fs-pass")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify operator.
	if gotReq.Operator == nil {
		t.Fatal("operator is nil")
	}
	if gotReq.Operator.Username != "pfx.cloud_admin" {
		t.Fatalf("operator.Username=%q, want %q", gotReq.Operator.Username, "pfx.cloud_admin")
	}
	if gotReq.Operator.AuthMethod != "password" {
		t.Fatalf("operator.AuthMethod=%q, want %q", gotReq.Operator.AuthMethod, "password")
	}
	wantCred := base64.URLEncoding.EncodeToString([]byte("admin-pass"))
	if gotReq.Operator.Credential != wantCred {
		t.Fatalf("operator.Credential=%q, want %q", gotReq.Operator.Credential, wantCred)
	}

	// Verify cluster ID.
	if gotReq.ClusterID != 12345 {
		t.Fatalf("clusterID=%d, want 12345", gotReq.ClusterID)
	}

	// Verify queries.
	if len(gotReq.Queries) != 7 {
		t.Fatalf("queries count=%d, want 7", len(gotReq.Queries))
	}
	if gotReq.Queries[0] != "CREATE DATABASE IF NOT EXISTS `_drive9_fs`" {
		t.Fatalf("queries[0]=%q", gotReq.Queries[0])
	}
	if gotReq.Queries[1] != "CREATE ROLE IF NOT EXISTS 'role_fs_admin'" {
		t.Fatalf("queries[1]=%q", gotReq.Queries[1])
	}
	if gotReq.Queries[2] != "GRANT CREATE, ALTER, DROP, INDEX, SELECT, INSERT, UPDATE, DELETE ON _drive9_fs.* TO 'role_fs_admin'" {
		t.Fatalf("queries[2]=%q", gotReq.Queries[2])
	}
	if gotReq.Queries[3] != "CREATE USER IF NOT EXISTS 'pfx.fs_admin' IDENTIFIED BY 'fs-pass'" {
		t.Fatalf("queries[3]=%q", gotReq.Queries[3])
	}
	if gotReq.Queries[4] != "ALTER USER 'pfx.fs_admin' IDENTIFIED BY 'fs-pass'" {
		t.Fatalf("queries[4]=%q", gotReq.Queries[4])
	}
	if gotReq.Queries[5] != "GRANT 'role_fs_admin' TO 'pfx.fs_admin'" {
		t.Fatalf("queries[5]=%q", gotReq.Queries[5])
	}
	if gotReq.Queries[6] != "SET DEFAULT ROLE 'role_fs_admin' TO 'pfx.fs_admin'" {
		t.Fatalf("queries[6]=%q", gotReq.Queries[6])
	}
}

func TestCreateServiceUserViaProxy_EmptyEndpoint(t *testing.T) {
	err := CreateServiceUserViaProxy(context.Background(), "", 1,
		"user", "pass", "new", "new-pass")
	if err == nil {
		t.Fatal("expected error for empty endpoint")
	}
}

func TestCreateServiceUserViaProxy_SQLError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(proxyExecuteResponse{
			ErrNumber: 1396,
			ErrMsg:    "Operation CREATE USER failed",
		})
	}))
	defer srv.Close()

	err := CreateServiceUserViaProxy(context.Background(), srv.URL, 1,
		"op", "pass", "new", "np")
	if err == nil {
		t.Fatal("expected error for SQL error response")
	}
	if got := err.Error(); !contains(got, "SQL error 1396") {
		t.Fatalf("error=%q, want SQL error 1396", got)
	}
}

func TestCreateServiceUserViaProxy_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	}))
	defer srv.Close()

	err := CreateServiceUserViaProxy(context.Background(), srv.URL, 1,
		"op", "pass", "new", "np")
	if err == nil {
		t.Fatal("expected error for HTTP 500")
	}
	if got := err.Error(); !contains(got, "proxy returned 500") {
		t.Fatalf("error=%q, want 'proxy returned 500'", got)
	}
}

func TestCreateServiceUserViaProxy_InvalidUsername(t *testing.T) {
	err := CreateServiceUserViaProxy(context.Background(), "https://proxy", 1,
		"op", "pass", "user'inject", "safe-pass")
	if err == nil {
		t.Fatal("expected error for username with single quote")
	}
	if got := err.Error(); !contains(got, "invalid username") {
		t.Fatalf("error=%q, want 'invalid username'", got)
	}
}

func TestCreateServiceUserViaProxy_InvalidPassword(t *testing.T) {
	err := CreateServiceUserViaProxy(context.Background(), "https://proxy", 1,
		"op", "pass", "safe-user", "pass'word")
	if err == nil {
		t.Fatal("expected error for password with single quote")
	}
	if got := err.Error(); !contains(got, "invalid password") {
		t.Fatalf("error=%q, want 'invalid password'", got)
	}
}

func TestGeneratePassword(t *testing.T) {
	p1 := GeneratePassword()
	p2 := GeneratePassword()
	if len(p1) != 32 {
		t.Fatalf("password length=%d, want 32", len(p1))
	}
	if p1 == p2 {
		t.Fatal("two generated passwords should not be equal")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
