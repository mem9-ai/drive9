package tidbcloud

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestExecuteSQL_Success(t *testing.T) {
	var gotReq proxyExecuteRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != proxyExecutePath {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotReq); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"errNumber":0,"errMessage":""}`))
	}))
	defer srv.Close()

	c := &ClusterProxyClient{
		baseURL:   srv.URL,
		clusterID: 12345,
		username:  "admin",
		password:  "secret",
		client:    srv.Client(),
	}

	if err := c.ExecuteSQL(context.Background(), "SELECT 1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotReq.ClusterID != 12345 {
		t.Fatalf("got cluster ID %d, want 12345", gotReq.ClusterID)
	}
	if gotReq.Query != "SELECT 1" {
		t.Fatalf("got query %q, want %q", gotReq.Query, "SELECT 1")
	}
	if gotReq.Operator.Username != "admin" {
		t.Fatalf("got operator username %q, want %q", gotReq.Operator.Username, "admin")
	}
	if gotReq.Operator.AuthMethod != proxyAuthMethod {
		t.Fatalf("got auth method %q, want %q", gotReq.Operator.AuthMethod, proxyAuthMethod)
	}
}

func TestExecuteSQL_SQLError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"errNumber":8121,"errMessage":"privilege check for 'Grant Option' fail"}`))
	}))
	defer srv.Close()

	c := &ClusterProxyClient{
		baseURL:   srv.URL,
		clusterID: 1,
		username:  "u",
		password:  "p",
		client:    srv.Client(),
	}

	err := c.ExecuteSQL(context.Background(), "GRANT SELECT ON *.* TO 'x'@'%'")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "8121") {
		t.Fatalf("expected error to contain 8121, got: %v", err)
	}
}

func TestExecuteSQL_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`internal server error`))
	}))
	defer srv.Close()

	c := &ClusterProxyClient{
		baseURL:   srv.URL,
		clusterID: 1,
		username:  "u",
		password:  "p",
		client:    srv.Client(),
	}

	err := c.ExecuteSQL(context.Background(), "SELECT 1")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Fatalf("expected error to contain HTTP 500, got: %v", err)
	}
}

func TestCreateServiceUser_Success(t *testing.T) {
	var stmts []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req proxyExecuteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		stmts = append(stmts, req.Query)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"errNumber":0,"errMessage":""}`))
	}))
	defer srv.Close()

	c := &ClusterProxyClient{
		baseURL:   srv.URL,
		clusterID: 99,
		username:  "pfx.cloud_admin",
		password:  "pwd",
		client:    srv.Client(),
	}

	svc, err := c.CreateServiceUser(context.Background(), "pfx")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if svc.Username != "pfx.fs_admin" {
		t.Fatalf("got username %q, want %q", svc.Username, "pfx.fs_admin")
	}
	if svc.Password == "" {
		t.Fatal("expected non-empty password")
	}
	if len(svc.Password) != 64 { // 32 bytes → 64 hex chars
		t.Fatalf("expected 64-char hex password, got len %d", len(svc.Password))
	}

	if len(stmts) != 4 {
		t.Fatalf("expected 4 SQL statements, got %d", len(stmts))
	}
	// Verify expected SQL sequence.
	if !strings.HasPrefix(stmts[0], "CREATE USER IF NOT EXISTS") {
		t.Fatalf("stmt[0] should be CREATE USER, got: %s", stmts[0])
	}
	if !strings.HasPrefix(stmts[1], "ALTER USER") {
		t.Fatalf("stmt[1] should be ALTER USER, got: %s", stmts[1])
	}
	if !strings.HasPrefix(stmts[2], "GRANT 'role_admin'") {
		t.Fatalf("stmt[2] should be GRANT role_admin, got: %s", stmts[2])
	}
	if !strings.HasPrefix(stmts[3], "SET DEFAULT ROLE") {
		t.Fatalf("stmt[3] should be SET DEFAULT ROLE, got: %s", stmts[3])
	}
}

func TestCreateServiceUser_NoPrefix(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"errNumber":0,"errMessage":""}`))
	}))
	defer srv.Close()

	c := &ClusterProxyClient{
		baseURL:   srv.URL,
		clusterID: 1,
		username:  "cloud_admin",
		password:  "p",
		client:    srv.Client(),
	}

	svc, err := c.CreateServiceUser(context.Background(), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if svc.Username != "fs_admin" {
		t.Fatalf("got username %q, want %q", svc.Username, "fs_admin")
	}
}

func TestCreateServiceUser_SQLFailure_IncludesStep(t *testing.T) {
	// Phase 1 failure (CREATE USER) must still be fatal.
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		if callCount == 1 { // fail on CREATE USER (1st statement)
			_, _ = w.Write([]byte(`{"errNumber":1396,"errMessage":"create user failed"}`))
			return
		}
		_, _ = w.Write([]byte(`{"errNumber":0,"errMessage":""}`))
	}))
	defer srv.Close()

	c := &ClusterProxyClient{
		baseURL:   srv.URL,
		clusterID: 1,
		username:  "u",
		password:  "p",
		client:    srv.Client(),
	}

	_, err := c.CreateServiceUser(context.Background(), "pfx")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "create user") {
		t.Fatalf("expected error to mention step 'create user', got: %v", err)
	}
}

func TestCreateServiceUser_GrantFailure_BestEffort(t *testing.T) {
	// Phase 2 failure (GRANT role_admin) should be non-fatal.
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		if callCount == 3 { // fail on GRANT (3rd statement)
			_, _ = w.Write([]byte(`{"errNumber":1227,"errMessage":"Access denied; need ROLE_ADMIN"}`))
			return
		}
		_, _ = w.Write([]byte(`{"errNumber":0,"errMessage":""}`))
	}))
	defer srv.Close()

	c := &ClusterProxyClient{
		baseURL:   srv.URL,
		clusterID: 1,
		username:  "u",
		password:  "p",
		client:    srv.Client(),
	}

	svc, err := c.CreateServiceUser(context.Background(), "pfx")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if svc.Username != "pfx.fs_admin" {
		t.Fatalf("got username %q, want %q", svc.Username, "pfx.fs_admin")
	}
	// GRANT failed at call 3, SET DEFAULT ROLE (call 4) should be skipped.
	if callCount != 3 {
		t.Fatalf("expected 3 proxy calls (create + alter + grant), got %d", callCount)
	}
}

func TestCreateServiceUser_GrantUnexpectedError_Fatal(t *testing.T) {
	// Phase 2 failure with a non-privilege error (e.g. syntax error) should be fatal.
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		if callCount == 3 { // fail on GRANT with unexpected error
			_, _ = w.Write([]byte(`{"errNumber":1064,"errMessage":"syntax error"}`))
			return
		}
		_, _ = w.Write([]byte(`{"errNumber":0,"errMessage":""}`))
	}))
	defer srv.Close()

	c := &ClusterProxyClient{
		baseURL:   srv.URL,
		clusterID: 1,
		username:  "u",
		password:  "p",
		client:    srv.Client(),
	}

	_, err := c.CreateServiceUser(context.Background(), "pfx")
	if err == nil {
		t.Fatal("expected error for non-privilege grant failure")
	}
	if !strings.Contains(err.Error(), "grant role_admin") {
		t.Fatalf("expected error to mention step 'grant role_admin', got: %v", err)
	}
}

func TestEscapeSQLString(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"hello", "hello"},
		{"it's", "it''s"},
		{`back\slash`, `back\\slash`},
		{"a'b\\c", "a''b\\\\c"},
	}
	for _, tt := range tests {
		got := escapeSQLString(tt.input)
		if got != tt.want {
			t.Errorf("escapeSQLString(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
