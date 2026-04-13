package tidbcloud

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewClusterProxyClient_AddsHTTPS(t *testing.T) {
	c := NewClusterProxyClient("proxy.internal:8080", 1, "user", "pass")
	if !strings.HasPrefix(c.baseURL, "https://") {
		t.Fatalf("expected https prefix, got %s", c.baseURL)
	}
}

func TestNewClusterProxyClient_KeepsExistingScheme(t *testing.T) {
	c := NewClusterProxyClient("http://proxy.local", 1, "user", "pass")
	if c.baseURL != "http://proxy.local" {
		t.Fatalf("expected http://proxy.local, got %s", c.baseURL)
	}
}

func TestExecuteSQL_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != proxyExecutePath {
			t.Fatalf("expected path %s, got %s", proxyExecutePath, r.URL.Path)
		}

		var req proxyExecuteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.ClusterID != 42 {
			t.Fatalf("expected clusterID 42, got %d", req.ClusterID)
		}
		if req.Query != "SELECT 1" {
			t.Fatalf("expected query 'SELECT 1', got %q", req.Query)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(proxyExecuteResponse{})
	}))
	defer srv.Close()

	c := NewClusterProxyClient(srv.URL, 42, "admin", "pwd")
	if err := c.ExecuteSQL(context.Background(), "SELECT 1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestExecuteSQL_SQLError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(proxyExecuteResponse{
			ErrNumber:  1045,
			ErrMessage: "Access denied",
		})
	}))
	defer srv.Close()

	c := NewClusterProxyClient(srv.URL, 1, "u", "p")
	err := c.ExecuteSQL(context.Background(), "DROP TABLE x")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "1045") {
		t.Fatalf("expected error to contain 1045, got: %v", err)
	}
}

func TestExecuteSQL_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewClusterProxyClient(srv.URL, 1, "u", "p")
	err := c.ExecuteSQL(context.Background(), "SELECT 1")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Fatalf("expected HTTP 500 in error, got: %v", err)
	}
}

func TestExecSchemaStatements_IgnoresDuplicates(t *testing.T) {
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		if callCount == 2 {
			// Second statement returns "already exists"
			_ = json.NewEncoder(w).Encode(proxyExecuteResponse{
				ErrNumber:  1050,
				ErrMessage: "Table already exists",
			})
			return
		}
		_ = json.NewEncoder(w).Encode(proxyExecuteResponse{})
	}))
	defer srv.Close()

	c := NewClusterProxyClient(srv.URL, 1, "u", "p")
	err := c.ExecSchemaStatements(context.Background(), []string{
		"CREATE TABLE t1 (id INT)",
		"CREATE TABLE t2 (id INT)",
		"CREATE TABLE t3 (id INT)",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if callCount != 3 {
		t.Fatalf("expected 3 calls, got %d", callCount)
	}
}

func TestExecSchemaStatements_NonIgnorableError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(proxyExecuteResponse{
			ErrNumber:  1064,
			ErrMessage: "syntax error",
		})
	}))
	defer srv.Close()

	c := NewClusterProxyClient(srv.URL, 1, "u", "p")
	err := c.ExecSchemaStatements(context.Background(), []string{"BAD SQL"})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "syntax error") {
		t.Fatalf("expected syntax error, got: %v", err)
	}
}

func TestCreateServiceUser_Success(t *testing.T) {
	var executedStmts []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req proxyExecuteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode: %v", err)
		}
		executedStmts = append(executedStmts, req.Query)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(proxyExecuteResponse{})
	}))
	defer srv.Close()

	c := NewClusterProxyClient(srv.URL, 1, "u", "p")
	svc, err := c.CreateServiceUser(context.Background(), "2wCQ", "mydb")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if svc.Username != "2wCQ.fs_admin" {
		t.Fatalf("expected username 2wCQ.fs_admin, got %s", svc.Username)
	}
	if svc.Password == "" {
		t.Fatal("expected non-empty password")
	}
	if len(svc.Password) != 64 { // 32 bytes → 64 hex chars
		t.Fatalf("expected 64-char hex password, got len %d", len(svc.Password))
	}

	if len(executedStmts) != 3 {
		t.Fatalf("expected 3 SQL statements, got %d", len(executedStmts))
	}
	if !strings.HasPrefix(executedStmts[0], "CREATE USER IF NOT EXISTS") {
		t.Fatalf("stmt[0] should be CREATE USER, got: %s", executedStmts[0])
	}
	if !strings.HasPrefix(executedStmts[1], "ALTER USER") {
		t.Fatalf("stmt[1] should be ALTER USER, got: %s", executedStmts[1])
	}
	if !strings.HasPrefix(executedStmts[2], "GRANT") {
		t.Fatalf("stmt[2] should be GRANT, got: %s", executedStmts[2])
	}
	if !strings.Contains(executedStmts[2], "`mydb`.*") {
		t.Fatalf("GRANT should target mydb.*, got: %s", executedStmts[2])
	}
}

func TestCreateServiceUser_NoPrefix(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(proxyExecuteResponse{})
	}))
	defer srv.Close()

	c := NewClusterProxyClient(srv.URL, 1, "u", "p")
	svc, err := c.CreateServiceUser(context.Background(), "", "db")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if svc.Username != "fs_admin" {
		t.Fatalf("expected username fs_admin, got %s", svc.Username)
	}
}

func TestCreateServiceUser_ExecuteError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(proxyExecuteResponse{
			ErrNumber:  1045,
			ErrMessage: "Access denied",
		})
	}))
	defer srv.Close()

	c := NewClusterProxyClient(srv.URL, 1, "u", "p")
	_, err := c.CreateServiceUser(context.Background(), "pfx", "db")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestIsIgnorableProxyError(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{fmt.Errorf("sql err: 1050 Table already exists"), true},
		{fmt.Errorf("sql err: 1062 Duplicate key"), true},
		{fmt.Errorf("sql err: 1060 Duplicate column name"), true},
		{fmt.Errorf("sql err: 1064 syntax error"), false},
		{fmt.Errorf("connection refused"), false},
	}

	for _, tt := range tests {
		got := isIgnorableProxyError(tt.err)
		if got != tt.want {
			t.Fatalf("isIgnorableProxyError(%v) = %v, want %v", tt.err, got, tt.want)
		}
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
		{`both\'`, `both\\''`},
	}
	for _, tt := range tests {
		got := escapeSQLString(tt.input)
		if got != tt.want {
			t.Fatalf("escapeSQLString(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestEscapeSQLIdent(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"mydb", "mydb"},
		{"my`db", "my``db"},
	}
	for _, tt := range tests {
		got := escapeSQLIdent(tt.input)
		if got != tt.want {
			t.Fatalf("escapeSQLIdent(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestTruncate(t *testing.T) {
	if got := truncate("hello", 10); got != "hello" {
		t.Fatalf("expected hello, got %s", got)
	}
	if got := truncate("hello world", 5); got != "hello" {
		t.Fatalf("expected hello, got %s", got)
	}
}
