package fuse

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant/token"
)

func TestUnverifiedMountTokenTenantID(t *testing.T) {
	raw, err := token.IssueToken([]byte("test-secret"), "tenant-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := unverifiedMountTokenTenantID(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got != "tenant-a" {
		t.Fatalf("tenant id = %q, want tenant-a", got)
	}
	if _, err := unverifiedMountTokenTenantID("not-a-token"); err == nil {
		t.Fatal("expected malformed token to be rejected")
	}
}

func TestReadMountTokenFileRejectsAccessibleFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("secret-token\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readMountTokenFile(path); err == nil {
		t.Fatal("expected group-or-other-readable token file to be rejected")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := readMountTokenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != "secret-token" {
		t.Fatalf("token = %q, want secret-token", got)
	}
}

func TestReadMountTokenFileRejectsWritableParent(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o770); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "token")
	if err := os.WriteFile(path, []byte("secret-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readMountTokenFile(path); err == nil {
		t.Fatal("expected group-writable token parent to be rejected")
	}
}

func TestReloadMountTokenSwapsAfterSameTenantDataPlaneValidation(t *testing.T) {
	previous := issueMountToken(t, "tenant-a", 1)
	candidate := issueMountToken(t, "tenant-a", 2)
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")
	receiptFile := filepath.Join(dir, "receipt")
	writePrivateFile(t, tokenFile, candidate)

	var statusCalls atomic.Int32
	var dataCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer "+candidate {
			t.Errorf("Authorization = %q, want candidate token", got)
			return
		}
		if r.URL.Path == "/v1/status" {
			statusCalls.Add(1)
			writeCredentialStatus(w, meta.APIKeyScopeKindFS)
			return
		}
		dataCalls.Add(1)
		_, _ = w.Write([]byte(`{"entries":[]}`))
	}))
	defer server.Close()

	c := client.NewWithToken(server.URL, previous)
	var restarts atomic.Int32
	if err := reloadMountToken(context.Background(), c, server.URL, "/", tokenFile, receiptFile, "tenant-a", func() error {
		restarts.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := c.APIKey(); got != candidate {
		t.Fatalf("active token = %q, want candidate", got)
	}
	if got := restarts.Load(); got != 1 {
		t.Fatalf("watcher restarts = %d, want 1", got)
	}
	if got := statusCalls.Load(); got != 1 {
		t.Fatalf("status validation calls = %d, want 1", got)
	}
	if got := dataCalls.Load(); got != 1 {
		t.Fatalf("data-plane validation calls = %d, want 1", got)
	}
	assertTokenReceipt(t, receiptFile, candidate)
}

func TestReloadMountTokenRejectsDifferentTenantBeforeDataPlaneCall(t *testing.T) {
	previous := issueMountToken(t, "tenant-a", 1)
	candidate := issueMountToken(t, "tenant-b", 2)
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")
	writePrivateFile(t, tokenFile, candidate)

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"entries":[]}`))
	}))
	defer server.Close()

	c := client.NewWithToken(server.URL, previous)
	if err := reloadMountToken(context.Background(), c, server.URL, "/", tokenFile, filepath.Join(dir, "receipt"), "tenant-a", nil); err == nil {
		t.Fatal("expected cross-tenant replacement to fail")
	}
	if got := c.APIKey(); got != previous {
		t.Fatalf("active token changed to %q after rejected replacement", got)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("validation calls = %d, want 0", got)
	}
}

func TestReloadMountTokenRejectsOwnerCredentialBeforeDataPlaneCall(t *testing.T) {
	previous := issueMountToken(t, "tenant-a", 1)
	candidate := issueMountToken(t, "tenant-a", 2)
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")
	writePrivateFile(t, tokenFile, candidate)

	var dataCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			writeCredentialStatus(w, meta.APIKeyScopeKindOwner)
			return
		}
		dataCalls.Add(1)
		_, _ = w.Write([]byte(`{"entries":[]}`))
	}))
	defer server.Close()

	c := client.NewWithToken(server.URL, previous)
	err := reloadMountToken(context.Background(), c, server.URL, "/", tokenFile, filepath.Join(dir, "receipt"), "tenant-a", nil)
	if err == nil {
		t.Fatal("expected owner credential replacement to fail")
	}
	if got := c.APIKey(); got != previous {
		t.Fatalf("active token changed to %q after owner credential rejection", got)
	}
	if got := dataCalls.Load(); got != 0 {
		t.Fatalf("data-plane validation calls = %d, want 0", got)
	}
}

func TestReloadMountTokenRollsBackWhenReceiptCannotBePublished(t *testing.T) {
	previous := issueMountToken(t, "tenant-a", 1)
	candidate := issueMountToken(t, "tenant-a", 2)
	tokenDir := t.TempDir()
	tokenFile := filepath.Join(tokenDir, "token")
	writePrivateFile(t, tokenFile, candidate)
	receiptDir := t.TempDir()
	if err := os.Chmod(receiptDir, 0o770); err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			writeCredentialStatus(w, meta.APIKeyScopeKindFS)
			return
		}
		_, _ = w.Write([]byte(`{"entries":[]}`))
	}))
	defer server.Close()

	c := client.NewWithToken(server.URL, previous)
	var restarts atomic.Int32
	err := reloadMountToken(context.Background(), c, server.URL, "/", tokenFile, filepath.Join(receiptDir, "receipt"), "tenant-a", func() error {
		restarts.Add(1)
		return nil
	})
	if err == nil {
		t.Fatal("expected receipt publication failure")
	}
	if got := c.APIKey(); got != previous {
		t.Fatalf("active token = %q, want rollback to predecessor", got)
	}
	if got := restarts.Load(); got != 2 {
		t.Fatalf("watcher restarts = %d, want candidate plus rollback", got)
	}
}

func TestReloadMountTokenRollsBackWhenCacheBarrierFails(t *testing.T) {
	previous := issueMountToken(t, "tenant-a", 1)
	candidate := issueMountToken(t, "tenant-a", 2)
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")
	receiptFile := filepath.Join(dir, "receipt")
	writePrivateFile(t, tokenFile, candidate)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			writeCredentialStatus(w, meta.APIKeyScopeKindFS)
			return
		}
		_, _ = w.Write([]byte(`{"entries":[]}`))
	}))
	defer server.Close()

	c := client.NewWithToken(server.URL, previous)
	var calls atomic.Int32
	err := reloadMountToken(context.Background(), c, server.URL, "/", tokenFile, receiptFile, "tenant-a", func() error {
		if calls.Add(1) == 1 {
			return errors.New("cache reset failed")
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected cache barrier failure")
	}
	if got := c.APIKey(); got != previous {
		t.Fatalf("active token = %q, want rollback to predecessor", got)
	}
	if _, statErr := os.Stat(receiptFile); !os.IsNotExist(statErr) {
		t.Fatalf("receipt published before cache barrier: %v", statErr)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("cache barrier calls = %d, want candidate plus rollback", got)
	}
}

func TestMountRejectsIdenticalTokenAndReceiptPaths(t *testing.T) {
	secretPath := filepath.Join(t.TempDir(), "credential")
	err := Mount(&MountOptions{
		MountPoint:             t.TempDir(),
		TokenFile:              secretPath,
		TokenReloadReceiptFile: secretPath,
	})
	if err == nil || !strings.Contains(err.Error(), "must be different paths") {
		t.Fatalf("Mount error = %v, want identical-path rejection", err)
	}
}

func writeCredentialStatus(w http.ResponseWriter, scopeKind meta.APIKeyScopeKind) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(w, `{"status":"active","scope_kind":%q,"max_upload_bytes":1}`, scopeKind)
}

func issueMountToken(t *testing.T, tenantID string, version int) string {
	t.Helper()
	raw, err := token.IssueToken([]byte("test-secret"), tenantID, version)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func writePrivateFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func assertTokenReceipt(t *testing.T, path, rawToken string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256([]byte(rawToken))
	if got := string(raw); got != fmt.Sprintf("%x\n", want) {
		t.Fatalf("receipt = %q, want token hash", got)
	}
}
