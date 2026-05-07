package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/tenant/token"
)

func createManagedAPIKey(t *testing.T, baseURL, ownerToken, keyName string) string {
	t.Helper()
	body, err := json.Marshal(map[string]string{"key_name": keyName})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, baseURL+tenantAPIKeysPath, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+ownerToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create api key status=%d", resp.StatusCode)
	}
	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out["key_name"] != keyName {
		t.Fatalf("key_name=%q, want %q", out["key_name"], keyName)
	}
	if out["api_key"] == "" {
		t.Fatal("empty api_key")
	}
	return out["api_key"]
}

func doJSONRequest(t *testing.T, method, url, bearer string, body io.Reader) (*http.Response, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return resp, out
}

func TestDefaultAPIKeyCanCreateAndDeleteOtherKeys(t *testing.T) {
	srv, ownerToken, cleanup := newAuthServer(t)
	defer cleanup()
	ts := httptest.NewServer(srv)
	defer ts.Close()

	workerToken := createManagedAPIKey(t, ts.URL, ownerToken, "worker")

	statusReq, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/status", nil)
	if err != nil {
		t.Fatal(err)
	}
	statusReq.Header.Set("Authorization", "Bearer "+workerToken)
	statusResp, err := http.DefaultClient.Do(statusReq)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = statusResp.Body.Close() }()
	if statusResp.StatusCode != http.StatusOK {
		t.Fatalf("worker status=%d, want %d", statusResp.StatusCode, http.StatusOK)
	}

	deleteReq, err := http.NewRequest(http.MethodDelete, ts.URL+tenantAPIKeysPath+"/worker", nil)
	if err != nil {
		t.Fatal(err)
	}
	deleteReq.Header.Set("Authorization", "Bearer "+ownerToken)
	deleteResp, err := http.DefaultClient.Do(deleteReq)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = deleteResp.Body.Close() }()
	if deleteResp.StatusCode != http.StatusOK {
		t.Fatalf("delete status=%d, want %d", deleteResp.StatusCode, http.StatusOK)
	}

	revokedReq, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/status", nil)
	if err != nil {
		t.Fatal(err)
	}
	revokedReq.Header.Set("Authorization", "Bearer "+workerToken)
	revokedResp, err := http.DefaultClient.Do(revokedReq)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = revokedResp.Body.Close() }()
	if revokedResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("revoked key status=%d, want %d", revokedResp.StatusCode, http.StatusUnauthorized)
	}
}

func TestDefaultAPIKeyCanListAndGetKeys(t *testing.T) {
	srv, ownerToken, cleanup := newAuthServer(t)
	defer cleanup()
	ts := httptest.NewServer(srv)
	defer ts.Close()

	workerToken := createManagedAPIKey(t, ts.URL, ownerToken, "worker")

	listResp, listBody := doJSONRequest(t, http.MethodGet, ts.URL+tenantAPIKeysPath, ownerToken, nil)
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("list status=%d, want %d", listResp.StatusCode, http.StatusOK)
	}
	rawKeys, ok := listBody["keys"].([]any)
	if !ok {
		t.Fatalf("keys type=%T, want []any", listBody["keys"])
	}
	if len(rawKeys) != 2 {
		t.Fatalf("len(keys)=%d, want 2", len(rawKeys))
	}
	names := make([]string, 0, len(rawKeys))
	for _, raw := range rawKeys {
		item, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("item type=%T, want map[string]any", raw)
		}
		name, _ := item["key_name"].(string)
		names = append(names, name)
	}
	sort.Strings(names)
	if names[0] != "default" || names[1] != "worker" {
		t.Fatalf("unexpected key names: %#v", names)
	}

	getResp, getBody := doJSONRequest(t, http.MethodGet, ts.URL+tenantAPIKeysPath+"/worker", ownerToken, nil)
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("get status=%d, want %d", getResp.StatusCode, http.StatusOK)
	}
	if cacheControl := getResp.Header.Get("Cache-Control"); cacheControl != "no-store" {
		t.Fatalf("Cache-Control=%q, want no-store", cacheControl)
	}
	if pragma := getResp.Header.Get("Pragma"); pragma != "no-cache" {
		t.Fatalf("Pragma=%q, want no-cache", pragma)
	}
	if got := getBody["key_name"]; got != "worker" {
		t.Fatalf("key_name=%v, want worker", got)
	}
	if got := getBody["api_key"]; got != workerToken {
		t.Fatalf("api_key=%v, want worker token", got)
	}
}

func TestCreateAPIKeyRejectsSlashAndDisablesCaching(t *testing.T) {
	srv, ownerToken, cleanup := newAuthServer(t)
	defer cleanup()
	ts := httptest.NewServer(srv)
	defer ts.Close()

	body := bytes.NewReader([]byte(`{"key_name":"worker/blue"}`))
	badResp, badBody := doJSONRequest(t, http.MethodPost, ts.URL+tenantAPIKeysPath, ownerToken, body)
	if badResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad create status=%d, want %d", badResp.StatusCode, http.StatusBadRequest)
	}
	if got := badBody["error"]; got != "key_name must not contain /" {
		t.Fatalf("error=%v, want key_name must not contain /", got)
	}

	goodBody := bytes.NewReader([]byte(`{"key_name":"worker-blue"}`))
	goodResp, goodOut := doJSONRequest(t, http.MethodPost, ts.URL+tenantAPIKeysPath, ownerToken, goodBody)
	if goodResp.StatusCode != http.StatusCreated {
		t.Fatalf("good create status=%d, want %d", goodResp.StatusCode, http.StatusCreated)
	}
	if cacheControl := goodResp.Header.Get("Cache-Control"); cacheControl != "no-store" {
		t.Fatalf("Cache-Control=%q, want no-store", cacheControl)
	}
	if pragma := goodResp.Header.Get("Pragma"); pragma != "no-cache" {
		t.Fatalf("Pragma=%q, want no-cache", pragma)
	}
	if got := goodOut["key_name"]; got != "worker-blue" {
		t.Fatalf("key_name=%v, want worker-blue", got)
	}
}

func TestCreateAPIKeyRejectsOversizeBody(t *testing.T) {
	srv, ownerToken, cleanup := newAuthServer(t)
	defer cleanup()
	ts := httptest.NewServer(srv)
	defer ts.Close()

	largePadding := strings.Repeat("x", 1<<20)
	body := bytes.NewReader([]byte(`{"key_name":"worker-big","padding":"` + largePadding + `"}`))
	resp, out := doJSONRequest(t, http.MethodPost, ts.URL+tenantAPIKeysPath, ownerToken, body)
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize create status=%d, want %d", resp.StatusCode, http.StatusRequestEntityTooLarge)
	}
	if got := out["error"]; got != "request body too large" {
		t.Fatalf("error=%v, want request body too large", got)
	}
}

func TestRevokedAPIKeyNameCannotBeReused(t *testing.T) {
	srv, ownerToken, cleanup := newAuthServer(t)
	defer cleanup()
	ts := httptest.NewServer(srv)
	defer ts.Close()

	createManagedAPIKey(t, ts.URL, ownerToken, "worker")
	deleteResp, deleteBody := doJSONRequest(t, http.MethodDelete, ts.URL+tenantAPIKeysPath+"/worker", ownerToken, nil)
	if deleteResp.StatusCode != http.StatusOK {
		t.Fatalf("delete status=%d, want %d", deleteResp.StatusCode, http.StatusOK)
	}
	if got := deleteBody["status"]; got != "ok" {
		t.Fatalf("delete status body=%v, want ok", got)
	}

	recreateBody := bytes.NewReader([]byte(`{"key_name":"worker"}`))
	recreateResp, recreateOut := doJSONRequest(t, http.MethodPost, ts.URL+tenantAPIKeysPath, ownerToken, recreateBody)
	if recreateResp.StatusCode != http.StatusConflict {
		t.Fatalf("recreate status=%d, want %d", recreateResp.StatusCode, http.StatusConflict)
	}
	if got := recreateOut["error"]; got != "api key name has been revoked and cannot be reused" {
		t.Fatalf("error=%v, want revoked-name conflict", got)
	}
}

func TestNonDefaultAPIKeyCannotManageAPIKeys(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()
	srv := NewWithConfig(Config{Meta: rt.meta, Pool: rt.pool, TokenSecret: rt.tokenSecret})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	workerToken := createManagedAPIKey(t, ts.URL, rt.token, "worker")
	body := []byte(`{"key_name":"worker-2"}`)
	createReq, err := http.NewRequest(http.MethodPost, ts.URL+tenantAPIKeysPath, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	createReq.Header.Set("Authorization", "Bearer "+workerToken)
	createReq.Header.Set("Content-Type", "application/json")
	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = createResp.Body.Close() }()
	if createResp.StatusCode != http.StatusForbidden {
		t.Fatalf("create status=%d, want %d", createResp.StatusCode, http.StatusForbidden)
	}

	deleteReq, err := http.NewRequest(http.MethodDelete, ts.URL+tenantAPIKeysPath+"/default", nil)
	if err != nil {
		t.Fatal(err)
	}
	deleteReq.Header.Set("Authorization", "Bearer "+workerToken)
	deleteResp, err := http.DefaultClient.Do(deleteReq)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = deleteResp.Body.Close() }()
	if deleteResp.StatusCode != http.StatusForbidden {
		t.Fatalf("delete status=%d, want %d", deleteResp.StatusCode, http.StatusForbidden)
	}

	listResp, _ := doJSONRequest(t, http.MethodGet, ts.URL+tenantAPIKeysPath, workerToken, nil)
	if listResp.StatusCode != http.StatusForbidden {
		t.Fatalf("list status=%d, want %d", listResp.StatusCode, http.StatusForbidden)
	}

	getResp, _ := doJSONRequest(t, http.MethodGet, ts.URL+tenantAPIKeysPath+"/default", workerToken, nil)
	if getResp.StatusCode != http.StatusForbidden {
		t.Fatalf("get status=%d, want %d", getResp.StatusCode, http.StatusForbidden)
	}
}

func TestDefaultAPIKeyCannotDeleteItself(t *testing.T) {
	srv, ownerToken, cleanup := newAuthServer(t)
	defer cleanup()
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodDelete, ts.URL+tenantAPIKeysPath+"/default", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+ownerToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestCreatedAPIKeyIsPersistedForTenant(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()
	srv := NewWithConfig(Config{Meta: rt.meta, Pool: rt.pool, TokenSecret: rt.tokenSecret})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	workerToken := createManagedAPIKey(t, ts.URL, rt.token, "worker")
	resolved, err := rt.meta.ResolveByAPIKeyHash(context.Background(), token.HashToken(workerToken))
	if err != nil {
		t.Fatal(err)
	}
	if resolved.APIKey.KeyName != "worker" {
		t.Fatalf("key_name=%q, want worker", resolved.APIKey.KeyName)
	}
	if resolved.APIKey.Status != meta.APIKeyActive {
		t.Fatalf("status=%s, want %s", resolved.APIKey.Status, meta.APIKeyActive)
	}
}
