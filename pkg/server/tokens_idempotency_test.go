package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestScopedTokenIssueIdempotencyReplay(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()
	ts := httptest.NewServer(NewWithConfig(Config{Meta: rt.meta, Pool: rt.pool, TokenSecret: rt.tokenSecret}))
	defer ts.Close()

	body := `{"subject":"session-123","ttl_seconds":3600,"scopes":[{"prefix":"/session_data/session-123/z","ops":["read","list"]},{"prefix":"/session_data/session-123/a","ops":["read","list","write","delete"]}]}`
	first, firstStatus := issueScopedTokenForIdempotencyTest(t, ts.URL, rt.token, "session-123:7", body)
	second, secondStatus := issueScopedTokenForIdempotencyTest(t, ts.URL, rt.token, "session-123:7", body)
	if firstStatus != http.StatusCreated || secondStatus != http.StatusOK {
		t.Fatalf("statuses = %d, %d; want 201, 200", firstStatus, secondStatus)
	}
	if first.Token != second.Token || first.TokenID != second.TokenID || first.ExpiresAt == nil || second.ExpiresAt == nil || !first.ExpiresAt.Equal(*second.ExpiresAt) {
		t.Fatalf("replayed credential changed: first=%+v second=%+v", first, second)
	}
	if len(first.Scopes) != 2 || len(second.Scopes) != 2 || first.Scopes[0].Prefix != "/session_data/session-123/a" || second.Scopes[0].Prefix != first.Scopes[0].Prefix || second.Scopes[1].Prefix != first.Scopes[1].Prefix {
		t.Fatalf("initial/replay scope order differs: first=%+v second=%+v", first.Scopes, second.Scopes)
	}
	var count int
	if err := rt.meta.DB().QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM tenant_api_keys WHERE tenant_id = ? AND issued_by_provider = ? AND issued_by_subject_key = ?`,
		rt.tenantID, scopedTokenIdempotencyProvider, "session-123:7").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("idempotent API key rows = %d, want 1", count)
	}
}

func TestScopedTokenIssueIdempotencyRejectsDifferentPayload(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()
	ts := httptest.NewServer(NewWithConfig(Config{Meta: rt.meta, Pool: rt.pool, TokenSecret: rt.tokenSecret}))
	defer ts.Close()

	body := `{"ttl_seconds":3600,"scopes":[{"prefix":"/session_data/session-123","ops":["read"]}]}`
	_, status := issueScopedTokenForIdempotencyTest(t, ts.URL, rt.token, "session-123:8", body)
	if status != http.StatusCreated {
		t.Fatalf("first status = %d, want 201", status)
	}
	changed := `{"ttl_seconds":3600,"scopes":[{"prefix":"/session_data/session-123","ops":["read","write"]}]}`
	_, status = issueScopedTokenForIdempotencyTest(t, ts.URL, rt.token, "session-123:8", changed)
	if status != http.StatusConflict {
		t.Fatalf("changed replay status = %d, want 409", status)
	}
}

func TestScopedTokenIssueIdempotencyDoesNotResurrectRevokedToken(t *testing.T) {
	rt, cleanup := newAuthRuntime(t)
	defer cleanup()
	ts := httptest.NewServer(NewWithConfig(Config{Meta: rt.meta, Pool: rt.pool, TokenSecret: rt.tokenSecret}))
	defer ts.Close()

	body := `{"ttl_seconds":3600,"scopes":[{"prefix":"/session_data/session-123","ops":["read"]}]}`
	issued, status := issueScopedTokenForIdempotencyTest(t, ts.URL, rt.token, "session-123:9", body)
	if status != http.StatusCreated {
		t.Fatalf("first status = %d, want 201", status)
	}
	if err := rt.meta.RevokeAPIKey(context.Background(), rt.tenantID, issued.TokenID); err != nil {
		t.Fatal(err)
	}
	_, status = issueScopedTokenForIdempotencyTest(t, ts.URL, rt.token, "session-123:9", body)
	if status != http.StatusConflict {
		t.Fatalf("revoked replay status = %d, want 409", status)
	}
}

func issueScopedTokenForIdempotencyTest(t *testing.T, baseURL, ownerToken, idempotencyKey, body string) (scopedTokenResponse, int) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/tokens", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+ownerToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idempotencyKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out scopedTokenResponse
	if resp.StatusCode < http.StatusBadRequest {
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
	}
	return out, resp.StatusCode
}
