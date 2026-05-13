package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/journal"
)

func TestJournalHTTPCreateAppendSearchVerify(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	createBody := []byte(`{"journal_id":"jrn_http","kind":"agent","meta":{"repo":"github.com/mem9-ai/drive9"}}`)
	resp, err := http.Post(ts.URL+"/v1/journals", "application/json", bytes.NewReader(createBody))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create status = %d", resp.StatusCode)
	}
	var created journal.Journal
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	if created.JournalID != "jrn_http" || created.HeadHash == "" {
		t.Fatalf("created = %#v", created)
	}

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/journals/jrn_http/entries", strings.NewReader(`{"type":"tool.call.completed","status":"ok","subjects":["tool:exec_command"],"summary":{"cmd":"go test ./..."}}`))
	req.Header.Set("Content-Type", "application/json")
	missingKeyResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = missingKeyResp.Body.Close()
	if missingKeyResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing idempotency status = %d, want 400", missingKeyResp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/journals/jrn_http/entries", strings.NewReader(`{"type":"tool.call.completed","status":"ok","subjects":["tool:exec_command"],"summary":{"cmd":"go test ./..."}}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "app_http")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("append status = %d", resp.StatusCode)
	}
	var appendResp journal.AppendResponse
	if err := json.NewDecoder(resp.Body).Decode(&appendResp); err != nil {
		t.Fatalf("decode append: %v", err)
	}
	if appendResp.FirstSeq != 1 || appendResp.LastSeq != 1 || !appendResp.Idempotent {
		t.Fatalf("append response = %#v", appendResp)
	}

	resp, err = http.Get(ts.URL + "/v1/journals/jrn_http/entries?limit=10")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cat status = %d", resp.StatusCode)
	}
	scanner := bufio.NewScanner(resp.Body)
	if !scanner.Scan() {
		t.Fatal("expected one journal entry")
	}
	var entry journal.Entry
	if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
		t.Fatalf("decode entry: %v", err)
	}
	if entry.Seq != 1 || entry.Source != journal.SourceSelf || entry.EntryHash == "" {
		t.Fatalf("entry = %#v", entry)
	}

	resp, err = http.Get(ts.URL + "/v1/journal-entries?subject=tool%3Aexec_command&limit=10")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("search status = %d", resp.StatusCode)
	}
	scanner = bufio.NewScanner(resp.Body)
	if !scanner.Scan() {
		t.Fatal("expected one search match")
	}
	var match journal.SearchMatch
	if err := json.Unmarshal(scanner.Bytes(), &match); err != nil {
		t.Fatalf("decode match: %v", err)
	}
	if match.JournalID != "jrn_http" || match.Seq != 1 || match.Cursor == "" {
		t.Fatalf("match = %#v", match)
	}

	resp, err = http.Get(ts.URL + "/v1/journals/jrn_http/verify")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("verify status = %d", resp.StatusCode)
	}
	var verify journal.VerifyResult
	if err := json.NewDecoder(resp.Body).Decode(&verify); err != nil {
		t.Fatalf("decode verify: %v", err)
	}
	if !verify.OK || verify.Entries != 1 || verify.HeadHash != appendResp.HeadHash {
		t.Fatalf("verify = %#v", verify)
	}
	if verify.ProjectionOK == nil || !*verify.ProjectionOK {
		t.Fatalf("verify projection_ok = %#v, want true", verify.ProjectionOK)
	}
}

func TestJournalHTTPRejectsMalformedQueriesAndMissingJournal(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	for _, target := range []string{
		"/v1/journal-entries?meta=repo",
		"/v1/journal-entries?meta=%3Dprod",
		"/v1/journal-entries?subject=bad-subject",
		"/v1/journal-entries?limit=not-an-int",
		"/v1/journal-entries?include=entry",
		"/v1/journals/jrn_missing/entries?after=abc",
	} {
		resp, err := http.Get(ts.URL + target)
		if err != nil {
			t.Fatalf("GET %s: %v", target, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("GET %s status = %d, want 400", target, resp.StatusCode)
		}
	}

	resp, err := http.Get(ts.URL + "/v1/journals/jrn_missing/entries")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("missing journal cat status = %d, want 404", resp.StatusCode)
	}

	resp, err = http.Post(ts.URL+"/v1/journals", "application/json", strings.NewReader(strings.Repeat(" ", journal.MaxBatchBytes+1)))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized create body status = %d, want 413", resp.StatusCode)
	}

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/journals/jrn_missing/entries", strings.NewReader(strings.Repeat(" ", journal.MaxBatchBytes+1)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "app_too_large_body")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized append body status = %d, want 413", resp.StatusCode)
	}

	createBody := []byte(`{"journal_id":"jrn_payload","kind":"agent"}`)
	resp, err = http.Post(ts.URL+"/v1/journals", "application/json", bytes.NewReader(createBody))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create payload journal status = %d", resp.StatusCode)
	}
	oversizedSummary := `{"type":"tool.call.completed","summary":{"x":"` + strings.Repeat("a", journal.MaxInlineSummaryBytes) + `"}}`
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/journals/jrn_payload/entries", strings.NewReader(oversizedSummary))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "app_too_large_summary")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized summary status = %d, want 413", resp.StatusCode)
	}
}

func TestJournalHTTPFullEntriesRequireReadPermission(t *testing.T) {
	s := newTestServer(t)
	store := s.fallback.Store()
	ctx := context.Background()
	tenantID := "local"

	if _, err := store.CreateJournal(ctx, tenantID, journal.CreateRequest{
		JournalID: "jrn_read_perm",
		Labels:    []journal.Label{{Key: "env", Value: "prod"}},
	}); err != nil {
		t.Fatalf("CreateJournal: %v", err)
	}
	if _, err := store.AppendJournalEntries(ctx, tenantID, "jrn_read_perm", "app_read_perm", datastore.JournalWriter{Type: "api_key", ID: "key-1"}, []journal.EntryInput{{
		Type:    "tool.call.completed",
		Summary: []byte(`{"cmd":"secret-ish details"}`),
	}}); err != nil {
		t.Fatalf("AppendJournalEntries: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/journal-entries?meta=env%3Dprod&include=entry", nil)
	scope := &TenantScope{
		TenantID:           tenantID,
		APIKeyID:           "find-only",
		JournalPermissions: map[string]bool{JournalPermissionFind: true},
	}
	rr := httptest.NewRecorder()
	s.handleJournalSearch(rr, req.WithContext(withScope(req.Context(), scope)), store, tenantID)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("include=entry without journal:read status = %d, want 403 body=%s", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/journals/jrn_read_perm/entries", nil)
	rr = httptest.NewRecorder()
	s.handleJournalEntries(rr, req.WithContext(withScope(req.Context(), scope)), store, tenantID, "jrn_read_perm")
	if rr.Code != http.StatusForbidden {
		t.Fatalf("cat without journal:read status = %d, want 403 body=%s", rr.Code, rr.Body.String())
	}
}

func TestJournalHTTPRequiresSpecificPermissions(t *testing.T) {
	s := newTestServer(t)
	store := s.fallback.Store()
	ctx := context.Background()
	tenantID := "local"

	if _, err := store.CreateJournal(ctx, tenantID, journal.CreateRequest{
		JournalID: "jrn_permissions",
		Title:     "private title",
		Labels:    []journal.Label{{Key: "env", Value: "prod"}},
	}); err != nil {
		t.Fatalf("CreateJournal setup: %v", err)
	}
	if _, err := store.AppendJournalEntries(ctx, tenantID, "jrn_permissions", "app_permissions_setup", datastore.JournalWriter{Type: "api_key", ID: "setup"}, []journal.EntryInput{{
		Type: "tool.call.completed",
	}}); err != nil {
		t.Fatalf("AppendJournalEntries setup: %v", err)
	}

	t.Run("create", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/journals", strings.NewReader(`{"journal_id":"jrn_denied_create"}`))
		rr := httptest.NewRecorder()
		s.handleJournalCreate(rr, req.WithContext(withScope(req.Context(), journalScope(tenantID))), store, tenantID)
		if rr.Code != http.StatusForbidden {
			t.Fatalf("create without permission status = %d, want 403 body=%s", rr.Code, rr.Body.String())
		}
	})

	t.Run("append", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/journals/jrn_permissions/entries", strings.NewReader(`{"type":"tool.call.completed"}`))
		req.Header.Set("Idempotency-Key", "app_denied_append")
		rr := httptest.NewRecorder()
		s.handleJournalAppend(rr, req.WithContext(withScope(req.Context(), journalScope(tenantID))), store, tenantID, "jrn_permissions")
		if rr.Code != http.StatusForbidden {
			t.Fatalf("append without permission status = %d, want 403 body=%s", rr.Code, rr.Body.String())
		}
	})

	t.Run("find", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/journal-entries?type=tool.call.completed", nil)
		rr := httptest.NewRecorder()
		scope := journalScope(tenantID)
		scope.JournalPermissions[JournalPermissionRead] = true
		s.handleJournalSearch(rr, req.WithContext(withScope(req.Context(), scope)), store, tenantID)
		if rr.Code != http.StatusForbidden {
			t.Fatalf("find without permission status = %d, want 403 body=%s", rr.Code, rr.Body.String())
		}
	})

	t.Run("find metadata without read omits title", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/journal-entries?meta=env%3Dprod", nil)
		rr := httptest.NewRecorder()
		scope := journalScope(tenantID)
		scope.JournalPermissions[JournalPermissionFind] = true
		s.handleJournalSearch(rr, req.WithContext(withScope(req.Context(), scope)), store, tenantID)
		if rr.Code != http.StatusOK {
			t.Fatalf("find-only status = %d, want 200 body=%s", rr.Code, rr.Body.String())
		}
		body := rr.Body.String()
		if !strings.Contains(body, `"journal_id":"jrn_permissions"`) {
			t.Fatalf("find-only body = %s, want journal id", body)
		}
		if strings.Contains(body, "private title") {
			t.Fatalf("find-only body leaked title: %s", body)
		}
	})

	t.Run("find entries needs read too", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/journal-entries?type=tool.call.completed&include=entry", nil)
		rr := httptest.NewRecorder()
		scope := journalScope(tenantID)
		scope.JournalPermissions[JournalPermissionFind] = true
		s.handleJournalSearch(rr, req.WithContext(withScope(req.Context(), scope)), store, tenantID)
		if rr.Code != http.StatusForbidden {
			t.Fatalf("include=entry without read status = %d, want 403 body=%s", rr.Code, rr.Body.String())
		}
	})

	t.Run("verify", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/journals/jrn_permissions/verify", nil)
		rr := httptest.NewRecorder()
		s.handleJournalVerify(rr, req.WithContext(withScope(req.Context(), journalScope(tenantID))), store, tenantID, "jrn_permissions")
		if rr.Code != http.StatusForbidden {
			t.Fatalf("verify without permission status = %d, want 403 body=%s", rr.Code, rr.Body.String())
		}
	})
}

func TestJournalHTTPAppendSourcePermissions(t *testing.T) {
	s := newTestServer(t)
	store := s.fallback.Store()
	ctx := context.Background()
	tenantID := "local"

	if _, err := store.CreateJournal(ctx, tenantID, journal.CreateRequest{JournalID: "jrn_source_perm"}); err != nil {
		t.Fatalf("CreateJournal: %v", err)
	}

	appendOnly := journalScope(tenantID)
	appendOnly.JournalPermissions[JournalPermissionAppend] = true
	req := httptest.NewRequest(http.MethodPost, "/v1/journals/jrn_source_perm/entries", strings.NewReader(`{"type":"tool.call.completed","source":"gateway_observed"}`))
	req.Header.Set("Idempotency-Key", "app_source_denied")
	rr := httptest.NewRecorder()
	s.handleJournalAppend(rr, req.WithContext(withScope(req.Context(), appendOnly)), store, tenantID, "jrn_source_perm")
	if rr.Code != http.StatusForbidden {
		t.Fatalf("gateway source without source permission status = %d, want 403 body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), JournalPermissionSourceGateway) {
		t.Fatalf("gateway source denial body = %s, want missing permission", rr.Body.String())
	}

	gatewayScope := journalScope(tenantID)
	gatewayScope.JournalPermissions[JournalPermissionAppend] = true
	gatewayScope.JournalPermissions[JournalPermissionSourceGateway] = true
	req = httptest.NewRequest(http.MethodPost, "/v1/journals/jrn_source_perm/entries", strings.NewReader(`{"type":"tool.call.completed","source":"gateway_observed"}`))
	req.Header.Set("Idempotency-Key", "app_source_allowed")
	rr = httptest.NewRecorder()
	s.handleJournalAppend(rr, req.WithContext(withScope(req.Context(), gatewayScope)), store, tenantID, "jrn_source_perm")
	if rr.Code != http.StatusOK {
		t.Fatalf("gateway source with permission status = %d, want 200 body=%s", rr.Code, rr.Body.String())
	}

	entries, err := store.ListJournalEntries(ctx, tenantID, "jrn_source_perm", 0, 10)
	if err != nil {
		t.Fatalf("ListJournalEntries: %v", err)
	}
	if len(entries) != 1 || entries[0].Source != journal.SourceGateway {
		t.Fatalf("entries = %#v, want gateway source", entries)
	}

	mixedScope := journalScope(tenantID)
	mixedScope.JournalPermissions[JournalPermissionAppend] = true
	mixedScope.JournalPermissions[JournalPermissionSourceGateway] = true
	mixedScope.JournalPermissions[JournalPermissionSourceImport] = true
	req = httptest.NewRequest(http.MethodPost, "/v1/journals/jrn_source_perm/entries", strings.NewReader(`[
		{"type":"tool.call.completed","source":"gateway_observed"},
		{"type":"tool.call.completed","source":"imported"}
	]`))
	req.Header.Set("Idempotency-Key", "app_source_mixed")
	rr = httptest.NewRecorder()
	s.handleJournalAppend(rr, req.WithContext(withScope(req.Context(), mixedScope)), store, tenantID, "jrn_source_perm")
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("mixed source batch status = %d, want 422 body=%s", rr.Code, rr.Body.String())
	}
}

func journalScope(tenantID string) *TenantScope {
	return &TenantScope{
		TenantID:           tenantID,
		APIKeyID:           "limited",
		JournalPermissions: map[string]bool{},
	}
}

func TestJournalRelativeSinceCursorReusesEffectiveBound(t *testing.T) {
	s := &Server{journalCursorSecret: []byte("test-secret")}
	tenantID := "tenant-cursor"
	q := url.Values{
		"type":  []string{"tool.call.completed"},
		"since": []string{"1h"},
	}
	req1, queryHash, err := s.parseJournalSearchRequest(context.Background(), tenantID, q)
	if err != nil {
		t.Fatalf("parse first query: %v", err)
	}
	if req1.Since == nil {
		t.Fatal("first query since is nil")
	}
	cursor := s.encodeJournalCursor(tenantID, queryHash, req1, journal.SearchMatch{
		JournalID:  "jrn_cursor",
		Seq:        10,
		ObservedAt: time.Date(2026, 5, 12, 1, 2, 3, 0, time.UTC),
	})
	if cursor == "" {
		t.Fatal("empty cursor")
	}

	time.Sleep(2 * time.Millisecond)
	q.Set("cursor", cursor)
	req2, _, err := s.parseJournalSearchRequest(context.Background(), tenantID, q)
	if err != nil {
		t.Fatalf("parse cursor query: %v", err)
	}
	if req2.Since == nil || !req2.Since.Equal(*req1.Since) {
		t.Fatalf("cursor since = %v, want original effective since %v", req2.Since, req1.Since)
	}
	if req2.After.JournalID != "jrn_cursor" || req2.After.Seq != 10 {
		t.Fatalf("cursor after = %#v", req2.After)
	}
}

func TestJournalCursorRequiresOriginalFilters(t *testing.T) {
	s := &Server{journalCursorSecret: []byte("test-secret")}
	tenantID := "tenant-cursor-filter"
	q := url.Values{"type": []string{"tool.call.completed"}}
	req, queryHash, err := s.parseJournalSearchRequest(context.Background(), tenantID, q)
	if err != nil {
		t.Fatalf("parse first query: %v", err)
	}
	cursor := s.encodeJournalCursor(tenantID, queryHash, req, journal.SearchMatch{
		JournalID:  "jrn_cursor",
		Seq:        10,
		ObservedAt: time.Date(2026, 5, 12, 1, 2, 3, 0, time.UTC),
	})
	if cursor == "" {
		t.Fatal("empty cursor")
	}
	_, _, err = s.parseJournalSearchRequest(context.Background(), tenantID, url.Values{"cursor": []string{cursor}})
	if err == nil || !strings.Contains(err.Error(), "query-mismatched cursor") {
		t.Fatalf("parse cursor without original filters err = %v, want query-mismatched cursor", err)
	}
}

func TestJournalCursorUsesInternalSortTimeForJournalMatches(t *testing.T) {
	s := &Server{journalCursorSecret: []byte("test-secret")}
	tenantID := "tenant-label-cursor"
	q := url.Values{"meta": []string{"env=prod"}}
	req, queryHash, err := s.parseJournalSearchRequest(context.Background(), tenantID, q)
	if err != nil {
		t.Fatalf("parse label query: %v", err)
	}
	createdAt := time.Date(2026, 5, 12, 1, 2, 3, 0, time.UTC)
	cursorAt := createdAt.Add(time.Hour)
	cursor := s.encodeJournalCursor(tenantID, queryHash, req, journal.SearchMatch{
		JournalID: "jrn_cursor",
		CreatedAt: createdAt,
		CursorAt:  cursorAt,
	})
	if cursor == "" {
		t.Fatal("empty cursor")
	}
	q.Set("cursor", cursor)
	req2, _, err := s.parseJournalSearchRequest(context.Background(), tenantID, q)
	if err != nil {
		t.Fatalf("parse cursor query: %v", err)
	}
	if !req2.After.CreatedAt.Equal(journal.NormalizeTime(cursorAt)) {
		t.Fatalf("cursor created sort time = %s, want %s", req2.After.CreatedAt, journal.NormalizeTime(cursorAt))
	}
}

func TestJournalStoreErrorDefaultIsUnavailable(t *testing.T) {
	rr := httptest.NewRecorder()
	writeJournalStoreError(rr, errors.New("backend down"))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("default store error status = %d, want 503 body=%s", rr.Code, rr.Body.String())
	}
	var body map[string]map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if _, ok := body["error"]["request_id"]; !ok {
		t.Fatalf("error body missing request_id: %#v", body)
	}
}

func TestJournalHTTPSearchOmitsZeroTimesAndSupportsRepeatedLabels(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	createBody := []byte(`{"journal_id":"jrn_labels","kind":"agent","labels":[{"key":"env","value":"prod"},{"key":"env","value":"us-east"}]}`)
	resp, err := http.Post(ts.URL+"/v1/journals", "application/json", bytes.NewReader(createBody))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create status = %d", resp.StatusCode)
	}

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/journals/jrn_labels/entries", strings.NewReader(`{"type":"tool.call.completed","status":"ok","summary":{"cmd":"labels"}}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "app_labels_entry")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("append label entry status = %d", resp.StatusCode)
	}

	resp, err = http.Get(ts.URL + "/v1/journal-entries?meta=env%3Dprod&meta=env%3Dus-east&limit=10")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		t.Fatalf("search status = %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte(`"journal_id":"jrn_labels"`)) {
		t.Fatalf("search body = %s, want jrn_labels", body)
	}
	if bytes.Contains(body, []byte("0001-01-01")) || bytes.Contains(body, []byte(`"observed_at"`)) {
		t.Fatalf("search body has zero entry time: %s", body)
	}

	resp, err = http.Get(ts.URL + "/v1/journal-entries?meta=env%3Dprod&include=entry&limit=10")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		t.Fatalf("search label entries status = %d", resp.StatusCode)
	}
	body, err = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte(`"entry"`)) || !bytes.Contains(body, []byte(`"journal_id":"jrn_labels"`)) {
		t.Fatalf("label include=entry body = %s, want full entry match", body)
	}
}
