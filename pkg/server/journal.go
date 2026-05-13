package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/journal"
)

const journalCursorTTL = 24 * time.Hour

func newJournalCursorSecret(tokenSecret []byte) []byte {
	if len(tokenSecret) > 0 {
		return append([]byte(nil), tokenSecret...)
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		sum := sha256.Sum256([]byte(time.Now().UTC().Format(time.RFC3339Nano)))
		return sum[:]
	}
	return secret
}

func (s *Server) handleJournal(w http.ResponseWriter, r *http.Request) {
	b := backendFromRequest(r)
	if b == nil {
		journalErrJSON(w, http.StatusUnauthorized, "unauthenticated", "missing tenant scope", false)
		return
	}
	scope := ScopeFromContext(r.Context())
	tenantID := "local"
	if scope != nil && scope.TenantID != "" {
		tenantID = scope.TenantID
	}
	store := b.Store()
	switch {
	case r.URL.Path == "/v1/journals":
		if r.Method != http.MethodPost {
			journalErrJSON(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false)
			return
		}
		s.handleJournalCreate(w, r, store, tenantID)
	case r.URL.Path == "/v1/journal-entries":
		if r.Method != http.MethodGet {
			journalErrJSON(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false)
			return
		}
		s.handleJournalSearch(w, r, store, tenantID)
	case strings.HasPrefix(r.URL.Path, "/v1/journals/"):
		s.handleJournalObject(w, r, store, tenantID)
	default:
		journalErrJSON(w, http.StatusNotFound, "not_found", "not found", false)
	}
}

func (s *Server) handleJournalCreate(w http.ResponseWriter, r *http.Request, store *datastore.Store, tenantID string) {
	defer func() { _ = r.Body.Close() }()
	if !requireJournalPermission(w, r, JournalPermissionCreate) {
		return
	}
	var req journal.CreateRequest
	body, err := readJournalRequestBody(r.Body)
	if err != nil {
		if errors.Is(err, journal.ErrPayloadTooLarge) {
			journalErrJSON(w, http.StatusRequestEntityTooLarge, "payload_too_large", err.Error(), false)
			return
		}
		journalErrJSON(w, http.StatusBadRequest, "bad_request", err.Error(), false)
		return
	}
	if err := json.Unmarshal(body, &req); err != nil {
		journalErrJSON(w, http.StatusBadRequest, "bad_request", "malformed JSON", false)
		return
	}
	j, err := store.CreateJournal(r.Context(), tenantID, req)
	if err != nil {
		writeJournalStoreError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(j)
}

func (s *Server) handleJournalObject(w http.ResponseWriter, r *http.Request, store *datastore.Store, tenantID string) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/journals/")
	journalID, sub, hasSub := strings.Cut(rest, "/")
	if journalID == "" {
		journalErrJSON(w, http.StatusNotFound, "not_found", "not found", false)
		return
	}
	switch {
	case hasSub && sub == "entries":
		switch r.Method {
		case http.MethodPost:
			s.handleJournalAppend(w, r, store, tenantID, journalID)
		case http.MethodGet:
			s.handleJournalEntries(w, r, store, tenantID, journalID)
		default:
			journalErrJSON(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false)
		}
	case hasSub && sub == "verify":
		if r.Method != http.MethodGet {
			journalErrJSON(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false)
			return
		}
		s.handleJournalVerify(w, r, store, tenantID, journalID)
	default:
		journalErrJSON(w, http.StatusNotFound, "not_found", "not found", false)
	}
}

func (s *Server) handleJournalAppend(w http.ResponseWriter, r *http.Request, store *datastore.Store, tenantID, journalID string) {
	if !requireJournalPermission(w, r, JournalPermissionAppend) {
		return
	}
	appendID := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if appendID == "" {
		journalErrJSON(w, http.StatusBadRequest, "bad_request", "missing Idempotency-Key", false)
		return
	}
	entries, err := decodeJournalAppendBody(r)
	if err != nil {
		if errors.Is(err, journal.ErrPayloadTooLarge) {
			journalErrJSON(w, http.StatusRequestEntityTooLarge, "payload_too_large", err.Error(), false)
			return
		}
		journalErrJSON(w, http.StatusBadRequest, "bad_request", err.Error(), false)
		return
	}
	scope := ScopeFromContext(r.Context())
	effectiveSource, missingPermission, err := resolveJournalAppendSource(scope, entries)
	if missingPermission != "" {
		journalErrJSON(w, http.StatusForbidden, "forbidden", missingPermission+" permission is required", false)
		return
	}
	if err != nil {
		journalErrJSON(w, http.StatusUnprocessableEntity, "validation_failed", err.Error(), false)
		return
	}
	writer := datastore.JournalWriter{Type: "api_key", ID: "local"}
	if scope != nil && scope.APIKeyID != "" {
		writer.ID = scope.APIKeyID
	}
	writer.Source = effectiveSource
	resp, err := store.AppendJournalEntries(r.Context(), tenantID, journalID, appendID, writer, entries)
	if err != nil {
		writeJournalStoreError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func decodeJournalAppendBody(r *http.Request) ([]journal.EntryInput, error) {
	defer func() { _ = r.Body.Close() }()
	contentType := strings.ToLower(strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"), ";")[0]))
	switch contentType {
	case "application/x-ndjson", "application/jsonl", "text/plain":
		return decodeJournalNDJSON(r.Body)
	default:
		body, err := readJournalRequestBody(r.Body)
		if err != nil {
			return nil, err
		}
		body = bytes.TrimSpace(body)
		if len(body) == 0 {
			return nil, fmt.Errorf("empty append body")
		}
		if body[0] == '[' {
			var entries []journal.EntryInput
			if err := json.Unmarshal(body, &entries); err != nil {
				return nil, fmt.Errorf("malformed JSON")
			}
			return entries, nil
		}
		var entry journal.EntryInput
		if err := json.Unmarshal(body, &entry); err != nil {
			return nil, fmt.Errorf("malformed JSON")
		}
		return []journal.EntryInput{entry}, nil
	}
}

func decodeJournalNDJSON(r io.Reader) ([]journal.EntryInput, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), journal.MaxBatchBytes+1)
	var entries []journal.EntryInput
	var totalBytes int
	for scanner.Scan() {
		rawLine := scanner.Bytes()
		totalBytes += len(rawLine) + 1
		if totalBytes > journal.MaxBatchBytes {
			return nil, fmt.Errorf("%w: append body exceeds %d bytes", journal.ErrPayloadTooLarge, journal.MaxBatchBytes)
		}
		line := bytes.TrimSpace(rawLine)
		if len(line) == 0 {
			continue
		}
		if len(entries) >= journal.MaxEntriesPerBatch {
			return nil, fmt.Errorf("%w: too many entries: %d > %d", journal.ErrPayloadTooLarge, len(entries)+1, journal.MaxEntriesPerBatch)
		}
		var entry journal.EntryInput
		if err := json.Unmarshal(line, &entry); err != nil {
			return nil, fmt.Errorf("malformed JSONL")
		}
		entries = append(entries, entry)
	}
	if err := scanner.Err(); err != nil {
		if strings.Contains(err.Error(), "token too long") {
			return nil, fmt.Errorf("%w: append line exceeds %d bytes", journal.ErrPayloadTooLarge, journal.MaxBatchBytes)
		}
		return nil, err
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("empty append body")
	}
	return entries, nil
}

func resolveJournalAppendSource(scope *TenantScope, entries []journal.EntryInput) (string, string, error) {
	requestedSource := ""
	for _, entry := range entries {
		source := strings.ToLower(strings.TrimSpace(entry.Source))
		if source == "" {
			continue
		}
		if err := journal.ValidateSource(source); err != nil {
			return "", "", err
		}
		if requestedSource == "" {
			requestedSource = source
			continue
		}
		if requestedSource != source {
			return "", "", fmt.Errorf("append batch uses multiple source values")
		}
	}
	if requestedSource == "" || requestedSource == journal.SourceSelf {
		return journal.SourceSelf, "", nil
	}
	permission := journalSourcePermission(requestedSource)
	if permission == "" {
		return "", "", fmt.Errorf("unsupported source %q", requestedSource)
	}
	if scope.HasJournalPermission(permission) {
		return requestedSource, "", nil
	}
	return "", permission, nil
}

func readJournalRequestBody(r io.Reader) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r, int64(journal.MaxBatchBytes)+1))
	if err != nil {
		return nil, err
	}
	if len(body) > journal.MaxBatchBytes {
		return nil, fmt.Errorf("%w: append body exceeds %d bytes", journal.ErrPayloadTooLarge, journal.MaxBatchBytes)
	}
	return body, nil
}

func (s *Server) handleJournalEntries(w http.ResponseWriter, r *http.Request, store *datastore.Store, tenantID, journalID string) {
	if !requireJournalPermission(w, r, JournalPermissionRead) {
		return
	}
	q := r.URL.Query()
	afterSeq, err := parseOptionalInt64Param(q, "after_seq")
	if err != nil {
		journalErrJSON(w, http.StatusBadRequest, "bad_request", err.Error(), false)
		return
	}
	if q.Get("after") != "" {
		afterSeq, err = parseOptionalInt64Param(q, "after")
		if err != nil {
			journalErrJSON(w, http.StatusBadRequest, "bad_request", err.Error(), false)
			return
		}
	}
	limit, err := parseOptionalIntParam(q, "limit")
	if err != nil {
		journalErrJSON(w, http.StatusBadRequest, "bad_request", err.Error(), false)
		return
	}
	entries, err := store.ListJournalEntries(r.Context(), tenantID, journalID, afterSeq, limit)
	if err != nil {
		writeJournalStoreError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	enc := json.NewEncoder(w)
	for _, entry := range entries {
		_ = enc.Encode(entry)
	}
}

func (s *Server) handleJournalSearch(w http.ResponseWriter, r *http.Request, store *datastore.Store, tenantID string) {
	if !requireJournalPermission(w, r, JournalPermissionFind) {
		return
	}
	req, queryHash, err := s.parseJournalSearchRequest(r.Context(), tenantID, r.URL.Query())
	if err != nil {
		journalErrJSON(w, http.StatusBadRequest, "bad_request", err.Error(), false)
		return
	}
	readAllowed := ScopeFromContext(r.Context()).HasJournalPermission(JournalPermissionRead)
	if req.Entries {
		if !readAllowed {
			journalErrJSON(w, http.StatusForbidden, "forbidden", JournalPermissionRead+" permission is required", false)
			return
		}
	}
	matches, err := store.SearchJournal(r.Context(), tenantID, req)
	if err != nil {
		writeJournalStoreError(w, err)
		return
	}
	if !readAllowed {
		redactJournalFindOnlyMatches(matches)
	}
	for i := range matches {
		matches[i].Cursor = s.encodeJournalCursor(tenantID, queryHash, req, matches[i])
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	enc := json.NewEncoder(w)
	for _, match := range matches {
		_ = enc.Encode(match)
	}
}

func redactJournalFindOnlyMatches(matches []journal.SearchMatch) {
	for i := range matches {
		matches[i].Title = ""
		matches[i].Entry = nil
	}
}

func (s *Server) parseJournalSearchRequest(ctx context.Context, tenantID string, q url.Values) (journal.SearchRequest, string, error) {
	var req journal.SearchRequest
	rawSince := strings.TrimSpace(q.Get("since"))
	rawUntil := strings.TrimSpace(q.Get("until"))
	req.Type = strings.TrimSpace(q.Get("type"))
	if req.Type == "" {
		req.Type = strings.TrimSpace(q.Get("t"))
	}
	req.Status = strings.TrimSpace(q.Get("status"))
	req.Kind = strings.TrimSpace(q.Get("kind"))
	req.Subjects = append(req.Subjects, q["subject"]...)
	req.Subjects = append(req.Subjects, q["s"]...)
	labels, err := parseJournalLabelParams(q)
	if err != nil {
		return req, "", err
	}
	req.Labels = labels
	req.Limit, err = parseOptionalIntParam(q, "limit")
	if err != nil {
		return req, "", err
	}
	req.Limit = journal.NormalizeLimit(req.Limit)
	req.Entries = q.Get("include") == "entry" || q.Get("entries") == "1"
	if actor := q.Get("actor"); actor != "" {
		actorType, actorID, err := journal.SplitActor(actor)
		if err != nil {
			return req, "", err
		}
		req.ActorType, req.ActorID = actorType, actorID
	}
	if rawSince != "" {
		t, err := parseJournalTimeOrDuration(rawSince)
		if err != nil {
			return req, "", fmt.Errorf("invalid since: %w", err)
		}
		req.Since = &t
	}
	if rawUntil != "" {
		t, err := parseJournalTime(rawUntil)
		if err != nil {
			return req, "", fmt.Errorf("invalid until: %w", err)
		}
		req.Until = &t
	}
	if req.Entries {
		req.ResultKind = journal.SearchResultEntries
	} else if req.Type == "" && req.Status == "" && len(req.Subjects) == 0 && req.ActorType == "" {
		req.ResultKind = journal.SearchResultJournals
	} else {
		req.ResultKind = journal.SearchResultEntries
	}
	req, err = journal.NormalizeSearchRequest(req)
	if err != nil {
		return req, "", err
	}
	if req.ResultKind == journal.SearchResultEntries && !hasJournalEntrySearchAnchor(req) {
		return req, "", fmt.Errorf("entry search requires type, status, actor, subject, or metadata filter")
	}
	queryHash, err := journal.HashCanonical(map[string]any{
		"type":        req.Type,
		"status":      req.Status,
		"kind":        req.Kind,
		"actor_type":  req.ActorType,
		"actor_id":    req.ActorID,
		"subjects":    req.Subjects,
		"labels":      req.Labels,
		"since":       rawSince,
		"until":       rawUntil,
		"entries":     req.Entries,
		"result_kind": req.ResultKind,
	})
	if err != nil {
		return req, "", err
	}
	if cursor := q.Get("cursor"); cursor != "" {
		after, cursorSince, cursorUntil, err := s.decodeJournalCursor(ctx, cursor, tenantID, queryHash, req.ResultKind)
		if err != nil {
			return req, "", err
		}
		req.After = after
		if cursorSince != nil {
			req.Since = cursorSince
		}
		if cursorUntil != nil {
			req.Until = cursorUntil
		}
	}
	return req, queryHash, nil
}

func parseJournalLabelParams(q url.Values) ([]journal.Label, error) {
	var labels []journal.Label
	for _, raw := range append(q["meta"], q["m"]...) {
		key, value, ok := strings.Cut(raw, "=")
		if !ok {
			return nil, fmt.Errorf("invalid metadata %q (expected key=value)", raw)
		}
		labels = append(labels, journal.Label{Key: key, Value: value})
	}
	if len(labels) == 0 {
		return nil, nil
	}
	labels = journal.NormalizeLabels(labels)
	if err := journal.ValidateLabels(labels); err != nil {
		return nil, err
	}
	return labels, nil
}

func parseOptionalIntParam(q url.Values, name string) (int, error) {
	raw := strings.TrimSpace(q.Get(name))
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", name, err)
	}
	if value < 0 {
		return 0, fmt.Errorf("invalid %s: must be non-negative", name)
	}
	return value, nil
}

func parseOptionalInt64Param(q url.Values, name string) (int64, error) {
	raw := strings.TrimSpace(q.Get(name))
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", name, err)
	}
	if value < 0 {
		return 0, fmt.Errorf("invalid %s: must be non-negative", name)
	}
	return value, nil
}

func parseJournalTimeOrDuration(raw string) (time.Time, error) {
	if d, err := time.ParseDuration(raw); err == nil {
		return journal.NormalizeTime(time.Now().Add(-d)), nil
	}
	return parseJournalTime(raw)
}

func parseJournalTime(raw string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, err
	}
	return journal.NormalizeTime(t), nil
}

func (s *Server) handleJournalVerify(w http.ResponseWriter, r *http.Request, store *datastore.Store, tenantID, journalID string) {
	if !requireJournalPermission(w, r, JournalPermissionVerify) {
		return
	}
	result, err := store.VerifyJournal(r.Context(), tenantID, journalID)
	if err != nil {
		writeJournalStoreError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

type journalCursorEnvelope struct {
	V         int                      `json:"v"`
	Kind      journal.SearchResultKind `json:"kind"`
	TenantID  string                   `json:"tenant_id"`
	QueryHash string                   `json:"query_hash"`
	Since     string                   `json:"since,omitempty"`
	Until     string                   `json:"until,omitempty"`
	Observed  string                   `json:"observed_at,omitempty"`
	Created   string                   `json:"created_at,omitempty"`
	JournalID string                   `json:"journal_id"`
	Seq       int64                    `json:"seq,omitempty"`
	Expires   int64                    `json:"expires"`
}

func (s *Server) encodeJournalCursor(tenantID, queryHash string, req journal.SearchRequest, match journal.SearchMatch) string {
	env := journalCursorEnvelope{
		V:         1,
		Kind:      req.ResultKind,
		TenantID:  tenantID,
		QueryHash: queryHash,
		JournalID: match.JournalID,
		Seq:       match.Seq,
		Expires:   time.Now().Add(journalCursorTTL).Unix(),
	}
	if req.Since != nil {
		env.Since = journal.FormatTime(*req.Since)
	}
	if req.Until != nil {
		env.Until = journal.FormatTime(*req.Until)
	}
	if req.ResultKind == journal.SearchResultJournals {
		sortAt := match.CreatedAt
		if !match.CursorAt.IsZero() {
			sortAt = match.CursorAt
		}
		env.Created = journal.FormatTime(sortAt)
	} else {
		env.Observed = journal.FormatTime(match.ObservedAt)
	}
	payload, err := json.Marshal(env)
	if err != nil {
		return ""
	}
	mac := hmac.New(sha256.New, s.journalCursorSecret)
	_, _ = mac.Write(payload)
	sum := mac.Sum(nil)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(sum)
}

func (s *Server) decodeJournalCursor(ctx context.Context, token, tenantID, queryHash string, kind journal.SearchResultKind) (journal.SearchAfter, *time.Time, *time.Time, error) {
	payloadPart, macPart, ok := strings.Cut(token, ".")
	if !ok {
		return journal.SearchAfter{}, nil, nil, fmt.Errorf("malformed cursor")
	}
	payload, err := base64.RawURLEncoding.DecodeString(payloadPart)
	if err != nil {
		return journal.SearchAfter{}, nil, nil, fmt.Errorf("malformed cursor")
	}
	gotMAC, err := base64.RawURLEncoding.DecodeString(macPart)
	if err != nil {
		return journal.SearchAfter{}, nil, nil, fmt.Errorf("malformed cursor")
	}
	mac := hmac.New(sha256.New, s.journalCursorSecret)
	_, _ = mac.Write(payload)
	if !hmac.Equal(gotMAC, mac.Sum(nil)) {
		return journal.SearchAfter{}, nil, nil, fmt.Errorf("malformed cursor")
	}
	var env journalCursorEnvelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return journal.SearchAfter{}, nil, nil, fmt.Errorf("malformed cursor")
	}
	if env.V != 1 || env.TenantID != tenantID || env.QueryHash != queryHash || env.Kind != kind {
		return journal.SearchAfter{}, nil, nil, fmt.Errorf("query-mismatched cursor")
	}
	if env.Expires > 0 && time.Now().Unix() > env.Expires {
		return journal.SearchAfter{}, nil, nil, fmt.Errorf("expired cursor")
	}
	after := journal.SearchAfter{JournalID: env.JournalID, Seq: env.Seq}
	var since *time.Time
	var until *time.Time
	if env.Since != "" {
		t, err := parseJournalTime(env.Since)
		if err != nil {
			return journal.SearchAfter{}, nil, nil, fmt.Errorf("malformed cursor")
		}
		since = &t
	}
	if env.Until != "" {
		t, err := parseJournalTime(env.Until)
		if err != nil {
			return journal.SearchAfter{}, nil, nil, fmt.Errorf("malformed cursor")
		}
		until = &t
	}
	if env.Observed != "" {
		t, err := parseJournalTime(env.Observed)
		if err != nil {
			return journal.SearchAfter{}, nil, nil, fmt.Errorf("malformed cursor")
		}
		after.ObservedAt = t
	}
	if env.Created != "" {
		t, err := parseJournalTime(env.Created)
		if err != nil {
			return journal.SearchAfter{}, nil, nil, fmt.Errorf("malformed cursor")
		}
		after.CreatedAt = t
	}
	_ = ctx
	return after, since, until, nil
}

func writeJournalStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, datastore.ErrNotFound):
		journalErrJSON(w, http.StatusNotFound, "not_found", "not found", false)
	case errors.Is(err, datastore.ErrJournalConflict), errors.Is(err, datastore.ErrIdempotencyConflict):
		journalErrJSON(w, http.StatusConflict, "conflict", err.Error(), false)
	case errors.Is(err, datastore.ErrJournalClosed):
		journalErrJSON(w, http.StatusConflict, "conflict", err.Error(), false)
	case errors.Is(err, datastore.ErrJournalPayloadTooLarge):
		journalErrJSON(w, http.StatusRequestEntityTooLarge, "payload_too_large", err.Error(), false)
	case errors.Is(err, datastore.ErrJournalValidation):
		journalErrJSON(w, http.StatusUnprocessableEntity, "validation_failed", err.Error(), false)
	default:
		journalErrJSON(w, http.StatusServiceUnavailable, "unavailable", err.Error(), true)
	}
}

func journalErrJSON(w http.ResponseWriter, status int, code, message string, retryable bool) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":       code,
			"message":    message,
			"retryable":  retryable,
			"request_id": "",
		},
	})
}

func requireJournalPermission(w http.ResponseWriter, r *http.Request, permission string) bool {
	scope := ScopeFromContext(r.Context())
	if scope.HasJournalPermission(permission) {
		return true
	}
	journalErrJSON(w, http.StatusForbidden, "forbidden", permission+" permission is required", false)
	return false
}

func hasJournalEntrySearchAnchor(req journal.SearchRequest) bool {
	return req.Type != "" ||
		req.Status != "" ||
		req.ActorType != "" ||
		len(req.Subjects) > 0 ||
		len(req.Labels) > 0
}
