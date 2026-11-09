package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// AdminMigrationCredentials authenticate the migration admin API. The migration
// API reuses TiDB Cloud organization credentials; it does not accept tenant
// filesystem API keys.
type AdminMigrationCredentials struct {
	PublicKey  string
	PrivateKey string
}

func (c AdminMigrationCredentials) setHeaders(req *http.Request) {
	setQuotaHeaders(req, c.PublicKey, c.PrivateKey)
}

// AdminMigrationBatchCreateRequest registers a batch of tenant IDs. The batch
// itself is a controller-owned concept; drive9 only admits the request against
// the caller's migration plan.
type AdminMigrationBatchCreateRequest struct {
	Credentials AdminMigrationCredentials
	Name        string
	TenantIDs   []string
}

// AdminMigrationBatchRequest identifies one batch for get/start/pause/resume.
type AdminMigrationBatchRequest struct {
	Credentials AdminMigrationCredentials
	BatchID     string
}

// AdminMigrationTenantRequest identifies one tenant for status/state.
type AdminMigrationTenantRequest struct {
	Credentials AdminMigrationCredentials
	TenantID    string
}

// AdminMigrationRollbackRequest asks the controller to return a switched tenant
// to the source region before the target has accepted writes.
type AdminMigrationRollbackRequest struct {
	Credentials AdminMigrationCredentials
	TenantID    string
}

// AdminMigrationTenantListRequest filters the controller's tenant list. All
// filters are optional; the server still scopes the response to the caller's
// organization and migration plan.
type AdminMigrationTenantListRequest struct {
	Credentials AdminMigrationCredentials
	BatchID     string
	State       string
	TenantID    string
	TenantIDs   []string
	Page        int
	PageSize    int
	Limit       int
}

// AdminMigrationWatchRequest selects the tenants/batch whose controller events
// should be streamed.
type AdminMigrationWatchRequest struct {
	Credentials AdminMigrationCredentials
	BatchID     string
	TenantID    string
	TenantIDs   []string
}

// AdminMigrationEvent is one SSE event from the migration controller, relayed
// by the drive9 migration admin API. Data is kept raw so a controller that adds
// fields does not require a CLI release.
type AdminMigrationEvent struct {
	Type string
	Data json.RawMessage
}

// AdminCreateMigrationBatch registers a migration batch.
func (c *Client) AdminCreateMigrationBatch(ctx context.Context, req AdminMigrationBatchCreateRequest) (json.RawMessage, error) {
	body := struct {
		Name      string   `json:"name,omitempty"`
		TenantIDs []string `json:"tenant_ids"`
	}{
		Name:      strings.TrimSpace(req.Name),
		TenantIDs: append([]string(nil), req.TenantIDs...),
	}
	if len(body.TenantIDs) == 0 {
		return nil, fmt.Errorf("at least one tenant id is required")
	}
	return c.adminMigrationRequest(ctx, http.MethodPost, "/v1/admin/migration/batches", nil, body, req.Credentials)
}

// AdminGetMigrationBatch returns one batch.
func (c *Client) AdminGetMigrationBatch(ctx context.Context, req AdminMigrationBatchRequest) (json.RawMessage, error) {
	if strings.TrimSpace(req.BatchID) == "" {
		return nil, fmt.Errorf("batch id is required")
	}
	return c.adminMigrationRequest(ctx, http.MethodGet, "/v1/admin/migration/batches/"+url.PathEscape(req.BatchID), nil, nil, req.Credentials)
}

// AdminStartMigration starts all prepared tenants in a batch.
func (c *Client) AdminStartMigration(ctx context.Context, req AdminMigrationBatchRequest) (json.RawMessage, error) {
	return c.adminMigrationBatchAction(ctx, "start", req)
}

// AdminPauseMigration pauses a running batch.
func (c *Client) AdminPauseMigration(ctx context.Context, req AdminMigrationBatchRequest) (json.RawMessage, error) {
	return c.adminMigrationBatchAction(ctx, "pause", req)
}

// AdminResumeMigration resumes a paused batch.
func (c *Client) AdminResumeMigration(ctx context.Context, req AdminMigrationBatchRequest) (json.RawMessage, error) {
	return c.adminMigrationBatchAction(ctx, "resume", req)
}

func (c *Client) adminMigrationBatchAction(ctx context.Context, action string, req AdminMigrationBatchRequest) (json.RawMessage, error) {
	if strings.TrimSpace(req.BatchID) == "" {
		return nil, fmt.Errorf("batch id is required")
	}
	body := struct {
		BatchID string `json:"batch_id"`
	}{BatchID: req.BatchID}
	return c.adminMigrationRequest(ctx, http.MethodPost, "/v1/admin/migration/"+action, nil, body, req.Credentials)
}

// AdminRollbackMigration rolls a switched tenant back before the target has
// accepted a write. The tenant is carried in both the query and the body: the
// server validates the pair and rewrites the forwarded body to the admitted
// tenant.
func (c *Client) AdminRollbackMigration(ctx context.Context, req AdminMigrationRollbackRequest) (json.RawMessage, error) {
	if strings.TrimSpace(req.TenantID) == "" {
		return nil, fmt.Errorf("tenant id is required")
	}
	query := url.Values{"tenant_id": []string{req.TenantID}}
	body := struct {
		TenantID string `json:"tenant_id"`
	}{TenantID: req.TenantID}
	return c.adminMigrationRequest(ctx, http.MethodPost, "/v1/admin/migration/rollback", query, body, req.Credentials)
}

// AdminListMigrationTenants lists controller-known tenants.
func (c *Client) AdminListMigrationTenants(ctx context.Context, req AdminMigrationTenantListRequest) (json.RawMessage, error) {
	query := url.Values{}
	if req.BatchID != "" {
		query.Set("batch_id", req.BatchID)
	}
	if req.State != "" {
		query.Set("state", req.State)
	}
	if req.TenantID != "" {
		query.Set("tenant_id", req.TenantID)
	}
	if len(req.TenantIDs) > 0 {
		query.Set("tenant_ids", strings.Join(req.TenantIDs, ","))
	}
	if req.Page > 0 {
		query.Set("page", strconv.Itoa(req.Page))
	}
	if req.PageSize > 0 {
		query.Set("page_size", strconv.Itoa(req.PageSize))
	}
	if req.Limit > 0 {
		query.Set("limit", strconv.Itoa(req.Limit))
	}
	return c.adminMigrationRequest(ctx, http.MethodGet, "/v1/admin/migration/tenants", query, nil, req.Credentials)
}

// AdminGetMigrationTenant returns one controller-known tenant.
func (c *Client) AdminGetMigrationTenant(ctx context.Context, req AdminMigrationTenantRequest) (json.RawMessage, error) {
	if strings.TrimSpace(req.TenantID) == "" {
		return nil, fmt.Errorf("tenant id is required")
	}
	return c.adminMigrationRequest(ctx, http.MethodGet, "/v1/admin/migration/tenants/"+url.PathEscape(req.TenantID), nil, nil, req.Credentials)
}

// AdminGetMigrationTenantState reads the tenant's local migration fence state
// (status, epoch, quota_reconciled) from drive9-server. This is the local view,
// not the controller's richer migration state.
func (c *Client) AdminGetMigrationTenantState(ctx context.Context, req AdminMigrationTenantRequest) (json.RawMessage, error) {
	if strings.TrimSpace(req.TenantID) == "" {
		return nil, fmt.Errorf("tenant id is required")
	}
	return c.adminMigrationRequest(ctx, http.MethodGet, "/v1/admin/migration/tenants/"+url.PathEscape(req.TenantID)+"/state", nil, nil, req.Credentials)
}

func (c *Client) adminMigrationRequest(ctx context.Context, method, path string, query url.Values, body any, creds AdminMigrationCredentials) (json.RawMessage, error) {
	target := c.baseURL + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal migration request: %w", err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return nil, fmt.Errorf("create migration request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	creds.setHeaders(req)
	resp, err := c.do(req)
	if err != nil {
		return nil, fmt.Errorf("migration request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read migration response: %w", err)
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil, nil
	}
	if !json.Valid(raw) {
		return nil, fmt.Errorf("migration response is not JSON: %s", truncateForError(raw))
	}
	return json.RawMessage(raw), nil
}

// WatchAdminMigrationEvents streams controller events through the drive9
// migration admin SSE endpoint. It reconnects with bounded exponential backoff
// until ctx is canceled or the handler returns an error. A 4xx response is not
// retried: the caller's plan/admission/credential is wrong and retrying would
// only repeat the same failure.
func (c *Client) WatchAdminMigrationEvents(ctx context.Context, req AdminMigrationWatchRequest, handler func(AdminMigrationEvent) error) error {
	if handler == nil {
		return fmt.Errorf("migration event handler is required")
	}
	backoff := time.Second
	for {
		err := c.streamAdminMigrationEvents(ctx, req, handler)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var handlerErr *migrationEventHandlerError
		if errors.As(err, &handlerErr) {
			return handlerErr.err
		}
		var statusErr *StatusError
		if errors.As(err, &statusErr) && statusErr.StatusCode >= 400 && statusErr.StatusCode < 500 {
			return err
		}
		if err != nil && !isRetryableMigrationStreamError(err) {
			return err
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		if err == nil {
			backoff = time.Second
		} else if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}

func isRetryableMigrationStreamError(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var statusErr *StatusError
	if errors.As(err, &statusErr) {
		return statusErr.StatusCode >= 500
	}
	return true
}

func (c *Client) streamAdminMigrationEvents(ctx context.Context, req AdminMigrationWatchRequest, handler func(AdminMigrationEvent) error) error {
	query := url.Values{}
	if req.BatchID != "" {
		query.Set("batch_id", req.BatchID)
	}
	if req.TenantID != "" {
		query.Set("tenant_id", req.TenantID)
	}
	if len(req.TenantIDs) > 0 {
		query.Set("tenant_ids", strings.Join(req.TenantIDs, ","))
	}
	target := c.baseURL + "/v1/admin/migration/events"
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("create migration event request: %w", err)
	}
	httpReq.Header.Set("Accept", "text/event-stream")
	req.Credentials.setHeaders(httpReq)
	resp, err := c.do(httpReq)
	if err != nil {
		return fmt.Errorf("migration event connect: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return readError(resp)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 1<<20)
	var eventType string
	var dataLines []string
	dispatch := func() error {
		if len(dataLines) == 0 {
			eventType = ""
			return nil
		}
		event := AdminMigrationEvent{Type: eventType, Data: json.RawMessage(strings.Join(dataLines, "\n"))}
		eventType = ""
		dataLines = nil
		if err := handler(event); err != nil {
			return &migrationEventHandlerError{err: err}
		}
		return nil
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if err := dispatch(); err != nil {
				return err
			}
			continue
		}
		if strings.HasPrefix(line, "event:") {
			eventType = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			continue
		}
		if strings.HasPrefix(line, "data:") {
			value := strings.TrimPrefix(line, "data:")
			value = strings.TrimPrefix(value, " ")
			dataLines = append(dataLines, value)
			continue
		}
		// id:, retry:, and comment lines are intentionally ignored.
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read migration event stream: %w", err)
	}
	return dispatch()
}

func truncateForError(raw []byte) string {
	const max = 256
	if len(raw) <= max {
		return string(raw)
	}
	return string(raw[:max]) + "..."
}

// migrationEventHandlerError distinguishes a caller-stopping handler failure
// from a transport failure that WatchAdminMigrationEvents may retry.
type migrationEventHandlerError struct {
	err error
}

func (e *migrationEventHandlerError) Error() string { return e.err.Error() }
func (e *migrationEventHandlerError) Unwrap() error { return e.err }
