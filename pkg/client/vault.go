package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// VaultSecret holds secret metadata returned by the management API.
type VaultSecret struct {
	Name       string    `json:"name"`
	SecretType string    `json:"secret_type"`
	Revision   int64     `json:"revision"`
	CreatedBy  string    `json:"created_by"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// VaultTokenIssueResponse is returned when issuing a scoped capability token.
type VaultTokenIssueResponse struct {
	Token     string    `json:"token"`
	TokenID   string    `json:"token_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

// VaultAuditEvent is an audit event returned by the vault audit API.
type VaultAuditEvent struct {
	EventID    string         `json:"event_id"`
	EventType  string         `json:"event_type"`
	TokenID    string         `json:"token_id,omitempty"`
	AgentID    string         `json:"agent_id,omitempty"`
	TaskID     string         `json:"task_id,omitempty"`
	SecretName string         `json:"secret_name,omitempty"`
	FieldName  string         `json:"field_name,omitempty"`
	Adapter    string         `json:"adapter,omitempty"`
	Detail     map[string]any `json:"detail,omitempty"`
	Timestamp  time.Time      `json:"timestamp"`
}

func (c *Client) vaultURL(path string) string {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return c.baseURL + "/v1/vault" + path
}

// CreateVaultSecret creates a new secret via the management API.
func (c *Client) CreateVaultSecret(ctx context.Context, name string, fields map[string]string) (*VaultSecret, error) {
	body, err := json.Marshal(map[string]any{
		"name":       name,
		"fields":     fields,
		"created_by": "drive9-cli",
	})
	if err != nil {
		return nil, fmt.Errorf("marshal secret create request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.vaultURL("/secrets"), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var sec VaultSecret
	if err := json.NewDecoder(resp.Body).Decode(&sec); err != nil {
		return nil, fmt.Errorf("decode secret create response: %w", err)
	}
	return &sec, nil
}

// UpdateVaultSecret rotates a secret via the management API.
func (c *Client) UpdateVaultSecret(ctx context.Context, name string, fields map[string]string) (*VaultSecret, error) {
	body, err := json.Marshal(map[string]any{
		"fields":     fields,
		"updated_by": "drive9-cli",
	})
	if err != nil {
		return nil, fmt.Errorf("marshal secret update request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.vaultURL("/secrets/"+url.PathEscape(name)), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var sec VaultSecret
	if err := json.NewDecoder(resp.Body).Decode(&sec); err != nil {
		return nil, fmt.Errorf("decode secret update response: %w", err)
	}
	return &sec, nil
}

// DeleteVaultSecret deletes a secret via the management API.
func (c *Client) DeleteVaultSecret(ctx context.Context, name string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.vaultURL("/secrets/"+url.PathEscape(name)), nil)
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return readError(resp)
	}
	return nil
}

// ListVaultSecrets lists secret metadata via the management API.
func (c *Client) ListVaultSecrets(ctx context.Context) ([]VaultSecret, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.vaultURL("/secrets"), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var result struct {
		Secrets []VaultSecret `json:"secrets"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode secret list response: %w", err)
	}
	if result.Secrets == nil {
		result.Secrets = []VaultSecret{}
	}
	return result.Secrets, nil
}

// IssueVaultToken issues a scoped capability token via the management API.
func (c *Client) IssueVaultToken(ctx context.Context, agentID, taskID string, scope []string, ttl time.Duration) (*VaultTokenIssueResponse, error) {
	ttlSeconds := int(ttl / time.Second)
	body, err := json.Marshal(map[string]any{
		"agent_id":    agentID,
		"task_id":     taskID,
		"scope":       scope,
		"ttl_seconds": ttlSeconds,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal token issue request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.vaultURL("/tokens"), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var result VaultTokenIssueResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode token issue response: %w", err)
	}
	return &result, nil
}

// RevokeVaultToken revokes a capability token via the management API.
func (c *Client) RevokeVaultToken(ctx context.Context, tokenID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.vaultURL("/tokens/"+url.PathEscape(tokenID)), nil)
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return readError(resp)
	}
	return nil
}

// VaultGrantIssueRequest is the wire payload sent to /v1/vault/grants.
// Fields mirror the server handler; principal_type defaults to "owner" when empty.
type VaultGrantIssueRequest struct {
	PrincipalType string   `json:"principal_type,omitempty"`
	Agent         string   `json:"agent"`
	Scope         []string `json:"scope"`
	Perm          string   `json:"perm"`
	TTLSeconds    int      `json:"ttl_seconds"`
	LabelHint     string   `json:"label_hint,omitempty"`
}

// VaultGrantIssueResponse is the decoded response from /v1/vault/grants.
// ExpiresAt is an RFC3339 timestamp; Token is the `vt_`-prefixed bearer string.
type VaultGrantIssueResponse struct {
	Token     string    `json:"token"`
	GrantID   string    `json:"grant_id"`
	ExpiresAt time.Time `json:"expires_at"`
	Scope     []string  `json:"scope"`
	Perm      string    `json:"perm"`
}

// IssueVaultGrant mints a vault grant via the management API. Requires the
// caller to be authenticated as the tenant owner (API key); delegated callers
// cannot reach this endpoint so re-delegation is blocked at the router layer.
func (c *Client) IssueVaultGrant(ctx context.Context, req VaultGrantIssueRequest) (*VaultGrantIssueResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal grant issue request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.vaultURL("/grants"), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.do(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var result VaultGrantIssueResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode grant issue response: %w", err)
	}
	return &result, nil
}

// RevokeVaultGrant revokes a vault grant by grant_id via the management API.
func (c *Client) RevokeVaultGrant(ctx context.Context, grantID, revokedBy, reason string) error {
	body, err := json.Marshal(map[string]any{
		"revoked_by": revokedBy,
		"reason":     reason,
	})
	if err != nil {
		return fmt.Errorf("marshal grant revoke request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.vaultURL("/grants/"+url.PathEscape(grantID)), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return readError(resp)
	}
	return nil
}

// QueryVaultAudit queries the audit log via the management API.
func (c *Client) QueryVaultAudit(ctx context.Context, secretName string, limit int) ([]VaultAuditEvent, error) {
	u, err := url.Parse(c.vaultURL("/audit"))
	if err != nil {
		return nil, err
	}
	q := u.Query()
	if secretName != "" {
		q.Set("secret", secretName)
	}
	if limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", limit))
	}
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var result struct {
		Events []VaultAuditEvent `json:"events"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode audit response: %w", err)
	}
	if result.Events == nil {
		result.Events = []VaultAuditEvent{}
	}
	return result.Events, nil
}

// ListReadableVaultSecrets enumerates secrets visible to the bearer capability token.
func (c *Client) ListReadableVaultSecrets(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.vaultURL("/read"), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var result struct {
		Secrets []string `json:"secrets"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode readable secret list response: %w", err)
	}
	if result.Secrets == nil {
		result.Secrets = []string{}
	}
	return result.Secrets, nil
}

// ReadVaultSecret reads all fields of a secret using the consumption API.
func (c *Client) ReadVaultSecret(ctx context.Context, name string) (map[string]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.vaultURL("/read/"+url.PathEscape(name)), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode secret read response: %w", err)
	}
	if result == nil {
		result = map[string]string{}
	}
	return result, nil
}

// ReadVaultSecretField reads a single field via the consumption API.
func (c *Client) ReadVaultSecretField(ctx context.Context, name, field string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.vaultURL("/read/"+url.PathEscape(name)+"/"+url.PathEscape(field)), nil)
	if err != nil {
		return "", err
	}
	resp, err := c.do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return "", readError(resp)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read field response: %w", err)
	}
	return string(data), nil
}
