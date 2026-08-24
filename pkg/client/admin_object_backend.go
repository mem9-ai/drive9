package client

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
)

type AdminObjectBackend struct {
	ID             string `json:"id"`
	OrganizationID string `json:"organization_id"`
	Scheme         string `json:"scheme"`
	Endpoint       string `json:"endpoint"`
	Region         string `json:"region"`
	ForcePathStyle bool   `json:"force_path_style"`
	Bucket         string `json:"bucket"`
	Prefix         string `json:"prefix"`
	CredentialKind string `json:"credential_kind"`
	RoleARN        string `json:"role_arn"`
	AccessKeyID    string `json:"access_key_id,omitempty"`
	HasSecret      bool   `json:"has_secret"`
	HasExternalID  bool   `json:"has_external_id"`
	MaxSessionTTL  int    `json:"max_session_ttl_sec"`
}

type AdminObjectBackendCreateRequest struct {
	PublicKey       string `json:"public_key"`
	PrivateKey      string `json:"private_key"`
	Scheme          string `json:"scheme"`
	Endpoint        string `json:"endpoint,omitempty"`
	Region          string `json:"region,omitempty"`
	ForcePathStyle  bool   `json:"force_path_style,omitempty"`
	Bucket          string `json:"bucket"`
	Prefix          string `json:"prefix,omitempty"`
	CredentialKind  string `json:"credential_kind,omitempty"`
	RoleARN         string `json:"role_arn,omitempty"`
	AccessKeyID     string `json:"access_key_id,omitempty"`
	SecretAccessKey string `json:"secret_access_key,omitempty"`
	ExternalID      string `json:"external_id,omitempty"`
	MaxSessionTTL   int    `json:"max_session_ttl_sec,omitempty"`
}

func (c *Client) AdminListObjectBackends(ctx context.Context, publicKey, privateKey string) ([]AdminObjectBackend, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/admin/object-backends", nil)
	if err != nil {
		return nil, err
	}
	setQuotaHeaders(req, publicKey, privateKey)
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out struct {
		Backends []AdminObjectBackend `json:"backends"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out.Backends, nil
}

func (c *Client) AdminCreateObjectBackend(ctx context.Context, in AdminObjectBackendCreateRequest) (*AdminObjectBackend, error) {
	raw, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/admin/object-backends", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	setQuotaHeaders(req, in.PublicKey, in.PrivateKey)
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out AdminObjectBackend
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) AdminDeleteObjectBackend(ctx context.Context, id, publicKey, privateKey string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL+"/v1/admin/object-backends/"+url.PathEscape(id), nil)
	if err != nil {
		return err
	}
	setQuotaHeaders(req, publicKey, privateKey)
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

type AdminObjectNamespace struct {
	TenantID    string `json:"tenant_id"`
	NamespaceID string `json:"namespace_id"`
}

func (c *Client) AdminGetObjectNamespace(ctx context.Context, tenantID, publicKey, privateKey string) (*AdminObjectNamespace, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/admin/tenants/"+url.PathEscape(tenantID)+"/object-namespace", nil)
	if err != nil {
		return nil, err
	}
	setQuotaHeaders(req, publicKey, privateKey)
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out AdminObjectNamespace
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) AdminSetObjectNamespace(ctx context.Context, tenantID, namespaceID, publicKey, privateKey string) (*AdminObjectNamespace, error) {
	raw, err := json.Marshal(map[string]string{
		"public_key":   publicKey,
		"private_key":  privateKey,
		"namespace_id": namespaceID,
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.baseURL+"/v1/admin/tenants/"+url.PathEscape(tenantID)+"/object-namespace", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	setQuotaHeaders(req, publicKey, privateKey)
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out AdminObjectNamespace
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) AdminClearObjectNamespace(ctx context.Context, tenantID, publicKey, privateKey string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL+"/v1/admin/tenants/"+url.PathEscape(tenantID)+"/object-namespace", nil)
	if err != nil {
		return err
	}
	setQuotaHeaders(req, publicKey, privateKey)
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
