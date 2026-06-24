package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
)

type AdminTenant struct {
	TenantID string            `json:"tenant_id"`
	Status   string            `json:"status"`
	Kind     string            `json:"kind"`
	Quota    *AdminTenantQuota `json:"quota,omitempty"`
}

type AdminTenantQuota struct {
	Config QuotaConfig `json:"config"`
	Usage  QuotaUsage  `json:"usage"`
}

type AdminTenantListRequest struct {
	PublicKey    string
	PrivateKey   string
	PageSize     int
	Page         int
	IncludeQuota bool
}

type AdminTenantListResponse struct {
	Tenants  []AdminTenant `json:"tenants"`
	Page     int           `json:"page"`
	PageSize int           `json:"page_size"`
	NextPage int           `json:"next_page,omitempty"`
}

type AdminTenantCreateRequest struct {
	PublicKey              string `json:"public_key"`
	PrivateKey             string `json:"private_key"`
	MaxStorageSize         *int64 `json:"max_storage_size,omitempty"`
	MaxFileSize            *int64 `json:"max_file_size,omitempty"`
	MaxFileCount           *int64 `json:"max_file_count,omitempty"`
	TiDBCloudSpendingLimit *int64 `json:"tidbcloud_spending_limit,omitempty"`
}

type AdminTenantCreateResponse struct {
	TenantID      string `json:"tenant_id"`
	APIKey        string `json:"api_key"`
	Status        string `json:"status"`
	CloudProvider string `json:"cloud_provider,omitempty"`
	Region        string `json:"region,omitempty"`
}

type AdminTenantDeleteRequest struct {
	TenantID   string `json:"tenant_id"`
	PublicKey  string `json:"public_key"`
	PrivateKey string `json:"private_key"`
}

type AdminTenantDeleteResponse struct {
	TenantID string `json:"tenant_id"`
	Status   string `json:"status"`
}

func (c *Client) AdminListTenants(ctx context.Context, query AdminTenantListRequest) (*AdminTenantListResponse, error) {
	values := url.Values{}
	if query.PageSize > 0 {
		values.Set("page_size", strconv.Itoa(query.PageSize))
	}
	if query.Page > 0 {
		values.Set("page", strconv.Itoa(query.Page))
	}
	if query.IncludeQuota {
		values.Set("include_quota", "true")
	}
	u := c.baseURL + "/v1/admin/tenants"
	if encoded := values.Encode(); encoded != "" {
		u += "?" + encoded
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	setQuotaHeaders(req, query.PublicKey, query.PrivateKey)
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out AdminTenantListResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode admin tenant list response: %w", err)
	}
	return &out, nil
}

func (c *Client) AdminCreateTenant(ctx context.Context, req AdminTenantCreateRequest) (*AdminTenantCreateResponse, error) {
	raw, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal admin tenant create request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/admin/tenants", bytes.NewReader(raw))
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
	var out AdminTenantCreateResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode admin tenant create response: %w", err)
	}
	return &out, nil
}

func (c *Client) AdminGetTenant(ctx context.Context, query QuotaRequest) (*AdminTenant, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/admin/tenants/"+url.PathEscape(query.TenantID), nil)
	if err != nil {
		return nil, err
	}
	setQuotaHeaders(req, query.PublicKey, query.PrivateKey)
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out AdminTenant
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode admin tenant get response: %w", err)
	}
	return &out, nil
}

func (c *Client) AdminDeleteTenant(ctx context.Context, req AdminTenantDeleteRequest) (*AdminTenantDeleteResponse, error) {
	raw, err := json.Marshal(map[string]string{
		"public_key":  req.PublicKey,
		"private_key": req.PrivateKey,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal admin tenant delete request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL+"/v1/admin/tenants/"+url.PathEscape(req.TenantID), bytes.NewReader(raw))
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
	var out AdminTenantDeleteResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode admin tenant delete response: %w", err)
	}
	return &out, nil
}

func (c *Client) AdminSetTenantQuota(ctx context.Context, req QuotaSetRequest) (*QuotaResponse, error) {
	body := struct {
		PublicKey              string `json:"public_key"`
		PrivateKey             string `json:"private_key"`
		MaxStorageSize         *int64 `json:"max_storage_size,omitempty"`
		MaxFileSize            *int64 `json:"max_file_size,omitempty"`
		MaxFileCount           *int64 `json:"max_file_count,omitempty"`
		TiDBCloudSpendingLimit *int64 `json:"tidbcloud_spending_limit,omitempty"`
	}{
		PublicKey:              req.PublicKey,
		PrivateKey:             req.PrivateKey,
		MaxStorageSize:         req.MaxStorageSize,
		MaxFileSize:            req.MaxFileSize,
		MaxFileCount:           req.MaxFileCount,
		TiDBCloudSpendingLimit: req.TiDBCloudSpendingLimit,
	}
	return c.postQuota(ctx, "/v1/admin/tenants/"+url.PathEscape(req.TenantID)+"/quota", body, "admin tenant quota set")
}
