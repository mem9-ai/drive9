package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

// QuotaConfig is the tenant quota configuration returned by the quota API.
type QuotaConfig struct {
	MaxStorageSize         int64  `json:"max_storage_size"`
	MaxFileSize            int64  `json:"max_file_size"`
	MaxFileCount           int64  `json:"max_file_count"`
	TiDBCloudSpendingLimit *int64 `json:"tidbcloud_spending_limit"`
}

// QuotaUsage is the tenant's current storage quota usage counters.
type QuotaUsage struct {
	StorageBytes  int64 `json:"storage_bytes"`
	ReservedBytes int64 `json:"reserved_bytes"`
	FileCount     int64 `json:"file_count"`
}

// QuotaResponse is returned by all quota query and update APIs.
type QuotaResponse struct {
	TenantID       string      `json:"tenant_id"`
	Provider       string      `json:"provider,omitempty"`
	Status         string      `json:"status"`
	SupportsUpdate bool        `json:"supports_update,omitempty"`
	Config         QuotaConfig `json:"config"`
	Usage          QuotaUsage  `json:"usage"`
}

// QuotaRequest identifies a tidb_cloud_native tenant for quota lookup.
type QuotaRequest struct {
	TenantID   string `json:"tenant_id"`
	PublicKey  string `json:"public_key"`
	PrivateKey string `json:"private_key"`
}

// QuotaSetRequest updates TiDBCloud Mode tenant quota with TiDB Cloud API
// credentials. MaxStorageSize and MaxFileSize are expressed in Mi.
// TiDBCloudSpendingLimit is the TiDB Cloud Cluster Spending Limit value passed
// through to TiDB Cloud.
type QuotaSetRequest struct {
	TenantID               string `json:"tenant_id"`
	PublicKey              string `json:"public_key"`
	PrivateKey             string `json:"private_key"`
	MaxStorageSize         *int64 `json:"max_storage_size,omitempty"`
	MaxFileSize            *int64 `json:"max_file_size,omitempty"`
	MaxFileCount           *int64 `json:"max_file_count,omitempty"`
	TiDBCloudSpendingLimit *int64 `json:"tidbcloud_spending_limit,omitempty"`
}

// GetQuota queries quota through the deprecated compatibility /v1/quota
// endpoint.
//
// Deprecated: use AdminGetTenant or AdminListTenants with IncludeQuota to read
// quota. The /v1/quota endpoint remains only for compatibility.
func (c *Client) GetQuota(ctx context.Context, query QuotaRequest) (*QuotaResponse, error) {
	values := url.Values{}
	values.Set("tenant_id", query.TenantID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/quota?"+values.Encode(), nil)
	if err != nil {
		return nil, err
	}
	setQuotaHeaders(req, query.PublicKey, query.PrivateKey)
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	return decodeQuotaResponse(resp, "quota get")
}

// SetQuota updates quota through the deprecated compatibility /v1/quota
// endpoint. Drive9 tenant API keys cannot authorize quota updates for their
// own tenant.
//
// Deprecated: use AdminSetTenantQuota.
func (c *Client) SetQuota(ctx context.Context, req QuotaSetRequest) (*QuotaResponse, error) {
	return c.postQuota(ctx, "/v1/quota", req, "quota set")
}

func setQuotaHeaders(req *http.Request, publicKey, privateKey string) {
	req.Header.Set("X-TiDBCloud-Public-Key", publicKey)
	req.Header.Set("X-TiDBCloud-Private-Key", privateKey)
}

func (c *Client) postQuota(ctx context.Context, path string, body any, action string) (*QuotaResponse, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal %s request: %w", action, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	return decodeQuotaResponse(resp, action)
}

func decodeQuotaResponse(resp *http.Response, action string) (*QuotaResponse, error) {
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out QuotaResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode %s response: %w", action, err)
	}
	return &out, nil
}
