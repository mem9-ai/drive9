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
	TiDBCloudSpendingLimit *int64 `json:"tidbcloud_spending_limit"`
}

// QuotaUsage is the tenant's current quota usage counters.
type QuotaUsage struct {
	StorageBytes   int64 `json:"storage_bytes"`
	ReservedBytes  int64 `json:"reserved_bytes"`
	MediaFileCount int64 `json:"media_file_count"`
	MonthlyCostMC  int64 `json:"monthly_cost_mc"`
}

// QuotaResponse is returned by all quota query and update APIs.
type QuotaResponse struct {
	TenantID       string      `json:"tenant_id"`
	Provider       string      `json:"provider"`
	Status         string      `json:"status"`
	SupportsUpdate bool        `json:"supports_update"`
	Config         QuotaConfig `json:"config"`
	Usage          QuotaUsage  `json:"usage"`
}

// QuotaRequest identifies a tidb_cloud_native tenant for quota lookup.
type QuotaRequest struct {
	TenantID   string `json:"tenant_id"`
	PublicKey  string `json:"public_key"`
	PrivateKey string `json:"private_key"`
}

// QuotaSetRequest updates cloud-native tenant quota with TiDB Cloud API
// credentials. MaxStorageSize is expressed in Mi. TiDBCloudSpendingLimit is
// the TiDB Cloud Cluster Spending Limit value passed through to TiDB Cloud.
type QuotaSetRequest struct {
	TenantID               string `json:"tenant_id"`
	PublicKey              string `json:"public_key"`
	PrivateKey             string `json:"private_key"`
	MaxStorageSize         *int64 `json:"max_storage_size,omitempty"`
	TiDBCloudSpendingLimit *int64 `json:"tidbcloud_spending_limit,omitempty"`
}

// GetQuota queries quota for a tidb_cloud_native tenant.
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

// SetQuota updates quota for a tidb_cloud_native tenant. Drive9 tenant API keys
// cannot authorize quota updates for their own tenant.
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
