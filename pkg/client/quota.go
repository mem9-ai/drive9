package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// QuotaConfig is the tenant quota configuration returned by the quota API.
type QuotaConfig struct {
	MaxStorageSize int64 `json:"max_storage_size"`
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

// QuotaCredentialRequest identifies a cloud-native tenant with TiDB Cloud API
// credentials for credential-based quota lookup.
type QuotaCredentialRequest struct {
	TenantID   string `json:"tenant_id"`
	PublicKey  string `json:"public_key"`
	PrivateKey string `json:"private_key"`
}

// QuotaSetRequest updates cloud-native tenant quota with TiDB Cloud API
// credentials. MaxStorageSize is expressed in Mi.
type QuotaSetRequest struct {
	TenantID       string `json:"tenant_id"`
	PublicKey      string `json:"public_key"`
	PrivateKey     string `json:"private_key"`
	MaxStorageSize *int64 `json:"max_storage_size,omitempty"`
}

// GetQuota queries quota for the tenant represented by the client's owner API
// key. Filesystem-scoped keys and delegated tokens are rejected server-side.
func (c *Client) GetQuota(ctx context.Context) (*QuotaResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/quota", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	return decodeQuotaResponse(resp, "quota get")
}

// QueryQuotaWithCredentials queries quota for a tidb_cloud_native tenant using
// TiDB Cloud API credentials and a Drive9 tenant id.
func (c *Client) QueryQuotaWithCredentials(ctx context.Context, req QuotaCredentialRequest) (*QuotaResponse, error) {
	return c.postQuota(ctx, "/v1/quota/query", req, "quota query")
}

// SetQuotaWithCredentials updates quota for a tidb_cloud_native tenant using
// TiDB Cloud API credentials and a Drive9 tenant id. Drive9 tenant API keys
// cannot authorize quota updates for their own tenant.
func (c *Client) SetQuotaWithCredentials(ctx context.Context, req QuotaSetRequest) (*QuotaResponse, error) {
	return c.postQuota(ctx, "/v1/quota", req, "quota set")
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
