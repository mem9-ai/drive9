package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// AdminTenantLogsRequest selects one tenant's logs. The server resolves the
// environment, organization and tenant label filters; none of them is taken
// from this request.
type AdminTenantLogsRequest struct {
	TenantID   string
	PublicKey  string
	PrivateKey string
	// Keyword is an optional line-contains filter.
	Keyword string
	// TimeRange is a Go duration string (e.g. "20m"). Empty uses the server
	// default (5m); the server rejects values above 30m.
	TimeRange string
	// Limit caps the number of entries. Zero uses the server default (100).
	Limit int
}

// AdminLogEntry is one returned log line.
type AdminLogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Line      string    `json:"line"`
}

// AdminTenantLogsResponse is the admin log query result.
type AdminTenantLogsResponse struct {
	TenantID  string          `json:"tenant_id"`
	Keyword   string          `json:"keyword,omitempty"`
	TimeRange string          `json:"time_range"`
	Start     time.Time       `json:"start"`
	End       time.Time       `json:"end"`
	Limit     int             `json:"limit"`
	Count     int             `json:"count"`
	Entries   []AdminLogEntry `json:"entries"`
}

// AdminGetTenantLogs queries one tenant's logs (newest first).
func (c *Client) AdminGetTenantLogs(ctx context.Context, query AdminTenantLogsRequest) (*AdminTenantLogsResponse, error) {
	tenantID := strings.TrimSpace(query.TenantID)
	if tenantID == "" {
		return nil, fmt.Errorf("tenant id is required")
	}
	params := url.Values{}
	if query.Keyword != "" {
		params.Set("keyword", query.Keyword)
	}
	if timeRange := strings.TrimSpace(query.TimeRange); timeRange != "" {
		params.Set("time_range", timeRange)
	}
	if query.Limit > 0 {
		params.Set("limit", strconv.Itoa(query.Limit))
	}
	target := c.baseURL + "/v1/admin/tenants/" + url.PathEscape(tenantID) + "/logs"
	if encoded := params.Encode(); encoded != "" {
		target += "?" + encoded
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, fmt.Errorf("create admin tenant logs request: %w", err)
	}
	setQuotaHeaders(req, query.PublicKey, query.PrivateKey)
	resp, err := c.do(req)
	if err != nil {
		return nil, fmt.Errorf("admin tenant logs request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out AdminTenantLogsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode admin tenant logs response: %w", err)
	}
	return &out, nil
}
