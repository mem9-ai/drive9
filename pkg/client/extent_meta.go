package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/mem9-ai/drive9/pkg/extent"
)

// ExtentMeta calls POST /v1/extent/meta and returns the JSON body.
func (c *Client) ExtentMeta(ctx context.Context, op string, req any) ([]byte, error) {
	var body io.Reader
	if req != nil {
		raw, err := json.Marshal(req)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(raw)
	} else {
		body = bytes.NewReader([]byte("{}"))
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/extent/meta", body)
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Drive9-Extent-Op", op)
	resp, err := c.do(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("extent meta %s: HTTP %d: %s", op, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return raw, nil
}

// DataCredential is the tenant-prefix object-store session returned by
// POST /v1/data-credential.
type DataCredential = extent.Credential

func (c *Client) GetDataCredential(ctx context.Context) (*DataCredential, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/data-credential", bytes.NewReader([]byte("{}")))
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
	var out DataCredential
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}
