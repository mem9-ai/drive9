package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
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
type DataCredential struct {
	Scheme          string    `json:"scheme"`
	Endpoint        string    `json:"endpoint"`
	Bucket          string    `json:"bucket,omitempty"`
	Prefix          string    `json:"prefix"`
	AccessKeyID     string    `json:"access_key_id,omitempty"`
	SecretAccessKey string    `json:"secret_access_key,omitempty"`
	SessionToken    string    `json:"session_token,omitempty"`
	Region          string    `json:"region,omitempty"`
	ForcePathStyle  bool      `json:"force_path_style,omitempty"`
	ExpiresAt       string `json:"expires_at,omitempty"`
}

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

func (c *Client) PutExtentBlock(ctx context.Context, key string, r io.Reader, size int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.extentBlockURL(key), r)
	if err != nil {
		return err
	}
	if size >= 0 {
		req.ContentLength = size
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

func (c *Client) GetExtentBlock(ctx context.Context, key string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.extentBlockURL(key), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		defer func() { _ = resp.Body.Close() }()
		return nil, readError(resp)
	}
	return resp.Body, nil
}

func (c *Client) DeleteExtentBlock(ctx context.Context, key string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.extentBlockURL(key), nil)
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

func (c *Client) extentBlockURL(key string) string {
	key = strings.TrimPrefix(key, "/")
	return c.baseURL + "/v1/extent/blocks/" + key
}
