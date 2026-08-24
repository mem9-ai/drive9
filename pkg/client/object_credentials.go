package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// ObjectCredentials is a short-lived object-store session minted by the server.
type ObjectCredentials struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	SessionToken    string `json:"session_token,omitempty"`
	Expiration      string `json:"expiration,omitempty"`
	Endpoint        string `json:"endpoint,omitempty"`
	Region          string `json:"region,omitempty"`
	ForcePathStyle  bool   `json:"force_path_style"`
	Scheme          string `json:"scheme"`
	Bucket          string `json:"bucket"`
	Prefix          string `json:"prefix"`
}

// MintObjectCredentials asks drive9-server for STS credentials scoped to uri.
func (c *Client) MintObjectCredentials(ctx context.Context, uri string, write bool) (*ObjectCredentials, error) {
	body, err := json.Marshal(map[string]any{"uri": uri, "write": write})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/object-credentials", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return nil, fmt.Errorf("mint object credentials: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out ObjectCredentials
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode object credentials: %w", err)
	}
	if out.AccessKeyID == "" || out.SecretAccessKey == "" {
		return nil, fmt.Errorf("server returned empty object credentials")
	}
	return &out, nil
}
