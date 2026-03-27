package tenant

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	envDB9APIURL = "DAT9_DB9_API_URL"
	envDB9APIKey = "DAT9_DB9_API_KEY"
)

type DB9Provisioner struct {
	baseURL string
	apiKey  string
	client  *http.Client
}

func NewDB9ProvisionerFromEnv() (*DB9Provisioner, error) {
	base := os.Getenv(envDB9APIURL)
	key := os.Getenv(envDB9APIKey)
	if base == "" || key == "" {
		return nil, fmt.Errorf("%s and %s are required", envDB9APIURL, envDB9APIKey)
	}
	return &DB9Provisioner{baseURL: strings.TrimRight(base, "/"), apiKey: key, client: &http.Client{Timeout: 30 * time.Second}}, nil
}

func (p *DB9Provisioner) ProviderType() string { return ProviderDB9 }

func (p *DB9Provisioner) Provision(ctx context.Context, tenantID string) (*ClusterInfo, error) {
	type reqBody struct {
		TenantID string `json:"tenant_id"`
	}
	type respBody struct {
		ClusterID string `json:"cluster_id"`
		Host      string `json:"host"`
		Port      int    `json:"port"`
		Username  string `json:"username"`
		Password  string `json:"password"`
		DBName    string `json:"db_name"`
	}
	payload, _ := json.Marshal(reqBody{TenantID: tenantID})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/instances", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.apiKey)

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("db9 provision status %d: %s", resp.StatusCode, string(body))
	}
	var out respBody
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	if out.Host == "" || out.Port == 0 || out.Username == "" || out.Password == "" || out.DBName == "" {
		return nil, fmt.Errorf("db9 response missing required connection fields")
	}
	return &ClusterInfo{
		TenantID:  tenantID,
		ClusterID: out.ClusterID,
		Host:      out.Host,
		Port:      out.Port,
		Username:  out.Username,
		Password:  out.Password,
		DBName:    out.DBName,
		Provider:  ProviderDB9,
	}, nil
}
