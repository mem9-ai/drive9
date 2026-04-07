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

const envZeroAPIURL = "DRIVE9_ZERO_API_URL"

type ZeroProvisioner struct {
	baseURL string
	client  *http.Client
}

func NewZeroProvisionerFromEnv() (*ZeroProvisioner, error) {
	base := os.Getenv(envZeroAPIURL)
	if base == "" {
		return nil, fmt.Errorf("%s is required", envZeroAPIURL)
	}
	return &ZeroProvisioner{baseURL: strings.TrimRight(base, "/"), client: &http.Client{Timeout: 30 * time.Second}}, nil
}

func (p *ZeroProvisioner) ProviderType() string { return ProviderTiDBZero }

func (p *ZeroProvisioner) InitSchema(_ context.Context, dsn string) error {
	return initZeroSchema(dsn)
}

func (p *ZeroProvisioner) Provision(ctx context.Context, tenantID string) (*ClusterInfo, error) {
	type reqBody struct {
		Tag string `json:"tag"`
	}
	type respBody struct {
		Instance struct {
			ID         string `json:"id"`
			ExpiresAt  string `json:"expiresAt"`
			Connection struct {
				Host     string `json:"host"`
				Port     int    `json:"port"`
				Username string `json:"username"`
				Password string `json:"password"`
			} `json:"connection"`
			ClaimInfo struct {
				ClaimURL string `json:"claimUrl"`
			} `json:"claimInfo"`
		} `json:"instance"`
	}

	payload, _ := json.Marshal(reqBody{Tag: "dat9"})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/instances", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("zero provision status %d: %s", resp.StatusCode, string(body))
	}

	var out respBody
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	if out.Instance.Connection.Host == "" || out.Instance.Connection.Port == 0 {
		return nil, fmt.Errorf("zero provision response missing connection info")
	}

	var claimExp *time.Time
	if out.Instance.ExpiresAt != "" {
		if t, err := time.Parse(time.RFC3339, out.Instance.ExpiresAt); err == nil {
			t = t.UTC()
			claimExp = &t
		}
	}

	return &ClusterInfo{
		TenantID:       tenantID,
		ClusterID:      out.Instance.ID,
		Host:           out.Instance.Connection.Host,
		Port:           out.Instance.Connection.Port,
		Username:       out.Instance.Connection.Username,
		Password:       out.Instance.Connection.Password,
		DBName:         "test",
		Provider:       ProviderTiDBZero,
		ClaimURL:       out.Instance.ClaimInfo.ClaimURL,
		ClaimExpiresAt: claimExp,
	}, nil
}
