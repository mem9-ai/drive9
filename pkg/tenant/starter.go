package tenant

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/meta"
)

// TiDB Cloud Starter API configuration environment variables.
const (
	envTiDBAPIKey    = "DAT9_TIDBCLOUD_API_KEY"    // public key for digest auth
	envTiDBAPISecret = "DAT9_TIDBCLOUD_API_SECRET" // private key for digest auth
	envTiDBPoolID    = "DAT9_TIDBCLOUD_POOL_ID"    // pre-provisioned cluster pool ID

	tidbStarterBaseURL = "https://serverless.tidbapi.com"
)

// TiDBStarterProvisioner acquires clusters from the TiDB Cloud Starter pool.
type TiDBStarterProvisioner struct {
	apiKey    string
	apiSecret string
	poolID    string
	baseURL   string
	client    *http.Client
}

// NewTiDBStarterFromEnv creates a TiDB Starter provisioner from environment variables.
func NewTiDBStarterFromEnv() (*TiDBStarterProvisioner, error) {
	apiKey := os.Getenv(envTiDBAPIKey)
	apiSecret := os.Getenv(envTiDBAPISecret)
	poolID := os.Getenv(envTiDBPoolID)

	if apiKey == "" || apiSecret == "" {
		return nil, fmt.Errorf("%s and %s must be set", envTiDBAPIKey, envTiDBAPISecret)
	}
	if poolID == "" {
		return nil, fmt.Errorf("%s must be set", envTiDBPoolID)
	}

	return &TiDBStarterProvisioner{
		apiKey:    apiKey,
		apiSecret: apiSecret,
		poolID:    poolID,
		baseURL:   tidbStarterBaseURL,
		client:    &http.Client{Timeout: 60 * time.Second},
	}, nil
}

func (p *TiDBStarterProvisioner) ProviderType() string { return "tidb_starter" }

// Provision takes over a pre-provisioned cluster from the TiDB Cloud pool.
func (p *TiDBStarterProvisioner) Provision(ctx context.Context) (*ClusterInfo, error) {
	password, err := generateRandomPassword(16)
	if err != nil {
		return nil, fmt.Errorf("generate password: %w", err)
	}

	reqBody := takeoverRequest{
		PoolID:       p.poolID,
		RootPassword: password,
	}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	url := p.baseURL + "/v1beta1/clusters:takeoverFromPool"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(p.apiKey, p.apiSecret)

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("tidb starter API call: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("tidb starter API returned %d: %s", resp.StatusCode, string(respBody))
	}

	var result takeoverResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	host, port := parseEndpoint(result.Endpoints.Public)

	return &ClusterInfo{
		ClusterID: result.ClusterID,
		Host:      host,
		Port:      port,
		Username:  result.UserPrefix + ".root",
		Password:  password,
		DBName:    "test",            // TiDB Starter default database
		TLSMode:   TLSConfigName(),   // TiDB Cloud requires TLS
	}, nil
}

// InitSchema runs dat9 meta store migrations on the provisioned cluster.
// meta.Open internally runs all table creation statements.
func (p *TiDBStarterProvisioner) InitSchema(_ context.Context, dsn string) error {
	store, err := meta.Open(dsn)
	if err != nil {
		return fmt.Errorf("init schema: %w", err)
	}
	store.Close()
	return nil
}

// --- request/response types ---

type takeoverRequest struct {
	PoolID       string `json:"pool_id"`
	RootPassword string `json:"root_password"`
}

type takeoverResponse struct {
	ClusterID  string `json:"cluster_id"`
	UserPrefix string `json:"user_prefix"`
	Endpoints  struct {
		Public string `json:"public"` // "gateway01.us-east-1.prod.aws.tidbcloud.com:4000"
	} `json:"endpoints"`
}

// --- helpers ---

func generateRandomPassword(length int) (string, error) {
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func parseEndpoint(endpoint string) (host string, port int) {
	host = endpoint
	port = 4000 // TiDB default
	if idx := strings.LastIndex(endpoint, ":"); idx >= 0 {
		host = endpoint[:idx]
		fmt.Sscanf(endpoint[idx+1:], "%d", &port)
	}
	return
}
