package tenant

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	envTiDBAPIURL    = "DAT9_TIDBCLOUD_API_URL"
	envTiDBAPIKey    = "DAT9_TIDBCLOUD_API_KEY"
	envTiDBAPISecret = "DAT9_TIDBCLOUD_API_SECRET"
	envTiDBPoolID    = "DAT9_TIDBCLOUD_POOL_ID"
)

type StarterProvisioner struct {
	apiURL    string
	apiKey    string
	apiSecret string
	poolID    string
	client    *http.Client
}

func NewStarterProvisionerFromEnv() (*StarterProvisioner, error) {
	apiURL := os.Getenv(envTiDBAPIURL)
	apiKey := os.Getenv(envTiDBAPIKey)
	apiSecret := os.Getenv(envTiDBAPISecret)
	poolID := os.Getenv(envTiDBPoolID)
	if apiURL == "" || apiKey == "" || apiSecret == "" || poolID == "" {
		return nil, fmt.Errorf("%s, %s, %s and %s are required", envTiDBAPIURL, envTiDBAPIKey, envTiDBAPISecret, envTiDBPoolID)
	}
	return &StarterProvisioner{
		apiURL:    strings.TrimRight(apiURL, "/"),
		apiKey:    apiKey,
		apiSecret: apiSecret,
		poolID:    poolID,
		client:    &http.Client{Timeout: 60 * time.Second},
	}, nil
}

func (p *StarterProvisioner) ProviderType() string { return ProviderTiDBCloudStarter }

// InitSchema is intentionally a no-op for TiDB Cloud Starter.
//
// Starter tenants come from an externally initialized cluster/pool whose
// database bootstrap and schema setup happen outside this repository. Returning
// nil here means "schema is managed externally", not "schema initialization was
// forgotten".
func (p *StarterProvisioner) InitSchema(_ context.Context, _ string) error {
	return nil
}

func (p *StarterProvisioner) Provision(ctx context.Context, tenantID string) (*ClusterInfo, error) {
	password, err := generateRandomPassword(24)
	if err != nil {
		return nil, err
	}
	body, _ := json.Marshal(map[string]string{"pool_id": p.poolID, "root_password": password})
	endpoint := p.apiURL + "/v1beta1/clusters:takeoverFromPool"
	resp, err := p.doDigestAuthRequest(ctx, http.MethodPost, endpoint, body)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("starter provision status %d: %s", resp.StatusCode, string(raw))
	}

	var out struct {
		ClusterID string `json:"clusterId"`
		Endpoints struct {
			Public struct {
				Host string `json:"host"`
				Port int    `json:"port"`
			} `json:"public"`
		} `json:"endpoints"`
		UserPrefix string `json:"userPrefix"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	if out.Endpoints.Public.Host == "" || out.Endpoints.Public.Port == 0 {
		return nil, fmt.Errorf("starter response missing endpoint")
	}

	return &ClusterInfo{
		TenantID:  tenantID,
		ClusterID: out.ClusterID,
		Host:      out.Endpoints.Public.Host,
		Port:      out.Endpoints.Public.Port,
		Username:  out.UserPrefix + ".root",
		Password:  password,
		DBName:    "test",
		Provider:  ProviderTiDBCloudStarter,
	}, nil
}

func (p *StarterProvisioner) doDigestAuthRequest(ctx context.Context, method, uri string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, uri, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusUnauthorized {
		return resp, nil
	}
	_ = resp.Body.Close()

	wwwAuth := resp.Header.Get("WWW-Authenticate")
	nonce, realm, qop := parseDigestChallenge(wwwAuth)
	if nonce == "" {
		return nil, fmt.Errorf("invalid digest challenge")
	}
	auth, err := buildDigestAuth(p.apiKey, p.apiSecret, method, uri, nonce, realm, qop)
	if err != nil {
		return nil, err
	}
	req2, err := http.NewRequestWithContext(ctx, method, uri, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Authorization", auth)
	return p.client.Do(req2)
}

func parseDigestChallenge(header string) (nonce, realm, qop string) {
	header = strings.TrimPrefix(header, "Digest ")
	parts := strings.Split(header, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "nonce=") {
			nonce = strings.Trim(strings.TrimPrefix(part, "nonce="), `"`)
		}
		if strings.HasPrefix(part, "realm=") {
			realm = strings.Trim(strings.TrimPrefix(part, "realm="), `"`)
		}
		if strings.HasPrefix(part, "qop=") {
			qop = strings.Trim(strings.TrimPrefix(part, "qop="), `"`)
		}
	}
	return
}

func buildDigestAuth(username, password, method, uri, nonce, realm, qop string) (string, error) {
	nc := "00000001"
	cnonce, err := generateNonce()
	if err != nil {
		return "", err
	}
	ha1 := md5Hash(fmt.Sprintf("%s:%s:%s", username, realm, password))
	parsed, err := url.Parse(uri)
	if err != nil {
		return "", err
	}
	path := parsed.Path
	if parsed.RawQuery != "" {
		path += "?" + parsed.RawQuery
	}
	ha2 := md5Hash(fmt.Sprintf("%s:%s", method, path))
	resp := md5Hash(fmt.Sprintf("%s:%s:%s:%s:%s:%s", ha1, nonce, nc, cnonce, qop, ha2))
	return fmt.Sprintf(`Digest username="%s", realm="%s", nonce="%s", uri="%s", qop=%s, nc=%s, cnonce="%s", response="%s"`, username, realm, nonce, path, qop, nc, cnonce, resp), nil
}

func md5Hash(s string) string { return fmt.Sprintf("%x", md5.Sum([]byte(s))) }

func generateNonce() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", b), nil
}

func generateRandomPassword(length int) (string, error) {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	for i := range b {
		b[i] = chars[int(b[i])%len(chars)]
	}
	return string(b), nil
}
