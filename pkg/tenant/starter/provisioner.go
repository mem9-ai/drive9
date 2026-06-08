// Package starter implements the TiDB Cloud Starter tenant provisioner.
package starter

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/tenant"
	"github.com/mem9-ai/dat9/pkg/tenant/schema"
)

const (
	envTiDBAPIURL     = "DRIVE9_TIDBCLOUD_API_URL"
	envTiDBAPIKey     = "DRIVE9_TIDBCLOUD_API_KEY"
	envTiDBAPISecret  = "DRIVE9_TIDBCLOUD_API_SECRET"
	envTiDBPoolID     = "DRIVE9_TIDBCLOUD_POOL_ID"
	envTiDBSpendLimit = "DRIVE9_TIDBCLOUD_DEFAULT_SPENDING_LIMIT"
)

type Provisioner struct {
	apiURL            string
	apiKey            string
	apiSecret         string
	poolID            string
	defaultSpendLimit *int32
	client            *http.Client
}

func NewProvisionerFromEnv() (*Provisioner, error) {
	apiURL := os.Getenv(envTiDBAPIURL)
	apiKey := os.Getenv(envTiDBAPIKey)
	apiSecret := os.Getenv(envTiDBAPISecret)
	poolID := os.Getenv(envTiDBPoolID)
	if apiURL == "" || apiKey == "" || apiSecret == "" || poolID == "" {
		return nil, fmt.Errorf("%s, %s, %s and %s are required", envTiDBAPIURL, envTiDBAPIKey, envTiDBAPISecret, envTiDBPoolID)
	}
	defaultSpendLimit, err := parseDefaultSpendLimit(os.Getenv(envTiDBSpendLimit))
	if err != nil {
		return nil, err
	}
	return &Provisioner{
		apiURL:            strings.TrimRight(apiURL, "/"),
		apiKey:            apiKey,
		apiSecret:         apiSecret,
		poolID:            poolID,
		defaultSpendLimit: defaultSpendLimit,
		client:            &http.Client{Timeout: 60 * time.Second},
	}, nil
}

func (p *Provisioner) ProviderType() string { return tenant.ProviderTiDBCloudStarter }

// InitSchema repairs known launch-schema drift (for example, missing uploads
// columns from legacy tenants) and validates the TiDB auto-embedding contract.
func (p *Provisioner) InitSchema(ctx context.Context, dsn string) error {
	return schema.EnsureTiDBSchemaForModeDSN(ctx, dsn, schema.TiDBEmbeddingModeAuto)
}

func (p *Provisioner) Provision(ctx context.Context, tenantID string) (*tenant.ClusterInfo, error) {
	password, err := generateRandomPassword(24)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(map[string]string{"pool_id": p.poolID, "root_password": password})
	if err != nil {
		return nil, err
	}
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
	if out.ClusterID == "" {
		return nil, fmt.Errorf("starter response missing cluster id")
	}
	if out.UserPrefix == "" {
		return nil, fmt.Errorf("starter response missing user prefix")
	}
	cluster := &tenant.ClusterInfo{
		TenantID:  tenantID,
		ClusterID: out.ClusterID,
		Host:      out.Endpoints.Public.Host,
		Port:      out.Endpoints.Public.Port,
		Username:  out.UserPrefix + ".root",
		Password:  password,
		DBName:    "test",
		Provider:  tenant.ProviderTiDBCloudStarter,
	}
	if p.defaultSpendLimit != nil {
		if err := p.UpdateSpendingLimit(ctx, out.ClusterID, *p.defaultSpendLimit); err != nil {
			return cluster, fmt.Errorf("update starter spending limit for cluster %s: %w", out.ClusterID, err)
		}
	}

	return cluster, nil
}

func parseDefaultSpendLimit(raw string) (*int32, error) {
	if raw == "" {
		return nil, nil
	}
	trimmed := strings.TrimSpace(raw)
	monthly, err := strconv.ParseInt(trimmed, 10, 32)
	if err != nil || monthly < 0 {
		return nil, fmt.Errorf("invalid %s value %q: must be a non-negative integer in USD cents", envTiDBSpendLimit, raw)
	}
	out := int32(monthly)
	return &out, nil
}

func (p *Provisioner) UpdateSpendingLimit(ctx context.Context, clusterID string, monthly int32) error {
	if clusterID == "" {
		return fmt.Errorf("cluster id is required")
	}
	if monthly < 0 {
		return fmt.Errorf("spending limit must be non-negative")
	}
	body, err := json.Marshal(map[string]any{
		"updateMask": "spendingLimit.monthly",
		"cluster": map[string]any{
			"spendingLimit": map[string]int32{"monthly": monthly},
		},
	})
	if err != nil {
		return err
	}
	endpoint := fmt.Sprintf("%s/v1beta1/clusters/%s", p.apiURL, clusterID)
	resp, err := p.doDigestAuthRequest(ctx, http.MethodPatch, endpoint, body)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("starter spending limit update status %d: %s", resp.StatusCode, string(raw))
	}
	return nil
}

func (p *Provisioner) ProvisionBranch(ctx context.Context, forkTenantID string, source *tenant.ClusterInfo) (*tenant.ClusterInfo, error) {
	out, err := p.CreateBranch(ctx, forkTenantID, source)
	if err != nil {
		return out, err
	}
	if out.Host != "" && out.Port != 0 && out.Username != "" {
		return out, nil
	}
	return p.WaitForBranchActive(ctx, out)
}

func (p *Provisioner) CreateBranch(ctx context.Context, forkTenantID string, source *tenant.ClusterInfo) (*tenant.ClusterInfo, error) {
	if source == nil {
		return nil, fmt.Errorf("source cluster info is required")
	}
	parentID := source.BranchID
	if parentID == "" {
		parentID = source.ClusterID
	}
	if source.ClusterID == "" || parentID == "" {
		return nil, fmt.Errorf("source cluster id is required")
	}
	reqBody := map[string]string{
		"displayName": forkTenantID,
		"parentId":    parentID,
	}
	if source.Password != "" {
		reqBody["rootPassword"] = source.Password
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}
	endpoint := fmt.Sprintf("%s/v1beta1/clusters/%s/branches", p.apiURL, source.ClusterID)
	resp, err := p.doDigestAuthRequest(ctx, http.MethodPost, endpoint, body)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("starter branch provision status %d: %s", resp.StatusCode, string(raw))
	}

	branch, err := parseStarterBranchInfo(raw)
	if err != nil {
		return nil, err
	}
	if branch.BranchID == "" {
		return nil, fmt.Errorf("starter branch response missing branch id")
	}
	dbName := source.DBName
	if dbName == "" {
		dbName = "test"
	}
	out := &tenant.ClusterInfo{
		TenantID:  forkTenantID,
		ClusterID: source.ClusterID,
		BranchID:  branch.BranchID,
		Password:  source.Password,
		DBName:    dbName,
		Provider:  tenant.ProviderTiDBCloudStarter,
	}
	if branch.State != "" && branch.State != "ACTIVE" {
		return out, nil
	}
	if branch.State == "ACTIVE" || branch.Endpoints.Public.Host != "" || branch.UserPrefix != "" {
		if err := fillBranchEndpoint(out, branch); err != nil {
			return out, err
		}
	}
	return out, nil
}

func (p *Provisioner) WaitForBranchActive(ctx context.Context, branch *tenant.ClusterInfo) (*tenant.ClusterInfo, error) {
	if branch == nil {
		return nil, fmt.Errorf("branch cluster info is required")
	}
	if branch.ClusterID == "" || branch.BranchID == "" {
		return nil, fmt.Errorf("cluster id and branch id are required")
	}
	out := *branch
	info, err := p.waitForBranchActive(ctx, branch.ClusterID, branch.BranchID)
	if err != nil {
		return &out, err
	}
	if err := fillBranchEndpoint(&out, info); err != nil {
		return &out, err
	}
	return &out, nil
}

func fillBranchEndpoint(out *tenant.ClusterInfo, branch *starterBranchInfo) error {
	if branch.Endpoints.Public.Host == "" || branch.Endpoints.Public.Port == 0 {
		return fmt.Errorf("starter branch response missing endpoint")
	}
	if branch.UserPrefix == "" {
		return fmt.Errorf("starter branch response missing user prefix")
	}
	out.Host = branch.Endpoints.Public.Host
	out.Port = branch.Endpoints.Public.Port
	out.Username = branch.UserPrefix + ".root"
	return nil
}

func (p *Provisioner) DeleteBranch(ctx context.Context, clusterID, branchID string) error {
	if clusterID == "" || branchID == "" {
		return fmt.Errorf("cluster id and branch id are required")
	}
	endpoint := fmt.Sprintf("%s/v1beta1/clusters/%s/branches/%s", p.apiURL, clusterID, branchID)
	resp, err := p.doDigestAuthRequest(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("starter branch delete status %d: %s", resp.StatusCode, string(raw))
	}
	return nil
}

type starterBranchInfo struct {
	BranchID  string `json:"branchId"`
	State     string `json:"state"`
	Endpoints struct {
		Public struct {
			Host string `json:"host"`
			Port int    `json:"port"`
		} `json:"public"`
	} `json:"endpoints"`
	UserPrefix string `json:"userPrefix"`
}

func parseStarterBranchInfo(raw []byte) (*starterBranchInfo, error) {
	var out starterBranchInfo
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (p *Provisioner) waitForBranchActive(ctx context.Context, clusterID, branchID string) (*starterBranchInfo, error) {
	deadline := time.Now().Add(10 * time.Minute)
	for {
		endpoint := fmt.Sprintf("%s/v1beta1/clusters/%s/branches/%s?view=BASIC", p.apiURL, clusterID, branchID)
		resp, err := p.doDigestAuthRequest(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		raw, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("starter branch get status %d: %s", resp.StatusCode, string(raw))
		}
		branch, err := parseStarterBranchInfo(raw)
		if err != nil {
			return nil, err
		}
		if branch.State == "ACTIVE" {
			return branch, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("starter branch %s not active before timeout: %s", branchID, branch.State)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

func (p *Provisioner) doDigestAuthRequest(ctx context.Context, method, uri string, body []byte) (*http.Response, error) {
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
	max := big.NewInt(int64(len(chars)))
	for i := range b {
		n, err := rand.Int(rand.Reader, max)
		if err != nil {
			return "", err
		}
		b[i] = chars[n.Int64()]
	}
	return string(b), nil
}
