// Package starter implements compatibility operations for TiDB Cloud Starter
// tenants persisted before starter provisioning was removed.
package starter

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

	"github.com/mem9-ai/drive9/pkg/tenant"
	"github.com/mem9-ai/drive9/pkg/tenant/schema"
)

const (
	EnvTiDBCloudNativeAPIURL    = "DRIVE9_TIDBCLOUD_NATIVE_API_URL"
	EnvTiDBCloudLegacyAPIURL    = "DRIVE9_TIDBCLOUD_API_URL"
	EnvTiDBCloudAPIKey          = "DRIVE9_TIDBCLOUD_API_KEY"
	EnvTiDBCloudDAT9APISecret   = "DRIVE9_DAT9_TIDBCLOUD_API_SECRET"
	EnvTiDBCloudLegacyAPISecret = "DRIVE9_TIDBCLOUD_API_SECRET"
)

// LegacyProvisioner keeps delete/fork operations available for persisted
// tidb_cloud_starter tenants. It must not be used for new tenant provisioning.
type LegacyProvisioner struct {
	apiURL    string
	apiKey    string
	apiSecret string
	client    *http.Client
}

func NewLegacyProvisionerFromEnv() (*LegacyProvisioner, error) {
	apiURL := strings.TrimSpace(os.Getenv(EnvTiDBCloudNativeAPIURL))
	if apiURL == "" {
		apiURL = strings.TrimSpace(os.Getenv(EnvTiDBCloudLegacyAPIURL))
	}
	apiKey := strings.TrimSpace(os.Getenv(EnvTiDBCloudAPIKey))
	apiSecret := strings.TrimSpace(os.Getenv(EnvTiDBCloudDAT9APISecret))
	if apiSecret == "" {
		apiSecret = strings.TrimSpace(os.Getenv(EnvTiDBCloudLegacyAPISecret))
	}
	if apiURL == "" || apiKey == "" || apiSecret == "" {
		return nil, fmt.Errorf("%s or %s, %s and %s are required for legacy starter operations",
			EnvTiDBCloudNativeAPIURL, EnvTiDBCloudLegacyAPIURL, EnvTiDBCloudAPIKey, EnvTiDBCloudDAT9APISecret)
	}
	parsed, err := url.Parse(apiURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return nil, fmt.Errorf("%s or %s must be a valid https URL", EnvTiDBCloudNativeAPIURL, EnvTiDBCloudLegacyAPIURL)
	}
	return &LegacyProvisioner{
		apiURL:    strings.TrimRight(apiURL, "/"),
		apiKey:    apiKey,
		apiSecret: apiSecret,
		client:    &http.Client{Timeout: 60 * time.Second},
	}, nil
}

func LegacyEnvPresent() bool {
	return strings.TrimSpace(os.Getenv(EnvTiDBCloudAPIKey)) != "" ||
		strings.TrimSpace(os.Getenv(EnvTiDBCloudDAT9APISecret)) != "" ||
		strings.TrimSpace(os.Getenv(EnvTiDBCloudLegacyAPISecret)) != ""
}

func (p *LegacyProvisioner) ProviderType() string {
	return tenant.ProviderTiDBCloudStarterLegacy
}

func (p *LegacyProvisioner) InitSchema(ctx context.Context, dsn string) error {
	return schema.EnsureTiDBSchemaForModeDSN(ctx, dsn, schema.TiDBEmbeddingModeAuto)
}

func (p *LegacyProvisioner) InitSchemaForAutoEmbeddingProfile(ctx context.Context, dsn string, profile schema.TiDBAutoEmbeddingProfile) error {
	return schema.EnsureTiDBSchemaForAutoEmbeddingProfileDSN(ctx, dsn, profile)
}

func (p *LegacyProvisioner) Provision(context.Context, string) (*tenant.ClusterInfo, error) {
	return nil, fmt.Errorf("legacy TiDB Cloud Starter provisioning is disabled")
}

func (p *LegacyProvisioner) Deprovision(ctx context.Context, cluster *tenant.ClusterInfo) error {
	if cluster == nil || strings.TrimSpace(cluster.ClusterID) == "" {
		return fmt.Errorf("cluster id is required")
	}
	endpoint := fmt.Sprintf("%s/v1beta1/clusters/%s", p.apiURL, url.PathEscape(strings.TrimSpace(cluster.ClusterID)))
	resp, err := p.doDigestAuthRequest(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return fmt.Errorf("delete starter cluster digest request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("starter cluster delete status %d: %s", resp.StatusCode, string(raw))
	}
	return nil
}

func (p *LegacyProvisioner) ProvisionBranch(ctx context.Context, forkTenantID string, source *tenant.ClusterInfo) (*tenant.ClusterInfo, error) {
	out, err := p.CreateBranch(ctx, forkTenantID, source)
	if err != nil {
		return out, err
	}
	if out.Host != "" && out.Port != 0 && out.Username != "" {
		return out, nil
	}
	return p.WaitForBranchActive(ctx, out)
}

func (p *LegacyProvisioner) CreateBranch(ctx context.Context, forkTenantID string, source *tenant.ClusterInfo) (*tenant.ClusterInfo, error) {
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
	endpoint := fmt.Sprintf("%s/v1beta1/clusters/%s/branches", p.apiURL, url.PathEscape(source.ClusterID))
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
		Provider:  tenant.ProviderTiDBCloudStarterLegacy,
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

func (p *LegacyProvisioner) WaitForBranchActive(ctx context.Context, branch *tenant.ClusterInfo) (*tenant.ClusterInfo, error) {
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
	out.Provider = tenant.ProviderTiDBCloudStarterLegacy
	return &out, nil
}

func (p *LegacyProvisioner) DeleteBranch(ctx context.Context, clusterID, branchID string) error {
	if clusterID == "" || branchID == "" {
		return fmt.Errorf("cluster id and branch id are required")
	}
	endpoint := fmt.Sprintf("%s/v1beta1/clusters/%s/branches/%s", p.apiURL, url.PathEscape(clusterID), url.PathEscape(branchID))
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

func (p *LegacyProvisioner) waitForBranchActive(ctx context.Context, clusterID, branchID string) (*starterBranchInfo, error) {
	deadline := time.Now().Add(10 * time.Minute)
	for {
		endpoint := fmt.Sprintf("%s/v1beta1/clusters/%s/branches/%s?view=BASIC", p.apiURL, url.PathEscape(clusterID), url.PathEscape(branchID))
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

func (p *LegacyProvisioner) doDigestAuthRequest(ctx context.Context, method, uri string, body []byte) (*http.Response, error) {
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
