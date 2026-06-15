// Package tidbcloudnative implements customer-account TiDB Cloud Serverless
// tenant provisioning.
package tidbcloudnative

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/dat9/pkg/tenant"
	"github.com/mem9-ai/dat9/pkg/tenant/schema"
)

const (
	EnvTiDBCloudNativeAPIURL              = "DRIVE9_TIDBCLOUD_NATIVE_API_URL"
	EnvTiDBCloudNativeCloudProvider       = "DRIVE9_TIDBCLOUD_NATIVE_CLOUD_PROVIDER"
	EnvTiDBCloudNativeRegion              = "DRIVE9_TIDBCLOUD_NATIVE_REGION"
	EnvTiDBCloudNativeDefaultDatabaseName = "DRIVE9_TIDBCLOUD_NATIVE_DEFAULT_DATABASE_NAME"

	DefaultDatabaseName = "tidbcloud_fs"
)

var databaseNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)
var displayNameCharPattern = regexp.MustCompile(`[^A-Za-z0-9-]`)

var ensureDatabaseFunc = ensureDatabase

type Provisioner struct {
	apiURL              string
	cloudProvider       string
	region              string
	defaultDatabaseName string
	client              *http.Client
}

func NewProvisionerFromEnv() (*Provisioner, error) {
	apiURL := strings.TrimSpace(os.Getenv(EnvTiDBCloudNativeAPIURL))
	cloudProvider := strings.TrimSpace(os.Getenv(EnvTiDBCloudNativeCloudProvider))
	region := strings.TrimSpace(os.Getenv(EnvTiDBCloudNativeRegion))
	defaultDB := strings.TrimSpace(os.Getenv(EnvTiDBCloudNativeDefaultDatabaseName))
	if defaultDB == "" {
		defaultDB = DefaultDatabaseName
	}
	if apiURL == "" || cloudProvider == "" || region == "" {
		return nil, fmt.Errorf("%s, %s and %s are required", EnvTiDBCloudNativeAPIURL, EnvTiDBCloudNativeCloudProvider, EnvTiDBCloudNativeRegion)
	}
	if _, err := normalizeDatabaseName(defaultDB); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", EnvTiDBCloudNativeDefaultDatabaseName, err)
	}
	return &Provisioner{
		apiURL:              strings.TrimRight(apiURL, "/"),
		cloudProvider:       cloudProvider,
		region:              region,
		defaultDatabaseName: defaultDB,
		client:              &http.Client{Timeout: 60 * time.Second},
	}, nil
}

func (p *Provisioner) ProviderType() string { return tenant.ProviderTiDBCloudNative }

func (p *Provisioner) InitSchema(ctx context.Context, dsn string) error {
	return schema.EnsureTiDBSchemaForModeDSN(ctx, dsn, schema.TiDBEmbeddingModeAuto)
}

func (p *Provisioner) InitSchemaForAutoEmbeddingProfile(ctx context.Context, dsn string, profile schema.TiDBAutoEmbeddingProfile) error {
	return schema.EnsureTiDBSchemaForAutoEmbeddingProfileDSN(ctx, dsn, profile)
}

func (p *Provisioner) Provision(ctx context.Context, tenantID string) (*tenant.ClusterInfo, error) {
	return nil, fmt.Errorf("tidbcloud native requires request credentials")
}

func (p *Provisioner) ValidateCredentialProvisionRequest(req tenant.CredentialProvisionRequest) error {
	publicKey := strings.TrimSpace(req.PublicKey)
	privateKey := strings.TrimSpace(req.PrivateKey)
	if publicKey == "" || privateKey == "" {
		return fmt.Errorf("public_key and private_key are required")
	}
	_, err := p.resolveDatabaseName(req.DatabaseName)
	return err
}

func (p *Provisioner) ProvisionWithCredentials(ctx context.Context, tenantID string, req tenant.CredentialProvisionRequest) (*tenant.ClusterInfo, error) {
	publicKey := strings.TrimSpace(req.PublicKey)
	privateKey := strings.TrimSpace(req.PrivateKey)
	if publicKey == "" || privateKey == "" {
		return nil, fmt.Errorf("public_key and private_key are required")
	}
	dbName, err := p.resolveDatabaseName(req.DatabaseName)
	if err != nil {
		return nil, err
	}
	password, err := generateRandomPassword(24)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(map[string]any{
		"displayName":  clusterDisplayName(tenantID),
		"rootPassword": password,
		"region": map[string]string{
			"name": p.regionName(),
		},
	})
	if err != nil {
		return nil, err
	}
	endpoint := p.apiURL + "/v1beta1/clusters"
	resp, err := p.doDigestAuthRequest(ctx, publicKey, privateKey, http.MethodPost, endpoint, body)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("tidbcloud native provision status %d", resp.StatusCode)
	}
	info, err := parseClusterInfo(raw)
	if err != nil {
		return nil, err
	}
	if info.ClusterID == "" {
		return nil, fmt.Errorf("tidbcloud native response missing cluster id")
	}
	if info.State != "" && info.State != "ACTIVE" && (info.Endpoints.Public.Host == "" || info.UserPrefix == "") {
		info, err = p.waitForClusterActive(ctx, publicKey, privateKey, info.ClusterID)
		if err != nil {
			return &tenant.ClusterInfo{
				TenantID:  tenantID,
				ClusterID: info.ClusterID,
				Password:  password,
				DBName:    dbName,
				Provider:  tenant.ProviderTiDBCloudNative,
			}, err
		}
	}
	out := &tenant.ClusterInfo{
		TenantID:  tenantID,
		ClusterID: info.ClusterID,
		Host:      info.Endpoints.Public.Host,
		Port:      info.Endpoints.Public.Port,
		Username:  info.Username,
		Password:  password,
		DBName:    dbName,
		Provider:  tenant.ProviderTiDBCloudNative,
	}
	if out.Username == "" && info.UserPrefix != "" {
		out.Username = info.UserPrefix + ".root"
	}
	if out.Host == "" || out.Port == 0 {
		return out, fmt.Errorf("tidbcloud native response missing endpoint")
	}
	if out.Username == "" {
		return out, fmt.Errorf("tidbcloud native response missing username")
	}
	if err := ensureDatabaseFunc(ctx, out.Username, out.Password, out.Host, out.Port, dbName); err != nil {
		return out, fmt.Errorf("ensure tidbcloud native database: %w", err)
	}
	return out, nil
}

func (p *Provisioner) regionName() string {
	if strings.HasPrefix(p.region, "regions/") {
		return p.region
	}
	return "regions/" + p.cloudProvider + "-" + p.region
}

func clusterDisplayName(tenantID string) string {
	const maxDisplayNameLen = 64
	name := displayNameCharPattern.ReplaceAllString("drive9-"+tenantID, "-")
	if len(name) <= maxDisplayNameLen {
		return name
	}
	name = name[:maxDisplayNameLen]
	return strings.TrimRight(name, "-")
}

func (p *Provisioner) resolveDatabaseName(raw string) (string, error) {
	name := strings.TrimSpace(raw)
	if name == "" {
		name = p.defaultDatabaseName
	}
	return normalizeDatabaseName(name)
}

func normalizeDatabaseName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if !databaseNamePattern.MatchString(name) {
		return "", fmt.Errorf("database_name must match %s", databaseNamePattern.String())
	}
	switch strings.ToLower(name) {
	case "test", "mysql", "information_schema", "performance_schema", "sys":
		return "", fmt.Errorf("database_name %q is reserved", name)
	default:
		return name, nil
	}
}

func ensureDatabase(ctx context.Context, user, password, host string, port int, dbName string) error {
	cfg := mysql.NewConfig()
	cfg.User = user
	cfg.Passwd = password
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%d", host, port)
	cfg.ParseTime = true
	cfg.TLSConfig = "true"
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+dbName+"`"); err != nil {
		return err
	}
	return nil
}

type clusterInfo struct {
	ClusterID string `json:"clusterId"`
	State     string `json:"state"`
	Endpoints struct {
		Public struct {
			Host string `json:"host"`
			Port int    `json:"port"`
		} `json:"public"`
	} `json:"endpoints"`
	UserPrefix string `json:"userPrefix"`
	Username   string `json:"username"`
}

func parseClusterInfo(raw []byte) (*clusterInfo, error) {
	var out clusterInfo
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (p *Provisioner) waitForClusterActive(ctx context.Context, publicKey, privateKey, clusterID string) (*clusterInfo, error) {
	deadline := time.Now().Add(10 * time.Minute)
	for {
		endpoint := fmt.Sprintf("%s/v1beta1/clusters/%s?view=BASIC", p.apiURL, clusterID)
		resp, err := p.doDigestAuthRequest(ctx, publicKey, privateKey, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		raw, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("tidbcloud native cluster get status %d", resp.StatusCode)
		}
		info, err := parseClusterInfo(raw)
		if err != nil {
			return nil, err
		}
		if info.State == "ACTIVE" {
			return info, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("tidbcloud native cluster %s not active before timeout: %s", clusterID, info.State)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

func (p *Provisioner) doDigestAuthRequest(ctx context.Context, publicKey, privateKey, method, uri string, body []byte) (*http.Response, error) {
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
	auth, err := buildDigestAuth(publicKey, privateKey, method, uri, nonce, realm, qop)
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
