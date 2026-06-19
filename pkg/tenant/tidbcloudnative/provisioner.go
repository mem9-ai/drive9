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
	"strconv"
	"strings"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"github.com/mem9-ai/drive9/pkg/tenant/schema"
)

const (
	EnvTiDBCloudNativeAPIURL              = "DRIVE9_TIDBCLOUD_NATIVE_API_URL"
	EnvTiDBCloudNativeCloudProvider       = "DRIVE9_TIDBCLOUD_NATIVE_CLOUD_PROVIDER"
	EnvTiDBCloudNativeRegion              = "DRIVE9_TIDBCLOUD_NATIVE_REGION"
	EnvTiDBCloudNativeDefaultDatabaseName = "DRIVE9_TIDBCLOUD_NATIVE_DEFAULT_DATABASE_NAME"
	EnvTiDBCloudDefaultSpendingLimit      = "DRIVE9_TIDBCLOUD_DEFAULT_SPENDING_LIMIT"
	EnvTiDBCloudNativePublicKey           = "DRIVE9_TIDBCLOUD_NATIVE_PUBLIC_KEY"
	EnvTiDBCloudNativePrivateKey          = "DRIVE9_TIDBCLOUD_NATIVE_PRIVATE_KEY"

	DefaultDatabaseName = "tidbcloud_fs"
	DefaultSpendLimit   = int32(1000)

	upstreamErrorBodyLimit = 2048
)

var databaseNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)
var displayNameCharPattern = regexp.MustCompile(`[^A-Za-z0-9-]`)

var ensureDatabaseFunc = ensureDatabase

type Provisioner struct {
	apiURL              string
	cloudProvider       string
	region              string
	defaultDatabaseName string
	defaultSpendLimit   *int32
	defaultPublicKey    string
	defaultPrivateKey   string
	client              *http.Client
}

func NewProvisionerFromEnv() (*Provisioner, error) {
	apiURL := strings.TrimSpace(os.Getenv(EnvTiDBCloudNativeAPIURL))
	cloudProvider := strings.TrimSpace(os.Getenv(EnvTiDBCloudNativeCloudProvider))
	region := strings.TrimSpace(os.Getenv(EnvTiDBCloudNativeRegion))
	defaultDB := strings.TrimSpace(os.Getenv(EnvTiDBCloudNativeDefaultDatabaseName))
	defaultSpendLimit, err := parseDefaultSpendLimit(os.Getenv(EnvTiDBCloudDefaultSpendingLimit))
	if err != nil {
		return nil, err
	}
	if defaultDB == "" {
		defaultDB = DefaultDatabaseName
	}
	if apiURL == "" || cloudProvider == "" || region == "" {
		return nil, fmt.Errorf("%s, %s and %s are required", EnvTiDBCloudNativeAPIURL, EnvTiDBCloudNativeCloudProvider, EnvTiDBCloudNativeRegion)
	}
	parsedAPIURL, err := url.Parse(apiURL)
	if err != nil || parsedAPIURL.Scheme != "https" || parsedAPIURL.Host == "" {
		return nil, fmt.Errorf("%s must be a valid https URL", EnvTiDBCloudNativeAPIURL)
	}
	if _, err := normalizeDatabaseName(defaultDB); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", EnvTiDBCloudNativeDefaultDatabaseName, err)
	}
	return &Provisioner{
		apiURL:              strings.TrimRight(apiURL, "/"),
		cloudProvider:       cloudProvider,
		region:              region,
		defaultDatabaseName: defaultDB,
		defaultSpendLimit:   defaultSpendLimit,
		defaultPublicKey:    strings.TrimSpace(os.Getenv(EnvTiDBCloudNativePublicKey)),
		defaultPrivateKey:   strings.TrimSpace(os.Getenv(EnvTiDBCloudNativePrivateKey)),
		client:              &http.Client{Timeout: 60 * time.Second},
	}, nil
}

func (p *Provisioner) ProviderType() string { return tenant.ProviderTiDBCloudNative }

func (p *Provisioner) ProvisioningCloudProvider() string { return p.cloudProvider }

func (p *Provisioner) DefaultCredentials() (tenant.CredentialProvisionRequest, bool) {
	if p.defaultPublicKey == "" || p.defaultPrivateKey == "" {
		return tenant.CredentialProvisionRequest{}, false
	}
	return tenant.CredentialProvisionRequest{
		PublicKey:  p.defaultPublicKey,
		PrivateKey: p.defaultPrivateKey,
	}, true
}

func (p *Provisioner) ProvisioningRegion() string { return p.region }

func (p *Provisioner) InitSchema(ctx context.Context, dsn string) error {
	return schema.InitTiDBTenantSchemaForModeWithOptionsContext(ctx, dsn, schema.TiDBEmbeddingModeAuto, schema.InitTiDBTenantSchemaOptions{})
}

func (p *Provisioner) InitSchemaForAutoEmbeddingProfile(ctx context.Context, dsn string, profile schema.TiDBAutoEmbeddingProfile) error {
	return schema.InitTiDBTenantSchemaForAutoEmbeddingProfileContext(ctx, dsn, profile)
}

func (p *Provisioner) EnsureSystemUser(ctx context.Context, dsn, _ string) (string, string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", "", fmt.Errorf("parse native tenant DSN: %w", err)
	}
	username, needsSetup, err := systemUsernameForCurrent(cfg.User)
	if err != nil {
		return "", "", fmt.Errorf("resolve native system username: %w", err)
	}
	if cfg.Passwd == "" {
		return "", "", fmt.Errorf("native tenant DSN password is empty")
	}
	if !needsSetup {
		return cfg.User, cfg.Passwd, nil
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return "", "", fmt.Errorf("open native tenant database: %w", err)
	}
	defer func() { _ = db.Close() }()
	dbName, err := normalizeDatabaseName(cfg.DBName)
	if err != nil {
		return "", "", fmt.Errorf("resolve native system user database: %w", err)
	}
	password := cfg.Passwd
	if err := ensureSystemUser(ctx, db, dbName, username, password); err != nil {
		return "", "", fmt.Errorf("ensure native system user: %w", err)
	}
	return username, password, nil
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
	_, err := p.resolveDatabaseName("")
	return err
}

func (p *Provisioner) ProvisionWithCredentials(ctx context.Context, tenantID string, req tenant.CredentialProvisionRequest) (*tenant.ClusterInfo, error) {
	publicKey := strings.TrimSpace(req.PublicKey)
	privateKey := strings.TrimSpace(req.PrivateKey)
	if publicKey == "" || privateKey == "" {
		return nil, fmt.Errorf("public_key and private_key are required")
	}
	dbName, err := p.resolveDatabaseName("")
	if err != nil {
		return nil, err
	}
	password, err := generateRandomPassword(24)
	if err != nil {
		return nil, err
	}
	reqBody := map[string]any{
		"displayName":  clusterDisplayName(tenantID),
		"rootPassword": password,
		"region": map[string]string{
			"name": p.regionName(),
		},
	}
	if p.defaultSpendLimit != nil {
		reqBody["spendingLimit"] = map[string]int32{"monthly": *p.defaultSpendLimit}
	}
	body, err := json.Marshal(reqBody)
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
		return nil, fmt.Errorf("%s", statusError("provision", resp.StatusCode, sanitizeUpstreamBody(raw)))
	}
	info, err := parseClusterInfo(raw)
	if err != nil {
		return nil, err
	}
	if info.ClusterID == "" {
		return nil, fmt.Errorf("tidbcloud native response missing cluster id")
	}
	if info.State != "ACTIVE" || clusterConnectionIncomplete(info) {
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

func (p *Provisioner) DeprovisionWithCredentials(ctx context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) error {
	publicKey := strings.TrimSpace(req.PublicKey)
	privateKey := strings.TrimSpace(req.PrivateKey)
	if publicKey == "" || privateKey == "" {
		return fmt.Errorf("public_key and private_key are required")
	}
	if cluster == nil || strings.TrimSpace(cluster.ClusterID) == "" {
		return fmt.Errorf("cluster id is required")
	}
	endpoint := fmt.Sprintf("%s/v1beta1/clusters/%s", p.apiURL, url.PathEscape(strings.TrimSpace(cluster.ClusterID)))
	resp, err := p.doDigestAuthRequest(ctx, publicKey, privateKey, http.MethodDelete, endpoint, nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("%s", statusError("cluster delete", resp.StatusCode, sanitizeUpstreamBody(raw)))
	}
	return nil
}

func (p *Provisioner) regionName() string {
	if strings.HasPrefix(p.region, "regions/") {
		return p.region
	}
	return "regions/" + p.cloudProvider + "-" + p.region
}

func clusterDisplayName(tenantID string) string {
	const maxDisplayNameLen = 64
	name := displayNameCharPattern.ReplaceAllString("tidbcloud-fs-"+tenantID, "-")
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

func ensureSystemUser(ctx context.Context, db *sql.DB, dbName, username, password string) error {
	for i, stmt := range systemUserStatements(dbName, username, password) {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("execute native system user statement %d: %w", i+1, err)
		}
	}
	return nil
}

func systemUserStatements(dbName, username, password string) []string {
	const roleName = "tdc_fs_admin"
	return []string{
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", quoteIdent(dbName)),
		fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", quoteString(roleName)),
		fmt.Sprintf("GRANT CREATE, ALTER, DROP, INDEX, SELECT, INSERT, UPDATE, DELETE ON %s.* TO %s", quoteIdent(dbName), quoteString(roleName)),
		fmt.Sprintf("CREATE USER IF NOT EXISTS %s IDENTIFIED BY %s", quoteString(username), quoteString(password)),
		fmt.Sprintf("ALTER USER %s IDENTIFIED BY %s", quoteString(username), quoteString(password)),
		fmt.Sprintf("GRANT %s TO %s", quoteString(roleName), quoteString(username)),
		fmt.Sprintf("SET DEFAULT ROLE %s TO %s", quoteString(roleName), quoteString(username)),
	}
}

func systemUsernameForCurrent(currentUsername string) (string, bool, error) {
	currentUsername = strings.TrimSpace(currentUsername)
	if currentUsername == "" {
		return "", false, fmt.Errorf("native database username is empty")
	}
	prefix, ok := strings.CutSuffix(currentUsername, ".root")
	if ok {
		if prefix == "" {
			return "", false, fmt.Errorf("native root username %q missing user prefix", currentUsername)
		}
		return prefix + ".tdc_fs_sys", true, nil
	}
	if prefix, ok := strings.CutSuffix(currentUsername, ".tdc_fs_sys"); ok && prefix != "" {
		return currentUsername, false, nil
	}
	return "", false, fmt.Errorf("native database username %q is not a root or tdc_fs_sys account", currentUsername)
}

func quoteIdent(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}

func quoteString(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "'", "''")
	return "'" + value + "'"
}

func parseDefaultSpendLimit(raw string) (*int32, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		out := DefaultSpendLimit
		return &out, nil
	}
	monthly, err := strconv.ParseInt(trimmed, 10, 32)
	if err != nil || monthly < 0 {
		return nil, fmt.Errorf("invalid %s value %q: must be a non-negative integer in USD cents", EnvTiDBCloudDefaultSpendingLimit, raw)
	}
	out := int32(monthly)
	return &out, nil
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

func sanitizeUpstreamBody(raw []byte) string {
	s := strings.TrimSpace(string(raw))
	if s == "" {
		return ""
	}
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > upstreamErrorBodyLimit {
		s = s[:upstreamErrorBodyLimit] + "...(truncated)"
	}
	return s
}

func statusError(operation string, code int, upstreamBody string) string {
	msg := fmt.Sprintf("tidbcloud native %s status %d", operation, code)
	if upstreamBody != "" {
		msg += ": " + upstreamBody
	} else {
		switch code {
		case http.StatusUnauthorized:
			msg += ": invalid TiDB Cloud API key"
		case http.StatusForbidden:
			msg += ": access denied"
		default:
			msg += ": upstream error"
		}
	}
	return msg
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

func clusterConnectionIncomplete(info *clusterInfo) bool {
	if info == nil {
		return true
	}
	return info.Endpoints.Public.Host == "" || info.Endpoints.Public.Port == 0 || (info.UserPrefix == "" && info.Username == "")
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
			return nil, fmt.Errorf("%s", statusError("cluster get", resp.StatusCode, sanitizeUpstreamBody(raw)))
		}
		info, err := parseClusterInfo(raw)
		if err != nil {
			return nil, err
		}
		if info.State == "ACTIVE" && !clusterConnectionIncomplete(info) {
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
