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
	"net"
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
	stateActive         = "ACTIVE"

	Drive9ManagedLabel         = "drive9.ai/managed"
	Drive9TenantIDLabel        = "drive9.ai/tenant_id"
	Drive9QuotaUpdateAtLabel   = "drive9.ai/update_quota_at"
	TiDBCloudOrganizationLabel = "tidb.cloud/organization"

	upstreamErrorBodyLimit   = 2048
	upstreamClusterBodyLimit = 1 << 20
)

var (
	immutableLabelKeys = []string{
		"tidb.cloud/project",
		TiDBCloudOrganizationLabel,
	}
)

var databaseNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)
var displayNameCharPattern = regexp.MustCompile(`[^A-Za-z0-9-]`)

var (
	ensureDatabaseFunc          = ensureDatabase
	tidbCloudNativePollInterval = 5 * time.Second
)

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
	if err := p.EnsureDatabase(ctx, dsn); err != nil {
		return err
	}
	return schema.InitTiDBTenantSchemaForModeWithOptionsContext(ctx, dsn, schema.TiDBEmbeddingModeAuto, schema.InitTiDBTenantSchemaOptions{})
}

func (p *Provisioner) InitSchemaForAutoEmbeddingProfile(ctx context.Context, dsn string, profile schema.TiDBAutoEmbeddingProfile) error {
	return schema.InitTiDBTenantSchemaForAutoEmbeddingProfileContext(ctx, dsn, profile)
}

func (p *Provisioner) EnsureDatabase(ctx context.Context, dsn string) error {
	if err := ensureDatabaseFromDSN(ctx, dsn); err != nil {
		return fmt.Errorf("ensure tidbcloud native database: %w", err)
	}
	return nil
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
		"labels": map[string]string{
			Drive9ManagedLabel:  "true",
			Drive9TenantIDLabel: tenantID,
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
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, readErr := readUpstreamBody(resp.Body, upstreamErrorBodyLimit+1)
		if readErr != nil {
			return nil, readErr
		}
		return nil, fmt.Errorf("%s", statusError("provision", resp.StatusCode, sanitizeUpstreamBody(raw)))
	}
	raw, readErr := readUpstreamBody(resp.Body, upstreamClusterBodyLimit)
	if readErr != nil {
		return nil, readErr
	}
	info, err := parseClusterInfo(raw)
	if err != nil {
		return nil, err
	}
	if info.ClusterID == "" {
		return nil, fmt.Errorf("tidbcloud native response missing cluster id")
	}
	if clusterProvisionMetadataIncomplete(info) {
		info, err = p.waitForClusterProvisionMetadata(ctx, publicKey, privateKey, info.ClusterID)
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
		TenantID:       tenantID,
		ClusterID:      info.ClusterID,
		OrganizationID: strings.TrimSpace(info.Labels[TiDBCloudOrganizationLabel]),
		Host:           info.Endpoints.Public.Host,
		Port:           info.Endpoints.Public.Port,
		Username:       info.Username,
		Password:       password,
		DBName:         dbName,
		Provider:       tenant.ProviderTiDBCloudNative,
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
	return out, nil
}

func (p *Provisioner) ProvisionBranch(ctx context.Context, forkTenantID string, source *tenant.ClusterInfo) (*tenant.ClusterInfo, error) {
	req, ok := p.DefaultCredentials()
	if !ok {
		return nil, tenant.ErrCredentialsRequired
	}
	return p.ProvisionBranchWithCredentials(ctx, forkTenantID, source, req)
}

func (p *Provisioner) ProvisionBranchWithCredentials(ctx context.Context, forkTenantID string, source *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) (*tenant.ClusterInfo, error) {
	out, err := p.CreateBranchWithCredentials(ctx, forkTenantID, source, req)
	if err != nil {
		return out, err
	}
	if out.Host != "" && out.Port != 0 && out.Username != "" {
		return out, nil
	}
	return p.WaitForBranchActiveWithCredentials(ctx, out, req)
}

func (p *Provisioner) CreateBranch(ctx context.Context, forkTenantID string, source *tenant.ClusterInfo) (*tenant.ClusterInfo, error) {
	req, ok := p.DefaultCredentials()
	if !ok {
		return nil, tenant.ErrCredentialsRequired
	}
	return p.CreateBranchWithCredentials(ctx, forkTenantID, source, req)
}

func (p *Provisioner) CreateBranchWithCredentials(ctx context.Context, forkTenantID string, source *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) (*tenant.ClusterInfo, error) {
	publicKey := strings.TrimSpace(req.PublicKey)
	privateKey := strings.TrimSpace(req.PrivateKey)
	if publicKey == "" || privateKey == "" {
		return nil, tenant.ErrCredentialsRequired
	}
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
		"displayName": clusterDisplayName(forkTenantID),
		"parentId":    parentID,
	}
	if source.Password != "" {
		reqBody["rootPassword"] = source.Password
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal branch provision request: %w", err)
	}
	endpoint := fmt.Sprintf("%s/v1beta1/clusters/%s/branches", p.apiURL, url.PathEscape(source.ClusterID))
	resp, err := p.doDigestAuthRequest(ctx, publicKey, privateKey, http.MethodPost, endpoint, body)
	if err != nil {
		return nil, fmt.Errorf("create branch request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		raw, readErr := readUpstreamBody(resp.Body, upstreamErrorBodyLimit+1)
		if readErr != nil {
			return nil, readErr
		}
		return nil, fmt.Errorf("%s", statusError("branch provision", resp.StatusCode, sanitizeUpstreamBody(raw)))
	}

	raw, readErr := readUpstreamBody(resp.Body, upstreamClusterBodyLimit)
	if readErr != nil {
		return nil, readErr
	}
	branch, err := parseBranchInfo(raw)
	if err != nil {
		return nil, fmt.Errorf("parse branch provision response: %w", err)
	}
	if branch.BranchID == "" {
		return nil, fmt.Errorf("tidbcloud native branch response missing branch id")
	}
	dbName := source.DBName
	if dbName == "" {
		dbName = p.defaultDatabaseName
	}
	out := &tenant.ClusterInfo{
		TenantID:  forkTenantID,
		ClusterID: source.ClusterID,
		BranchID:  branch.BranchID,
		Password:  source.Password,
		DBName:    dbName,
		Provider:  tenant.ProviderTiDBCloudNative,
	}
	if !branchConnectionIncomplete(branch) {
		if err := fillBranchEndpoint(out, branch); err != nil {
			return out, err
		}
		return out, nil
	}
	if branch.State == "" {
		return out, fmt.Errorf("tidbcloud native branch response missing state and endpoint")
	}
	return out, nil
}

func (p *Provisioner) WaitForBranchActive(ctx context.Context, branch *tenant.ClusterInfo) (*tenant.ClusterInfo, error) {
	req, ok := p.DefaultCredentials()
	if !ok {
		return nil, tenant.ErrCredentialsRequired
	}
	return p.WaitForBranchActiveWithCredentials(ctx, branch, req)
}

func (p *Provisioner) WaitForBranchActiveWithCredentials(ctx context.Context, branch *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) (*tenant.ClusterInfo, error) {
	publicKey := strings.TrimSpace(req.PublicKey)
	privateKey := strings.TrimSpace(req.PrivateKey)
	if publicKey == "" || privateKey == "" {
		return nil, tenant.ErrCredentialsRequired
	}
	if branch == nil {
		return nil, fmt.Errorf("branch cluster info is required")
	}
	if branch.ClusterID == "" || branch.BranchID == "" {
		return nil, fmt.Errorf("cluster id and branch id are required")
	}
	out := *branch
	info, err := p.waitForBranchActive(ctx, publicKey, privateKey, branch.ClusterID, branch.BranchID)
	if err != nil {
		return &out, fmt.Errorf("wait for branch active: %w", err)
	}
	if err := fillBranchEndpoint(&out, info); err != nil {
		return &out, err
	}
	return &out, nil
}

func (p *Provisioner) DeleteBranch(ctx context.Context, clusterID, branchID string) error {
	req, ok := p.DefaultCredentials()
	if !ok {
		return tenant.ErrCredentialsRequired
	}
	return p.DeleteBranchWithCredentials(ctx, clusterID, branchID, req)
}

func (p *Provisioner) DeleteBranchWithCredentials(ctx context.Context, clusterID, branchID string, req tenant.CredentialProvisionRequest) error {
	publicKey := strings.TrimSpace(req.PublicKey)
	privateKey := strings.TrimSpace(req.PrivateKey)
	if publicKey == "" || privateKey == "" {
		return tenant.ErrCredentialsRequired
	}
	if clusterID == "" || branchID == "" {
		return fmt.Errorf("cluster id and branch id are required")
	}
	endpoint := fmt.Sprintf("%s/v1beta1/clusters/%s/branches/%s", p.apiURL, url.PathEscape(clusterID), url.PathEscape(branchID))
	resp, err := p.doDigestAuthRequest(ctx, publicKey, privateKey, http.MethodDelete, endpoint, nil)
	if err != nil {
		return fmt.Errorf("delete branch request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		raw, readErr := readUpstreamBody(resp.Body, upstreamErrorBodyLimit+1)
		if readErr != nil {
			return readErr
		}
		return fmt.Errorf("%s", statusError("branch delete", resp.StatusCode, sanitizeUpstreamBody(raw)))
	}
	return nil
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
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		raw, readErr := readUpstreamBody(resp.Body, upstreamErrorBodyLimit+1)
		if readErr != nil {
			return readErr
		}
		return fmt.Errorf("%s", statusError("cluster delete", resp.StatusCode, sanitizeUpstreamBody(raw)))
	}
	return nil
}

func (p *Provisioner) UpdateQuota(ctx context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest, opts tenant.QuotaUpdateOptions) (*tenant.QuotaCloudConfig, error) {
	publicKey := strings.TrimSpace(req.PublicKey)
	privateKey := strings.TrimSpace(req.PrivateKey)
	if publicKey == "" || privateKey == "" {
		return nil, fmt.Errorf("public_key and private_key are required")
	}
	if cluster == nil || strings.TrimSpace(cluster.ClusterID) == "" {
		return nil, fmt.Errorf("cluster id is required")
	}
	if opts.TiDBCloudSpendingLimitMonthly != nil {
		if err := validateTiDBCloudSpendingLimit(*opts.TiDBCloudSpendingLimitMonthly); err != nil {
			return nil, err
		}
	}
	clusterID := strings.TrimSpace(cluster.ClusterID)
	cloudCfg := &tenant.QuotaCloudConfig{}
	if opts.TiDBCloudSpendingLimitMonthly != nil {
		monthly := *opts.TiDBCloudSpendingLimitMonthly
		if err := p.updateSpendingLimitWithCredentials(ctx, publicKey, privateKey, clusterID, monthly); err != nil {
			return nil, fmt.Errorf("update cluster spending limit: %w", err)
		}
		cloudCfg.TiDBCloudSpendingLimitMonthly = ptrInt64(monthly)
	}
	return cloudCfg, nil
}

func (p *Provisioner) MarkQuotaUpdateStarted(ctx context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) (*tenant.QuotaCloudConfig, error) {
	publicKey := strings.TrimSpace(req.PublicKey)
	privateKey := strings.TrimSpace(req.PrivateKey)
	if publicKey == "" || privateKey == "" {
		return nil, fmt.Errorf("public_key and private_key are required")
	}
	if cluster == nil || strings.TrimSpace(cluster.ClusterID) == "" {
		return nil, fmt.Errorf("cluster id is required")
	}
	clusterID := strings.TrimSpace(cluster.ClusterID)
	labels, cloudCfg, err := p.clusterQuotaInfo(ctx, publicKey, privateKey, clusterID)
	if err != nil {
		return nil, fmt.Errorf("load cluster quota info: %w", err)
	}
	if cloudCfg == nil {
		cloudCfg = &tenant.QuotaCloudConfig{}
	}
	labels[Drive9ManagedLabel] = "true"
	if tenantID := strings.TrimSpace(cluster.TenantID); tenantID != "" {
		labels[Drive9TenantIDLabel] = tenantID
	}
	labels[Drive9QuotaUpdateAtLabel] = strconv.FormatInt(time.Now().UTC().Unix(), 10)
	for _, k := range immutableLabelKeys {
		delete(labels, k)
	}
	if err := p.updateQuotaLabelsWithCredentials(ctx, publicKey, privateKey, clusterID, labels); err != nil {
		return nil, fmt.Errorf("update cluster quota labels: %w", err)
	}
	cloudCfg.Labels = labels
	return cloudCfg, nil
}

func (p *Provisioner) updateQuotaLabelsWithCredentials(ctx context.Context, publicKey, privateKey, clusterID string, labels map[string]string) error {
	body, err := json.Marshal(map[string]any{
		"cluster": map[string]any{
			"labels": labels,
		},
		"updateMask": "labels",
	})
	if err != nil {
		return fmt.Errorf("marshal cluster label patch: %w", err)
	}
	endpoint := fmt.Sprintf("%s/v1beta1/clusters/%s", p.apiURL, url.PathEscape(clusterID))
	resp, err := p.doDigestAuthRequest(ctx, publicKey, privateKey, http.MethodPatch, endpoint, body)
	if err != nil {
		return fmt.Errorf("patch cluster labels: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, readErr := readUpstreamBody(resp.Body, upstreamErrorBodyLimit+1)
		if readErr != nil {
			return fmt.Errorf("read cluster label update error body: %w", readErr)
		}
		return quotaStatusError("cluster label update", resp.StatusCode, sanitizeUpstreamBody(raw))
	}
	return nil
}

func (p *Provisioner) GetQuota(ctx context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) (*tenant.QuotaCloudConfig, error) {
	publicKey := strings.TrimSpace(req.PublicKey)
	privateKey := strings.TrimSpace(req.PrivateKey)
	if publicKey == "" || privateKey == "" {
		return nil, fmt.Errorf("public_key and private_key are required")
	}
	if cluster == nil || strings.TrimSpace(cluster.ClusterID) == "" {
		return nil, fmt.Errorf("cluster id is required")
	}
	_, cloudCfg, err := p.clusterQuotaInfo(ctx, publicKey, privateKey, strings.TrimSpace(cluster.ClusterID))
	if err != nil {
		return nil, fmt.Errorf("load cluster quota info: %w", err)
	}
	return cloudCfg, nil
}

func (p *Provisioner) ListManagedClusters(ctx context.Context, req tenant.CredentialProvisionRequest, opts tenant.ManagedClusterListOptions) (*tenant.ManagedClusterListResult, error) {
	publicKey := strings.TrimSpace(req.PublicKey)
	privateKey := strings.TrimSpace(req.PrivateKey)
	if publicKey == "" || privateKey == "" {
		return nil, fmt.Errorf("public_key and private_key are required")
	}
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}
	values := url.Values{}
	values.Set("pageSize", strconv.Itoa(pageSize))
	if token := strings.TrimSpace(opts.PageToken); token != "" {
		values.Set("pageToken", token)
	}
	filter := fmt.Sprintf(`labels.%q = "true"`, Drive9ManagedLabel)
	if clusterID := strings.TrimSpace(opts.ClusterID); clusterID != "" {
		filter = fmt.Sprintf(`clusterId = %q AND %s`, clusterID, filter)
	}
	values.Set("filter", filter)
	endpoint := p.apiURL + "/v1beta1/clusters?" + values.Encode()
	resp, err := p.doDigestAuthRequest(ctx, publicKey, privateKey, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("list managed clusters: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, readErr := readUpstreamBody(resp.Body, upstreamErrorBodyLimit+1)
		if readErr != nil {
			return nil, fmt.Errorf("read cluster list error body: %w", readErr)
		}
		return nil, quotaStatusError("cluster list", resp.StatusCode, sanitizeUpstreamBody(raw))
	}
	raw, readErr := readUpstreamBody(resp.Body, upstreamClusterBodyLimit)
	if readErr != nil {
		return nil, fmt.Errorf("read cluster list body: %w", readErr)
	}
	list, err := parseClusterList(raw)
	if err != nil {
		return nil, fmt.Errorf("parse cluster list: %w", err)
	}
	out := &tenant.ManagedClusterListResult{
		Clusters:      make([]tenant.CloudClusterInfo, 0, len(list.Clusters)),
		NextPageToken: strings.TrimSpace(list.NextPageToken),
	}
	for _, info := range list.Clusters {
		out.Clusters = append(out.Clusters, cloudClusterInfoFromClusterInfo(info))
	}
	return out, nil
}

func (p *Provisioner) clusterQuotaInfo(ctx context.Context, publicKey, privateKey, clusterID string) (map[string]string, *tenant.QuotaCloudConfig, error) {
	endpoint := fmt.Sprintf("%s/v1beta1/clusters/%s", p.apiURL, url.PathEscape(clusterID))
	resp, err := p.doDigestAuthRequest(ctx, publicKey, privateKey, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("get cluster quota info: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, readErr := readUpstreamBody(resp.Body, upstreamErrorBodyLimit+1)
		if readErr != nil {
			return nil, nil, fmt.Errorf("read cluster get error body: %w", readErr)
		}
		return nil, nil, quotaStatusError("cluster get", resp.StatusCode, sanitizeUpstreamBody(raw))
	}
	raw, readErr := readUpstreamBody(resp.Body, upstreamClusterBodyLimit)
	if readErr != nil {
		return nil, nil, fmt.Errorf("read cluster body: %w", readErr)
	}
	info, err := parseClusterInfo(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("parse cluster quota info: %w", err)
	}
	labels := make(map[string]string, len(info.Labels)+3)
	for k, v := range info.Labels {
		labels[k] = v
	}
	cloudCfg := &tenant.QuotaCloudConfig{}
	if info.SpendingLimit != nil {
		cloudCfg.TiDBCloudSpendingLimitMonthly = ptrInt64(int64(info.SpendingLimit.Monthly))
	}
	return labels, cloudCfg, nil
}

func (p *Provisioner) updateSpendingLimitWithCredentials(ctx context.Context, publicKey, privateKey, clusterID string, monthly int64) error {
	if err := validateTiDBCloudSpendingLimit(monthly); err != nil {
		return err
	}
	body, err := json.Marshal(map[string]any{
		"updateMask": "spendingLimit.monthly",
		"cluster": map[string]any{
			"spendingLimit": map[string]int32{"monthly": int32(monthly)},
		},
	})
	if err != nil {
		return fmt.Errorf("marshal cluster spending limit patch: %w", err)
	}
	endpoint := fmt.Sprintf("%s/v1beta1/clusters/%s", p.apiURL, url.PathEscape(clusterID))
	resp, err := p.doDigestAuthRequest(ctx, publicKey, privateKey, http.MethodPatch, endpoint, body)
	if err != nil {
		return fmt.Errorf("patch cluster spending limit: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, readErr := readUpstreamBody(resp.Body, upstreamErrorBodyLimit+1)
		if readErr != nil {
			return fmt.Errorf("read cluster spending limit update error body: %w", readErr)
		}
		return quotaStatusError("cluster spending limit update", resp.StatusCode, sanitizeUpstreamBody(raw))
	}
	return nil
}

func validateTiDBCloudSpendingLimit(monthly int64) error {
	const maxInt32 = int64(1<<31 - 1)
	if monthly < 0 {
		return fmt.Errorf("tidbcloud_spending_limit must be non-negative")
	}
	if monthly > maxInt32 {
		return fmt.Errorf("tidbcloud_spending_limit is too large")
	}
	return nil
}

func ptrInt64(v int64) *int64 {
	return &v
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
		return nil, fmt.Errorf("invalid %s value %q: must be a non-negative integer", EnvTiDBCloudDefaultSpendingLimit, raw)
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

func readUpstreamBody(r io.Reader, limit int64) ([]byte, error) {
	if r == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = upstreamErrorBodyLimit + 1
	}
	raw, err := io.ReadAll(io.LimitReader(r, limit))
	if err != nil {
		return nil, fmt.Errorf("read upstream response body: %w", err)
	}
	return raw, nil
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

func quotaStatusError(operation string, code int, upstreamBody string) error {
	msg := statusError(operation, code, upstreamBody)
	switch code {
	case http.StatusUnauthorized:
		return fmt.Errorf("%w: %s", tenant.ErrQuotaPermissionDenied, msg)
	case http.StatusForbidden:
		return fmt.Errorf("%w: %s", tenant.ErrQuotaPermissionDenied, msg)
	case http.StatusNotFound:
		return fmt.Errorf("%w: %s", tenant.ErrQuotaBackendNotFound, msg)
	default:
		return fmt.Errorf("%s", msg)
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

func ensureDatabaseFromDSN(ctx context.Context, dsn string) error {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return fmt.Errorf("parse native tenant DSN: %w", err)
	}
	dbName, err := normalizeDatabaseName(cfg.DBName)
	if err != nil {
		return err
	}
	if cfg.User == "" {
		return fmt.Errorf("native tenant DSN user is empty")
	}
	if cfg.Passwd == "" {
		return fmt.Errorf("native tenant DSN password is empty")
	}
	if cfg.Net != "tcp" {
		return fmt.Errorf("native tenant DSN network must be tcp, got %q", cfg.Net)
	}
	host, port, err := splitTCPAddr(cfg.Addr)
	if err != nil {
		return err
	}
	return ensureDatabaseFunc(ctx, cfg.User, cfg.Passwd, host, port, dbName)
}

func splitTCPAddr(addr string) (string, int, error) {
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, fmt.Errorf("parse native tenant DSN address %q: %w", addr, err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return "", 0, fmt.Errorf("parse native tenant DSN port %q: %w", portText, err)
	}
	if port <= 0 {
		return "", 0, fmt.Errorf("native tenant DSN port must be positive, got %d", port)
	}
	if strings.TrimSpace(host) == "" {
		return "", 0, fmt.Errorf("native tenant DSN host is empty")
	}
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		host = "[" + host + "]"
	}
	return host, port, nil
}

type clusterInfo struct {
	ClusterID     string            `json:"clusterId"`
	State         string            `json:"state"`
	Labels        map[string]string `json:"labels"`
	SpendingLimit *struct {
		Monthly int32 `json:"monthly"`
	} `json:"spendingLimit"`
	Endpoints struct {
		Public struct {
			Host string `json:"host"`
			Port int    `json:"port"`
		} `json:"public"`
	} `json:"endpoints"`
	UserPrefix string `json:"userPrefix"`
	Username   string `json:"username"`
}

type branchInfo struct {
	BranchID  string `json:"branchId"`
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

type clusterListResponse struct {
	Clusters      []clusterInfo `json:"clusters"`
	NextPageToken string        `json:"nextPageToken"`
}

func parseClusterInfo(raw []byte) (*clusterInfo, error) {
	var out clusterInfo
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func parseClusterList(raw []byte) (*clusterListResponse, error) {
	var out clusterListResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func cloudClusterInfoFromClusterInfo(info clusterInfo) tenant.CloudClusterInfo {
	labels := make(map[string]string, len(info.Labels))
	for k, v := range info.Labels {
		labels[k] = v
	}
	out := tenant.CloudClusterInfo{
		ClusterID:      strings.TrimSpace(info.ClusterID),
		OrganizationID: strings.TrimSpace(info.Labels[TiDBCloudOrganizationLabel]),
		Labels:         labels,
	}
	if info.SpendingLimit != nil {
		out.TiDBCloudSpendingLimitMonthly = ptrInt64(int64(info.SpendingLimit.Monthly))
	}
	return out
}

func parseBranchInfo(raw []byte) (*branchInfo, error) {
	var out branchInfo
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

func clusterProvisionMetadataIncomplete(info *clusterInfo) bool {
	if clusterConnectionIncomplete(info) {
		return true
	}
	return strings.TrimSpace(info.Labels[TiDBCloudOrganizationLabel]) == ""
}

func branchConnectionIncomplete(info *branchInfo) bool {
	if info == nil {
		return true
	}
	return info.Endpoints.Public.Host == "" || info.Endpoints.Public.Port == 0 || (info.UserPrefix == "" && info.Username == "")
}

func fillBranchEndpoint(out *tenant.ClusterInfo, branch *branchInfo) error {
	if branch.Endpoints.Public.Host == "" || branch.Endpoints.Public.Port == 0 {
		return fmt.Errorf("tidbcloud native branch response missing endpoint")
	}
	if branch.Username != "" {
		out.Username = branch.Username
	} else if branch.UserPrefix != "" {
		out.Username = branch.UserPrefix + ".root"
	}
	if out.Username == "" {
		return fmt.Errorf("tidbcloud native branch response missing username")
	}
	out.Host = branch.Endpoints.Public.Host
	out.Port = branch.Endpoints.Public.Port
	return nil
}

func (p *Provisioner) waitForClusterProvisionMetadata(ctx context.Context, publicKey, privateKey, clusterID string) (*clusterInfo, error) {
	deadline := time.Now().Add(10 * time.Minute)
	for {
		endpoint := fmt.Sprintf("%s/v1beta1/clusters/%s?view=BASIC", p.apiURL, clusterID)
		resp, err := p.doDigestAuthRequest(ctx, publicKey, privateKey, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		limit := int64(upstreamClusterBodyLimit)
		if resp.StatusCode != http.StatusOK {
			limit = upstreamErrorBodyLimit + 1
		}
		raw, readErr := readUpstreamBody(resp.Body, limit)
		_ = resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("%s", statusError("cluster get", resp.StatusCode, sanitizeUpstreamBody(raw)))
		}
		info, err := parseClusterInfo(raw)
		if err != nil {
			return nil, err
		}
		if !clusterProvisionMetadataIncomplete(info) {
			return info, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("tidbcloud native cluster %s missing connection metadata or organization label before timeout: %s", clusterID, info.State)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(tidbCloudNativePollInterval):
		}
	}
}

func (p *Provisioner) waitForBranchActive(ctx context.Context, publicKey, privateKey, clusterID, branchID string) (*branchInfo, error) {
	deadline := time.Now().Add(10 * time.Minute)
	for {
		endpoint := fmt.Sprintf("%s/v1beta1/clusters/%s/branches/%s?view=BASIC", p.apiURL, url.PathEscape(clusterID), url.PathEscape(branchID))
		resp, err := p.doDigestAuthRequest(ctx, publicKey, privateKey, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		limit := int64(upstreamClusterBodyLimit)
		if resp.StatusCode != http.StatusOK {
			limit = upstreamErrorBodyLimit + 1
		}
		raw, readErr := readUpstreamBody(resp.Body, limit)
		_ = resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("%s", statusError("branch get", resp.StatusCode, sanitizeUpstreamBody(raw)))
		}
		info, err := parseBranchInfo(raw)
		if err != nil {
			return nil, err
		}
		if info.State == stateActive && !branchConnectionIncomplete(info) {
			return info, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("tidbcloud native branch %s not active before timeout: %s", branchID, info.State)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(tidbCloudNativePollInterval):
		}
	}
}

func (p *Provisioner) WaitForBranchUserWithCredentials(ctx context.Context, clusterID, branchID string, req tenant.CredentialProvisionRequest) (string, error) {
	publicKey := strings.TrimSpace(req.PublicKey)
	privateKey := strings.TrimSpace(req.PrivateKey)
	if publicKey == "" || privateKey == "" {
		return "", tenant.ErrCredentialsRequired
	}
	deadline := time.Now().Add(10 * time.Minute)
	for {
		endpoint := fmt.Sprintf("%s/v1beta1/clusters/%s/branches/%s?view=BASIC", p.apiURL, url.PathEscape(clusterID), url.PathEscape(branchID))
		resp, err := p.doDigestAuthRequest(ctx, publicKey, privateKey, http.MethodGet, endpoint, nil)
		if err != nil {
			return "", err
		}
		limit := int64(upstreamClusterBodyLimit)
		if resp.StatusCode != http.StatusOK {
			limit = upstreamErrorBodyLimit + 1
		}
		raw, readErr := readUpstreamBody(resp.Body, limit)
		_ = resp.Body.Close()
		if readErr != nil {
			return "", readErr
		}
		if resp.StatusCode != http.StatusOK {
			return "", fmt.Errorf("%s", statusError("branch get", resp.StatusCode, sanitizeUpstreamBody(raw)))
		}
		info, err := parseBranchInfo(raw)
		if err != nil {
			return "", err
		}
		if info.Username != "" {
			return info.Username, nil
		}
		if info.UserPrefix != "" {
			return info.UserPrefix + ".root", nil
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("tidbcloud native branch %s user prefix not available before timeout", branchID)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(tidbCloudNativePollInterval):
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
