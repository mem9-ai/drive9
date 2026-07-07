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
	"errors"
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
	"sync"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"github.com/mem9-ai/drive9/pkg/tenant/schema"
)

const (
	EnvTiDBCloudNativeAPIURL                  = "DRIVE9_TIDBCLOUD_NATIVE_API_URL"
	EnvTiDBCloudNativeCloudProvider           = "DRIVE9_TIDBCLOUD_NATIVE_CLOUD_PROVIDER"
	EnvTiDBCloudNativeRegion                  = "DRIVE9_TIDBCLOUD_NATIVE_REGION"
	EnvTiDBCloudNativeDefaultDatabaseName     = "DRIVE9_TIDBCLOUD_NATIVE_DEFAULT_DATABASE_NAME"
	EnvTiDBCloudDefaultSpendingLimit          = "DRIVE9_TIDBCLOUD_DEFAULT_SPENDING_LIMIT"
	EnvTiDBCloudNativePublicKey               = "DRIVE9_TIDBCLOUD_NATIVE_PUBLIC_KEY"
	EnvTiDBCloudNativePrivateKey              = "DRIVE9_TIDBCLOUD_NATIVE_PRIVATE_KEY"
	EnvTiDBCloudNativeUsePrivateEndpoint      = "DRIVE9_TIDBCLOUD_NATIVE_USE_PRIVATE_ENDPOINT"
	EnvTiDBCloudTencentPrivateEndpointHost    = "DRIVE9_TIDBCLOUD_TENCENT_PRIVATE_ENDPOINT_HOST"
	EnvTiDBCloudAlicloudPrivateEndpointDomain = "DRIVE9_TIDBCLOUD_ALICLOUD_PRIVATE_ENDPOINT_DOMAIN"

	DefaultDatabaseName = "tidbcloud_fs"
	DefaultSpendLimit   = int32(1000)
	stateActive         = "ACTIVE"

	cloudProviderTencentCloud = "tencentcloud"
	cloudProviderAliCloud     = "alicloud"
	cloudProviderAWS          = "aws"

	Drive9ManagedLabel         = "drive9.ai/managed"
	Drive9TenantIDLabel        = "drive9.ai/tenant_id"
	Drive9PoolStatusLabel      = "drive9.ai/status"
	Drive9PoolIDLabel          = "drive9.ai/pool_id"
	Drive9PoolUsedAtLabel      = "drive9.ai/used_at"
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
	ensureDatabaseFunc                       = ensureDatabase
	tidbCloudNativePollInterval              = 5 * time.Second
	tidbCloudNativeBatchMetadataGroupSize    = 10
	tidbCloudNativeBatchMetadataPollInterval = 15 * time.Second
)

type Provisioner struct {
	apiURL                      string
	cloudProvider               string
	region                      string
	defaultDatabaseName         string
	defaultSpendLimit           *int32
	defaultPublicKey            string
	defaultPrivateKey           string
	usePrivateEndpoint          bool
	tencentPrivateEndpointHost  string
	alicloudPrivateEndpointHost string
	client                      *http.Client
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
	usePrivate, err := parseBoolEnv(EnvTiDBCloudNativeUsePrivateEndpoint)
	if err != nil {
		return nil, err
	}
	tencentPrivateHost := strings.TrimSpace(os.Getenv(EnvTiDBCloudTencentPrivateEndpointHost))
	if usePrivate && strings.EqualFold(cloudProvider, cloudProviderTencentCloud) && tencentPrivateHost == "" {
		return nil, fmt.Errorf("%s is required when %s=true and cloud provider is tencentcloud",
			EnvTiDBCloudTencentPrivateEndpointHost, EnvTiDBCloudNativeUsePrivateEndpoint)
	}
	alicloudPrivateHost := strings.TrimSpace(os.Getenv(EnvTiDBCloudAlicloudPrivateEndpointDomain))
	if usePrivate && strings.EqualFold(cloudProvider, cloudProviderAliCloud) && alicloudPrivateHost == "" {
		return nil, fmt.Errorf("%s is required when %s=true and cloud provider is alicloud",
			EnvTiDBCloudAlicloudPrivateEndpointDomain, EnvTiDBCloudNativeUsePrivateEndpoint)
	}
	return &Provisioner{
		apiURL:                      strings.TrimRight(apiURL, "/"),
		cloudProvider:               cloudProvider,
		region:                      region,
		defaultDatabaseName:         defaultDB,
		defaultSpendLimit:           defaultSpendLimit,
		defaultPublicKey:            strings.TrimSpace(os.Getenv(EnvTiDBCloudNativePublicKey)),
		defaultPrivateKey:           strings.TrimSpace(os.Getenv(EnvTiDBCloudNativePrivateKey)),
		usePrivateEndpoint:          usePrivate,
		tencentPrivateEndpointHost:  tencentPrivateHost,
		alicloudPrivateEndpointHost: alicloudPrivateHost,
		client:                      &http.Client{Timeout: 60 * time.Second},
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
	// Direct callers still need database creation; the server auto-embedding
	// path hoists the same ensure before provider pre-configuration.
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
	out, _, err := p.ProvisionWithCredentialsAndQuota(ctx, tenantID, req, tenant.QuotaUpdateOptions{})
	return out, err
}

func (p *Provisioner) ProvisionWithCredentialsAndQuota(ctx context.Context, tenantID string, req tenant.CredentialProvisionRequest, opts tenant.QuotaUpdateOptions) (*tenant.ClusterInfo, *tenant.QuotaCloudConfig, error) {
	publicKey := strings.TrimSpace(req.PublicKey)
	privateKey := strings.TrimSpace(req.PrivateKey)
	if publicKey == "" || privateKey == "" {
		return nil, nil, fmt.Errorf("public_key and private_key are required")
	}
	dbName, err := p.resolveDatabaseName("")
	if err != nil {
		return nil, nil, err
	}
	if opts.TiDBCloudSpendingLimitMonthly != nil {
		if err := validateTiDBCloudSpendingLimit(*opts.TiDBCloudSpendingLimitMonthly); err != nil {
			return nil, nil, err
		}
	}
	password, err := generateRandomPassword(24)
	if err != nil {
		return nil, nil, err
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
	var spendingLimit *int64
	if opts.TiDBCloudSpendingLimitMonthly != nil {
		spendingLimit = opts.TiDBCloudSpendingLimitMonthly
		reqBody["spendingLimit"] = map[string]int32{"monthly": int32(*spendingLimit)}
	} else if p.defaultSpendLimit != nil {
		defaultLimit := int64(*p.defaultSpendLimit)
		spendingLimit = &defaultLimit
		reqBody["spendingLimit"] = map[string]int32{"monthly": *p.defaultSpendLimit}
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, nil, err
	}
	endpoint := p.apiURL + "/v1beta1/clusters"
	resp, err := p.doDigestAuthRequest(ctx, publicKey, privateKey, http.MethodPost, endpoint, body)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, readErr := readUpstreamBody(resp.Body, upstreamErrorBodyLimit+1)
		if readErr != nil {
			return nil, nil, readErr
		}
		return nil, nil, fmt.Errorf("%s", statusError("provision", resp.StatusCode, sanitizeUpstreamBody(raw)))
	}
	raw, readErr := readUpstreamBody(resp.Body, upstreamClusterBodyLimit)
	if readErr != nil {
		return nil, nil, readErr
	}
	info, err := parseClusterInfo(raw)
	if err != nil {
		return nil, nil, err
	}
	if info.ClusterID == "" {
		return nil, nil, fmt.Errorf("tidbcloud native response missing cluster id")
	}
	if clusterProvisionMetadataIncomplete(info, p.usePrivateEndpoint, p.privateEndpointOverrideHost()) {
		clusterID := info.ClusterID
		info, err = p.waitForClusterProvisionMetadata(ctx, publicKey, privateKey, clusterID)
		if err != nil {
			return &tenant.ClusterInfo{
				TenantID:  tenantID,
				ClusterID: clusterID,
				Password:  password,
				DBName:    dbName,
				Provider:  tenant.ProviderTiDBCloudNative,
			}, nil, err
		}
	}
	var host string
	var port int
	if p.usePrivateEndpoint {
		overrideHost := p.privateEndpointOverrideHost()
		if overrideHost != "" {
			host = overrideHost
		} else {
			host = info.Endpoints.Private.Host
		}
		port = info.Endpoints.Private.Port
	} else {
		host = info.Endpoints.Public.Host
		port = info.Endpoints.Public.Port
	}
	out := &tenant.ClusterInfo{
		TenantID:       tenantID,
		ClusterID:      info.ClusterID,
		OrganizationID: strings.TrimSpace(info.Labels[TiDBCloudOrganizationLabel]),
		Host:           host,
		Port:           port,
		Password:       password,
		DBName:         dbName,
		Provider:       tenant.ProviderTiDBCloudNative,
	}
	if info.UserPrefix != "" {
		out.Username = info.UserPrefix + ".root"
	}
	cloudCfg := &tenant.QuotaCloudConfig{
		Labels: map[string]string{
			Drive9ManagedLabel:  "true",
			Drive9TenantIDLabel: tenantID,
		},
	}
	if spendingLimit != nil {
		cloudCfg.TiDBCloudSpendingLimitMonthly = ptrInt64(*spendingLimit)
	}
	return out, cloudCfg, nil
}

func (p *Provisioner) BatchProvisionFreeClustersWithCredentialsAndQuota(ctx context.Context, tenantIDs []string, req tenant.CredentialProvisionRequest, opts tenant.QuotaUpdateOptions) ([]*tenant.ClusterInfo, *tenant.QuotaCloudConfig, error) {
	publicKey := strings.TrimSpace(req.PublicKey)
	privateKey := strings.TrimSpace(req.PrivateKey)
	if publicKey == "" || privateKey == "" {
		return nil, nil, fmt.Errorf("public_key and private_key are required")
	}
	if len(tenantIDs) == 0 {
		return []*tenant.ClusterInfo{}, nil, nil
	}
	dbName, err := p.resolveDatabaseName("")
	if err != nil {
		return nil, nil, err
	}
	if opts.TiDBCloudSpendingLimitMonthly != nil {
		if err := validateTiDBCloudSpendingLimit(*opts.TiDBCloudSpendingLimitMonthly); err != nil {
			return nil, nil, err
		}
	}
	var spendingLimit *int64
	requests := make([]map[string]any, 0, len(tenantIDs))
	passwords := make(map[string]string, len(tenantIDs))
	for _, tenantID := range tenantIDs {
		tenantID = strings.TrimSpace(tenantID)
		if tenantID == "" {
			return nil, nil, fmt.Errorf("tenant id is required")
		}
		password, err := generateRandomPassword(24)
		if err != nil {
			return nil, nil, err
		}
		passwords[tenantID] = password
		labels := map[string]string{
			Drive9ManagedLabel:    "true",
			Drive9TenantIDLabel:   tenantID,
			Drive9PoolStatusLabel: "free",
		}
		if poolID := strings.TrimSpace(opts.TenantPoolID); poolID != "" {
			labels[Drive9PoolIDLabel] = poolID
		}
		cluster := map[string]any{
			"displayName":  clusterDisplayName(tenantID),
			"rootPassword": password,
			"region": map[string]string{
				"name": p.regionName(),
			},
			"labels": labels,
		}
		if opts.TiDBCloudSpendingLimitMonthly != nil {
			spendingLimit = opts.TiDBCloudSpendingLimitMonthly
			cluster["spendingLimit"] = map[string]int32{"monthly": int32(*spendingLimit)}
		} else if p.defaultSpendLimit != nil {
			defaultLimit := int64(*p.defaultSpendLimit)
			spendingLimit = &defaultLimit
			cluster["spendingLimit"] = map[string]int32{"monthly": *p.defaultSpendLimit}
		}
		requests = append(requests, map[string]any{"cluster": cluster})
	}
	body, err := json.Marshal(map[string]any{"requests": requests})
	if err != nil {
		return nil, nil, err
	}
	endpoint := p.apiURL + "/v1beta1/clusters:batchCreate"
	resp, err := p.doDigestAuthRequest(ctx, publicKey, privateKey, http.MethodPost, endpoint, body)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, readErr := readUpstreamBody(resp.Body, upstreamErrorBodyLimit+1)
		if readErr != nil {
			return nil, nil, readErr
		}
		return nil, nil, fmt.Errorf("%s", statusError("batch provision", resp.StatusCode, sanitizeUpstreamBody(raw)))
	}
	raw, readErr := readUpstreamBody(resp.Body, upstreamClusterBodyLimit)
	if readErr != nil {
		return nil, nil, readErr
	}
	var created clusterListResponse
	if err := json.Unmarshal(raw, &created); err != nil {
		return nil, nil, err
	}
	if len(created.Clusters) != len(tenantIDs) {
		return nil, nil, fmt.Errorf("tidbcloud native batch provision returned %d clusters, want %d", len(created.Clusters), len(tenantIDs))
	}
	out := make([]*tenant.ClusterInfo, len(created.Clusters))
	errs := make([]error, len(created.Clusters))
	for i := range created.Clusters {
		i := i
		info := created.Clusters[i]
		tenantID := strings.TrimSpace(info.Labels[Drive9TenantIDLabel])
		if tenantID == "" {
			errs[i] = fmt.Errorf("tidbcloud native batch response missing %s label for cluster %q", Drive9TenantIDLabel, info.ClusterID)
			continue
		}
		password, ok := passwords[tenantID]
		if !ok {
			errs[i] = fmt.Errorf("tidbcloud native batch response returned unknown tenant id %q", tenantID)
			continue
		}
		if strings.TrimSpace(info.ClusterID) == "" {
			errs[i] = fmt.Errorf("tidbcloud native batch response missing cluster id for tenant %q", tenantID)
			continue
		}
		out[i] = p.clusterInfoFromResponse(tenantID, dbName, password, &info)
	}
	for _, err := range errs {
		if err != nil {
			return fallbackBatchClusterInfos(created.Clusters, dbName, passwords), nil, err
		}
	}
	cloudCfg := &tenant.QuotaCloudConfig{Labels: map[string]string{
		Drive9ManagedLabel:    "true",
		Drive9PoolStatusLabel: "free",
	}}
	if poolID := strings.TrimSpace(opts.TenantPoolID); poolID != "" {
		cloudCfg.Labels[Drive9PoolIDLabel] = poolID
	}
	if spendingLimit != nil {
		cloudCfg.TiDBCloudSpendingLimitMonthly = ptrInt64(*spendingLimit)
	}
	return out, cloudCfg, nil
}

type batchClusterMetadataTarget struct {
	index    int
	tenantID string
	password string
	dbName   string
	initial  clusterInfo
}

func (p *Provisioner) waitForBatchClusterProvisionMetadata(ctx context.Context, publicKey, privateKey, poolID string, pending []batchClusterMetadataTarget, out []*tenant.ClusterInfo, errs []error) {
	groupSize := tidbCloudNativeBatchMetadataGroupSize
	if groupSize <= 0 {
		groupSize = 10
	}
	var wg sync.WaitGroup
	for start := 0; start < len(pending); start += groupSize {
		end := start + groupSize
		if end > len(pending) {
			end = len(pending)
		}
		group := pending[start:end]
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.waitForBatchClusterProvisionMetadataGroup(ctx, publicKey, privateKey, poolID, group, out, errs)
		}()
	}
	wg.Wait()
}

func (p *Provisioner) waitForBatchClusterProvisionMetadataGroup(ctx context.Context, publicKey, privateKey, poolID string, group []batchClusterMetadataTarget, out []*tenant.ClusterInfo, errs []error) {
	deadline := time.Now().Add(10 * time.Minute)
	pending := make(map[string]batchClusterMetadataTarget, len(group))
	clusterIDs := make([]string, 0, len(group))
	for _, target := range group {
		clusterID := strings.TrimSpace(target.initial.ClusterID)
		if clusterID == "" {
			errs[target.index] = fmt.Errorf("tidbcloud native batch response missing cluster id for tenant %q", target.tenantID)
			continue
		}
		pending[clusterID] = target
		clusterIDs = append(clusterIDs, clusterID)
	}
	for len(pending) > 0 {
		infos, err := p.listClusterInfosWithCredentials(ctx, publicKey, privateKey, clusterIDs, len(clusterIDs))
		if err != nil {
			if !isTiDBCloudStatus(err, http.StatusTooManyRequests) || time.Now().After(deadline) {
				for _, target := range pending {
					errs[target.index] = err
				}
				return
			}
		} else {
			for i := range infos {
				info := infos[i]
				clusterID := strings.TrimSpace(info.ClusterID)
				target, ok := pending[clusterID]
				if !ok {
					continue
				}
				if tenantID := strings.TrimSpace(info.Labels[Drive9TenantIDLabel]); tenantID != target.tenantID {
					errs[target.index] = fmt.Errorf("tidbcloud native batch metadata tenant label mismatch for cluster %q: got %q, want %q", clusterID, tenantID, target.tenantID)
					delete(pending, clusterID)
					continue
				}
				if poolID != "" && strings.TrimSpace(info.Labels[Drive9PoolIDLabel]) != poolID {
					errs[target.index] = fmt.Errorf("tidbcloud native batch metadata pool label mismatch for cluster %q", clusterID)
					delete(pending, clusterID)
					continue
				}
				if clusterProvisionMetadataIncomplete(&info, p.usePrivateEndpoint, p.privateEndpointOverrideHost()) {
					continue
				}
				out[target.index] = p.clusterInfoFromResponse(target.tenantID, target.dbName, target.password, &info)
				delete(pending, clusterID)
			}
		}
		if len(pending) == 0 {
			return
		}
		if time.Now().After(deadline) {
			for clusterID, target := range pending {
				errs[target.index] = fmt.Errorf("tidbcloud native cluster %s missing connection metadata or organization label before timeout", clusterID)
			}
			return
		}
		select {
		case <-ctx.Done():
			for _, target := range pending {
				errs[target.index] = ctx.Err()
			}
			return
		case <-time.After(batchMetadataPollInterval()):
		}
	}
}

func batchMetadataPollInterval() time.Duration {
	if tidbCloudNativeBatchMetadataPollInterval <= 0 {
		return tidbCloudNativePollInterval
	}
	return tidbCloudNativeBatchMetadataPollInterval
}

func fallbackBatchClusterInfos(clusters []clusterInfo, dbName string, passwords map[string]string) []*tenant.ClusterInfo {
	out := make([]*tenant.ClusterInfo, 0, len(clusters))
	for i := range clusters {
		info := clusters[i]
		tenantID := strings.TrimSpace(info.Labels[Drive9TenantIDLabel])
		out = append(out, &tenant.ClusterInfo{
			TenantID:       tenantID,
			ClusterID:      strings.TrimSpace(info.ClusterID),
			OrganizationID: strings.TrimSpace(info.Labels[TiDBCloudOrganizationLabel]),
			Password:       passwords[tenantID],
			DBName:         dbName,
			Provider:       tenant.ProviderTiDBCloudNative,
		})
	}
	return out
}

func (p *Provisioner) WaitForPoolClusterMetadata(ctx context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) (*tenant.ClusterInfo, error) {
	publicKey := strings.TrimSpace(req.PublicKey)
	privateKey := strings.TrimSpace(req.PrivateKey)
	if publicKey == "" || privateKey == "" {
		return nil, fmt.Errorf("public_key and private_key are required")
	}
	if cluster == nil || strings.TrimSpace(cluster.ClusterID) == "" {
		return nil, fmt.Errorf("cluster id is required")
	}
	dbName := strings.TrimSpace(cluster.DBName)
	if dbName == "" {
		var err error
		dbName, err = p.resolveDatabaseName("")
		if err != nil {
			return nil, err
		}
	}
	info, err := p.waitForClusterProvisionMetadata(ctx, publicKey, privateKey, strings.TrimSpace(cluster.ClusterID))
	if err != nil {
		return nil, err
	}
	return p.clusterInfoFromResponse(strings.TrimSpace(cluster.TenantID), dbName, cluster.Password, info), nil
}

func (p *Provisioner) WaitForPoolClustersMetadata(ctx context.Context, clusters []*tenant.ClusterInfo, req tenant.CredentialProvisionRequest) ([]*tenant.ClusterInfo, error) {
	publicKey := strings.TrimSpace(req.PublicKey)
	privateKey := strings.TrimSpace(req.PrivateKey)
	if publicKey == "" || privateKey == "" {
		return nil, fmt.Errorf("public_key and private_key are required")
	}
	if len(clusters) == 0 {
		return []*tenant.ClusterInfo{}, nil
	}
	out := make([]*tenant.ClusterInfo, len(clusters))
	pending := make([]batchClusterMetadataTarget, 0, len(clusters))
	errs := make([]error, len(clusters))
	for i, cluster := range clusters {
		if cluster == nil {
			errs[i] = fmt.Errorf("cluster is required")
			continue
		}
		tenantID := strings.TrimSpace(cluster.TenantID)
		if tenantID == "" {
			errs[i] = fmt.Errorf("cluster tenant id is required")
			continue
		}
		if strings.TrimSpace(cluster.ClusterID) == "" {
			errs[i] = fmt.Errorf("cluster id is required for tenant %q", tenantID)
			continue
		}
		dbName := strings.TrimSpace(cluster.DBName)
		if dbName == "" {
			var err error
			dbName, err = p.resolveDatabaseName("")
			if err != nil {
				errs[i] = err
				continue
			}
		}
		pending = append(pending, batchClusterMetadataTarget{
			index:    i,
			tenantID: tenantID,
			password: cluster.Password,
			dbName:   dbName,
			initial: clusterInfo{
				ClusterID: strings.TrimSpace(cluster.ClusterID),
				Labels: map[string]string{
					Drive9TenantIDLabel: tenantID,
				},
			},
		})
	}
	if len(pending) > 0 {
		p.waitForBatchClusterProvisionMetadata(ctx, publicKey, privateKey, "", pending, out, errs)
	}
	return out, errors.Join(errs...)
}

func (p *Provisioner) MarkClusterPoolUsed(ctx context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest, usedAt time.Time, opts tenant.QuotaUpdateOptions) (*tenant.QuotaCloudConfig, error) {
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
	labels, cloudCfg, err := p.clusterQuotaInfo(ctx, publicKey, privateKey, clusterID)
	if err != nil {
		return nil, fmt.Errorf("load cluster pool label info: %w", err)
	}
	labels[Drive9ManagedLabel] = "true"
	if tenantID := strings.TrimSpace(cluster.TenantID); tenantID != "" {
		labels[Drive9TenantIDLabel] = tenantID
	}
	labels[Drive9PoolStatusLabel] = "used"
	labels[Drive9PoolUsedAtLabel] = usedAt.UTC().Format(time.RFC3339)
	for _, k := range immutableLabelKeys {
		delete(labels, k)
	}
	if err := p.updateQuotaLabelsWithCredentials(ctx, publicKey, privateKey, clusterID, labels); err != nil {
		return nil, fmt.Errorf("update cluster pool labels: %w", err)
	}
	if cloudCfg == nil {
		cloudCfg = &tenant.QuotaCloudConfig{}
	}
	cloudCfg.Labels = labels
	if opts.TiDBCloudSpendingLimitMonthly != nil {
		monthly := *opts.TiDBCloudSpendingLimitMonthly
		if err := p.updateSpendingLimitWithCredentials(ctx, publicKey, privateKey, clusterID, monthly); err != nil {
			return nil, fmt.Errorf("update cluster spending limit: %w", err)
		}
		cloudCfg.TiDBCloudSpendingLimitMonthly = ptrInt64(monthly)
	}
	return cloudCfg, nil
}

func (p *Provisioner) MarkClusterPoolFree(ctx context.Context, cluster *tenant.ClusterInfo, req tenant.CredentialProvisionRequest) error {
	publicKey := strings.TrimSpace(req.PublicKey)
	privateKey := strings.TrimSpace(req.PrivateKey)
	if publicKey == "" || privateKey == "" {
		return fmt.Errorf("public_key and private_key are required")
	}
	if cluster == nil || strings.TrimSpace(cluster.ClusterID) == "" {
		return fmt.Errorf("cluster id is required")
	}
	clusterID := strings.TrimSpace(cluster.ClusterID)
	labels, _, err := p.clusterQuotaInfo(ctx, publicKey, privateKey, clusterID)
	if err != nil {
		return fmt.Errorf("load cluster pool label info: %w", err)
	}
	labels[Drive9ManagedLabel] = "true"
	if tenantID := strings.TrimSpace(cluster.TenantID); tenantID != "" {
		labels[Drive9TenantIDLabel] = tenantID
	}
	labels[Drive9PoolStatusLabel] = "free"
	delete(labels, Drive9PoolUsedAtLabel)
	for _, k := range immutableLabelKeys {
		delete(labels, k)
	}
	return p.updateQuotaLabelsWithCredentials(ctx, publicKey, privateKey, clusterID, labels)
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
	if !branchConnectionIncomplete(branch, p.usePrivateEndpoint, p.privateEndpointOverrideHost()) {
		if err := p.fillBranchEndpoint(out, branch); err != nil {
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
	if err := p.fillBranchEndpoint(&out, info); err != nil {
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
	infos, nextPageToken, err := p.listClusterInfosPageWithCredentials(ctx, publicKey, privateKey, []string{opts.ClusterID}, pageSize, opts.PageToken)
	if err != nil {
		return nil, fmt.Errorf("list managed clusters: %w", err)
	}
	out := &tenant.ManagedClusterListResult{
		Clusters:      make([]tenant.CloudClusterInfo, 0, len(infos)),
		NextPageToken: strings.TrimSpace(nextPageToken),
	}
	for _, info := range infos {
		out.Clusters = append(out.Clusters, cloudClusterInfoFromClusterInfo(info))
	}
	return out, nil
}

func (p *Provisioner) listClusterInfosWithCredentials(ctx context.Context, publicKey, privateKey string, clusterIDs []string, pageSize int) ([]clusterInfo, error) {
	var out []clusterInfo
	pageToken := ""
	for {
		infos, nextPageToken, err := p.listClusterInfosPageWithCredentials(ctx, publicKey, privateKey, clusterIDs, pageSize, pageToken)
		if err != nil {
			return nil, err
		}
		out = append(out, infos...)
		if nextPageToken == "" {
			return out, nil
		}
		pageToken = nextPageToken
	}
}

func (p *Provisioner) listClusterInfosPageWithCredentials(ctx context.Context, publicKey, privateKey string, clusterIDs []string, pageSize int, pageToken string) ([]clusterInfo, string, error) {
	if pageSize <= 0 {
		pageSize = 100
	}
	values := url.Values{}
	values.Set("pageSize", strconv.Itoa(pageSize))
	if token := strings.TrimSpace(pageToken); token != "" {
		values.Set("pageToken", token)
	}
	filter := fmt.Sprintf(`labels.%q = "true"`, Drive9ManagedLabel)
	clusterIDFilter := compactNonEmptyStrings(clusterIDs)
	if len(clusterIDFilter) > 0 {
		// TiDB Cloud serverless cvtGlobalFilter splits comma-separated clusterId values server-side.
		filter = fmt.Sprintf("clusterId = %q AND %s", strings.Join(clusterIDFilter, ","), filter)
	}
	values.Set("filter", filter)
	endpoint := p.apiURL + "/v1beta1/clusters?" + values.Encode()
	resp, err := p.doDigestAuthRequest(ctx, publicKey, privateKey, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, readErr := readUpstreamBody(resp.Body, upstreamErrorBodyLimit+1)
		if readErr != nil {
			return nil, "", fmt.Errorf("read cluster list error body: %w", readErr)
		}
		return nil, "", &tidbCloudStatusError{operation: "cluster list", code: resp.StatusCode, upstreamBody: sanitizeUpstreamBody(raw)}
	}
	raw, readErr := readUpstreamBody(resp.Body, upstreamClusterBodyLimit)
	if readErr != nil {
		return nil, "", fmt.Errorf("read cluster list body: %w", readErr)
	}
	list, err := parseClusterList(raw)
	if err != nil {
		return nil, "", fmt.Errorf("parse cluster list: %w", err)
	}
	return list.Clusters, strings.TrimSpace(list.NextPageToken), nil
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

func (p *Provisioner) privateEndpointOverrideHost() string {
	switch strings.ToLower(p.cloudProvider) {
	case cloudProviderTencentCloud:
		return p.tencentPrivateEndpointHost
	case cloudProviderAliCloud:
		return p.alicloudPrivateEndpointHost
	case cloudProviderAWS:
		return ""
	default:
		return ""
	}
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
		fmt.Sprintf("GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO %s", quoteString(roleName)),
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

func parseBoolEnv(name string) (bool, error) {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	if v == "" {
		return false, nil
	}
	switch v {
	case "1", "true", "yes":
		return true, nil
	case "0", "false", "no":
		return false, nil
	}
	return false, fmt.Errorf("%s must be 1/true/yes or 0/false/no, got %q", name, os.Getenv(name))
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

type tidbCloudStatusError struct {
	operation    string
	code         int
	upstreamBody string
}

func (e *tidbCloudStatusError) Error() string {
	if e == nil {
		return ""
	}
	return statusError(e.operation, e.code, e.upstreamBody)
}

func isTiDBCloudStatus(err error, code int) bool {
	var statusErr *tidbCloudStatusError
	return errors.As(err, &statusErr) && statusErr.code == code
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
	if usePrivate, _ := parseBoolEnv(EnvTiDBCloudNativeUsePrivateEndpoint); usePrivate {
		cfg.TLSConfig = "skip-verify"
	}
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
		Private struct {
			Host string `json:"host"`
			Port int    `json:"port"`
		} `json:"private"`
	} `json:"endpoints"`
	UserPrefix string `json:"userPrefix"`
}

type branchInfo struct {
	BranchID  string `json:"branchId"`
	State     string `json:"state"`
	Endpoints struct {
		Public struct {
			Host string `json:"host"`
			Port int    `json:"port"`
		} `json:"public"`
		Private struct {
			Host string `json:"host"`
			Port int    `json:"port"`
		} `json:"private"`
	} `json:"endpoints"`
	UserPrefix string `json:"userPrefix"`
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

func compactNonEmptyStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func (p *Provisioner) clusterInfoFromResponse(tenantID, dbName, password string, info *clusterInfo) *tenant.ClusterInfo {
	if info == nil {
		return &tenant.ClusterInfo{
			TenantID: tenantID,
			Password: password,
			DBName:   dbName,
			Provider: tenant.ProviderTiDBCloudNative,
		}
	}
	var host string
	var port int
	if p.usePrivateEndpoint {
		overrideHost := p.privateEndpointOverrideHost()
		if overrideHost != "" {
			host = overrideHost
		} else {
			host = info.Endpoints.Private.Host
		}
		port = info.Endpoints.Private.Port
	} else {
		host = info.Endpoints.Public.Host
		port = info.Endpoints.Public.Port
	}
	out := &tenant.ClusterInfo{
		TenantID:       tenantID,
		ClusterID:      info.ClusterID,
		OrganizationID: strings.TrimSpace(info.Labels[TiDBCloudOrganizationLabel]),
		Host:           host,
		Port:           port,
		Password:       password,
		DBName:         dbName,
		Provider:       tenant.ProviderTiDBCloudNative,
	}
	if info.UserPrefix != "" {
		out.Username = info.UserPrefix + ".root"
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

func clusterConnectionIncomplete(info *clusterInfo, usePrivate bool, overridePrivateHost string) bool {
	if info == nil {
		return true
	}
	if usePrivate {
		if overridePrivateHost != "" {
			return info.Endpoints.Private.Port == 0 || info.UserPrefix == ""
		}
		return info.Endpoints.Private.Host == "" || info.Endpoints.Private.Port == 0 || info.UserPrefix == ""
	}
	return info.Endpoints.Public.Host == "" || info.Endpoints.Public.Port == 0 || info.UserPrefix == ""
}

func clusterProvisionMetadataIncomplete(info *clusterInfo, usePrivate bool, overridePrivateHost string) bool {
	if clusterConnectionIncomplete(info, usePrivate, overridePrivateHost) {
		return true
	}
	return strings.TrimSpace(info.Labels[TiDBCloudOrganizationLabel]) == ""
}

func branchConnectionIncomplete(info *branchInfo, usePrivate bool, overridePrivateHost string) bool {
	if info == nil {
		return true
	}
	if usePrivate {
		if overridePrivateHost != "" {
			return info.Endpoints.Private.Port == 0 || info.UserPrefix == ""
		}
		return info.Endpoints.Private.Host == "" || info.Endpoints.Private.Port == 0 || info.UserPrefix == ""
	}
	return info.Endpoints.Public.Host == "" || info.Endpoints.Public.Port == 0 || info.UserPrefix == ""
}

func (p *Provisioner) fillBranchEndpoint(out *tenant.ClusterInfo, branch *branchInfo) error {
	var host string
	var port int
	if p.usePrivateEndpoint {
		overrideHost := p.privateEndpointOverrideHost()
		if overrideHost != "" {
			host = overrideHost
		} else {
			host = branch.Endpoints.Private.Host
		}
		port = branch.Endpoints.Private.Port
	} else {
		host = branch.Endpoints.Public.Host
		port = branch.Endpoints.Public.Port
	}
	if branch.UserPrefix != "" {
		out.Username = branch.UserPrefix + ".root"
	}
	out.Host = host
	out.Port = port
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
			if resp.StatusCode != http.StatusTooManyRequests || time.Now().After(deadline) {
				return nil, fmt.Errorf("%s", statusError("cluster get", resp.StatusCode, sanitizeUpstreamBody(raw)))
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(tidbCloudNativePollInterval):
				continue
			}
		}
		info, err := parseClusterInfo(raw)
		if err != nil {
			return nil, err
		}
		if !clusterProvisionMetadataIncomplete(info, p.usePrivateEndpoint, p.privateEndpointOverrideHost()) {
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
		if info.State == stateActive && !branchConnectionIncomplete(info, p.usePrivateEndpoint, p.privateEndpointOverrideHost()) {
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
