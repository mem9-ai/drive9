// Package local is the test-only tenant.Provisioner for DRIVE9_TENANT_PROVIDER=local.
//
// It replaces drive9-server-local: each POST /v1/provision creates a database
// on an existing TiDB instance, inits the tenant schema, and returns
// coordinates for the production pool.Acquire path.
package local

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"unicode"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/mem9-ai/drive9/pkg/tenant"
	"github.com/mem9-ai/drive9/pkg/tenant/schema"
)

var (
	_ tenant.Provisioner   = (*Provisioner)(nil)
	_ tenant.Deprovisioner = (*Provisioner)(nil)
)

const (
	// EnvAdminDSN is the privileged DSN used to CREATE/DROP tenant databases.
	EnvAdminDSN = "DRIVE9_LOCAL_MYSQL_DSN"
	// EnvLegacyDSN is accepted as a fallback admin DSN (former drive9-server-local).
	EnvLegacyDSN     = "DRIVE9_LOCAL_DSN"
	EnvEmbeddingMode = "DRIVE9_LOCAL_EMBEDDING_MODE"

	DefaultOrganizationID     = "local-org"
	DefaultTokenSigningKeyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	DefaultMasterKeyHex       = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"

	dbNamePrefix = "drive9_"
	maxDBNameLen = 64
	embeddingApp = "app"
)

// Provisioner implements tenant.Provisioner for a single existing TiDB
// instance. It is not a ClustersAPI backend and not a production provider.
type Provisioner struct {
	admin *mysql.Config
	mode  string
	orgID string
	role  string
}

func NewProvisionerFromEnv() (*Provisioner, error) {
	dsn := firstNonEmpty(
		strings.TrimSpace(os.Getenv(EnvAdminDSN)),
		strings.TrimSpace(os.Getenv(EnvLegacyDSN)),
		strings.TrimSpace(os.Getenv("DRIVE9_META_DSN")),
	)
	if dsn == "" {
		return nil, fmt.Errorf("%s, %s, or DRIVE9_META_DSN is required for provider %s", EnvAdminDSN, EnvLegacyDSN, tenant.ProviderLocal)
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse local mysql DSN: %w", err)
	}
	cfg.ParseTime = true
	mode, err := embeddingModeFromEnv()
	if err != nil {
		return nil, err
	}
	return &Provisioner{
		admin: cfg,
		mode:  mode,
		orgID: DefaultOrganizationID,
		role:  tenant.TiDBCloudRoleOrgOwner,
	}, nil
}

func (p *Provisioner) ProviderType() string { return tenant.ProviderLocal }

func (p *Provisioner) Provision(ctx context.Context, tenantID string) (*tenant.ClusterInfo, error) {
	if p == nil || p.admin == nil {
		return nil, fmt.Errorf("local provisioner is not configured")
	}
	dbName, err := databaseName(tenantID)
	if err != nil {
		return nil, err
	}
	admin, err := p.openAdmin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = admin.Close() }()
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(dbName)); err != nil {
		return nil, fmt.Errorf("create local tenant database %s: %w", dbName, err)
	}
	host, port := splitAddr(p.admin.Addr)
	return &tenant.ClusterInfo{
		TenantID:       tenantID,
		ClusterID:      dbName,
		OrganizationID: p.orgID,
		BranchID:       "main",
		Host:           host,
		Port:           port,
		Username:       p.admin.User,
		Password:       p.admin.Passwd,
		DBName:         dbName,
		Provider:       tenant.ProviderLocal,
	}, nil
}

func (p *Provisioner) InitSchema(ctx context.Context, dsn string) error {
	if p == nil {
		return fmt.Errorf("local provisioner is not configured")
	}
	if p.mode != "" && p.mode != embeddingApp {
		return fmt.Errorf("unsupported local embedding mode %q (want app)", p.mode)
	}
	return schema.InitTiDBTenantSchemaForModeWithOptionsContext(ctx, dsn, schema.TiDBEmbeddingModeApp, schema.InitTiDBTenantSchemaOptions{
		AllowUnsupportedOptionalIndexes: true,
	})
}

func (p *Provisioner) Deprovision(ctx context.Context, cluster *tenant.ClusterInfo) error {
	if p == nil || p.admin == nil {
		return fmt.Errorf("local provisioner is not configured")
	}
	if cluster == nil {
		return fmt.Errorf("nil cluster")
	}
	dbName := strings.TrimSpace(cluster.DBName)
	if dbName == "" {
		dbName = strings.TrimSpace(cluster.ClusterID)
	}
	if !validTenantDatabaseName(dbName) {
		return fmt.Errorf("refusing to drop database %q", dbName)
	}
	admin, err := p.openAdmin()
	if err != nil {
		return err
	}
	defer func() { _ = admin.Close() }()
	if _, err := admin.ExecContext(ctx, "DROP DATABASE IF EXISTS "+quoteIdent(dbName)); err != nil {
		return fmt.Errorf("drop local tenant database %s: %w", dbName, err)
	}
	return nil
}

func (p *Provisioner) ResolveAPIKeyIdentity(_ context.Context, req tenant.CredentialProvisionRequest) (*tenant.TiDBCloudAPIKeyIdentity, error) {
	if strings.TrimSpace(req.PublicKey) == "" || strings.TrimSpace(req.PrivateKey) == "" {
		return nil, tenant.ErrCredentialsRequired
	}
	org := p.orgID
	if org == "" {
		org = DefaultOrganizationID
	}
	role := p.role
	if role == "" {
		role = tenant.TiDBCloudRoleOrgOwner
	}
	return &tenant.TiDBCloudAPIKeyIdentity{OrganizationID: org, Role: role}, nil
}

func (p *Provisioner) openAdmin() (*sql.DB, error) {
	cfg := p.admin.Clone()
	cfg.DBName = ""
	cfg.ParseTime = true
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("open local mysql admin: %w", err)
	}
	return db, nil
}

func embeddingModeFromEnv() (string, error) {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv(EnvEmbeddingMode)))
	switch raw {
	case "", "app":
		return embeddingApp, nil
	default:
		return "", fmt.Errorf("invalid %s %q (want app)", EnvEmbeddingMode, raw)
	}
}

func databaseName(tenantID string) (string, error) {
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(tenantID)) {
		if r == '-' {
			continue
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			b.WriteRune(r)
		}
	}
	cleaned := b.String()
	if cleaned == "" {
		return "", fmt.Errorf("tenant id %q produces empty database name", tenantID)
	}
	name := dbNamePrefix + cleaned
	if len(name) > maxDBNameLen {
		name = name[:maxDBNameLen]
	}
	if !validTenantDatabaseName(name) {
		return "", fmt.Errorf("invalid local database name %q", name)
	}
	return name, nil
}

func validTenantDatabaseName(name string) bool {
	if !strings.HasPrefix(name, dbNamePrefix) || len(name) > maxDBNameLen {
		return false
	}
	for _, r := range name[len(dbNamePrefix):] {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return false
		}
	}
	return name != dbNamePrefix
}

func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func splitAddr(addr string) (string, int) {
	host, port := "127.0.0.1", 3306
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return host, port
	}
	h, p, err := net.SplitHostPort(addr)
	if err != nil {
		if strings.Contains(addr, "/") {
			return host, port
		}
		if !strings.Contains(addr, ":") {
			return addr, port
		}
		return host, port
	}
	if h != "" {
		host = h
	}
	if n, convErr := strconv.Atoi(p); convErr == nil && n > 0 {
		port = n
	}
	return host, port
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
