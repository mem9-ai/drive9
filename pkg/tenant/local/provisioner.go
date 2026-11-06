// Package local is the test-only tenant.Provisioner for DRIVE9_TENANT_PROVIDER=local.
//
// It replaces drive9-server-local: each POST /v1/provision creates a database
// on an existing TiDB instance, inits the tenant schema, and returns
// coordinates for the production pool.Acquire path.
package local

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
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
	// EnvTenantPasswordSecret seeds the per-tenant database passwords. It falls back
	// to DRIVE9_MASTER_KEY, which the local provider always sets, so the default
	// local DSN's missing password never becomes the derivation key.
	EnvTenantPasswordSecret = "DRIVE9_LOCAL_TENANT_SECRET"

	DefaultOrganizationID = "local-org"

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
	// tenantSecret seeds the per-tenant database passwords. Deriving them keeps
	// Provision idempotent: re-provisioning must not rotate a credential the
	// metadata store already holds, and the value must not be computable from the
	// tenant id alone.
	tenantSecret string
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
	// The derivation key must be a stable, high-entropy secret. The admin DSN's
	// password is not: it can be empty (the default local DSN) or weak, and anyone
	// holding one tenant's derived password could use it to guess the rest.
	secret := firstNonEmpty(
		strings.TrimSpace(os.Getenv(EnvTenantPasswordSecret)),
		strings.TrimSpace(os.Getenv("DRIVE9_MASTER_KEY")),
	)
	if secret == "" {
		return nil, fmt.Errorf("%s (or DRIVE9_MASTER_KEY) is required to derive local tenant database passwords", EnvTenantPasswordSecret)
	}
	return &Provisioner{
		admin:        cfg,
		mode:         mode,
		orgID:        DefaultOrganizationID,
		role:         tenant.TiDBCloudRoleOrgOwner,
		tenantSecret: secret,
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
	// Every local tenant shares one TiDB instance, so lending each tenant the admin
	// account would give it every other tenant's database (and the control plane,
	// which lives on the same instance). The tenant gets its own account, granted
	// only on its own schema.
	credential, err := ensureLocalTenantAccount(ctx, admin, p.admin, p.tenantSecret, tenantID, dbName)
	if err != nil {
		return nil, err
	}
	host, port := splitAddr(p.admin.Addr)
	return &tenant.ClusterInfo{
		TenantID:       tenantID,
		ClusterID:      dbName,
		OrganizationID: p.orgID,
		BranchID:       "main",
		Host:           host,
		Port:           port,
		Username:       credential.username,
		Password:       credential.password,
		DBName:         dbName,
		Provider:       tenant.ProviderLocal,
	}, nil
}

// localTenantGrant is the privilege set a tenant needs: schema DDL during init
// and DML afterwards, on its own database only. SYSTEM_VARIABLES_ADMIN is
// deliberately absent — no local tenant writes server variables.
const localTenantGrant = "CREATE, ALTER, DROP, INDEX, SELECT, INSERT, UPDATE, DELETE"

type localTenantCredential struct {
	username string
	password string
}

// ensureLocalTenantAccount creates or converges the MySQL account for one local
// tenant and grants it access to that tenant's schema only.
//
// The password is derived deterministically from the admin secret and the tenant
// id, so re-provisioning an existing tenant converges instead of rotating a
// credential the metadata store already holds. The account name is derived the
// same way, which keeps it stable across restarts.
func ensureLocalTenantAccount(ctx context.Context, admin *sql.DB, adminCfg *mysql.Config, tenantSecret, tenantID, dbName string) (localTenantCredential, error) {
	username := localTenantUserName(tenantID)
	if username == "" {
		return localTenantCredential{}, fmt.Errorf("derive local tenant credential for %s", tenantID)
	}
	// Deterministic in (deployment secret, tenant id): re-provisioning an existing
	// tenant must not rotate a credential the metadata store already holds, and the
	// derivation key is a real secret (never the default local DSN's empty password),
	// so the password is not computable from the tenant id alone.
	password := deriveLocalTenantPassword(tenantSecret, tenantID)
	if password == "" {
		return localTenantCredential{}, fmt.Errorf("derive local tenant password for %s", tenantID)
	}
	user := quoteString(username) + "@'%'"
	exists, err := localTenantAccountExists(ctx, admin, username)
	if err != nil {
		return localTenantCredential{}, err
	}
	if exists {
		// Re-provisioning must converge, never rotate: the caller persists the
		// returned credential only after Provision succeeds, so setting a new
		// password here could leave the account and the metadata store disagreeing.
		// If the derived password no longer works, the deployment secret changed
		// under an existing account, which needs a deliberate rotation, not a silent
		// one.
		ok, verifyErr := localTenantAccountAuthenticates(ctx, adminCfg, username, password)
		if verifyErr != nil {
			return localTenantCredential{}, verifyErr
		}
		if !ok {
			return localTenantCredential{}, fmt.Errorf(
				"local tenant %s already has an account whose password was derived from a different %s; restore that secret (or set a new %s and re-provision after dropping the account), refusing to rotate it silently",
				tenantID, EnvTenantPasswordSecret, EnvTenantPasswordSecret)
		}
		if _, err := admin.ExecContext(ctx, "GRANT "+localTenantGrant+" ON "+localTenantGrantScope(dbName)+" TO "+user); err != nil {
			return localTenantCredential{}, fmt.Errorf("grant schema access for local tenant %s: %w", tenantID, err)
		}
		return localTenantCredential{username: username, password: password}, nil
	}
	statements := []struct {
		what string
		sql  string
	}{
		{"create account", "CREATE USER IF NOT EXISTS " + user + " IDENTIFIED BY " + quoteString(password)},
		{"grant schema access", "GRANT " + localTenantGrant + " ON " + localTenantGrantScope(dbName) + " TO " + user},
	}
	for _, stmt := range statements {
		if _, err := admin.ExecContext(ctx, stmt.sql); err != nil {
			return localTenantCredential{}, fmt.Errorf("%s for local tenant %s: %w", stmt.what, tenantID, err)
		}
	}
	return localTenantCredential{username: username, password: password}, nil
}

// localTenantAccountExists reports whether the tenant's MySQL account exists.
func localTenantAccountExists(ctx context.Context, admin *sql.DB, username string) (bool, error) {
	var count int
	err := admin.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.user_privileges WHERE grantee = ?",
		"'"+username+"'@'%'").Scan(&count)
	if err != nil {
		return false, fmt.Errorf("check local tenant account %s: %w", username, err)
	}
	return count > 0, nil
}

// localTenantAccountAuthenticates reports whether the derived password is the one
// the existing account actually uses.
//
// The probe copies the admin connection's network, address and TLS settings — a
// Unix socket or a TLS-required server would otherwise fail the probe even with the
// right password and be misreported as a changed secret. Only an authentication
// failure means "different password"; any other error is returned so an operator can
// see what actually went wrong.
func localTenantAccountAuthenticates(ctx context.Context, admin *mysql.Config, username, password string) (bool, error) {
	if admin == nil {
		return false, fmt.Errorf("local admin config is unknown")
	}
	cfg := admin.Clone()
	cfg.User = username
	cfg.Passwd = password
	// No default database: the tenant account has no privileges on the admin's, and
	// selecting it would fail with access-denied for the *database*, which is not a
	// password mismatch.
	cfg.DBName = ""
	probe, openErr := sql.Open("mysql", cfg.FormatDSN())
	if openErr != nil {
		return false, fmt.Errorf("probe local tenant %s: %w", username, openErr)
	}
	defer func() { _ = probe.Close() }()
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if pingErr := probe.PingContext(pingCtx); pingErr != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(pingErr, &mysqlErr) && (mysqlErr.Number == 1045 || mysqlErr.Number == 1044) {
			// Access denied: the derived password is not the account's.
			return false, nil
		}
		return false, fmt.Errorf("probe local tenant %s: %w", username, pingErr)
	}
	return true, nil
}

// localTenantUserName is the MySQL account name for a local tenant. MySQL caps
// account names at 32 characters, and a tenant id can be longer, so the derived
// name is a fixed prefix plus a digest.
func localTenantUserName(tenantID string) string {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return ""
	}
	// 32 characters is the MySQL account-name limit: a 10-character prefix leaves
	// room for 22 hex characters (11 bytes of digest).
	sum := sha256.Sum256([]byte("drive9:local_tenant:" + tenantID))
	return "drive9_t_" + hex.EncodeToString(sum[:11])
}

// deriveLocalTenantPassword renders one tenant's password from the deployment
// secret.
//
// A random password would be stronger, but re-provisioning has to leave an
// existing account's credential alone, and MySQL never exposes a stored password,
// so the value has to be reproducible. The key is the operator secret (or the
// generated master key), never the empty password of the default local DSN.
func deriveLocalTenantPassword(tenantSecret, tenantID string) string {
	if strings.TrimSpace(tenantSecret) == "" || strings.TrimSpace(tenantID) == "" {
		return ""
	}
	mac := hmac.New(sha256.New, []byte(tenantSecret))
	_, _ = mac.Write([]byte("drive9:local_tenant_pw:" + tenantID))
	return hex.EncodeToString(mac.Sum(nil))[:32]
}

// localTenantGrantScope renders the database scope of a grant.
//
// MySQL and TiDB treat `_` and `%` in a database-level GRANT scope as wildcards
// even inside backticks, so `drive9_abc_def` would also grant `drive9_abcXdef`.
// They are escaped to literals here.
func localTenantGrantScope(dbName string) string {
	escaped := strings.NewReplacer(
		"\\", "\\\\",
		"_", "\\_",
		"%", "\\%",
		"`", "``",
	).Replace(dbName)
	return "`" + escaped + "`.*"
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
	if username := localTenantUserName(cluster.TenantID); username != "" {
		if _, err := admin.ExecContext(ctx, "DROP USER IF EXISTS "+quoteString(username)+"@'%'"); err != nil {
			return fmt.Errorf("drop local tenant account %s: %w", username, err)
		}
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

// quoteString renders a MySQL string literal. Account names and passwords cannot
// be bound parameters in CREATE/ALTER USER, so they are escaped here; the values
// are derived hex digests, and this keeps the quoting correct anyway.
func quoteString(value string) string {
	escaped := strings.NewReplacer(
		"\\", "\\\\",
		"'", "\\'",
		"\x00", "\\0",
	).Replace(value)
	return "'" + escaped + "'"
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
