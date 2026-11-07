package local

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"testing"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/mem9-ai/drive9/internal/testtidb"
	"github.com/mem9-ai/drive9/pkg/tenant"
)

func TestDatabaseName(t *testing.T) {
	got, err := databaseName("AbC-12_def")
	if err != nil {
		t.Fatalf("databaseName: %v", err)
	}
	if got != "drive9_abc12_def" {
		t.Fatalf("databaseName = %q, want drive9_abc12_def", got)
	}
	if _, err := databaseName("???"); err == nil {
		t.Fatal("expected error for non-alnum tenant id")
	}
	if !validTenantDatabaseName(got) {
		t.Fatalf("validTenantDatabaseName(%q) = false", got)
	}
	if validTenantDatabaseName("mysql") || validTenantDatabaseName("drive9_") {
		t.Fatal("validTenantDatabaseName accepted a non-tenant name")
	}
}

func TestSplitAddr(t *testing.T) {
	host, port := splitAddr("10.0.0.8:4000")
	if host != "10.0.0.8" || port != 4000 {
		t.Fatalf("splitAddr = %s:%d, want 10.0.0.8:4000", host, port)
	}
	host, port = splitAddr("")
	if host != "127.0.0.1" || port != 3306 {
		t.Fatalf("splitAddr empty = %s:%d", host, port)
	}
	host, port = splitAddr("[::1]:4000")
	if host != "::1" || port != 4000 {
		t.Fatalf("splitAddr ipv6 = %s:%d, want [::1]:4000", host, port)
	}
}

func TestDeprovisionRefusesNonPrefixedName(t *testing.T) {
	cfg, err := mysql.ParseDSN("root@tcp(127.0.0.1:3306)/")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	p := &Provisioner{admin: cfg}
	if err := p.Deprovision(context.Background(), &tenant.ClusterInfo{DBName: "mysql"}); err == nil {
		t.Fatal("expected refuse to drop non-tenant database")
	}
}

func TestResolveAPIKeyIdentity(t *testing.T) {
	p := &Provisioner{orgID: DefaultOrganizationID, role: tenant.TiDBCloudRoleOrgOwner}
	if _, err := p.ResolveAPIKeyIdentity(context.Background(), tenant.CredentialProvisionRequest{}); err == nil {
		t.Fatal("expected credentials required")
	}
	id, err := p.ResolveAPIKeyIdentity(context.Background(), tenant.CredentialProvisionRequest{
		PublicKey:  "pub",
		PrivateKey: "priv",
	})
	if err != nil {
		t.Fatalf("ResolveAPIKeyIdentity: %v", err)
	}
	if id.OrganizationID != DefaultOrganizationID || id.Role != tenant.TiDBCloudRoleOrgOwner {
		t.Fatalf("identity = %+v", id)
	}
}

func TestInitSchemaApp(t *testing.T) {
	if testDSN == "" {
		t.Skip("no tidb test DSN")
	}
	p := &Provisioner{mode: embeddingApp}
	if err := p.InitSchema(context.Background(), testDSN); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	db := testtidb.OpenDB(t, testDSN)
	testtidb.ResetDB(t, db)
}

func TestEmbeddingModeFromEnv(t *testing.T) {
	t.Setenv(EnvEmbeddingMode, "")
	got, err := embeddingModeFromEnv()
	if err != nil {
		t.Fatalf("empty mode: %v", err)
	}
	if got != embeddingApp {
		t.Fatalf("empty mode = %q, want %q", got, embeddingApp)
	}
	t.Setenv(EnvEmbeddingMode, "none")
	if _, err := embeddingModeFromEnv(); err == nil {
		t.Fatal("expected none embedding mode to be rejected")
	}
	t.Setenv(EnvEmbeddingMode, "auto")
	if _, err := embeddingModeFromEnv(); err == nil {
		t.Fatal("expected auto embedding mode to be rejected")
	}
}

func TestProvisionAndDeprovision(t *testing.T) {
	if testDSN == "" {
		t.Skip("no tidb test DSN")
	}
	t.Setenv(EnvAdminDSN, testDSN)
	t.Setenv(EnvTenantPasswordSecret, strings.Repeat("test-secret-", 4))
	p, err := NewProvisionerFromEnv()
	if err != nil {
		t.Fatalf("NewProvisionerFromEnv: %v", err)
	}
	ctx := context.Background()
	info, err := p.Provision(ctx, "11111111-2222-3333-4444-555555555555")
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "denied") || strings.Contains(strings.ToLower(err.Error()), "privilege") {
			t.Skipf("test tidb user cannot CREATE DATABASE: %v", err)
		}
		t.Fatalf("Provision: %v", err)
	}
	t.Cleanup(func() { _ = p.Deprovision(context.Background(), info) })
	if info.Provider != tenant.ProviderLocal || info.OrganizationID != DefaultOrganizationID {
		t.Fatalf("cluster = %+v", info)
	}
	if !strings.HasPrefix(info.DBName, dbNamePrefix) {
		t.Fatalf("DBName = %q", info.DBName)
	}
	// Local tenants share one TiDB instance, so the tenant must not run as the
	// admin account: its grant is scoped to its own database.
	if info.Username == p.admin.User || info.Password == p.admin.Passwd {
		t.Fatalf("tenant reuses the admin credential (%q)", info.Username)
	}
	// Assert the derived name for this tenant, not a prefix of the empty string
	// (which is always true).
	wantUser := localTenantUserName("11111111-2222-3333-4444-555555555555")
	if info.Username != wantUser {
		t.Fatalf("tenant account = %q, want %q", info.Username, wantUser)
	}
	if len(info.Username) > 32 {
		t.Fatalf("tenant account %q exceeds the MySQL 32-character limit", info.Username)
	}
	// The password is derived from the deployment secret (never the empty password
	// of the default local DSN) so re-provisioning converges, and the grant scope
	// escapes SQL wildcards so a sibling database cannot match it.
	if info.Password != deriveLocalTenantPassword(p.tenantSecret, "11111111-2222-3333-4444-555555555555") {
		t.Fatal("tenant password is not the derived value")
	}
	if len(info.Password) != 32 {
		t.Fatalf("tenant password length = %d, want a 32-character derived value", len(info.Password))
	}
	// Re-provisioning must not rotate a credential the caller has already stored.
	again, err := p.Provision(ctx, "11111111-2222-3333-4444-555555555555")
	if err != nil {
		t.Fatalf("re-provision: %v", err)
	}
	if again.Username != info.Username || again.Password != info.Password {
		t.Fatalf("re-provision rotated the credential: %q -> %q", info.Username, again.Username)
	}
	// The first credential still authenticates after the second Provision.
	firstDSN := info.Username + ":" + info.Password + "@tcp(" + info.Host + ":" + strconv.Itoa(info.Port) + ")/" + info.DBName + "?parseTime=true"
	checkCredential := func(t *testing.T) {
		t.Helper()
		// A fresh pool: reusing one could answer from an already-authenticated
		// connection instead of authenticating again.
		db, err := sql.Open("mysql", firstDSN)
		if err != nil {
			t.Fatalf("open first dsn: %v", err)
		}
		defer func() { _ = db.Close() }()
		if _, err := db.ExecContext(ctx, "SELECT 1"); err != nil {
			t.Fatalf("credential from the first provision no longer works: %v", err)
		}
	}
	checkCredential(t)
	// A pre-existing account with an unknown password (created outside drive9, or
	// under a different secret with a restricted admin whose USER_PRIVILEGES view is
	// filtered) must not produce a "successful" credential that cannot authenticate:
	// the creation branch is idempotent, so the only way to honour the contract is to
	// authenticate before returning.
	foreign := localTenantUserName("55555555-6666-7777-8888-999999999999")
	adminDB, err := p.openAdmin()
	if err != nil {
		t.Fatalf("open admin: %v", err)
	}
	defer func() { _ = adminDB.Close() }()
	if _, err := adminDB.ExecContext(ctx, "CREATE USER IF NOT EXISTS "+quoteString(foreign)+"@'%' IDENTIFIED BY 'some-other-password'"); err != nil {
		t.Fatalf("create foreign account: %v", err)
	}
	t.Cleanup(func() {
		cleanup, err := p.openAdmin()
		if err != nil {
			return
		}
		defer func() { _ = cleanup.Close() }()
		_, _ = cleanup.ExecContext(context.Background(), "DROP USER IF EXISTS "+quoteString(foreign)+"@'%'")
	})
	if _, err := p.Provision(ctx, "55555555-6666-7777-8888-999999999999"); err == nil {
		t.Fatal("provisioning returned success for an account whose password it does not know")
	}

	// A changed deployment secret must fail closed rather than silently rotate the
	// account out from under the credential the caller already persisted.
	rotated := *p
	rotated.tenantSecret = "a-different-secret"
	if _, err := rotated.Provision(ctx, "11111111-2222-3333-4444-555555555555"); err == nil {
		t.Fatal("a changed tenant secret rotated the existing account instead of failing closed")
	}
	checkCredential(t)
	if scope := localTenantGrantScope("drive9_abc_def"); strings.Contains(scope, "drive9_abc_def") {
		t.Fatalf("grant scope %q does not escape the wildcard", scope)
	}
	tenantDSN := info.Username + ":" + info.Password + "@tcp(" + info.Host + ":" + strconv.Itoa(info.Port) + ")/" + info.DBName + "?parseTime=true"
	if err := p.InitSchema(ctx, tenantDSN); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	// The tenant account can reach its own schema...
	tenantDB, err := sql.Open("mysql", tenantDSN)
	if err != nil {
		t.Fatalf("open tenant dsn: %v", err)
	}
	defer func() { _ = tenantDB.Close() }()
	if _, err := tenantDB.ExecContext(ctx, "SELECT 1"); err != nil {
		t.Fatalf("tenant cannot query its own schema: %v", err)
	}
	// ...but not another tenant's database. The other tenant is real, so the probe
	// cannot pass on "table does not exist".
	other, otherErr := p.Provision(ctx, "99999999-8888-7777-6666-555555555555")
	if otherErr != nil {
		t.Fatalf("provision second tenant: %v", otherErr)
	}
	t.Cleanup(func() { _ = p.Deprovision(context.Background(), other) })
	if other.DBName == info.DBName {
		t.Fatalf("both tenants resolved to %q", info.DBName)
	}
	if _, err := tenantDB.ExecContext(ctx, "SELECT COUNT(*) FROM "+quoteIdent(other.DBName)+".file_nodes"); err == nil {
		t.Fatalf("tenant account was allowed to read %s", other.DBName)
	}
	// information_schema is privilege-filtered rather than denied: the tenant must
	// not see any schema but its own.
	var visible int
	if err := tenantDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name LIKE 'drive9\_%' AND schema_name <> ?`,
		info.DBName).Scan(&visible); err != nil {
		t.Fatalf("enumerate schemata: %v", err)
	}
	if visible != 0 {
		t.Fatalf("tenant account sees %d other drive9 schemas", visible)
	}
	// Interruption between account creation and the caller persisting the credential:
	// the database and account exist, the metadata store does not know them yet, and
	// a retry must hand back a credential that works.
	interrupted, err := p.Provision(ctx, "77777777-1111-2222-3333-444444444444")
	if err != nil {
		t.Fatalf("provision interrupted tenant: %v", err)
	}
	retried, err := p.Provision(ctx, "77777777-1111-2222-3333-444444444444")
	if err != nil {
		t.Fatalf("retry after interruption: %v", err)
	}
	if retried.Username != interrupted.Username || retried.Password != interrupted.Password {
		t.Fatalf("retry returned a different credential: %q -> %q", interrupted.Username, retried.Username)
	}
	retryDB, err := sql.Open("mysql", retried.Username+":"+retried.Password+"@tcp("+retried.Host+":"+strconv.Itoa(retried.Port)+")/"+retried.DBName+"?parseTime=true")
	if err != nil {
		t.Fatalf("open retried dsn: %v", err)
	}
	defer func() { _ = retryDB.Close() }()
	if _, err := retryDB.ExecContext(ctx, "SELECT 1"); err != nil {
		t.Fatalf("credential from the retry does not work: %v", err)
	}
	if err := p.Deprovision(ctx, retried); err != nil {
		t.Fatalf("Deprovision(interrupted): %v", err)
	}

	if err := p.Deprovision(ctx, info); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
}
