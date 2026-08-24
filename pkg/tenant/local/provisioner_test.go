package local

import (
	"context"
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
	if info.Provider != tenant.ProviderLocal || info.OrganizationID != DefaultOrganizationID {
		t.Fatalf("cluster = %+v", info)
	}
	if !strings.HasPrefix(info.DBName, dbNamePrefix) {
		t.Fatalf("DBName = %q", info.DBName)
	}
	tenantDSN := info.Username + ":" + info.Password + "@tcp(" + info.Host + ":" + strconv.Itoa(info.Port) + ")/" + info.DBName + "?parseTime=true"
	if err := p.InitSchema(ctx, tenantDSN); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	if err := p.Deprovision(ctx, info); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
}
