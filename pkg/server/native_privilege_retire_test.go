package server

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
)

// A failing tenant is retried with backoff rather than on every pass, and the delay
// is capped so it keeps being retried while this pod leads.
func TestNativePrivilegeRetireRetryBackoff(t *testing.T) {
	s := &Server{}
	if s.nativePrivilegeRetireBackoff("tenant-a") {
		t.Fatal("a tenant without a failure is in backoff")
	}
	s.recordNativePrivilegeRetireFailure("tenant-a")
	if !s.nativePrivilegeRetireBackoff("tenant-a") {
		t.Fatal("no backoff after a failure")
	}
	for i := 0; i < 10; i++ {
		s.recordNativePrivilegeRetireFailure("tenant-a")
	}
	s.nativePrivilegeRetireMu.Lock()
	attempts := s.nativePrivilegeRetireFailures["tenant-a"]
	retryAt := s.nativePrivilegeRetireRetryAt["tenant-a"]
	s.nativePrivilegeRetireMu.Unlock()
	if attempts != 11 {
		t.Fatalf("attempt count = %d, want 11", attempts)
	}
	if delay := time.Until(retryAt); delay > nativePrivilegeRetireMaxBackoff+time.Minute {
		t.Fatalf("retry delay %s exceeds the cap %s", delay, nativePrivilegeRetireMaxBackoff)
	}
	// Another tenant is unaffected.
	if s.nativePrivilegeRetireBackoff("tenant-b") {
		t.Fatal("a failure leaked to another tenant")
	}
	s.clearNativePrivilegeRetireFailure("tenant-a")
	if s.nativePrivilegeRetireBackoff("tenant-a") {
		t.Fatal("retry state survived a success")
	}
}

// The scan visits every page, decides from each stored record, and records a retry for
// a tenant whose database cannot be reached while leaving the others alone.
func TestNativePrivilegeRetireScanPagesAndClassifies(t *testing.T) {
	rt := newObjectGCTestRuntime(t)
	ctx := context.Background()
	// One tenant per page forces the cursor path.
	oldBatch := nativePrivilegeRetireBatchSize
	nativePrivilegeRetireBatchSize = 1
	t.Cleanup(func() { nativePrivilegeRetireBatchSize = oldBatch })

	// A native tenant whose restricted account cannot be reached: the remediation
	// fails and schedules a retry.
	rt.insertPrivilegeRetireTenant(t, "native-unreachable", tenant.ProviderTiDBCloudNative, "t1.tdc_fs_sys", "127.0.0.1", 1)
	// A non-native tenant and a native tenant still naming the root principal are
	// both skipped without a retry entry.
	rt.insertPrivilegeRetireTenant(t, "tidb-zero", tenant.ProviderTiDBZero, "t2.root", "127.0.0.1", 1)
	rt.insertPrivilegeRetireTenant(t, "native-root", tenant.ProviderTiDBCloudNative, "t3.root", "127.0.0.1", 1)

	srv := &Server{meta: rt.meta, pool: rt.pool}
	srv.retireNativeTenantVariablesPrivilege(ctx)

	if !srv.nativePrivilegeRetireBackoff("native-unreachable") {
		t.Fatal("an unreachable native tenant did not schedule a retry")
	}
	if srv.nativePrivilegeRetireRecentlyDone("native-unreachable") {
		t.Fatal("a failed retire was memoized as done")
	}
	for _, id := range []string{"tidb-zero", "native-root"} {
		if srv.nativePrivilegeRetireBackoff(id) {
			t.Fatalf("skipped tenant %s scheduled a retry", id)
		}
	}
}

// A native tenant whose legacy grant can actually be revoked loses it, and keeps its
// own-schema privileges. This uses the real cluster root principal (the test DSN) as
// the account the stored credential authenticates as.
func TestNativePrivilegeRetireRevokesLegacyGrant(t *testing.T) {
	rt := newObjectGCTestRuntime(t)
	if rt.dbUser != "root" {
		t.Skip("needs the test TiDB root account to create the tenant principals")
	}
	ctx := context.Background()
	admin, err := sql.Open("mysql", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	// Close last: the cleanup below needs a live handle, and a bare defer would run
	// before t.Cleanup and leave the fixture's privileged principal behind.
	t.Cleanup(func() { _ = admin.Close() })

	const role = "tdc_fs_admin"
	const rootUser = "ti-retire.root"
	const password = "drive9-retire-test"
	// The stored credential has to authenticate as <prefix>.root, and that principal
	// needs the privilege WITH GRANT OPTION to revoke it from the role.
	setup := []string{
		"DROP USER IF EXISTS '" + rootUser + "'@'%'",
		"CREATE USER '" + rootUser + "'@'%' IDENTIFIED BY '" + password + "'",
		"GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO '" + rootUser + "'@'%' WITH GRANT OPTION",
		// The remediation DSN names the tenant database, so the root principal needs
		// access to it as a real cluster root would.
		"GRANT ALL ON `" + rt.dbName + "`.* TO '" + rootUser + "'@'%'",
		"CREATE ROLE IF NOT EXISTS '" + role + "'",
		"GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO '" + role + "'",
		"GRANT SELECT ON `" + rt.dbName + "`.* TO '" + role + "'",
	}
	for _, stmt := range setup {
		if _, err := admin.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	t.Cleanup(func() {
		ctx := context.Background()
		if _, err := admin.ExecContext(ctx, "REVOKE SYSTEM_VARIABLES_ADMIN ON *.* FROM '"+role+"'"); err != nil && !missingRoleGrant(err) {
			t.Errorf("cleanup: revoke role grant: %v", err)
		}
		if _, err := admin.ExecContext(ctx, "DROP ROLE IF EXISTS '"+role+"'"); err != nil {
			t.Errorf("cleanup: drop role: %v", err)
		}
		if _, err := admin.ExecContext(ctx, "DROP USER IF EXISTS '"+rootUser+"'@'%'"); err != nil {
			t.Errorf("cleanup: drop privileged principal: %v", err)
		}
	})

	rt.insertPrivilegeRetireTenantWithPassword(t, "native-revocable", tenant.ProviderTiDBCloudNative, "ti-retire.tdc_fs_sys", password)

	srv := &Server{meta: rt.meta, pool: rt.pool}
	srv.retireNativeTenantVariablesPrivilege(ctx)

	if srv.nativePrivilegeRetireBackoff("native-revocable") {
		t.Fatal("the revoke failed and scheduled a retry")
	}
	if !srv.nativePrivilegeRetireRecentlyDone("native-revocable") {
		t.Fatal("a successful retire was not memoized; every pass would dial it again")
	}
	grants := roleGrants(t, admin, role)
	if containsGrant(grants, "SYSTEM_VARIABLES_ADMIN") {
		t.Fatalf("legacy cluster-wide grant survived: %v", grants)
	}
	if !containsGrant(grants, "SELECT") {
		t.Fatalf("own-schema grant was lost: %v", grants)
	}
}

func roleGrants(t *testing.T, db *sql.DB, role string) []string {
	t.Helper()
	rows, err := db.Query("SHOW GRANTS FOR '" + role + "'")
	if err != nil {
		t.Fatalf("show grants: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan grant: %v", err)
		}
		out = append(out, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("grant rows: %v", err)
	}
	return out
}

func containsGrant(grants []string, want string) bool {
	for _, g := range grants {
		if strings.Contains(g, want) {
			return true
		}
	}
	return false
}

// insertPrivilegeRetireTenant records an active tenant whose stored credential is
// encrypted with the test pool, so the pass can decrypt it like a real row.
func (rt *objectGCTestRuntime) insertPrivilegeRetireTenant(t *testing.T, id string, provider, dbUser, host string, port int) {
	t.Helper()
	rt.insertPrivilegeRetireTenantAt(t, id, provider, dbUser, rt.dbPass, host, port)
}

func (rt *objectGCTestRuntime) insertPrivilegeRetireTenantWithPassword(t *testing.T, id string, provider, dbUser, password string) {
	t.Helper()
	rt.insertPrivilegeRetireTenantAt(t, id, provider, dbUser, password, rt.dbHost, rt.dbPort)
}

// insertPrivilegeRetireTenantAt stores the endpoint the caller names, so a test can
// point a tenant at a closed port instead of silently inheriting the live instance.
func (rt *objectGCTestRuntime) insertPrivilegeRetireTenantAt(t *testing.T, id string, provider, dbUser, password, host string, port int) {
	t.Helper()
	ctx := context.Background()
	cipher, err := rt.pool.Encrypt(ctx, []byte(password))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := rt.meta.InsertTenant(ctx, &meta.Tenant{
		ID:               id,
		Status:           meta.TenantActive,
		Kind:             meta.TenantKindLive,
		DBHost:           host,
		DBPort:           port,
		DBUser:           dbUser,
		DBPasswordCipher: cipher,
		DBName:           rt.dbName,
		Provider:         provider,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}
}

// missingRoleGrant reports whether the server rejected a REVOKE because the role did
// not hold the privilege (the cleanup tolerates that, nothing else).
func missingRoleGrant(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && (mysqlErr.Number == 1141 || mysqlErr.Number == 1147)
}
