package server

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"github.com/mem9-ai/drive9/pkg/tenant/tidbcloudnative"
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
	// An expired delay leaves the attempt count alone, so the next failure continues
	// towards the cap instead of restarting at one attempt on every pass.
	s.nativePrivilegeRetireMu.Lock()
	s.nativePrivilegeRetireRetryAt["tenant-a"] = time.Now().Add(-time.Second)
	s.nativePrivilegeRetireMu.Unlock()
	if s.nativePrivilegeRetireBackoff("tenant-a") {
		t.Fatal("an expired delay still reports backoff")
	}
	s.nativePrivilegeRetireMu.Lock()
	attempts = s.nativePrivilegeRetireFailures["tenant-a"]
	s.nativePrivilegeRetireMu.Unlock()
	if attempts != 11 {
		t.Fatalf("attempt count after an expired delay = %d, want 11", attempts)
	}
	s.recordNativePrivilegeRetireFailure("tenant-a")
	s.nativePrivilegeRetireMu.Lock()
	retryAt = s.nativePrivilegeRetireRetryAt["tenant-a"]
	s.nativePrivilegeRetireMu.Unlock()
	if delay := time.Until(retryAt); delay < nativePrivilegeRetireMaxBackoff-time.Minute {
		t.Fatalf("retry delay %s restarted below the cap %s", delay, nativePrivilegeRetireMaxBackoff)
	}
	s.clearNativePrivilegeRetireFailure("tenant-a")
	if s.nativePrivilegeRetireBackoff("tenant-a") {
		t.Fatal("retry state survived a success")
	}
	s.nativePrivilegeRetireMu.Lock()
	attempts = s.nativePrivilegeRetireFailures["tenant-a"]
	s.nativePrivilegeRetireMu.Unlock()
	if attempts != 0 {
		t.Fatalf("attempt count survived a success: %d", attempts)
	}
}

// One gateway endpoint can front several instances, so the endpoint alone must not fold
// two instances into one dedupe key: the account prefix names the instance.
func TestNativePrivilegeRetireClusterKey(t *testing.T) {
	a := &meta.Tenant{DBHost: "gateway01.example", DBPort: 4000, DBUser: "insta.tdc_fs_sys"}
	b := &meta.Tenant{DBHost: "gateway01.example", DBPort: 4000, DBUser: "instb.tdc_fs_sys"}
	same := &meta.Tenant{DBHost: "gateway01.example", DBPort: 4000, DBUser: "insta.tdc_fs_sys"}
	if nativePrivilegeRetireClusterKey(a) == nativePrivilegeRetireClusterKey(b) {
		t.Fatal("two instances behind one endpoint share a dedupe key")
	}
	if nativePrivilegeRetireClusterKey(a) != nativePrivilegeRetireClusterKey(same) {
		t.Fatal("tenants on the same instance do not share a dedupe key")
	}
	if nativePrivilegeRetireClusterKey(nil) != "" || nativePrivilegeRetireClusterKey(&meta.Tenant{DBUser: "x"}) != "" {
		t.Fatal("a tenant without an endpoint got a cluster key")
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
	// A suspended tenant keeps its grant too, and is now behind its own cursor in a
	// later pass, so the retry only appears if that pass reads suspended records.
	rt.insertPrivilegeRetireTenantAt(t, meta.TenantSuspended, "native-suspended", tenant.ProviderTiDBCloudNative, "t4.tdc_fs_sys", rt.dbPass, "127.0.0.1", 1)

	srv := &Server{meta: rt.meta, pool: rt.pool}
	srv.retireNativeTenantVariablesPrivilege(ctx)

	if !srv.nativePrivilegeRetireBackoff("native-unreachable") {
		t.Fatal("an unreachable native tenant did not schedule a retry")
	}
	if srv.nativePrivilegeRetireRecentlyDone("native-unreachable") {
		t.Fatal("a failed retire was memoized as done")
	}
	if !srv.nativePrivilegeRetireBackoff("native-suspended") {
		t.Fatal("a suspended tenant was not scanned")
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
	const password = "drive9-retire-test"
	// A principal unique to this run: the fixture then never has to delete an account it
	// did not create, and cannot leave a fixed-name privileged principal behind either.
	// The name has to keep the <prefix>.root form the remediation derives from
	// <prefix>.tdc_fs_sys.
	prefix := "ti" + strconv.FormatInt(time.Now().UnixNano(), 36)
	rootUser := prefix + ".root"
	tenantUser := prefix + ".tdc_fs_sys"
	// The revoke targets the fixed production role, so a cluster that already has it is
	// not a safe fixture: setup would add a grant and the pass would strip a grant that
	// belongs to real tenants. Refuse instead of mutating a shared instance.
	var probe string
	if err := admin.QueryRowContext(ctx, "SHOW GRANTS FOR '"+role+"'").Scan(&probe); err == nil {
		t.Skipf("role %s already exists on this cluster", role)
	} else if !tidbcloudnative.IsMissingGrantError(err) {
		t.Fatalf("probe role %s: %v", role, err)
	}
	// The stored credential has to authenticate as <prefix>.root, and that principal
	// needs the privilege WITH GRANT OPTION to revoke it from the role.
	// Teardown first: a mid-setup failure must not leave the privileged principal or the
	// cluster-wide role grant behind on a persistent instance. Everything it does is
	// idempotent, and it only undoes what setup actually created.
	var roleCreated, principalCreated bool
	t.Cleanup(func() {
		ctx := context.Background()
		if roleCreated {
			if _, err := admin.ExecContext(ctx, "REVOKE SYSTEM_VARIABLES_ADMIN ON *.* FROM '"+role+"'"); err != nil && !tidbcloudnative.IsMissingGrantError(err) {
				t.Errorf("cleanup: revoke role grant: %v", err)
			}
		}
		if roleCreated {
			if _, err := admin.ExecContext(ctx, "DROP ROLE IF EXISTS '"+role+"'"); err != nil {
				t.Errorf("cleanup: drop role: %v", err)
			}
		}
		if principalCreated {
			if _, err := admin.ExecContext(ctx, "DROP USER IF EXISTS '"+rootUser+"'@'%'"); err != nil {
				t.Errorf("cleanup: drop privileged principal: %v", err)
			}
		}
	})
	setup := []struct {
		stmt string
		flag *bool
	}{
		{"CREATE USER '" + rootUser + "'@'%' IDENTIFIED BY '" + password + "'", &principalCreated},
		{"GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO '" + rootUser + "'@'%' WITH GRANT OPTION", nil},
		// The remediation DSN names the tenant database, so the root principal needs
		// access to it as a real cluster root would.
		{"GRANT ALL ON `" + rt.dbName + "`.* TO '" + rootUser + "'@'%'", nil},
		{"CREATE ROLE IF NOT EXISTS '" + role + "'", &roleCreated},
		{"GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO '" + role + "'", nil},
		{"GRANT SELECT ON `" + rt.dbName + "`.* TO '" + role + "'", nil},
	}
	for _, step := range setup {
		if _, err := admin.ExecContext(ctx, step.stmt); err != nil {
			t.Fatalf("%s: %v", step.stmt, err)
		}
		if step.flag != nil {
			*step.flag = true
		}
	}

	rt.insertPrivilegeRetireTenantWithPassword(t, "native-revocable", tenant.ProviderTiDBCloudNative, tenantUser, password)
	// A second tenant on the same cluster: the revoke is cluster-global, so the pass must
	// retire it without opening its own connection.
	rt.insertPrivilegeRetireTenantWithPassword(t, "native-sibling", tenant.ProviderTiDBCloudNative, tenantUser, password)

	srv := &Server{meta: rt.meta, pool: rt.pool}
	srv.retireNativeTenantVariablesPrivilege(ctx)

	if srv.nativePrivilegeRetireBackoff("native-revocable") {
		t.Fatal("the revoke failed and scheduled a retry")
	}
	for _, id := range []string{"native-revocable", "native-sibling"} {
		if !srv.nativePrivilegeRetireRecentlyDone(id) {
			t.Fatalf("%s was not memoized after the cluster-wide revoke", id)
		}
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
	rows, err := db.QueryContext(context.Background(), "SHOW GRANTS FOR '"+role+"'")
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
	rt.insertPrivilegeRetireTenantAt(t, meta.TenantActive, id, provider, dbUser, rt.dbPass, host, port)
}

func (rt *objectGCTestRuntime) insertPrivilegeRetireTenantWithPassword(t *testing.T, id string, provider, dbUser, password string) {
	t.Helper()
	rt.insertPrivilegeRetireTenantAt(t, meta.TenantActive, id, provider, dbUser, password, rt.dbHost, rt.dbPort)
}

// insertPrivilegeRetireTenantAt stores the status and endpoint the caller names, so a
// test can point a tenant at a closed port instead of silently inheriting the live
// instance.
func (rt *objectGCTestRuntime) insertPrivilegeRetireTenantAt(t *testing.T, status meta.TenantStatus, id string, provider, dbUser, password, host string, port int) {
	t.Helper()
	ctx := context.Background()
	cipher, err := rt.pool.Encrypt(ctx, []byte(password))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := rt.meta.InsertTenant(ctx, &meta.Tenant{
		ID:               id,
		Status:           status,
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
