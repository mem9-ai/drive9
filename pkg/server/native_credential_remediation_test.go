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
func TestNativeCredentialRemediationBackoff(t *testing.T) {
	s := &Server{}
	if s.nativeCredentialRemediationBackoff("tenant-a") {
		t.Fatal("a tenant without a failure is in backoff")
	}
	s.recordNativeCredentialRemediationFailure("tenant-a")
	if !s.nativeCredentialRemediationBackoff("tenant-a") {
		t.Fatal("no backoff after a failure")
	}
	for i := 0; i < 10; i++ {
		s.recordNativeCredentialRemediationFailure("tenant-a")
	}
	s.nativeCredentialRemediationMu.Lock()
	attempts := s.nativeCredentialRemediationFailures["tenant-a"]
	retryAt := s.nativeCredentialRemediationRetryAt["tenant-a"]
	s.nativeCredentialRemediationMu.Unlock()
	if attempts != 11 {
		t.Fatalf("attempt count = %d, want 11", attempts)
	}
	if delay := time.Until(retryAt); delay > nativeCredentialRemediationMaxBackoff+time.Minute {
		t.Fatalf("retry delay %s exceeds the cap %s", delay, nativeCredentialRemediationMaxBackoff)
	}
	// Another tenant is unaffected.
	if s.nativeCredentialRemediationBackoff("tenant-b") {
		t.Fatal("a failure leaked to another tenant")
	}
	// An expired delay leaves the attempt count alone, so the next failure continues
	// towards the cap instead of restarting at one attempt on every pass.
	s.nativeCredentialRemediationMu.Lock()
	s.nativeCredentialRemediationRetryAt["tenant-a"] = time.Now().Add(-time.Second)
	s.nativeCredentialRemediationMu.Unlock()
	if s.nativeCredentialRemediationBackoff("tenant-a") {
		t.Fatal("an expired delay still reports backoff")
	}
	s.nativeCredentialRemediationMu.Lock()
	attempts = s.nativeCredentialRemediationFailures["tenant-a"]
	s.nativeCredentialRemediationMu.Unlock()
	if attempts != 11 {
		t.Fatalf("attempt count after an expired delay = %d, want 11", attempts)
	}
	s.recordNativeCredentialRemediationFailure("tenant-a")
	s.nativeCredentialRemediationMu.Lock()
	retryAt = s.nativeCredentialRemediationRetryAt["tenant-a"]
	s.nativeCredentialRemediationMu.Unlock()
	if delay := time.Until(retryAt); delay < nativeCredentialRemediationMaxBackoff-time.Minute {
		t.Fatalf("retry delay %s restarted below the cap %s", delay, nativeCredentialRemediationMaxBackoff)
	}
	s.clearNativeCredentialRemediationFailure("tenant-a")
	if s.nativeCredentialRemediationBackoff("tenant-a") {
		t.Fatal("retry state survived a success")
	}
	s.nativeCredentialRemediationMu.Lock()
	attempts = s.nativeCredentialRemediationFailures["tenant-a"]
	s.nativeCredentialRemediationMu.Unlock()
	if attempts != 0 {
		t.Fatalf("attempt count survived a success: %d", attempts)
	}
}

// One gateway endpoint can front several instances, so the endpoint alone must not fold
// two instances into one dedupe key: the account prefix names the instance.
// The scan visits every page, decides from each stored record, and records a retry for
// a tenant whose database cannot be reached while leaving the others alone.
func TestNativeCredentialRemediationScanPagesAndClassifies(t *testing.T) {
	rt := newObjectGCTestRuntime(t)
	ctx := context.Background()
	// One tenant per page forces the cursor path.
	oldBatch := nativeCredentialRemediationBatchSize
	nativeCredentialRemediationBatchSize = 1
	t.Cleanup(func() { nativeCredentialRemediationBatchSize = oldBatch })

	// A native tenant whose restricted account cannot be reached: the remediation
	// fails and schedules a retry.
	rt.insertRemediationTenant(t, "native-unreachable", tenant.ProviderTiDBCloudNative, "t1.tdc_fs_sys", "127.0.0.1", 1)
	// A non-native tenant and a native tenant still naming the root principal are
	// both skipped without a retry entry.
	rt.insertRemediationTenant(t, "tidb-zero", tenant.ProviderTiDBZero, "t2.root", "127.0.0.1", 1)
	rt.insertRemediationTenant(t, "native-root", tenant.ProviderTiDBCloudNative, "t3.root", "127.0.0.1", 1)
	// A suspended tenant keeps its grant too, and is now behind its own cursor in a
	// later pass, so the retry only appears if that pass reads suspended records.
	rt.insertRemediationTenantAt(t, meta.TenantSuspended, "native-suspended", tenant.ProviderTiDBCloudNative, "t4.tdc_fs_sys", rt.dbPass, "127.0.0.1", 1)
	// Recorded outcomes are authoritative: neither is dialled, so neither can schedule a
	// retry from a closed port.
	rt.insertRemediationTenantAt(t, meta.TenantActive, "native-review", tenant.ProviderTiDBCloudNative, "t5.tdc_fs_sys", rt.dbPass, "127.0.0.1", 1)
	if err := rt.meta.SetTenantCredentialState(ctx, "native-review", meta.TenantCredentialReviewRequired, "legacy grant unverifiable"); err != nil {
		t.Fatal(err)
	}
	rt.insertRemediationTenantAt(t, meta.TenantActive, "native-remediated", tenant.ProviderTiDBCloudNative, "t6.tdc_fs_sys", rt.dbPass, "127.0.0.1", 1)
	if err := rt.meta.SetTenantCredentialState(ctx, "native-remediated", meta.TenantCredentialRemediated, ""); err != nil {
		t.Fatal(err)
	}

	srv := &Server{meta: rt.meta, pool: rt.pool}
	srv.remediateNativeTenantCredentials(ctx)

	if !srv.nativeCredentialRemediationBackoff("native-unreachable") {
		t.Fatal("an unreachable native tenant did not schedule a retry")
	}
	if srv.nativeCredentialRemediationRecentlyDone("native-unreachable") {
		t.Fatal("a failed retire was memoized as done")
	}
	if !srv.nativeCredentialRemediationBackoff("native-suspended") {
		t.Fatal("a suspended tenant was not scanned")
	}
	for _, id := range []string{"tidb-zero", "native-root", "native-review", "native-remediated"} {
		if srv.nativeCredentialRemediationBackoff(id) {
			t.Fatalf("skipped tenant %s scheduled a retry", id)
		}
	}
}

// A native tenant provisioned by the legacy setup has two problems: its restricted account
// holds the cluster-wide grant, and its password is the cluster root password. One pass
// must fix both — strip the grant, rotate the account onto the derived per-tenant password,
// and persist that password so the tenant can still connect — while keeping the
// own-schema privileges. This uses a real cluster root principal to stand in for the
// account the stored credential authenticates as.
func TestNativeCredentialRemediationRotatesLegacyCredential(t *testing.T) {
	rt := newObjectGCTestRuntime(t)
	f := setupLegacyNativeAccount(t, rt)
	ctx := context.Background()

	rt.insertRemediationTenantWithPassword(t, "native-revocable", tenant.ProviderTiDBCloudNative, f.tenantUser, f.password)

	srv := &Server{meta: rt.meta, pool: rt.pool}
	srv.remediateNativeTenantCredentials(ctx)

	if srv.nativeCredentialRemediationBackoff("native-revocable") {
		t.Fatal("the rotation failed and scheduled a retry")
	}
	if !srv.nativeCredentialRemediationRecentlyDone("native-revocable") {
		t.Fatal("the tenant was not memoized after the rotation")
	}
	grants := roleGrants(t, f.admin, f.role)
	if containsGrant(grants, "SYSTEM_VARIABLES_ADMIN") {
		t.Fatalf("legacy cluster-wide grant survived: %v", grants)
	}
	if !containsGrant(grants, "SELECT") {
		t.Fatalf("own-schema grant was lost: %v", grants)
	}

	// The account must no longer accept the cluster root password, and the credential the
	// pass persisted must work: the tenant would otherwise be locked out of its database.
	rotated := rt.tenantCredential(t, "native-revocable")
	if rotated == f.password {
		t.Fatal("the tenant account still uses the cluster root password")
	}
	if len(rotated) != 32 {
		t.Fatalf("derived credential = %q, want a 32-character derived secret", rotated)
	}
	if err := pingAs(t, rt, f.tenantUser, f.password); err == nil {
		t.Fatal("the rotated account still accepts the cluster root password")
	}
	if err := pingAs(t, rt, f.tenantUser, rotated); err != nil {
		t.Fatalf("the persisted credential does not authenticate: %v", err)
	}
	// The outcome is recorded, so the next leader does not dial this tenant again.
	state, _, err := rt.meta.TenantCredentialState(ctx, "native-revocable")
	if err != nil {
		t.Fatalf("read recorded credential state: %v", err)
	}
	if state != meta.TenantCredentialRemediated {
		t.Fatalf("recorded credential state = %d, want remediated", state)
	}
}

// A tenant whose stored credential no longer authenticates as the cluster root cannot have
// its legacy grant checked or revoked from here. That is recorded as "still possibly
// exposed", never as remediated, so an operator sees it instead of a silent pass.
func TestNativeCredentialRemediationRecordsUnverifiableTenant(t *testing.T) {
	rt := newObjectGCTestRuntime(t)
	if rt.dbUser != "root" {
		t.Skip("needs the test TiDB root account to create the tenant principals")
	}
	ctx := context.Background()
	admin, err := sql.Open("mysql", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })

	prefix := "tu" + strconv.FormatInt(time.Now().UnixNano(), 36)
	rootUser := prefix + ".root"
	tenantUser := prefix + ".tdc_fs_sys"
	t.Cleanup(func() {
		ctx := context.Background()
		for _, user := range []string{rootUser, tenantUser} {
			if _, err := admin.ExecContext(ctx, "DROP USER IF EXISTS '"+user+"'@'%'"); err != nil {
				t.Errorf("cleanup: drop %s: %v", user, err)
			}
		}
	})
	// The tenant account exists with a password the root principal does not share, which is
	// exactly the state an operator rotation leaves behind.
	for _, stmt := range []string{
		"CREATE USER '" + rootUser + "'@'%' IDENTIFIED BY 'root-only-password'",
		"CREATE USER '" + tenantUser + "'@'%' IDENTIFIED BY 'operator-set-password'",
	} {
		if _, err := admin.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	rt.insertRemediationTenantWithPassword(t, "native-unverifiable", tenant.ProviderTiDBCloudNative, tenantUser, "operator-set-password")

	srv := &Server{meta: rt.meta, pool: rt.pool}
	srv.remediateNativeTenantCredentials(ctx)

	state, reason, err := rt.meta.TenantCredentialState(ctx, "native-unverifiable")
	if err != nil {
		t.Fatalf("read recorded credential state: %v", err)
	}
	if state != meta.TenantCredentialReviewRequired {
		t.Fatalf("recorded credential state = %d, want review required", state)
	}
	if !strings.Contains(reason, "root-equivalent") {
		t.Fatalf("review reason = %q, want the reason the grant could not be verified", reason)
	}
	if srv.nativeCredentialRemediationBackoff("native-unverifiable") {
		t.Fatal("a recorded review state scheduled a retry")
	}
	// The stored credential is untouched: the pass must not invent a rotation it could not
	// perform.
	if got := rt.tenantCredential(t, "native-unverifiable"); got != "operator-set-password" {
		t.Fatalf("stored credential = %q, want the operator-set password unchanged", got)
	}
}

// pingAs dials the test instance as one principal, proving which password the database
// actually accepts.
func pingAs(t *testing.T, rt *objectGCTestRuntime, user, password string) error {
	t.Helper()
	db, err := sql.Open("mysql", tenantDSN(user, password, rt.dbHost, rt.dbPort, rt.dbName, false, tenant.ProviderTiDBCloudNative))
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return db.PingContext(ctx)
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

// tenantCredential decrypts the tenant row's stored database password, which is what the
// tenant runtime would dial with after the pass persisted a rotated credential.
func (rt *objectGCTestRuntime) tenantCredential(t *testing.T, id string) string {
	t.Helper()
	stored, err := rt.meta.GetTenant(context.Background(), id)
	if err != nil {
		t.Fatalf("read tenant %s: %v", id, err)
	}
	plain, err := rt.pool.Decrypt(context.Background(), stored.DBPasswordCipher)
	if err != nil {
		t.Fatalf("decrypt tenant %s credential: %v", id, err)
	}
	return string(plain)
}

// insertRemediationTenant records an active tenant whose stored credential is
// encrypted with the test pool, so the pass can decrypt it like a real row.
func (rt *objectGCTestRuntime) insertRemediationTenant(t *testing.T, id string, provider, dbUser, host string, port int) {
	t.Helper()
	rt.insertRemediationTenantAt(t, meta.TenantActive, id, provider, dbUser, rt.dbPass, host, port)
}

func (rt *objectGCTestRuntime) insertRemediationTenantWithPassword(t *testing.T, id string, provider, dbUser, password string) {
	t.Helper()
	rt.insertRemediationTenantAt(t, meta.TenantActive, id, provider, dbUser, password, rt.dbHost, rt.dbPort)
}

// insertRemediationTenantAt stores the status and endpoint the caller names, so a
// test can point a tenant at a closed port instead of silently inheriting the live
// instance.
func (rt *objectGCTestRuntime) insertRemediationTenantAt(t *testing.T, status meta.TenantStatus, id string, provider, dbUser, password, host string, port int) {
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

// Finalization must not claim remediation when the provisioner could not verify the legacy
// grant: an operator-managed account reports that, and the tenant is recorded for review
// instead of being skipped as fixed by every later pass.
func TestFinalizeRecordsUnverifiedCredentialForReview(t *testing.T) {
	rt := newObjectGCTestRuntime(t)
	ctx := context.Background()
	const tenantID = "native-finalize-review"
	const password = "operator-set-password"
	rt.insertRemediationTenantWithPassword(t, tenantID, tenant.ProviderTiDBCloudNative, "t7.tdc_fs_sys", password)

	srv := &Server{
		meta: rt.meta,
		pool: rt.pool,
		provisioner: &fakeProvisioner{
			provider:             tenant.ProviderTiDBCloudNative,
			systemUsername:       "t7.tdc_fs_sys",
			systemPassword:       password,
			systemUserUnverified: true,
		},
	}
	dsn := tenantDSN("t7.tdc_fs_sys", password, rt.dbHost, rt.dbPort, rt.dbName, false, tenant.ProviderTiDBCloudNative)
	if err := srv.finalizeTenantSchemaInit(ctx, tenantID, dsn, tenant.ProviderTiDBCloudNative); err != nil {
		t.Fatalf("finalize: %v", err)
	}

	state, reason, err := rt.meta.TenantCredentialState(ctx, tenantID)
	if err != nil {
		t.Fatalf("read recorded credential state: %v", err)
	}
	if state != meta.TenantCredentialReviewRequired {
		t.Fatalf("recorded credential state = %d, want review required", state)
	}
	if !strings.Contains(reason, "finalization") {
		t.Fatalf("review reason = %q, want the finalization reason", reason)
	}
	// The credential itself is persisted: finalization still records what the tenant dials with.
	if got := rt.tenantCredential(t, tenantID); got != password {
		t.Fatalf("stored credential = %q, want %q", got, password)
	}
}

// A verified finalize records remediation, so the pass never re-dials the tenant.
func TestFinalizeRecordsRemediatedCredential(t *testing.T) {
	rt := newObjectGCTestRuntime(t)
	ctx := context.Background()
	const tenantID = "native-finalize-remediated"
	rt.insertRemediationTenantWithPassword(t, tenantID, tenant.ProviderTiDBCloudNative, "t8.root", "db-pass")

	srv := &Server{
		meta: rt.meta,
		pool: rt.pool,
		provisioner: &fakeProvisioner{
			provider:       tenant.ProviderTiDBCloudNative,
			systemUsername: "t8.tdc_fs_sys",
			systemPassword: "derived-pass",
		},
	}
	dsn := tenantDSN("t8.root", "db-pass", rt.dbHost, rt.dbPort, rt.dbName, false, tenant.ProviderTiDBCloudNative)
	if err := srv.finalizeTenantSchemaInit(ctx, tenantID, dsn, tenant.ProviderTiDBCloudNative); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	state, reason, err := rt.meta.TenantCredentialState(ctx, tenantID)
	if err != nil {
		t.Fatalf("read recorded credential state: %v", err)
	}
	if state != meta.TenantCredentialRemediated || reason != "" {
		t.Fatalf("recorded credential state = %d/%q, want remediated", state, reason)
	}
	if got := rt.tenantCredential(t, tenantID); got != "derived-pass" {
		t.Fatalf("stored credential = %q, want the provisioner's derived password", got)
	}
}

// legacyNativeAccount is the live fixture the remediation needs: a cluster-root principal
// standing in for the account a legacy credential authenticates as, plus the fixed role the
// setup revokes from.
type legacyNativeAccount struct {
	admin      *sql.DB
	role       string
	rootUser   string
	tenantUser string
	password   string
}

// setupLegacyNativeAccount creates that fixture and registers its teardown. It skips when the
// cluster cannot host it: the test TiDB has to be reachable as root, and the fixed production
// role must not already exist, since setup would otherwise add a grant the pass then strips
// from a real tenant.
func setupLegacyNativeAccount(t *testing.T, rt *objectGCTestRuntime) *legacyNativeAccount {
	t.Helper()
	if rt.dbUser != "root" {
		t.Skip("needs the test TiDB root account to create the tenant principals")
	}
	ctx := context.Background()
	admin, err := sql.Open("mysql", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	// Close last: the cleanup below needs a live handle, and a bare defer would run before
	// t.Cleanup and leave the fixture's privileged principal behind.
	t.Cleanup(func() { _ = admin.Close() })

	const role = "tdc_fs_admin"
	const password = "drive9-retire-test"
	// A principal unique to this run, so the fixture never deletes an account it did not
	// create. The name has to keep the <prefix>.root form the remediation derives from
	// <prefix>.tdc_fs_sys.
	prefix := "ti" + strconv.FormatInt(time.Now().UnixNano(), 36)
	f := &legacyNativeAccount{
		admin:      admin,
		role:       role,
		rootUser:   prefix + ".root",
		tenantUser: prefix + ".tdc_fs_sys",
		password:   password,
	}
	var probe string
	if err := admin.QueryRowContext(ctx, "SHOW GRANTS FOR '"+role+"'").Scan(&probe); err == nil {
		t.Skipf("role %s already exists on this cluster", role)
	} else if !tidbcloudnative.IsMissingGrantError(err) {
		t.Fatalf("probe role %s: %v", role, err)
	}
	// Teardown first: a mid-setup failure must not leave the privileged principal or the
	// cluster-wide role grant behind on a persistent instance.
	var roleCreated, principalCreated bool
	t.Cleanup(func() {
		ctx := context.Background()
		if roleCreated {
			if _, err := admin.ExecContext(ctx, "REVOKE SYSTEM_VARIABLES_ADMIN ON *.* FROM '"+role+"'"); err != nil && !tidbcloudnative.IsMissingGrantError(err) {
				t.Errorf("cleanup: revoke role grant: %v", err)
			}
			if _, err := admin.ExecContext(ctx, "DROP ROLE IF EXISTS '"+role+"'"); err != nil {
				t.Errorf("cleanup: drop role: %v", err)
			}
		}
		// The pass creates the restricted account itself, so clean it up too.
		if _, err := admin.ExecContext(ctx, "DROP USER IF EXISTS '"+f.tenantUser+"'@'%'"); err != nil {
			t.Errorf("cleanup: drop tenant principal: %v", err)
		}
		if principalCreated {
			if _, err := admin.ExecContext(ctx, "DROP USER IF EXISTS '"+f.rootUser+"'@'%'"); err != nil {
				t.Errorf("cleanup: drop privileged principal: %v", err)
			}
		}
	})
	setup := []struct {
		stmt string
		flag *bool
	}{
		{"CREATE USER '" + f.rootUser + "'@'%' IDENTIFIED BY '" + password + "'", &principalCreated},
		{"GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO '" + f.rootUser + "'@'%' WITH GRANT OPTION", nil},
		// The rotation runs the full tenant setup through this principal — create the role,
		// create the account, grant the role and switch its password — so it needs the global
		// privileges a real cluster root has.
		{"GRANT ALL PRIVILEGES ON *.* TO '" + f.rootUser + "'@'%' WITH GRANT OPTION", nil},
		// The remediation DSN names the tenant database, so the root principal needs access to
		// it as a real cluster root would.
		{"GRANT ALL ON `" + rt.dbName + "`.* TO '" + f.rootUser + "'@'%'", nil},
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
	return f
}

// A rotation changes the database password before the metadata write. If that write fails or
// the pod dies between the two, the tenant's stored credential is behind its database until a
// retry lands. The retry has to be able to finish the job: the derived password is a pure
// function of the (unchanged) root secret and the tenant, so re-running the same rotation is
// idempotent rather than a second different password.
func TestNativeCredentialRemediationRecoversFromUnpersistedRotation(t *testing.T) {
	rt := newObjectGCTestRuntime(t)
	f := setupLegacyNativeAccount(t, rt)
	ctx := context.Background()
	const tenantID = "native-recovery"
	rt.insertRemediationTenantWithPassword(t, tenantID, tenant.ProviderTiDBCloudNative, f.tenantUser, f.password)

	// The legacy shape: the restricted account exists and shares the cluster root password,
	// so "the retired password is refused" later is a change of state and not an account that
	// was never there.
	for _, stmt := range []string{
		"CREATE USER '" + f.tenantUser + "'@'%' IDENTIFIED BY '" + f.password + "'",
		"GRANT SELECT ON `" + rt.dbName + "`.* TO '" + f.tenantUser + "'@'%'",
	} {
		if _, err := f.admin.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	if err := pingAs(t, rt, f.tenantUser, f.password); err != nil {
		t.Fatalf("the legacy account does not accept the shared root password before rotation: %v", err)
	}

	// Reproduce the window exactly: apply the database half and leave metadata untouched.
	dsn := tenantDSN(f.tenantUser, f.password, rt.dbHost, rt.dbPort, rt.dbName, false, tenant.ProviderTiDBCloudNative)
	rotatedPassword, rotated, err := tidbcloudnative.RotateLegacySystemUserCredential(ctx, dsn, tenantID)
	if err != nil || !rotated || rotatedPassword == "" {
		t.Fatalf("apply the rotation: password=%q rotated=%v err=%v", rotatedPassword, rotated, err)
	}
	if got := rt.tenantCredential(t, tenantID); got != f.password {
		t.Fatalf("metadata credential = %q, want the stale one in this window", got)
	}
	if err := pingAs(t, rt, f.tenantUser, f.password); err == nil {
		t.Fatal("the database still accepts the retired password, so there is no window to recover from")
	}

	// The next pass must notice, re-apply the same rotation and persist it. Recovery is the
	// point of deriving the password instead of generating one.
	srv := &Server{meta: rt.meta, pool: rt.pool}
	srv.remediateNativeTenantCredentials(ctx)

	if srv.nativeCredentialRemediationBackoff(tenantID) {
		t.Fatal("the retry after an unpersisted rotation failed")
	}
	recovered := rt.tenantCredential(t, tenantID)
	if recovered == f.password {
		t.Fatal("the retry did not persist the rotated credential")
	}
	if recovered != rotatedPassword {
		t.Fatalf("recovered credential differs from the applied rotation, so a retry would rotate again")
	}
	if err := pingAs(t, rt, f.tenantUser, recovered); err != nil {
		t.Fatalf("the tenant cannot connect with its persisted credential after recovery: %v", err)
	}
	state, _, err := rt.meta.TenantCredentialState(ctx, tenantID)
	if err != nil {
		t.Fatalf("read recorded credential state: %v", err)
	}
	if state != meta.TenantCredentialRemediated {
		t.Fatalf("recorded credential state = %d, want remediated", state)
	}
}
