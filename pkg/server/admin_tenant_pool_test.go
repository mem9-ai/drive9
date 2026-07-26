package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testmysql"
	"github.com/mem9-ai/drive9/pkg/encrypt"
	"github.com/mem9-ai/drive9/pkg/leader"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
)

func newFollowerLeaderManager(t *testing.T, metaStore *meta.Store) *leader.Manager {
	t.Helper()
	ctx := context.Background()
	conn, err := metaStore.DB().Conn(ctx)
	if err != nil {
		t.Fatalf("open leader lock connection: %v", err)
	}
	lockName := fmt.Sprintf("d9:test-follower:%d", time.Now().UnixNano())
	var acquired sql.NullInt64
	if err := conn.QueryRowContext(ctx, `SELECT GET_LOCK(?, 0)`, lockName).Scan(&acquired); err != nil {
		_ = conn.Close()
		t.Fatalf("hold leader lock: %v", err)
	}
	if !acquired.Valid || acquired.Int64 != 1 {
		_ = conn.Close()
		t.Fatalf("hold leader lock result = %+v, want 1", acquired)
	}
	t.Cleanup(func() {
		var released sql.NullInt64
		_ = conn.QueryRowContext(context.Background(), `SELECT RELEASE_LOCK(?)`, lockName).Scan(&released)
		_ = conn.Close()
	})
	return leader.NewManager(metaStore.DB(), leader.WithLockName(lockName))
}

func TestRetryTenantPoolClaimCASSucceedsOnEighthAttempt(t *testing.T) {
	attempts := 0
	got, err := retryTenantPoolClaimCAS(func() (int, error) {
		attempts++
		if attempts < tenantPoolClaimCASRetryLimit {
			return 0, meta.ErrNotFound
		}
		return 42, nil
	})
	if err != nil {
		t.Fatalf("retryTenantPoolClaimCAS: %v", err)
	}
	if got != 42 || attempts != tenantPoolClaimCASRetryLimit {
		t.Fatalf("result=%d attempts=%d, want 42/%d", got, attempts, tenantPoolClaimCASRetryLimit)
	}
}

func TestRetryTenantPoolClaimCASReturnsMissAfterLimit(t *testing.T) {
	attempts := 0
	got, err := retryTenantPoolClaimCAS(func() (int, error) {
		attempts++
		return 0, meta.ErrNotFound
	})
	if err != nil {
		t.Fatalf("err = %v, want nil for exhausted claim miss", err)
	}
	if got != 0 {
		t.Fatalf("result = %d, want zero value", got)
	}
	if attempts != tenantPoolClaimCASRetryLimit {
		t.Fatalf("attempts=%d, want %d", attempts, tenantPoolClaimCASRetryLimit)
	}
}

func TestRetryTenantPoolClaimCASDoesNotRetryBusinessError(t *testing.T) {
	wantErr := errors.New("quota headroom exceeded")
	attempts := 0
	_, err := retryTenantPoolClaimCAS(func() (int, error) {
		attempts++
		return 0, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d, want 1", attempts)
	}
}

func TestTenantPoolClaimUsesNativeInventoryBeforeExternalSharedPool(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	rt.prov.iamIdentities = []*tenant.TiDBCloudAPIKeyIdentity{{
		OrganizationID: "org-mixed-inventory", Role: tenant.TiDBCloudRoleOrgOwner,
	}}
	now := time.Now().UTC()
	if err := rt.meta.CreateTenantPool(ctx, &meta.TenantPool{
		PoolID: "pool-mixed-inventory", OrganizationID: "org-mixed-inventory", Size: 1,
		Status: meta.TenantPoolActive, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("CreateTenantPool: %v", err)
	}
	nativeTenantID := insertAdminPoolFreeTenant(t, rt, "pool-mixed-inventory", "org-mixed-inventory", 1)
	if _, err := rt.meta.RegisterSharedDB(ctx, &meta.SharedDB{
		TiDBCloudOrganizationID: "org-mixed-inventory", Host: "shared.example.com", Port: 4000,
		User: "root", PasswordCipher: []byte("cipher"), Name: "shared_db", MaxTenants: 100,
	}); err != nil {
		t.Fatalf("RegisterSharedDB: %v", err)
	}

	res, pool, claimed, sharedPoolMatched, err := rt.server.claimAdminTenantFromPool(ctx,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"}, nil)
	if err != nil {
		t.Fatalf("claimAdminTenantFromPool: %v", err)
	}
	if !claimed || sharedPoolMatched || pool == nil || res == nil {
		t.Fatalf("claim = result=%+v pool=%+v claimed=%v sharedMatched=%v", res, pool, claimed, sharedPoolMatched)
	}
	if res.TenantID != nativeTenantID || res.Provider != tenant.ProviderTiDBCloudNative {
		t.Fatalf("claimed result = %+v, want native tenant %s", res, nativeTenantID)
	}
	deadline := time.Now().Add(time.Second)
	for {
		if _, ok := rt.server.tenantPoolReplenishJobs.Load("pool-mixed-inventory"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("native request did not trigger request-scoped tenant-pool replenishment")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestTenantPoolClaimConsumesMixedInventoryInGlobalAgeOrder(t *testing.T) {
	rt := newQuotaRuntimeWithOptions(t, tenant.ProviderTiDBCloudNative, quotaRuntimeOptions{
		defaultTenantProvider: tenant.ProviderTiDBCloudNativeShared,
	})
	ctx := context.Background()
	rt.prov.iamIdentities = []*tenant.TiDBCloudAPIKeyIdentity{{
		OrganizationID: "org-mixed-age", Role: tenant.TiDBCloudRoleProjectOwner,
	}}
	now := time.Now().UTC()
	if err := rt.meta.CreateTenantPool(ctx, &meta.TenantPool{
		PoolID: "pool-mixed-age", OrganizationID: "org-mixed-age", Size: 2,
		Status: meta.TenantPoolActive, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("CreateTenantPool: %v", err)
	}
	nativeTenantID := insertAdminPoolFreeTenant(t, rt, "pool-mixed-age", "org-mixed-age", 1)
	passwordCipher, err := rt.server.pool.Encrypt(ctx, []byte("shared-pass"))
	if err != nil {
		t.Fatal(err)
	}
	dbID, err := rt.meta.RegisterSharedDB(ctx, &meta.SharedDB{
		TiDBCloudOrganizationID: "org-mixed-age", Host: "shared.example.com", Port: 4000,
		User: "root", PasswordCipher: passwordCipher, Name: "shared_db", MaxTenants: 10,
	})
	if err != nil {
		t.Fatalf("RegisterSharedDB: %v", err)
	}
	sharedTenantID := "pool-mixed-age-shared"
	sharedCreatedAt := now.Add(time.Hour)
	if err := rt.server.insertPendingPoolTenant(ctx, sharedTenantID, tenant.ProviderTiDBCloudNativeShared, sharedCreatedAt); err != nil {
		t.Fatalf("insertPendingPoolTenant: %v", err)
	}
	if err := rt.server.materializeSharedTenantQuota(ctx, sharedTenantID, provisionTenantOptions{}); err != nil {
		t.Fatalf("materializeSharedTenantQuota: %v", err)
	}
	fsID, err := rt.meta.EnsureFsID(ctx, sharedTenantID)
	if err != nil {
		t.Fatalf("EnsureFsID: %v", err)
	}
	if err := rt.meta.CompleteSharedTenantPoolMember(ctx, sharedTenantID, tenant.ProviderTiDBCloudNativeShared,
		&meta.TenantPlacement{FsID: fsID, DbID: dbID, Placement: meta.PlacementShared,
			SchemaShape: meta.SchemaShapeShared, Status: meta.SharedDBStatusActive},
		&meta.TenantPoolMembership{TenantID: sharedTenantID, TiDBCloudOrganizationID: "org-mixed-age",
			PoolID: "pool-mixed-age", PoolStatus: meta.TenantPoolBindingFree,
			CreatedAt: sharedCreatedAt, UpdatedAt: sharedCreatedAt}); err != nil {
		t.Fatalf("CompleteSharedTenantPoolMember: %v", err)
	}
	free, err := rt.meta.CountFreeTenantPoolBindings(ctx, "org-mixed-age")
	if err != nil {
		t.Fatalf("CountFreeTenantPoolBindings: %v", err)
	}
	if free != 2 {
		t.Fatalf("mixed native/shared free inventory = %d, want 2", free)
	}

	cred := tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"}
	first, _, claimed, _, err := rt.server.claimAdminTenantFromPool(ctx, cred, nil)
	if err != nil || !claimed {
		t.Fatalf("first claim = %+v, claimed=%v, err=%v", first, claimed, err)
	}
	if first.TenantID != nativeTenantID || first.Provider != tenant.ProviderTiDBCloudNative {
		t.Fatalf("first claim = %+v, want older native tenant %s", first, nativeTenantID)
	}
	if _, ok := rt.server.tenantPoolReplenishJobs.Load("pool-mixed-age"); ok {
		t.Fatal("shared-default request unexpectedly triggered bulk tenant-pool replenishment")
	}
	second, _, claimed, _, err := rt.server.claimAdminTenantFromPool(ctx, cred, nil)
	if err != nil || !claimed {
		t.Fatalf("second claim = %+v, claimed=%v, err=%v", second, claimed, err)
	}
	if second.TenantID != sharedTenantID || second.Provider != tenant.ProviderTiDBCloudNativeShared {
		t.Fatalf("second claim = %+v, want shared tenant %s", second, sharedTenantID)
	}
	if second.TenantDSN != "" {
		t.Fatalf("shared claim TenantDSN = %q, want empty", second.TenantDSN)
	}
}

func TestSharedTenantPoolRefillPlansTenPoolsInOneBatch(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	poolManager := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer poolManager.Close()
	poolManager.SetMetaStore(metaStore)
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative, cloudProvider: "aws", region: "us-east-1"}
	workerCtx, cancel := context.WithCancel(context.Background())
	srv := &Server{
		meta: metaStore, pool: poolManager, provisioner: prov,
		defaultTenantProvider: tenant.ProviderTiDBCloudNativeShared,
		sharedDBMaxTenants:    1, managedSharedDBCloudBatchSize: 10,
		forkWorkerCtx: workerCtx, forkWorkerCancel: cancel,
	}
	defer srv.Close()
	now := time.Now().UTC()
	if err := metaStore.CreateTenantPool(context.Background(), &meta.TenantPool{PoolID: "pool-shared-10",
		OrganizationID: "org-shared-ten-pools", Size: 10, Status: meta.TenantPoolActive, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	results, err := srv.createFreePoolTenants(context.Background(), "pool-shared-10", 10,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"}, nil)
	if err != nil {
		t.Fatalf("createFreePoolTenants: %v", err)
	}
	if len(results) != 10 {
		t.Fatalf("results = %d, want 10", len(results))
	}
	for _, result := range results {
		if result.Status != meta.TenantPending {
			t.Fatalf("tenant %s status = %q, want pending while connection metadata is incomplete", result.TenantID, result.Status)
		}
	}
	if got := prov.sharedPoolBatchCalls.Load(); got != 1 {
		t.Fatalf("shared batch calls = %d, want 1", got)
	}
	if got := prov.sharedPoolBatchMembers.Load(); got != 10 {
		t.Fatalf("shared batch members = %d, want 10", got)
	}
	var managedPools int
	if err := metaStore.DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM db_pool
		WHERE org_id = ? AND status = ?`, "org-shared-ten-pools", meta.SharedDBStatusPending).Scan(&managedPools); err != nil {
		t.Fatal(err)
	}
	if managedPools != 10 {
		t.Fatalf("managed pools = %d, want 10", managedPools)
	}
	slots, err := metaStore.CountTenantPoolFreeSlots(context.Background(), "org-shared-ten-pools")
	if err != nil {
		t.Fatal(err)
	}
	if slots != 10 {
		t.Fatalf("free slots = %d, want 10", slots)
	}
}

func TestSharedTenantPoolRefillMakesWaveVisibleOnlyAfterMembershipReservation(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	poolManager := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer poolManager.Close()
	poolManager.SetMetaStore(metaStore)
	now := time.Now().UTC()
	if err := metaStore.CreateTenantPool(context.Background(), &meta.TenantPool{
		PoolID: "pool-stage-wave", OrganizationID: "org-stage-wave", Size: 6,
		Status: meta.TenantPoolActive, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	batchStarted := make(chan struct{}, 1)
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative, cloudProvider: "aws", region: "us-east-1",
		sharedPoolBatchStarted: batchStarted}
	workerCtx, workerCancel := context.WithCancel(context.Background())
	srv := &Server{meta: metaStore, pool: poolManager, provisioner: prov,
		defaultTenantProvider: tenant.ProviderTiDBCloudNativeShared, sharedDBMaxTenants: 2,
		managedSharedDBCloudBatchSize: 10, forkWorkerCtx: workerCtx, forkWorkerCancel: workerCancel}

	lockConn, err := metaStore.DB().Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockConn.ExecContext(context.Background(), `LOCK TABLES tenant_quota_config WRITE`); err != nil {
		_ = lockConn.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	finished := false
	go func() {
		_, err := srv.createFreeSharedPoolTenants(ctx, "pool-stage-wave", 6,
			tenant.CredentialProvisionRequest{}, nil)
		done <- err
	}()
	t.Cleanup(func() {
		cancel()
		_, _ = lockConn.ExecContext(context.Background(), `UNLOCK TABLES`)
		_ = lockConn.Close()
		if !finished {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("shared refill did not stop after releasing quota table lock")
			}
		}
		srv.Close()
	})

	select {
	case <-batchStarted:
		t.Fatal("Cloud batch started before tenant membership reservations committed")
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := metaStore.FindSharedDBForAllocation(context.Background(), "org-stage-wave"); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("direct allocation saw refill-staged capacity before membership commit: %v", err)
	}
	if _, err := lockConn.ExecContext(context.Background(), `UNLOCK TABLES`); err != nil {
		t.Fatal(err)
	}
	select {
	case <-batchStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("Cloud batch did not start after durable wave staging committed")
	}
	select {
	case err := <-done:
		finished = true
		if err != nil {
			t.Fatalf("createFreeSharedPoolTenants: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shared refill did not finish after durable wave staging")
	}
	free, err := metaStore.CountTenantPoolFreeSlots(context.Background(), "org-stage-wave")
	if err != nil {
		t.Fatal(err)
	}
	if free != 6 {
		t.Fatalf("durably reserved free slots = %d, want 6", free)
	}
	if _, err := metaStore.FindSharedDBForAllocation(context.Background(), "org-stage-wave"); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("direct allocation saw capacity already reserved by the completed refill wave: %v", err)
	}
	active, err := metaStore.CountFreeTenantPoolBindings(context.Background(), "org-stage-wave")
	if err != nil {
		t.Fatal(err)
	}
	planned, err := metaStore.CountTenantPoolPlannedSlots(context.Background(), "org-stage-wave")
	if err != nil {
		t.Fatal(err)
	}
	if active+planned != 6 {
		t.Fatalf("active + planned slots after atomic wave commit = %d + %d, want 6", active, planned)
	}
}

func TestSharedTenantPoolRefillDoesNotCommitAfterLogicalPoolDelete(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	poolManager := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	t.Cleanup(poolManager.Close)
	poolManager.SetMetaStore(metaStore)
	ctx := context.Background()
	logicalPool := &meta.TenantPool{PoolID: "pool-delete-during-wave", OrganizationID: "org-delete-during-wave",
		Size: 2, Status: meta.TenantPoolActive, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if err := metaStore.CreateTenantPool(ctx, logicalPool); err != nil {
		t.Fatal(err)
	}
	batchStarted := make(chan struct{}, 1)
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative, cloudProvider: "aws", region: "us-east-1",
		sharedPoolBatchStarted: batchStarted}
	workerCtx, workerCancel := context.WithCancel(context.Background())
	srv := &Server{meta: metaStore, pool: poolManager, provisioner: prov,
		defaultTenantProvider: tenant.ProviderTiDBCloudNativeShared, sharedDBMaxTenants: 1,
		managedSharedDBCloudBatchSize: 10, forkWorkerCtx: workerCtx, forkWorkerCancel: workerCancel}
	t.Cleanup(srv.Close)

	lockConn, err := metaStore.DB().Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockConn.ExecContext(ctx, `LOCK TABLES tenant_quota_config WRITE`); err != nil {
		_ = lockConn.Close()
		t.Fatal(err)
	}
	locked := true
	t.Cleanup(func() {
		if locked {
			_, _ = lockConn.ExecContext(context.Background(), `UNLOCK TABLES`)
		}
		_ = lockConn.Close()
	})
	waveDone := make(chan error, 1)
	go func() {
		_, createErr := srv.createFreeSharedPoolTenants(ctx, logicalPool.PoolID, logicalPool.Size,
			tenant.CredentialProvisionRequest{}, nil)
		waveDone <- createErr
	}()
	select {
	case err := <-waveDone:
		t.Fatalf("refill returned before quota lock was released: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := metaStore.UpdateTenantPoolStatus(ctx, logicalPool.PoolID, meta.TenantPoolDeleting); err != nil {
		t.Fatal(err)
	}
	if err := metaStore.DeleteTenantPoolAndDetachUsedMembers(ctx, logicalPool.PoolID); err != nil {
		t.Fatalf("delete logical pool while refill is blocked: %v", err)
	}
	if _, err := lockConn.ExecContext(ctx, `UNLOCK TABLES`); err != nil {
		t.Fatal(err)
	}
	locked = false
	select {
	case err := <-waveDone:
		if err == nil {
			t.Fatal("refill committed after logical pool deletion")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("refill did not stop after logical pool deletion")
	}
	select {
	case <-batchStarted:
		t.Fatal("Cloud batch started after logical pool deletion won")
	default:
	}
	if _, err := metaStore.GetTenantPoolByID(ctx, logicalPool.PoolID); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("logical pool after delete = %v, want not found", err)
	}
	var physicalRows, memberships int
	if err := metaStore.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM db_pool WHERE org_id = ?`, logicalPool.OrganizationID).Scan(&physicalRows); err != nil {
		t.Fatal(err)
	}
	if err := metaStore.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM tenant_pool_memberships WHERE pool_id = ?`, logicalPool.PoolID).Scan(&memberships); err != nil {
		t.Fatal(err)
	}
	if physicalRows != 0 || memberships != 0 {
		t.Fatalf("durable refill residue after delete: physical=%d memberships=%d", physicalRows, memberships)
	}
}

func TestSharedTenantPoolRefillDoesNotOverfillAfterLogicalPoolShrink(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	poolManager := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	t.Cleanup(poolManager.Close)
	poolManager.SetMetaStore(metaStore)
	ctx := context.Background()
	logicalPool := &meta.TenantPool{PoolID: "pool-shrink-during-wave", OrganizationID: "org-shrink-during-wave",
		Size: 2, Status: meta.TenantPoolActive, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if err := metaStore.CreateTenantPool(ctx, logicalPool); err != nil {
		t.Fatal(err)
	}
	batchStarted := make(chan struct{}, 2)
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative, cloudProvider: "aws", region: "us-east-1",
		sharedPoolBatchStarted: batchStarted}
	workerCtx, workerCancel := context.WithCancel(context.Background())
	srv := &Server{meta: metaStore, pool: poolManager, provisioner: prov,
		defaultTenantProvider: tenant.ProviderTiDBCloudNativeShared, sharedDBMaxTenants: 1,
		managedSharedDBCloudBatchSize: 10, forkWorkerCtx: workerCtx, forkWorkerCancel: workerCancel}
	t.Cleanup(srv.Close)

	lockConn, err := metaStore.DB().Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockConn.ExecContext(ctx, `LOCK TABLES tenant_quota_config WRITE`); err != nil {
		_ = lockConn.Close()
		t.Fatal(err)
	}
	locked := true
	t.Cleanup(func() {
		if locked {
			_, _ = lockConn.ExecContext(context.Background(), `UNLOCK TABLES`)
		}
		_ = lockConn.Close()
	})
	waveDone := make(chan error, 1)
	go func() {
		_, createErr := srv.createFreeSharedPoolTenants(ctx, logicalPool.PoolID, logicalPool.Size,
			tenant.CredentialProvisionRequest{}, nil)
		waveDone <- createErr
	}()
	select {
	case err := <-waveDone:
		t.Fatalf("refill returned before quota lock was released: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := metaStore.UpdateTenantPoolSize(ctx, logicalPool.PoolID, 1); err != nil {
		t.Fatalf("shrink logical pool while refill is blocked: %v", err)
	}
	if _, err := lockConn.ExecContext(ctx, `UNLOCK TABLES`); err != nil {
		t.Fatal(err)
	}
	locked = false
	select {
	case err := <-waveDone:
		if err == nil {
			t.Fatal("stale refill committed after logical pool shrink")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stale refill did not stop after logical pool shrink")
	}
	select {
	case <-batchStarted:
		t.Fatal("Cloud batch started for stale oversized refill")
	default:
	}
	gotPool, err := metaStore.GetTenantPoolByID(ctx, logicalPool.PoolID)
	if err != nil {
		t.Fatal(err)
	}
	if gotPool.Size != 1 {
		t.Fatalf("logical pool size = %d, want 1", gotPool.Size)
	}
	if free, err := metaStore.CountTenantPoolFreeSlots(ctx, logicalPool.OrganizationID); err != nil || free != 0 {
		t.Fatalf("free slots after rejected stale refill = %d, err=%v", free, err)
	}
	if _, err := srv.createFreeSharedPoolTenants(ctx, logicalPool.PoolID, 1,
		tenant.CredentialProvisionRequest{}, nil); err != nil {
		t.Fatalf("correctly sized refill after shrink: %v", err)
	}
	select {
	case <-batchStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("Cloud batch did not start for correctly sized refill")
	}
	if free, err := metaStore.CountTenantPoolFreeSlots(ctx, logicalPool.OrganizationID); err != nil || free != 1 {
		t.Fatalf("free slots after correctly sized refill = %d, err=%v", free, err)
	}
}

func TestSharedTenantPoolRefillPlansWithServerOwnedSharedCredential(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	poolManager := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer poolManager.Close()
	poolManager.SetMetaStore(metaStore)
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative, cloudProvider: "aws", region: "us-east-1",
		sharedPoolBatchErr: errors.New("stop after managed pool planning")}
	srv := NewWithConfig(Config{Meta: metaStore, Pool: poolManager, Provisioner: prov,
		DefaultTenantProvider: tenant.ProviderTiDBCloudNativeShared, TokenSecret: make([]byte, 32),
		Leader: newFollowerLeaderManager(t, metaStore)})
	defer srv.Close()
	now := time.Now().UTC()
	if err := metaStore.CreateTenantPool(context.Background(), &meta.TenantPool{PoolID: "pool-shared-credential",
		OrganizationID: "org-shared", Size: 1, Status: meta.TenantPoolActive, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	_, err = srv.createFreeSharedPoolTenants(context.Background(), "pool-shared-credential", 1,
		tenant.CredentialProvisionRequest{PublicKey: "customer-public", PrivateKey: "customer-private"}, nil)
	if err == nil {
		t.Fatal("createFreeSharedPoolTenants succeeded, want forced batch failure")
	}
	rows, err := metaStore.ListSharedDBsByStatus(context.Background(), meta.SharedDBStatusPending, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("managed pools = %d, want 1", len(rows))
	}
	wantProvisioningKey := sharedDBProvisioningKey(tenant.CredentialProvisionRequest{PublicKey: "shared-public"})
	if !bytes.Equal(rows[0].ProvisioningKey, wantProvisioningKey) {
		t.Fatalf("managed pool provisioning key = %x, want server-owned shared credential key %x",
			rows[0].ProvisioningKey, wantProvisioningKey)
	}
	if got := prov.iamCalls.Load(); got != 0 {
		t.Fatalf("identity lookups = %d, want 0", got)
	}
}

func TestSharedTenantPoolRefillStagesDedicatedCapacityWithoutIdentityLookup(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	poolManager := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer poolManager.Close()
	poolManager.SetMetaStore(metaStore)
	passwordCipher, err := poolManager.Encrypt(context.Background(), []byte("shared-pass"))
	if err != nil {
		t.Fatal(err)
	}
	existingDBID, err := metaStore.RegisterSharedDB(context.Background(), &meta.SharedDB{
		TiDBCloudOrganizationID: "org-shared-existing", Host: "shared.example.com", Port: 4000,
		User: "root", PasswordCipher: passwordCipher, Name: "tidbcloud_fs", MaxTenants: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := metaStore.CreateTenantPool(context.Background(), &meta.TenantPool{
		PoolID: "pool-shared-existing", OrganizationID: "org-shared-existing", Size: 50,
		Status: meta.TenantPoolActive, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative, cloudProvider: "aws", region: "us-east-1",
		identityOrg: "org-shared-existing"}
	srv := &Server{meta: metaStore, pool: poolManager, provisioner: prov,
		tidbCloudRBACCache: newTiDBCloudRBACCache(time.Hour)}
	results, err := srv.createFreeSharedPoolTenants(context.Background(), "pool-shared-existing", 50,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"}, nil)
	if err != nil {
		t.Fatalf("createFreeSharedPoolTenants: %v", err)
	}
	if len(results) != 50 {
		t.Fatalf("results = %d, want 50", len(results))
	}
	if got := prov.sharedPoolBatchCalls.Load(); got != 1 {
		t.Fatalf("physical batch calls = %d, want one dedicated refill wave", got)
	}
	if got := prov.iamCalls.Load(); got != 0 {
		t.Fatalf("identity lookups = %d, want 0 when tenant pool organization is known", got)
	}
	var existingTenantCount, physicalPoolCount int
	if err := metaStore.DB().QueryRowContext(context.Background(),
		`SELECT tenant_count FROM db_pool WHERE db_id = ?`, existingDBID).Scan(&existingTenantCount); err != nil {
		t.Fatal(err)
	}
	if err := metaStore.DB().QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM db_pool WHERE org_id = ?`, "org-shared-existing").Scan(&physicalPoolCount); err != nil {
		t.Fatal(err)
	}
	if existingTenantCount != 0 || physicalPoolCount != 2 {
		t.Fatalf("existing tenant_count=%d physical pools=%d, want dedicated staged pool and untouched existing capacity",
			existingTenantCount, physicalPoolCount)
	}
}

func TestSharedTenantPoolDefensivelyReportsProvisioningWhenBatchMetadataComplete(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	poolManager := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer poolManager.Close()
	poolManager.SetMetaStore(metaStore)
	prov := &fakeProvisioner{
		provider:      tenant.ProviderTiDBCloudNative,
		cloudProvider: "aws",
		region:        "us-east-1",
		managedClusters: []tenant.CloudClusterInfo{{
			OrganizationID: "org-shared-defensive",
		}},
		sharedPoolResults: []*tenant.SharedDBPoolInfo{{
			ClusterID: "cluster-ready", Host: "127.0.0.1", Port: 4000,
			Username: "root", DBName: "tidbcloud_fs",
		}},
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		t.Fatal(err)
	}
	srv := NewWithConfig(Config{Meta: metaStore, Pool: poolManager, Provisioner: prov,
		DefaultTenantProvider: tenant.ProviderTiDBCloudNativeShared, TokenSecret: secret})
	defer srv.Close()
	now := time.Now().UTC()
	if err := metaStore.CreateTenantPool(context.Background(), &meta.TenantPool{
		PoolID: "pool-shared-defensive", OrganizationID: "org-shared-defensive", Size: 1,
		Status: meta.TenantPoolActive, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	results, err := srv.createFreePoolTenants(context.Background(), "pool-shared-defensive", 1,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"}, nil)
	if err != nil {
		t.Fatalf("createFreePoolTenants: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results = %d, want 1", len(results))
	}
	if results[0].Status != meta.TenantProvisioning {
		t.Fatalf("result status = %q, want provisioning", results[0].Status)
	}
	got, err := metaStore.GetTenant(context.Background(), results[0].TenantID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != meta.TenantProvisioning {
		t.Fatalf("persisted tenant status = %q, want provisioning", got.Status)
	}
}

func TestAdminTenantPoolCreateCleansPartialSharedMembersOnBatchFailure(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	poolManager := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer poolManager.Close()
	poolManager.SetMetaStore(metaStore)
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative, cloudProvider: "aws", region: "us-east-1",
		managedClusters:   []tenant.CloudClusterInfo{{OrganizationID: "org-shared-cleanup"}},
		sharedPoolResults: []*tenant.SharedDBPoolInfo{{DBPoolID: 999999, ClusterID: "cluster-unknown", OrganizationID: "org-shared-cleanup"}}}
	secret := make([]byte, 32)
	_, _ = rand.Read(secret)
	srv := NewWithConfig(Config{Meta: metaStore, Pool: poolManager, Provisioner: prov,
		DefaultTenantProvider: tenant.ProviderTiDBCloudNativeShared, TokenSecret: secret})
	defer srv.Close()
	ts := httptest.NewServer(srv)
	defer ts.Close()

	resp := postJSON(t, ts.URL+"/v1/admin/tenant-pool", map[string]any{
		"public_key": "public", "private_key": "private", "pool_size": 1,
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadGateway {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want %d: %s", resp.StatusCode, http.StatusBadGateway, body)
	}
	var memberships, placements, tenantCount int
	if err := metaStore.DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM tenant_pool_memberships
		WHERE tidbcloud_organization_id = ?`, "org-shared-cleanup").Scan(&memberships); err != nil {
		t.Fatal(err)
	}
	if err := metaStore.DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM tenant_placements p
		JOIN db_pool d ON d.db_id = p.db_id WHERE d.org_id = ?`, "org-shared-cleanup").Scan(&placements); err != nil {
		t.Fatal(err)
	}
	if err := metaStore.DB().QueryRowContext(context.Background(), `SELECT COALESCE(SUM(tenant_count), 0)
		FROM db_pool WHERE org_id = ?`, "org-shared-cleanup").Scan(&tenantCount); err != nil {
		t.Fatal(err)
	}
	if memberships != 0 || placements != 0 || tenantCount != 0 {
		t.Fatalf("partial shared cleanup left memberships=%d placements=%d tenant_count=%d", memberships, placements, tenantCount)
	}
}

func TestDeleteFreeSharedPoolTenantSkipsPurgeWhenDBPoolIsUnready(t *testing.T) {
	rt := newQuotaRuntimeWithOptions(t, tenant.ProviderTiDBCloudNative, quotaRuntimeOptions{
		defaultTenantProvider: tenant.ProviderTiDBCloudNativeShared,
	})
	ctx := context.Background()
	now := time.Now().UTC()
	if err := rt.meta.CreateTenantPool(ctx, &meta.TenantPool{
		PoolID: "pool-unready-delete", OrganizationID: "org-unready-delete", Size: 1,
		Status: meta.TenantPoolActive, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	tenantID := "shared-unready-delete"
	if err := rt.server.insertPendingPoolTenant(ctx, tenantID, tenant.ProviderTiDBCloudNativeShared, now); err != nil {
		t.Fatal(err)
	}
	if err := rt.server.materializeSharedTenantQuota(ctx, tenantID, provisionTenantOptions{}); err != nil {
		t.Fatal(err)
	}
	passwordCipher, err := rt.server.pool.Encrypt(ctx, []byte("root-pass"))
	if err != nil {
		t.Fatal(err)
	}
	spendingTarget := meta.MaxTiDBCloudSpendingLimit
	dbID, err := rt.meta.CreateManagedSharedDBPool(ctx, &meta.SharedDB{
		TiDBCloudOrganizationID: "org-unready-delete", ProvisioningKey: make([]byte, 32),
		CloudProvider: "aws", Region: "us-east-1",
		MaxTenants: 100, SpendingLimit: &spendingTarget, PasswordCipher: passwordCipher, Name: "tidbcloud_fs",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.UpdateManagedSharedDBPoolCloudResult(ctx, &meta.SharedDB{
		ID: dbID, TiDBCloudOrganizationID: "org-unready-delete", ClusterID: "cluster-unready-delete",
		PasswordCipher: passwordCipher, Name: "tidbcloud_fs", TLSMode: "true",
	}); err != nil {
		t.Fatal(err)
	}
	fsID, err := rt.meta.EnsureFsID(ctx, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.CompleteSharedTenantPoolMember(ctx, tenantID, tenant.ProviderTiDBCloudNativeShared,
		&meta.TenantPlacement{FsID: fsID, DbID: dbID, Placement: meta.PlacementShared,
			SchemaShape: meta.SchemaShapeShared, Status: meta.SharedDBStatusActive},
		&meta.TenantPoolMembership{TenantID: tenantID, TiDBCloudOrganizationID: "org-unready-delete",
			PoolID: "pool-unready-delete", PoolStatus: meta.TenantPoolBindingFree, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	tenantRow, err := rt.meta.GetTenant(ctx, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.meta.MarkFreeSharedTenantPoolTenantDeleting(ctx, tenantID, tenantRow.Status); err != nil {
		t.Fatal(err)
	}
	if err := rt.server.deleteFreeSharedPoolTenant(ctx, tenantRow); err != nil {
		t.Fatalf("deleteFreeSharedPoolTenant: %v", err)
	}
	if _, err := rt.meta.GetTenantPlacement(ctx, fsID); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("placement lookup error = %v, want ErrNotFound", err)
	}
	if _, err := rt.meta.GetTenantPoolMembership(ctx, tenantID); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("membership lookup error = %v, want ErrNotFound", err)
	}
	dbPool, err := rt.meta.GetSharedDB(ctx, dbID)
	if err != nil {
		t.Fatal(err)
	}
	if dbPool.TenantCount != 0 {
		t.Fatalf("db pool tenant_count = %d, want 0", dbPool.TenantCount)
	}
}

func TestAdminTenantPoolMetadataResumeResultRank(t *testing.T) {
	ordered := []string{"ok", "canceled", "deadline_exceeded", "bad_conn", "error", "unknown"}
	for i := 1; i < len(ordered); i++ {
		prev := ordered[i-1]
		next := ordered[i]
		if adminTenantPoolMetadataResumeResultRank(next) < adminTenantPoolMetadataResumeResultRank(prev) {
			t.Fatalf("rank(%q) < rank(%q)", next, prev)
		}
	}
}

func TestAdminTenantPoolCreateUsesPrivateEndpointDBTLS(t *testing.T) {
	t.Setenv("DRIVE9_TIDBCLOUD_NATIVE_USE_PRIVATE_ENDPOINT", "1")
	rt, schemaInitRecorder := newAdminTenantPoolRuntime(t)
	rt.prov.listPages = []*tenant.ManagedClusterListResult{
		{},
		{Clusters: []tenant.CloudClusterInfo{{OrganizationID: "org-1"}}},
	}
	ts := httptest.NewServer(rt.server)
	t.Cleanup(ts.Close)

	resp := postJSON(t, ts.URL+"/v1/admin/tenant-pool", map[string]any{
		"public_key":  "public-1",
		"private_key": "private-1",
		"pool_size":   1,
	}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body=%s", resp.StatusCode, body)
	}

	deadline := time.Now().Add(5 * time.Second)
	var rows []meta.TenantWithTiDBCloudOrgBinding
	for {
		var err error
		rows, err = rt.meta.ListTenantPoolFreeSlotsForDelete(context.Background(), "org-1", false, 10)
		if err != nil {
			t.Fatalf("list free slots: %v", err)
		}
		if len(rows) == 1 && rows[0].Tenant.DBHost == "db.example.com" && schemaInitRecorder.schemaInitCalls.Load() >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("free slots = %d, schema init calls=%d", len(rows), schemaInitRecorder.schemaInitCalls.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if rows[0].Tenant.DBTLS {
		t.Fatalf("pool tenant DBTLS = true, want false for private endpoint")
	}
	assertTenantUsesPrivateEndpointTLS(t, rows[0].Tenant)
	assertSchemaInitUsesPrivateEndpointTLS(t, schemaInitRecorder.lastSchemaInitDSNSnapshot())
}

func TestTenantPoolMetadataResumeUsesPrivateEndpointDBTLS(t *testing.T) {
	t.Setenv("DRIVE9_TIDBCLOUD_NATIVE_USE_PRIVATE_ENDPOINT", "1")
	rt, schemaInitRecorder := newAdminTenantPoolRuntime(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if err := rt.meta.CreateTenantPool(ctx, &meta.TenantPool{
		PoolID:         "pool-1",
		OrganizationID: "org-1",
		Size:           1,
		Status:         meta.TenantPoolActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("create pool: %v", err)
	}
	passCipher, err := rt.server.pool.Encrypt(ctx, []byte("pool-pass"))
	if err != nil {
		t.Fatalf("encrypt password: %v", err)
	}
	tenantID := "pool-private-resume-1"
	clusterID := "cluster-private-resume-1"
	if err := rt.meta.InsertTenant(ctx, &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantPending,
		DBPasswordCipher: passCipher,
		DBName:           "tidbcloud_fs",
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBCloudNative,
		ClusterID:        clusterID,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatalf("insert tenant: %v", err)
	}
	if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
		TenantID:       tenantID,
		OrganizationID: "org-1",
		ClusterID:      clusterID,
		PoolID:         "pool-1",
		PoolStatus:     meta.TenantPoolBindingFree,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("upsert binding: %v", err)
	}

	rt.server.startPoolClustersMetadataResume(ctx, "pool-1", []*tenant.ClusterInfo{{
		TenantID:       tenantID,
		ClusterID:      clusterID,
		OrganizationID: "org-1",
		Password:       "pool-pass",
		DBName:         "tidbcloud_fs",
		Provider:       tenant.ProviderTiDBCloudNative,
	}}, tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})

	deadline := time.Now().Add(5 * time.Second)
	var got *meta.Tenant
	for {
		got, err = rt.meta.GetTenant(ctx, tenantID)
		if err != nil {
			t.Fatalf("get tenant: %v", err)
		}
		if rt.prov.metadataBatchWaitCalls.Load() >= 1 && schemaInitRecorder.schemaInitCalls.Load() >= 1 && got.DBHost == "db.example.com" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant after resume = status %s host %q, metadata waits=%d, schema init calls=%d", got.Status, got.DBHost, rt.prov.metadataBatchWaitCalls.Load(), schemaInitRecorder.schemaInitCalls.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got.DBTLS {
		t.Fatalf("resumed pool tenant DBTLS = true, want false for private endpoint")
	}
	assertTenantUsesPrivateEndpointTLS(t, *got)
	assertSchemaInitUsesPrivateEndpointTLS(t, schemaInitRecorder.lastSchemaInitDSNSnapshot())
}

func TestTenantPoolMetadataResumePersistsAfterWaitDeadline(t *testing.T) {
	oldWaitTimeout := tenantPoolMetadataResumeWaitTimeout
	tenantPoolMetadataResumeWaitTimeout = 20 * time.Millisecond
	t.Cleanup(func() {
		tenantPoolMetadataResumeWaitTimeout = oldWaitTimeout
	})

	var waiter *deadlineMetadataResumeProvisioner
	rt, schemaInitRecorder := newAdminTenantPoolRuntimeWithProvisioner(t, func(recorder *adminTenantPoolSchemaInitRecorder) tenant.Provisioner {
		waiter = &deadlineMetadataResumeProvisioner{
			adminTenantPoolSchemaInitRecorder: recorder,
			waitStarted:                       make(chan struct{}),
		}
		return waiter
	})

	ctx := context.Background()
	now := time.Now().UTC()
	if err := rt.meta.CreateTenantPool(ctx, &meta.TenantPool{
		PoolID:         "pool-1",
		OrganizationID: "org-1",
		Size:           1,
		Status:         meta.TenantPoolActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("create pool: %v", err)
	}
	passCipher, err := rt.server.pool.Encrypt(ctx, []byte("pool-pass"))
	if err != nil {
		t.Fatalf("encrypt password: %v", err)
	}
	tenantID := "pool-deadline-resume-1"
	clusterID := "cluster-deadline-resume-1"
	if err := rt.meta.InsertTenant(ctx, &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantPending,
		DBPasswordCipher: passCipher,
		DBName:           "tidbcloud_fs",
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBCloudNative,
		ClusterID:        clusterID,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatalf("insert tenant: %v", err)
	}
	if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
		TenantID:       tenantID,
		OrganizationID: "org-1",
		ClusterID:      clusterID,
		PoolID:         "pool-1",
		PoolStatus:     meta.TenantPoolBindingFree,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("upsert binding: %v", err)
	}

	rt.server.startPoolClustersMetadataResume(ctx, "pool-1", []*tenant.ClusterInfo{{
		TenantID:       tenantID,
		ClusterID:      clusterID,
		OrganizationID: "org-1",
		Password:       "pool-pass",
		DBName:         "tidbcloud_fs",
		Provider:       tenant.ProviderTiDBCloudNative,
	}}, tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})

	select {
	case <-waiter.waitStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("metadata resume did not start")
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		got, err := rt.meta.GetTenant(ctx, tenantID)
		if err != nil {
			t.Fatalf("get tenant: %v", err)
		}
		if rt.prov.metadataBatchWaitCalls.Load() >= 1 && schemaInitRecorder.schemaInitCalls.Load() >= 1 && got.DBHost == "db.example.com" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant after deadline resume = status %s host %q, metadata waits=%d, schema init calls=%d", got.Status, got.DBHost, rt.prov.metadataBatchWaitCalls.Load(), schemaInitRecorder.schemaInitCalls.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestTenantPoolMetadataResumePersistsReadyGroupBeforeSlowGroup(t *testing.T) {
	oldGroupSize := tenantPoolMetadataResumeGroupSize
	tenantPoolMetadataResumeGroupSize = 1
	t.Cleanup(func() {
		tenantPoolMetadataResumeGroupSize = oldGroupSize
	})

	var waiter *groupStreamingMetadataResumeProvisioner
	rt, _ := newAdminTenantPoolRuntimeWithProvisioner(t, func(recorder *adminTenantPoolSchemaInitRecorder) tenant.Provisioner {
		waiter = &groupStreamingMetadataResumeProvisioner{
			adminTenantPoolSchemaInitRecorder: recorder,
			slowTenantID:                      "pool-stream-resume-slow",
			slowStarted:                       make(chan struct{}),
			releaseSlow:                       make(chan struct{}),
		}
		return waiter
	})
	var releaseSlowOnce sync.Once
	t.Cleanup(func() {
		releaseSlowOnce.Do(func() { close(waiter.releaseSlow) })
	})
	ctx := context.Background()
	now := time.Now().UTC()
	if err := rt.meta.CreateTenantPool(ctx, &meta.TenantPool{
		PoolID:         "pool-1",
		OrganizationID: "org-1",
		Size:           2,
		Status:         meta.TenantPoolActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("create pool: %v", err)
	}
	makePending := func(tenantID, clusterID string) *tenant.ClusterInfo {
		t.Helper()
		passCipher, err := rt.server.pool.Encrypt(ctx, []byte("pool-pass"))
		if err != nil {
			t.Fatalf("encrypt password: %v", err)
		}
		if err := rt.meta.InsertTenant(ctx, &meta.Tenant{
			ID:               tenantID,
			Status:           meta.TenantPending,
			DBPasswordCipher: passCipher,
			DBName:           "tidbcloud_fs",
			DBTLS:            true,
			Provider:         tenant.ProviderTiDBCloudNative,
			ClusterID:        clusterID,
			SchemaVersion:    1,
			CreatedAt:        now,
			UpdatedAt:        now,
		}); err != nil {
			t.Fatalf("insert tenant %s: %v", tenantID, err)
		}
		if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
			TenantID:       tenantID,
			OrganizationID: "org-1",
			ClusterID:      clusterID,
			PoolID:         "pool-1",
			PoolStatus:     meta.TenantPoolBindingFree,
			CreatedAt:      now,
			UpdatedAt:      now,
		}); err != nil {
			t.Fatalf("upsert binding %s: %v", tenantID, err)
		}
		return &tenant.ClusterInfo{
			TenantID:       tenantID,
			ClusterID:      clusterID,
			OrganizationID: "org-1",
			Password:       "pool-pass",
			DBName:         "tidbcloud_fs",
			Provider:       tenant.ProviderTiDBCloudNative,
		}
	}
	slow := makePending(waiter.slowTenantID, "cluster-stream-resume-slow")
	fast := makePending("pool-stream-resume-fast", "cluster-stream-resume-fast")

	rt.server.startPoolClustersMetadataResume(ctx, "pool-1", []*tenant.ClusterInfo{slow, fast}, tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})

	select {
	case <-waiter.slowStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("slow metadata resume group did not start")
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		got, err := rt.meta.GetTenant(ctx, fast.TenantID)
		if err != nil {
			t.Fatalf("get fast tenant: %v", err)
		}
		if got.DBHost == "db.example.com" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fast tenant was not persisted while slow group was blocked: status %s host %q", got.Status, got.DBHost)
		}
		time.Sleep(10 * time.Millisecond)
	}
	slowTenant, err := rt.meta.GetTenant(ctx, slow.TenantID)
	if err != nil {
		t.Fatalf("get slow tenant: %v", err)
	}
	if slowTenant.DBHost != "" {
		t.Fatalf("slow tenant host = %q before slow group release, want empty", slowTenant.DBHost)
	}

	releaseSlowOnce.Do(func() { close(waiter.releaseSlow) })
	deadline = time.Now().Add(5 * time.Second)
	for {
		got, err := rt.meta.GetTenant(ctx, slow.TenantID)
		if err != nil {
			t.Fatalf("get slow tenant: %v", err)
		}
		if got.DBHost == "db.example.com" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("slow tenant was not persisted after release: status %s host %q", got.Status, got.DBHost)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestTenantPoolMetadataResumePersistContextPreservesServerCancellation(t *testing.T) {
	srv := NewWithConfig(Config{})
	ctx, cancel := srv.tenantPoolMetadataResumePersistContext(context.Background())
	defer cancel()

	srv.Close()

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("persist context was not canceled by server close")
	}
}

func TestAdminTenantPoolReplenishSkipsAtFreeWatermark(t *testing.T) {
	rt, _ := newAdminTenantPoolRuntime(t)
	ctx := context.Background()
	now := time.Now().UTC()
	pool := &meta.TenantPool{
		PoolID:         "pool-watermark-skip",
		OrganizationID: "org-1",
		Size:           10,
		Status:         meta.TenantPoolActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := rt.meta.CreateTenantPool(ctx, pool); err != nil {
		t.Fatalf("create pool: %v", err)
	}
	for i := 1; i <= 8; i++ {
		insertAdminPoolFreeTenant(t, rt, pool.PoolID, pool.OrganizationID, i)
	}
	lockHeld := make(chan struct{})
	releaseLock := make(chan struct{})
	lockDone := make(chan error, 1)
	go func() {
		lockDone <- rt.meta.WithTenantPoolLock(ctx, pool.PoolID, func(context.Context) error {
			close(lockHeld)
			<-releaseLock
			return nil
		})
	}()
	select {
	case <-lockHeld:
	case err := <-lockDone:
		t.Fatalf("hold tenant pool lock: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out acquiring tenant pool lock for test")
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseLock) }) }
	t.Cleanup(func() {
		release()
		<-lockDone
	})

	rt.server.replenishTenantPoolAsync(ctx, pool, tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	workerDone := make(chan struct{})
	go func() {
		rt.server.forkWorkerWG.Wait()
		close(workerDone)
	}()
	select {
	case <-workerDone:
	case <-time.After(time.Second):
		release()
		<-workerDone
		t.Fatal("above-watermark replenish waited for the MySQL tenant pool lock")
	}
	release()

	if got := rt.prov.batchPoolCalls.Load(); got != 0 {
		t.Fatalf("batch pool calls = %d, want 0", got)
	}
	free, err := rt.meta.CountFreeTenantPoolBindings(ctx, pool.OrganizationID)
	if err != nil {
		t.Fatalf("count free: %v", err)
	}
	if free != 8 {
		t.Fatalf("free size = %d, want 8", free)
	}
}

func TestTenantPoolReplenishmentCoalescesConcurrentTriggersByPool(t *testing.T) {
	s := &Server{}
	now := time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)

	if !s.beginTenantPoolReplenishmentAt("pool-1", now) {
		t.Fatal("first trigger was not accepted")
	}
	if s.beginTenantPoolReplenishmentAt("pool-1", now) {
		t.Fatal("concurrent trigger for the same pool was accepted")
	}
	if !s.beginTenantPoolReplenishmentAt("pool-2", now) {
		t.Fatal("trigger for a different pool was not accepted")
	}

	s.finishTenantPoolReplenishmentAt("pool-1", now)
	if s.beginTenantPoolReplenishmentAt("pool-1", now.Add(tenantPoolReplenishMinInterval-time.Nanosecond)) {
		t.Fatal("trigger was accepted during the per-pool minimum interval")
	}
	if !s.beginTenantPoolReplenishmentAt("pool-1", now.Add(tenantPoolReplenishMinInterval)) {
		t.Fatal("trigger was not accepted after the per-pool minimum interval")
	}
}

func TestTenantPoolReplenishmentRerunsCoalescedTriggerAfterWorkerFinishes(t *testing.T) {
	s := &Server{}
	now := time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)

	first := s.requestTenantPoolReplenishmentAt("pool-1", now)
	if !first.start || first.scheduleAfter != 0 {
		t.Fatalf("first decision = %+v, want immediate start", first)
	}
	coalesced := s.requestTenantPoolReplenishmentAt("pool-1", now)
	if coalesced.start || coalesced.scheduleAfter != 0 {
		t.Fatalf("running decision = %+v, want coalesced trigger", coalesced)
	}
	finished := s.finishTenantPoolReplenishmentAt("pool-1", now)
	if finished.start || finished.scheduleAfter != tenantPoolReplenishMinInterval {
		t.Fatalf("finish decision = %+v, want one delayed rerun", finished)
	}
	cooldown := s.requestTenantPoolReplenishmentAt("pool-1", now.Add(tenantPoolReplenishMinInterval/2))
	if cooldown.start || cooldown.scheduleAfter != 0 {
		t.Fatalf("cooldown decision = %+v, want existing delayed rerun to absorb trigger", cooldown)
	}
	rerun := s.requestTenantPoolReplenishmentAt("pool-1", now.Add(tenantPoolReplenishMinInterval))
	if !rerun.start || rerun.scheduleAfter != 0 {
		t.Fatalf("rerun decision = %+v, want immediate start after cooldown", rerun)
	}
}

func TestTenantPoolReplenishmentSchedulesCoalescedRerunWhenWorkStartFails(t *testing.T) {
	s := &Server{}
	pool := &meta.TenantPool{PoolID: "pool-work-start-failed", OrganizationID: "org-1", Size: 1}
	timerStarts := 0
	workStarter := func(context.Context, func(context.Context)) bool {
		decision := s.requestTenantPoolReplenishmentAt(pool.PoolID, time.Now())
		if decision.start || decision.scheduleAfter != 0 {
			t.Fatalf("coalesced decision = %+v, want no immediate or delayed start", decision)
		}
		return false
	}
	timerStarter := func(context.Context, func(context.Context)) bool {
		timerStarts++
		return true
	}

	s.replenishTenantPoolAsyncWithStarters(context.Background(), pool, tenant.CredentialProvisionRequest{}, workStarter, timerStarter)

	if timerStarts != 1 {
		t.Fatalf("delayed rerun starts = %d, want 1 after work start failure", timerStarts)
	}
}

func TestTenantPoolReplenishmentRetriesSchedulingWhenTimerStartFails(t *testing.T) {
	s := &Server{}
	pool := &meta.TenantPool{PoolID: "pool-timer-start-failed", OrganizationID: "org-1", Size: 1}
	value, _ := s.tenantPoolReplenishJobs.LoadOrStore(pool.PoolID, &tenantPoolWorkGate{})
	gate := value.(*tenantPoolWorkGate)
	gate.mu.Lock()
	gate.rerun = true
	gate.nextAllowed = time.Now().Add(time.Minute)
	gate.mu.Unlock()
	timerStarts := 0
	timerStarter := func(context.Context, func(context.Context)) bool {
		timerStarts++
		return false
	}
	workStarter := func(context.Context, func(context.Context)) bool {
		t.Fatal("cooldown trigger unexpectedly started replenish work")
		return false
	}

	s.replenishTenantPoolAsyncWithStarters(context.Background(), pool, tenant.CredentialProvisionRequest{}, workStarter, timerStarter)
	s.replenishTenantPoolAsyncWithStarters(context.Background(), pool, tenant.CredentialProvisionRequest{}, workStarter, timerStarter)

	if timerStarts != 2 {
		t.Fatalf("timer start attempts = %d, want 2 after the first start failed", timerStarts)
	}
}

func TestLeaderTenantPoolReplenishmentTimerDoesNotConsumeWorkSlot(t *testing.T) {
	s := &Server{}
	pool := &meta.TenantPool{PoolID: "pool-leader-rerun", OrganizationID: "org-leader-rerun", Size: 1}
	value, _ := s.tenantPoolReplenishJobs.LoadOrStore(pool.PoolID, &tenantPoolWorkGate{})
	gate := value.(*tenantPoolWorkGate)
	gate.mu.Lock()
	gate.rerun = true
	gate.scheduled = true
	gate.nextAllowed = time.Now()
	gate.mu.Unlock()

	workStarted := make(chan struct{}, 1)
	workStarter := func(context.Context, func(context.Context)) bool {
		workStarted <- struct{}{}
		return true
	}
	timerStarted := make(chan struct{}, 1)
	timerStarter := func(ctx context.Context, fn func(context.Context)) bool {
		timerStarted <- struct{}{}
		go fn(ctx)
		return true
	}

	s.scheduleTenantPoolReplenishment(context.Background(), pool, tenant.CredentialProvisionRequest{}, 0, workStarter, timerStarter)
	select {
	case <-timerStarted:
	case <-time.After(time.Second):
		t.Fatal("leader rerun timer did not start")
	}
	select {
	case <-workStarted:
	case <-time.After(time.Second):
		t.Fatal("leader rerun did not reacquire a refill work slot")
	}
}

func TestTenantPoolPendingResumeScanCooldownAfterEmptyResult(t *testing.T) {
	s := &Server{}
	now := time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)

	if !s.beginTenantPoolResumeScanAt("pool-1", now) {
		t.Fatal("first pending resume scan was not accepted")
	}
	if s.beginTenantPoolResumeScanAt("pool-1", now) {
		t.Fatal("concurrent pending resume scan was accepted")
	}
	s.finishTenantPoolResumeScanAt("pool-1", now, true)
	if s.beginTenantPoolResumeScanAt("pool-1", now.Add(tenantPoolPendingResumeEmptyInterval-time.Nanosecond)) {
		t.Fatal("empty pending resume scan was retried during cooldown")
	}
	if !s.beginTenantPoolResumeScanAt("pool-1", now.Add(tenantPoolPendingResumeEmptyInterval)) {
		t.Fatal("pending resume scan was not accepted after cooldown")
	}
}

func TestTenantPoolPendingResumeRequestsRerunBeforeListing(t *testing.T) {
	s := &Server{}
	job := &tenantPoolResumeJob{}
	s.tenantPoolResumeJobs.Store("pool-1", job)

	s.resumePendingTenantPoolAsync(context.Background(), &meta.TenantPool{
		PoolID:         "pool-1",
		OrganizationID: "org-1",
		Size:           10,
	}, tenant.CredentialProvisionRequest{})

	if !job.rerun.Load() {
		t.Fatal("active metadata resume job did not receive a rerun request")
	}
}

func TestAdminTenantPoolReplenishBatchesBelowFreeWatermark(t *testing.T) {
	rt, _ := newAdminTenantPoolRuntime(t)
	ctx := context.Background()
	now := time.Now().UTC()
	pool := &meta.TenantPool{
		PoolID:         "pool-watermark-refill",
		OrganizationID: "org-1",
		Size:           10,
		Status:         meta.TenantPoolActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := rt.meta.CreateTenantPool(ctx, pool); err != nil {
		t.Fatalf("create pool: %v", err)
	}
	for i := 1; i <= 7; i++ {
		insertAdminPoolFreeTenant(t, rt, pool.PoolID, pool.OrganizationID, i)
	}

	rt.server.replenishTenantPoolAsync(ctx, pool, tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	rt.server.forkWorkerWG.Wait()

	if got := rt.prov.batchPoolCalls.Load(); got != 1 {
		t.Fatalf("batch pool calls = %d, want 1", got)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		free, err := rt.meta.CountFreeTenantPoolBindings(ctx, pool.OrganizationID)
		if err != nil {
			t.Fatalf("count free: %v", err)
		}
		if free == 10 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("free size = %d, want 10 after refill", free)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestAdminTenantPoolReplenishSubmitsLargeRefillAsSingleWave(t *testing.T) {
	oldRetryWindow := schemaInitRetryWindow
	schemaInitRetryWindow = 100 * time.Millisecond
	t.Cleanup(func() { schemaInitRetryWindow = oldRetryWindow })
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	poolManager := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer poolManager.Close()
	poolManager.SetMetaStore(metaStore)
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative, cloudProvider: "aws", region: "us-east-1"}
	srv := NewWithConfig(Config{Meta: metaStore, Pool: poolManager, Provisioner: prov,
		DefaultTenantProvider: tenant.ProviderTiDBCloudNativeShared, SharedDBMaxTenants: 100,
		TokenSecret: make([]byte, 32), Leader: newFollowerLeaderManager(t, metaStore)})
	defer srv.Close()
	// Make the complete three-pool refill wave visible as one Cloud batch.
	srv.managedSharedDBCloudBatchSize = 250
	ctx := context.Background()
	now := time.Now().UTC()
	pool := &meta.TenantPool{
		PoolID:         "pool-large-refill",
		OrganizationID: "org-1",
		Size:           250,
		Status:         meta.TenantPoolActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := metaStore.CreateTenantPool(ctx, pool); err != nil {
		t.Fatalf("create pool: %v", err)
	}

	lockHeld := make(chan struct{})
	releaseLock := make(chan struct{})
	lockDone := make(chan error, 1)
	go func() {
		lockDone <- metaStore.WithTenantPoolLock(ctx, pool.PoolID, func(context.Context) error {
			close(lockHeld)
			<-releaseLock
			return nil
		})
	}()
	select {
	case <-lockHeld:
	case err := <-lockDone:
		t.Fatalf("hold tenant pool lock: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out acquiring tenant pool lock for test")
	}
	t.Cleanup(func() {
		close(releaseLock)
		if err := <-lockDone; err != nil {
			t.Errorf("release tenant pool lock: %v", err)
		}
	})

	srv.replenishTenantPoolAsync(ctx, pool, tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	deadline := time.Now().Add(10 * time.Second)
	for {
		if got := prov.sharedPoolBatchCalls.Load(); got >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("shared cloud batch calls = %d, want one complete refill wave", prov.sharedPoolBatchCalls.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		free, err := metaStore.CountTenantPoolFreeSlots(ctx, pool.OrganizationID)
		if err != nil {
			t.Fatalf("count free: %v", err)
		}
		if free == pool.Size {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("free size = %d, want %d after refill", free, pool.Size)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := prov.sharedPoolBatchCalls.Load(); got != 1 {
		t.Fatalf("shared cloud batch calls = %d, want 1", got)
	}
	if got := prov.sharedPoolBatchMembers.Load(); got != 3 {
		t.Fatalf("shared cloud batch members = %d, want 3 physical pools", got)
	}
}

func TestManagedSharedDBCloudBatchRequestsAreGloballyBounded(t *testing.T) {
	const wantMaxConcurrentBatches = 5
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	poolManager := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	t.Cleanup(poolManager.Close)
	poolManager.SetMetaStore(metaStore)
	batchStarted := make(chan struct{}, 7)
	batchRelease := make(chan struct{}, 7)
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative, cloudProvider: "aws", region: "us-east-1",
		sharedPoolBatchStarted: batchStarted, sharedPoolBatchRelease: batchRelease}
	srv := &Server{meta: metaStore, pool: poolManager, provisioner: prov, sharedDBMaxTenants: 1,
		managedSharedDBCloudBatchSize: 1}
	ctx := context.Background()
	cred := tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"}
	dbIDs := make([]int64, 0, 7)
	for range 7 {
		row, err := srv.createManagedSharedDBPlan(ctx, "org-cloud-batch-bound", sharedDBProvisioningKey(cred))
		if err != nil {
			t.Fatal(err)
		}
		dbIDs = append(dbIDs, row.ID)
	}
	done := make(chan error, 1)
	go func() {
		_, createErr := srv.provisionManagedSharedDBPoolsBatchWithCredentials(ctx, dbIDs, cred)
		done <- createErr
	}()
	for i := 0; i < wantMaxConcurrentBatches; i++ {
		select {
		case <-batchStarted:
		case <-time.After(2 * time.Second):
			t.Fatalf("Cloud batch %d did not start", i+1)
		}
	}
	select {
	case <-batchStarted:
		t.Fatalf("more than %d Cloud batch requests started concurrently", wantMaxConcurrentBatches)
	case <-time.After(150 * time.Millisecond):
	}
	batchRelease <- struct{}{}
	select {
	case <-batchStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("next Cloud batch did not start after a request slot was released")
	}
	close(batchRelease)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("provision managed shared DB batches: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Cloud batch provisioning did not finish")
	}
}

func TestAdminTenantPoolReplenishContinuesPastWatermarkToFillSlots(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	poolManager := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer poolManager.Close()
	poolManager.SetMetaStore(metaStore)
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative, cloudProvider: "aws", region: "us-east-1"}
	srv := NewWithConfig(Config{Meta: metaStore, Pool: poolManager, Provisioner: prov,
		DefaultTenantProvider: tenant.ProviderTiDBCloudNativeShared, SharedDBMaxTenants: 200,
		TokenSecret: make([]byte, 32), Leader: newFollowerLeaderManager(t, metaStore)})
	defer srv.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	pool := &meta.TenantPool{PoolID: "pool-watermark-shared", OrganizationID: "org-watermark-shared",
		Size: 250, Status: meta.TenantPoolActive, CreatedAt: now, UpdatedAt: now}
	if err := metaStore.CreateTenantPool(ctx, pool); err != nil {
		t.Fatalf("create pool: %v", err)
	}
	passwordCipher, err := poolManager.Encrypt(ctx, []byte("shared-pass"))
	if err != nil {
		t.Fatalf("encrypt shared password: %v", err)
	}
	if _, err := metaStore.RegisterSharedDB(ctx, &meta.SharedDB{
		TiDBCloudOrganizationID: pool.OrganizationID,
		Host:                    "shared.example.com",
		Port:                    4000,
		User:                    "root",
		PasswordCipher:          passwordCipher,
		Name:                    "tidbcloud_fs",
		MaxTenants:              200,
	}); err != nil {
		t.Fatalf("register shared db: %v", err)
	}
	if _, err := srv.createFreeSharedPoolTenants(ctx, pool.PoolID, 100,
		tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}, nil); err != nil {
		t.Fatalf("seed shared pool: %v", err)
	}

	srv.replenishTenantPoolAsync(ctx, pool, tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	deadline := time.Now().Add(10 * time.Second)
	for {
		free, err := metaStore.CountTenantPoolFreeSlots(ctx, pool.OrganizationID)
		if err != nil {
			t.Fatalf("count free slots: %v", err)
		}
		if free == pool.Size {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("free slots = %d, want %d after refill", free, pool.Size)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSharedTenantPoolLeaderReconcilerRefillsZeroInventory(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	poolManager := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer poolManager.Close()
	poolManager.SetMetaStore(metaStore)
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative, cloudProvider: "aws", region: "us-east-1"}
	srv := NewWithConfig(Config{Meta: metaStore, Pool: poolManager, Provisioner: prov,
		DefaultTenantProvider: tenant.ProviderTiDBCloudNativeShared, SharedDBMaxTenants: 100,
		TokenSecret: make([]byte, 32)})
	defer srv.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	pool := &meta.TenantPool{PoolID: "pool-leader-reconcile", OrganizationID: "org-leader-reconcile",
		Size: 1, Status: meta.TenantPoolActive, CreatedAt: now, UpdatedAt: now}
	if err := metaStore.CreateTenantPool(ctx, pool); err != nil {
		t.Fatalf("create pool: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		slots, countErr := metaStore.CountTenantPoolFreeSlots(ctx, pool.OrganizationID)
		if countErr != nil {
			t.Fatalf("count free slots: %v", countErr)
		}
		if slots == pool.Size {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("free slots = %d, want %d after leader reconcile", slots, pool.Size)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSharedTenantPoolRefillDoesNotReplaceRecoverablePlannedCapacity(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	poolManager := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	t.Cleanup(func() { poolManager.Close() })
	poolManager.SetMetaStore(metaStore)
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative, cloudProvider: "aws", region: "us-east-1"}
	srv := NewWithConfig(Config{Meta: metaStore, Pool: poolManager, Provisioner: prov,
		DefaultTenantProvider: tenant.ProviderTiDBCloudNativeShared, SharedDBMaxTenants: 1,
		TokenSecret: make([]byte, 32),
		Leader:      newFollowerLeaderManager(t, metaStore)})
	t.Cleanup(func() { srv.Close() })
	ctx := context.Background()
	now := time.Now().UTC()
	logicalPool := &meta.TenantPool{PoolID: "pool-expired-plan", OrganizationID: "org-expired-plan",
		Size: 1, Status: meta.TenantPoolActive, CreatedAt: now, UpdatedAt: now}
	if err := metaStore.CreateTenantPool(ctx, logicalPool); err != nil {
		t.Fatalf("CreateTenantPool: %v", err)
	}
	spendingLimit := int64(meta.MaxTiDBCloudSpendingLimit)
	dbID, err := metaStore.CreateManagedSharedDBPool(ctx, &meta.SharedDB{
		TiDBCloudOrganizationID: logicalPool.OrganizationID, ProvisioningKey: bytes.Repeat([]byte{1}, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 1, SpendingLimit: &spendingLimit,
	})
	if err != nil {
		t.Fatalf("CreateManagedSharedDBPool: %v", err)
	}
	tenantID := "tenant-expired-plan"
	if err := srv.insertPendingPoolTenant(ctx, tenantID, tenant.ProviderTiDBCloudNativeShared, now); err != nil {
		t.Fatalf("insertPendingPoolTenant: %v", err)
	}
	fsID, err := metaStore.EnsureFsID(ctx, tenantID)
	if err != nil {
		t.Fatalf("EnsureFsID: %v", err)
	}
	if err := metaStore.CompleteSharedTenantPoolMember(ctx, tenantID, tenant.ProviderTiDBCloudNativeShared,
		&meta.TenantPlacement{FsID: fsID, DbID: dbID, Placement: meta.PlacementShared, SchemaShape: meta.SchemaShapeShared},
		&meta.TenantPoolMembership{TenantID: tenantID, TiDBCloudOrganizationID: logicalPool.OrganizationID,
			PoolID: logicalPool.PoolID, PoolStatus: meta.TenantPoolBindingFree}); err != nil {
		t.Fatalf("CompleteSharedTenantPoolMember: %v", err)
	}
	if _, err := metaStore.DB().ExecContext(ctx, `UPDATE db_pool SET updated_at = ? WHERE db_id = ?`, now.Add(-2*time.Minute), dbID); err != nil {
		t.Fatalf("age planned pool: %v", err)
	}

	srv.replenishTenantPool(ctx, logicalPool, tenant.CredentialProvisionRequest{})
	var physicalPools int
	if err := metaStore.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM db_pool WHERE org_id = ?`, logicalPool.OrganizationID).Scan(&physicalPools); err != nil {
		t.Fatalf("count pending physical pools: %v", err)
	}
	if physicalPools != 1 {
		t.Fatalf("pending recoverable physical pools = %d, want no replacement", physicalPools)
	}
	if _, err := metaStore.DB().ExecContext(ctx, `UPDATE db_pool SET status = ?, updated_at = ? WHERE db_id = ?`,
		meta.SharedDBStatusActive, now.Add(-20*time.Minute), dbID); err != nil {
		t.Fatalf("activate and age physical pool: %v", err)
	}
	if _, err := metaStore.DB().ExecContext(ctx, `UPDATE tenants SET status = ? WHERE id = ?`, meta.TenantProvisioning, tenantID); err != nil {
		t.Fatalf("leave tenant activation pending: %v", err)
	}
	srv.replenishTenantPool(ctx, logicalPool, tenant.CredentialProvisionRequest{})
	if err := metaStore.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM db_pool WHERE org_id = ?`, logicalPool.OrganizationID).Scan(&physicalPools); err != nil {
		t.Fatalf("count active physical pools: %v", err)
	}
	if physicalPools != 1 {
		t.Fatalf("active pool with provisioning tenant produced %d physical pools, want no replacement", physicalPools)
	}
}

func TestLeaderTenantPoolReplenishmentDoesNotStartOnFollower(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	mgr := leader.NewManager(metaStore.DB())
	srv := &Server{
		leader: mgr, tenantPoolReconcileQueue: make(chan tenantPoolReconcileJob),
		leaderWorkersStarted: true, leaderWorkerCtx: context.Background(),
	}
	if srv.enqueueTenantPoolLeaderReplenishment(context.Background(), &meta.TenantPool{
		PoolID: "pool-follower", OrganizationID: "org-follower", Size: 1, Status: meta.TenantPoolActive,
	}, tenant.CredentialProvisionRequest{}) {
		t.Fatal("follower unexpectedly queued leader refill work")
	}
	if _, ok := srv.tenantPoolReplenishJobs.Load("pool-follower"); ok {
		t.Fatal("leader refill unexpectedly initialized the request-side per-pool gate")
	}
}

func TestTenantPoolLeaderReconcileWorkersQueueAndRestBetweenTasks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rest := 40 * time.Millisecond
	srv := &Server{tenantPoolReconcileQueue: make(chan tenantPoolReconcileJob), tenantPoolReconcileWorkerRest: rest}
	enqueue := func(fn func(context.Context)) bool {
		select {
		case srv.tenantPoolReconcileQueue <- tenantPoolReconcileJob{run: fn}:
			return true
		case <-ctx.Done():
			return false
		}
	}
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		srv.runTenantPoolReconcileWorker(ctx)
	}()
	firstStarted := make(chan struct{}, 1)
	release := make(chan struct{})
	if !enqueue(func(context.Context) {
		firstStarted <- struct{}{}
		<-release
	}) {
		t.Fatal("first leader reconcile task was not queued")
	}
	<-firstStarted
	secondStarted := make(chan struct{}, 1)
	secondQueued := make(chan bool, 1)
	go func() {
		secondQueued <- enqueue(func(context.Context) {
			secondStarted <- struct{}{}
		})
	}()
	select {
	case <-secondQueued:
		t.Fatal("second task left the queue while the only worker was occupied")
	case <-time.After(20 * time.Millisecond):
	}
	releasedAt := time.Now()
	close(release)
	select {
	case <-secondStarted:
		if elapsed := time.Since(releasedAt); elapsed < rest {
			t.Fatalf("queued task started after %s, want worker rest of at least %s", elapsed, rest)
		}
	case <-time.After(time.Second):
		t.Fatal("queued task did not start after the first worker rested")
	}
	if queued := <-secondQueued; !queued {
		t.Fatal("second leader reconcile task was not queued")
	}
	cancel()
	select {
	case <-workerDone:
	case <-time.After(time.Second):
		t.Fatal("tenant-pool reconcile worker did not stop")
	}
}

func TestTenantPoolLeaderReconcileCoalescesSamePoolIntoOneRerun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := &Server{
		tenantPoolReconcileQueue:      make(chan tenantPoolReconcileJob),
		tenantPoolReconcileWorkerRest: 20 * time.Millisecond,
	}
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		srv.runTenantPoolReconcileWorker(ctx)
	}()

	started := make(chan int, 2)
	releaseFirst := make(chan struct{})
	var attempts atomic.Int32
	run := func(context.Context) {
		attempt := int(attempts.Add(1))
		started <- attempt
		if attempt == 1 {
			<-releaseFirst
		}
	}
	if !srv.enqueueTenantPoolLeaderReconcile(ctx, "pool-repeated-scan", run) {
		t.Fatal("initial leader reconcile task was not queued")
	}
	select {
	case attempt := <-started:
		if attempt != 1 {
			t.Fatalf("first attempt = %d, want 1", attempt)
		}
	case <-time.After(time.Second):
		t.Fatal("initial leader reconcile task did not start")
	}

	for i := 0; i < 3; i++ {
		if !srv.enqueueTenantPoolLeaderReconcile(ctx, "pool-repeated-scan", run) {
			t.Fatalf("coalesced trigger %d was rejected", i+1)
		}
	}
	select {
	case attempt := <-started:
		t.Fatalf("attempt %d started while the first attempt was still running", attempt)
	case <-time.After(30 * time.Millisecond):
	}
	close(releaseFirst)
	select {
	case attempt := <-started:
		if attempt != 2 {
			t.Fatalf("rerun attempt = %d, want 2", attempt)
		}
	case <-time.After(time.Second):
		t.Fatal("coalesced rerun did not start")
	}
	select {
	case attempt := <-started:
		t.Fatalf("unexpected extra coalesced attempt %d", attempt)
	case <-time.After(50 * time.Millisecond):
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("leader reconcile attempts = %d, want 2", got)
	}
	if _, ok := srv.tenantPoolReplenishJobs.Load("pool-repeated-scan"); ok {
		t.Fatal("leader refill unexpectedly used the request-side per-pool gate")
	}

	cancel()
	select {
	case <-workerDone:
	case <-time.After(time.Second):
		t.Fatal("tenant-pool reconcile worker did not stop")
	}
}

func TestTenantPoolLeaderReconcileDoesNotRerunForDuplicateQueuedScan(t *testing.T) {
	state := &tenantPoolLeaderReconcileState{}
	if !state.request() {
		t.Fatal("initial scan was not accepted")
	}
	if state.request() {
		t.Fatal("duplicate queued scan was accepted as a second job")
	}
	state.start()
	if state.next() {
		t.Fatal("duplicate scan received before work started requested an unnecessary rerun")
	}
}

func TestManagedSharedDBStuckReconcilerFailsPoolAndPlacedTenant(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	ctx := context.Background()
	now := time.Now().UTC()
	logicalPool := &meta.TenantPool{PoolID: "logical-stuck", OrganizationID: "org-stuck", Size: 1,
		Status: meta.TenantPoolActive, CreatedAt: now, UpdatedAt: now}
	if err := metaStore.CreateTenantPool(ctx, logicalPool); err != nil {
		t.Fatalf("CreateTenantPool: %v", err)
	}
	spendingLimit := meta.MaxTiDBCloudSpendingLimit
	dbID, err := metaStore.CreateManagedSharedDBPool(ctx, &meta.SharedDB{
		TiDBCloudOrganizationID: "org-stuck", ProvisioningKey: bytes.Repeat([]byte{1}, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingLimit,
	})
	if err != nil {
		t.Fatalf("CreateManagedSharedDBPool: %v", err)
	}
	tenantID := "tenant-stuck-reconcile"
	if err := metaStore.InsertTenant(ctx, &meta.Tenant{ID: tenantID, Status: meta.TenantPending,
		Provider: tenant.ProviderTiDBCloudNative, DBPasswordCipher: []byte{}, SchemaVersion: 1,
		CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("InsertTenant: %v", err)
	}
	fsID, err := metaStore.EnsureFsID(ctx, tenantID)
	if err != nil {
		t.Fatalf("EnsureFsID: %v", err)
	}
	if err := metaStore.CompleteSharedTenantPoolMember(ctx, tenantID, tenant.ProviderTiDBCloudNativeShared,
		&meta.TenantPlacement{FsID: fsID, DbID: dbID, Placement: meta.PlacementShared, SchemaShape: meta.SchemaShapeShared},
		&meta.TenantPoolMembership{TenantID: tenantID, TiDBCloudOrganizationID: "org-stuck",
			PoolID: logicalPool.PoolID, PoolStatus: meta.TenantPoolBindingFree}); err != nil {
		t.Fatalf("CompleteSharedTenantPoolMember: %v", err)
	}
	if _, err := metaStore.DB().ExecContext(ctx, `UPDATE db_pool SET updated_at = ? WHERE db_id = ?`, now.Add(-20*time.Minute), dbID); err != nil {
		t.Fatalf("age db pool: %v", err)
	}

	srv := &Server{meta: metaStore, managedSharedDBStuckTimeout: 15 * time.Minute}
	srv.reconcileStuckManagedSharedDBPoolsWithCtx(ctx)
	pool, err := metaStore.GetSharedDB(ctx, dbID)
	if err != nil || pool.Status != meta.SharedDBStatusFailed {
		t.Fatalf("pool after stuck reconcile = %+v, %v", pool, err)
	}
	gotTenant, err := metaStore.GetTenant(ctx, tenantID)
	if err != nil || gotTenant.Status != meta.TenantFailed {
		t.Fatalf("tenant after stuck reconcile = %+v, %v", gotTenant, err)
	}
}

func TestManagedSharedDBActiveTenantReconcilerFinalizesStrandedTenant(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	ctx := context.Background()
	now := time.Now().UTC()
	spendingLimit := meta.MaxTiDBCloudSpendingLimit
	dbID, err := metaStore.CreateManagedSharedDBPool(ctx, &meta.SharedDB{
		TiDBCloudOrganizationID: "org-active-recovery", ProvisioningKey: bytes.Repeat([]byte{1}, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingLimit,
	})
	if err != nil {
		t.Fatal(err)
	}
	tenantID := "tenant-active-recovery"
	if err := metaStore.InsertTenant(ctx, &meta.Tenant{ID: tenantID, Status: meta.TenantPending,
		Provider: tenant.ProviderTiDBCloudNativeShared, DBPasswordCipher: []byte{}, SchemaVersion: 1,
		CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	fsID, err := metaStore.EnsureFsID(ctx, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if err := metaStore.CompleteSharedTenantPoolMember(ctx, tenantID, tenant.ProviderTiDBCloudNativeShared,
		&meta.TenantPlacement{FsID: fsID, DbID: dbID, Placement: meta.PlacementShared, SchemaShape: meta.SchemaShapeShared},
		&meta.TenantPoolMembership{TenantID: tenantID, TiDBCloudOrganizationID: "org-active-recovery",
			PoolID: "logical-active-recovery", PoolStatus: meta.TenantPoolBindingFree}); err != nil {
		t.Fatal(err)
	}
	if _, err := metaStore.DB().ExecContext(ctx, `UPDATE db_pool SET status = ? WHERE db_id = ?`, meta.SharedDBStatusActive, dbID); err != nil {
		t.Fatal(err)
	}
	if _, err := metaStore.DB().ExecContext(ctx, `UPDATE tenants SET status = ? WHERE id = ?`, meta.TenantProvisioning, tenantID); err != nil {
		t.Fatal(err)
	}

	srv := &Server{meta: metaStore}
	srv.resumeActiveManagedSharedDBTenantsWithCtx(ctx)
	got, err := metaStore.GetTenant(ctx, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != meta.TenantActive {
		t.Fatalf("stranded tenant status = %q, want active", got.Status)
	}
}

func TestManagedSharedDBStuckReconcilerSkipsPoolWithActiveRecoveryClaim(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	ctx := context.Background()
	spendingLimit := meta.MaxTiDBCloudSpendingLimit
	dbID, err := metaStore.CreateManagedSharedDBPool(ctx, &meta.SharedDB{
		TiDBCloudOrganizationID: "org-stuck-active", ProvisioningKey: bytes.Repeat([]byte{8}, 32),
		CloudProvider: "aws", Region: "us-east-1", MaxTenants: 100, SpendingLimit: &spendingLimit,
	})
	if err != nil {
		t.Fatalf("CreateManagedSharedDBPool: %v", err)
	}
	if _, err := metaStore.DB().ExecContext(ctx, `UPDATE db_pool SET updated_at = ? WHERE db_id = ?`, time.Now().UTC().Add(-20*time.Minute), dbID); err != nil {
		t.Fatalf("age db pool: %v", err)
	}

	srv := &Server{meta: metaStore, managedSharedDBStuckTimeout: 15 * time.Minute}
	holderStarted := make(chan struct{})
	releaseHolder := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- srv.withSharedDBPoolWorkLock(ctx, dbID, func(context.Context) error {
			close(holderStarted)
			<-releaseHolder
			return nil
		})
	}()
	<-holderStarted

	srv.reconcileStuckManagedSharedDBPoolsWithCtx(ctx)
	row, err := metaStore.GetSharedDB(ctx, dbID)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != meta.SharedDBStatusPending {
		t.Fatalf("busy recovery pool status = %q, want pending", row.Status)
	}

	close(releaseHolder)
	if err := <-holderDone; err != nil {
		t.Fatalf("recovery claim holder: %v", err)
	}
	srv.reconcileStuckManagedSharedDBPoolsWithCtx(ctx)
	row, err = metaStore.GetSharedDB(ctx, dbID)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != meta.SharedDBStatusFailed {
		t.Fatalf("idle stuck pool status = %q, want failed", row.Status)
	}
}

func TestManagedSharedDBRefillCapsOneWaveAtFiftyPhysicalPools(t *testing.T) {
	if got := managedSharedDBRefillTenantCount(10_000, 100, 50); got != 5_000 {
		t.Fatalf("refill tenant count = %d, want capacity of 50 physical pools", got)
	}
	if got := managedSharedDBRefillTenantCount(1_600, 100, 50); got != 1_600 {
		t.Fatalf("refill tenant count = %d, want all 1600 tenants across 16 physical pools", got)
	}
	if got := managedSharedDBRefillTenantCount(100, 1, 50); got != 50 {
		t.Fatalf("refill tenant count with one tenant per DB = %d, want 50", got)
	}
	if got := managedSharedDBRefillTenantCount(10_000, 100, 12); got != 1_200 {
		t.Fatalf("refill tenant count with configured 12-DB limit = %d, want 1200", got)
	}
}

func TestManagedSharedDBReplenishSubmitsWholePhysicalPoolWaveConcurrently(t *testing.T) {
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metaStore.Close() }()
	testmysql.ResetMetaDB(t, metaStore.DB())
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	poolManager := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost"}, enc)
	defer poolManager.Close()
	poolManager.SetMetaStore(metaStore)
	batchStarted := make(chan struct{}, 1)
	batchRelease := make(chan struct{})
	prov := &fakeProvisioner{
		provider:               tenant.ProviderTiDBCloudNative,
		cloudProvider:          "aws",
		region:                 "us-east-1",
		sharedPoolBatchStarted: batchStarted,
		sharedPoolBatchRelease: batchRelease,
	}
	workerCtx, cancel := context.WithCancel(context.Background())
	srv := &Server{
		meta: metaStore, pool: poolManager, provisioner: prov,
		defaultTenantProvider: tenant.ProviderTiDBCloudNativeShared,
		sharedDBMaxTenants:    1, managedSharedDBCloudBatchSize: 50,
		tenantPoolRefillFreeRatio: DefaultTenantPoolRefillFreeRatio,
		forkWorkerCtx:             workerCtx, forkWorkerCancel: cancel,
	}
	defer srv.Close()
	now := time.Now().UTC()
	logicalPool := &meta.TenantPool{PoolID: "pool-whole-wave", OrganizationID: "org-whole-wave",
		Size: 50, Status: meta.TenantPoolActive, CreatedAt: now, UpdatedAt: now}
	if err := metaStore.CreateTenantPool(context.Background(), logicalPool); err != nil {
		t.Fatal(err)
	}

	workerDone := make(chan struct{})
	workStarter := func(ctx context.Context, fn func(context.Context)) bool {
		go func() {
			defer close(workerDone)
			fn(ctx)
		}()
		return true
	}
	var unexpectedTimer atomic.Bool
	timerStarter := func(context.Context, func(context.Context)) bool {
		unexpectedTimer.Store(true)
		return false
	}
	var releaseOnce sync.Once
	releaseBatches := func() { releaseOnce.Do(func() { close(batchRelease) }) }
	defer func() {
		releaseBatches()
		select {
		case <-workerDone:
		case <-time.After(10 * time.Second):
			t.Error("replenish worker did not stop during test cleanup")
		}
	}()

	srv.replenishTenantPoolAsyncWithStarters(context.Background(), logicalPool,
		tenant.CredentialProvisionRequest{}, workStarter, timerStarter)
	select {
	case <-batchStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("Cloud batch did not start")
	}
	releaseBatches()
	select {
	case <-workerDone:
	case <-time.After(10 * time.Second):
		t.Fatal("whole-wave replenish did not finish")
	}
	if got := prov.sharedPoolBatchCalls.Load(); got != 1 {
		t.Fatalf("Cloud batch calls = %d, want 1", got)
	}
	if got := prov.sharedPoolBatchMembers.Load(); got != 50 {
		t.Fatalf("Cloud batch members = %d, want 50", got)
	}
	if unexpectedTimer.Load() {
		t.Fatal("unexpected replenish rerun timer")
	}
}

func TestTenantPoolEffectiveRefillRatioRejectsNaN(t *testing.T) {
	s := &Server{tenantPoolRefillFreeRatio: math.NaN()}
	if got := s.effectiveTenantPoolRefillFreeRatio(); got != DefaultTenantPoolRefillFreeRatio {
		t.Fatalf("effective refill ratio = %f, want %f", got, DefaultTenantPoolRefillFreeRatio)
	}
}

type adminTenantPoolSchemaInitRecorder struct {
	*quotaTestProvisioner

	schemaInitCalls   atomic.Int32
	mu                sync.Mutex
	lastSchemaInitDSN string
}

func newAdminTenantPoolRuntime(t *testing.T) (*quotaRuntime, *adminTenantPoolSchemaInitRecorder) {
	return newAdminTenantPoolRuntimeWithProvisioner(t, nil)
}

func newAdminTenantPoolRuntimeWithProvisioner(t *testing.T, wrap func(*adminTenantPoolSchemaInitRecorder) tenant.Provisioner) (*quotaRuntime, *adminTenantPoolSchemaInitRecorder) {
	t.Helper()
	var recorder *adminTenantPoolSchemaInitRecorder
	rt := newQuotaRuntimeWithOptions(t, tenant.ProviderTiDBCloudNative, quotaRuntimeOptions{provisioner: func(prov *quotaTestProvisioner) tenant.Provisioner {
		recorder = &adminTenantPoolSchemaInitRecorder{quotaTestProvisioner: prov}
		if wrap != nil {
			return wrap(recorder)
		}
		return recorder
	}})
	return rt, recorder
}

func insertAdminPoolFreeTenant(t *testing.T, rt *quotaRuntime, poolID, organizationID string, index int) string {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC().Add(time.Duration(index) * time.Second)
	tenantID := fmt.Sprintf("%s-free-%d", poolID, index)
	clusterID := fmt.Sprintf("%s-cluster-%d", poolID, index)
	passCipher, err := rt.server.pool.Encrypt(ctx, []byte("pool-pass"))
	if err != nil {
		t.Fatalf("encrypt password: %v", err)
	}
	if err := rt.meta.InsertTenant(ctx, &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantActive,
		DBHost:           "db.example.com",
		DBPort:           4000,
		DBUser:           "u.root",
		DBPasswordCipher: passCipher,
		DBName:           "tidbcloud_fs",
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBCloudNative,
		ClusterID:        clusterID,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatalf("insert tenant %s: %v", tenantID, err)
	}
	if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
		TenantID:       tenantID,
		OrganizationID: organizationID,
		ClusterID:      clusterID,
		PoolID:         poolID,
		PoolStatus:     meta.TenantPoolBindingFree,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("upsert binding %s: %v", tenantID, err)
	}
	return tenantID
}

type deadlineMetadataResumeProvisioner struct {
	*adminTenantPoolSchemaInitRecorder

	waitStarted     chan struct{}
	waitStartedOnce sync.Once
}

func (p *deadlineMetadataResumeProvisioner) WaitForPoolClustersMetadata(ctx context.Context, clusters []*tenant.ClusterInfo, req tenant.CredentialProvisionRequest) ([]*tenant.ClusterInfo, error) {
	p.waitStartedOnce.Do(func() { close(p.waitStarted) })
	<-ctx.Done()
	return p.quotaTestProvisioner.WaitForPoolClustersMetadata(context.Background(), clusters, req)
}

type groupStreamingMetadataResumeProvisioner struct {
	*adminTenantPoolSchemaInitRecorder

	slowTenantID string
	slowStarted  chan struct{}
	releaseSlow  chan struct{}
	slowOnce     sync.Once
}

func (p *groupStreamingMetadataResumeProvisioner) WaitForPoolClustersMetadata(ctx context.Context, clusters []*tenant.ClusterInfo, req tenant.CredentialProvisionRequest) ([]*tenant.ClusterInfo, error) {
	for _, cluster := range clusters {
		if cluster != nil && cluster.TenantID == p.slowTenantID {
			p.slowOnce.Do(func() { close(p.slowStarted) })
			select {
			case <-p.releaseSlow:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			break
		}
	}
	return p.quotaTestProvisioner.WaitForPoolClustersMetadata(ctx, clusters, req)
}

func (p *adminTenantPoolSchemaInitRecorder) InitSchema(_ context.Context, dsn string) error {
	p.mu.Lock()
	p.lastSchemaInitDSN = dsn
	p.mu.Unlock()
	p.schemaInitCalls.Add(1)
	return nil
}

func (p *adminTenantPoolSchemaInitRecorder) lastSchemaInitDSNSnapshot() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastSchemaInitDSN
}

func assertTenantUsesPrivateEndpointTLS(t *testing.T, got meta.Tenant) {
	t.Helper()
	dsn := tenantDSN("u.root", "pass", got.DBHost, got.DBPort, got.DBName, got.DBTLS, got.Provider)
	if !strings.Contains(dsn, "tls=skip-verify") {
		t.Fatalf("tenant DSN = %q, want tls=skip-verify", dsn)
	}
	if strings.Contains(dsn, "tls=true") {
		t.Fatalf("tenant DSN = %q, should not use tls=true for private endpoint", dsn)
	}
}

func assertSchemaInitUsesPrivateEndpointTLS(t *testing.T, dsn string) {
	t.Helper()
	if !strings.Contains(dsn, "tls=skip-verify") {
		t.Fatalf("schema init DSN = %q, want tls=skip-verify", dsn)
	}
	if strings.Contains(dsn, "tls=true") {
		t.Fatalf("schema init DSN = %q, should not use tls=true for private endpoint", dsn)
	}
}
