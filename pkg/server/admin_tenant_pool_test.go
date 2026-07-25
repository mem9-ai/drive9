package server

import (
	"bytes"
	"context"
	"crypto/rand"
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
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
)

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
}

func TestTenantPoolClaimConsumesMixedInventoryInGlobalAgeOrder(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.server.defaultTenantProvider = tenant.ProviderTiDBCloudNativeShared
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
	secret := make([]byte, 32)
	_, _ = rand.Read(secret)
	srv := NewWithConfig(Config{Meta: metaStore, Pool: poolManager, Provisioner: prov,
		DefaultTenantProvider: tenant.ProviderTiDBCloudNativeShared, SharedDBMaxTenants: 1, TokenSecret: secret})
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
		DefaultTenantProvider: tenant.ProviderTiDBCloudNativeShared, TokenSecret: make([]byte, 32)})
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

func TestSharedTenantPoolRefillLimitsTenantWorkersToThirty(t *testing.T) {
	const wantConcurrency = 30
	started := make(chan struct{}, wantConcurrency+1)
	release := make(chan struct{})
	done := make(chan []sharedPoolTenantCreateOutcome, 1)
	go func() {
		done <- runSharedPoolTenantCreateWorkers(context.Background(), wantConcurrency+1, func() sharedPoolTenantCreateOutcome {
			started <- struct{}{}
			<-release
			return sharedPoolTenantCreateOutcome{}
		})
	}()
	for i := 0; i < wantConcurrency; i++ {
		select {
		case <-started:
		case <-time.After(500 * time.Millisecond):
			close(release)
			<-done
			t.Fatalf("started %d/%d tenant workers before release", i, wantConcurrency)
		}
	}
	select {
	case <-started:
		close(release)
		<-done
		t.Fatal("started a 31st tenant worker while 30 were still in flight")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	outcomes := <-done
	if len(outcomes) != wantConcurrency+1 {
		t.Fatalf("outcomes = %d, want %d", len(outcomes), wantConcurrency+1)
	}
}

func TestSharedTenantPoolRefillUsesExistingCapacityWithoutIdentityLookup(t *testing.T) {
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
	if _, err := metaStore.RegisterSharedDB(context.Background(), &meta.SharedDB{
		TiDBCloudOrganizationID: "org-shared-existing", Host: "shared.example.com", Port: 4000,
		User: "root", PasswordCipher: passwordCipher, Name: "tidbcloud_fs", MaxTenants: 100,
	}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := metaStore.CreateTenantPool(context.Background(), &meta.TenantPool{
		PoolID: "pool-shared-existing", OrganizationID: "org-shared-existing", Size: 50,
		Status: meta.TenantPoolActive, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{provider: tenant.ProviderTiDBCloudNative, identityOrg: "org-shared-existing"}
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
	if got := prov.sharedPoolBatchCalls.Load(); got != 0 {
		t.Fatalf("physical batch calls = %d, want 0 for existing capacity", got)
	}
	if got := prov.iamCalls.Load(); got != 0 {
		t.Fatalf("identity lookups = %d, want 0 when tenant pool organization is known", got)
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
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.server.defaultTenantProvider = tenant.ProviderTiDBCloudNativeShared
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

	rt, schemaInitRecorder := newAdminTenantPoolRuntime(t)
	waiter := &deadlineMetadataResumeProvisioner{
		adminTenantPoolSchemaInitRecorder: schemaInitRecorder,
		waitStarted:                       make(chan struct{}),
	}
	rt.server.provisioner = waiter

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

	rt, schemaInitRecorder := newAdminTenantPoolRuntime(t)
	waiter := &groupStreamingMetadataResumeProvisioner{
		adminTenantPoolSchemaInitRecorder: schemaInitRecorder,
		slowTenantID:                      "pool-stream-resume-slow",
		slowStarted:                       make(chan struct{}),
		releaseSlow:                       make(chan struct{}),
	}
	var releaseSlowOnce sync.Once
	t.Cleanup(func() {
		releaseSlowOnce.Do(func() { close(waiter.releaseSlow) })
	})
	rt.server.provisioner = waiter

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

func TestAdminTenantPoolReplenishChunksLargeRefill(t *testing.T) {
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
		TokenSecret: make([]byte, 32)})
	defer srv.Close()
	// Make each refill batch visible as one cloud batch. The production default
	// is smaller, but the physical pool count is independent of this test knob.
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
		if got := prov.sharedPoolBatchCalls.Load(); got >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("shared cloud batch calls = %d, want three bounded refill batches", prov.sharedPoolBatchCalls.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	free, err := metaStore.CountTenantPoolFreeSlots(ctx, pool.OrganizationID)
	if err != nil {
		t.Fatalf("count free: %v", err)
	}
	if free != pool.Size {
		t.Fatalf("free size = %d, want %d after refill", free, pool.Size)
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
	if got := managedSharedDBRefillBatchTenantCount(10_000, 100, 50); got != 100 {
		t.Fatalf("refill batch tenant count = %d, want one physical DB capacity", got)
	}
	if got := managedSharedDBRefillBatchTenantCount(50, 100, 50); got != 50 {
		t.Fatalf("small refill batch tenant count = %d, want 50", got)
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
	t.Helper()
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	recorder := &adminTenantPoolSchemaInitRecorder{quotaTestProvisioner: rt.prov}
	rt.server.provisioner = recorder
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
