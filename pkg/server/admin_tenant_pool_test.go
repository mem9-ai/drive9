package server

import (
	"context"
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

func TestFreeTenantPoolClaimRejectsUncountedCandidateAtLimit(t *testing.T) {
	rt, candidateID := newFreeNativePoolClaimRuntime(t, "org-free-claim-limit")
	ctx := context.Background()
	zero := int64(0)
	if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
		TenantID: rt.tenantID, OrganizationID: "org-free-claim-limit", ClusterID: "cluster-quota-1",
	}); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.SetQuotaConfigPatch(ctx, rt.tenantID, meta.QuotaConfigPatch{TiDBCloudSpendingLimit: &zero}); err != nil {
		t.Fatal(err)
	}

	res, _, claimed, err := rt.server.claimAdminTenantFromPool(ctx,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"}, nil)
	if !errors.Is(err, tenant.ErrTiDBCloudFreeTenantLimitReached) {
		t.Fatalf("claim error = %v, want tenant limit", err)
	}
	if res != nil || claimed {
		t.Fatalf("claim result = %+v claimed=%v, want rejection", res, claimed)
	}
	binding, err := rt.meta.GetTenantTiDBCloudOrgBinding(ctx, candidateID)
	if err != nil || binding.PoolStatus != meta.TenantPoolBindingFree {
		t.Fatalf("candidate pool binding = %+v, err=%v, want free", binding, err)
	}
}

func TestFreeTenantPoolClaimWithHeadroomBecomesCounted(t *testing.T) {
	rt, candidateID := newFreeNativePoolClaimRuntime(t, "org-free-claim-headroom")
	ctx := context.Background()
	res, _, claimed, err := rt.server.claimAdminTenantFromPool(ctx,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"}, nil)
	if err != nil || !claimed || res == nil || res.TenantID != candidateID {
		t.Fatalf("claim result = %+v claimed=%v err=%v", res, claimed, err)
	}
	count, err := rt.meta.CountTiDBCloudFreeTenants(ctx, "org-free-claim-headroom")
	if err != nil || count != 1 {
		t.Fatalf("free count = %d, err=%v, want 1", count, err)
	}
}

func TestFreeTenantPoolClaimFailureRollsBackQuotaAndPoolStatus(t *testing.T) {
	rt, candidateID := newFreeNativePoolClaimRuntime(t, "org-free-claim-rollback")
	ctx := context.Background()
	wantErr := errors.New("mark pool used failed")
	rt.prov.markPoolUsedErr = wantErr

	res, _, claimed, err := rt.server.claimAdminTenantFromPool(ctx,
		tenant.CredentialProvisionRequest{PublicKey: "public", PrivateKey: "private"}, nil)
	if !errors.Is(err, wantErr) {
		t.Fatalf("claim error = %v, want %v", err, wantErr)
	}
	if res != nil || claimed {
		t.Fatalf("claim result = %+v claimed=%v, want failure", res, claimed)
	}
	var quotaRows int
	if err := rt.meta.DB().QueryRowContext(ctx,
		"SELECT COUNT(*) FROM tenant_quota_config WHERE tenant_id = ?", candidateID).Scan(&quotaRows); err != nil {
		t.Fatal(err)
	}
	if quotaRows != 0 {
		t.Fatalf("quota rows = %d, want 0", quotaRows)
	}
	binding, err := rt.meta.GetTenantTiDBCloudOrgBinding(ctx, candidateID)
	if err != nil || binding.PoolStatus != meta.TenantPoolBindingFree {
		t.Fatalf("pool binding = %+v, err=%v, want free", binding, err)
	}
	count, err := rt.meta.CountTiDBCloudFreeTenants(ctx, "org-free-claim-rollback")
	if err != nil || count != 0 {
		t.Fatalf("free count = %d, err=%v, want 0", count, err)
	}
}

func newFreeNativePoolClaimRuntime(t *testing.T, organizationID string) (*quotaRuntime, string) {
	t.Helper()
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	rt.prov.billingFree = true
	rt.prov.iamIdentities = []*tenant.TiDBCloudAPIKeyIdentity{{
		OrganizationID: organizationID, Role: tenant.TiDBCloudRoleOrgOwner,
	}}
	rt.server.tidbCloudFreePlanLimits = TiDBCloudFreePlanLimits{
		TenantCount:      1,
		MaxStorageBytes:  DefaultTiDBCloudFreeMaxStorageBytes,
		MaxFileSizeBytes: DefaultTiDBCloudFreeMaxFileSizeBytes,
		MaxFileCount:     DefaultTiDBCloudFreeMaxFileCount,
	}
	ctx := context.Background()
	now := time.Now().UTC()
	poolID := "pool-" + organizationID
	if err := rt.meta.CreateTenantPool(ctx, &meta.TenantPool{
		PoolID: poolID, OrganizationID: organizationID, Size: 1,
		Status: meta.TenantPoolActive, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	return rt, insertAdminPoolFreeTenant(t, rt, poolID, organizationID, 1)
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
