package server

import (
	"context"
	"fmt"
	"io"
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

func TestAdminTenantPoolRefillSkipsWhenPoolFull(t *testing.T) {
	rt, _ := newAdminTenantPoolRuntime(t)
	ctx := context.Background()
	now := time.Now().UTC()
	pool := &meta.TenantPool{
		PoolID:         "pool-refill-full",
		OrganizationID: "org-1",
		Size:           10,
		Status:         meta.TenantPoolActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := rt.meta.CreateTenantPool(ctx, pool); err != nil {
		t.Fatalf("create pool: %v", err)
	}
	for i := 1; i <= 10; i++ {
		insertAdminPoolFreeTenant(t, rt, pool.PoolID, pool.OrganizationID, i)
	}

	rt.server.kickTenantPoolRefill(ctx, pool, tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	rt.server.forkWorkerWG.Wait()

	if got := rt.prov.batchPoolCalls.Load(); got != 0 {
		t.Fatalf("batch pool calls = %d, want 0", got)
	}
	if _, ok := rt.server.tenantPoolRefillJobs.Load(pool.PoolID); ok {
		t.Fatalf("refill job for %s still registered after worker exit", pool.PoolID)
	}
}

func TestAdminTenantPoolRefillTopsUpInFixedBatches(t *testing.T) {
	rt, _ := newAdminTenantPoolRuntime(t)
	rt.server.tenantPoolRefillBatchSize = 2
	rt.server.tenantPoolRefillInterval = time.Millisecond
	ctx := context.Background()
	now := time.Now().UTC()
	pool := &meta.TenantPool{
		PoolID:         "pool-refill-batches",
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

	// Repeated kicks coalesce into a single worker via the rerun flag.
	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	rt.server.kickTenantPoolRefill(ctx, pool, cred)
	rt.server.kickTenantPoolRefill(ctx, pool, cred)
	rt.server.kickTenantPoolRefill(ctx, pool, cred)
	rt.server.forkWorkerWG.Wait()

	// Deficit 3: three separate one-cluster create calls dispatched across
	// rounds (2 + 1), never one catch-up batch of 3.
	if got := rt.prov.batchPoolCalls.Load(); got != 3 {
		t.Fatalf("batch pool calls = %d, want 3", got)
	}
	if _, ok := rt.server.tenantPoolRefillJobs.Load(pool.PoolID); ok {
		t.Fatalf("refill job for %s still registered after worker exit", pool.PoolID)
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

func TestAdminTenantPoolRefillWorkerStopsAfterConsecutiveFailures(t *testing.T) {
	rt, _ := newAdminTenantPoolRuntime(t)
	rt.server.tenantPoolRefillBatchSize = 1
	rt.server.tenantPoolRefillInterval = time.Millisecond
	// Every batch returns a cluster without a tenant id, so no pool tenant can
	// be persisted and every refill round fails.
	rt.prov.batchPoolMissingTenant = map[int]bool{0: true}
	ctx := context.Background()
	now := time.Now().UTC()
	pool := &meta.TenantPool{
		PoolID:         "pool-refill-failing",
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

	rt.server.kickTenantPoolRefill(ctx, pool, tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	rt.server.forkWorkerWG.Wait()

	// The breaker counts failures as async creations finish, so a few
	// already-dispatched calls may land after the limit trips: the worker
	// must stop promptly, not at exactly N calls.
	if got := rt.prov.batchPoolCalls.Load(); got < tenantPoolRefillMaxConsecutiveFailures || got > tenantPoolRefillMaxConsecutiveFailures+5 {
		t.Fatalf("batch pool calls = %d, want within [%d, %d] (worker stops after consecutive failures)", got, tenantPoolRefillMaxConsecutiveFailures, tenantPoolRefillMaxConsecutiveFailures+5)
	}
	if _, ok := rt.server.tenantPoolRefillJobs.Load(pool.PoolID); ok {
		t.Fatalf("refill job for %s still registered after worker exit", pool.PoolID)
	}
}

func TestAdminTenantPoolRefillWorkerExitsWhenPoolDeleted(t *testing.T) {
	rt, _ := newAdminTenantPoolRuntime(t)
	rt.server.tenantPoolRefillInterval = time.Millisecond
	ctx := context.Background()
	now := time.Now().UTC()
	pool := &meta.TenantPool{
		PoolID:         "pool-refill-deleted",
		OrganizationID: "org-1",
		Size:           10,
		Status:         meta.TenantPoolActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := rt.meta.CreateTenantPool(ctx, pool); err != nil {
		t.Fatalf("create pool: %v", err)
	}
	if err := rt.meta.DeleteTenantPool(ctx, pool.PoolID); err != nil {
		t.Fatalf("delete pool: %v", err)
	}

	rt.server.kickTenantPoolRefill(ctx, pool, tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	rt.server.forkWorkerWG.Wait()

	if got := rt.prov.batchPoolCalls.Load(); got != 0 {
		t.Fatalf("batch pool calls = %d, want 0", got)
	}
}

func TestTenantPoolRefillConfigDefaults(t *testing.T) {
	s := &Server{}
	batchSize, interval := s.tenantPoolRefillConfig()
	if batchSize != DefaultTenantPoolRefillBatchSize {
		t.Fatalf("default refill batch size = %d, want %d", batchSize, DefaultTenantPoolRefillBatchSize)
	}
	if interval != DefaultTenantPoolRefillInterval {
		t.Fatalf("default refill interval = %s, want %s", interval, DefaultTenantPoolRefillInterval)
	}
}

func TestAdminTenantPoolRefillPlaceholderSlotsVisibleInFlight(t *testing.T) {
	rt, _ := newAdminTenantPoolRuntime(t)
	rt.server.tenantPoolRefillBatchSize = 3
	rt.server.tenantPoolRefillInterval = time.Millisecond
	block := make(chan struct{})
	rt.prov.batchPoolHook = func(int) { <-block }
	ctx := context.Background()
	now := time.Now().UTC()
	pool := &meta.TenantPool{
		PoolID:         "pool-refill-placeholder",
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

	rt.server.kickTenantPoolRefill(ctx, pool, tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})

	// While the three creates are blocked in flight, their placeholder
	// bindings already occupy pool slots (so no pod over-provisions)...
	deadline := time.Now().Add(5 * time.Second)
	for {
		slots, err := rt.meta.CountTenantPoolFreeSlots(ctx, pool.OrganizationID)
		if err != nil {
			t.Fatalf("count slots: %v", err)
		}
		if slots == 10 && rt.prov.batchPoolCalls.Load() == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("slots = %d, calls = %d; want 10 slots (7 free + 3 placeholders) and 3 calls", slots, rt.prov.batchPoolCalls.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
	// ...but they are not active free tenants, so claims cannot take them.
	free, err := rt.meta.CountFreeTenantPoolBindings(ctx, pool.OrganizationID)
	if err != nil {
		t.Fatalf("count free: %v", err)
	}
	if free != 7 {
		t.Fatalf("active free size = %d, want 7 while creations are in flight", free)
	}

	close(block)
	rt.server.forkWorkerWG.Wait()

	deadline = time.Now().Add(5 * time.Second)
	for {
		free, err = rt.meta.CountFreeTenantPoolBindings(ctx, pool.OrganizationID)
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

func TestAdminTenantPoolRefillReapsStalePlaceholders(t *testing.T) {
	rt, _ := newAdminTenantPoolRuntime(t)
	rt.server.tenantPoolRefillBatchSize = 3
	rt.server.tenantPoolRefillInterval = time.Millisecond
	ctx := context.Background()
	now := time.Now().UTC()
	pool := &meta.TenantPool{
		PoolID:         "pool-refill-reaper",
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
	staleTenantID := insertAdminPoolPlaceholderTenant(t, rt, pool.PoolID, pool.OrganizationID, "stale-1", now.Add(-tenantPoolPlaceholderMaxAge - time.Minute))

	rt.server.kickTenantPoolRefill(ctx, pool, tenant.CredentialProvisionRequest{
		PublicKey:  "public-1",
		PrivateKey: "private-1",
	})
	rt.server.forkWorkerWG.Wait()

	// The stale placeholder was reaped (tenant failed, slot freed) and the
	// freed slot was refilled like any other deficit.
	stale, err := rt.meta.GetTenant(ctx, staleTenantID)
	if err != nil {
		t.Fatalf("get stale tenant: %v", err)
	}
	if stale.Status != meta.TenantFailed {
		t.Fatalf("stale placeholder tenant status = %s, want %s", stale.Status, meta.TenantFailed)
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

func TestAdminTenantPoolRefillReclaimedPlaceholderDeprovisionsCluster(t *testing.T) {
	rt, _ := newAdminTenantPoolRuntime(t)
	rt.server.tenantPoolRefillBatchSize = 1
	rt.server.tenantPoolRefillInterval = 200 * time.Millisecond
	block := make(chan struct{})
	var once sync.Once
	rt.prov.batchPoolHook = func(int) { once.Do(func() { <-block }) }
	ctx := context.Background()
	now := time.Now().UTC()
	pool := &meta.TenantPool{
		PoolID:         "pool-refill-reclaim",
		OrganizationID: "org-1",
		Size:           10,
		Status:         meta.TenantPoolActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := rt.meta.CreateTenantPool(ctx, pool); err != nil {
		t.Fatalf("create pool: %v", err)
	}
	// Backdate the free tenants so the dispatcher's placeholder is the newest
	// slot and gets picked by the shrink below.
	for i := 1; i <= 7; i++ {
		insertAdminPoolFreeTenantAt(t, rt, pool.PoolID, pool.OrganizationID, i, now.Add(-time.Duration(10-i)*time.Second))
	}
	cred := tenant.CredentialProvisionRequest{PublicKey: "public-1", PrivateKey: "private-1"}
	rt.server.kickTenantPoolRefill(ctx, pool, cred)

	// Wait for the placeholder creation to be in flight.
	var placeholderTenantID string
	deadline := time.Now().Add(5 * time.Second)
	for {
		rows, err := rt.meta.ListTenantPoolFreeSlotsForDelete(ctx, pool.OrganizationID, true, 1)
		if err != nil {
			t.Fatalf("list slots: %v", err)
		}
		if len(rows) == 1 && isTenantPoolPlaceholderClusterID(rows[0].Binding.ClusterID) && rt.prov.batchPoolCalls.Load() == 1 {
			placeholderTenantID = rows[0].Tenant.ID
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no in-flight placeholder observed; calls = %d", rt.prov.batchPoolCalls.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Shrink reclaims the placeholder slot while its create call is blocked:
	// no cloud deprovision yet — there is no real cluster.
	deleted, err := rt.server.deleteNewestFreePoolTenants(ctx, pool.PoolID, pool.OrganizationID, 1, cred, false)
	if err != nil {
		t.Fatalf("shrink: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 0 {
		t.Fatalf("deprovision calls = %d, want 0 for a placeholder slot", got)
	}

	close(block)
	rt.server.forkWorkerWG.Wait()

	// When the blocked create returned, the creator saw the reclaimed tenant
	// and deprovisioned the cluster instead of resurrecting the slot.
	if got := rt.prov.deprovisionCalls.Load(); got != 1 {
		t.Fatalf("deprovision calls = %d, want 1 (reclaimed creation cleaned up)", got)
	}
	reclaimed, err := rt.meta.GetTenant(ctx, placeholderTenantID)
	if err != nil {
		t.Fatalf("get reclaimed tenant: %v", err)
	}
	if reclaimed.Status != meta.TenantDeleted {
		t.Fatalf("reclaimed placeholder tenant status = %s, want %s", reclaimed.Status, meta.TenantDeleted)
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
	return insertAdminPoolFreeTenantAt(t, rt, poolID, organizationID, index, time.Now().UTC().Add(time.Duration(index)*time.Second))
}

func insertAdminPoolFreeTenantAt(t *testing.T, rt *quotaRuntime, poolID, organizationID string, index int, createdAt time.Time) string {
	t.Helper()
	ctx := context.Background()
	now := createdAt
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

// insertAdminPoolPlaceholderTenant inserts a pending pool tenant with a
// placeholder binding, mimicking a refill creation that was dispatched (or
// abandoned) but never completed.
func insertAdminPoolPlaceholderTenant(t *testing.T, rt *quotaRuntime, poolID, organizationID, suffix string, createdAt time.Time) string {
	t.Helper()
	ctx := context.Background()
	tenantID := fmt.Sprintf("%s-placeholder-%s", poolID, suffix)
	if err := rt.meta.InsertTenant(ctx, &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantPending,
		DBPasswordCipher: []byte{},
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBCloudNative,
		SchemaVersion:    1,
		CreatedAt:        createdAt,
		UpdatedAt:        createdAt,
	}); err != nil {
		t.Fatalf("insert placeholder tenant %s: %v", tenantID, err)
	}
	if err := rt.meta.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
		TenantID:       tenantID,
		OrganizationID: organizationID,
		ClusterID:      tenantPoolPlaceholderClusterIDPrefix + tenantID,
		PoolID:         poolID,
		PoolStatus:     meta.TenantPoolBindingFree,
		CreatedAt:      createdAt,
		UpdatedAt:      createdAt,
	}); err != nil {
		t.Fatalf("upsert placeholder binding %s: %v", tenantID, err)
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
