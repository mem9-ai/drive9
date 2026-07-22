package server

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testmysql"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"github.com/mem9-ai/drive9/pkg/tenant"
)

func TestObserveTenantPoolBindingCountsRecordsUsedAndFreeByPoolAndOrg(t *testing.T) {
	initServerTenantSchema(t, testDSN)
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	testmysql.ResetMetaDB(t, metaStore.DB())

	ctx := context.Background()
	now := time.Now().UTC()
	if err := metaStore.CreateTenantPool(ctx, &meta.TenantPool{
		PoolID:         "pool-observe-bindings",
		OrganizationID: "org-observe-bindings",
		Size:           3,
		Status:         meta.TenantPoolActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("create tenant pool: %v", err)
	}
	for _, tc := range []struct {
		tenantID string
		status   meta.TenantPoolBindingStatus
	}{
		{tenantID: "tenant-observe-free-1", status: meta.TenantPoolBindingFree},
		{tenantID: "tenant-observe-free-2", status: meta.TenantPoolBindingFree},
		{tenantID: "tenant-observe-used-1", status: meta.TenantPoolBindingUsed},
	} {
		insertTenantPoolMetricTenant(t, metaStore, tc.tenantID, "cluster-"+tc.tenantID, now)
		if err := metaStore.UpsertTenantTiDBCloudOrgBinding(ctx, &meta.TenantTiDBCloudOrgBinding{
			TenantID:       tc.tenantID,
			OrganizationID: "org-observe-bindings",
			ClusterID:      "cluster-" + tc.tenantID,
			PoolID:         "pool-observe-bindings",
			PoolStatus:     tc.status,
			CreatedAt:      now,
			UpdatedAt:      now,
		}); err != nil {
			t.Fatalf("upsert binding %s: %v", tc.tenantID, err)
		}
	}

	s := &Server{meta: metaStore, metrics: newServerMetrics()}
	s.observeTenantPoolBindingCounts(ctx)

	rec := httptest.NewRecorder()
	s.metrics.writePrometheus(rec)
	text := rec.Body.String()
	for _, want := range []string{
		`drive9_tenant_pool_bindings{pool_id="pool-observe-bindings",status="free",tidbcloud_org_id="org-observe-bindings"} 2`,
		`drive9_tenant_pool_bindings{pool_id="pool-observe-bindings",status="used",tidbcloud_org_id="org-observe-bindings"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing tenant pool binding metric %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "pool_status=") {
		t.Fatalf("tenant pool binding metrics must use status label, not pool_status:\n%s", text)
	}

	if err := metaStore.DeleteTenantPool(ctx, "pool-observe-bindings"); err != nil {
		t.Fatalf("delete tenant pool: %v", err)
	}
	s.observeTenantPoolBindingCounts(ctx)

	rec = httptest.NewRecorder()
	s.metrics.writePrometheus(rec)
	text = rec.Body.String()
	if strings.Contains(text, `drive9_tenant_pool_bindings{pool_id="pool-observe-bindings"`) {
		t.Fatalf("deleted pool binding metrics should be removed after next observation:\n%s", text)
	}
}

func TestObserveTenantCountsRecordsAllRealStatuses(t *testing.T) {
	initServerTenantSchema(t, testDSN)
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	testmysql.ResetMetaDB(t, metaStore.DB())

	now := time.Now().UTC()
	for _, tc := range []struct {
		tenantID string
		status   meta.TenantStatus
	}{
		{tenantID: "tenant-count-pending", status: meta.TenantPending},
		{tenantID: "tenant-count-provisioning", status: meta.TenantProvisioning},
		{tenantID: "tenant-count-active", status: meta.TenantActive},
		{tenantID: "tenant-count-failed", status: meta.TenantFailed},
		{tenantID: "tenant-count-suspended", status: meta.TenantSuspended},
		{tenantID: "tenant-count-deleting", status: meta.TenantDeleting},
		{tenantID: "tenant-count-deleted", status: meta.TenantDeleted},
	} {
		insertTenantMetricTenant(t, metaStore, tc.tenantID, tc.status, now)
	}

	s := &Server{meta: metaStore, metrics: newServerMetrics()}
	s.observeTenantCounts(context.Background())

	rec := httptest.NewRecorder()
	s.metrics.writePrometheus(rec)
	text := rec.Body.String()
	for _, status := range []meta.TenantStatus{
		meta.TenantPending,
		meta.TenantProvisioning,
		meta.TenantActive,
		meta.TenantFailed,
		meta.TenantSuspended,
		meta.TenantDeleting,
		meta.TenantDeleted,
	} {
		want := `drive9_tenant_count{status="` + string(status) + `"} 1.000000`
		if !strings.Contains(text, want) {
			t.Fatalf("missing tenant count metric %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "drive9_tenant_non_deleted_count") || strings.Contains(text, "total_non_deleted") {
		t.Fatalf("tenant count metrics must only use real status values:\n%s", text)
	}
}

func TestObserveSharedDBPoolMetricsRecordsCapacityTenantsAndSpending(t *testing.T) {
	initServerTenantSchema(t, testDSN)
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	testmysql.ResetMetaDB(t, metaStore.DB())

	ctx := context.Background()
	spendingLimit := int64(10_000)
	const activePoolUUID = "11111111-1111-4111-8111-111111111111"
	const provisioningPoolUUID = "22222222-2222-4222-8222-222222222222"
	dbID, err := metaStore.CreateManagedSharedDBPool(ctx, &meta.SharedDB{
		UUID:                    activePoolUUID,
		TiDBCloudOrganizationID: "org-shared-db-metrics",
		ProvisioningKey:         make([]byte, 32),
		CloudProvider:           "aws",
		Region:                  "us-east-1",
		MaxTenants:              5,
		SpendingLimit:           &spendingLimit,
	})
	if err != nil {
		t.Fatalf("CreateManagedSharedDBPool active: %v", err)
	}
	if _, err := metaStore.DB().ExecContext(ctx, `UPDATE db_pool SET status = ? WHERE db_id = ?`, meta.SharedDBStatusActive, dbID); err != nil {
		t.Fatalf("activate db pool: %v", err)
	}
	provisioningDBID, err := metaStore.CreateManagedSharedDBPool(ctx, &meta.SharedDB{
		UUID:                    provisioningPoolUUID,
		TiDBCloudOrganizationID: "org-shared-db-metrics",
		ProvisioningKey:         []byte("12345678901234567890123456789012"),
		CloudProvider:           "aws",
		Region:                  "us-east-1",
		MaxTenants:              5,
		SpendingLimit:           &spendingLimit,
	})
	if err != nil {
		t.Fatalf("CreateManagedSharedDBPool provisioning: %v", err)
	}

	now := time.Now().UTC()
	for _, tc := range []struct {
		tenantID     string
		status       meta.TenantStatus
		virtualLimit int64
	}{
		{tenantID: "tenant-shared-metrics-active", status: meta.TenantActive, virtualLimit: 1_000},
		{tenantID: "tenant-shared-metrics-provisioning", status: meta.TenantProvisioning, virtualLimit: 2_000},
	} {
		insertSharedDBMetricTenant(t, metaStore, tc.tenantID, tc.status, now)
		if err := metaStore.SetQuotaConfigPatch(ctx, tc.tenantID, meta.QuotaConfigPatch{TiDBCloudSpendingLimit: &tc.virtualLimit}); err != nil {
			t.Fatalf("SetQuotaConfigPatch %s: %v", tc.tenantID, err)
		}
		fsID, err := metaStore.EnsureFsID(ctx, tc.tenantID)
		if err != nil {
			t.Fatalf("EnsureFsID %s: %v", tc.tenantID, err)
		}
		if err := metaStore.UpsertTenantPlacement(ctx, &meta.TenantPlacement{
			FsID: fsID, DbID: dbID, Placement: meta.PlacementShared,
			SchemaShape: meta.SchemaShapeShared, Status: meta.SharedDBStatusActive,
		}); err != nil {
			t.Fatalf("UpsertTenantPlacement %s: %v", tc.tenantID, err)
		}
		if err := metaStore.IncrSharedDBTenantCount(ctx, dbID, 1); err != nil {
			t.Fatalf("IncrSharedDBTenantCount %s: %v", tc.tenantID, err)
		}
	}

	s := &Server{meta: metaStore, metrics: newServerMetrics()}
	s.observeSharedDBPoolMetrics(ctx)

	rec := httptest.NewRecorder()
	s.metrics.writePrometheus(rec)
	text := rec.Body.String()
	dbPoolID := fmt.Sprint(dbID)
	provisioningDBPoolID := fmt.Sprint(provisioningDBID)
	for _, want := range []string{
		`drive9_shared_db_pool_total{db_pool_id="` + dbPoolID + `",db_pool_uuid="` + activePoolUUID + `",status="active",tidbcloud_org_id="org-shared-db-metrics"} 1`,
		`drive9_shared_db_pool_total{db_pool_id="` + provisioningDBPoolID + `",db_pool_uuid="` + provisioningPoolUUID + `",status="provisioning",tidbcloud_org_id="org-shared-db-metrics"} 1`,
		`drive9_shared_db_pool_capacity{db_pool_id="` + dbPoolID + `",db_pool_uuid="` + activePoolUUID + `",tidbcloud_org_id="org-shared-db-metrics",type="soft_max"} 5`,
		`drive9_shared_db_pool_capacity{db_pool_id="` + dbPoolID + `",db_pool_uuid="` + activePoolUUID + `",tidbcloud_org_id="org-shared-db-metrics",type="hard_max"} 6`,
		`drive9_shared_db_pool_capacity{db_pool_id="` + dbPoolID + `",db_pool_uuid="` + activePoolUUID + `",tidbcloud_org_id="org-shared-db-metrics",type="used"} 2`,
		`drive9_shared_db_pool_capacity{db_pool_id="` + dbPoolID + `",db_pool_uuid="` + activePoolUUID + `",tidbcloud_org_id="org-shared-db-metrics",type="free"} 3`,
		`drive9_shared_db_pool_tenants{db_pool_id="` + dbPoolID + `",db_pool_uuid="` + activePoolUUID + `",state="active",tidbcloud_org_id="org-shared-db-metrics"} 1`,
		`drive9_shared_db_pool_tenants{db_pool_id="` + dbPoolID + `",db_pool_uuid="` + activePoolUUID + `",state="provisioning",tidbcloud_org_id="org-shared-db-metrics"} 1`,
		`drive9_shared_db_pool_spending_limit{db_pool_id="` + dbPoolID + `",db_pool_uuid="` + activePoolUUID + `",tidbcloud_org_id="org-shared-db-metrics",type="target"} 10000`,
		`drive9_shared_db_pool_spending_limit{db_pool_id="` + dbPoolID + `",db_pool_uuid="` + activePoolUUID + `",tidbcloud_org_id="org-shared-db-metrics",type="tenant_sum"} 3000`,
		`drive9_shared_db_pool_spending_limit{db_pool_id="` + dbPoolID + `",db_pool_uuid="` + activePoolUUID + `",tidbcloud_org_id="org-shared-db-metrics",type="headroom"} 7000`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing shared DB-pool metric %q:\n%s", want, text)
		}
	}
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "drive9_shared_db_pool_") && strings.Contains(line, "organization_id=") {
			t.Fatalf("shared DB-pool metrics must use tidbcloud_org_id:\n%s", text)
		}
	}

	if _, err := metaStore.DB().ExecContext(ctx, `DELETE FROM tenant_placements`); err != nil {
		t.Fatalf("delete placements: %v", err)
	}
	if _, err := metaStore.DB().ExecContext(ctx, `DELETE FROM db_pool`); err != nil {
		t.Fatalf("delete db pools: %v", err)
	}
	s.observeSharedDBPoolMetrics(ctx)
	rec = httptest.NewRecorder()
	s.metrics.writePrometheus(rec)
	if strings.Contains(rec.Body.String(), `db_pool_id="`+dbPoolID+`"`) {
		t.Fatalf("deleted shared DB-pool metrics should be removed after next observation:\n%s", rec.Body.String())
	}
}

func TestStopLeaderWorkersClearsMetricSnapshots(t *testing.T) {
	tenantPoolKey := tenantPoolBindingMetricKey{
		poolID:         "pool-leader-loss-clear",
		tidbCloudOrgID: "org-leader-loss-clear",
		status:         string(meta.TenantPoolBindingUsed),
	}
	sharedDBKey := sharedDBPoolMetricKey{
		kind: sharedDBPoolMetricCapacity, dbPoolID: 987654, dbPoolUUID: "33333333-3333-4333-8333-333333333333",
		tidbCloudOrgID: "org-shared-leader-loss-clear", dimension: "used",
	}
	s := &Server{metrics: newServerMetrics(), leaderWorkersStarted: true}
	metrics.RecordTenantPoolBindings(tenantPoolKey.poolID, tenantPoolKey.tidbCloudOrgID, tenantPoolKey.status, 1)
	s.metrics.syncTenantPoolBindingSnapshot(map[tenantPoolBindingMetricKey]struct{}{tenantPoolKey: {}})
	metrics.RecordSharedDBPoolCapacity(sharedDBKey.tidbCloudOrgID, sharedDBKey.dbPoolID, sharedDBKey.dbPoolUUID, sharedDBKey.dimension, 1)
	s.metrics.syncSharedDBPoolSnapshot(map[sharedDBPoolMetricKey]struct{}{sharedDBKey: {}})

	rec := httptest.NewRecorder()
	s.metrics.writePrometheus(rec)
	if !strings.Contains(rec.Body.String(), `drive9_tenant_pool_bindings{pool_id="pool-leader-loss-clear",status="used",tidbcloud_org_id="org-leader-loss-clear"} 1`) {
		t.Fatalf("missing tenant pool binding metric before leadership loss:\n%s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `drive9_shared_db_pool_capacity{db_pool_id="987654",db_pool_uuid="33333333-3333-4333-8333-333333333333",tidbcloud_org_id="org-shared-leader-loss-clear",type="used"} 1`) {
		t.Fatalf("missing shared DB-pool metric before leadership loss:\n%s", rec.Body.String())
	}

	s.stopLeaderWorkers()

	rec = httptest.NewRecorder()
	s.metrics.writePrometheus(rec)
	if strings.Contains(rec.Body.String(), `drive9_tenant_pool_bindings{pool_id="pool-leader-loss-clear"`) {
		t.Fatalf("tenant pool binding metric should be removed after leadership loss:\n%s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), `drive9_shared_db_pool_capacity{db_pool_id="987654"`) {
		t.Fatalf("shared DB-pool metric should be removed after leadership loss:\n%s", rec.Body.String())
	}
}

func insertTenantMetricTenant(t *testing.T, s *meta.Store, tenantID string, status meta.TenantStatus, now time.Time) {
	t.Helper()
	if err := s.InsertTenant(context.Background(), &meta.Tenant{
		ID:               tenantID,
		Status:           status,
		Kind:             meta.TenantKindLive,
		DBHost:           "db.example.com",
		DBPort:           4000,
		DBUser:           "u.root",
		DBPasswordCipher: []byte("cipher"),
		DBName:           "tidbcloud_fs",
		DBTLS:            true,
		Provider:         "tidb_zero",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatalf("insert tenant %s: %v", tenantID, err)
	}
}

func insertTenantPoolMetricTenant(t *testing.T, s *meta.Store, tenantID, clusterID string, now time.Time) {
	t.Helper()
	if err := s.InsertTenant(context.Background(), &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantActive,
		Kind:             meta.TenantKindLive,
		DBHost:           "db.example.com",
		DBPort:           4000,
		DBUser:           "u.root",
		DBPasswordCipher: []byte("cipher"),
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
}

func insertSharedDBMetricTenant(t *testing.T, s *meta.Store, tenantID string, status meta.TenantStatus, now time.Time) {
	t.Helper()
	if err := s.InsertTenant(context.Background(), &meta.Tenant{
		ID:               tenantID,
		Status:           status,
		Kind:             meta.TenantKindLive,
		DBHost:           "shared.example.com",
		DBPort:           4000,
		DBUser:           "root",
		DBPasswordCipher: []byte("cipher"),
		DBName:           "tidbcloud_fs",
		DBTLS:            true,
		Provider:         tenant.ProviderTiDBCloudNativeShared,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatalf("insert shared tenant %s: %v", tenantID, err)
	}
}
