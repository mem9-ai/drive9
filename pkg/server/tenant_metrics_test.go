package server

import (
	"context"
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

func TestStopLeaderWorkersClearsTenantPoolBindingSnapshot(t *testing.T) {
	key := tenantPoolBindingMetricKey{
		poolID:         "pool-leader-loss-clear",
		tidbCloudOrgID: "org-leader-loss-clear",
		status:         string(meta.TenantPoolBindingUsed),
	}
	s := &Server{metrics: newServerMetrics(), leaderWorkersStarted: true}
	metrics.RecordTenantPoolBindings(key.poolID, key.tidbCloudOrgID, key.status, 1)
	s.metrics.syncTenantPoolBindingSnapshot(map[tenantPoolBindingMetricKey]struct{}{key: struct{}{}})

	rec := httptest.NewRecorder()
	s.metrics.writePrometheus(rec)
	if !strings.Contains(rec.Body.String(), `drive9_tenant_pool_bindings{pool_id="pool-leader-loss-clear",status="used",tidbcloud_org_id="org-leader-loss-clear"} 1`) {
		t.Fatalf("missing tenant pool binding metric before leadership loss:\n%s", rec.Body.String())
	}

	s.stopLeaderWorkers()

	rec = httptest.NewRecorder()
	s.metrics.writePrometheus(rec)
	if strings.Contains(rec.Body.String(), `drive9_tenant_pool_bindings{pool_id="pool-leader-loss-clear"`) {
		t.Fatalf("tenant pool binding metric should be removed after leadership loss:\n%s", rec.Body.String())
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
