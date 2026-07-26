package meta

import (
	"strings"
	"testing"
)

func TestCountTenantPoolBindingsByStatusSQLAggregatesBeforeTenantLookup(t *testing.T) {
	for _, fragment := range []string{
		"FROM tenant_tidbcloud_org_bindings\n\t\tWHERE pool_id <> ''\n\t\tGROUP BY organization_id, pool_id, pool_status",
		"FROM tenant_pool_memberships\n\t\tWHERE pool_id <> ''\n\t\tGROUP BY tidbcloud_organization_id, pool_id, pool_status",
		"FROM tenants t\n\t\tSTRAIGHT_JOIN tenant_tidbcloud_org_bindings b ON b.tenant_id = t.id",
		"FROM tenants t\n\t\tSTRAIGHT_JOIN tenant_pool_memberships m ON m.tenant_id = t.id",
	} {
		if !strings.Contains(countTenantPoolBindingsByStatusSQL, fragment) {
			t.Fatalf("optimized pool binding count query missing %q:\n%s", fragment, countTenantPoolBindingsByStatusSQL)
		}
	}
	if strings.Contains(countTenantPoolBindingsByStatusSQL, "LEFT JOIN tenants t") {
		t.Fatalf("pool binding count query still looks up every tenant row:\n%s", countTenantPoolBindingsByStatusSQL)
	}
}

func TestListSharedDBPoolMetricSnapshotsSQLReclassifiesOnlyNonActiveTenants(t *testing.T) {
	for _, fragment := range []string{
		"FROM tenants t FORCE INDEX (idx_tenant_status)",
		"STRAIGHT_JOIN fs_registry f ON f.tenant_id = t.id",
		"STRAIGHT_JOIN tenant_placements p ON p.fs_id = f.fs_id",
		"WHERE t.status <> ?",
		"SELECT db_id, ? AS tenant_status, COUNT(*) AS tenant_count\n\t\tFROM tenant_placements\n\t\tGROUP BY db_id",
		"SELECT db_id, ? AS tenant_status, -SUM(tenant_count) AS tenant_count",
	} {
		if !strings.Contains(listSharedDBPoolMetricSnapshotsSQL, fragment) {
			t.Fatalf("optimized shared DB snapshot query missing %q:\n%s", fragment, listSharedDBPoolMetricSnapshotsSQL)
		}
	}
	if strings.Contains(listSharedDBPoolMetricSnapshotsSQL, "FROM tenant_placements p\n\t\t\tJOIN fs_registry") {
		t.Fatalf("shared DB snapshot query still looks up every placement's tenant:\n%s", listSharedDBPoolMetricSnapshotsSQL)
	}
}

func TestObservePendingMutationsSQLAggregatesBeforeOrganizationLookups(t *testing.T) {
	for _, fragment := range []string{
		"WITH pending_mutations AS (",
		"SELECT tenant_id, COUNT(*) AS pending_count, MIN(created_at) AS oldest_created_at",
		"FROM quota_mutation_log FORCE INDEX (idx_pending_tenant_age)",
		"WHERE status = 'pending'\n\t\tGROUP BY tenant_id",
		"pending.pending_count, pending.oldest_created_at",
		"FROM pending_mutations pending\n\tLEFT JOIN tenants t ON t.id = pending.tenant_id",
	} {
		if !strings.Contains(observePendingMutationsSQL, fragment) {
			t.Fatalf("optimized pending mutation query missing %q:\n%s", fragment, observePendingMutationsSQL)
		}
	}
	if strings.Contains(observePendingMutationsSQL, "COUNT(*), MIN(q.created_at)") {
		t.Fatalf("pending mutation query still joins metadata before aggregation:\n%s", observePendingMutationsSQL)
	}
}
