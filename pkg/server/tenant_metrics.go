package server

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
)

type tenantPoolBindingMetricKey struct {
	poolID         string
	tidbCloudOrgID string
	status         string
}

const (
	sharedDBPoolMetricTotal    = "total"
	sharedDBPoolMetricCapacity = "capacity"
	sharedDBPoolMetricTenants  = "tenants"
	sharedDBPoolMetricSpending = "spending_limit"
)

type sharedDBPoolMetricKey struct {
	kind           string
	dbPoolID       int64
	dbPoolUUID     string
	tidbCloudOrgID string
	dimension      string
}

func (s *Server) observeTenantCounts(ctx context.Context) {
	if s.meta == nil {
		return
	}
	start := time.Now()
	counts, err := s.meta.CountTenants(ctx)
	result := metrics.ResultForError(err)
	metrics.RecordOperation("tenant_usage", "count_tenants", result, time.Since(start))
	if err != nil {
		if ctx.Err() == nil {
			logger.Warn(ctx, "tenant_count_metrics_failed", zap.Error(err))
		}
		return
	}
	for _, count := range counts.Statuses {
		metrics.RecordTenantCount(string(count.Status), count.Count)
	}
}

func (s *Server) observeTenantPoolBindingCounts(ctx context.Context) {
	if s.meta == nil {
		return
	}
	start := time.Now()
	counts, err := s.meta.CountTenantPoolBindingsByStatus(ctx)
	result := metrics.ResultForError(err)
	metrics.RecordOperation(adminTenantPoolMetricsComponent, "count_pool_bindings", result, time.Since(start))
	if err != nil {
		if ctx.Err() == nil {
			logger.Warn(ctx, "tenant_pool_binding_metrics_failed", zap.Error(err))
		}
		return
	}
	next := make(map[tenantPoolBindingMetricKey]struct{}, len(counts))
	for _, count := range counts {
		switch count.Status {
		case meta.TenantPoolBindingFree, meta.TenantPoolBindingUsed:
			metrics.RecordTenantPoolBindings(count.PoolID, count.OrganizationID, string(count.Status), count.Count)
			next[tenantPoolBindingMetricKey{
				poolID:         count.PoolID,
				tidbCloudOrgID: count.OrganizationID,
				status:         string(count.Status),
			}] = struct{}{}
		}
	}
	if s.metrics != nil {
		s.metrics.syncTenantPoolBindingSnapshot(next)
	}
}

func (s *Server) observeSharedDBPoolMetrics(ctx context.Context) {
	if s.meta == nil {
		return
	}
	start := time.Now()
	snapshots, err := s.meta.ListSharedDBPoolMetricSnapshots(ctx)
	metrics.RecordOperation("shared_db_pool", "observe", metrics.ResultForError(err), time.Since(start))
	if err != nil {
		if ctx.Err() == nil {
			logger.Warn(ctx, "shared_db_pool_metrics_failed", zap.Error(err))
		}
		return
	}

	next := make(map[sharedDBPoolMetricKey]struct{})
	for _, snapshot := range snapshots {
		orgID := snapshot.TiDBCloudOrganizationID
		totalKey := sharedDBPoolMetricKey{
			kind: sharedDBPoolMetricTotal, dbPoolID: snapshot.ID, dbPoolUUID: snapshot.UUID,
			tidbCloudOrgID: orgID, dimension: snapshot.Status,
		}
		metrics.RecordSharedDBPoolTotal(orgID, snapshot.ID, snapshot.UUID, snapshot.Status, 1)
		next[totalKey] = struct{}{}

		free := int64(0)
		if snapshot.MaxTenants > snapshot.TenantCount && !snapshot.SoftCapReached {
			free = int64(snapshot.MaxTenants - snapshot.TenantCount)
		}
		hardMax := int64(0)
		if snapshot.MaxTenants > 0 {
			if hard, hardErr := s.managedSharedDBHardCap(snapshot.MaxTenants); hardErr == nil {
				hardMax = int64(hard)
			}
		}
		for capacityType, value := range map[string]int64{
			"soft_max": int64(snapshot.MaxTenants), "hard_max": hardMax,
			"used": int64(snapshot.TenantCount), "free": free,
		} {
			metrics.RecordSharedDBPoolCapacity(orgID, snapshot.ID, snapshot.UUID, capacityType, value)
			next[sharedDBPoolMetricKey{
				kind: sharedDBPoolMetricCapacity, dbPoolID: snapshot.ID, dbPoolUUID: snapshot.UUID,
				tidbCloudOrgID: orgID, dimension: capacityType,
			}] = struct{}{}
		}
		for _, state := range snapshot.TenantStates {
			metrics.RecordSharedDBPoolTenants(orgID, snapshot.ID, snapshot.UUID, string(state.State), state.Count)
			next[sharedDBPoolMetricKey{
				kind: sharedDBPoolMetricTenants, dbPoolID: snapshot.ID, dbPoolUUID: snapshot.UUID,
				tidbCloudOrgID: orgID, dimension: string(state.State),
			}] = struct{}{}
		}
		if snapshot.SpendingLimit != nil {
			metrics.RecordSharedDBPoolSpendingLimit(orgID, snapshot.ID, snapshot.UUID, "target", *snapshot.SpendingLimit)
			next[sharedDBPoolMetricKey{
				kind: sharedDBPoolMetricSpending, dbPoolID: snapshot.ID, dbPoolUUID: snapshot.UUID,
				tidbCloudOrgID: orgID, dimension: "target",
			}] = struct{}{}
		}
	}
	if s.metrics != nil {
		s.metrics.syncSharedDBPoolSnapshot(next)
	}
}

func (m *serverMetrics) syncTenantPoolBindingSnapshot(next map[tenantPoolBindingMetricKey]struct{}) {
	if m == nil {
		return
	}
	m.tenantPoolBindMu.Lock()
	defer m.tenantPoolBindMu.Unlock()
	for prev := range m.tenantPoolBinding {
		if _, ok := next[prev]; ok {
			continue
		}
		metrics.DeleteTenantPoolBindings(prev.poolID, prev.tidbCloudOrgID, prev.status)
	}
	m.tenantPoolBinding = next
}

func (m *serverMetrics) clearTenantPoolBindingSnapshot() {
	if m == nil {
		return
	}
	m.tenantPoolBindMu.Lock()
	defer m.tenantPoolBindMu.Unlock()
	for prev := range m.tenantPoolBinding {
		metrics.DeleteTenantPoolBindings(prev.poolID, prev.tidbCloudOrgID, prev.status)
	}
	m.tenantPoolBinding = map[tenantPoolBindingMetricKey]struct{}{}
}

func (m *serverMetrics) syncSharedDBPoolSnapshot(next map[sharedDBPoolMetricKey]struct{}) {
	if m == nil {
		return
	}
	m.sharedDBPoolMu.Lock()
	defer m.sharedDBPoolMu.Unlock()
	for prev := range m.sharedDBPool {
		if _, ok := next[prev]; ok {
			continue
		}
		deleteSharedDBPoolMetric(prev)
	}
	m.sharedDBPool = next
}

func (m *serverMetrics) clearSharedDBPoolSnapshot() {
	if m == nil {
		return
	}
	m.sharedDBPoolMu.Lock()
	defer m.sharedDBPoolMu.Unlock()
	for prev := range m.sharedDBPool {
		deleteSharedDBPoolMetric(prev)
	}
	m.sharedDBPool = map[sharedDBPoolMetricKey]struct{}{}
}

func deleteSharedDBPoolMetric(key sharedDBPoolMetricKey) {
	switch key.kind {
	case sharedDBPoolMetricTotal:
		metrics.DeleteSharedDBPoolTotal(key.tidbCloudOrgID, key.dbPoolID, key.dbPoolUUID, key.dimension)
	case sharedDBPoolMetricCapacity:
		metrics.DeleteSharedDBPoolCapacity(key.tidbCloudOrgID, key.dbPoolID, key.dbPoolUUID, key.dimension)
	case sharedDBPoolMetricTenants:
		metrics.DeleteSharedDBPoolTenants(key.tidbCloudOrgID, key.dbPoolID, key.dbPoolUUID, key.dimension)
	case sharedDBPoolMetricSpending:
		metrics.DeleteSharedDBPoolSpendingLimit(key.tidbCloudOrgID, key.dbPoolID, key.dbPoolUUID, key.dimension)
	}
}
