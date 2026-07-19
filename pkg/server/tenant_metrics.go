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
