package server

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
)

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
			logger.Warn(ctx, "tenant_count_metrics_failed", zap.String("detail", err.Error()))
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
			logger.Warn(ctx, "tenant_pool_binding_metrics_failed", zap.String("detail", err.Error()))
		}
		return
	}
	for _, count := range counts {
		switch count.Status {
		case meta.TenantPoolBindingFree, meta.TenantPoolBindingUsed:
			metrics.RecordTenantPoolBindings(count.PoolID, count.OrganizationID, string(count.Status), count.Count)
		}
	}
}
