package server

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
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
			logger.Warn(ctx, "tenant_count_metrics_failed", zap.Error(err))
		}
		return
	}
	metrics.RecordTenantCount("total_non_deleted", counts.TotalNonDeleted)
	metrics.RecordTenantCount("active", counts.Active)
}
