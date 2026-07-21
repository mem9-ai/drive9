package meta

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/mem9-ai/drive9/pkg/metrics"
)

// MonthlyLLMCostMillicents returns the total LLM cost for a tenant in the
// current calendar month (UTC). Reads from the pre-aggregated counter table
// (tenant_monthly_llm_cost) for O(1) lookups. Tenants without a counter row
// have no recorded usage, so the result is 0.
func (s *Store) MonthlyLLMCostMillicents(ctx context.Context, tenantID string) (total int64, err error) {
	start := time.Now()
	defer func() {
		result := "ok"
		if err != nil {
			result = "error"
		}
		metrics.RecordOperation("meta", "monthly_llm_cost", result, time.Since(start))
	}()
	now := time.Now().UTC()
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)

	err = s.db.QueryRowContext(ctx,
		`SELECT COALESCE(total_mc, 0) FROM tenant_monthly_llm_cost
		 WHERE tenant_id = ? AND month_start = ?`,
		tenantID, monthStart).Scan(&total)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return total, err
}
