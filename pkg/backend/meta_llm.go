package backend

import "context"

// MetaLLMUsageStore is the interface for control-plane LLM usage operations.
// It is satisfied by *meta.Store and by *meta.LLMCostCache.
type MetaLLMUsageStore interface {
	InsertLLMUsage(ctx context.Context, tenantID, taskType, taskID string, costMillicents, rawUnits int64, rawUnitType string) error
	MonthlyLLMCostMillicents(ctx context.Context, tenantID string) (int64, error)
}
