package datastore

import (
	"time"
)

// InsertLLMUsage records one billable LLM call.
func (s *Store) InsertLLMUsage(taskType, taskID string, costMillicents, rawUnits int64, rawUnitType string) error {
	_, err := s.db.Exec(`INSERT INTO llm_usage (task_type, task_id, cost_millicents, raw_units, raw_unit_type, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		taskType, taskID, costMillicents, rawUnits, rawUnitType, time.Now().UTC())
	return err
}

// MonthlyLLMCostMillicents returns the sum of cost_millicents for the current
// calendar month (UTC). Returns 0 on error (fail-open).
func (s *Store) MonthlyLLMCostMillicents() (int64, error) {
	now := time.Now().UTC()
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	var total int64
	err := s.db.QueryRow(`SELECT COALESCE(SUM(cost_millicents), 0) FROM llm_usage WHERE created_at >= ?`, monthStart).Scan(&total)
	return total, err
}
