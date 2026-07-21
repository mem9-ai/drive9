package datastore

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
)

// ErrExecSQLNotSupportedShared is returned by ExecSQL when the Store
// addresses a shared-schema (fs_id) database. Raw agent SQL carries no fs_id
// predicates and cannot be rewritten to be tenant-safe, so it is rejected
// outright in shared shape.
var ErrExecSQLNotSupportedShared = errors.New("exec SQL is not supported on shared-schema stores")

var wsNorm = regexp.MustCompile(`\s+`)

func normalizeSQL(s string) string {
	return wsNorm.ReplaceAllString(strings.TrimSpace(s), " ")
}

func (s *Store) ExecSQL(ctx context.Context, query string) (out []map[string]interface{}, err error) {
	start := time.Now()
	defer func() {
		result := "ok"
		if err != nil {
			result = "error"
		}
		metrics.RecordOperation("datastore", "exec_sql", result, time.Since(start))
	}()

	// Shared shape: raw agent SQL cannot be made tenant-safe, so reject it
	// before anything executes.
	if s != nil && s.scope.Shared() {
		err = ErrExecSQLNotSupportedShared
		return nil, err
	}

	q := strings.TrimSpace(query)
	norm := strings.ToUpper(normalizeSQL(q))

	isSelect := strings.HasPrefix(norm, "SELECT")
	if strings.HasPrefix(norm, "WITH") {
		hasDML := strings.Contains(norm, "INSERT") ||
			strings.Contains(norm, "UPDATE") ||
			strings.Contains(norm, "DELETE") ||
			strings.Contains(norm, "DROP") ||
			strings.Contains(norm, "ALTER") ||
			strings.Contains(norm, "TRUNCATE")
		if !hasDML {
			isSelect = true
		}
	}
	isTagWrite := strings.HasPrefix(norm, "INSERT INTO FILE_TAGS") ||
		strings.HasPrefix(norm, "UPDATE FILE_TAGS") ||
		strings.HasPrefix(norm, "DELETE FROM FILE_TAGS")

	if isTagWrite {
		if strings.HasPrefix(norm, "UPDATE") || strings.HasPrefix(norm, "DELETE") {
			if strings.Contains(norm, " JOIN ") || strings.Contains(norm, " USING ") {
				return nil, fmt.Errorf("multi-table DML not allowed; single-table statements on file_tags only")
			}
			if strings.HasPrefix(norm, "UPDATE") {
				setIdx := strings.Index(norm, " SET ")
				if setIdx > 0 && strings.Contains(norm[:setIdx], ",") {
					return nil, fmt.Errorf("multi-table DML not allowed; single-table statements on file_tags only")
				}
			}
			if strings.HasPrefix(norm, "DELETE") {
				fromIdx := strings.Index(norm, " FROM ")
				if fromIdx > 0 {
					rest := norm[fromIdx+6:]
					endIdx := strings.IndexAny(rest, " ;")
					if endIdx < 0 {
						endIdx = len(rest)
					}
					tablePart := rest[:endIdx]
					if strings.Contains(tablePart, ",") {
						return nil, fmt.Errorf("multi-table DML not allowed; single-table statements on file_tags only")
					}
				}
			}
		}
	}

	if !isSelect && !isTagWrite {
		err = fmt.Errorf("only SELECT queries and INSERT/UPDATE/DELETE on file_tags are allowed")
		logger.Warn(ctx, "datastore_exec_sql_rejected", zap.Int("query_len", len(q)), zap.Error(err))
		return nil, err
	}
	if s == nil || s.db == nil {
		err = fmt.Errorf("database is closed")
		logger.Error(ctx, "datastore_exec_sql_closed", zap.Error(err))
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if isTagWrite {
		res, err := s.db.ExecContext(ctx, q)
		if err != nil {
			logger.Error(ctx, "datastore_exec_sql_tag_write_failed", zap.Int("query_len", len(q)), zap.Error(err))
			return nil, err
		}
		affected, _ := res.RowsAffected()
		return []map[string]interface{}{{"rows_affected": affected}}, nil
	}

	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		logger.Error(ctx, "datastore_exec_sql_query_failed", zap.Int("query_len", len(q)), zap.Error(err))
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	const maxRows = 1000
	result := make([]map[string]interface{}, 0)
	for rows.Next() {
		if len(result) >= maxRows {
			break
		}
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(cols))
		for i, col := range cols {
			v := vals[i]
			if b, ok := v.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = v
			}
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		logger.Error(ctx, "datastore_exec_sql_scan_failed", zap.Int("query_len", len(q)), zap.Error(err))
		return nil, err
	}
	return result, nil
}
