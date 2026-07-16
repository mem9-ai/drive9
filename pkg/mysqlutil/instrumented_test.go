package mysqlutil

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/mem9-ai/drive9/pkg/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestObserveDBOperationLogsSQLTraceWithoutArgs(t *testing.T) {
	t.Setenv("DRIVE9_DB_TRACE_LOG_ENABLED", "true")
	logger.ResetBenchTimingLogEnabledForTest()
	logger.ResetDBTraceLogEnabledForTest()
	t.Cleanup(logger.ResetBenchTimingLogEnabledForTest)
	t.Cleanup(logger.ResetDBTraceLogEnabledForTest)

	core, recorded := observer.New(zap.InfoLevel)
	prevLogger := logger.L()
	logger.Set(zap.New(core))
	t.Cleanup(func() { logger.Set(prevLogger) })

	ctx := context.Background()
	start := time.Now().Add(-350 * time.Millisecond)
	observeDBOperation(ctx, RoleUser, "exec", "  INSERT INTO file_nodes\n(path, name) VALUES (?, ?)  ", start, errors.New("duplicate key"))

	entries := recorded.FilterMessage("db_operation_timing").All()
	if len(entries) != 1 {
		t.Fatalf("db_operation_timing entries = %d, want 1", len(entries))
	}
	fields := entries[0].ContextMap()
	if fields["role"] != RoleUser {
		t.Errorf("role field = %v, want %s", fields["role"], RoleUser)
	}
	if fields["operation"] != "exec" {
		t.Errorf("operation field = %v, want exec", fields["operation"])
	}
	if fields["result"] != "error" {
		t.Errorf("result field = %v, want error", fields["result"])
	}
	if fields["sql"] != "INSERT INTO file_nodes (path, name) VALUES (?, ?)" {
		t.Errorf("sql field = %v", fields["sql"])
	}
	if _, ok := fields["args"]; ok {
		t.Errorf("db trace log must not include args: %#v", fields)
	}
	if fields["duration_ms"] == nil {
		t.Errorf("duration_ms field missing: %#v", fields)
	}
}

func TestObserveDBOperationLogsSQLTraceWhenDBTraceEnabled(t *testing.T) {
	t.Setenv("DRIVE9_BENCH_TIMING_LOG_ENABLED", "false")
	t.Setenv("DRIVE9_DB_TRACE_LOG_ENABLED", "true")
	logger.ResetBenchTimingLogEnabledForTest()
	logger.ResetDBTraceLogEnabledForTest()
	t.Cleanup(logger.ResetBenchTimingLogEnabledForTest)
	t.Cleanup(logger.ResetDBTraceLogEnabledForTest)

	core, recorded := observer.New(zap.InfoLevel)
	prevLogger := logger.L()
	logger.Set(zap.New(core))
	t.Cleanup(func() { logger.Set(prevLogger) })

	observeDBOperation(context.Background(), RoleUser, "query", "SELECT * FROM file_nodes WHERE path = ?", time.Now().Add(-350*time.Millisecond), nil)

	entries := recorded.FilterMessage("db_operation_timing").All()
	if len(entries) != 1 {
		t.Fatalf("db_operation_timing entries = %d, want 1", len(entries))
	}
	fields := entries[0].ContextMap()
	if fields["sql"] != "SELECT * FROM file_nodes WHERE path = ?" {
		t.Errorf("sql field = %v", fields["sql"])
	}
}

func TestNormalizeSQLForTraceRedactsLiterals(t *testing.T) {
	query := `SELECT * FROM files
		WHERE name = 'agent secret'
		AND path = "project literal"
		AND size > 123
		AND note = 'can''t leak'
		AND raw = 'can\'t leak either'`
	want := `SELECT * FROM files WHERE name = ? AND path = ? AND size > ? AND note = ? AND raw = ?`
	if got := normalizeSQLForTrace(query); got != want {
		t.Fatalf("normalizeSQLForTrace() = %q, want %q", got, want)
	}
}

func TestNormalizeSQLForTraceStripsComments(t *testing.T) {
	query := `SELECT '/* quoted@example.com */', "-- quoted-dash@example.com", '# quoted-hash@example.com'
		/* block@example.com */ FROM files -- dash@example.com
		WHERE id = 42 # hash@example.com`
	want := `SELECT ?, ?, ? FROM files WHERE id = ?`
	got := normalizeSQLForTrace(query)
	if got != want {
		t.Fatalf("normalizeSQLForTrace() = %q, want %q", got, want)
	}
	for _, secret := range []string{"block@example.com", "dash@example.com", "hash@example.com"} {
		if strings.Contains(got, secret) {
			t.Fatalf("normalizeSQLForTrace leaked comment secret %q in %q", secret, got)
		}
	}
}

func TestObserveDBOperationLogsSafeErrorDetails(t *testing.T) {
	t.Setenv("DRIVE9_DB_TRACE_LOG_ENABLED", "true")
	logger.ResetBenchTimingLogEnabledForTest()
	logger.ResetDBTraceLogEnabledForTest()
	t.Cleanup(logger.ResetBenchTimingLogEnabledForTest)
	t.Cleanup(logger.ResetDBTraceLogEnabledForTest)

	core, recorded := observer.New(zap.InfoLevel)
	prevLogger := logger.L()
	logger.Set(zap.New(core))
	t.Cleanup(func() { logger.Set(prevLogger) })

	const secret = "CHILD_SECRET"
	dbErr := &mysql.MySQLError{
		Number:  1062,
		Message: "Duplicate entry '" + secret + "' for key 'files.name'",
	}
	observeDBOperation(context.Background(), RoleUser, "exec", "INSERT INTO files(name) VALUES (?)", time.Now().Add(-350*time.Millisecond), dbErr)

	entries := recorded.FilterMessage("db_operation_timing").All()
	if len(entries) != 1 {
		t.Fatalf("db_operation_timing entries = %d, want 1", len(entries))
	}
	fields := entries[0].ContextMap()
	if _, ok := fields["error"]; ok {
		t.Errorf("db trace log must not include raw error field: %#v", fields)
	}
	if fields["db_error_type"] != "mysql" {
		t.Errorf("db_error_type = %v, want mysql", fields["db_error_type"])
	}
	if fmt.Sprint(fields["db_error_number"]) != "1062" {
		t.Errorf("db_error_number = %v, want 1062", fields["db_error_number"])
	}
	assertFieldsDoNotContain(t, fields, secret)
}

func TestObserveDBOperationBoundsSQLTraceField(t *testing.T) {
	t.Setenv("DRIVE9_DB_TRACE_LOG_ENABLED", "true")
	logger.ResetBenchTimingLogEnabledForTest()
	logger.ResetDBTraceLogEnabledForTest()
	t.Cleanup(logger.ResetBenchTimingLogEnabledForTest)
	t.Cleanup(logger.ResetDBTraceLogEnabledForTest)

	core, recorded := observer.New(zap.InfoLevel)
	prevLogger := logger.L()
	logger.Set(zap.New(core))
	t.Cleanup(func() { logger.Set(prevLogger) })

	query := "SELECT " + strings.Repeat("x", maxSQLTraceLen*3) + " FROM files"
	observeDBOperation(context.Background(), RoleUser, "query", query, time.Now().Add(-350*time.Millisecond), nil)

	entries := recorded.FilterMessage("db_operation_timing").All()
	if len(entries) != 1 {
		t.Fatalf("db_operation_timing entries = %d, want 1", len(entries))
	}
	fields := entries[0].ContextMap()
	sqlText, ok := fields["sql"].(string)
	if !ok {
		t.Fatalf("sql field = %T, want string", fields["sql"])
	}
	if len(sqlText) != maxSQLTraceLen {
		t.Fatalf("sql field len = %d, want %d", len(sqlText), maxSQLTraceLen)
	}
	if fields["sql_truncated"] != true {
		t.Errorf("sql_truncated = %v, want true", fields["sql_truncated"])
	}
	if fmt.Sprint(fields["sql_len"]) != fmt.Sprint(len(query)) {
		t.Errorf("sql_len = %v, want %d", fields["sql_len"], len(query))
	}
}

func TestObserveDBOperationSkipsTraceFieldsWhenBenchTimingDisabled(t *testing.T) {
	t.Setenv("DRIVE9_BENCH_TIMING_LOG_ENABLED", "false")
	t.Setenv("DRIVE9_DB_TRACE_LOG_ENABLED", "false")
	logger.ResetBenchTimingLogEnabledForTest()
	logger.ResetDBTraceLogEnabledForTest()
	t.Cleanup(logger.ResetBenchTimingLogEnabledForTest)
	t.Cleanup(logger.ResetDBTraceLogEnabledForTest)

	core, recorded := observer.New(zap.InfoLevel)
	prevLogger := logger.L()
	logger.Set(zap.New(core))
	t.Cleanup(func() { logger.Set(prevLogger) })

	res := &recordingResult{}
	observeDBOperation(context.Background(), RoleUser, "exec", "SELECT 'secret'", time.Now(), nil, res)

	if entries := recorded.FilterMessage("db_operation_timing").All(); len(entries) != 0 {
		t.Fatalf("db_operation_timing entries = %d, want 0", len(entries))
	}
	if res.rowsAffectedCalled {
		t.Fatal("RowsAffected called while DB trace logs are disabled")
	}
}

func TestObserveDBOperationSkipsTraceWhenOnlyBenchTimingEnabled(t *testing.T) {
	t.Setenv("DRIVE9_BENCH_TIMING_LOG_ENABLED", "true")
	t.Setenv("DRIVE9_DB_TRACE_LOG_ENABLED", "false")
	logger.ResetBenchTimingLogEnabledForTest()
	logger.ResetDBTraceLogEnabledForTest()
	t.Cleanup(logger.ResetBenchTimingLogEnabledForTest)
	t.Cleanup(logger.ResetDBTraceLogEnabledForTest)

	core, recorded := observer.New(zap.InfoLevel)
	prevLogger := logger.L()
	logger.Set(zap.New(core))
	t.Cleanup(func() { logger.Set(prevLogger) })

	observeDBOperation(context.Background(), RoleUser, "query", "SELECT * FROM file_nodes", time.Now(), nil)

	if entries := recorded.FilterMessage("db_operation_timing").All(); len(entries) != 0 {
		t.Fatalf("db_operation_timing entries = %d, want 0", len(entries))
	}
}

func TestObserveDBOperationSkipsTraceBelowDefaultSlowThreshold(t *testing.T) {
	t.Setenv("DRIVE9_DB_TRACE_LOG_ENABLED", "true")
	logger.ResetDBTraceLogEnabledForTest()
	logger.ResetDBSlowTraceThresholdForTest()
	t.Cleanup(logger.ResetDBTraceLogEnabledForTest)
	t.Cleanup(logger.ResetDBSlowTraceThresholdForTest)

	core, recorded := observer.New(zap.InfoLevel)
	prevLogger := logger.L()
	logger.Set(zap.New(core))
	t.Cleanup(func() { logger.Set(prevLogger) })

	observeDBOperation(context.Background(), RoleUser, "query", "SELECT * FROM file_nodes", time.Now().Add(-50*time.Millisecond), nil)

	if entries := recorded.FilterMessage("db_operation_timing").All(); len(entries) != 0 {
		t.Fatalf("db_operation_timing entries = %d, want 0", len(entries))
	}
}

func TestObserveDBOperationLogsTraceAboveDefaultSlowThreshold(t *testing.T) {
	logger.ResetDBTraceLogEnabledForTest()
	logger.ResetDBSlowTraceThresholdForTest()
	t.Cleanup(logger.ResetDBTraceLogEnabledForTest)
	t.Cleanup(logger.ResetDBSlowTraceThresholdForTest)

	core, recorded := observer.New(zap.InfoLevel)
	prevLogger := logger.L()
	logger.Set(zap.New(core))
	t.Cleanup(func() { logger.Set(prevLogger) })

	observeDBOperation(context.Background(), RoleUser, "query", "SELECT * FROM file_nodes", time.Now().Add(-350*time.Millisecond), nil)

	if entries := recorded.FilterMessage("db_operation_timing").All(); len(entries) != 1 {
		t.Fatalf("db_operation_timing entries = %d, want 1", len(entries))
	}
}

func TestObserveDBOperationLogsTraceWhenSlowThresholdIsZero(t *testing.T) {
	t.Setenv("DRIVE9_DB_TRACE_LOG_ENABLED", "true")
	t.Setenv("DRIVE9_DB_SLOW_TRACE_MS", "0")
	logger.ResetDBTraceLogEnabledForTest()
	logger.ResetDBSlowTraceThresholdForTest()
	t.Cleanup(logger.ResetDBTraceLogEnabledForTest)
	t.Cleanup(logger.ResetDBSlowTraceThresholdForTest)

	core, recorded := observer.New(zap.InfoLevel)
	prevLogger := logger.L()
	logger.Set(zap.New(core))
	t.Cleanup(func() { logger.Set(prevLogger) })

	observeDBOperation(context.Background(), RoleUser, "query", "SELECT * FROM file_nodes", time.Now(), nil)

	if entries := recorded.FilterMessage("db_operation_timing").All(); len(entries) != 1 {
		t.Fatalf("db_operation_timing entries = %d, want 1", len(entries))
	}
}

type recordingResult struct {
	rowsAffectedCalled bool
}

func (r *recordingResult) LastInsertId() (int64, error) {
	return 0, driver.ErrSkip
}

func (r *recordingResult) RowsAffected() (int64, error) {
	r.rowsAffectedCalled = true
	return 1, nil
}

func assertFieldsDoNotContain(t *testing.T, fields map[string]interface{}, secret string) {
	t.Helper()
	for key, value := range fields {
		if strings.Contains(fmt.Sprint(value), secret) {
			t.Fatalf("field %q leaked %q in %#v", key, secret, fields)
		}
	}
}
