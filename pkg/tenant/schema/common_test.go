package schema

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestIsExpectedSchemaInitRetryError(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		wantTransient bool
		wantExpected  bool
	}{
		{
			name:          "tidb information schema changed code",
			err:           &mysql.MySQLError{Number: 8028, Message: "Information schema is changed during the execution of the statement [try again later]"},
			wantTransient: true,
			wantExpected:  true,
		},
		{
			name:          "wrapped tidb retry message",
			err:           fmt.Errorf("exec ddl: %w", &mysql.MySQLError{Number: 1105, Message: "Information schema is changed during the execution of the statement [try again later]"}),
			wantTransient: true,
			wantExpected:  true,
		},
		{
			name:          "duplicate column",
			err:           &mysql.MySQLError{Number: 1060, Message: "Duplicate column name 'worktree_name'"},
			wantTransient: false,
			wantExpected:  true,
		},
		{
			name:          "table already exists",
			err:           &mysql.MySQLError{Number: 1050, Message: "Table 'fs_nodes' already exists"},
			wantTransient: false,
			wantExpected:  true,
		},
		{
			name:          "ordinary ddl error",
			err:           &mysql.MySQLError{Number: 1146, Message: "Table 'missing' doesn't exist"},
			wantTransient: false,
			wantExpected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.wantTransient, IsTransientSchemaError(tt.err))
			require.Equal(t, tt.wantExpected, IsExpectedSchemaInitRetryError(tt.err))
		})
	}
}

func TestExecSchemaStatementRetriesTransientInformationSchemaError(t *testing.T) {
	var attempts atomic.Int32
	transient := &mysql.MySQLError{Number: 8028, Message: "Information schema is changed during the execution of the statement [try again later]"}
	db := newTestRepairDB(t, func(string) testRepairQueryResult {
		return testRepairQueryResult{}
	}, func(string) error {
		if attempts.Add(1) <= 2 {
			return transient
		}
		return nil
	})
	err := execSchemaStatement(context.Background(), db, `ALTER TABLE uploads ADD COLUMN mode INT UNSIGNED NOT NULL DEFAULT 420`, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int32(3), attempts.Load())
}

func TestExecSchemaStatementsParallelRetriesTransientInformationSchemaError(t *testing.T) {
	var uploadAlterAttempts atomic.Int32
	var inodeExecs atomic.Int32
	transient := &mysql.MySQLError{Number: 8028, Message: "Information schema is changed during the execution of the statement [try again later]"}
	db := newTestRepairDB(t, func(string) testRepairQueryResult {
		return testRepairQueryResult{}
	}, func(query string) error {
		if strings.HasPrefix(query, "ALTER TABLE uploads") {
			if uploadAlterAttempts.Add(1) <= 2 {
				return transient
			}
			return nil
		}
		if strings.Contains(query, "inodes") {
			inodeExecs.Add(1)
		}
		return nil
	})

	stmts := []string{
		`CREATE TABLE IF NOT EXISTS uploads (upload_id VARCHAR(64) PRIMARY KEY)`,
		`ALTER TABLE uploads ADD COLUMN mode INT UNSIGNED NOT NULL DEFAULT 420`,
		`CREATE TABLE IF NOT EXISTS inodes (inode_id BIGINT PRIMARY KEY)`,
		`ALTER TABLE inodes ADD COLUMN nlink INT NOT NULL DEFAULT 1`,
	}
	err := ExecSchemaStatementsParallelByTableContext(context.Background(), db, stmts)
	require.NoError(t, err)
	require.Equal(t, int32(3), uploadAlterAttempts.Load())
	require.Equal(t, int32(2), inodeExecs.Load())
}

func TestExecOptionalSchemaStatementsRetriesTransientInformationSchemaError(t *testing.T) {
	var attempts atomic.Int32
	transient := &mysql.MySQLError{Number: 8028, Message: "Information schema is changed during the execution of the statement [try again later]"}
	db := newTestRepairDB(t, func(string) testRepairQueryResult {
		return testRepairQueryResult{}
	}, func(string) error {
		if attempts.Add(1) <= 2 {
			return transient
		}
		return nil
	})
	skipped, err := ExecOptionalSchemaStatements(context.Background(), db, []string{
		`ALTER TABLE semantic ADD FULLTEXT INDEX idx_semantic_fts_content(content_text) WITH PARSER MULTILINGUAL`,
	})
	require.NoError(t, err)
	require.Zero(t, skipped)
	require.Equal(t, int32(3), attempts.Load())
}

func TestExecSchemaStatementStopsWhenRetryWaitIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transient := &mysql.MySQLError{Number: 8028, Message: "Information schema is changed during the execution of the statement [try again later]"}
	db := newTestRepairDB(t, func(string) testRepairQueryResult {
		return testRepairQueryResult{}
	}, func(string) error {
		cancel()
		return transient
	})
	err := execSchemaStatement(ctx, db, `ALTER TABLE uploads ADD COLUMN mode INT UNSIGNED NOT NULL DEFAULT 420`, 1, 1)
	require.ErrorIs(t, err, context.Canceled)
}

func TestSchemaRetryDelayCaps(t *testing.T) {
	require.Equal(t, 50*time.Millisecond, schemaRetryDelay(1))
	require.Equal(t, 100*time.Millisecond, schemaRetryDelay(2))
	require.Equal(t, schemaStatementRetryMax, schemaRetryDelay(8))
}

func TestExecSchemaStatementExhaustsMaxAttemptsOnPersistentTransientError(t *testing.T) {
	var attempts atomic.Int32
	transient := &mysql.MySQLError{Number: 8028, Message: "Information schema is changed during the execution of the statement [try again later]"}
	db := newTestRepairDB(t, func(string) testRepairQueryResult {
		return testRepairQueryResult{}
	}, func(string) error {
		attempts.Add(1)
		return transient
	})
	err := execSchemaStatement(context.Background(), db, `ALTER TABLE uploads ADD COLUMN mode INT UNSIGNED NOT NULL DEFAULT 420`, 1, 1)
	require.ErrorIs(t, err, transient)
	require.Equal(t, int32(maxSchemaStatementAttempts), attempts.Load())
}

func TestExecSchemaStatementFailsImmediatelyOnNonRetryableError(t *testing.T) {
	var attempts atomic.Int32
	nonRetryable := &mysql.MySQLError{Number: 1146, Message: "Table 'missing' doesn't exist"}
	db := newTestRepairDB(t, func(string) testRepairQueryResult {
		return testRepairQueryResult{}
	}, func(string) error {
		attempts.Add(1)
		return nonRetryable
	})
	err := execSchemaStatement(context.Background(), db, `ALTER TABLE missing ADD COLUMN mode INT UNSIGNED NOT NULL DEFAULT 420`, 1, 1)
	require.ErrorIs(t, err, nonRetryable)
	require.Equal(t, int32(1), attempts.Load())
}
