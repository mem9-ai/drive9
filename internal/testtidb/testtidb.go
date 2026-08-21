// Package testtidb starts a shared TiDB instance for tests.
package testtidb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	envDSN       = "DRIVE9_TEST_TIDB_DSN"
	tidbImage    = "pingcap/tidb:v8.5.6"
	tidbPort     = "4000/tcp"
	testDatabase = "drive9_test"
	startTimeout = 4 * time.Minute
)

type Instance struct {
	DSN       string
	terminate func(context.Context) error
}

func (i *Instance) Close(ctx context.Context) error {
	if i == nil || i.terminate == nil {
		return nil
	}
	return i.terminate(ctx)
}

func Start(ctx context.Context) (*Instance, error) {
	if dsn := strings.TrimSpace(os.Getenv(envDSN)); dsn != "" {
		return &Instance{DSN: dsn}, nil
	}

	ctx, cancel := context.WithTimeout(ctx, startTimeout)
	defer cancel()

	var c testcontainers.Container
	var err error
	for attempt := 1; ; attempt++ {
		c, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        tidbImage,
				ExposedPorts: []string{tidbPort},
				// pingcap/tidb is tidb-server. Without unistore it expects an
				// external PD and never becomes a usable MySQL endpoint.
				Cmd: []string{"--store=unistore", "--path=/tmp/tidb"},
				WaitingFor: wait.ForAll(
					wait.ForLog("server is running MySQL protocol"),
					wait.ForListeningPort(tidbPort),
				).WithDeadline(2 * time.Minute),
			},
			Started: true,
		})
		if err == nil || attempt >= 3 || !isPortBindConflict(err) {
			break
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("start tidb container: %w", ctx.Err())
		case <-time.After(time.Duration(attempt) * 2 * time.Second):
		}
	}
	if err != nil {
		return nil, fmt.Errorf("start tidb container: %w", err)
	}

	host, err := c.Host(ctx)
	if err != nil {
		_ = c.Terminate(context.Background())
		return nil, fmt.Errorf("tidb container host: %w", err)
	}
	mapped, err := c.MappedPort(ctx, tidbPort)
	if err != nil {
		_ = c.Terminate(context.Background())
		return nil, fmt.Errorf("tidb container port: %w", err)
	}
	adminDSN := fmt.Sprintf("root@tcp(%s:%s)/?parseTime=true", host, mapped.Port())
	if err := waitAndCreateTestDatabase(ctx, adminDSN); err != nil {
		_ = c.Terminate(context.Background())
		return nil, err
	}

	return &Instance{
		DSN: fmt.Sprintf("root@tcp(%s:%s)/%s?parseTime=true", host, mapped.Port(), testDatabase),
		terminate: func(ctx context.Context) error {
			return c.Terminate(ctx)
		},
	}, nil
}

func waitAndCreateTestDatabase(ctx context.Context, adminDSN string) error {
	db, err := sql.Open("mysql", adminDSN)
	if err != nil {
		return fmt.Errorf("open tidb admin: %w", err)
	}
	defer func() { _ = db.Close() }()
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(2 * time.Minute)
	}
	var pingErr error
	for {
		pingErr = db.PingContext(ctx)
		if pingErr == nil {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("ping tidb: %w", pingErr)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("ping tidb: %w", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
	if _, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+testDatabase+"`"); err != nil {
		return fmt.Errorf("create test database: %w", err)
	}
	return nil
}

func OpenDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	if dsn == "" {
		t.Fatal("tidb test DSN is empty")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open tidb test db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func ResetDB(t *testing.T, db *sql.DB) {
	t.Helper()
	execResetQueries(t, db, append(resetDeleteQueries, "DELETE FROM files"))
	// DELETE does not rewind TiDB AUTO_INCREMENT (unlike MySQL). Tests that
	// assert seq=1 / SSE since=1 need a real reset.
	truncateAutoIncrementTable(t, db, "fs_events")
}

// ResetMetaDB clears control-plane tenant metadata tables for meta store tests.
func ResetMetaDB(t *testing.T, db *sql.DB) {
	t.Helper()
	queries := []string{
		"DELETE FROM tenant_api_key_fs_scopes",
		"DELETE FROM tenant_api_keys",
		"DELETE FROM tenant_external_bindings",
		"DELETE FROM tenant_tidbcloud_org_bindings",
		"DELETE FROM tenant_tidbcloud_pools",
		"DELETE FROM tenants",
		"DELETE FROM tenant_notify_outbox",
		"DELETE FROM tenant_outbox_cursor",
	}
	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			if isMissingTableError(err) {
				continue
			}
			t.Fatalf("reset meta test db: %v", err)
		}
	}
}

var resetDeleteQueries = []string{
	"DELETE FROM fs_layer_checkpoints",
	"DELETE FROM fs_layer_events",
	"DELETE FROM fs_layer_tags",
	"DELETE FROM fs_layer_entries",
	"DELETE FROM fs_layers",
	"DELETE FROM quota_outbox",
	"DELETE FROM quota_admission_locks",
	"DELETE FROM file_gc_tasks",
	"DELETE FROM semantic_tasks",
	"DELETE FROM file_nodes",
	"DELETE FROM file_tags",
	"DELETE FROM uploads",
	"DELETE FROM inodes",
	"DELETE FROM contents",
	"DELETE FROM semantic",
}

// ResetDBWithoutFiles is like ResetDB but for tests that intentionally drop
// the legacy files table to exercise the no-legacy-table code path.
func ResetDBWithoutFiles(t *testing.T, db *sql.DB) {
	t.Helper()
	execResetQueries(t, db, resetDeleteQueries)
	truncateAutoIncrementTable(t, db, "fs_events")
}

func execResetQueries(t *testing.T, db *sql.DB, queries []string) {
	t.Helper()
	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			if isMissingTableError(err) {
				continue
			}
			t.Fatalf("reset test db: %v", err)
		}
	}
}

func truncateAutoIncrementTable(t *testing.T, db *sql.DB, table string) {
	t.Helper()
	if _, err := db.Exec("TRUNCATE TABLE `" + table + "`"); err != nil {
		if isMissingTableError(err) {
			return
		}
		t.Fatalf("truncate %s: %v", table, err)
	}
}

func isPortBindConflict(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "failed to bind host port") &&
		strings.Contains(msg, "address already in use")
}

func isMissingTableError(err error) bool {
	if err == nil {
		return false
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1146 {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "error 1146") ||
		(strings.Contains(msg, "table") && strings.Contains(msg, "doesn't exist"))
}
