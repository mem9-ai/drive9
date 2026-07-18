package testmysql

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
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
)

const envDSN = "DRIVE9_TEST_MYSQL_DSN"

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
	if dsn := os.Getenv(envDSN); dsn != "" {
		return &Instance{DSN: dsn}, nil
	}

	ctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()

	c, err := tcmysql.Run(ctx,
		"mysql:8.0.36",
		tcmysql.WithDatabase("dat9_test"),
		tcmysql.WithUsername("dat9"),
		tcmysql.WithPassword("dat9pass"),
	)
	if err != nil {
		return nil, fmt.Errorf("start mysql container: %w", err)
	}

	// parseTime=true lets DATETIME scan directly into time.Time.
	dsn, err := c.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		_ = c.Terminate(context.Background())
		return nil, fmt.Errorf("build mysql dsn: %w", err)
	}

	return &Instance{
		DSN: dsn,
		terminate: func(ctx context.Context) error {
			return c.Terminate(ctx)
		},
	}, nil
}

func OpenDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	if dsn == "" {
		t.Fatal("mysql test DSN is empty")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql test db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func ResetDB(t *testing.T, db *sql.DB) {
	t.Helper()
	queries := []string{
		"DELETE FROM fs_layer_checkpoints",
		"DELETE FROM fs_layer_events",
		"DELETE FROM fs_layer_tags",
		"DELETE FROM fs_layer_entries",
		"DELETE FROM fs_layers",
		"DELETE FROM quota_outbox",
		"DELETE FROM quota_admission_locks",
		"DELETE FROM file_gc_tasks",
		"DELETE FROM fs_events",
		"DELETE FROM semantic_tasks",
		"DELETE FROM file_nodes",
		"DELETE FROM file_tags",
		"DELETE FROM uploads",
		"DELETE FROM files",
		"DELETE FROM inodes",
		"DELETE FROM contents",
		"DELETE FROM semantic",
	}
	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			if isMissingTableError(err) {
				continue
			}
			t.Fatalf("reset test db: %v", err)
		}
	}
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
		"DELETE FROM fs_registry",
		"DELETE FROM tenants",
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

// ResetDBWithoutFiles is like ResetDB but for tests that intentionally drop
// the legacy files table to exercise the no-legacy-table code path.
func ResetDBWithoutFiles(t *testing.T, db *sql.DB) {
	t.Helper()
	queries := []string{
		"DELETE FROM fs_layer_checkpoints",
		"DELETE FROM fs_layer_events",
		"DELETE FROM fs_layer_tags",
		"DELETE FROM fs_layer_entries",
		"DELETE FROM fs_layers",
		"DELETE FROM quota_outbox",
		"DELETE FROM quota_admission_locks",
		"DELETE FROM file_gc_tasks",
		"DELETE FROM fs_events",
		"DELETE FROM semantic_tasks",
		"DELETE FROM file_nodes",
		"DELETE FROM file_tags",
		"DELETE FROM uploads",
		"DELETE FROM inodes",
		"DELETE FROM contents",
		"DELETE FROM semantic",
	}
	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			if isMissingTableError(err) {
				continue
			}
			t.Fatalf("reset test db: %v", err)
		}
	}
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
