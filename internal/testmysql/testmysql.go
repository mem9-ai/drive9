package testmysql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
)

const envDSN = "DAT9_MYSQL_DSN"

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

func ResetDB(t *testing.T, db *sql.DB) {
	t.Helper()
	queries := []string{
		"DELETE FROM file_nodes",
		"DELETE FROM file_tags",
		"DELETE FROM uploads",
		"DELETE FROM files",
	}
	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("reset test db: %v", err)
		}
	}
}
