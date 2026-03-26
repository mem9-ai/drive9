package testmysql

import (
	"context"
	"fmt"
	"os"
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

	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	c, err := tcmysql.Run(ctx,
		"mysql:8.4",
		tcmysql.WithDatabase("dat9_test"),
		tcmysql.WithUsername("dat9"),
		tcmysql.WithPassword("dat9pass"),
	)
	if err != nil {
		return nil, fmt.Errorf("start mysql container: %w", err)
	}

	dsn, err := c.ConnectionString(ctx, "parseTime=true", "multiStatements=true")
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
