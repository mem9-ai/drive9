// Package mysqlutil provides shared helpers for MySQL client configuration.
package mysqlutil

import (
	"database/sql"
	"os"
	"strconv"
	"time"
)

const (
	defaultConnMaxLifetime = 5 * time.Minute
	defaultConnMaxIdleTime = 45 * time.Second
)

// ApplyPoolDefaults rotates and prunes idle connections before common LB/NAT
// idle timeout windows. It also applies configurable open/idle connection
// limits via DRIVE9_DB_MAX_OPEN_CONNS and DRIVE9_DB_MAX_IDLE_CONNS env vars
// (default 0 = unlimited, backward compatible). In a multi-pod deployment,
// set these to ensure N_pods × maxOpenConns ≤ TiDB max_connections.
func ApplyPoolDefaults(db *sql.DB) {
	db.SetConnMaxLifetime(defaultConnMaxLifetime)
	db.SetConnMaxIdleTime(defaultConnMaxIdleTime)
	if v := envInt("DRIVE9_DB_MAX_OPEN_CONNS", 0); v > 0 {
		db.SetMaxOpenConns(v)
	}
	if v := envInt("DRIVE9_DB_MAX_IDLE_CONNS", 0); v > 0 {
		db.SetMaxIdleConns(v)
	}
}

func envInt(key string, def int) int {
	s := os.Getenv(key)
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return v
}
