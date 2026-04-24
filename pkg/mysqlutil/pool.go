// Package mysqlutil provides shared helpers for MySQL client configuration.
package mysqlutil

import (
	"database/sql"
	"time"
)

const (
	defaultConnMaxLifetime = 5 * time.Minute
	defaultConnMaxIdleTime = 45 * time.Second
	defaultMaxOpenConns    = 50
	defaultMaxIdleConns    = 10
)

// ApplyPoolDefaults rotates and prunes idle connections before common LB/NAT
// idle timeout windows, and caps pool size to prevent connection storms.
func ApplyPoolDefaults(db *sql.DB) {
	db.SetConnMaxLifetime(defaultConnMaxLifetime)
	db.SetConnMaxIdleTime(defaultConnMaxIdleTime)
	db.SetMaxOpenConns(defaultMaxOpenConns)
	db.SetMaxIdleConns(defaultMaxIdleConns)
}
