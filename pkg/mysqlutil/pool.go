// Package mysqlutil provides shared helpers for MySQL client configuration.
package mysqlutil

import (
	"database/sql"
	"time"
)

const (
	defaultConnMaxLifetime = 5 * time.Minute
	defaultConnMaxIdleTime = 45 * time.Second
)

// ApplyPoolDefaults rotates and prunes idle connections before common LB/NAT
// idle timeout windows.
func ApplyPoolDefaults(db *sql.DB) {
	db.SetConnMaxLifetime(defaultConnMaxLifetime)
	db.SetConnMaxIdleTime(defaultConnMaxIdleTime)
}