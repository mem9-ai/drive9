// Package mysqlutil provides shared helpers for MySQL client configuration.
package mysqlutil

import (
	"database/sql"
	"os"
	"strconv"
	"time"
)

const (
	defaultMetaConnMaxLifetime       = 5 * time.Minute
	defaultMetaConnMaxIdleTime       = 45 * time.Second
	defaultMetaMaxOpenConns          = 100
	defaultMetaMaxIdleConns          = 20
	defaultUserConnMaxLifetime       = 45 * time.Second
	defaultUserConnMaxIdleTime       = 30 * time.Second
	defaultUserMaxOpenConns          = 6
	defaultUserMaxIdleConns          = 2
	defaultUserSchemaConnMaxLifetime = 1 * time.Minute
	defaultUserSchemaConnMaxIdleTime = 20 * time.Second
	defaultUserSchemaMaxOpenConns    = 8
	defaultUserSchemaMaxIdleConns    = 2
)

// ApplyPoolDefaults rotates and prunes idle connections before common LB/NAT
// idle timeout windows. It also applies configurable open/idle connection
// limits via role-specific env vars.
//
// Supported role-specific env vars:
//   - DRIVE9_META_DB_MAX_OPEN_CONNS / DRIVE9_META_DB_MAX_IDLE_CONNS
//   - DRIVE9_USER_DB_MAX_OPEN_CONNS / DRIVE9_USER_DB_MAX_IDLE_CONNS
//   - DRIVE9_USER_SCHEMA_DB_MAX_OPEN_CONNS / DRIVE9_USER_SCHEMA_DB_MAX_IDLE_CONNS
//
// The limits apply to each *sql.DB pool, not to all pools in the process. In a
// multi-pod deployment, set these so pod_count * pools_per_pod * maxOpenConns
// stays within TiDB max_connections.
func ApplyPoolDefaults(db *sql.DB, role string) {
	lifetime, idleTime := defaultPoolLifetime(role)
	db.SetConnMaxLifetime(lifetime)
	db.SetConnMaxIdleTime(idleTime)
	maxOpen, maxIdle := defaultPoolLimits(role)
	if v := poolEnvInt(role, "MAX_OPEN_CONNS", maxOpen); v > 0 {
		db.SetMaxOpenConns(v)
	}
	if v := poolEnvInt(role, "MAX_IDLE_CONNS", maxIdle); v >= 0 {
		db.SetMaxIdleConns(v)
	}
}

func defaultPoolLifetime(role string) (time.Duration, time.Duration) {
	switch role {
	case RoleUser:
		return defaultUserConnMaxLifetime, defaultUserConnMaxIdleTime
	case RoleUserSchema:
		return defaultUserSchemaConnMaxLifetime, defaultUserSchemaConnMaxIdleTime
	default:
		return defaultMetaConnMaxLifetime, defaultMetaConnMaxIdleTime
	}
}

func defaultPoolLimits(role string) (int, int) {
	switch role {
	case RoleMeta:
		return defaultMetaMaxOpenConns, defaultMetaMaxIdleConns
	case RoleUser:
		return defaultUserMaxOpenConns, defaultUserMaxIdleConns
	case RoleUserSchema:
		return defaultUserSchemaMaxOpenConns, defaultUserSchemaMaxIdleConns
	default:
		return 0, 0
	}
}

func poolEnvInt(role, suffix string, def int) int {
	if key := rolePoolEnvKey(role, suffix); key != "" {
		if v, ok := lookupEnvInt(key); ok {
			return v
		}
	}
	return def
}

func rolePoolEnvKey(role, suffix string) string {
	switch role {
	case RoleMeta:
		return "DRIVE9_META_DB_" + suffix
	case RoleUser:
		return "DRIVE9_USER_DB_" + suffix
	case RoleUserSchema:
		return "DRIVE9_USER_SCHEMA_DB_" + suffix
	default:
		return ""
	}
}

func lookupEnvInt(key string) (int, bool) {
	s := os.Getenv(key)
	if s == "" {
		return 0, false
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, false
	}
	return v, true
}
