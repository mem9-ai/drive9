// Package mysqlutil provides shared helpers for MySQL client configuration.
package mysqlutil

import (
	"database/sql"
	"os"
	"strconv"
	"time"
)

const (
	defaultMetaConnMaxLifetime       = 10 * time.Minute
	defaultMetaConnMaxIdleTime       = 1 * time.Minute
	defaultMetaMaxOpenConns          = 100
	defaultMetaMaxIdleConns          = 20
	defaultUserConnMaxLifetime       = 5 * time.Minute
	defaultUserConnMaxIdleTime       = 1 * time.Minute
	defaultUserMaxOpenConns          = 6
	defaultUserMaxIdleConns          = 2
	defaultUserSchemaConnMaxLifetime = 3 * time.Minute
	defaultUserSchemaConnMaxIdleTime = 20 * time.Second
	defaultUserSchemaMaxOpenConns    = 8
	defaultUserSchemaMaxIdleConns    = 2
	// Shared-schema handles serve many tenants through one *sql.DB, so they
	// get meta-like lifetimes and a much larger connection budget than
	// per-tenant user pools.
	defaultSharedConnMaxLifetime = 30 * time.Minute
	defaultSharedConnMaxIdleTime = 5 * time.Minute
	defaultSharedMaxOpenConns    = 300
	defaultSharedMaxIdleConns    = 50
)

// ApplyPoolDefaults rotates and prunes idle connections before common LB/NAT
// idle timeout windows. It also applies configurable open/idle connection
// limits via role-specific env vars.
//
// Supported role-specific env vars:
//   - DRIVE9_META_DB_MAX_OPEN_CONNS / DRIVE9_META_DB_MAX_IDLE_CONNS
//   - DRIVE9_USER_DB_MAX_OPEN_CONNS / DRIVE9_USER_DB_MAX_IDLE_CONNS
//   - DRIVE9_USER_SCHEMA_DB_MAX_OPEN_CONNS / DRIVE9_USER_SCHEMA_DB_MAX_IDLE_CONNS
//   - DRIVE9_SHARED_DB_MAX_OPEN_CONNS / DRIVE9_SHARED_DB_MAX_IDLE_CONNS
//   - DRIVE9_SHARED_DB_CONN_MAX_LIFETIME / DRIVE9_SHARED_DB_CONN_MAX_IDLE_TIME
//
// The limits apply to each *sql.DB pool. In a multi-pod deployment, size them
// against TiDB max_connections and the expected number of active shared pools.
func ApplyPoolDefaults(db *sql.DB, role string) {
	lifetime, idleTime := poolLifetime(role)
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

func poolLifetime(role string) (time.Duration, time.Duration) {
	lifetime, idleTime := defaultPoolLifetime(role)
	if role != RoleShared {
		return lifetime, idleTime
	}
	return poolEnvDuration(role, "CONN_MAX_LIFETIME", lifetime),
		poolEnvDuration(role, "CONN_MAX_IDLE_TIME", idleTime)
}

func defaultPoolLifetime(role string) (time.Duration, time.Duration) {
	switch role {
	case RoleUser:
		return defaultUserConnMaxLifetime, defaultUserConnMaxIdleTime
	case RoleUserSchema:
		return defaultUserSchemaConnMaxLifetime, defaultUserSchemaConnMaxIdleTime
	case RoleShared:
		return defaultSharedConnMaxLifetime, defaultSharedConnMaxIdleTime
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
	case RoleShared:
		return defaultSharedMaxOpenConns, defaultSharedMaxIdleConns
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

func poolEnvDuration(role, suffix string, def time.Duration) time.Duration {
	key := rolePoolEnvKey(role, suffix)
	if key == "" {
		return def
	}
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return def
	}
	return value
}

func rolePoolEnvKey(role, suffix string) string {
	switch role {
	case RoleMeta:
		return "DRIVE9_META_DB_" + suffix
	case RoleUser:
		return "DRIVE9_USER_DB_" + suffix
	case RoleUserSchema:
		return "DRIVE9_USER_SCHEMA_DB_" + suffix
	case RoleShared:
		return "DRIVE9_SHARED_DB_" + suffix
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
