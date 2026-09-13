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
)

// ExtentRoleUserEnvPrefix is the env prefix for the extent data plane's own
// tenant-pool budget. A tenant that actually uses content_layout=extent issues
// one TiDB transaction per JuiceFS metadata op, so the established 6-connection
// default queues sqlite's exclusive COMMITs past its own wait window; the
// budget is raised only for that tenant and only once its extent meta is
// initialized (datastore.applyExtentPoolBudget), never for the classic path.
const (
	ExtentRoleUserEnvPrefix   = "DRIVE9_EXTENT_DB_"
	DefaultExtentMaxOpenConns = 64
	DefaultExtentMaxIdleConns = 16
)

// ExtentPoolLimits returns the connection budget an extent-using tenant may
// raise its RoleUser pool to. Both values are configurable through
// DRIVE9_EXTENT_DB_MAX_OPEN_CONNS / DRIVE9_EXTENT_DB_MAX_IDLE_CONNS; a
// non-positive open limit keeps the global default.
func ExtentPoolLimits() (maxOpen, maxIdle int) {
	// An operator who set the tenant pool explicitly keeps that decision: the
	// extent budget only fills in a default for tenants that never had one.
	if os.Getenv(rolePoolEnvKey(RoleUser, "MAX_OPEN_CONNS")) != "" {
		return 0, 0
	}
	maxOpen = envIntWithDefault(ExtentRoleUserEnvPrefix+"MAX_OPEN_CONNS", DefaultExtentMaxOpenConns)
	maxIdle = envIntWithDefault(ExtentRoleUserEnvPrefix+"MAX_IDLE_CONNS", DefaultExtentMaxIdleConns)
	if maxIdle < 0 {
		maxIdle = 0
	}
	if maxOpen > 0 && maxIdle > maxOpen {
		maxIdle = maxOpen
	}
	return maxOpen, maxIdle
}

func envIntWithDefault(key string, def int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return v
}

// ApplyPoolDefaults rotates and prunes idle connections before common LB/NAT
// idle timeout windows. It also applies configurable open/idle connection
// limits via role-specific env vars.
//
// Supported role-specific env vars:
//   - DRIVE9_META_DB_MAX_OPEN_CONNS / DRIVE9_META_DB_MAX_IDLE_CONNS
//   - DRIVE9_META_DB_CONN_MAX_LIFETIME / DRIVE9_META_DB_CONN_MAX_IDLE_TIME
//   - DRIVE9_USER_DB_MAX_OPEN_CONNS / DRIVE9_USER_DB_MAX_IDLE_CONNS
//   - DRIVE9_USER_SCHEMA_DB_MAX_OPEN_CONNS / DRIVE9_USER_SCHEMA_DB_MAX_IDLE_CONNS
//
// The limits apply to each *sql.DB pool.
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
	if role != RoleMeta {
		return lifetime, idleTime
	}
	return poolEnvDuration(role, "CONN_MAX_LIFETIME", lifetime, false),
		poolEnvDuration(role, "CONN_MAX_IDLE_TIME", idleTime, role == RoleMeta)
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

func poolEnvDuration(role, suffix string, def time.Duration, allowZero bool) time.Duration {
	key := rolePoolEnvKey(role, suffix)
	if key == "" {
		return def
	}
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value < 0 || value == 0 && !allowZero {
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
