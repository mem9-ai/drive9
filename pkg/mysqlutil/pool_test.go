package mysqlutil

import (
	"testing"
	"time"
)

func TestPoolEnvIntUsesRoleSpecificValue(t *testing.T) {
	t.Setenv("DRIVE9_META_DB_MAX_OPEN_CONNS", "20")
	t.Setenv("DRIVE9_USER_DB_MAX_OPEN_CONNS", "5")
	t.Setenv("DRIVE9_USER_SCHEMA_DB_MAX_OPEN_CONNS", "2")

	if got := poolEnvInt(RoleMeta, "MAX_OPEN_CONNS", 0); got != 20 {
		t.Fatalf("meta max open = %d, want 20", got)
	}
	if got := poolEnvInt(RoleUser, "MAX_OPEN_CONNS", 0); got != 5 {
		t.Fatalf("user max open = %d, want 5", got)
	}
	if got := poolEnvInt(RoleUserSchema, "MAX_OPEN_CONNS", 0); got != 2 {
		t.Fatalf("user schema max open = %d, want 2", got)
	}
}

func TestPoolEnvIntUsesDefaultWhenRoleSpecificValueUnset(t *testing.T) {
	maxOpen, maxIdle := defaultPoolLimits(RoleMeta)
	if got := poolEnvInt(RoleMeta, "MAX_OPEN_CONNS", maxOpen); got != defaultMetaMaxOpenConns {
		t.Fatalf("meta max open = %d, want %d", got, defaultMetaMaxOpenConns)
	}
	if got := poolEnvInt(RoleMeta, "MAX_IDLE_CONNS", maxIdle); got != defaultMetaMaxIdleConns {
		t.Fatalf("meta max idle = %d, want %d", got, defaultMetaMaxIdleConns)
	}
	maxOpen, maxIdle = defaultPoolLimits(RoleUser)
	if got := poolEnvInt(RoleUser, "MAX_OPEN_CONNS", maxOpen); got != defaultUserMaxOpenConns {
		t.Fatalf("user max open = %d, want %d", got, defaultUserMaxOpenConns)
	}
	if got := poolEnvInt(RoleUser, "MAX_IDLE_CONNS", maxIdle); got != defaultUserMaxIdleConns {
		t.Fatalf("user max idle = %d, want %d", got, defaultUserMaxIdleConns)
	}
}

func TestPoolEnvIntIgnoresInvalidRoleSpecificValue(t *testing.T) {
	t.Setenv("DRIVE9_USER_DB_MAX_IDLE_CONNS", "bad")

	if got := poolEnvInt(RoleUser, "MAX_IDLE_CONNS", defaultUserMaxIdleConns); got != defaultUserMaxIdleConns {
		t.Fatalf("user max idle = %d, want %d", got, defaultUserMaxIdleConns)
	}
}

func TestDefaultPoolLifetime(t *testing.T) {
	lifetime, idleTime := defaultPoolLifetime(RoleUser)
	if lifetime != defaultUserConnMaxLifetime {
		t.Fatalf("user lifetime = %s, want %s", lifetime, defaultUserConnMaxLifetime)
	}
	if idleTime != defaultUserConnMaxIdleTime {
		t.Fatalf("user idle time = %s, want %s", idleTime, defaultUserConnMaxIdleTime)
	}

	lifetime, idleTime = defaultPoolLifetime(RoleUserSchema)
	if lifetime != defaultUserSchemaConnMaxLifetime {
		t.Fatalf("user schema lifetime = %s, want %s", lifetime, defaultUserSchemaConnMaxLifetime)
	}
	if idleTime != defaultUserSchemaConnMaxIdleTime {
		t.Fatalf("user schema idle time = %s, want %s", idleTime, defaultUserSchemaConnMaxIdleTime)
	}

	lifetime, idleTime = defaultPoolLifetime(RoleShared)
	if lifetime != 30*time.Minute {
		t.Fatalf("shared lifetime = %s, want 30m", lifetime)
	}
	if idleTime != 5*time.Minute {
		t.Fatalf("shared idle time = %s, want 5m", idleTime)
	}
}

func TestDefaultPoolLimits(t *testing.T) {
	maxOpen, maxIdle := defaultPoolLimits(RoleMeta)
	if maxOpen != defaultMetaMaxOpenConns {
		t.Fatalf("meta max open = %d, want %d", maxOpen, defaultMetaMaxOpenConns)
	}
	if maxIdle != defaultMetaMaxIdleConns {
		t.Fatalf("meta max idle = %d, want %d", maxIdle, defaultMetaMaxIdleConns)
	}

	maxOpen, maxIdle = defaultPoolLimits(RoleUser)
	if maxOpen != defaultUserMaxOpenConns {
		t.Fatalf("user max open = %d, want %d", maxOpen, defaultUserMaxOpenConns)
	}
	if maxIdle != defaultUserMaxIdleConns {
		t.Fatalf("user max idle = %d, want %d", maxIdle, defaultUserMaxIdleConns)
	}

	maxOpen, maxIdle = defaultPoolLimits(RoleUserSchema)
	if maxOpen != defaultUserSchemaMaxOpenConns {
		t.Fatalf("user schema max open = %d, want %d", maxOpen, defaultUserSchemaMaxOpenConns)
	}
	if maxIdle != defaultUserSchemaMaxIdleConns {
		t.Fatalf("user schema max idle = %d, want %d", maxIdle, defaultUserSchemaMaxIdleConns)
	}

	maxOpen, maxIdle = defaultPoolLimits(RoleShared)
	if maxOpen != 300 || maxIdle != 50 {
		t.Fatalf("shared limits = %d/%d, want 300/50", maxOpen, maxIdle)
	}

	// The schema handle only serves one ensure at a time, so it must not claim a
	// data-plane sized budget against the same physical DB.
	maxOpen, maxIdle = defaultPoolLimits(RoleSharedSchema)
	if maxOpen != defaultSharedSchemaMaxOpenConns {
		t.Fatalf("shared schema max open = %d, want %d", maxOpen, defaultSharedSchemaMaxOpenConns)
	}
	if maxIdle != defaultSharedSchemaMaxIdleConns {
		t.Fatalf("shared schema max idle = %d, want %d", maxIdle, defaultSharedSchemaMaxIdleConns)
	}
	if maxOpen >= defaultSharedMaxOpenConns {
		t.Fatalf("shared schema max open = %d, want well under the shared budget %d", maxOpen, defaultSharedMaxOpenConns)
	}
}

func TestSharedSchemaPoolEnvOverridesLimits(t *testing.T) {
	t.Setenv("DRIVE9_SHARED_SCHEMA_DB_MAX_OPEN_CONNS", "3")

	if got := poolEnvInt(RoleSharedSchema, "MAX_OPEN_CONNS", defaultSharedSchemaMaxOpenConns); got != 3 {
		t.Fatalf("shared schema max open = %d, want 3", got)
	}
	// The shared data-plane knob must not bleed into the schema handle.
	t.Setenv("DRIVE9_SHARED_DB_MAX_OPEN_CONNS", "77")
	if got := poolEnvInt(RoleSharedSchema, "MAX_OPEN_CONNS", defaultSharedSchemaMaxOpenConns); got != 3 {
		t.Fatalf("shared schema max open after shared override = %d, want 3", got)
	}
}

func TestDefaultPoolLifetimeSharedSchemaIsShortLived(t *testing.T) {
	lifetime, idleTime := defaultPoolLifetime(RoleSharedSchema)
	if lifetime != defaultSharedSchemaConnMaxLifetime || idleTime != defaultSharedSchemaConnMaxIdleTime {
		t.Fatalf("shared schema lifetimes = %v/%v, want %v/%v",
			lifetime, idleTime, defaultSharedSchemaConnMaxLifetime, defaultSharedSchemaConnMaxIdleTime)
	}
	if lifetime >= defaultSharedConnMaxLifetime {
		t.Fatalf("shared schema lifetime = %v, want shorter than the shared handle %v", lifetime, defaultSharedConnMaxLifetime)
	}
}

func TestSharedPoolDurationEnv(t *testing.T) {
	t.Setenv("DRIVE9_SHARED_DB_CONN_MAX_LIFETIME", "45m")
	t.Setenv("DRIVE9_SHARED_DB_CONN_MAX_IDLE_TIME", "12m")
	lifetime, idleTime := poolLifetime(RoleShared)
	if lifetime != 45*time.Minute || idleTime != 12*time.Minute {
		t.Fatalf("shared env durations = %s/%s, want 45m/12m", lifetime, idleTime)
	}
}

func TestSharedPoolDurationEnvRejectsZeroIdleTime(t *testing.T) {
	t.Setenv("DRIVE9_SHARED_DB_CONN_MAX_IDLE_TIME", "0s")

	_, idleTime := poolLifetime(RoleShared)
	if idleTime != defaultSharedConnMaxIdleTime {
		t.Fatalf("shared idle time = %s, want default %s", idleTime, defaultSharedConnMaxIdleTime)
	}
}

func TestMetaPoolDurationEnvAllowsZeroIdleTime(t *testing.T) {
	t.Setenv("DRIVE9_META_DB_CONN_MAX_LIFETIME", "1h")
	t.Setenv("DRIVE9_META_DB_CONN_MAX_IDLE_TIME", "0s")

	lifetime, idleTime := poolLifetime(RoleMeta)
	if lifetime != time.Hour || idleTime != 0 {
		t.Fatalf("meta env durations = %s/%s, want 1h/0s", lifetime, idleTime)
	}
}

func TestMetaPoolDurationEnvRejectsZeroLifetime(t *testing.T) {
	t.Setenv("DRIVE9_META_DB_CONN_MAX_LIFETIME", "0s")
	t.Setenv("DRIVE9_META_DB_CONN_MAX_IDLE_TIME", "0s")

	lifetime, idleTime := poolLifetime(RoleMeta)
	if lifetime != defaultMetaConnMaxLifetime || idleTime != 0 {
		t.Fatalf("meta env durations = %s/%s, want default %s/0s", lifetime, idleTime, defaultMetaConnMaxLifetime)
	}
}
