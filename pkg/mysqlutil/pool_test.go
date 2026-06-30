package mysqlutil

import "testing"

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
}
