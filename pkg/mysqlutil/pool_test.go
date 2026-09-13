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

}

func TestUserPoolKeepsEstablishedDefault(t *testing.T) {
	maxOpen, maxIdle := defaultPoolLimits(RoleUser)
	// The 6/2 default is what every existing tenant pool runs with; the extent
	// data plane gets its own, explicitly scoped budget instead of changing
	// the global one (see ExtentPoolLimits).
	if maxOpen != 6 || maxIdle != 2 {
		t.Fatalf("user pool = %d/%d, want the established 6/2 default", maxOpen, maxIdle)
	}
}

func TestExtentPoolLimitsAreScopedAndConfigurable(t *testing.T) {
	t.Setenv(ExtentRoleUserEnvPrefix+"MAX_OPEN_CONNS", "")
	t.Setenv(ExtentRoleUserEnvPrefix+"MAX_IDLE_CONNS", "")
	maxOpen, maxIdle := ExtentPoolLimits()
	if maxOpen != DefaultExtentMaxOpenConns || maxIdle != DefaultExtentMaxIdleConns {
		t.Fatalf("extent limits = %d/%d, want %d/%d", maxOpen, maxIdle, DefaultExtentMaxOpenConns, DefaultExtentMaxIdleConns)
	}
	t.Setenv(ExtentRoleUserEnvPrefix+"MAX_OPEN_CONNS", "24")
	t.Setenv(ExtentRoleUserEnvPrefix+"MAX_IDLE_CONNS", "8")
	maxOpen, maxIdle = ExtentPoolLimits()
	if maxOpen != 24 || maxIdle != 8 {
		t.Fatalf("configured extent limits = %d/%d, want 24/8", maxOpen, maxIdle)
	}
	t.Setenv(ExtentRoleUserEnvPrefix+"MAX_OPEN_CONNS", "-1")
	if maxOpen, _ := ExtentPoolLimits(); maxOpen != -1 {
		t.Fatalf("disabled extent budget = %d, want -1 (keep the global default)", maxOpen)
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
