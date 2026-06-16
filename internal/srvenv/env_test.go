package srvenv

import (
	"testing"
	"time"
)

func TestPositiveInt(t *testing.T) {
	const key = "DRIVE9_TEST_POSITIVE_INT"
	t.Setenv(key, "")
	got, err := PositiveInt(key, 15)
	if err != nil {
		t.Fatal(err)
	}
	if got != 15 {
		t.Fatalf("unset value = %d, want fallback 15", got)
	}

	t.Setenv(key, "3")
	got, err = PositiveInt(key, 15)
	if err != nil {
		t.Fatal(err)
	}
	if got != 3 {
		t.Fatalf("parsed value = %d, want 3", got)
	}

	for _, value := range []string{"abc", "0", "-1"} {
		t.Setenv(key, value)
		if _, err := PositiveInt(key, 15); err == nil {
			t.Fatalf("PositiveInt(%q) error = nil, want error", value)
		}
	}
}

func TestSSECatchupOptionsDefaults(t *testing.T) {
	clearSSEEnv(t)

	serverOpts, err := SSECatchupOptions(true)
	if err != nil {
		t.Fatal(err)
	}
	if serverOpts.Disabled {
		t.Fatal("server catchup default disabled, want enabled")
	}
	if serverOpts.PollInterval != time.Second {
		t.Fatalf("PollInterval=%s, want 1s", serverOpts.PollInterval)
	}
	if serverOpts.IdleMaxInterval != 10*time.Second {
		t.Fatalf("IdleMaxInterval=%s, want 10s", serverOpts.IdleMaxInterval)
	}
	if serverOpts.BatchSize != 1000 {
		t.Fatalf("BatchSize=%d, want 1000", serverOpts.BatchSize)
	}
	if serverOpts.MaxConcurrentTenants != 16 {
		t.Fatalf("MaxConcurrentTenants=%d, want 16", serverOpts.MaxConcurrentTenants)
	}

	localOpts, err := SSECatchupOptions(false)
	if err != nil {
		t.Fatal(err)
	}
	if !localOpts.Disabled {
		t.Fatal("local catchup default enabled, want disabled")
	}
}

func TestSSECatchupOptionsStrictPositiveInts(t *testing.T) {
	tests := []string{
		EnvSSECatchupPollIntervalMS,
		EnvSSECatchupIdleMaxIntervalMS,
		EnvSSECatchupBatchSize,
		EnvSSECatchupMaxConcurrentTenantDBs,
	}
	for _, key := range tests {
		t.Run(key, func(t *testing.T) {
			clearSSEEnv(t)
			t.Setenv(key, "abc")
			if _, err := SSECatchupOptions(true); err == nil {
				t.Fatalf("SSECatchupOptions accepted non-numeric %s", key)
			}
			t.Setenv(key, "0")
			if _, err := SSECatchupOptions(true); err == nil {
				t.Fatalf("SSECatchupOptions accepted zero %s", key)
			}
		})
	}
}

func TestSSECatchupOptionsDisabledSkipsTunableValidation(t *testing.T) {
	clearSSEEnv(t)
	t.Setenv(EnvSSEDurableCatchup, "false")
	t.Setenv(EnvSSECatchupPollIntervalMS, "abc")

	opts, err := SSECatchupOptions(true)
	if err != nil {
		t.Fatal(err)
	}
	if !opts.Disabled {
		t.Fatal("disabled catchup env did not disable catchup")
	}
}

func clearSSEEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		EnvSSEHeartbeatIntervalSeconds,
		EnvSSEDurableCatchup,
		EnvSSECatchupPollIntervalMS,
		EnvSSECatchupIdleMaxIntervalMS,
		EnvSSECatchupBatchSize,
		EnvSSECatchupMaxConcurrentTenantDBs,
	} {
		t.Setenv(key, "")
	}
}
