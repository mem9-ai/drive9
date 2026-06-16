package srvenv

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/server"
)

const (
	EnvSSEHeartbeatIntervalSeconds      = "DRIVE9_SSE_HEARTBEAT_INTERVAL_SECONDS"
	EnvSSEDurableCatchup                = "DRIVE9_SSE_DURABLE_CATCHUP"
	EnvSSECatchupPollIntervalMS         = "DRIVE9_SSE_CATCHUP_POLL_INTERVAL_MS"
	EnvSSECatchupIdleMaxIntervalMS      = "DRIVE9_SSE_CATCHUP_IDLE_MAX_INTERVAL_MS"
	EnvSSECatchupBatchSize              = "DRIVE9_SSE_CATCHUP_BATCH_SIZE"
	EnvSSECatchupMaxConcurrentTenantDBs = "DRIVE9_SSE_CATCHUP_MAX_CONCURRENT_TENANT_DBS"
)

func PositiveInt(key string, fallback int) (int, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("invalid %s: must be a positive integer", key)
	}
	return v, nil
}

func SSECatchupOptions(defaultEnabled bool) (server.SSECatchupOptions, error) {
	enabled := envBool(EnvSSEDurableCatchup, defaultEnabled)
	opts := server.SSECatchupOptions{Disabled: !enabled}
	if !enabled {
		return opts, nil
	}

	pollIntervalMS, err := PositiveInt(EnvSSECatchupPollIntervalMS, 1000)
	if err != nil {
		return opts, err
	}
	idleMaxIntervalMS, err := PositiveInt(EnvSSECatchupIdleMaxIntervalMS, 10000)
	if err != nil {
		return opts, err
	}
	batchSize, err := PositiveInt(EnvSSECatchupBatchSize, 1000)
	if err != nil {
		return opts, err
	}
	maxConcurrentTenantDBs, err := PositiveInt(EnvSSECatchupMaxConcurrentTenantDBs, 16)
	if err != nil {
		return opts, err
	}

	opts.PollInterval = time.Duration(pollIntervalMS) * time.Millisecond
	opts.IdleMaxInterval = time.Duration(idleMaxIntervalMS) * time.Millisecond
	opts.BatchSize = batchSize
	opts.MaxConcurrentTenants = maxConcurrentTenantDBs
	return opts, nil
}

func envBool(key string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}
