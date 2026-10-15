package cli

import (
	"os"
	"strings"

	"github.com/mem9-ai/drive9/pkg/telemetry"
)

// telemetryDisabledEnv returns environ with telemetry switched off, for
// processes drive9 spawns itself (background mounts, supervisors, detached
// hydration). The invocation that started them is reported by the foreground
// process, so counting the child as well would double-count one user command —
// and a supervised worker lives as long as the mount, which would also pollute
// the duration distribution with mount lifetimes.
func telemetryDisabledEnv(environ []string) []string {
	out := make([]string, 0, len(environ)+1)
	for _, kv := range environ {
		if strings.HasPrefix(kv, telemetry.EnvironmentVariable+"=") {
			continue
		}
		out = append(out, kv)
	}
	return append(out, telemetry.EnvironmentVariable+"=off")
}

// TelemetryPreference reports the persisted telemetry opt-in from
// ~/.drive9/config. The second result is false when the user has never
// recorded a preference, and also when the config cannot be read or parsed —
// an unreadable config must never turn telemetry on.
func TelemetryPreference() (enabled bool, recorded bool) {
	cfg := loadConfig()
	pref := cfg.Telemetry
	if pref == nil || pref.Enabled == nil {
		return false, false
	}
	return *pref.Enabled, true
}

// TelemetryContextMetadata returns allowlisted placement fields from the
// active context without exposing credentials, context names, or tokens.
func TelemetryContextMetadata() (cloudProvider, region, profileSource string) {
	cfg := loadConfig()
	ctx := cfg.currentContextEntry()
	if ctx != nil {
		cloudProvider = strings.TrimSpace(ctx.CloudProvider)
		region = strings.TrimSpace(ctx.Region)
	}
	switch {
	case strings.TrimSpace(os.Getenv(EnvAPIKey)) != "" || strings.TrimSpace(os.Getenv(EnvVaultToken)) != "":
		profileSource = "env"
	case ctx != nil:
		profileSource = "default"
	default:
		profileSource = "unknown"
	}
	return cloudProvider, region, profileSource
}
