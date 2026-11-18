package main

import (
	"strings"
	"testing"
)

func TestResolveMetricsListenAddr(t *testing.T) {
	for _, tc := range []struct {
		env     string
		local   bool
		want    string
		enabled bool
	}{
		{env: "", local: false, want: defaultMetricsListenAddr, enabled: true},
		{env: "", local: true, want: defaultLocalMetricsListenAddr, enabled: true},
		{env: "127.0.0.1:9191", want: "127.0.0.1:9191", enabled: true},
		{env: "off", want: "", enabled: false},
		{env: " disabled ", want: "", enabled: false},
		{env: "NONE", want: "", enabled: false},
	} {
		gotAddr, gotEnabled := resolveMetricsListenAddr(tc.env, tc.local)
		if gotAddr != tc.want || gotEnabled != tc.enabled {
			t.Errorf("resolveMetricsListenAddr(%q, %v) = (%q, %v), want (%q, %v)",
				tc.env, tc.local, gotAddr, gotEnabled, tc.want, tc.enabled)
		}
	}
}

// Local mode has no tenant SQL isolation, so every non-loopback bind must be
// refused — including a specific LAN address, not only the wildcard ones.
func TestRejectLocalProviderNonLoopbackMetrics(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:9090", "localhost:9090", "[::1]:9090"} {
		if err := rejectLocalProviderNonLoopbackMetrics(addr); err != nil {
			t.Errorf("loopback address %q rejected: %v", addr, err)
		}
	}
	for _, addr := range []string{":9090", "0.0.0.0:9090", "[::]:9090", "10.0.0.5:9090", "192.168.1.7:9090", "metrics.internal:9090"} {
		if err := rejectLocalProviderNonLoopbackMetrics(addr); err == nil {
			t.Errorf("non-loopback address %q accepted", addr)
		}
	}
	if err := rejectLocalProviderNonLoopbackMetrics("not-an-address"); err == nil {
		t.Error("invalid address accepted")
	} else if !strings.Contains(err.Error(), "invalid metrics listen address") {
		t.Errorf("invalid address error = %v", err)
	}
}
