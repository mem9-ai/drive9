package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
)

const (
	// defaultMetricsListenAddr is the dedicated metrics listener for
	// multi-tenant deployments. It is deliberately separate from the API port
	// (DRIVE9_LISTEN_ADDR): the container port, Service and Ingress expose only
	// the API address, so /metrics stays reachable from the pod network
	// (vmagent scrapes) but not through the public API endpoint.
	defaultMetricsListenAddr = ":9090"
	// defaultLocalMetricsListenAddr keeps the local provider inside its
	// loopback boundary: local mode has no tenant SQL isolation, and the
	// metrics carry tenant and organization labels.
	defaultLocalMetricsListenAddr = "127.0.0.1:9090"
	metricsReadHeaderTimeout      = 10 * time.Second
	metricsShutdownTimeout        = 5 * time.Second
)

// resolveMetricsListenAddr returns the dedicated metrics listen address and
// whether the listener should run. DRIVE9_METRICS_LISTEN overrides the default;
// "off", "disabled" or "none" turns the listener off. The local provider
// defaults to loopback because its API listener is loopback-only too.
func resolveMetricsListenAddr(envAddr string, localProvider bool) (string, bool) {
	addr := strings.TrimSpace(envAddr)
	if addr == "" {
		if localProvider {
			return defaultLocalMetricsListenAddr, true
		}
		return defaultMetricsListenAddr, true
	}
	switch strings.ToLower(addr) {
	case "off", "disabled", "none":
		return "", false
	}
	return addr, true
}

// rejectLocalProviderNonLoopbackMetrics keeps provider=local inside its
// loopback boundary. Local mode does not isolate tenants in SQL, so serving
// process metrics — which carry tenant and organization labels — on a routable
// address would leak one local tenant's identifiers to the network. Any
// non-loopback bind is refused, not just the wildcard ones: a specific LAN
// address is just as reachable as 0.0.0.0 from the network's point of view.
func rejectLocalProviderNonLoopbackMetrics(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid metrics listen address %q: %w", addr, err)
	}
	if isLoopbackMetricsHost(host) {
		return nil
	}
	return fmt.Errorf("DRIVE9_METRICS_LISTEN=%q must bind loopback while DRIVE9_TENANT_PROVIDER=local; use 127.0.0.1:<port> (or [::1]:<port>), or disable the listener", addr)
}

// isLoopbackMetricsHost reports whether a listen host only accepts loopback
// connections.
func isLoopbackMetricsHost(host string) bool {
	h := strings.Trim(strings.TrimSpace(host), "[]")
	if h == "" {
		return false
	}
	if strings.EqualFold(h, "localhost") {
		return true
	}
	ip := net.ParseIP(h)
	return ip != nil && ip.IsLoopback()
}

// serveMetrics serves the dedicated metrics handler on ln until ctx is
// canceled, then drains it. The caller binds ln first so an address conflict
// fails startup instead of silently losing /metrics.
func serveMetrics(ctx context.Context, ln net.Listener) error {
	// Serve may return on its own (e.g. the listener is closed); tying the
	// shutdown watcher to a child context keeps it from outliving the server.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	srv := &http.Server{
		Handler:           metricsHandler(),
		ReadHeaderTimeout: metricsReadHeaderTimeout,
	}
	logger.Info(ctx, "metrics_server_start", zap.String("addr", ln.Addr().String()))
	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), metricsShutdownTimeout)
		defer shutdownCancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("metrics server: %w", err)
	}
	return nil
}

// metricsHandler exposes the Prometheus text export on its own mux so the
// metrics listener serves exactly one route and no API surface.
func metricsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		metrics.WritePrometheus(w)
	})
	return mux
}
