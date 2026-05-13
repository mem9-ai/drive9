package server

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/metrics"
	"github.com/mem9-ai/dat9/pkg/traceid"
	"go.uber.org/zap"
)

type metricsKeyType int

const metricsKey metricsKeyType = iota

func eventFields(ctx context.Context, event string, kv ...any) []zap.Field {
	fields := make([]zap.Field, 0, len(kv)/2+3)
	fields = append(fields, zap.String("event", event))
	if scope := ScopeFromContext(ctx); scope != nil {
		if scope.TenantID != "" {
			fields = append(fields, zap.String("tenant_id", scope.TenantID))
		}
		if scope.APIKeyID != "" {
			fields = append(fields, zap.String("api_key_id", scope.APIKeyID))
		}
	}
	for i := 0; i+1 < len(kv); i += 2 {
		key := strings.ReplaceAll(fmt.Sprint(kv[i]), " ", "_")
		if key == "" {
			continue
		}
		fields = append(fields, zap.Any(key, kv[i+1]))
	}
	return fields
}

func withMetrics(ctx context.Context, m *serverMetrics) context.Context {
	return context.WithValue(ctx, metricsKey, m)
}

func metricsFromContext(ctx context.Context) *serverMetrics {
	v, _ := ctx.Value(metricsKey).(*serverMetrics)
	return v
}

func metricEvent(ctx context.Context, event string, labels ...string) {
	m := metricsFromContext(ctx)
	if m == nil {
		return
	}
	m.recordEvent(event, labels...)
}

type serverMetrics struct {
	inFlight atomic.Int64
	routeMu  sync.Mutex
	routes   map[string]int64
}

func newServerMetrics() *serverMetrics {
	metrics.SetModuleAvailability("server", true)
	metrics.RecordHTTPInFlight(0)
	return &serverMetrics{routes: map[string]int64{}}
}

func (m *serverMetrics) record(method, route string, status int, d time.Duration) {
	metrics.RecordHTTPRequest(method, route, status, d)
}

func (m *serverMetrics) recordEvent(event string, labels ...string) {
	metrics.RecordEvent(event, labels...)
}

func (m *serverMetrics) writePrometheus(w http.ResponseWriter) {
	metrics.WritePrometheus(w)
}

func (m *serverMetrics) adjustRouteInFlight(route string, delta int64) int64 {
	route = strings.TrimSpace(route)
	if route == "" {
		route = "other"
	}
	m.routeMu.Lock()
	defer m.routeMu.Unlock()
	next := m.routes[route] + delta
	if next <= 0 {
		delete(m.routes, route)
		return 0
	}
	m.routes[route] = next
	return next
}

type observedResponseWriter struct {
	http.ResponseWriter
	status int
	size   int
}

type flusherResponseWriter struct {
	*observedResponseWriter
	flusher http.Flusher
}

func (w *flusherResponseWriter) Flush() {
	w.flusher.Flush()
}

func (w *observedResponseWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *observedResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.size += n
	return n, err
}

func requestRoute(path string) string {
	switch {
	case path == "/metrics":
		return "/metrics"
	case path == "/healthz":
		return "/healthz"
	case path == "/v1/provision":
		return "/v1/provision"
	case path == "/v1/status":
		return "/v1/status"
	case path == "/v1/sql":
		return "/v1/sql"
	case path == "/v1/events":
		return "/v1/events"
	case path == "/v1/ctx" || path == "/v1/ctx/fork":
		return "/v1/ctx/*"
	case strings.HasPrefix(path, "/v1/fs/"):
		return "/v1/fs/*"
	case path == "/v1/uploads":
		return "/v1/uploads"
	case strings.HasPrefix(path, "/v1/uploads/"):
		return "/v1/uploads/*"
	case strings.HasPrefix(path, "/v2/uploads/"):
		return "/v2/uploads/*"
	case path == "/v1/vault/secrets" || strings.HasPrefix(path, "/v1/vault/secrets/"):
		return "/v1/vault/secrets/*"
	case path == "/v1/vault/tokens" || strings.HasPrefix(path, "/v1/vault/tokens/"):
		return "/v1/vault/tokens/*"
	case path == "/v1/vault/grants" || strings.HasPrefix(path, "/v1/vault/grants/"):
		return "/v1/vault/grants/*"
	case path == "/v1/vault/audit":
		return "/v1/vault/audit"
	case path == "/v1/vault/read" || strings.HasPrefix(path, "/v1/vault/read/"):
		return "/v1/vault/read/*"
	case strings.HasPrefix(path, "/s3/"):
		return "/s3/*"
	default:
		return "other"
	}
}

func generateTraceID() string { return traceid.Generate() }

func (s *Server) observe(next http.Handler, w http.ResponseWriter, r *http.Request) {
	traceID := strings.TrimSpace(r.Header.Get("X-Trace-ID"))
	if traceID == "" {
		traceID = generateTraceID()
	}

	r = r.WithContext(traceid.With(r.Context(), traceID))
	r = r.WithContext(logger.WithContext(r.Context(), s.logger.With(zap.String("trace_id", traceID))))
	r = r.WithContext(withMetrics(r.Context(), s.metrics))
	w.Header().Set("X-Trace-ID", traceID)

	start := time.Now()
	route := requestRoute(r.URL.Path)
	ow := &observedResponseWriter{ResponseWriter: w}
	rw := http.ResponseWriter(ow)
	if f, ok := w.(http.Flusher); ok {
		rw = &flusherResponseWriter{observedResponseWriter: ow, flusher: f}
	}
	metrics.RecordHTTPInFlight(float64(s.metrics.inFlight.Add(1)))
	metrics.RecordHTTPInFlightRoute(route, float64(s.metrics.adjustRouteInFlight(route, 1)))
	defer func() {
		metrics.RecordHTTPInFlight(float64(s.metrics.inFlight.Add(-1)))
		metrics.RecordHTTPInFlightRoute(route, float64(s.metrics.adjustRouteInFlight(route, -1)))
	}()

	next.ServeHTTP(rw, r)
	if ow.status == 0 {
		ow.status = http.StatusOK
	}

	dur := time.Since(start)
	s.metrics.record(r.Method, route, ow.status, dur)

	tenantID := ""
	apiKeyID := ""
	if scope := ScopeFromContext(r.Context()); scope != nil {
		tenantID = scope.TenantID
		apiKeyID = scope.APIKeyID
	}

	fields := []zap.Field{
		zap.String("method", r.Method),
		zap.String("path", r.URL.Path),
		zap.String("route", route),
		zap.Int("status", ow.status),
		zap.Int("bytes", ow.size),
		zap.Float64("duration_ms", dur.Seconds()*1000),
		zap.String("remote", r.RemoteAddr),
		zap.String("user_agent", r.UserAgent()),
	}
	if tenantID != "" {
		fields = append(fields, zap.String("tenant_id", tenantID))
	}
	if apiKeyID != "" {
		fields = append(fields, zap.String("api_key_id", apiKeyID))
	}
	if route == "/metrics" || route == "/healthz" {
		return
	}
	logger.Info(r.Context(), "http_request", fields...)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	s.metrics.writePrometheus(w)
}
