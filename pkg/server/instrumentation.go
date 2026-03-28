package server

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
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

	mu             sync.RWMutex
	requests       map[string]int64
	durationCount  map[string]int64
	durationSecond map[string]float64
	events         map[string]int64
}

func newServerMetrics() *serverMetrics {
	return &serverMetrics{
		requests:       make(map[string]int64),
		durationCount:  make(map[string]int64),
		durationSecond: make(map[string]float64),
		events:         make(map[string]int64),
	}
}

func (m *serverMetrics) record(method, route string, status int, d time.Duration) {
	statusKey := metricLabels(method, route, strconv.Itoa(status))
	durationKey := metricLabels(method, route)

	m.mu.Lock()
	m.requests[statusKey]++
	m.durationCount[durationKey]++
	m.durationSecond[durationKey] += d.Seconds()
	m.mu.Unlock()
}

func (m *serverMetrics) recordEvent(event string, labels ...string) {
	key := metricEventLabels(event, labels...)
	m.mu.Lock()
	m.events[key]++
	m.mu.Unlock()
}

func (m *serverMetrics) writePrometheus(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")

	_, _ = fmt.Fprintln(w, "# HELP dat9_http_inflight_requests Current in-flight HTTP requests")
	_, _ = fmt.Fprintln(w, "# TYPE dat9_http_inflight_requests gauge")
	_, _ = fmt.Fprintf(w, "dat9_http_inflight_requests %d\n", m.inFlight.Load())

	m.mu.RLock()
	requestKeys := sortedKeys(m.requests)
	durationKeys := sortedKeys(m.durationCount)
	eventKeys := sortedKeys(m.events)
	requests := cloneIntMap(m.requests)
	durationCount := cloneIntMap(m.durationCount)
	durationSecond := cloneFloatMap(m.durationSecond)
	events := cloneIntMap(m.events)
	m.mu.RUnlock()

	_, _ = fmt.Fprintln(w, "# HELP dat9_http_requests_total Total HTTP requests by method/route/status")
	_, _ = fmt.Fprintln(w, "# TYPE dat9_http_requests_total counter")
	for _, k := range requestKeys {
		_, _ = fmt.Fprintf(w, "dat9_http_requests_total{%s} %d\n", k, requests[k])
	}

	_, _ = fmt.Fprintln(w, "# HELP dat9_http_request_duration_seconds_count HTTP request duration count by method/route")
	_, _ = fmt.Fprintln(w, "# TYPE dat9_http_request_duration_seconds_count counter")
	for _, k := range durationKeys {
		_, _ = fmt.Fprintf(w, "dat9_http_request_duration_seconds_count{%s} %d\n", k, durationCount[k])
	}

	_, _ = fmt.Fprintln(w, "# HELP dat9_http_request_duration_seconds_sum HTTP request duration sum in seconds by method/route")
	_, _ = fmt.Fprintln(w, "# TYPE dat9_http_request_duration_seconds_sum counter")
	for _, k := range durationKeys {
		_, _ = fmt.Fprintf(w, "dat9_http_request_duration_seconds_sum{%s} %.6f\n", k, durationSecond[k])
	}

	_, _ = fmt.Fprintln(w, "# HELP dat9_tenant_events_total Tenant/business lifecycle events")
	_, _ = fmt.Fprintln(w, "# TYPE dat9_tenant_events_total counter")
	for _, k := range eventKeys {
		_, _ = fmt.Fprintf(w, "dat9_tenant_events_total{%s} %d\n", k, events[k])
	}
}

func cloneIntMap(src map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func cloneFloatMap(src map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func metricLabels(method, route string, status ...string) string {
	b := strings.Builder{}
	b.WriteString(`method="`)
	b.WriteString(escapePromLabel(method))
	b.WriteString(`",route="`)
	b.WriteString(escapePromLabel(route))
	b.WriteString(`"`)
	if len(status) > 0 {
		b.WriteString(`,status="`)
		b.WriteString(escapePromLabel(status[0]))
		b.WriteString(`"`)
	}
	return b.String()
}

func metricEventLabels(event string, labels ...string) string {
	b := strings.Builder{}
	b.WriteString(`event="`)
	b.WriteString(escapePromLabel(event))
	b.WriteString(`"`)
	for i := 0; i+1 < len(labels); i += 2 {
		b.WriteString(",")
		b.WriteString(escapePromLabel(labels[i]))
		b.WriteString(`="`)
		b.WriteString(escapePromLabel(labels[i+1]))
		b.WriteString(`"`)
	}
	return b.String()
}

func escapePromLabel(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, `"`, `\"`)
	s = strings.ReplaceAll(s, "\n", "\\n")
	return s
}

type observedResponseWriter struct {
	http.ResponseWriter
	status int
	size   int
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
	case path == "/v1/provision":
		return "/v1/provision"
	case path == "/v1/status":
		return "/v1/status"
	case path == "/v1/sql":
		return "/v1/sql"
	case strings.HasPrefix(path, "/v1/fs/"):
		return "/v1/fs/*"
	case path == "/v1/uploads":
		return "/v1/uploads"
	case strings.HasPrefix(path, "/v1/uploads/"):
		return "/v1/uploads/*"
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
	ow := &observedResponseWriter{ResponseWriter: w}
	s.metrics.inFlight.Add(1)
	defer s.metrics.inFlight.Add(-1)

	next.ServeHTTP(ow, r)
	if ow.status == 0 {
		ow.status = http.StatusOK
	}

	dur := time.Since(start)
	route := requestRoute(r.URL.Path)
	s.metrics.record(r.Method, route, ow.status, dur)

	tenantID := ""
	apiKeyID := ""
	if scope := ScopeFromContext(r.Context()); scope != nil {
		tenantID = scope.TenantID
		apiKeyID = scope.APIKeyID
	}

	fields := []zap.Field{
		zap.String("trace_id", traceID),
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
	logger.Info(r.Context(), "http_request", fields...)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	s.metrics.writePrometheus(w)
	metrics.WritePrometheus(w)
}
