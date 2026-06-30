package server

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"github.com/mem9-ai/drive9/pkg/traceid"
	"go.uber.org/zap"
)

type metricsKeyType int

const (
	metricsKey metricsKeyType = iota
	requestMetricsKey
)

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

type requestMetricState struct {
	mu             sync.Mutex
	tenantID       string
	apiKeyID       string
	provider       string
	surface        string
	action         string
	tenantInFlight bool

	bodyReadObserved bool
	bodyReadMethod   string
	bodyReadRoute    string
	bodyReadBytes    int64
	bodyReadDuration time.Duration

	// sseStreamEstablished is set by an SSE handler (handleEvents) once it has
	// written the 200 streaming headers and entered its long-lived select
	// loop. observe uses it to distinguish a real SSE connection lifetime
	// (skip HTTP duration, record SSE metrics) from a bounded error response
	// on the same route (bad auth, invalid since, non-GET) that must still be
	// recorded into the HTTP/tenant duration histograms. See the route guard
	// in observe.
	sseStreamEstablished bool
}

func withMetrics(ctx context.Context, m *serverMetrics) context.Context {
	return context.WithValue(ctx, metricsKey, m)
}

func withRequestMetricState(ctx context.Context, state *requestMetricState) context.Context {
	return context.WithValue(ctx, requestMetricsKey, state)
}

func metricsFromContext(ctx context.Context) *serverMetrics {
	v, _ := ctx.Value(metricsKey).(*serverMetrics)
	return v
}

func requestMetricStateFromContext(ctx context.Context) *requestMetricState {
	v, _ := ctx.Value(requestMetricsKey).(*requestMetricState)
	return v
}

// markSSEStreamEstablished signals that the SSE handler has written the
// streaming response headers and entered its long-lived loop, so observe
// should treat this as a real SSE connection lifetime (skip the HTTP
// duration histogram) rather than a bounded error response. Safe to call
// from the SSE handler after WriteHeader(200).
func markSSEStreamEstablished(ctx context.Context) {
	if st := requestMetricStateFromContext(ctx); st != nil {
		st.mu.Lock()
		st.sseStreamEstablished = true
		st.mu.Unlock()
	}
}

// sseStreamEstablished reports whether the SSE handler marked the response
// as an established streaming connection. observe uses this to decide whether
// to skip the HTTP duration histogram.
func sseStreamEstablished(ctx context.Context) bool {
	if st := requestMetricStateFromContext(ctx); st != nil {
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.sseStreamEstablished
	}
	return false
}

// recordTenantHTTPResponseBytes records the response body bytes for a request
// without the duration histogram (used for established SSE streams where the
// duration is excluded but byte accounting is still wanted).
func recordTenantHTTPResponseBytes(r *http.Request, responseBytes int) {
	tenantID := requestTenantID(r)
	if tenantID == "" || responseBytes <= 0 {
		return
	}
	class := classifyTenantRequest(r)
	if scopedClass, ok := requestMetricClass(r.Context()); ok {
		class = scopedClass
	}
	metrics.RecordTenantHTTPBytes(tenantID, class.surface, class.action, "response", int64(responseBytes))
}

func setRequestMetricScope(ctx context.Context, scope *TenantScope, class tenantRequestClass) {
	if scope == nil {
		return
	}
	setRequestMetricTenant(ctx, scope.TenantID, scope.APIKeyID, scope.Provider, class)
}

func setRequestMetricTenant(ctx context.Context, tenantID, apiKeyID, provider string, class tenantRequestClass) {
	state := requestMetricStateFromContext(ctx)
	if state == nil || tenantID == "" {
		return
	}
	surface := strings.TrimSpace(class.surface)
	if surface == "" {
		surface = "other"
	}
	action := strings.TrimSpace(class.action)
	if action == "" {
		action = "other"
	}

	var oldTenantID, oldSurface, oldAction string
	var shouldMove bool
	state.mu.Lock()
	if state.tenantInFlight {
		shouldMove = state.tenantID != tenantID || state.surface != surface || state.action != action
		if shouldMove {
			oldTenantID = state.tenantID
			oldSurface = state.surface
			oldAction = state.action
			state.surface = surface
			state.action = action
		}
		state.tenantID = tenantID
		state.apiKeyID = apiKeyID
		state.provider = provider
		state.mu.Unlock()
		if shouldMove {
			if m := metricsFromContext(ctx); m != nil {
				m.adjustTenantInFlight(oldTenantID, oldSurface, oldAction, -1)
				m.adjustTenantInFlight(tenantID, surface, action, 1)
			}
		}
		return
	}
	state.tenantID = tenantID
	state.apiKeyID = apiKeyID
	state.provider = provider
	state.surface = surface
	state.action = action
	state.tenantInFlight = true
	state.mu.Unlock()

	if m := metricsFromContext(ctx); m != nil {
		m.adjustTenantInFlight(tenantID, surface, action, 1)
	}
}

func requestMetricScope(ctx context.Context) (tenantID, apiKeyID, provider string) {
	state := requestMetricStateFromContext(ctx)
	if state == nil {
		return "", "", ""
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.tenantID, state.apiKeyID, state.provider
}

func requestMetricClass(ctx context.Context) (tenantRequestClass, bool) {
	state := requestMetricStateFromContext(ctx)
	if state == nil {
		return tenantRequestClass{}, false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.tenantID == "" || state.surface == "" || state.action == "" {
		return tenantRequestClass{}, false
	}
	return tenantRequestClass{surface: state.surface, action: state.action}, true
}

func setRequestBodyReadMetric(ctx context.Context, method, route string, bytes int64, d time.Duration) {
	state := requestMetricStateFromContext(ctx)
	if state == nil {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	state.bodyReadObserved = true
	state.bodyReadMethod = method
	state.bodyReadRoute = route
	state.bodyReadBytes = bytes
	state.bodyReadDuration = d
}

func requestBodyReadMetric(ctx context.Context) (method, route string, bytes int64, d time.Duration, ok bool) {
	state := requestMetricStateFromContext(ctx)
	if state == nil {
		return "", "", 0, 0, false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.bodyReadObserved {
		return "", "", 0, 0, false
	}
	return state.bodyReadMethod, state.bodyReadRoute, state.bodyReadBytes, state.bodyReadDuration, true
}

func finishRequestMetricTenant(ctx context.Context) {
	state := requestMetricStateFromContext(ctx)
	if state == nil {
		return
	}
	state.mu.Lock()
	if !state.tenantInFlight {
		state.mu.Unlock()
		return
	}
	tenantID := state.tenantID
	surface := state.surface
	action := state.action
	state.tenantInFlight = false
	state.mu.Unlock()

	if m := metricsFromContext(ctx); m != nil {
		m.adjustTenantInFlight(tenantID, surface, action, -1)
	}
}

func metricEvent(ctx context.Context, event string, labels ...string) {
	m := metricsFromContext(ctx)
	if m == nil {
		return
	}
	tenantID, _, _ := requestMetricScope(ctx)
	m.recordEvent(tenantID, event, labels...)
}

type serverMetrics struct {
	inFlight atomic.Int64
	routeMu  sync.Mutex
	routes   map[string]int64

	tenantMu       sync.Mutex
	tenantInFlight map[string]int64
}

func newServerMetrics() *serverMetrics {
	metrics.SetModuleAvailability("server", true)
	metrics.RecordHTTPInFlight(0)
	return &serverMetrics{
		routes:         map[string]int64{},
		tenantInFlight: map[string]int64{},
	}
}

func (m *serverMetrics) record(method, route string, status int, d time.Duration) {
	metrics.RecordHTTPRequest(method, route, status, d)
}

func (m *serverMetrics) recordBodyRead(method, route string, status int, bodyBytes int64, d time.Duration) {
	metrics.RecordHTTPRequestBodyRead(method, route, status, bodyBytes, d)
}

// recordCount records only the request counter (no duration histogram).
// Used for SSE/streaming routes whose handler blocks for the connection
// lifetime — recording that lifetime into the HTTP duration histogram would
// pollute all HTTP P95/P99 alerts. See observe for the route guard.
func (m *serverMetrics) recordCount(method, route string, status int) {
	metrics.RecordHTTPRequestCount(method, route, status)
}

func (m *serverMetrics) recordEvent(tenantID, event string, labels ...string) {
	metrics.RecordTenantEvent(tenantID, event, labels...)
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

func (m *serverMetrics) adjustTenantInFlight(tenantID, surface, action string, delta int64) int64 {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return 0
	}
	surface = strings.TrimSpace(surface)
	if surface == "" {
		surface = "other"
	}
	action = strings.TrimSpace(action)
	if action == "" {
		action = "other"
	}
	key := tenantInFlightKey(tenantID, surface, action)

	m.tenantMu.Lock()
	defer m.tenantMu.Unlock()
	next := m.tenantInFlight[key] + delta
	if next <= 0 {
		delete(m.tenantInFlight, key)
		metrics.RecordTenantInFlight(tenantID, surface, action, 0)
		return 0
	}
	m.tenantInFlight[key] = next
	metrics.RecordTenantInFlight(tenantID, surface, action, float64(next))
	return next
}

func tenantInFlightKey(tenantID, surface, action string) string {
	return tenantID + "\x00" + surface + "\x00" + action
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
	case path == "/v1/quota":
		return "/v1/quota"
	case path == "/v1/status":
		return "/v1/status"
	case path == "/v1/tenant":
		return "/v1/tenant"
	case path == "/v1/tokens" || strings.HasPrefix(path, "/v1/tokens/"):
		return "/v1/tokens/*"
	case path == "/v1/sql":
		return "/v1/sql"
	case path == sseEventsRoute:
		return sseEventsRoute
	case path == sseNotifyInternalRoute:
		return sseNotifyInternalRoute
	case path == "/v1/fork":
		return "/v1/fork"
	case path == "/v1/fs:batch-stat":
		return "/v1/fs:batch-stat"
	case path == "/v1/fs:batch-read-small":
		return "/v1/fs:batch-read-small"
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

type tenantRequestClass struct {
	surface string
	action  string
}

func classifyTenantRequest(r *http.Request) tenantRequestClass {
	if r == nil || r.URL == nil {
		return tenantRequestClass{surface: "other", action: "other"}
	}
	path := r.URL.Path
	switch {
	case path == "/v1/fs:batch-stat":
		return tenantRequestClass{surface: "fs", action: "batch_stat"}
	case path == "/v1/fs:batch-read-small":
		return tenantRequestClass{surface: "fs", action: "batch_read_small"}
	case strings.HasPrefix(path, "/v1/fs/"):
		return tenantRequestClass{surface: "fs", action: classifyFSAction(r)}
	case path == "/v1/uploads":
		if r.Method == http.MethodPost {
			return tenantRequestClass{surface: "upload", action: "initiate"}
		}
		return tenantRequestClass{surface: "upload", action: "list"}
	case path == "/v1/uploads/initiate":
		return tenantRequestClass{surface: "upload", action: "initiate"}
	case strings.HasPrefix(path, "/v1/uploads/"):
		return tenantRequestClass{surface: "upload", action: classifyV1UploadAction(r)}
	case strings.HasPrefix(path, "/v2/uploads/"):
		return tenantRequestClass{surface: "upload", action: classifyV2UploadAction(r)}
	case path == "/v1/sql":
		return tenantRequestClass{surface: "sql", action: strings.ToLower(r.Method)}
	case path == sseEventsRoute:
		return tenantRequestClass{surface: "events", action: strings.ToLower(r.Method)}
	case path == "/v1/tokens" || strings.HasPrefix(path, "/v1/tokens/"):
		return tenantRequestClass{surface: "tokens", action: classifyTokenAction(r)}
	case path == "/v1/journals" || strings.HasPrefix(path, "/v1/journals/") || path == "/v1/journal-entries":
		return tenantRequestClass{surface: "journal", action: strings.ToLower(r.Method)}
	case path == "/v1/git-workspaces" || strings.HasPrefix(path, "/v1/git-workspaces/"):
		return tenantRequestClass{surface: "git_workspace", action: strings.ToLower(r.Method)}
	case strings.HasPrefix(path, "/v1/vault/"):
		return tenantRequestClass{surface: "vault", action: classifyVaultAction(r)}
	case strings.HasPrefix(path, "/s3/"):
		return tenantRequestClass{surface: "object_store", action: classifyS3Action(r)}
	case path == "/v1/status":
		return tenantRequestClass{surface: "status", action: strings.ToLower(r.Method)}
	case path == "/v1/provision":
		return tenantRequestClass{surface: "provision", action: strings.ToLower(r.Method)}
	case path == "/v1/quota":
		return tenantRequestClass{surface: "quota", action: strings.ToLower(r.Method)}
	case path == "/v1/tenant":
		return tenantRequestClass{surface: "tenant", action: strings.ToLower(r.Method)}
	default:
		return tenantRequestClass{surface: "other", action: strings.ToLower(r.Method)}
	}
}

func classifyFSAction(r *http.Request) string {
	switch r.Method {
	case http.MethodGet:
		q := r.URL.Query()
		switch {
		case q.Has("stat"):
			return "stat"
		case q.Has("grep"):
			return "grep"
		case q.Has("find"):
			return "find"
		case q.Has("list"):
			return "list"
		default:
			return "read"
		}
	case http.MethodHead:
		return "stat"
	case http.MethodPut:
		return "write"
	case http.MethodPatch:
		return "patch"
	case http.MethodDelete:
		return "delete"
	case http.MethodPost:
		q := r.URL.Query()
		for _, key := range []string{"append", "copy", "rename", "mkdir", "chmod", "create", "symlink", "hardlink"} {
			if q.Has(key) {
				return key
			}
		}
		return "post"
	default:
		return strings.ToLower(r.Method)
	}
}

func classifyV1UploadAction(r *http.Request) string {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/uploads/")
	if rest == "" {
		return strings.ToLower(r.Method)
	}
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) == 1 {
		if r.Method == http.MethodDelete {
			return "abort"
		}
		return "other"
	}
	action := strings.Trim(parts[1], "/")
	if action == "" && r.Method == http.MethodDelete {
		return "abort"
	}
	switch {
	case strings.HasPrefix(action, "complete"):
		return "complete"
	case strings.HasPrefix(action, "resume"):
		return "resume"
	default:
		return "other"
	}
}

func classifyV2UploadAction(r *http.Request) string {
	rest := strings.TrimPrefix(r.URL.Path, "/v2/uploads/")
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) == 0 || parts[0] == "" {
		return strings.ToLower(r.Method)
	}
	if parts[0] == "initiate" {
		return "initiate"
	}
	if len(parts) == 1 {
		return "other"
	}
	switch strings.Trim(parts[1], "/") {
	case "presign":
		return "presign"
	case "presign-batch":
		return "presign_batch"
	case "complete":
		return "complete"
	case "abort":
		return "abort"
	default:
		return "other"
	}
}

func classifyVaultAction(r *http.Request) string {
	path := r.URL.Path
	switch {
	case strings.HasPrefix(path, "/v1/vault/secrets"):
		return "secrets_" + strings.ToLower(r.Method)
	case strings.HasPrefix(path, "/v1/vault/tokens"):
		return "tokens_" + strings.ToLower(r.Method)
	case strings.HasPrefix(path, "/v1/vault/grants"):
		return "grants_" + strings.ToLower(r.Method)
	case strings.HasPrefix(path, "/v1/vault/read"):
		return "read"
	case path == "/v1/vault/audit":
		return "audit"
	default:
		return strings.ToLower(r.Method)
	}
}

func classifyTokenAction(r *http.Request) string {
	switch {
	case r.URL.Path == "/v1/tokens" && r.Method == http.MethodPost:
		return "issue"
	case r.URL.Path == "/v1/tokens/revoke" && r.Method == http.MethodPost:
		return "revoke_by_key"
	case strings.HasPrefix(r.URL.Path, "/v1/tokens/") && r.Method == http.MethodDelete:
		return "revoke"
	default:
		return strings.ToLower(r.Method)
	}
}

func classifyS3Action(r *http.Request) string {
	if strings.Contains(r.URL.Path, "/upload/") && r.Method == http.MethodPut {
		return "upload_part"
	}
	if strings.Contains(r.URL.Path, "/objects/") && r.Method == http.MethodGet {
		return "get_object"
	}
	return strings.ToLower(r.Method)
}

func tenantRequestResult(status int) string {
	switch {
	case status >= 200 && status < 400:
		return "ok"
	case status == http.StatusUnauthorized:
		return "unauthorized"
	case status == http.StatusForbidden:
		return "denied"
	case status == http.StatusNotFound:
		return "not_found"
	case status == http.StatusConflict:
		return "conflict"
	case status == http.StatusInsufficientStorage || status == http.StatusTooManyRequests:
		return "quota_or_rate_limited"
	case status >= 400 && status < 500:
		return "client_error"
	case status >= 500:
		return "server_error"
	default:
		return "unknown"
	}
}

func requestTenantID(r *http.Request) string {
	tenantID, _, _ := requestMetricScope(r.Context())
	if tenantID != "" {
		return tenantID
	}
	if r.URL != nil && strings.HasPrefix(r.URL.Path, "/s3/") {
		rest := strings.TrimPrefix(r.URL.Path, "/s3/")
		if tenant, _, ok := strings.Cut(rest, "/"); ok && tenant != "" {
			if tenant == "local" || tenant == "upload" || tenant == "objects" {
				return "local"
			}
		}
	}
	return ""
}

func recordTenantHTTPRequest(r *http.Request, status int, d time.Duration, responseBytes int) {
	tenantID := requestTenantID(r)
	if tenantID == "" {
		return
	}
	class := classifyTenantRequest(r)
	if scopedClass, ok := requestMetricClass(r.Context()); ok {
		class = scopedClass
	}
	metrics.RecordTenantRequest(tenantID, class.surface, class.action, tenantRequestResult(status), status, d)
	if r.ContentLength > 0 {
		metrics.RecordTenantHTTPBytes(tenantID, class.surface, class.action, "request", r.ContentLength)
	}
	if responseBytes > 0 {
		metrics.RecordTenantHTTPBytes(tenantID, class.surface, class.action, "response", int64(responseBytes))
	}
}

// recordTenantHTTPRequestCount records only the tenant request counter (no
// duration histogram) for SSE/streaming routes. Companion to recordCount.
func recordTenantHTTPRequestCount(r *http.Request, status int) {
	tenantID := requestTenantID(r)
	if tenantID == "" {
		return
	}
	class := classifyTenantRequest(r)
	if scopedClass, ok := requestMetricClass(r.Context()); ok {
		class = scopedClass
	}
	metrics.RecordTenantRequestCount(tenantID, class.surface, class.action, tenantRequestResult(status), status)
	if r.ContentLength > 0 {
		metrics.RecordTenantHTTPBytes(tenantID, class.surface, class.action, "request", r.ContentLength)
	}
}

func recordTenantFileBytes(ctx context.Context, surface, action, direction string, bytes int64) {
	tenantID, _, _ := requestMetricScope(ctx)
	if tenantID == "" || bytes <= 0 {
		return
	}
	metrics.RecordTenantFileBytes(tenantID, surface, action, direction, bytes)
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
	r = r.WithContext(withRequestMetricState(r.Context(), &requestMetricState{}))
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
		finishRequestMetricTenant(r.Context())
	}()

	if strings.HasPrefix(r.URL.Path, "/s3/") {
		setRequestMetricTenant(r.Context(), requestTenantID(r), "", "", classifyTenantRequest(r))
	}

	next.ServeHTTP(rw, r)
	if ow.status == 0 {
		ow.status = http.StatusOK
	}

	dur := time.Since(start)
	// An established SSE stream (/v1/events) blocks in a select loop for the
	// entire client connection lifetime — recording that lifetime into the
	// HTTP request duration histogram would pollute all HTTP P95/P99 alerts
	// (a 40s SSE connection would look like a 40s "request"). handleEvents sets
	// the sseStreamEstablished flag only after it has written the 200 streaming
	// headers, so bounded error responses on the same route (bad auth, invalid
	// since, non-GET, backend setup failures) are still recorded as normal
	// HTTP requests. For an established stream, record only the request counter
	// (+ tenant request counter + response bytes); the connection lifetime goes
	// into the dedicated SSE metrics recorded by handleEvents. duration_ms is
	// still logged below for debugging.
	if route == sseEventsRoute && sseStreamEstablished(r.Context()) {
		s.metrics.recordCount(r.Method, route, ow.status)
		recordTenantHTTPRequestCount(r, ow.status)
		if ow.size > 0 {
			recordTenantHTTPResponseBytes(r, ow.size)
		}
	} else {
		s.metrics.record(r.Method, route, ow.status, dur)
		if method, bodyRoute, bodyBytes, bodyReadDuration, ok := requestBodyReadMetric(r.Context()); ok {
			if method == "" {
				method = r.Method
			}
			if bodyRoute == "" {
				bodyRoute = route
			}
			s.metrics.recordBodyRead(method, bodyRoute, ow.status, bodyBytes, bodyReadDuration)
		}
		recordTenantHTTPRequest(r, ow.status, dur, ow.size)
	}

	tenantID, apiKeyID, _ := requestMetricScope(r.Context())

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
