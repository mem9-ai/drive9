// Package metrics provides process-wide metrics with an OpenTelemetry-like
// meter/instrument/attribute model and a Prometheus text exporter.
package metrics

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

var operationDurationBounds = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}
var httpDurationBounds = []float64{0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10}

// sseConnectionDurationBounds covers SSE connection lifetimes, which can
// range from sub-second probes to long-lived mounts. Buckets extend well
// beyond httpDurationBounds so long-lived connections are visible without
// piling into +Inf. These are recorded into a dedicated histogram
// (drive9_sse_connection_duration_seconds), NOT drive9_http_request_duration_seconds.
var sseConnectionDurationBounds = []float64{0.005, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300, 600, 1800, 3600}

// eventBusQueryDurationBounds covers fs_events DB query latencies. These
// should normally be low (PK range scan); a shift right indicates DB
// pressure or table growth.
var eventBusQueryDurationBounds = []float64{0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// ssePhase1DurationBounds covers the Phase-1 replay/reset cost of a new SSE
// connection (one EventsSince call + stream of buffered events). Used to
// distinguish slow first-replay from slow Phase-2 live streaming.
var ssePhase1DurationBounds = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

var globalRegistry = NewRegistry()
var globalMeterProvider = NewMeterProvider(globalRegistry)

var serviceMeter = globalMeterProvider.Meter("service")
var httpMeter = globalMeterProvider.Meter("http")
var eventMeter = globalMeterProvider.Meter("event")
var fuseMeter = globalMeterProvider.Meter("fuse")
var sseMeter = globalMeterProvider.Meter("sse")
var tenantMeter = globalMeterProvider.Meter("tenant")

var serviceOperationsTotal = serviceMeter.Int64Counter("drive9_service_operations_total", "Service operations by component/operation/result")
var serviceOperationDuration = serviceMeter.Float64Histogram("drive9_service_operation_duration_seconds", "Service operation duration histogram", operationDurationBounds)
var serviceGauge = serviceMeter.Float64Gauge("drive9_service_gauge", "Service gauges by component/name")

var httpRequestsTotal = httpMeter.Int64Counter("drive9_http_requests_total", "Total HTTP requests by method/route/status")
var httpRequestDuration = httpMeter.Float64Histogram("drive9_http_request_duration_seconds", "HTTP request duration histogram by method/route", httpDurationBounds)
var httpRequestBodyReadDuration = httpMeter.Float64Histogram("drive9_http_request_body_read_duration_seconds", "HTTP request body read duration histogram by method/route/status_class/body_size_bucket", httpDurationBounds)
var httpInflight = httpMeter.Float64Gauge("drive9_http_inflight_requests", "Current in-flight HTTP requests")

var businessEventsTotal = eventMeter.Int64Counter("drive9_business_events_total", "Business lifecycle events")

var fuseOperationsTotal = fuseMeter.Int64Counter("drive9_fuse_operations_total", "FUSE operations by operation/result")
var fuseOperationDuration = fuseMeter.Float64Histogram("drive9_fuse_operation_duration_seconds", "FUSE operation duration histogram", operationDurationBounds)
var fuseOperationBytes = fuseMeter.Int64Counter("drive9_fuse_operation_bytes_total", "Bytes processed by FUSE operation/result")
var fuseRemoteOperationsTotal = fuseMeter.Int64Counter("drive9_fuse_remote_operations_total", "Remote FUSE operations by operation/result")
var fuseRemoteOperationDuration = fuseMeter.Float64Histogram("drive9_fuse_remote_operation_duration_seconds", "Remote FUSE operation duration histogram", operationDurationBounds)
var fuseRemoteOperationBytes = fuseMeter.Int64Counter("drive9_fuse_remote_operation_bytes_total", "Bytes processed by remote FUSE operation/result")

var tenantRequestsTotal = tenantMeter.Int64Counter("drive9_tenant_requests_total", "Tenant-scoped requests by tenant/surface/action/result/status/status_class")
var tenantRequestDuration = tenantMeter.Float64Histogram("drive9_tenant_request_duration_seconds", "Tenant-scoped request duration histogram", httpDurationBounds)
var tenantInflight = tenantMeter.Float64Gauge("drive9_tenant_inflight_requests", "Current in-flight tenant-scoped requests by tenant/surface/action")
var tenantHTTPBytes = tenantMeter.Int64Counter("drive9_tenant_http_bytes_total", "Tenant-scoped HTTP transport bytes by tenant/surface/action/direction")
var tenantFileBytes = tenantMeter.Int64Counter("drive9_tenant_file_bytes_total", "Tenant-scoped logical file bytes by tenant/surface/action/direction")
var tenantStorageBytes = tenantMeter.Float64Gauge("drive9_tenant_storage_bytes", "Tenant storage bytes by state")
var tenantMediaFiles = tenantMeter.Float64Gauge("drive9_tenant_media_files", "Tenant media file count by state")

// SSE-specific instruments. SSE connection lifetimes are recorded here, not
// in drive9_http_request_duration_seconds (which would pollute HTTP latency
// alerts — SSE connections live as long as the client stays subscribed).
var sseConnectionsTotal = sseMeter.Int64Counter("drive9_sse_connections_total", "SSE /v1/events connections opened by tenant_id/reason")
var sseConnectionDuration = sseMeter.Float64Histogram("drive9_sse_connection_duration_seconds", "SSE connection lifetime (client stay-open duration, not request processing time)", sseConnectionDurationBounds)
var sseInflight = sseMeter.Float64Gauge("drive9_sse_inflight_connections", "Active SSE /v1/events connections by tenant_id")
var ssePhase1Duration = sseMeter.Float64Histogram("drive9_sse_phase1_duration_seconds", "SSE Phase-1 replay/reset duration (one EventsSince call + buffered stream)", ssePhase1DurationBounds)
var sseEventsSentTotal = sseMeter.Int64Counter("drive9_sse_events_sent_total", "SSE events sent to clients by type/tenant_id")
var sseResetsSentTotal = sseMeter.Int64Counter("drive9_sse_resets_sent_total", "SSE reset events sent by reason/tenant_id")
var sseHeartbeatsSentTotal = sseMeter.Int64Counter("drive9_sse_heartbeats_sent_total", "SSE heartbeat events sent by tenant_id")

// Event-bus query instruments. Covers all fs_events DB reads (events_since,
// poll, latest, oldest) so DB pressure and table growth on the events path
// are observable without direct DB access.
var eventBusQueryDuration = serviceMeter.Float64Histogram("drive9_event_bus_query_duration_seconds", "Event-bus fs_events query duration by operation/result/tenant_id", eventBusQueryDurationBounds)
var eventBusPollFailuresTotal = sseMeter.Int64Counter("drive9_event_bus_poll_failures_total", "Event-bus cross-pod poll query failures by tenant_id")
var eventBusPublishErrorsTotal = sseMeter.Int64Counter("drive9_event_bus_publish_errors_total", "Event-bus fs_events INSERT failures by tenant_id")

// fs_events table instruments. Compensates for the lack of direct TiDB
// access: row count and prune volume are reported by the server itself.
var fsEventsRows = sseMeter.Float64Gauge("drive9_fs_events_rows", "fs_events table row count by tenant_id")
var fsEventsPrunedTotal = sseMeter.Int64Counter("drive9_fs_events_pruned_total", "fs_events rows pruned by retention cleanup by tenant_id")

func RegisterModule(module string) {
	globalRegistry.RegisterModule(module)
}

func SetModuleAvailability(module string, up bool) {
	globalRegistry.SetModuleAvailability(module, up)
}

func RecordOperation(component, operation, result string, d time.Duration) {
	RecordTenantOperation("", component, operation, result, d)
}

func RecordTenantOperation(tenantID, component, operation, result string, d time.Duration) {
	component = cleanMetricValue(component, "unknown")
	operation = cleanMetricValue(operation, "unknown")
	result = cleanMetricValue(result, "unknown")
	tenantID = cleanMetricValue(tenantID, "unknown")
	RegisterModule(component)
	attrs := []Attribute{
		Attr("component", component),
		Attr("operation", operation),
		Attr("result", result),
	}
	if tenantID != "unknown" {
		attrs = append(attrs, Attr("tenant_id", tenantID))
	}
	serviceOperationsTotal.Add(1, attrs...)
	serviceOperationDuration.Observe(d.Seconds(), attrs...)
}

func RecordGauge(component, name string, value float64) {
	RecordTenantGauge("", component, name, value)
}

func RecordTenantGauge(tenantID, component, name string, value float64) {
	component = cleanMetricValue(component, "unknown")
	name = cleanMetricValue(name, "unknown")
	tenantID = strings.TrimSpace(tenantID)
	RegisterModule(component)
	serviceGauge.Set(value, Attr("component", component), Attr("name", name), Attr("tenant_id", tenantID))
}

func RecordHTTPRequest(method, route string, status int, d time.Duration) {
	RegisterModule("server")
	httpRequestsTotal.Add(1,
		Attr("method", cleanMetricValue(method, "UNKNOWN")),
		Attr("route", cleanMetricValue(route, "other")),
		Attr("status", strconv.Itoa(status)),
	)
	httpRequestDuration.Observe(d.Seconds(),
		Attr("method", cleanMetricValue(method, "UNKNOWN")),
		Attr("route", cleanMetricValue(route, "other")),
	)
}

func RecordHTTPRequestBodyRead(method, route string, status int, bodyBytes int64, d time.Duration) {
	RegisterModule("server")
	httpRequestBodyReadDuration.Observe(d.Seconds(),
		Attr("method", cleanMetricValue(method, "UNKNOWN")),
		Attr("route", cleanMetricValue(route, "other")),
		Attr("status_class", statusClass(status)),
		Attr("body_size_bucket", bodySizeBucket(bodyBytes)),
	)
}

// RecordHTTPRequestCount records only the request counter, not the duration
// histogram. Used for SSE/streaming routes whose handler blocks for the
// connection lifetime (which is not request processing time and would
// pollute HTTP latency alerts if recorded into the duration histogram).
func RecordHTTPRequestCount(method, route string, status int) {
	RegisterModule("server")
	httpRequestsTotal.Add(1,
		Attr("method", cleanMetricValue(method, "UNKNOWN")),
		Attr("route", cleanMetricValue(route, "other")),
		Attr("status", strconv.Itoa(status)),
	)
}

// RecordTenantRequestCount records only the tenant request counter, not the
// duration histogram. Companion to RecordHTTPRequestCount for SSE routes.
func RecordTenantRequestCount(tenantID, surface, action, result string, status int) {
	tenantID = cleanMetricValue(tenantID, "unknown")
	surface = cleanMetricValue(surface, "other")
	action = cleanMetricValue(action, "other")
	result = cleanMetricValue(result, "unknown")
	statusValue := "unknown"
	if status > 0 {
		statusValue = strconv.Itoa(status)
	}
	RegisterModule("tenant_usage")
	attrs := []Attribute{
		Attr("tenant_id", tenantID),
		Attr("surface", surface),
		Attr("action", action),
		Attr("result", result),
		Attr("status", statusValue),
		Attr("status_class", statusClass(status)),
	}
	tenantRequestsTotal.Add(1, attrs...)
}

func RecordHTTPInFlight(value float64) {
	RegisterModule("server")
	httpInflight.Set(value)
}

func RecordHTTPInFlightRoute(route string, value float64) {
	RegisterModule("server")
	httpInflight.Set(value, Attr("route", cleanMetricValue(route, "other")))
}

func RecordEvent(event string, labels ...string) {
	RecordTenantEvent("", event, labels...)
}

func RecordTenantEvent(tenantID, event string, labels ...string) {
	RegisterModule("server")
	attrs := make([]Attribute, 0, len(labels)/2+2)
	attrs = append(attrs, Attr("event", cleanMetricValue(event, "unknown")))
	tenantID = cleanMetricValue(tenantID, "unknown")
	if tenantID != "unknown" {
		attrs = append(attrs, Attr("tenant_id", tenantID))
	}
	for i := 0; i+1 < len(labels); i += 2 {
		attrs = append(attrs, Attr(labels[i], labels[i+1]))
	}
	businessEventsTotal.Add(1, attrs...)
}

func RecordTenantRequest(tenantID, surface, action, result string, status int, d time.Duration) {
	tenantID = cleanMetricValue(tenantID, "unknown")
	surface = cleanMetricValue(surface, "other")
	action = cleanMetricValue(action, "other")
	result = cleanMetricValue(result, "unknown")
	statusValue := "unknown"
	statusClass := "unknown"
	if status > 0 {
		statusValue = strconv.Itoa(status)
		statusClass = strconv.Itoa(status/100) + "xx"
	}
	RegisterModule("tenant_usage")
	attrs := []Attribute{
		Attr("tenant_id", tenantID),
		Attr("surface", surface),
		Attr("action", action),
		Attr("result", result),
		Attr("status", statusValue),
		Attr("status_class", statusClass),
	}
	tenantRequestsTotal.Add(1, attrs...)
	tenantRequestDuration.Observe(d.Seconds(), attrs...)
}

func RecordTenantHTTPBytes(tenantID, surface, action, direction string, bytes int64) {
	recordTenantBytes(tenantHTTPBytes, tenantID, surface, action, direction, bytes)
}

func RecordTenantFileBytes(tenantID, surface, action, direction string, bytes int64) {
	recordTenantBytes(tenantFileBytes, tenantID, surface, action, direction, bytes)
}

func RecordTenantInFlight(tenantID, surface, action string, value float64) {
	RegisterModule("tenant_usage")
	tenantInflight.Set(value,
		Attr("tenant_id", cleanMetricValue(tenantID, "unknown")),
		Attr("surface", cleanMetricValue(surface, "other")),
		Attr("action", cleanMetricValue(action, "other")),
	)
}

func RecordTenantStorageBytes(tenantID, state string, bytes int64) {
	if bytes < 0 {
		return
	}
	RegisterModule("tenant_usage")
	tenantStorageBytes.Set(float64(bytes),
		Attr("tenant_id", cleanMetricValue(tenantID, "unknown")),
		Attr("state", cleanMetricValue(state, "unknown")),
	)
}

func RecordTenantMediaFiles(tenantID, state string, count int64) {
	if count < 0 {
		return
	}
	RegisterModule("tenant_usage")
	tenantMediaFiles.Set(float64(count),
		Attr("tenant_id", cleanMetricValue(tenantID, "unknown")),
		Attr("state", cleanMetricValue(state, "unknown")),
	)
}

func recordTenantBytes(counter *Int64Counter, tenantID, surface, action, direction string, bytes int64) {
	if bytes <= 0 {
		return
	}
	RegisterModule("tenant_usage")
	counter.Add(bytes,
		Attr("tenant_id", cleanMetricValue(tenantID, "unknown")),
		Attr("surface", cleanMetricValue(surface, "other")),
		Attr("action", cleanMetricValue(action, "other")),
		Attr("direction", cleanMetricValue(direction, "unknown")),
	)
}

func RecordFuseOperation(operation, result string, d time.Duration, bytes uint64) {
	RegisterModule("fuse")
	attrs := []Attribute{
		Attr("operation", cleanMetricValue(operation, "unknown")),
		Attr("result", cleanMetricValue(result, "unknown")),
	}
	fuseOperationsTotal.Add(1, attrs...)
	fuseOperationDuration.Observe(d.Seconds(), attrs...)
	if bytes > 0 {
		fuseOperationBytes.Add(int64(bytes), attrs...)
	}
}

func RecordFuseRemoteOperation(operation, result string, d time.Duration, bytes uint64) {
	RegisterModule("fuse")
	attrs := []Attribute{
		Attr("operation", cleanMetricValue(operation, "unknown")),
		Attr("result", cleanMetricValue(result, "unknown")),
	}
	fuseRemoteOperationsTotal.Add(1, attrs...)
	fuseRemoteOperationDuration.Observe(d.Seconds(), attrs...)
	if bytes > 0 {
		fuseRemoteOperationBytes.Add(int64(bytes), attrs...)
	}
}

// RecordSSEConnection records an SSE connection lifecycle: it increments the
// connection counter and observes the connection lifetime (time from accept
// to client disconnect) into the dedicated SSE histogram. This duration must
// NOT be recorded into drive9_http_request_duration_seconds — SSE connections
// live as long as the client stays subscribed, so mixing them into the HTTP
// latency histogram would make all HTTP P95/P99 alerts meaningless.
func RecordSSEConnection(tenantID, reason string, d time.Duration) {
	RegisterModule("sse")
	tenantID = cleanMetricValue(tenantID, "unknown")
	sseConnectionsTotal.Add(1, Attr("tenant_id", tenantID), Attr("reason", cleanMetricValue(reason, "unknown")))
	sseConnectionDuration.Observe(d.Seconds(), Attr("tenant_id", tenantID))
}

// RecordSSEInFlight sets the active SSE connection count for a tenant. The
// caller maintains the absolute count (incremented on subscribe, decremented
// on unsubscribe); this records the current value so the gauge reflects the
// true number of live SSE connections per tenant.
func RecordSSEInFlight(tenantID string, count float64) {
	if count < 0 {
		count = 0
	}
	RegisterModule("sse")
	sseInflight.Set(count, Attr("tenant_id", cleanMetricValue(tenantID, "unknown")))
}

// RecordSSEPhase1 records the duration of the SSE Phase-1 replay/reset stage.
func RecordSSEPhase1(tenantID string, d time.Duration) {
	RegisterModule("sse")
	ssePhase1Duration.Observe(d.Seconds(), Attr("tenant_id", cleanMetricValue(tenantID, "unknown")))
}

// RecordSSEEventSent records a single SSE file_changed event delivery.
func RecordSSEEventSent(tenantID, op string) {
	RegisterModule("sse")
	sseEventsSentTotal.Add(1,
		Attr("tenant_id", cleanMetricValue(tenantID, "unknown")),
		Attr("op", cleanMetricValue(op, "unknown")),
	)
}

// RecordSSEResetSent records an SSE reset event delivery.
func RecordSSEResetSent(tenantID, reason string) {
	RegisterModule("sse")
	sseResetsSentTotal.Add(1,
		Attr("tenant_id", cleanMetricValue(tenantID, "unknown")),
		Attr("reason", cleanMetricValue(reason, "unknown")),
	)
}

// RecordSSEHeartbeatSent records an SSE heartbeat delivery.
func RecordSSEHeartbeatSent(tenantID string) {
	RegisterModule("sse")
	sseHeartbeatsSentTotal.Add(1, Attr("tenant_id", cleanMetricValue(tenantID, "unknown")))
}

// RecordEventBusQuery records the duration of an fs_events DB query. Unlike
// the error-only counters, this records every query (ok and error) so DB
// pressure and table growth on the events path are observable.
func RecordEventBusQuery(tenantID, operation, result string, d time.Duration) {
	RegisterModule("sse")
	eventBusQueryDuration.Observe(d.Seconds(),
		Attr("tenant_id", cleanMetricValue(tenantID, "unknown")),
		Attr("operation", cleanMetricValue(operation, "unknown")),
		Attr("result", cleanMetricValue(result, "unknown")),
	)
}

// RecordEventBusPollFailure records a cross-pod poll query failure (previously
// only logged, not metriced).
func RecordEventBusPollFailure(tenantID string) {
	RegisterModule("sse")
	eventBusPollFailuresTotal.Add(1, Attr("tenant_id", cleanMetricValue(tenantID, "unknown")))
}

// RecordEventBusPublishError records an fs_events INSERT failure.
func RecordEventBusPublishError(tenantID string) {
	RegisterModule("sse")
	eventBusPublishErrorsTotal.Add(1, Attr("tenant_id", cleanMetricValue(tenantID, "unknown")))
}

// RecordFSEventsRows records the current fs_events row count for a tenant.
// This compensates for the lack of direct TiDB access: the leader cleanup
// goroutine samples the count and reports it here.
func RecordFSEventsRows(tenantID string, count int64) {
	if count < 0 {
		return
	}
	RegisterModule("sse")
	fsEventsRows.Set(float64(count), Attr("tenant_id", cleanMetricValue(tenantID, "unknown")))
}

// RecordFSEventsPruned records the number of fs_events rows deleted by
// retention cleanup.
func RecordFSEventsPruned(tenantID string, count int64) {
	if count <= 0 {
		return
	}
	RegisterModule("sse")
	fsEventsPrunedTotal.Add(count, Attr("tenant_id", cleanMetricValue(tenantID, "unknown")))
}

func WritePrometheus(w http.ResponseWriter) {
	if w == nil {
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	globalRegistry.WritePrometheus(w)
	writeDBPrometheus(w)
	writeRuntimeMetrics(w)
	writeProcessMetrics(w)
}

func cleanMetricValue(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}

func statusClass(status int) string {
	if status <= 0 {
		return "unknown"
	}
	return strconv.Itoa(status/100) + "xx"
}

func bodySizeBucket(bytes int64) string {
	switch {
	case bytes < 0:
		return "unknown"
	case bytes == 0:
		return "0"
	case bytes <= 1<<10:
		return "le_1KiB"
	case bytes <= 10<<10:
		return "le_10KiB"
	case bytes <= 100<<10:
		return "le_100KiB"
	case bytes <= 1<<20:
		return "le_1MiB"
	case bytes <= 10<<20:
		return "le_10MiB"
	default:
		return "gt_10MiB"
	}
}
