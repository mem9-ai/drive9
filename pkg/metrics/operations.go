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

var globalRegistry = NewRegistry()
var globalMeterProvider = NewMeterProvider(globalRegistry)

var serviceMeter = globalMeterProvider.Meter("service")
var httpMeter = globalMeterProvider.Meter("http")
var eventMeter = globalMeterProvider.Meter("event")
var fuseMeter = globalMeterProvider.Meter("fuse")
var tenantMeter = globalMeterProvider.Meter("tenant")

var serviceOperationsTotal = serviceMeter.Int64Counter("drive9_service_operations_total", "Service operations by component/operation/result")
var serviceOperationDuration = serviceMeter.Float64Histogram("drive9_service_operation_duration_seconds", "Service operation duration histogram", operationDurationBounds)
var serviceGauge = serviceMeter.Float64Gauge("drive9_service_gauge", "Service gauges by component/name")

var httpRequestsTotal = httpMeter.Int64Counter("drive9_http_requests_total", "Total HTTP requests by method/route/status")
var httpRequestDuration = httpMeter.Float64Histogram("drive9_http_request_duration_seconds", "HTTP request duration histogram by method/route", httpDurationBounds)
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

func RegisterModule(module string) {
	globalRegistry.RegisterModule(module)
}

func SetModuleAvailability(module string, up bool) {
	globalRegistry.SetModuleAvailability(module, up)
}

func RecordOperation(component, operation, result string, d time.Duration) {
	component = cleanMetricValue(component, "unknown")
	operation = cleanMetricValue(operation, "unknown")
	result = cleanMetricValue(result, "unknown")
	RegisterModule(component)
	attrs := []Attribute{
		Attr("component", component),
		Attr("operation", operation),
		Attr("result", result),
	}
	serviceOperationsTotal.Add(1, attrs...)
	serviceOperationDuration.Observe(d.Seconds(), attrs...)
}

func RecordGauge(component, name string, value float64) {
	component = cleanMetricValue(component, "unknown")
	name = cleanMetricValue(name, "unknown")
	RegisterModule(component)
	serviceGauge.Set(value, Attr("component", component), Attr("name", name))
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

func RecordHTTPInFlight(value float64) {
	RegisterModule("server")
	httpInflight.Set(value)
}

func RecordHTTPInFlightRoute(route string, value float64) {
	RegisterModule("server")
	httpInflight.Set(value, Attr("route", cleanMetricValue(route, "other")))
}

func RecordEvent(event string, labels ...string) {
	RegisterModule("server")
	attrs := make([]Attribute, 0, len(labels)/2+1)
	attrs = append(attrs, Attr("event", cleanMetricValue(event, "unknown")))
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
