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
var httpDurationBounds = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

var globalRegistry = NewRegistry()
var globalMeterProvider = NewMeterProvider(globalRegistry)

var serviceMeter = globalMeterProvider.Meter("service")
var httpMeter = globalMeterProvider.Meter("http")
var eventMeter = globalMeterProvider.Meter("event")
var fuseMeter = globalMeterProvider.Meter("fuse")

var serviceOperationsTotal = serviceMeter.Int64Counter("dat9_service_operations_total", "Service operations by component/operation/result")
var serviceOperationDuration = serviceMeter.Float64Histogram("dat9_service_operation_duration_seconds", "Service operation duration histogram", operationDurationBounds)
var serviceGauge = serviceMeter.Float64Gauge("dat9_service_gauge", "Service gauges by component/name")

var httpRequestsTotal = httpMeter.Int64Counter("dat9_http_requests_total", "Total HTTP requests by method/route/status")
var httpRequestDuration = httpMeter.Float64Histogram("dat9_http_request_duration_seconds", "HTTP request duration histogram by method/route", httpDurationBounds)
var httpInflight = httpMeter.Float64Gauge("dat9_http_inflight_requests", "Current in-flight HTTP requests")

var tenantEventsTotal = eventMeter.Int64Counter("dat9_tenant_events_total", "Tenant/business lifecycle events")

var fuseOperationsTotal = fuseMeter.Int64Counter("dat9_fuse_operations_total", "FUSE operations by operation/result")
var fuseOperationDuration = fuseMeter.Float64Histogram("dat9_fuse_operation_duration_seconds", "FUSE operation duration histogram", operationDurationBounds)
var fuseOperationBytes = fuseMeter.Int64Counter("dat9_fuse_operation_bytes_total", "Bytes processed by FUSE operation/result")
var fuseRemoteOperationsTotal = fuseMeter.Int64Counter("dat9_fuse_remote_operations_total", "Remote FUSE operations by operation/result")
var fuseRemoteOperationDuration = fuseMeter.Float64Histogram("dat9_fuse_remote_operation_duration_seconds", "Remote FUSE operation duration histogram", operationDurationBounds)
var fuseRemoteOperationBytes = fuseMeter.Int64Counter("dat9_fuse_remote_operation_bytes_total", "Bytes processed by remote FUSE operation/result")

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
	tenantEventsTotal.Add(1, attrs...)
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
}

func cleanMetricValue(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}
