// Package metrics provides process-wide metrics with an OpenTelemetry-like
// meter/instrument/attribute model and a Prometheus text exporter.
package metrics

import (
	"context"
	"database/sql/driver"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var operationDurationBounds = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600}
var httpDurationBounds = []float64{0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10, 15, 20, 30, 60}
var tenantPoolMetadataResumeWaitDurationBounds = []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 240, 480, 600, 900, 1200}

const tenantMetricGuestTiDBCloudOrgID = "guest"

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
var serviceOperationDuration = serviceMeter.Float64Histogram("drive9_service_operation_duration_seconds", "Service operation duration histogram by component/operation/result", operationDurationBounds)
var serviceGauge = serviceMeter.Float64Gauge("drive9_service_gauge", "Service gauges by component/name")
var tenantPoolMetadataResumeWaitTotal = serviceMeter.Int64Counter("drive9_tenant_pool_metadata_resume_wait_total", "Tenant pool metadata resume wait attempts by pool_id/organization_id/scope/result")
var tenantPoolMetadataResumeWaitDuration = serviceMeter.Float64Histogram("drive9_tenant_pool_metadata_resume_wait_duration_seconds", "Tenant pool metadata resume wait duration by pool_id/organization_id/scope/result", tenantPoolMetadataResumeWaitDurationBounds)
var tenantPoolCapacity = serviceMeter.Float64Gauge("drive9_tenant_pool_capacity", "Tenant pool capacity by pool_id/organization_id/state")
var tenantPoolBindings = serviceMeter.Float64Gauge("drive9_tenant_pool_bindings", "Tenant pool binding count by pool_id/tidbcloud_org_id/status")
var sharedDBPoolTotal = serviceMeter.Float64Gauge("drive9_shared_db_pool_total", "Physical shared DB-pool presence by tidbcloud_org_id/db_pool_id/db_pool_uuid/status")
var sharedDBPoolCapacity = serviceMeter.Float64Gauge("drive9_shared_db_pool_capacity", "Physical shared DB-pool capacity by tidbcloud_org_id/db_pool_id/db_pool_uuid/type")
var sharedDBPoolTenants = serviceMeter.Float64Gauge("drive9_shared_db_pool_tenants", "Physical shared DB-pool tenant count by tidbcloud_org_id/db_pool_id/db_pool_uuid/state")
var sharedDBPoolSpendingLimit = serviceMeter.Float64Gauge("drive9_shared_db_pool_spending_limit", "Physical shared DB-pool spending limit by tidbcloud_org_id/db_pool_id/db_pool_uuid/type")
var sharedDBPoolCacheHandles = serviceMeter.Float64Gauge("drive9_shared_db_pool_cache_handles", "Per-pod cached physical shared DB handles by tidbcloud_org_id/db_pool_id/db_pool_uuid")
var sharedDBPoolCacheTenants = serviceMeter.Float64Gauge("drive9_shared_db_pool_cache_tenants", "Per-pod active tenant backend refs on a cached shared DB handle by tidbcloud_org_id/db_pool_id/db_pool_uuid")
var tidbCloudRBACCacheRequestsTotal = serviceMeter.Int64Counter("drive9_tidbcloud_rbac_cache_requests_total", "TiDB Cloud API key to cluster RBAC cache requests by path/scope/result")
var tidbCloudOpenAPIRequestsTotal = serviceMeter.Int64Counter("drive9_tidbcloud_openapi_requests_total", "TiDB Cloud OpenAPI requests by path/operation/result")
var tidbCloudSpendingLimitSyncTotal = serviceMeter.Int64Counter("drive9_tidbcloud_spending_limit_sync_total", "TiDB Cloud spending limit local sync outcomes by source/result")
var tidbCloudSpendingLimitMissingTotal = serviceMeter.Int64Counter("drive9_tidbcloud_spending_limit_missing_total", "TiDB Cloud spending limit missing local config observations by path")

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

var tenantRequestsTotal = tenantMeter.Int64Counter("drive9_tenant_requests_total", "Tenant-scoped requests by tenant/tidbcloud_org/surface/action/result/status_class")
var tenantRequestDuration = tenantMeter.Float64Histogram("drive9_tenant_request_duration_seconds", "Tenant request duration histogram by surface/status_class", httpDurationBounds)
var tenantInflight = tenantMeter.Float64Gauge("drive9_tenant_inflight_requests", "Current in-flight tenant-scoped requests by tenant/tidbcloud_org/surface/action")
var tenantHTTPBytes = tenantMeter.Int64Counter("drive9_tenant_http_bytes_total", "Tenant-scoped HTTP transport bytes by tenant/tidbcloud_org/surface/direction")
var tenantFileBytes = tenantMeter.Int64Counter("drive9_tenant_file_bytes_total", "Tenant-scoped logical file bytes by tenant/tidbcloud_org/surface/action/direction")
var tenantCount = tenantMeter.Float64Gauge("drive9_tenant_count", "Tenant count by status")
var tenantStorageBytes = tenantMeter.Float64Gauge("drive9_tenant_storage_bytes", "Tenant storage bytes by tenant/tidbcloud_org/state")
var tenantMediaFiles = tenantMeter.Float64Gauge("drive9_tenant_media_files", "Tenant media file count by tenant/tidbcloud_org/state")
var tenantVideoFiles = tenantMeter.Float64Gauge("drive9_tenant_video_files", "Tenant video extraction count by tenant/tidbcloud_org/state")

// SSE-specific instruments. SSE connection lifetimes are recorded here, not
// in drive9_http_request_duration_seconds (which would pollute HTTP latency
// alerts — SSE connections live as long as the client stays subscribed).
var sseConnectionsTotal = sseMeter.Int64Counter("drive9_sse_connections_total", "SSE /v1/events connections opened by tenant_id/tidbcloud_org_id/reason")
var sseConnectionDuration = sseMeter.Float64Histogram("drive9_sse_connection_duration_seconds", "SSE connection lifetime by tenant_id/tidbcloud_org_id (client stay-open duration, not request processing time)", sseConnectionDurationBounds)
var sseInflight = sseMeter.Float64Gauge("drive9_sse_inflight_connections", "Active SSE /v1/events connections by tenant_id/tidbcloud_org_id")
var ssePhase1Duration = sseMeter.Float64Histogram("drive9_sse_phase1_duration_seconds", "SSE Phase-1 replay/reset duration by tenant_id/tidbcloud_org_id (one EventsSince call + buffered stream)", ssePhase1DurationBounds)
var sseEventsSentTotal = sseMeter.Int64Counter("drive9_sse_events_sent_total", "SSE events sent to clients by type/tenant_id/tidbcloud_org_id")
var sseResetsSentTotal = sseMeter.Int64Counter("drive9_sse_resets_sent_total", "SSE reset events sent by reason/tenant_id/tidbcloud_org_id")
var sseHeartbeatsSentTotal = sseMeter.Int64Counter("drive9_sse_heartbeats_sent_total", "SSE heartbeat events sent by tenant_id/tidbcloud_org_id")

// Event-bus query instruments. Covers all fs_events DB reads (events_since,
// poll, latest, oldest) so DB pressure and table growth on the events path
// are observable without direct DB access.
var eventBusQueryDuration = serviceMeter.Float64Histogram("drive9_event_bus_query_duration_seconds", "Event-bus fs_events query duration by operation/result", eventBusQueryDurationBounds)
var eventBusPollFailuresTotal = sseMeter.Int64Counter("drive9_event_bus_poll_failures_total", "Event-bus cross-pod poll query failures by tenant_id/tidbcloud_org_id")
var eventBusPublishErrorsTotal = sseMeter.Int64Counter("drive9_event_bus_publish_errors_total", "Event-bus fs_events INSERT failures by tenant_id/tidbcloud_org_id")

// fs_events table instruments. Compensates for the lack of direct TiDB
// access: row count and prune volume are reported by the server itself.
var fsEventsRows = sseMeter.Float64Gauge("drive9_fs_events_rows", "fs_events table row count by tenant_id/tidbcloud_org_id")
var fsEventsPrunedTotal = sseMeter.Int64Counter("drive9_fs_events_pruned_total", "fs_events rows pruned by retention cleanup by tenant_id/tidbcloud_org_id")

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
	RecordTenantOperationWithOrg(tenantID, "", component, operation, result, d)
}

func RecordTenantOperationWithOrg(tenantID, tidbCloudOrgID, component, operation, result string, d time.Duration) {
	component = cleanMetricValue(component, "unknown")
	operation = cleanMetricValue(operation, "unknown")
	result = cleanMetricValue(result, "unknown")
	tenantID = cleanMetricValue(tenantID, "unknown")
	tidbCloudOrgID = cleanTiDBCloudOrgID(tidbCloudOrgID)
	RegisterModule(component)
	baseAttrs := []Attribute{
		Attr("component", component),
		Attr("operation", operation),
		Attr("result", result),
	}
	counterAttrs := baseAttrs
	if tenantID != "unknown" {
		counterAttrs = append(counterAttrs, Attr("tenant_id", tenantID), Attr("tidbcloud_org_id", tidbCloudOrgID))
	}
	serviceOperationsTotal.Add(1, counterAttrs...)
	if d <= 0 {
		return
	}
	serviceOperationDuration.Observe(d.Seconds(), baseAttrs...)
}

func RecordTenantPoolMetadataResumeWait(poolID, organizationID, scope, result string, d time.Duration) {
	poolID = cleanMetricValue(poolID, "unknown")
	organizationID = cleanMetricValue(organizationID, "unknown")
	scope = cleanMetricValue(scope, "unknown")
	result = cleanMetricValue(result, "unknown")
	attrs := []Attribute{
		Attr("pool_id", poolID),
		Attr("organization_id", organizationID),
		Attr("scope", scope),
		Attr("result", result),
	}
	tenantPoolMetadataResumeWaitTotal.Add(1, attrs...)
	tenantPoolMetadataResumeWaitDuration.Observe(d.Seconds(), attrs...)
}

func RecordTenantPoolCapacity(poolID, organizationID, state string, value float64) {
	RegisterModule("admin_tenant_pool")
	tenantPoolCapacity.Set(value,
		Attr("pool_id", cleanMetricValue(poolID, "unknown")),
		Attr("organization_id", cleanMetricValue(organizationID, "unknown")),
		Attr("state", cleanMetricValue(state, "unknown")),
	)
}

func RecordTenantPoolBindings(poolID, tidbCloudOrgID, poolStatus string, count int64) {
	if count < 0 {
		return
	}
	RegisterModule("admin_tenant_pool")
	tenantPoolBindings.Set(float64(count),
		Attr("pool_id", cleanMetricValue(poolID, "unknown")),
		Attr("tidbcloud_org_id", cleanTiDBCloudOrgID(tidbCloudOrgID)),
		Attr("status", cleanMetricValue(poolStatus, "unknown")),
	)
}

func DeleteTenantPoolBindings(poolID, tidbCloudOrgID, poolStatus string) {
	tenantPoolBindings.Delete(
		Attr("pool_id", cleanMetricValue(poolID, "unknown")),
		Attr("tidbcloud_org_id", cleanTiDBCloudOrgID(tidbCloudOrgID)),
		Attr("status", cleanMetricValue(poolStatus, "unknown")),
	)
}

func RecordSharedDBPoolTotal(tidbCloudOrgID string, dbPoolID int64, dbPoolUUID, status string, count int64) {
	if count < 0 {
		return
	}
	RegisterModule("shared_db_pool")
	sharedDBPoolTotal.Set(float64(count), sharedDBPoolAttrs(tidbCloudOrgID, dbPoolID, dbPoolUUID, "status", status)...)
}

func DeleteSharedDBPoolTotal(tidbCloudOrgID string, dbPoolID int64, dbPoolUUID, status string) {
	sharedDBPoolTotal.Delete(sharedDBPoolAttrs(tidbCloudOrgID, dbPoolID, dbPoolUUID, "status", status)...)
}

func RecordSharedDBPoolCapacity(tidbCloudOrgID string, dbPoolID int64, dbPoolUUID, capacityType string, value int64) {
	RegisterModule("shared_db_pool")
	sharedDBPoolCapacity.Set(float64(value), sharedDBPoolAttrs(tidbCloudOrgID, dbPoolID, dbPoolUUID, "type", capacityType)...)
}

func DeleteSharedDBPoolCapacity(tidbCloudOrgID string, dbPoolID int64, dbPoolUUID, capacityType string) {
	sharedDBPoolCapacity.Delete(sharedDBPoolAttrs(tidbCloudOrgID, dbPoolID, dbPoolUUID, "type", capacityType)...)
}

func RecordSharedDBPoolTenants(tidbCloudOrgID string, dbPoolID int64, dbPoolUUID, state string, count int64) {
	if count < 0 {
		return
	}
	RegisterModule("shared_db_pool")
	sharedDBPoolTenants.Set(float64(count), sharedDBPoolAttrs(tidbCloudOrgID, dbPoolID, dbPoolUUID, "state", state)...)
}

func DeleteSharedDBPoolTenants(tidbCloudOrgID string, dbPoolID int64, dbPoolUUID, state string) {
	sharedDBPoolTenants.Delete(sharedDBPoolAttrs(tidbCloudOrgID, dbPoolID, dbPoolUUID, "state", state)...)
}

func RecordSharedDBPoolSpendingLimit(tidbCloudOrgID string, dbPoolID int64, dbPoolUUID, limitType string, value int64) {
	RegisterModule("shared_db_pool")
	sharedDBPoolSpendingLimit.Set(float64(value), sharedDBPoolAttrs(tidbCloudOrgID, dbPoolID, dbPoolUUID, "type", limitType)...)
}

func DeleteSharedDBPoolSpendingLimit(tidbCloudOrgID string, dbPoolID int64, dbPoolUUID, limitType string) {
	sharedDBPoolSpendingLimit.Delete(sharedDBPoolAttrs(tidbCloudOrgID, dbPoolID, dbPoolUUID, "type", limitType)...)
}

func sharedDBPoolAttrs(tidbCloudOrgID string, dbPoolID int64, dbPoolUUID, dimension, value string) []Attribute {
	return []Attribute{
		Attr("tidbcloud_org_id", cleanMetricValue(tidbCloudOrgID, "unknown")),
		Attr("db_pool_id", strconv.FormatInt(dbPoolID, 10)),
		Attr("db_pool_uuid", cleanMetricValue(dbPoolUUID, "unknown")),
		Attr(dimension, cleanMetricValue(value, "unknown")),
	}
}

func RecordSharedDBPoolCacheHandles(tidbCloudOrgID string, dbPoolID int64, dbPoolUUID string, count int64) {
	RegisterModule("shared_db_pool")
	sharedDBPoolCacheHandles.Set(float64(count), sharedDBPoolCacheAttrs(tidbCloudOrgID, dbPoolID, dbPoolUUID)...)
}

func DeleteSharedDBPoolCacheHandles(tidbCloudOrgID string, dbPoolID int64, dbPoolUUID string) {
	sharedDBPoolCacheHandles.Delete(sharedDBPoolCacheAttrs(tidbCloudOrgID, dbPoolID, dbPoolUUID)...)
}

func RecordSharedDBPoolCacheTenants(tidbCloudOrgID string, dbPoolID int64, dbPoolUUID string, count int64) {
	if count < 0 {
		return
	}
	RegisterModule("shared_db_pool")
	sharedDBPoolCacheTenants.Set(float64(count), sharedDBPoolCacheAttrs(tidbCloudOrgID, dbPoolID, dbPoolUUID)...)
}

func DeleteSharedDBPoolCacheTenants(tidbCloudOrgID string, dbPoolID int64, dbPoolUUID string) {
	sharedDBPoolCacheTenants.Delete(sharedDBPoolCacheAttrs(tidbCloudOrgID, dbPoolID, dbPoolUUID)...)
}

func sharedDBPoolCacheAttrs(tidbCloudOrgID string, dbPoolID int64, dbPoolUUID string) []Attribute {
	return []Attribute{
		Attr("tidbcloud_org_id", cleanMetricValue(tidbCloudOrgID, "unknown")),
		Attr("db_pool_id", strconv.FormatInt(dbPoolID, 10)),
		Attr("db_pool_uuid", cleanMetricValue(dbPoolUUID, "unknown")),
	}
}

func DeleteTenantCounters(tenantID string) {
	tenantID = cleanMetricValue(tenantID, "unknown")
	if tenantID == "unknown" {
		return
	}
	globalRegistry.DeleteCountersByLabel("tenant_id", tenantID)
	globalRegistry.DeleteHistogramsByLabel("tenant_id", tenantID)
	globalRegistry.DeleteGaugesByLabel("tenant_id", tenantID)
}

func RecordTiDBCloudRBACCacheRequest(path, scope, result string) {
	RegisterModule("tidbcloud_quota")
	tidbCloudRBACCacheRequestsTotal.Add(1,
		Attr("path", cleanMetricValue(path, "unknown")),
		Attr("scope", cleanMetricValue(scope, "unknown")),
		Attr("result", cleanMetricValue(result, "unknown")),
	)
}

func RecordTiDBCloudOpenAPIRequest(path, operation, result string) {
	RegisterModule("tidbcloud_quota")
	tidbCloudOpenAPIRequestsTotal.Add(1,
		Attr("path", cleanMetricValue(path, "unknown")),
		Attr("operation", cleanMetricValue(operation, "unknown")),
		Attr("result", cleanMetricValue(result, "unknown")),
	)
}

func RecordTiDBCloudSpendingLimitSync(source, result string) {
	RegisterModule("tidbcloud_quota")
	tidbCloudSpendingLimitSyncTotal.Add(1,
		Attr("source", cleanMetricValue(source, "unknown")),
		Attr("result", cleanMetricValue(result, "unknown")),
	)
}

func RecordTiDBCloudSpendingLimitMissing(path string) {
	RegisterModule("tidbcloud_quota")
	tidbCloudSpendingLimitMissingTotal.Add(1,
		Attr("path", cleanMetricValue(path, "unknown")),
	)
}

// ResultForError returns a stable metric result label for common infrastructure
// errors. Callers should use this when recording generic worker/DB failures so
// transient bad connections do not get bucketed with semantic operation errors.
// Keep mysqlutil.dbResult delegated here so DB and worker labels stay aligned.
func ResultForError(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, driver.ErrBadConn):
		return "bad_conn"
	case strings.Contains(strings.ToLower(err.Error()), "invalid connection"):
		return "bad_conn"
	default:
		return "error"
	}
}

// RecordTenantOperationCount records an operation outcome without adding a
// duration sample. Use it for per-entry counters when one wall-clock duration
// would be duplicated across many logical items in the same batch.
func RecordTenantOperationCount(tenantID, component, operation, result string) {
	RecordTenantOperationCountWithOrg(tenantID, "", component, operation, result)
}

func RecordTenantOperationCountWithOrg(tenantID, tidbCloudOrgID, component, operation, result string) {
	component = cleanMetricValue(component, "unknown")
	operation = cleanMetricValue(operation, "unknown")
	result = cleanMetricValue(result, "unknown")
	tenantID = cleanMetricValue(tenantID, "unknown")
	tidbCloudOrgID = cleanTiDBCloudOrgID(tidbCloudOrgID)
	RegisterModule(component)
	attrs := []Attribute{
		Attr("component", component),
		Attr("operation", operation),
		Attr("result", result),
	}
	if tenantID != "unknown" {
		attrs = append(attrs, Attr("tenant_id", tenantID), Attr("tidbcloud_org_id", tidbCloudOrgID))
	}
	serviceOperationsTotal.Add(1, attrs...)
}

func RecordGauge(component, name string, value float64) {
	RecordTenantGauge("", component, name, value)
}

func RecordTenantGauge(tenantID, component, name string, value float64) {
	RecordTenantGaugeWithOrg(tenantID, "", component, name, value)
}

func RecordTenantGaugeWithOrg(tenantID, tidbCloudOrgID, component, name string, value float64) {
	component = cleanMetricValue(component, "unknown")
	name = cleanMetricValue(name, "unknown")
	tenantID = strings.TrimSpace(tenantID)
	tidbCloudOrgID = cleanTiDBCloudOrgID(tidbCloudOrgID)
	RegisterModule(component)
	serviceGauge.Set(value,
		Attr("component", component),
		Attr("name", name),
		Attr("tenant_id", tenantID),
		Attr("tidbcloud_org_id", tidbCloudOrgID),
	)
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
	RecordTenantRequestCountWithOrg(tenantID, "", surface, action, result, status)
}

func RecordTenantRequestCountWithOrg(tenantID, tidbCloudOrgID, surface, action, result string, status int) {
	tenantID = cleanMetricValue(tenantID, "unknown")
	tidbCloudOrgID = cleanTiDBCloudOrgID(tidbCloudOrgID)
	surface = cleanMetricValue(surface, "other")
	action = cleanMetricValue(action, "other")
	result = cleanMetricValue(result, "unknown")
	RegisterModule("tenant_usage")
	attrs := []Attribute{
		Attr("tenant_id", tenantID),
		Attr("tidbcloud_org_id", tidbCloudOrgID),
		Attr("surface", surface),
		Attr("action", action),
		Attr("result", result),
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
	RecordTenantEventWithOrg(tenantID, "", event, labels...)
}

func RecordTenantEventWithOrg(tenantID, tidbCloudOrgID, event string, labels ...string) {
	RegisterModule("server")
	attrs := make([]Attribute, 0, len(labels)/2+2)
	attrs = append(attrs, Attr("event", cleanMetricValue(event, "unknown")))
	tenantID = cleanMetricValue(tenantID, "unknown")
	tidbCloudOrgID = cleanTiDBCloudOrgID(tidbCloudOrgID)
	if tenantID != "unknown" {
		attrs = append(attrs, Attr("tenant_id", tenantID), Attr("tidbcloud_org_id", tidbCloudOrgID))
	}
	for i := 0; i+1 < len(labels); i += 2 {
		attrs = append(attrs, Attr(labels[i], labels[i+1]))
	}
	businessEventsTotal.Add(1, attrs...)
}

func RecordTenantRequest(tenantID, surface, action, result string, status int, d time.Duration) {
	RecordTenantRequestWithOrg(tenantID, "", surface, action, result, status, d)
}

func RecordTenantRequestWithOrg(tenantID, tidbCloudOrgID, surface, action, result string, status int, d time.Duration) {
	tenantID = cleanMetricValue(tenantID, "unknown")
	tidbCloudOrgID = cleanTiDBCloudOrgID(tidbCloudOrgID)
	surface = cleanMetricValue(surface, "other")
	action = cleanMetricValue(action, "other")
	result = cleanMetricValue(result, "unknown")
	statusClass := "unknown"
	if status > 0 {
		statusClass = strconv.Itoa(status/100) + "xx"
	}
	RegisterModule("tenant_usage")
	attrs := []Attribute{
		Attr("tenant_id", tenantID),
		Attr("tidbcloud_org_id", tidbCloudOrgID),
		Attr("surface", surface),
		Attr("action", action),
		Attr("result", result),
		Attr("status_class", statusClass),
	}
	tenantRequestsTotal.Add(1, attrs...)
	if d <= 0 {
		return
	}
	tenantRequestDuration.Observe(d.Seconds(),
		Attr("surface", surface),
		Attr("status_class", statusClass),
	)
}

func RecordTenantHTTPBytes(tenantID, surface, _, direction string, bytes int64) {
	RecordTenantHTTPBytesWithOrg(tenantID, "", surface, "", direction, bytes)
}

func RecordTenantHTTPBytesWithOrg(tenantID, tidbCloudOrgID, surface, _, direction string, bytes int64) {
	recordTenantHTTPBytes(tenantID, tidbCloudOrgID, surface, direction, bytes)
}

func RecordTenantFileBytes(tenantID, surface, action, direction string, bytes int64) {
	RecordTenantFileBytesWithOrg(tenantID, "", surface, action, direction, bytes)
}

func RecordTenantFileBytesWithOrg(tenantID, tidbCloudOrgID, surface, action, direction string, bytes int64) {
	recordTenantBytes(tenantFileBytes, tenantID, tidbCloudOrgID, surface, action, direction, bytes)
}

func RecordTenantInFlight(tenantID, surface, action string, value float64) {
	RecordTenantInFlightWithOrg(tenantID, "", surface, action, value)
}

func RecordTenantInFlightWithOrg(tenantID, tidbCloudOrgID, surface, action string, value float64) {
	RegisterModule("tenant_usage")
	attrs := []Attribute{
		Attr("tenant_id", cleanMetricValue(tenantID, "unknown")),
		Attr("tidbcloud_org_id", cleanTiDBCloudOrgID(tidbCloudOrgID)),
		Attr("surface", cleanMetricValue(surface, "other")),
		Attr("action", cleanMetricValue(action, "other")),
	}
	if value <= 0 {
		tenantInflight.Delete(attrs...)
		return
	}
	tenantInflight.Set(value, attrs...)
}

func RecordTenantCount(status string, count int64) {
	if count < 0 {
		return
	}
	RegisterModule("tenant_usage")
	tenantCount.Set(float64(count),
		Attr("status", cleanMetricValue(status, "unknown")),
	)
}

func RecordTenantStorageBytes(tenantID, state string, bytes int64) {
	RecordTenantStorageBytesWithOrg(tenantID, "", state, bytes)
}

func RecordTenantStorageBytesWithOrg(tenantID, tidbCloudOrgID, state string, bytes int64) {
	if bytes < 0 {
		return
	}
	RegisterModule("tenant_usage")
	tenantStorageBytes.Set(float64(bytes),
		Attr("tenant_id", cleanMetricValue(tenantID, "unknown")),
		Attr("tidbcloud_org_id", cleanTiDBCloudOrgID(tidbCloudOrgID)),
		Attr("state", cleanMetricValue(state, "unknown")),
	)
}

func RecordTenantMediaFiles(tenantID, state string, count int64) {
	RecordTenantMediaFilesWithOrg(tenantID, "", state, count)
}

func RecordTenantMediaFilesWithOrg(tenantID, tidbCloudOrgID, state string, count int64) {
	if count < 0 {
		return
	}
	RegisterModule("tenant_usage")
	tenantMediaFiles.Set(float64(count),
		Attr("tenant_id", cleanMetricValue(tenantID, "unknown")),
		Attr("tidbcloud_org_id", cleanTiDBCloudOrgID(tidbCloudOrgID)),
		Attr("state", cleanMetricValue(state, "unknown")),
	)
}

func RecordTenantVideoFiles(tenantID, state string, count int64) {
	RecordTenantVideoFilesWithOrg(tenantID, "", state, count)
}

func RecordTenantVideoFilesWithOrg(tenantID, tidbCloudOrgID, state string, count int64) {
	if count < 0 {
		return
	}
	RegisterModule("tenant_usage")
	tenantVideoFiles.Set(float64(count),
		Attr("tenant_id", cleanMetricValue(tenantID, "unknown")),
		Attr("tidbcloud_org_id", cleanTiDBCloudOrgID(tidbCloudOrgID)),
		Attr("state", cleanMetricValue(state, "unknown")),
	)
}

func recordTenantBytes(counter *Int64Counter, tenantID, tidbCloudOrgID, surface, action, direction string, bytes int64) {
	if bytes <= 0 {
		return
	}
	RegisterModule("tenant_usage")
	counter.Add(bytes,
		Attr("tenant_id", cleanMetricValue(tenantID, "unknown")),
		Attr("tidbcloud_org_id", cleanTiDBCloudOrgID(tidbCloudOrgID)),
		Attr("surface", cleanMetricValue(surface, "other")),
		Attr("action", cleanMetricValue(action, "other")),
		Attr("direction", cleanMetricValue(direction, "unknown")),
	)
}

func recordTenantHTTPBytes(tenantID, tidbCloudOrgID, surface, direction string, bytes int64) {
	if bytes <= 0 {
		return
	}
	RegisterModule("tenant_usage")
	tenantHTTPBytes.Add(bytes,
		Attr("tenant_id", cleanMetricValue(tenantID, "unknown")),
		Attr("tidbcloud_org_id", cleanTiDBCloudOrgID(tidbCloudOrgID)),
		Attr("surface", cleanMetricValue(surface, "other")),
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
	RecordSSEConnectionWithOrg(tenantID, "", reason, d)
}

func RecordSSEConnectionWithOrg(tenantID, tidbCloudOrgID, reason string, d time.Duration) {
	RegisterModule("sse")
	tenantID = cleanMetricValue(tenantID, "unknown")
	tidbCloudOrgID = cleanTiDBCloudOrgID(tidbCloudOrgID)
	sseConnectionsTotal.Add(1,
		Attr("tenant_id", tenantID),
		Attr("tidbcloud_org_id", tidbCloudOrgID),
		Attr("reason", cleanMetricValue(reason, "unknown")),
	)
	if d <= 0 {
		return
	}
	sseConnectionDuration.Observe(d.Seconds(),
		Attr("tenant_id", tenantID),
		Attr("tidbcloud_org_id", tidbCloudOrgID),
	)
}

// RecordSSEInFlight sets the active SSE connection count for a tenant. The
// caller maintains the absolute count (incremented on subscribe, decremented
// on unsubscribe); this records the current value so the gauge reflects the
// true number of live SSE connections per tenant.
func RecordSSEInFlight(tenantID string, count float64) {
	RecordSSEInFlightWithOrg(tenantID, "", count)
}

func RecordSSEInFlightWithOrg(tenantID, tidbCloudOrgID string, count float64) {
	if count < 0 {
		count = 0
	}
	RegisterModule("sse")
	sseInflight.Set(count,
		Attr("tenant_id", cleanMetricValue(tenantID, "unknown")),
		Attr("tidbcloud_org_id", cleanTiDBCloudOrgID(tidbCloudOrgID)),
	)
}

// RecordSSEPhase1 records the duration of the SSE Phase-1 replay/reset stage.
func RecordSSEPhase1(tenantID string, d time.Duration) {
	RecordSSEPhase1WithOrg(tenantID, "", d)
}

func RecordSSEPhase1WithOrg(tenantID, tidbCloudOrgID string, d time.Duration) {
	if d <= 0 {
		return
	}
	RegisterModule("sse")
	ssePhase1Duration.Observe(d.Seconds(),
		Attr("tenant_id", cleanMetricValue(tenantID, "unknown")),
		Attr("tidbcloud_org_id", cleanTiDBCloudOrgID(tidbCloudOrgID)),
	)
}

// RecordSSEEventSent records a single SSE file_changed event delivery.
func RecordSSEEventSent(tenantID, op string) {
	RecordSSEEventSentWithOrg(tenantID, "", op)
}

func RecordSSEEventSentWithOrg(tenantID, tidbCloudOrgID, op string) {
	RegisterModule("sse")
	sseEventsSentTotal.Add(1,
		Attr("tenant_id", cleanMetricValue(tenantID, "unknown")),
		Attr("tidbcloud_org_id", cleanTiDBCloudOrgID(tidbCloudOrgID)),
		Attr("op", cleanMetricValue(op, "unknown")),
	)
}

// RecordSSEResetSent records an SSE reset event delivery.
func RecordSSEResetSent(tenantID, reason string) {
	RecordSSEResetSentWithOrg(tenantID, "", reason)
}

func RecordSSEResetSentWithOrg(tenantID, tidbCloudOrgID, reason string) {
	RegisterModule("sse")
	sseResetsSentTotal.Add(1,
		Attr("tenant_id", cleanMetricValue(tenantID, "unknown")),
		Attr("tidbcloud_org_id", cleanTiDBCloudOrgID(tidbCloudOrgID)),
		Attr("reason", cleanMetricValue(reason, "unknown")),
	)
}

// RecordSSEHeartbeatSent records an SSE heartbeat delivery.
func RecordSSEHeartbeatSent(tenantID string) {
	RecordSSEHeartbeatSentWithOrg(tenantID, "")
}

func RecordSSEHeartbeatSentWithOrg(tenantID, tidbCloudOrgID string) {
	RegisterModule("sse")
	sseHeartbeatsSentTotal.Add(1,
		Attr("tenant_id", cleanMetricValue(tenantID, "unknown")),
		Attr("tidbcloud_org_id", cleanTiDBCloudOrgID(tidbCloudOrgID)),
	)
}

// RecordEventBusQuery records the duration of an fs_events DB query. Unlike
// the error-only counters, this records every query (ok and error) so DB
// pressure and table growth on the events path are observable.
func RecordEventBusQuery(tenantID, operation, result string, d time.Duration) {
	if d <= 0 {
		return
	}
	RegisterModule("sse")
	eventBusQueryDuration.Observe(d.Seconds(),
		Attr("operation", cleanMetricValue(operation, "unknown")),
		Attr("result", cleanMetricValue(result, "unknown")),
	)
}

// RecordEventBusPollFailure records a cross-pod poll query failure (previously
// only logged, not metriced).
func RecordEventBusPollFailure(tenantID string) {
	RecordEventBusPollFailureWithOrg(tenantID, "")
}

func RecordEventBusPollFailureWithOrg(tenantID, tidbCloudOrgID string) {
	RegisterModule("sse")
	eventBusPollFailuresTotal.Add(1,
		Attr("tenant_id", cleanMetricValue(tenantID, "unknown")),
		Attr("tidbcloud_org_id", cleanTiDBCloudOrgID(tidbCloudOrgID)),
	)
}

// RecordEventBusPublishError records an fs_events INSERT failure.
func RecordEventBusPublishError(tenantID string) {
	RecordEventBusPublishErrorWithOrg(tenantID, "")
}

func RecordEventBusPublishErrorWithOrg(tenantID, tidbCloudOrgID string) {
	RegisterModule("sse")
	eventBusPublishErrorsTotal.Add(1,
		Attr("tenant_id", cleanMetricValue(tenantID, "unknown")),
		Attr("tidbcloud_org_id", cleanTiDBCloudOrgID(tidbCloudOrgID)),
	)
}

// RecordFSEventsRows records the current fs_events row count for a tenant.
// This compensates for the lack of direct TiDB access: the leader cleanup
// goroutine samples the count and reports it here.
func RecordFSEventsRows(tenantID string, count int64) {
	RecordFSEventsRowsWithOrg(tenantID, "", count)
}

func RecordFSEventsRowsWithOrg(tenantID, tidbCloudOrgID string, count int64) {
	if count < 0 {
		return
	}
	RegisterModule("sse")
	fsEventsRows.Set(float64(count),
		Attr("tenant_id", cleanMetricValue(tenantID, "unknown")),
		Attr("tidbcloud_org_id", cleanTiDBCloudOrgID(tidbCloudOrgID)),
	)
}

// RecordFSEventsPruned records the number of fs_events rows deleted by
// retention cleanup.
func RecordFSEventsPruned(tenantID string, count int64) {
	RecordFSEventsPrunedWithOrg(tenantID, "", count)
}

func RecordFSEventsPrunedWithOrg(tenantID, tidbCloudOrgID string, count int64) {
	if count <= 0 {
		return
	}
	RegisterModule("sse")
	fsEventsPrunedTotal.Add(count,
		Attr("tenant_id", cleanMetricValue(tenantID, "unknown")),
		Attr("tidbcloud_org_id", cleanTiDBCloudOrgID(tidbCloudOrgID)),
	)
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

func cleanTiDBCloudOrgID(orgID string) string {
	return cleanMetricValue(orgID, tenantMetricGuestTiDBCloudOrgID)
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
