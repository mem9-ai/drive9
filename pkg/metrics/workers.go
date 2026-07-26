package metrics

import (
	"strconv"
	"time"
)

var queueDepthBounds = []float64{1, 4, 16, 64, 128, 256, 512, 1024, 2048, 3072, 4096}
var batchSizeBounds = []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1000}

var mutationDispatcherQueueDepth = serviceMeter.Float64Gauge("drive9_mutation_dispatcher_queue_depth", "Queued central quota mutations by dispatcher shard")
var mutationDispatcherQueueCapacity = serviceMeter.Float64Gauge("drive9_mutation_dispatcher_queue_capacity", "Central quota mutation queue capacity by dispatcher shard")
var mutationDispatcherEnqueueBlocked = serviceMeter.Float64Histogram("drive9_mutation_dispatcher_enqueue_blocked_seconds", "Time spent waiting to enqueue a central quota mutation by dispatcher shard", operationDurationBounds)
var mutationDispatcherBatchSize = serviceMeter.Float64Histogram("drive9_mutation_dispatcher_batch_size", "Central quota mutations collected per dispatcher batch", batchSizeBounds)
var mutationDispatcherBatchFallbackTotal = serviceMeter.Int64Counter("drive9_mutation_dispatcher_batch_fallback_total", "Central quota dispatcher batches that fell back to per-item transactions")

var apiKeyResolveCacheRequestsTotal = serviceMeter.Int64Counter("drive9_api_key_resolve_cache_requests_total", "API key resolve cache requests by result")
var apiKeyResolveCacheEntries = serviceMeter.Float64Gauge("drive9_api_key_resolve_cache_entries", "Current API key resolve cache entries")

var notifyCoalescerFlushTotal = serviceMeter.Int64Counter("drive9_notify_coalescer_flush_total", "Tenant notify coalescer flushes by final result")
var notifyCoalescerPerRowFallbackTotal = serviceMeter.Int64Counter("drive9_notify_coalescer_per_row_fallback_total", "Tenant notify coalescer per-row fallback inserts by result")
var notifyCoalescerPending = serviceMeter.Float64Gauge("drive9_notify_coalescer_pending", "Tenant notify coalescer tenants awaiting flush")
var notifyCoalescerBatchSize = serviceMeter.Float64Histogram("drive9_notify_coalescer_batch_size", "Tenants in a notify coalescer flush batch", batchSizeBounds)

var tenantOutboxPollDuration = serviceMeter.Float64Histogram("drive9_tenant_outbox_poll_duration_seconds", "Tenant outbox poll cycle duration by result", operationDurationBounds)
var tenantOutboxBatchSize = serviceMeter.Float64Histogram("drive9_tenant_outbox_batch_size", "Rows returned by a tenant outbox poll query", batchSizeBounds)
var tenantOutboxBacklogOldestAge = serviceMeter.Float64Gauge("drive9_tenant_outbox_backlog_oldest_age_seconds", "Age of the oldest tenant outbox row while a backlog remains")
var tenantOutboxFullBatchesTotal = serviceMeter.Int64Counter("drive9_tenant_outbox_full_batches_total", "Full tenant outbox batches requiring immediate drain")
var tenantOutboxCursorFlushTotal = serviceMeter.Int64Counter("drive9_tenant_outbox_cursor_flush_total", "Tenant outbox cursor flushes by result")

var sharedDBPoolStatusAge = serviceMeter.Float64Gauge("drive9_shared_db_pool_status_age_seconds", "Time a non-active shared DB pool has remained in its current status")
var sharedDBPoolStuckMarkedFailedTotal = serviceMeter.Int64Counter("drive9_shared_db_pool_stuck_marked_failed_total", "Stuck shared DB pools marked failed by previous status")
var sharedDBPoolWaveTotal = serviceMeter.Int64Counter("drive9_shared_db_pool_wave_total", "Managed shared DB pool reservation waves by result")
var sharedDBPoolWavePhysicalPools = serviceMeter.Float64Histogram("drive9_shared_db_pool_wave_physical_pools", "Physical DB pools staged per managed shared-pool wave", batchSizeBounds)
var sharedDBPoolWaveTenants = serviceMeter.Float64Histogram("drive9_shared_db_pool_wave_tenants", "Tenant slots staged per managed shared-pool wave", batchSizeBounds)
var sharedDBPoolCleanupTotal = serviceMeter.Int64Counter("drive9_shared_db_pool_cleanup_total", "Failed managed shared DB pool cleanup steps by stage/result")

func RecordMutationDispatcherQueue(shard, depth, capacity int) {
	RegisterModule("mutation_dispatcher")
	attrs := []Attribute{Attr("shard", strconv.Itoa(shard))}
	mutationDispatcherQueueDepth.Set(float64(max(depth, 0)), attrs...)
	mutationDispatcherQueueCapacity.Set(float64(max(capacity, 0)), attrs...)
}

func RecordMutationDispatcherEnqueueBlocked(shard int, d time.Duration) {
	RegisterModule("mutation_dispatcher")
	mutationDispatcherEnqueueBlocked.Observe(max(d.Seconds(), 0), Attr("shard", strconv.Itoa(shard)))
}

func RecordMutationDispatcherBatch(size int, fallback bool) {
	RegisterModule("mutation_dispatcher")
	if size > 0 {
		mutationDispatcherBatchSize.Observe(float64(size))
	}
	if fallback {
		mutationDispatcherBatchFallbackTotal.Add(1)
	}
}

func RecordAPIKeyResolveCache(result string, entries int) {
	RegisterModule("api_key_resolve_cache")
	apiKeyResolveCacheRequestsTotal.Add(1, Attr("result", cleanMetricValue(result, "unknown")))
	apiKeyResolveCacheEntries.Set(float64(max(entries, 0)))
}

func RecordAPIKeyResolveCacheEntries(entries int) {
	RegisterModule("api_key_resolve_cache")
	apiKeyResolveCacheEntries.Set(float64(max(entries, 0)))
}

func RecordNotifyCoalescerPending(pending int) {
	RegisterModule("notify_coalescer")
	notifyCoalescerPending.Set(float64(max(pending, 0)))
}

func RecordNotifyCoalescerFlush(result string, size int) {
	RegisterModule("notify_coalescer")
	notifyCoalescerFlushTotal.Add(1, Attr("result", cleanMetricValue(result, "unknown")))
	if size > 0 {
		notifyCoalescerBatchSize.Observe(float64(size))
	}
}

func RecordNotifyCoalescerPerRowFallback(result string) {
	RegisterModule("notify_coalescer")
	notifyCoalescerPerRowFallbackTotal.Add(1, Attr("result", cleanMetricValue(result, "unknown")))
}

func RecordTenantOutboxPoll(result string, d time.Duration, size int, oldestAge time.Duration, full bool) {
	RegisterModule("tenant_outbox_poller")
	tenantOutboxPollDuration.Observe(max(d.Seconds(), 0), Attr("result", cleanMetricValue(result, "unknown")))
	tenantOutboxBatchSize.Observe(float64(max(size, 0)))
	if full {
		tenantOutboxBacklogOldestAge.Set(max(oldestAge.Seconds(), 0))
		tenantOutboxFullBatchesTotal.Add(1)
	} else {
		tenantOutboxBacklogOldestAge.Set(0)
	}
}

func RecordTenantOutboxCursorFlush(result string) {
	RegisterModule("tenant_outbox_poller")
	tenantOutboxCursorFlushTotal.Add(1, Attr("result", cleanMetricValue(result, "unknown")))
}

func RecordSharedDBPoolStatusAge(tidbCloudOrgID, dbPoolUUID, status string, age time.Duration) {
	RegisterModule("shared_db_pool")
	sharedDBPoolStatusAge.Set(max(age.Seconds(), 0), sharedDBPoolStatusAgeAttrs(tidbCloudOrgID, dbPoolUUID, status)...)
}

func DeleteSharedDBPoolStatusAge(tidbCloudOrgID, dbPoolUUID, status string) {
	sharedDBPoolStatusAge.Delete(sharedDBPoolStatusAgeAttrs(tidbCloudOrgID, dbPoolUUID, status)...)
}

func sharedDBPoolStatusAgeAttrs(tidbCloudOrgID, dbPoolUUID, status string) []Attribute {
	return []Attribute{
		Attr("tidbcloud_org_id", cleanTiDBCloudOrgID(tidbCloudOrgID)),
		Attr("db_pool_uuid", cleanMetricValue(dbPoolUUID, "unknown")),
		Attr("status", cleanMetricValue(status, "unknown")),
	}
}

func RecordSharedDBPoolStuckMarkedFailed(previousStatus string) {
	RegisterModule("shared_db_pool")
	sharedDBPoolStuckMarkedFailedTotal.Add(1, Attr("previous_status", cleanMetricValue(previousStatus, "unknown")))
}

func RecordSharedDBPoolWave(result string, physicalPools, tenants int) {
	RegisterModule("shared_db_pool")
	sharedDBPoolWaveTotal.Add(1, Attr("result", cleanMetricValue(result, "unknown")))
	if physicalPools > 0 {
		sharedDBPoolWavePhysicalPools.Observe(float64(physicalPools))
	}
	if tenants > 0 {
		sharedDBPoolWaveTenants.Observe(float64(tenants))
	}
}

func RecordSharedDBPoolCleanup(stage, result string) {
	RegisterModule("shared_db_pool")
	sharedDBPoolCleanupTotal.Add(1,
		Attr("stage", cleanMetricValue(stage, "unknown")),
		Attr("result", cleanMetricValue(result, "unknown")),
	)
}
