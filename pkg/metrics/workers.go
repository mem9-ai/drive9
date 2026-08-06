package metrics

import (
	"strconv"
	"time"
)

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
var notifyCoalescerPending = serviceMeter.Float64Gauge("drive9_notify_coalescer_pending", "Tenant notify coalescer tenants pending or awaiting durable flush")
var notifyCoalescerBatchSize = serviceMeter.Float64Histogram("drive9_notify_coalescer_batch_size", "Tenants in a notify coalescer flush batch", batchSizeBounds)

var tenantOutboxPollDuration = serviceMeter.Float64Histogram("drive9_tenant_outbox_poll_duration_seconds", "Tenant outbox poll cycle duration by result", operationDurationBounds)
var tenantOutboxBatchSize = serviceMeter.Float64Histogram("drive9_tenant_outbox_batch_size", "Rows returned by a tenant outbox poll query", batchSizeBounds)
var tenantOutboxBacklogOldestAge = serviceMeter.Float64Gauge("drive9_tenant_outbox_backlog_oldest_age_seconds", "Age of the oldest tenant outbox row while a backlog remains")
var tenantOutboxFullBatchesTotal = serviceMeter.Int64Counter("drive9_tenant_outbox_full_batches_total", "Full tenant outbox batches requiring immediate drain")
var tenantOutboxCursorFlushTotal = serviceMeter.Int64Counter("drive9_tenant_outbox_cursor_flush_total", "Tenant outbox cursor flushes by result")

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

func RecordAPIKeyResolveCacheRequest(result string) {
	RegisterModule("api_key_resolve_cache")
	apiKeyResolveCacheRequestsTotal.Add(1, Attr("result", cleanMetricValue(result, "unknown")))
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
	result = cleanMetricValue(result, "unknown")
	tenantOutboxPollDuration.Observe(max(d.Seconds(), 0), Attr("result", result))
	if result != "ok" {
		return
	}
	tenantOutboxBatchSize.Observe(float64(max(size, 0)))
	if size > 0 {
		tenantOutboxBacklogOldestAge.Set(max(oldestAge.Seconds(), 0))
	} else {
		tenantOutboxBacklogOldestAge.Set(0)
	}
	if full {
		tenantOutboxFullBatchesTotal.Add(1)
	}
}

func RecordTenantOutboxCursorFlush(result string) {
	RegisterModule("tenant_outbox_poller")
	tenantOutboxCursorFlushTotal.Add(1, Attr("result", cleanMetricValue(result, "unknown")))
}
