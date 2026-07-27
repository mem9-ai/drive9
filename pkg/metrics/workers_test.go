package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCriticalWorkerMetricsUseBoundedLabels(t *testing.T) {
	RecordMutationDispatcherQueue(2, 10, 4096)
	RecordMutationDispatcherEnqueueBlocked(2, 25*time.Millisecond)
	RecordMutationDispatcherBatch(17, true)
	RecordAPIKeyResolveCacheEntries(8)
	RecordAPIKeyResolveCacheRequest("hit")
	RecordNotifyCoalescerPending(3)
	RecordNotifyCoalescerFlush("retry_ok", 3)
	RecordNotifyCoalescerPerRowFallback("error")
	RecordTenantOutboxPoll("ok", 50*time.Millisecond, 1000, 12*time.Second, true)
	RecordTenantOutboxCursorFlush("error")
	RecordSharedDBPoolStatusAge("org-workers", "pool-workers", "pending", 15*time.Minute)
	RecordSharedDBPoolStuckMarkedFailed("pending")
	RecordSharedDBPoolWave("ok", 2, 40)
	RecordSharedDBPoolCleanup("deprovision", "error")

	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	text := rec.Body.String()
	for _, want := range []string{
		`drive9_mutation_dispatcher_queue_depth{shard="2"} 10.000000`,
		`drive9_mutation_dispatcher_queue_capacity{shard="2"} 4096.000000`,
		`drive9_mutation_dispatcher_enqueue_blocked_seconds_count{shard="2"} 1`,
		`drive9_mutation_dispatcher_batch_size_count 1`,
		`drive9_mutation_dispatcher_batch_fallback_total 1`,
		`drive9_api_key_resolve_cache_requests_total{result="hit"} 1`,
		`drive9_api_key_resolve_cache_entries 8.000000`,
		`drive9_notify_coalescer_pending 3.000000`,
		`drive9_notify_coalescer_flush_total{result="retry_ok"} 1`,
		`drive9_notify_coalescer_batch_size_count 1`,
		`drive9_notify_coalescer_per_row_fallback_total{result="error"} 1`,
		`drive9_tenant_outbox_poll_duration_seconds_count{result="ok"} 1`,
		`drive9_tenant_outbox_batch_size_count 1`,
		`drive9_tenant_outbox_backlog_oldest_age_seconds 12.000000`,
		`drive9_tenant_outbox_full_batches_total 1`,
		`drive9_tenant_outbox_cursor_flush_total{result="error"} 1`,
		`drive9_shared_db_pool_status_age_seconds{db_pool_uuid="pool-workers",status="pending",tidbcloud_org_id="org-workers"} 900.000000`,
		`drive9_shared_db_pool_stuck_marked_failed_total{previous_status="pending"} 1`,
		`drive9_shared_db_pool_wave_total{result="ok"} 1`,
		`drive9_shared_db_pool_wave_physical_pools_count 1`,
		`drive9_shared_db_pool_wave_tenants_count 1`,
		`drive9_shared_db_pool_cleanup_total{result="error",stage="deprovision"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("missing worker metric %q:\n%s", want, text)
		}
	}
	for _, forbidden := range []string{`tenant_id=`, `db_pool_id=`} {
		for _, line := range strings.Split(text, "\n") {
			if !strings.HasPrefix(line, "drive9_mutation_dispatcher_") &&
				!strings.HasPrefix(line, "drive9_api_key_resolve_cache_") &&
				!strings.HasPrefix(line, "drive9_notify_coalescer_") &&
				!strings.HasPrefix(line, "drive9_tenant_outbox_") &&
				!strings.HasPrefix(line, "drive9_shared_db_pool_status_age_seconds") &&
				!strings.HasPrefix(line, "drive9_shared_db_pool_stuck_marked_failed_total") &&
				!strings.HasPrefix(line, "drive9_shared_db_pool_wave_") &&
				!strings.HasPrefix(line, "drive9_shared_db_pool_cleanup_total") {
				continue
			}
			if strings.Contains(line, forbidden) {
				t.Errorf("bounded worker metric contains forbidden label %q: %s", forbidden, line)
			}
		}
	}
}

func TestTenantOutboxBacklogAgeTracksObservedRowsAndSurvivesPollErrors(t *testing.T) {
	RecordTenantOutboxPoll("ok", time.Millisecond, 1000, 2*time.Minute, true)
	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	if !strings.Contains(rec.Body.String(), `drive9_tenant_outbox_batch_size_count 1`) {
		t.Fatalf("successful poll did not observe one batch-size sample:\n%s", rec.Body.String())
	}
	RecordTenantOutboxPoll("error", time.Millisecond, 0, 0, false)

	rec = httptest.NewRecorder()
	WritePrometheus(rec)
	if !strings.Contains(rec.Body.String(), `drive9_tenant_outbox_batch_size_count 1`) {
		t.Fatalf("error poll injected a zero batch-size sample:\n%s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `drive9_tenant_outbox_backlog_oldest_age_seconds 120.000000`) {
		t.Fatalf("poll error cleared last observed backlog age:\n%s", rec.Body.String())
	}

	RecordTenantOutboxPoll("ok", time.Millisecond, 1, 45*time.Second, false)
	rec = httptest.NewRecorder()
	WritePrometheus(rec)
	if !strings.Contains(rec.Body.String(), `drive9_tenant_outbox_backlog_oldest_age_seconds 45.000000`) {
		t.Fatalf("non-full batch did not report its oldest row age:\n%s", rec.Body.String())
	}

	RecordTenantOutboxPoll("ok", time.Millisecond, 0, 0, false)
	rec = httptest.NewRecorder()
	WritePrometheus(rec)
	if !strings.Contains(rec.Body.String(), `drive9_tenant_outbox_backlog_oldest_age_seconds 0.000000`) {
		t.Fatalf("empty successful poll did not clear backlog age:\n%s", rec.Body.String())
	}
}

func TestAPIKeyResolveCacheRequestDoesNotRewriteEntryGauge(t *testing.T) {
	RecordAPIKeyResolveCacheEntries(7)
	RecordAPIKeyResolveCacheRequest("hit")

	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	text := rec.Body.String()
	if !strings.Contains(text, `drive9_api_key_resolve_cache_requests_total{result="hit"}`) {
		t.Fatalf("missing API key cache request counter:\n%s", text)
	}
	if !strings.Contains(text, `drive9_api_key_resolve_cache_entries 7.000000`) {
		t.Fatalf("request path rewrote API key cache entry gauge:\n%s", text)
	}
}
