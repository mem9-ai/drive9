package perf

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// TestCountersSnapshotAndSummary ports v1's TestFusePerfCountersSummary onto
// the by-name recording API: fuse-op stats (count/errors/bytes/max/percentile
// buckets), remote-op stats, cache-hit counters, and the new SummaryString
// layout. Counter-key names are asserted through snapshot()/SnapshotJSON.
func TestCountersSnapshotAndSummary(t *testing.T) {
	counters := NewCounters(true)
	counters.RecordFuseOpByName("lookup", gofuse.OK, 10*time.Millisecond, 0)
	counters.RecordFuseOpByName("read", gofuse.EIO, 20*time.Millisecond, 128)
	counters.RecordRemoteOpByName("read", nil, 5*time.Millisecond, 128)
	// Unknown names are dropped rather than recorded.
	counters.RecordFuseOpByName("bogus", gofuse.OK, time.Millisecond, 0)
	counters.RecordRemoteOpByName("bogus", nil, time.Millisecond, 0)
	counters.RecordCacheHit("read", true)
	counters.RecordCacheHit("read", true)
	counters.RecordCacheHit("read", false)
	counters.RecordCacheHit("dir", true)
	counters.RecordCacheHit("prefetch", false)

	snap := counters.snapshot()
	if got := snap.FuseOps["lookup"].count; got != 1 {
		t.Fatalf("lookup count = %d, want 1", got)
	}
	if got := snap.FuseOps["lookup"].p50NS; got != uint64(16*time.Millisecond) {
		t.Fatalf("lookup p50NS = %d, want %d (10ms falls in the 16ms bucket)", got, uint64(16*time.Millisecond))
	}
	if got := snap.FuseOps["lookup"].p95NS; got != uint64(16*time.Millisecond) {
		t.Fatalf("lookup p95NS = %d, want %d", got, uint64(16*time.Millisecond))
	}
	if got := snap.FuseOps["read"].errors; got != 1 {
		t.Fatalf("read errors = %d, want 1", got)
	}
	if got := snap.FuseOps["bogus"].count; got != 0 {
		t.Fatalf("unknown fuse op recorded: count = %d", got)
	}
	if got := snap.RemoteOps["read"].bytes; got != 128 {
		t.Fatalf("remote read bytes = %d, want 128", got)
	}
	if got := snap.RemoteOps["bogus"].count; got != 0 {
		t.Fatalf("unknown remote op recorded: count = %d", got)
	}
	if got := snap.FuseOps["read"].maxNS; got != uint64(20*time.Millisecond) {
		t.Fatalf("read maxNS = %d, want %d", got, uint64(20*time.Millisecond))
	}
	for key, want := range map[string]uint64{
		"read_cache_hit":         2,
		"read_cache_miss":        1,
		"dir_cache_hit":          1,
		"prefetch_miss":          1,
		"dir_cache_miss":         0,
		"prefetch_hit":           0,
		"namespace_positive_hit": 0,
	} {
		if got := snap.Counters[key]; got != want {
			t.Fatalf("counter %s = %d, want %d", key, got, want)
		}
	}

	data, ok := counters.SnapshotJSON()
	if !ok {
		t.Fatal("SnapshotJSON ok = false, want true")
	}
	var jsonSnap struct {
		Counters map[string]uint64 `json:"Counters"`
	}
	if err := json.Unmarshal(data, &jsonSnap); err != nil {
		t.Fatalf("unmarshal snapshot JSON: %v", err)
	}
	if got := jsonSnap.Counters["read_cache_hit"]; got != 2 {
		t.Fatalf("snapshot JSON read_cache_hit = %d, want 2", got)
	}
	if got := jsonSnap.Counters["read_cache_miss"]; got != 1 {
		t.Fatalf("snapshot JSON read_cache_miss = %d, want 1", got)
	}

	text := counters.SummaryString()
	for _, want := range []string{
		"fuse perf summary",
		"fuse lookup",
		"fuse read",
		"remote read",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("summary missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "bogus") {
		t.Fatalf("summary lists unknown op:\n%s", text)
	}
	// Ops without observations are omitted from the summary.
	if strings.Contains(text, "fuse getattr") {
		t.Fatalf("summary lists unobserved op:\n%s", text)
	}
}

// TestCountersDisabled ports v1's TestFusePerfDisabledByDefault: a disabled
// mount gets nil counters, and the nil value is safe to record through.
func TestCountersDisabled(t *testing.T) {
	if NewCounters(false) != nil {
		t.Fatal("NewCounters(false) should return nil")
	}
	var counters *Counters
	counters.RecordFuseOpByName("lookup", gofuse.OK, time.Millisecond, 0)
	counters.RecordRemoteOpByName("read", nil, time.Millisecond, 0)
	counters.RecordCacheHit("read", true)
	if data, ok := counters.SnapshotJSON(); ok || data != nil {
		t.Fatalf("nil SnapshotJSON = %v, %t, want nil, false", data, ok)
	}
	if text := counters.SummaryString(); text != "" {
		t.Fatalf("nil SummaryString = %q, want empty", text)
	}
	if snap := counters.snapshot(); len(snap.Counters) != 0 {
		t.Fatalf("nil snapshot counters = %v, want empty", snap.Counters)
	}
}

// TestCountersSnapshotKeyRows locks the counter-key mapping for the engine
// events the facade records directly (v1 TestFusePerfCountsNotifyAndSSE and
// the git/retry/commit/uploader rows of TestFusePerfCountersSummary). The
// fields are white-box incremented here; the assertion is that each atomic
// field lands in the snapshot under its stable JSON key.
func TestCountersSnapshotKeyRows(t *testing.T) {
	counters := NewCounters(true)
	counters.lookupRetryTotal.add(1)
	counters.lookupRetrySuccess.add(1)
	counters.commitEnqueue.add(1)
	counters.commitSuccess.add(1)
	counters.uploaderSubmit.add(1)
	counters.sseChange.add(1)
	counters.sseReset.add(1)
	counters.sseSelfFiltered.add(2)
	counters.notifyEntry.add(1)
	counters.notifyInode.add(1)
	counters.gitCleanReadCount.add(3)
	counters.gitCleanTreeHit.add(1)
	counters.gitCleanBlobCacheHit.add(1)
	counters.gitCleanCacheMiss.add(1)
	counters.gitCatFileCount.add(1)
	counters.gitCatFileSlowCount.add(1)
	counters.gitCatFileTotalNS.add(uint64(75 * time.Millisecond))
	counters.gitHydrateStart.add(1)
	counters.gitHydrateSuccess.add(1)
	counters.gitHydrateBytes.add(1024)
	counters.gitHydrateTotalNS.add(uint64(2 * time.Second))
	counters.gitHydrateObjects.add(4)
	counters.gitHydrateObjectBytes.add(512)
	counters.gitHydrateObjectSkipped.add(1)
	counters.gitHydrateObjectMismatch.add(2)
	counters.gitHydrateObjectFallbacks.add(1)
	counters.gitOverlayEnqueue.add(2)
	counters.gitOverlaySync.add(1)
	counters.gitOverlaySuccess.add(3)
	counters.gitOverlayFailure.add(1)
	counters.gitOverlayDrainCount.add(1)
	counters.gitOverlayDrainTotalNS.add(uint64(150 * time.Millisecond))

	for key, want := range map[string]uint64{
		"lookup_retry_total":           1,
		"lookup_retry_success":         1,
		"commit_enqueue":               1,
		"commit_success":               1,
		"uploader_submit":              1,
		"sse_change":                   1,
		"sse_reset":                    1,
		"sse_self_filtered":            2,
		"notify_entry":                 1,
		"notify_inode":                 1,
		"git_clean_read_count":         3,
		"git_clean_tree_hit":           1,
		"git_clean_blob_cache_hit":     1,
		"git_clean_cache_miss":         1,
		"git_cat_file_count":           1,
		"git_cat_file_slow_count":      1,
		"git_cat_file_total_ns":        uint64(75 * time.Millisecond),
		"git_hydrate_start":            1,
		"git_hydrate_success":          1,
		"git_hydrate_bytes":            1024,
		"git_hydrate_total_ns":         uint64(2 * time.Second),
		"git_hydrate_objects":          4,
		"git_hydrate_object_bytes":     512,
		"git_hydrate_object_skipped":   1,
		"git_hydrate_object_mismatch":  2,
		"git_hydrate_object_fallbacks": 1,
		"git_overlay_enqueue":          2,
		"git_overlay_sync":             1,
		"git_overlay_success":          3,
		"git_overlay_failure":          1,
		"git_overlay_drain_count":      1,
		"git_overlay_drain_total_ns":   uint64(150 * time.Millisecond),
	} {
		if got := counters.snapshot().Counters[key]; got != want {
			t.Fatalf("counter %s = %d, want %d", key, got, want)
		}
	}
}

// TestCountersAppendLogRows ports v1's TestAppendLogPerfCounters: the
// append_log remote-op stats (recorded through RecordRemoteOpByName) and the
// append-log outcome/rewrite/generation-reset/fsync counter keys.
func TestCountersAppendLogRows(t *testing.T) {
	counters := NewCounters(true)
	counters.RecordRemoteOpByName("append_log", nil, time.Millisecond, 4)
	counters.RecordRemoteOpByName("append_log", errors.New("rebase"), 2*time.Millisecond, 4)
	counters.appendLogOutcomeSuccess.add(1)
	counters.appendLogOutcomeRebased.add(1)
	counters.appendLogOutcomeTooLarge.add(1)
	counters.appendLogFullRewriteCount.add(1)
	counters.appendLogFullRewriteBytes.add(12)
	counters.appendLogGenerationResetCount.add(1)
	counters.appendLogGenerationResetBytes.add(32)
	counters.appendLogGenerationResetShadowReady.add(1)
	counters.appendLogGenerationResetShadowDegraded.add(1)
	counters.appendLogRebaseRetryCount.add(1)
	counters.appendLogFsyncAppendCount.add(1)
	counters.appendLogFsyncRewriteCount.add(1)

	snap := counters.snapshot()
	if got := snap.RemoteOps["append_log"]; got.count != 2 || got.errors != 1 || got.bytes != 8 {
		t.Fatalf("append-log remote stats = %+v, want count=2 errors=1 bytes=8", got)
	}
	for key, want := range map[string]uint64{
		"append_log_outcome_success":                  1,
		"append_log_outcome_rebased":                  1,
		"append_log_outcome_too_large":                1,
		"append_log_full_rewrite_count":               1,
		"append_log_full_rewrite_bytes":               12,
		"append_log_generation_reset_count":           1,
		"append_log_generation_reset_bytes":           32,
		"append_log_generation_reset_shadow_ready":    1,
		"append_log_generation_reset_shadow_degraded": 1,
		"append_log_rebase_retry_count":               1,
		"append_log_fsync_append_count":               1,
		"append_log_fsync_full_rewrite_count":         1,
	} {
		if got := snap.Counters[key]; got != want {
			t.Fatalf("counter %s = %d, want %d", key, got, want)
		}
	}
}

// TestCountersFlushTimingRows ports v1's TestFlushPerfTimingCounters: flush
// stage shadow timings, snapshot write-back timings, and the write-back
// sub-phase (bytes view / lock wait / dat write / meta write) totals and
// maxima. The v1 record helpers are engine-side now; the counter arithmetic
// they performed is applied field-by-field here.
func TestCountersFlushTimingRows(t *testing.T) {
	counters := NewCounters(true)

	recordFlushStageShadow := func(d time.Duration) {
		counters.flushStageShadowCount.add(1)
		counters.flushStageShadowTotalNS.add(uint64(d))
		counters.flushStageShadowMaxNS.max(uint64(d))
	}
	recordFlushSnapshotWB := func(d time.Duration) {
		counters.flushSnapshotWBCount.add(1)
		counters.flushSnapshotWBTotalNS.add(uint64(d))
		counters.flushSnapshotWBMaxNS.max(uint64(d))
	}
	recordSnapshotWBSubPhases := func(bytesView time.Duration, t WriteBackPutTimings) {
		counters.snapshotWBBytesViewTotalNS.add(uint64(bytesView))
		counters.snapshotWBBytesViewMaxNS.max(uint64(bytesView))
		counters.snapshotWBLockWaitTotalNS.add(uint64(t.LockWait))
		counters.snapshotWBLockWaitMaxNS.max(uint64(t.LockWait))
		counters.snapshotWBDatWriteTotalNS.add(uint64(t.DatWrite))
		counters.snapshotWBDatWriteMaxNS.max(uint64(t.DatWrite))
		counters.snapshotWBMetaWriteTotalNS.add(uint64(t.MetaWrite))
		counters.snapshotWBMetaWriteMaxNS.max(uint64(t.MetaWrite))
	}

	recordFlushStageShadow(50 * time.Millisecond)
	recordFlushStageShadow(100 * time.Millisecond)
	recordFlushSnapshotWB(10 * time.Millisecond)
	recordFlushSnapshotWB(20 * time.Millisecond)
	recordFlushSnapshotWB(30 * time.Millisecond)

	snap := counters.snapshot()
	if got := snap.Counters["flush_stage_shadow_count"]; got != 2 {
		t.Fatalf("flush_stage_shadow_count = %d, want 2", got)
	}
	if got := snap.Counters["flush_stage_shadow_max_ns"]; got != uint64(100*time.Millisecond) {
		t.Fatalf("flush_stage_shadow_max_ns = %d, want %d", got, uint64(100*time.Millisecond))
	}
	if got := snap.Counters["flush_snapshot_wb_count"]; got != 3 {
		t.Fatalf("flush_snapshot_wb_count = %d, want 3", got)
	}
	if got := snap.Counters["flush_snapshot_wb_max_ns"]; got != uint64(30*time.Millisecond) {
		t.Fatalf("flush_snapshot_wb_max_ns = %d, want %d", got, uint64(30*time.Millisecond))
	}

	recordSnapshotWBSubPhases(5*time.Millisecond, WriteBackPutTimings{
		LockWait:  2 * time.Millisecond,
		DatWrite:  80 * time.Millisecond,
		MetaWrite: 40 * time.Millisecond,
	})
	recordSnapshotWBSubPhases(3*time.Millisecond, WriteBackPutTimings{
		LockWait:  1 * time.Millisecond,
		DatWrite:  90 * time.Millisecond,
		MetaWrite: 30 * time.Millisecond,
	})

	snap = counters.snapshot()
	if got := snap.Counters["snapshot_wb_bytes_view_max_ns"]; got != uint64(5*time.Millisecond) {
		t.Fatalf("snapshot_wb_bytes_view_max_ns = %d, want %d", got, uint64(5*time.Millisecond))
	}
	if got := snap.Counters["snapshot_wb_dat_write_max_ns"]; got != uint64(90*time.Millisecond) {
		t.Fatalf("snapshot_wb_dat_write_max_ns = %d, want %d", got, uint64(90*time.Millisecond))
	}
	if got := snap.Counters["snapshot_wb_lock_wait_max_ns"]; got != uint64(2*time.Millisecond) {
		t.Fatalf("snapshot_wb_lock_wait_max_ns = %d, want %d", got, uint64(2*time.Millisecond))
	}
	if got := snap.Counters["snapshot_wb_meta_write_max_ns"]; got != uint64(40*time.Millisecond) {
		t.Fatalf("snapshot_wb_meta_write_max_ns = %d, want %d", got, uint64(40*time.Millisecond))
	}
	wantBVTotal := uint64(5*time.Millisecond) + uint64(3*time.Millisecond)
	if got := snap.Counters["snapshot_wb_bytes_view_total_ns"]; got != wantBVTotal {
		t.Fatalf("snapshot_wb_bytes_view_total_ns = %d, want %d", got, wantBVTotal)
	}

	// Disabled counters must not panic when the engine records timings.
	var nilCounters *Counters
	nilCounters.snapshot()
}

// TestCountersLatencyPercentileBuckets keeps the percentile math on the
// log2-ish bucket ladder honest: single-sample percentiles land on the bucket
// upper bound, and out-of-range durations clamp to the last bucket.
func TestCountersLatencyPercentileBuckets(t *testing.T) {
	var buckets [perfLatencyBucketCount]uint64
	if got := perfLatencyPercentile(buckets, 0, 95); got != 0 {
		t.Fatalf("percentile of empty series = %d, want 0", got)
	}
	buckets[perfLatencyBucketIndex(uint64(10*time.Millisecond))] = 1
	if got := perfLatencyPercentile(buckets, 1, 95); got != uint64(16*time.Millisecond) {
		t.Fatalf("p95 of one 10ms sample = %d, want 16ms bucket", got)
	}
	if got := perfLatencyBucketIndex(uint64(121 * time.Second)); got != perfLatencyBucketCount-1 {
		t.Fatalf("bucket index of 121s = %d, want clamp to %d", got, perfLatencyBucketCount-1)
	}

	// Durations of zero are counted but skip the latency histogram.
	counters := NewCounters(true)
	counters.RecordFuseOpByName("lookup", gofuse.OK, 0, 0)
	snap := counters.snapshot()
	if got := snap.FuseOps["lookup"].count; got != 1 {
		t.Fatalf("lookup count = %d, want 1", got)
	}
	if got := snap.FuseOps["lookup"].maxNS; got != 0 {
		t.Fatalf("lookup maxNS = %d, want 0", got)
	}
}
