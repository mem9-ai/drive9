// Package perf provides the mount's performance observability: atomic
// kernel-op latency/error/byte counters with percentile buckets (Counters),
// a continuously-sampled recorder writing rotating JSONL samples
// (ContinuousPerfRecorder), and on-demand pprof/CPU profiling (Profiler).
// The facade owns the instances and records engine-side events.
package perf

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/metrics"
)

const perfLatencyBucketCount = 28

const (
	perfFuseLookup perfFuseOp = iota
	perfFuseGetAttr
	perfFuseOpenDir
	perfFuseReadDir
	perfFuseReadDirPlus
	perfFuseOpen
	perfFuseRead
	perfFuseWrite
	perfFuseFlush
	perfFuseFsync
	perfFuseFsyncDir
	perfFuseSyncFs
	perfFuseRelease
	perfFuseCreate
	perfFuseMkdir
	perfFuseMknod
	perfFuseUnlink
	perfFuseRmdir
	perfFuseRename
	perfFuseSetAttr
	perfFuseReadlink
	perfFuseSymlink
	perfFuseLink
	perfFuseAccess
	perfFuseOpCount
)

const (
	perfRemoteStat perfRemoteOp = iota
	perfRemoteList
	perfRemoteRead
	perfRemoteWrite
	perfRemoteMutation
	perfRemoteAppendLog
	perfRemoteOpCount
)

var perfLatencyBucketNS = [...]uint64{
	uint64(time.Microsecond),
	2 * uint64(time.Microsecond),
	4 * uint64(time.Microsecond),
	8 * uint64(time.Microsecond),
	16 * uint64(time.Microsecond),
	32 * uint64(time.Microsecond),
	64 * uint64(time.Microsecond),
	128 * uint64(time.Microsecond),
	256 * uint64(time.Microsecond),
	512 * uint64(time.Microsecond),
	uint64(time.Millisecond),
	2 * uint64(time.Millisecond),
	4 * uint64(time.Millisecond),
	8 * uint64(time.Millisecond),
	16 * uint64(time.Millisecond),
	32 * uint64(time.Millisecond),
	64 * uint64(time.Millisecond),
	128 * uint64(time.Millisecond),
	256 * uint64(time.Millisecond),
	512 * uint64(time.Millisecond),
	uint64(time.Second),
	2 * uint64(time.Second),
	4 * uint64(time.Second),
	8 * uint64(time.Second),
	16 * uint64(time.Second),
	32 * uint64(time.Second),
	64 * uint64(time.Second),
	120 * uint64(time.Second),
}

var perfFuseOpNames = [...]string{
	perfFuseLookup:      "lookup",
	perfFuseGetAttr:     "getattr",
	perfFuseOpenDir:     "opendir",
	perfFuseReadDir:     "readdir",
	perfFuseReadDirPlus: "readdirplus",
	perfFuseOpen:        "open",
	perfFuseRead:        "read",
	perfFuseWrite:       "write",
	perfFuseFlush:       "flush",
	perfFuseFsync:       "fsync",
	perfFuseFsyncDir:    "fsyncdir",
	perfFuseSyncFs:      "syncfs",
	perfFuseRelease:     "release",
	perfFuseCreate:      "create",
	perfFuseMkdir:       "mkdir",
	perfFuseMknod:       "mknod",
	perfFuseUnlink:      "unlink",
	perfFuseRmdir:       "rmdir",
	perfFuseRename:      "rename",
	perfFuseSetAttr:     "setattr",
	perfFuseReadlink:    "readlink",
	perfFuseSymlink:     "symlink",
	perfFuseLink:        "link",
	perfFuseAccess:      "access",
}

var perfRemoteOpNames = [...]string{
	perfRemoteStat:      "stat",
	perfRemoteList:      "list",
	perfRemoteRead:      "read",
	perfRemoteWrite:     "write",
	perfRemoteMutation:  "mutation",
	perfRemoteAppendLog: "append_log",
}

// Counters holds atomic per-op latency, error, and byte counters for the
// kernel ops the facade serves. A nil *Counters is valid and records
// nothing.
type Counters struct {
	enabled bool
	start   time.Time

	fuseOps                        [perfFuseOpCount]perfAtomicStats
	remoteOps                      [perfRemoteOpCount]perfAtomicStats
	closeSyncModeForbiddenFallback atomicUint64

	readCacheHit  atomicUint64
	readCacheMiss atomicUint64
	dirCacheHit   atomicUint64
	dirCacheMiss  atomicUint64
	prefetchHit   atomicUint64
	prefetchMiss  atomicUint64

	directoryPrefetchIssued            atomicUint64
	directoryPrefetchJoined            atomicUint64
	directoryPrefetchJoinSuccess       atomicUint64
	directoryPrefetchCompletedUnjoined atomicUint64
	directoryPrefetchRejected          atomicUint64
	directoryPrefetchEntries           atomicUint64
	directoryPrefetchBytes             atomicUint64
	directoryPrefetchDurationNS        atomicUint64
	directoryPrefetchMaxConcurrent     atomicUint64
	directoryPrefetchForegroundWaitNS  atomicUint64

	namespacePositiveHit  atomicUint64
	namespaceNegativeHit  atomicUint64
	namespaceCompleteMiss atomicUint64
	namespaceSessionMiss  atomicUint64
	namespacePartialMiss  atomicUint64
	lookupStormList       atomicUint64
	lookupStormDeferred   atomicUint64

	lookupRetryTotal     atomicUint64
	lookupRetrySuccess   atomicUint64
	lookupRetryExhausted atomicUint64
	readRetryTotal       atomicUint64
	readRetrySuccess     atomicUint64
	readRetryExhausted   atomicUint64

	commitEnqueue         atomicUint64
	commitEnqueueError    atomicUint64
	commitRetry           atomicUint64
	commitSuccess         atomicUint64
	commitFailure         atomicUint64
	commitTerminalFailure atomicUint64

	uploaderSubmit       atomicUint64
	uploaderSyncFallback atomicUint64
	uploaderSuccess      atomicUint64
	uploaderFailure      atomicUint64

	commitDrainCount   atomicUint64
	commitDrainTotalNS atomicUint64
	uploaderDrainCount atomicUint64
	uploaderDrainTotal atomicUint64

	flushStageShadowCount   atomicUint64
	flushStageShadowTotalNS atomicUint64
	flushStageShadowMaxNS   atomicUint64
	flushSnapshotWBCount    atomicUint64
	flushSnapshotWBTotalNS  atomicUint64
	flushSnapshotWBMaxNS    atomicUint64

	// snapshotWriteBackLocked sub-phase counters
	snapshotWBBytesViewTotalNS atomicUint64
	snapshotWBBytesViewMaxNS   atomicUint64
	snapshotWBLockWaitTotalNS  atomicUint64
	snapshotWBLockWaitMaxNS    atomicUint64
	snapshotWBDatWriteTotalNS  atomicUint64
	snapshotWBDatWriteMaxNS    atomicUint64
	snapshotWBMetaWriteTotalNS atomicUint64
	snapshotWBMetaWriteMaxNS   atomicUint64

	appendLogOutcomeSuccess                atomicUint64
	appendLogOutcomeRebased                atomicUint64
	appendLogOutcomeConflict               atomicUint64
	appendLogOutcomeUnsupported            atomicUint64
	appendLogOutcomeTooLarge               atomicUint64
	appendLogOutcomeError                  atomicUint64
	appendLogFullRewriteCount              atomicUint64
	appendLogFullRewriteBytes              atomicUint64
	appendLogGenerationResetCount          atomicUint64
	appendLogGenerationResetBytes          atomicUint64
	appendLogGenerationResetShadowReady    atomicUint64
	appendLogGenerationResetShadowDegraded atomicUint64
	appendLogRebaseRetryCount              atomicUint64
	appendLogFsyncAppendCount              atomicUint64
	appendLogFsyncAppendTotalNS            atomicUint64
	appendLogFsyncAppendMaxNS              atomicUint64
	appendLogFsyncRewriteCount             atomicUint64
	appendLogFsyncRewriteTotalNS           atomicUint64
	appendLogFsyncRewriteMaxNS             atomicUint64

	sseChange       atomicUint64
	sseReset        atomicUint64
	sseSelfFiltered atomicUint64

	notifyEntry atomicUint64
	notifyInode atomicUint64

	localPolicyLocalOnly      atomicUint64
	localPolicyRemoteOverride atomicUint64
	localPolicyRemoteDefault  atomicUint64

	gitCleanReadCount         atomicUint64
	gitCleanTreeHit           atomicUint64
	gitCleanBlobCacheHit      atomicUint64
	gitCleanCacheMiss         atomicUint64
	gitCatFileCount           atomicUint64
	gitCatFileSlowCount       atomicUint64
	gitCatFileTotalNS         atomicUint64
	gitHydrateStart           atomicUint64
	gitHydrateSuccess         atomicUint64
	gitHydrateFailure         atomicUint64
	gitHydrateBytes           atomicUint64
	gitHydrateTotalNS         atomicUint64
	gitHydrateObjects         atomicUint64
	gitHydrateObjectBytes     atomicUint64
	gitHydrateObjectSkipped   atomicUint64
	gitHydrateObjectMismatch  atomicUint64
	gitHydrateObjectFallbacks atomicUint64
	gitOverlayEnqueue         atomicUint64
	gitOverlaySync            atomicUint64
	gitOverlaySuccess         atomicUint64
	gitOverlayFailure         atomicUint64
	gitOverlayDrainCount      atomicUint64
	gitOverlayDrainTotalNS    atomicUint64

	gitWorkspaceRefresh       atomicUint64
	gitWorkspaceForcedRefresh atomicUint64
}

// WriteBackPutTimings breaks one write-back cache put into its lock and
// file-write phases for diagnostics.
type WriteBackPutTimings struct {
	LockWait  time.Duration // time spent waiting for WriteBackCache.mu
	DatWrite  time.Duration // atomicWrite of the .dat file (data + fsync + rename)
	MetaWrite time.Duration // atomicWrite of the .meta file (meta + fsync + rename)
}

type perfAtomicStats struct {
	count          atomicUint64
	errors         atomicUint64
	bytes          atomicUint64
	totalNS        atomicUint64
	maxNS          atomicUint64
	latencyBuckets [perfLatencyBucketCount]atomicUint64
}

type fusePerfSnapshot struct {
	Uptime    time.Duration
	FuseOps   map[string]perfOpStats
	RemoteOps map[string]perfOpStats
	Counters  map[string]uint64
}

type atomicUint64 struct{ v atomic.Uint64 }

type perfFuseOp int

type perfRemoteOp int

type perfOpStats struct {
	count   uint64
	errors  uint64
	bytes   uint64
	totalNS uint64
	p50NS   uint64
	p95NS   uint64
	p99NS   uint64
	maxNS   uint64
}

// NewCounters creates the counter set; a disabled mount gets nil (a
// valid, recording-nothing value).
func NewCounters(enabled bool) *Counters {
	if !enabled {
		return nil
	}
	metrics.SetModuleAvailability("fuse", true)
	return &Counters{
		enabled: true,
		start:   time.Now(),
	}
}

// RecordFuseOpByName records one kernel-op observation by counter
// name; unknown names are dropped. Nil-safe.
func (p *Counters) RecordFuseOpByName(name string, status gofuse.Status, dur time.Duration, bytes uint64) {
	if !p.isEnabled() {
		return
	}
	for i, n := range perfFuseOpNames {
		if n == name {
			p.recordFuseOp(perfFuseOp(i), status, dur, bytes)
			return
		}
	}
}

// RecordRemoteOpByName records one remote (HTTP) operation observation by
// counter name (stat/read/write/mutation); unknown names are dropped.
// Nil-safe. Also feeds the Prometheus remote-operation series (v1
// perfRecordRemote).
func (p *Counters) RecordRemoteOpByName(name string, err error, dur time.Duration, bytes uint64) {
	if !p.isEnabled() {
		return
	}
	for i, n := range perfRemoteOpNames {
		if n == name {
			p.remoteOps[i].record(err != nil, dur, bytes)
			metrics.RecordFuseRemoteOperation(name, remoteOpResult(err), dur, bytes)
			return
		}
	}
}

// RecordCacheHit counts one read-tier observation ("read", "dir", or
// "prefetch"). Nil-safe.
func (p *Counters) RecordCacheHit(kind string, hit bool) {
	if !p.isEnabled() {
		return
	}
	switch kind {
	case "read":
		if hit {
			p.readCacheHit.add(1)
		} else {
			p.readCacheMiss.add(1)
		}
	case "dir":
		if hit {
			p.dirCacheHit.add(1)
		} else {
			p.dirCacheMiss.add(1)
		}
	case "prefetch":
		if hit {
			p.prefetchHit.add(1)
		} else {
			p.prefetchMiss.add(1)
		}
	}
}

func remoteOpResult(err error) string {
	if err == nil {
		return "ok"
	}
	return "error"
}

// SnapshotJSON renders the counter snapshot (ok=false when disabled).
func (p *Counters) SnapshotJSON() ([]byte, bool) {
	if !p.isEnabled() {
		return nil, false
	}
	b, err := json.Marshal(p.snapshot())
	if err != nil {
		return nil, false
	}
	return b, true
}

// UptimeString reports the uptime the summary headline carries.
func (p *Counters) UptimeString() string {
	snap := p.snapshot()
	return snap.Uptime.Truncate(time.Millisecond).String()
}

// GitWorkspaceLine renders v1's workspace refresh accounting line (the
// git-ops refresh budget guard greps exactly this format).
func (p *Counters) GitWorkspaceLine() string {
	if !p.isEnabled() {
		return ""
	}
	snap := p.snapshot()
	return fmt.Sprintf("drive9: perf git_workspace refresh=%d forced_refresh=%d",
		snap.Counters["git_workspace_refresh"], snap.Counters["git_workspace_forced_refresh"])
}

// RecordGitWorkspaceRefresh counts one workspace list (forced marks a
// reload the arm/notify path asked for, not a lazy miss).
func (p *Counters) RecordGitWorkspaceRefresh(forced bool) {
	if p == nil || !p.isEnabled() {
		return
	}
	p.gitWorkspaceRefresh.add(1)
	if forced {
		p.gitWorkspaceForcedRefresh.add(1)
	}
}

// PerfLines renders v1's machine-readable per-op accounting lines (the
// write-path budget smoke greps exactly this format):
//
//	drive9: perf remote <op> count=N errors=N bytes=N avg=<dur>
//	drive9: perf fuse <op> count=N errors=N bytes=N avg=<dur>
func (p *Counters) PerfLines() []string {
	if !p.isEnabled() {
		return nil
	}
	snap := p.snapshot()
	var lines []string
	emit := func(group string, ops map[string]perfOpStats) {
		names := make([]string, 0, len(ops))
		for n := range ops {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			st := ops[n]
			if st.count == 0 {
				continue
			}
			avg := time.Duration(0)
			if st.count > 0 {
				avg = time.Duration(st.totalNS / st.count)
			}
			lines = append(lines, fmt.Sprintf("drive9: perf %s %s count=%d errors=%d bytes=%d avg=%s",
				group, n, st.count, st.errors, st.bytes, avg))
		}
	}
	emit("remote", snap.RemoteOps)
	emit("fuse", snap.FuseOps)
	return lines
}

// CommitLine renders v1's commit-queue accounting line: "drive9: perf
// commit enqueue=N enqueue_errors=N retries=N success=N failure=N".
func (p *Counters) CommitLine() string {
	if !p.isEnabled() {
		return ""
	}
	return fmt.Sprintf("drive9: perf commit enqueue=%d enqueue_errors=%d retries=%d success=%d failure=%d",
		p.commitEnqueue.load(), p.commitEnqueueError.load(), p.commitRetry.load(),
		p.commitSuccess.load(), p.commitFailure.load())
}

// SummaryString renders the unmount summary (v1 printSummary): one line
// per op with observations — totals, errors, and p95/p99 latencies. Empty
// when disabled.
func (p *Counters) SummaryString() string {
	if !p.isEnabled() {
		return ""
	}
	snap := p.snapshot()
	var b strings.Builder
	b.WriteString(fmt.Sprintf("fuse perf summary (uptime %s):\n", snap.Uptime.Truncate(time.Second)))
	writeOps := func(title string, ops map[string]perfOpStats) {
		names := make([]string, 0, len(ops))
		for n := range ops {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			st := ops[n]
			if st.count == 0 {
				continue
			}
			b.WriteString(fmt.Sprintf("  %s %-14s n=%-8d err=%-6d p95=%-12s p99=%s\n",
				title, n, st.count, st.errors, time.Duration(st.p95NS), time.Duration(st.p99NS)))
		}
	}
	writeOps("fuse", snap.FuseOps)
	writeOps("remote", snap.RemoteOps)
	return b.String()
}

func perfLatencyBucketIndex(durNS uint64) int {
	for i, upper := range perfLatencyBucketNS {
		if durNS <= upper {
			return i
		}
	}
	return len(perfLatencyBucketNS) - 1
}

func perfLatencyPercentile(buckets [perfLatencyBucketCount]uint64, count uint64, percentile uint64) uint64 {
	if count == 0 || percentile == 0 {
		return 0
	}
	target := (count*percentile + 99) / 100
	if target == 0 {
		target = 1
	}
	var seen uint64
	for i, n := range buckets {
		seen += n
		if seen >= target {
			return perfLatencyBucketNS[i]
		}
	}
	return 0
}

func (p *Counters) isEnabled() bool {
	return p != nil && p.enabled
}

func (p *Counters) recordFuseOp(op perfFuseOp, status gofuse.Status, dur time.Duration, bytes uint64) {
	if !p.isEnabled() || op < 0 || op >= perfFuseOpCount {
		return
	}
	p.fuseOps[op].record(status != gofuse.OK, dur, bytes)
	// Prometheus series (v1 perfRecordFuse): operation totals and
	// interrupt accounting live alongside the JSONL counters.
	result := "ok"
	if status != gofuse.OK {
		result = "error"
		if status == gofuse.EINTR {
			metrics.RecordFuseInterrupt()
		}
	}
	metrics.RecordFuseOperation(perfFuseOpNames[op], result, dur, bytes)
}

func (p *Counters) snapshot() fusePerfSnapshot {
	if !p.isEnabled() {
		return fusePerfSnapshot{}
	}
	snap := fusePerfSnapshot{
		Uptime:    time.Since(p.start),
		FuseOps:   make(map[string]perfOpStats, len(p.fuseOps)),
		RemoteOps: make(map[string]perfOpStats, len(p.remoteOps)),
		Counters:  make(map[string]uint64, 32),
	}
	for i := range p.fuseOps {
		snap.FuseOps[perfFuseOpNames[i]] = p.fuseOps[i].snapshot()
	}
	for i := range p.remoteOps {
		snap.RemoteOps[perfRemoteOpNames[i]] = p.remoteOps[i].snapshot()
	}
	snap.Counters["close_sync_mode_forbidden_fallback"] = p.closeSyncModeForbiddenFallback.load()
	snap.Counters["read_cache_hit"] = p.readCacheHit.load()
	snap.Counters["read_cache_miss"] = p.readCacheMiss.load()
	snap.Counters["dir_cache_hit"] = p.dirCacheHit.load()
	snap.Counters["dir_cache_miss"] = p.dirCacheMiss.load()
	snap.Counters["prefetch_hit"] = p.prefetchHit.load()
	snap.Counters["prefetch_miss"] = p.prefetchMiss.load()
	snap.Counters["directory_prefetch_issued"] = p.directoryPrefetchIssued.load()
	snap.Counters["directory_prefetch_joined"] = p.directoryPrefetchJoined.load()
	snap.Counters["directory_prefetch_join_success"] = p.directoryPrefetchJoinSuccess.load()
	snap.Counters["directory_prefetch_completed_unjoined"] = p.directoryPrefetchCompletedUnjoined.load()
	snap.Counters["directory_prefetch_rejected"] = p.directoryPrefetchRejected.load()
	snap.Counters["directory_prefetch_entries"] = p.directoryPrefetchEntries.load()
	snap.Counters["directory_prefetch_bytes"] = p.directoryPrefetchBytes.load()
	snap.Counters["directory_prefetch_duration_ns"] = p.directoryPrefetchDurationNS.load()
	snap.Counters["directory_prefetch_max_concurrent"] = p.directoryPrefetchMaxConcurrent.load()
	snap.Counters["directory_prefetch_foreground_wait_ns"] = p.directoryPrefetchForegroundWaitNS.load()
	snap.Counters["namespace_positive_hit"] = p.namespacePositiveHit.load()
	snap.Counters["namespace_negative_hit"] = p.namespaceNegativeHit.load()
	snap.Counters["namespace_complete_miss"] = p.namespaceCompleteMiss.load()
	snap.Counters["namespace_session_miss"] = p.namespaceSessionMiss.load()
	snap.Counters["namespace_partial_miss"] = p.namespacePartialMiss.load()
	snap.Counters["lookup_storm_list"] = p.lookupStormList.load()
	snap.Counters["lookup_storm_list_deferred"] = p.lookupStormDeferred.load()
	snap.Counters["lookup_retry_total"] = p.lookupRetryTotal.load()
	snap.Counters["lookup_retry_success"] = p.lookupRetrySuccess.load()
	snap.Counters["lookup_retry_exhausted"] = p.lookupRetryExhausted.load()
	snap.Counters["read_retry_total"] = p.readRetryTotal.load()
	snap.Counters["read_retry_success"] = p.readRetrySuccess.load()
	snap.Counters["read_retry_exhausted"] = p.readRetryExhausted.load()
	snap.Counters["commit_enqueue"] = p.commitEnqueue.load()
	snap.Counters["commit_enqueue_error"] = p.commitEnqueueError.load()
	snap.Counters["commit_retry"] = p.commitRetry.load()
	snap.Counters["commit_success"] = p.commitSuccess.load()
	snap.Counters["commit_failure"] = p.commitFailure.load()
	snap.Counters["commit_terminal_failure"] = p.commitTerminalFailure.load()
	snap.Counters["uploader_submit"] = p.uploaderSubmit.load()
	snap.Counters["uploader_sync_fallback"] = p.uploaderSyncFallback.load()
	snap.Counters["uploader_success"] = p.uploaderSuccess.load()
	snap.Counters["uploader_failure"] = p.uploaderFailure.load()
	snap.Counters["commit_drain_count"] = p.commitDrainCount.load()
	snap.Counters["commit_drain_total_ns"] = p.commitDrainTotalNS.load()
	snap.Counters["uploader_drain_count"] = p.uploaderDrainCount.load()
	snap.Counters["uploader_drain_total_ns"] = p.uploaderDrainTotal.load()
	snap.Counters["flush_stage_shadow_count"] = p.flushStageShadowCount.load()
	snap.Counters["flush_stage_shadow_total_ns"] = p.flushStageShadowTotalNS.load()
	snap.Counters["flush_stage_shadow_max_ns"] = p.flushStageShadowMaxNS.load()
	snap.Counters["flush_snapshot_wb_count"] = p.flushSnapshotWBCount.load()
	snap.Counters["flush_snapshot_wb_total_ns"] = p.flushSnapshotWBTotalNS.load()
	snap.Counters["flush_snapshot_wb_max_ns"] = p.flushSnapshotWBMaxNS.load()
	snap.Counters["snapshot_wb_bytes_view_total_ns"] = p.snapshotWBBytesViewTotalNS.load()
	snap.Counters["snapshot_wb_bytes_view_max_ns"] = p.snapshotWBBytesViewMaxNS.load()
	snap.Counters["snapshot_wb_lock_wait_total_ns"] = p.snapshotWBLockWaitTotalNS.load()
	snap.Counters["snapshot_wb_lock_wait_max_ns"] = p.snapshotWBLockWaitMaxNS.load()
	snap.Counters["snapshot_wb_dat_write_total_ns"] = p.snapshotWBDatWriteTotalNS.load()
	snap.Counters["snapshot_wb_dat_write_max_ns"] = p.snapshotWBDatWriteMaxNS.load()
	snap.Counters["snapshot_wb_meta_write_total_ns"] = p.snapshotWBMetaWriteTotalNS.load()
	snap.Counters["snapshot_wb_meta_write_max_ns"] = p.snapshotWBMetaWriteMaxNS.load()
	snap.Counters["append_log_outcome_success"] = p.appendLogOutcomeSuccess.load()
	snap.Counters["append_log_outcome_rebased"] = p.appendLogOutcomeRebased.load()
	snap.Counters["append_log_outcome_conflict"] = p.appendLogOutcomeConflict.load()
	snap.Counters["append_log_outcome_unsupported"] = p.appendLogOutcomeUnsupported.load()
	snap.Counters["append_log_outcome_too_large"] = p.appendLogOutcomeTooLarge.load()
	snap.Counters["append_log_outcome_error"] = p.appendLogOutcomeError.load()
	snap.Counters["append_log_full_rewrite_count"] = p.appendLogFullRewriteCount.load()
	snap.Counters["append_log_full_rewrite_bytes"] = p.appendLogFullRewriteBytes.load()
	snap.Counters["append_log_generation_reset_count"] = p.appendLogGenerationResetCount.load()
	snap.Counters["append_log_generation_reset_bytes"] = p.appendLogGenerationResetBytes.load()
	snap.Counters["append_log_generation_reset_shadow_ready"] = p.appendLogGenerationResetShadowReady.load()
	snap.Counters["append_log_generation_reset_shadow_degraded"] = p.appendLogGenerationResetShadowDegraded.load()
	snap.Counters["append_log_rebase_retry_count"] = p.appendLogRebaseRetryCount.load()
	snap.Counters["append_log_fsync_append_count"] = p.appendLogFsyncAppendCount.load()
	snap.Counters["append_log_fsync_append_total_ns"] = p.appendLogFsyncAppendTotalNS.load()
	snap.Counters["append_log_fsync_append_max_ns"] = p.appendLogFsyncAppendMaxNS.load()
	snap.Counters["append_log_fsync_full_rewrite_count"] = p.appendLogFsyncRewriteCount.load()
	snap.Counters["append_log_fsync_full_rewrite_total_ns"] = p.appendLogFsyncRewriteTotalNS.load()
	snap.Counters["append_log_fsync_full_rewrite_max_ns"] = p.appendLogFsyncRewriteMaxNS.load()
	snap.Counters["sse_change"] = p.sseChange.load()
	snap.Counters["sse_reset"] = p.sseReset.load()
	snap.Counters["sse_self_filtered"] = p.sseSelfFiltered.load()
	snap.Counters["notify_entry"] = p.notifyEntry.load()
	snap.Counters["notify_inode"] = p.notifyInode.load()
	snap.Counters["local_policy_local_only"] = p.localPolicyLocalOnly.load()
	snap.Counters["local_policy_remote_override"] = p.localPolicyRemoteOverride.load()
	snap.Counters["local_policy_remote_default"] = p.localPolicyRemoteDefault.load()
	snap.Counters["git_clean_read_count"] = p.gitCleanReadCount.load()
	snap.Counters["git_clean_tree_hit"] = p.gitCleanTreeHit.load()
	snap.Counters["git_clean_blob_cache_hit"] = p.gitCleanBlobCacheHit.load()
	snap.Counters["git_clean_cache_miss"] = p.gitCleanCacheMiss.load()
	snap.Counters["git_cat_file_count"] = p.gitCatFileCount.load()
	snap.Counters["git_cat_file_slow_count"] = p.gitCatFileSlowCount.load()
	snap.Counters["git_cat_file_total_ns"] = p.gitCatFileTotalNS.load()
	snap.Counters["git_hydrate_start"] = p.gitHydrateStart.load()
	snap.Counters["git_hydrate_success"] = p.gitHydrateSuccess.load()
	snap.Counters["git_hydrate_failure"] = p.gitHydrateFailure.load()
	snap.Counters["git_hydrate_bytes"] = p.gitHydrateBytes.load()
	snap.Counters["git_hydrate_total_ns"] = p.gitHydrateTotalNS.load()
	snap.Counters["git_hydrate_objects"] = p.gitHydrateObjects.load()
	snap.Counters["git_hydrate_object_bytes"] = p.gitHydrateObjectBytes.load()
	snap.Counters["git_hydrate_object_skipped"] = p.gitHydrateObjectSkipped.load()
	snap.Counters["git_hydrate_object_mismatch"] = p.gitHydrateObjectMismatch.load()
	snap.Counters["git_hydrate_object_fallbacks"] = p.gitHydrateObjectFallbacks.load()
	snap.Counters["git_overlay_enqueue"] = p.gitOverlayEnqueue.load()
	snap.Counters["git_overlay_sync"] = p.gitOverlaySync.load()
	snap.Counters["git_overlay_success"] = p.gitOverlaySuccess.load()
	snap.Counters["git_overlay_failure"] = p.gitOverlayFailure.load()
	snap.Counters["git_overlay_drain_count"] = p.gitOverlayDrainCount.load()
	snap.Counters["git_overlay_drain_total_ns"] = p.gitOverlayDrainTotalNS.load()
	snap.Counters["git_workspace_refresh"] = p.gitWorkspaceRefresh.load()
	snap.Counters["git_workspace_forced_refresh"] = p.gitWorkspaceForcedRefresh.load()
	return snap
}

func (s *perfAtomicStats) record(failed bool, dur time.Duration, bytes uint64) {
	s.count.add(1)
	if failed {
		s.errors.add(1)
	}
	if bytes > 0 {
		s.bytes.add(bytes)
	}
	if dur > 0 {
		durNS := uint64(dur)
		s.totalNS.add(durNS)
		s.maxNS.max(durNS)
		s.latencyBuckets[perfLatencyBucketIndex(durNS)].add(1)
	}
}

func (s *perfAtomicStats) snapshot() perfOpStats {
	var buckets [perfLatencyBucketCount]uint64
	for i := range s.latencyBuckets {
		buckets[i] = s.latencyBuckets[i].load()
	}
	count := s.count.load()
	return perfOpStats{
		count:   count,
		errors:  s.errors.load(),
		bytes:   s.bytes.load(),
		totalNS: s.totalNS.load(),
		p50NS:   perfLatencyPercentile(buckets, count, 50),
		p95NS:   perfLatencyPercentile(buckets, count, 95),
		p99NS:   perfLatencyPercentile(buckets, count, 99),
		maxNS:   s.maxNS.load(),
	}
}

func (a *atomicUint64) add(n uint64) { a.v.Add(n) }

func (a *atomicUint64) max(n uint64) {
	for {
		cur := a.v.Load()
		if cur >= n || a.v.CompareAndSwap(cur, n) {
			return
		}
	}
}

func (a *atomicUint64) load() uint64 { return a.v.Load() }
