package fuse

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/dat9/pkg/metrics"
)

type perfFuseOp int

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

type perfRemoteOp int

const (
	perfRemoteStat perfRemoteOp = iota
	perfRemoteList
	perfRemoteRead
	perfRemoteWrite
	perfRemoteMutation
	perfRemoteOpCount
)

var perfRemoteOpNames = [...]string{
	perfRemoteStat:     "stat",
	perfRemoteList:     "list",
	perfRemoteRead:     "read",
	perfRemoteWrite:    "write",
	perfRemoteMutation: "mutation",
}

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

type fusePerfCounters struct {
	enabled bool
	start   time.Time

	fuseOps   [perfFuseOpCount]perfAtomicStats
	remoteOps [perfRemoteOpCount]perfAtomicStats

	readCacheHit  atomicUint64
	readCacheMiss atomicUint64
	dirCacheHit   atomicUint64
	dirCacheMiss  atomicUint64
	prefetchHit   atomicUint64
	prefetchMiss  atomicUint64

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

	commitEnqueue      atomicUint64
	commitEnqueueError atomicUint64
	commitRetry        atomicUint64
	commitSuccess      atomicUint64
	commitFailure      atomicUint64

	uploaderSubmit       atomicUint64
	uploaderSyncFallback atomicUint64
	uploaderSuccess      atomicUint64
	uploaderFailure      atomicUint64

	commitDrainCount   atomicUint64
	commitDrainTotalNS atomicUint64
	uploaderDrainCount atomicUint64
	uploaderDrainTotal atomicUint64

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

// atomicUint64 is a small wrapper around sync/atomic.Uint64. Keeping it local
// keeps tests compact and avoids map allocations on FUSE hot paths.
type atomicUint64 struct {
	v uint64
}

func (a *atomicUint64) add(n uint64) { atomic.AddUint64(&a.v, n) }
func (a *atomicUint64) load() uint64 { return atomic.LoadUint64(&a.v) }
func (a *atomicUint64) max(n uint64) {
	for {
		cur := atomic.LoadUint64(&a.v)
		if n <= cur {
			return
		}
		if atomic.CompareAndSwapUint64(&a.v, cur, n) {
			return
		}
	}
}

const perfLatencyBucketCount = 28

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

type perfAtomicStats struct {
	count          atomicUint64
	errors         atomicUint64
	bytes          atomicUint64
	totalNS        atomicUint64
	maxNS          atomicUint64
	latencyBuckets [perfLatencyBucketCount]atomicUint64
}

func newFusePerfCounters(enabled bool) *fusePerfCounters {
	if !enabled {
		return nil
	}
	metrics.SetModuleAvailability("fuse", true)
	return &fusePerfCounters{
		enabled: true,
		start:   time.Now(),
	}
}

func (p *fusePerfCounters) isEnabled() bool {
	return p != nil && p.enabled
}

func (p *fusePerfCounters) recordFuseOp(op perfFuseOp, status gofuse.Status, dur time.Duration, bytes uint64) {
	if !p.isEnabled() || op < 0 || op >= perfFuseOpCount {
		return
	}
	p.fuseOps[op].record(status != gofuse.OK, dur, bytes)
}

func (p *fusePerfCounters) recordRemoteOp(op perfRemoteOp, err error, dur time.Duration, bytes uint64) {
	if !p.isEnabled() || op < 0 || op >= perfRemoteOpCount {
		return
	}
	p.remoteOps[op].record(err != nil, dur, bytes)
}

func (p *fusePerfCounters) recordLocalPolicy(source policyMatchSource) {
	if !p.isEnabled() {
		return
	}
	switch source {
	case policyMatchLocalOnly:
		p.localPolicyLocalOnly.add(1)
	case policyMatchRemoteOverride:
		p.localPolicyRemoteOverride.add(1)
	case policyMatchRemoteDefault:
		p.localPolicyRemoteDefault.add(1)
	}
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

type fusePerfSnapshot struct {
	Uptime    time.Duration
	FuseOps   map[string]perfOpStats
	RemoteOps map[string]perfOpStats
	Counters  map[string]uint64
}

func (p *fusePerfCounters) snapshot() fusePerfSnapshot {
	if !p.isEnabled() {
		return fusePerfSnapshot{}
	}
	snap := fusePerfSnapshot{
		Uptime:    time.Since(p.start),
		FuseOps:   make(map[string]perfOpStats, len(p.fuseOps)),
		RemoteOps: make(map[string]perfOpStats, len(p.remoteOps)),
		Counters:  make(map[string]uint64, 32),
	}
	for i, stats := range p.fuseOps {
		snap.FuseOps[perfFuseOpNames[i]] = stats.snapshot()
	}
	for i, stats := range p.remoteOps {
		snap.RemoteOps[perfRemoteOpNames[i]] = stats.snapshot()
	}
	snap.Counters["read_cache_hit"] = p.readCacheHit.load()
	snap.Counters["read_cache_miss"] = p.readCacheMiss.load()
	snap.Counters["dir_cache_hit"] = p.dirCacheHit.load()
	snap.Counters["dir_cache_miss"] = p.dirCacheMiss.load()
	snap.Counters["prefetch_hit"] = p.prefetchHit.load()
	snap.Counters["prefetch_miss"] = p.prefetchMiss.load()
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
	snap.Counters["uploader_submit"] = p.uploaderSubmit.load()
	snap.Counters["uploader_sync_fallback"] = p.uploaderSyncFallback.load()
	snap.Counters["uploader_success"] = p.uploaderSuccess.load()
	snap.Counters["uploader_failure"] = p.uploaderFailure.load()
	snap.Counters["commit_drain_count"] = p.commitDrainCount.load()
	snap.Counters["commit_drain_total_ns"] = p.commitDrainTotalNS.load()
	snap.Counters["uploader_drain_count"] = p.uploaderDrainCount.load()
	snap.Counters["uploader_drain_total_ns"] = p.uploaderDrainTotal.load()
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

func (p *fusePerfCounters) printSummary(w io.Writer) {
	if !p.isEnabled() || w == nil {
		return
	}
	snap := p.snapshot()
	writePerfLine(w, "drive9: FUSE perf summary uptime=%s\n", snap.Uptime.Truncate(time.Millisecond))
	writePerfOps(w, "fuse", perfFuseOpNames[:], snap.FuseOps)
	writePerfOps(w, "remote", perfRemoteOpNames[:], snap.RemoteOps)
	writePerfLine(w, "drive9: perf cache read_hit=%d read_miss=%d dir_hit=%d dir_miss=%d prefetch_hit=%d prefetch_miss=%d\n",
		snap.Counters["read_cache_hit"], snap.Counters["read_cache_miss"],
		snap.Counters["dir_cache_hit"], snap.Counters["dir_cache_miss"],
		snap.Counters["prefetch_hit"], snap.Counters["prefetch_miss"])
	writePerfLine(w, "drive9: perf namespace positive_hit=%d negative_hit=%d complete_miss=%d session_miss=%d partial_miss=%d\n",
		snap.Counters["namespace_positive_hit"], snap.Counters["namespace_negative_hit"],
		snap.Counters["namespace_complete_miss"], snap.Counters["namespace_session_miss"],
		snap.Counters["namespace_partial_miss"])
	writePerfLine(w, "drive9: perf retries lookup_total=%d lookup_success=%d lookup_exhausted=%d read_total=%d read_success=%d read_exhausted=%d\n",
		snap.Counters["lookup_retry_total"], snap.Counters["lookup_retry_success"], snap.Counters["lookup_retry_exhausted"],
		snap.Counters["read_retry_total"], snap.Counters["read_retry_success"], snap.Counters["read_retry_exhausted"])
	writePerfLine(w, "drive9: perf commit enqueue=%d enqueue_errors=%d retries=%d success=%d failure=%d drain_count=%d drain_total=%s\n",
		snap.Counters["commit_enqueue"], snap.Counters["commit_enqueue_error"], snap.Counters["commit_retry"],
		snap.Counters["commit_success"], snap.Counters["commit_failure"],
		snap.Counters["commit_drain_count"], time.Duration(snap.Counters["commit_drain_total_ns"]).Truncate(time.Millisecond))
	writePerfLine(w, "drive9: perf uploader submit=%d sync_fallback=%d success=%d failure=%d drain_count=%d drain_total=%s\n",
		snap.Counters["uploader_submit"], snap.Counters["uploader_sync_fallback"],
		snap.Counters["uploader_success"], snap.Counters["uploader_failure"],
		snap.Counters["uploader_drain_count"], time.Duration(snap.Counters["uploader_drain_total_ns"]).Truncate(time.Millisecond))
	writePerfLine(w, "drive9: perf sse change=%d reset=%d self_filtered=%d notify_entry=%d notify_inode=%d\n",
		snap.Counters["sse_change"], snap.Counters["sse_reset"], snap.Counters["sse_self_filtered"],
		snap.Counters["notify_entry"], snap.Counters["notify_inode"])
	writePerfLine(w, "drive9: perf local_policy local_only=%d remote_override=%d remote_default=%d\n",
		snap.Counters["local_policy_local_only"],
		snap.Counters["local_policy_remote_override"],
		snap.Counters["local_policy_remote_default"])
	writePerfLine(w, "drive9: perf git clean_read=%d tree_hit=%d blob_cache_hit=%d cache_miss=%d cat_file=%d cat_file_slow=%d cat_file_avg=%s hydrate_start=%d hydrate_success=%d hydrate_failure=%d hydrate_bytes=%d hydrate_total=%s hydrate_objects=%d hydrate_object_bytes=%d hydrate_object_skipped=%d hydrate_object_mismatch=%d hydrate_object_fallbacks=%d\n",
		snap.Counters["git_clean_read_count"],
		snap.Counters["git_clean_tree_hit"],
		snap.Counters["git_clean_blob_cache_hit"],
		snap.Counters["git_clean_cache_miss"],
		snap.Counters["git_cat_file_count"],
		snap.Counters["git_cat_file_slow_count"],
		avgDuration(snap.Counters["git_cat_file_total_ns"], snap.Counters["git_cat_file_count"]).Truncate(time.Microsecond),
		snap.Counters["git_hydrate_start"],
		snap.Counters["git_hydrate_success"],
		snap.Counters["git_hydrate_failure"],
		snap.Counters["git_hydrate_bytes"],
		time.Duration(snap.Counters["git_hydrate_total_ns"]).Truncate(time.Millisecond),
		snap.Counters["git_hydrate_objects"],
		snap.Counters["git_hydrate_object_bytes"],
		snap.Counters["git_hydrate_object_skipped"],
		snap.Counters["git_hydrate_object_mismatch"],
		snap.Counters["git_hydrate_object_fallbacks"])
	writePerfLine(w, "drive9: perf git_overlay enqueue=%d sync=%d success=%d failure=%d drain_count=%d drain_total=%s\n",
		snap.Counters["git_overlay_enqueue"],
		snap.Counters["git_overlay_sync"],
		snap.Counters["git_overlay_success"],
		snap.Counters["git_overlay_failure"],
		snap.Counters["git_overlay_drain_count"],
		time.Duration(snap.Counters["git_overlay_drain_total_ns"]).Truncate(time.Millisecond))
	writePerfLine(w, "drive9: perf git_workspace refresh=%d forced_refresh=%d\n",
		snap.Counters["git_workspace_refresh"],
		snap.Counters["git_workspace_forced_refresh"])
}

func writePerfOps(w io.Writer, group string, names []string, stats map[string]perfOpStats) {
	for _, name := range names {
		st := stats[name]
		if st.count == 0 {
			continue
		}
		avg := time.Duration(0)
		if st.count > 0 && st.totalNS > 0 {
			avg = time.Duration(st.totalNS / st.count)
		}
		writePerfLine(w, "drive9: perf %s %s count=%d errors=%d bytes=%d avg=%s p95=%s max=%s\n",
			group, name, st.count, st.errors, st.bytes,
			avg.Truncate(time.Microsecond),
			time.Duration(st.p95NS).Truncate(time.Microsecond),
			time.Duration(st.maxNS).Truncate(time.Microsecond))
	}
}

func writePerfLine(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, format, args...)
}

func avgDuration(totalNS, count uint64) time.Duration {
	if count == 0 || totalNS == 0 {
		return 0
	}
	return time.Duration(totalNS / count)
}

func (fs *Dat9FS) perfEnabled() bool {
	return fs != nil && fs.perf != nil && fs.perf.enabled
}

func (fs *Dat9FS) perfStart() time.Time {
	if !fs.perfEnabled() {
		return time.Time{}
	}
	return time.Now()
}

func (fs *Dat9FS) perfRecordFuse(op perfFuseOp, start time.Time, status gofuse.Status, bytes uint64) {
	if !fs.perfEnabled() || start.IsZero() {
		return
	}
	dur := time.Since(start)
	fs.perf.recordFuseOp(op, status, dur, bytes)
	result := "ok"
	if status != gofuse.OK {
		result = "error"
	}
	if op >= 0 && op < perfFuseOpCount {
		metrics.RecordFuseOperation(perfFuseOpNames[op], result, dur, bytes)
	}
}

func (fs *Dat9FS) perfRecordRemote(op perfRemoteOp, start time.Time, err error, bytes uint64) {
	if !fs.perfEnabled() || start.IsZero() {
		return
	}
	dur := time.Since(start)
	fs.perf.recordRemoteOp(op, err, dur, bytes)
	result := "ok"
	if err != nil {
		result = "error"
	}
	if op >= 0 && op < perfRemoteOpCount {
		metrics.RecordFuseRemoteOperation(perfRemoteOpNames[op], result, dur, bytes)
	}
}
