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
	perfFuseRelease
	perfFuseCreate
	perfFuseMkdir
	perfFuseUnlink
	perfFuseRmdir
	perfFuseRename
	perfFuseSetAttr
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
	perfFuseRelease:     "release",
	perfFuseCreate:      "create",
	perfFuseMkdir:       "mkdir",
	perfFuseUnlink:      "unlink",
	perfFuseRmdir:       "rmdir",
	perfFuseRename:      "rename",
	perfFuseSetAttr:     "setattr",
}

var (
	_ [int(perfFuseOpCount) - len(perfFuseOpNames)]struct{}
	_ [len(perfFuseOpNames) - int(perfFuseOpCount)]struct{}
)

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

var (
	_ [int(perfRemoteOpCount) - len(perfRemoteOpNames)]struct{}
	_ [len(perfRemoteOpNames) - int(perfRemoteOpCount)]struct{}
)

type perfOpStats struct {
	count   uint64
	errors  uint64
	bytes   uint64
	totalNS uint64
}

type fusePerfCounters struct {
	enabled bool
	start   time.Time

	fuseOps   [perfFuseOpCount]perfAtomicStats
	remoteOps [perfRemoteOpCount]perfAtomicStats

	readCacheHit  atomic.Uint64
	readCacheMiss atomic.Uint64
	dirCacheHit   atomic.Uint64
	dirCacheMiss  atomic.Uint64
	prefetchHit   atomic.Uint64
	prefetchMiss  atomic.Uint64

	namespacePositiveHit  atomic.Uint64
	namespaceNegativeHit  atomic.Uint64
	namespaceCompleteMiss atomic.Uint64
	namespaceSessionMiss  atomic.Uint64
	namespacePartialMiss  atomic.Uint64

	lookupRetryTotal     atomic.Uint64
	lookupRetrySuccess   atomic.Uint64
	lookupRetryExhausted atomic.Uint64
	readRetryTotal       atomic.Uint64
	readRetrySuccess     atomic.Uint64
	readRetryExhausted   atomic.Uint64

	commitEnqueue      atomic.Uint64
	commitEnqueueError atomic.Uint64
	commitRetry        atomic.Uint64
	commitSuccess      atomic.Uint64
	commitFailure      atomic.Uint64

	uploaderSubmit       atomic.Uint64
	uploaderSyncFallback atomic.Uint64
	uploaderSuccess      atomic.Uint64
	uploaderFailure      atomic.Uint64

	commitDrainCount   atomic.Uint64
	commitDrainTotalNS atomic.Uint64
	uploaderDrainCount atomic.Uint64
	uploaderDrainTotal atomic.Uint64

	sseChange       atomic.Uint64
	sseReset        atomic.Uint64
	sseSelfFiltered atomic.Uint64

	notifyEntry atomic.Uint64
	notifyInode atomic.Uint64
}

type perfAtomicStats struct {
	count   atomic.Uint64
	errors  atomic.Uint64
	bytes   atomic.Uint64
	totalNS atomic.Uint64
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

func (p *fusePerfCounters) recordFuseOp(op perfFuseOp, status gofuse.Status, dur time.Duration, bytes uint64) (string, bool) {
	name, ok := perfFuseName(op)
	if !p.isEnabled() || !ok {
		return "", false
	}
	p.fuseOps[op].record(status != gofuse.OK, dur, bytes)
	return name, true
}

func (p *fusePerfCounters) recordRemoteOp(op perfRemoteOp, err error, dur time.Duration, bytes uint64) (string, bool) {
	name, ok := perfRemoteName(op)
	if !p.isEnabled() || !ok {
		return "", false
	}
	p.remoteOps[op].record(err != nil, dur, bytes)
	return name, true
}

func (s *perfAtomicStats) record(failed bool, dur time.Duration, bytes uint64) {
	s.count.Add(1)
	if failed {
		s.errors.Add(1)
	}
	if bytes > 0 {
		s.bytes.Add(bytes)
	}
	if dur > 0 {
		s.totalNS.Add(uint64(dur))
	}
}

func (s *perfAtomicStats) snapshot() perfOpStats {
	return perfOpStats{
		count:   s.count.Load(),
		errors:  s.errors.Load(),
		bytes:   s.bytes.Load(),
		totalNS: s.totalNS.Load(),
	}
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
	snap.Counters["read_cache_hit"] = p.readCacheHit.Load()
	snap.Counters["read_cache_miss"] = p.readCacheMiss.Load()
	snap.Counters["dir_cache_hit"] = p.dirCacheHit.Load()
	snap.Counters["dir_cache_miss"] = p.dirCacheMiss.Load()
	snap.Counters["prefetch_hit"] = p.prefetchHit.Load()
	snap.Counters["prefetch_miss"] = p.prefetchMiss.Load()
	snap.Counters["namespace_positive_hit"] = p.namespacePositiveHit.Load()
	snap.Counters["namespace_negative_hit"] = p.namespaceNegativeHit.Load()
	snap.Counters["namespace_complete_miss"] = p.namespaceCompleteMiss.Load()
	snap.Counters["namespace_session_miss"] = p.namespaceSessionMiss.Load()
	snap.Counters["namespace_partial_miss"] = p.namespacePartialMiss.Load()
	snap.Counters["lookup_retry_total"] = p.lookupRetryTotal.Load()
	snap.Counters["lookup_retry_success"] = p.lookupRetrySuccess.Load()
	snap.Counters["lookup_retry_exhausted"] = p.lookupRetryExhausted.Load()
	snap.Counters["read_retry_total"] = p.readRetryTotal.Load()
	snap.Counters["read_retry_success"] = p.readRetrySuccess.Load()
	snap.Counters["read_retry_exhausted"] = p.readRetryExhausted.Load()
	snap.Counters["commit_enqueue"] = p.commitEnqueue.Load()
	snap.Counters["commit_enqueue_error"] = p.commitEnqueueError.Load()
	snap.Counters["commit_retry"] = p.commitRetry.Load()
	snap.Counters["commit_success"] = p.commitSuccess.Load()
	snap.Counters["commit_failure"] = p.commitFailure.Load()
	snap.Counters["uploader_submit"] = p.uploaderSubmit.Load()
	snap.Counters["uploader_sync_fallback"] = p.uploaderSyncFallback.Load()
	snap.Counters["uploader_success"] = p.uploaderSuccess.Load()
	snap.Counters["uploader_failure"] = p.uploaderFailure.Load()
	snap.Counters["commit_drain_count"] = p.commitDrainCount.Load()
	snap.Counters["commit_drain_total_ns"] = p.commitDrainTotalNS.Load()
	snap.Counters["uploader_drain_count"] = p.uploaderDrainCount.Load()
	snap.Counters["uploader_drain_total_ns"] = p.uploaderDrainTotal.Load()
	snap.Counters["sse_change"] = p.sseChange.Load()
	snap.Counters["sse_reset"] = p.sseReset.Load()
	snap.Counters["sse_self_filtered"] = p.sseSelfFiltered.Load()
	snap.Counters["notify_entry"] = p.notifyEntry.Load()
	snap.Counters["notify_inode"] = p.notifyInode.Load()
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
		writePerfLine(w, "drive9: perf %s %s count=%d errors=%d bytes=%d avg=%s\n",
			group, name, st.count, st.errors, st.bytes, avg.Truncate(time.Microsecond))
	}
}

func writePerfLine(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, format, args...)
}

func perfFuseName(op perfFuseOp) (string, bool) {
	if op < 0 || op >= perfFuseOpCount {
		return "", false
	}
	return perfFuseOpNames[op], true
}

func perfRemoteName(op perfRemoteOp) (string, bool) {
	if op < 0 || op >= perfRemoteOpCount {
		return "", false
	}
	return perfRemoteOpNames[op], true
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
	name, ok := fs.perf.recordFuseOp(op, status, dur, bytes)
	if !ok {
		return
	}
	result := "ok"
	if status != gofuse.OK {
		result = "error"
	}
	metrics.RecordFuseOperation(name, result, dur, bytes)
}

func (fs *Dat9FS) perfRecordRemote(op perfRemoteOp, start time.Time, err error, bytes uint64) {
	if !fs.perfEnabled() || start.IsZero() {
		return
	}
	dur := time.Since(start)
	name, ok := fs.perf.recordRemoteOp(op, err, dur, bytes)
	if !ok {
		return
	}
	result := "ok"
	if err != nil {
		result = "error"
	}
	metrics.RecordFuseRemoteOperation(name, result, dur, bytes)
}
