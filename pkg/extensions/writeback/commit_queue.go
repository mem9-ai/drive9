package writeback

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/mountpath"
)

const (
	maxInlineLayerEntryBytes     = client.DefaultFSLayerInlineEntryBytes
	zeroTruncateCommitQueueDelay = 50 * time.Millisecond
	defaultCommitBatchMaxItems   = 64
	defaultCommitBatchMaxBytes   = client.MaxBatchWriteBytes
)

// shadowSpillCompareChunk bounds memory during the streaming idempotency
// comparison of large ShadowSpill files in conflict auto-resolve.
const shadowSpillCompareChunk = 8 << 20

var errCommitPostUpload = errors.New("commit post-upload step failed")

var errCommitPayloadStale = errors.New("commit payload generation is stale")

// CommitEntry represents a pending remote commit.
type CommitEntry struct {
	Path                 string
	Inode                uint64
	MutationSeq          uint64
	BaseRev              int64 // revision when we started editing
	Size                 int64
	Kind                 staging.PendingKind
	ShadowSpill          bool // true when data is only in shadow file (auto-resolve would OOM)
	Mode                 uint32
	HasMode              bool
	LayerEntry           staging.LayerCacheIdentity // server identity returned by this Layer upload
	CoalesceZeroTruncate bool
	// PayloadBaseRev is the revision that the local bytes were derived from.
	// It is intentionally separate from BaseRev (the CAS revision sent to the
	// server): a stale writeback entry must never pair rev2-era bytes with a
	// later BaseRev just because a sibling handle committed rev3.
	PayloadBaseRev    int64
	PayloadBaseRevSet bool
	// SnapshotID identifies the exact full local image represented by this
	// entry. ParentSnapshotID identifies the immediately preceding staged
	// image from which these bytes were derived. Unlike size or staging
	// generation, the direct-parent relationship is a causal proof: two
	// handles that fork from the same image produce siblings, not descendants.
	SnapshotID       string
	ParentSnapshotID string
	// ShadowGen and PendingIndexGen bind this entry to the exact path-local
	// staging generations that existed when the entry was enqueued. Upload and
	// cleanup use these tokens so a later same-path write cannot swap in newer
	// shadow bytes or have its pending metadata removed by an older entry.
	//
	// ShadowGen is a CONTENT generation, not a pin token: it gates uploads and
	// cleanup by "is this still the same bytes", which is what every caller
	// does with it. Reads of the entry's bytes must go through the
	// content-generation APIs (ReadAllIfGeneration / OpenIfGeneration /
	// ReadAtIfGeneration); the pin-generation APIs (ReadAtGen) live in a
	// different number space and reject a content generation outright.
	ShadowGen       staging.ContentGen
	PendingIndexGen uint64
	WriteBackGen    uint64
	// DurableWatermarkRev is the newest revision that Dat9FS knows was already
	// made durable (for example by a strict fsync). Entries whose payload was
	// based on an older revision are stale and must fail instead of overwriting
	// that watermark through Release/CommitQueue.
	DurableWatermarkRev int64
	// DirtyRanges carries the write buffer's dirty part ranges at enqueue
	// time, coalesced and clipped to Size. Extent-layout commits upload only
	// these ranges as dirty blocks (incremental PATCH); classic commits
	// ignore them and send the whole image.
	DirtyRanges []ByteRange
	// DisableAutoResolveLWW forces conflicts for fenced entries to stay
	// terminal. Re-basing old bytes with a new BaseRev is exactly the silent
	// rollback class this fence prevents.
	DisableAutoResolveLWW bool
	// growthRebaseRev records a one-shot authorization to pair a descendant
	// payload (#896 base-zero growth, #935 live same-path rewrite) with the revision it is safe to overwrite. PayloadBaseRev
	// intentionally remains unchanged for auditability, so repeated
	// validation must consult this state instead of re-authorizing.
	growthRebaseRev              int64
	growthRebaseParentSnapshotID string
	recovered                    bool
	liveLineageProof             bool
	liveAncestors                []string // immutable, process-local snapshot ancestry
	dispatched                   bool
	canceled                     bool
	cancelCommit                 context.CancelFunc
	cancelUpload                 context.CancelFunc
	payload                      []byte
	payloadBound                 bool
}

// CommitSuccessFunc is called after a commit queue entry is successfully
// uploaded. committedRev is the server-returned revision (>0 for direct PUT,
// 0 for multipart where the revision is not returned inline).
type CommitSuccessFunc func(entry *CommitEntry, committedRev int64)

// CommitCleanupFunc is called after a successful commit's local shadow/index
// state has been removed but before the queue entry is dequeued.
type CommitCleanupFunc func(entry *CommitEntry)

// CommitSupersededFunc reports whether a queued live-handle snapshot has been
// overtaken by a newer mutation and must not reach remote storage.
type CommitSupersededFunc func(entry *CommitEntry) bool

// pathCommitFailure is the most recent terminal commit failure recorded for a
// path: the last upload error and a monotonic stamp for bounded trimming.
// standalone marks records installed by RecordTerminalFailureForPath: no
// queue entry backs them and the unlanded bytes stay under a DIFFERENT (old)
// pathname, so their own path typically carries no pending state at all —
// nothing durable can re-derive them, and only an explicit clear may drop
// them.
type pathCommitFailure struct {
	err        error
	seq        uint64
	standalone bool
}

// CommitQueue manages ordered background remote commits with baseRev tracking.
// It provides backpressure when the queue exceeds maxPending items.
type CommitQueue struct {
	mu           sync.Mutex
	queue        []*CommitEntry
	queuedByPath map[string]map[*CommitEntry]struct{}
	inFlight     map[string]*CommitEntry   // paths currently being processed by workers
	immediate    map[*CommitEntry]struct{} // synchronous live commits participating in inode ordering
	delayed      map[*CommitEntry]*time.Timer
	// landed is an intentionally process-local map of exact snapshots this
	// queue has committed. Shadow bytes and JSON metadata are not atomically
	// replaced across a crash, so persisting this proof would be unsafe.
	landed      map[string]pathCommitLandmark
	landedClock uint64
	// commitClock/commitSeq record EVERY successful commit per path with no
	// payload conditions — unlike the landed landmarks, which are a
	// selective lineage/rebase cache (no snapshot ID, no checksum above
	// 1MiB ⇒ no landmark). The rename flow's in-window detection and
	// failure-record rekey fences read these: they must observe a completed
	// upload regardless of size, entry provenance, or event-stream state.
	commitClock uint64
	commitSeq   map[string]uint64
	// terminalFailures records the most recent terminal commit failure per
	// path so a durability barrier that waited on the path can report EIO
	// instead of success: after onCommitTerminalFailure the path looks
	// idle to WaitPath* exactly like a landed commit. Recorded before the
	// entry leaves in-flight, cleared by the next successful commit of the
	// path (onCommitSuccessWithOptions), mirroring landed's lifecycle.
	terminalFailures map[string]pathCommitFailure
	failureClock     uint64
	maxPending       int
	client           *client.Client
	remoteRoot       string
	layerRef         string
	shadows          *staging.ShadowStore
	// retainedShadows marks a path whose landed entry must NOT have its
	// shadow reclaimed: the staging handle refused certification (its
	// buffer moved on), but the shadow is still the only restore source for
	// the landing's unchanged evicted parts. Consumed by the cleanup pass.
	retainedShadows   sync.Map
	extentGlobs       []globMatcher
	extentMatchCutoff int64
	index             *staging.PendingIndex
	journal           *staging.Journal
	wg                sync.WaitGroup
	stopped           bool
	// abandoned is set by AbandonLayer when the mounted layer was rolled
	// back. When true, Enqueue returns errLayerRolledBack and commitOne
	// skips tryAutoResolveConflict, going straight to terminal failure so
	// pending writes are preserved as staging.PendingConflict.
	abandoned bool

	// OnSuccess is called after successful upload with the committed
	// revision. Used by dat9fs to seed readCache and update inode revision.
	OnSuccess CommitSuccessFunc

	// OnUploaded is called at the data-commit boundary before fallible mode,
	// journal, or cleanup bookkeeping.
	OnUploaded CommitSuccessFunc

	// OnCleanup is called after local commit state has been removed.
	OnCleanup CommitCleanupFunc

	// OnDiscard is called after a superseded entry's owned staging and queue
	// state have been removed without publishing commit success.
	OnDiscard CommitCleanupFunc

	// IsSuperseded is an optional upload-boundary guard. Sequence-zero path and
	// recovery entries remain unfiltered by the Dat9FS implementation.
	IsSuperseded CommitSupersededFunc

	// serializeMutationInodes enables gVisor's newer-mutation supersession.
	// Live content commits always order by inode, including hardlink aliases.
	serializeMutationInodes bool

	// PathLock serializes upload and cleanup against Dat9FS same-path shadow
	// mutations. When unset, the queue still serializes same-path entries.
	PathLock func(path string) func()
	// DurableWatermark returns the latest path revision that Dat9FS has
	// observed as committed/durable. It is consulted while the path lock is
	// held so an entry that was fresh at enqueue time cannot LWW-rebase stale
	// payload bytes after a sibling strict fsync advances the same path.
	DurableWatermark func(path string) int64

	// workCh dispatches entries to upload workers. The buffer is always
	// larger than maxPending so Enqueue never blocks.
	workCh chan *CommitEntry

	zeroTruncateDelay time.Duration
	// extentTakeover arms the extent content-layout commit branch
	// (SetExtentTakeover).
	extentTakeover bool
	batchWindow    time.Duration
	batchMaxItems  int
	batchMaxBytes  int64

	perf *fusePerfCounters
}

// CommitQueueSnapshot is one queue-depth observation.
type CommitQueueSnapshot struct {
	Pending       int
	Bytes         int64
	InFlight      int
	Delayed       int
	Conflicts     int
	ConflictBytes int64
	FirstPath     string
}

type commitBatchConfig struct {
	window   time.Duration
	maxItems int
	maxBytes int64
}

// NewCommitQueue creates a CommitQueue with background workers.
func NewCommitQueue(c *client.Client, shadows *staging.ShadowStore, index *staging.PendingIndex, journal *staging.Journal, numWorkers int, maxPending int, remoteRoot ...string) *CommitQueue {
	index.SetShadowStore(shadows)
	if numWorkers <= 0 {
		numWorkers = 4
	}
	if maxPending <= 0 {
		maxPending = maxCommitQueuePending
	}
	root := "/"
	if len(remoteRoot) > 0 && remoteRoot[0] != "" {
		root = remoteRoot[0]
	}
	// Buffer must be > maxPending so Enqueue's send never blocks.
	bufSize := maxPending * 2
	if bufSize < 256 {
		bufSize = 256
	}
	cq := &CommitQueue{
		maxPending:        maxPending,
		client:            c,
		remoteRoot:        root,
		shadows:           shadows,
		index:             index,
		journal:           journal,
		inFlight:          make(map[string]*CommitEntry),
		immediate:         make(map[*CommitEntry]struct{}),
		queuedByPath:      make(map[string]map[*CommitEntry]struct{}),
		delayed:           make(map[*CommitEntry]*time.Timer),
		landed:            make(map[string]pathCommitLandmark),
		terminalFailures:  make(map[string]pathCommitFailure),
		workCh:            make(chan *CommitEntry, bufSize),
		zeroTruncateDelay: zeroTruncateCommitQueueDelay,
	}
	for i := 0; i < numWorkers; i++ {
		cq.wg.Add(1)
		go cq.worker()
	}
	return cq
}

// SetPerfCounters attaches the commit counters shim.
func (cq *CommitQueue) SetPerfCounters(perf *fusePerfCounters) {
	cq.perf = perf
}

// SetExtentTakeover arms the extent content-layout branch: large commits
// are probed (one stat per commit) and routed to extent block sessions.
// It is the v2 cutover kill-switch; queues without it keep the classic
// direct-PUT/multipart routing byte-for-byte.
func (cq *CommitQueue) SetExtentTakeover(enabled bool) {
	if cq == nil {
		return
	}
	cq.mu.Lock()
	cq.extentTakeover = enabled
	cq.mu.Unlock()
}

// SetExtentPatterns arms the pattern-owned extent routing: with a
// non-empty pattern set, a queued image promotes to an extent session
// only when its (mount-relative) path matches, at or above the matched
// cutoff; an empty set keeps the size-only hybrid promotion.
func (cq *CommitQueue) SetExtentPatterns(patterns []string, matchedCutoff int64) {
	if cq == nil {
		return
	}
	globs := compileExtentGlobs(patterns)
	cq.mu.Lock()
	cq.extentGlobs = globs
	cq.extentMatchCutoff = matchedCutoff
	cq.mu.Unlock()
}

// extentRouteFor reports the extent routing decision for a queued path.
func (cq *CommitQueue) extentRouteFor(path string, size int64) bool {
	cq.mu.Lock()
	globs, cutoff := cq.extentGlobs, cq.extentMatchCutoff
	cq.mu.Unlock()
	if len(globs) > 0 {
		if !matchExtentPattern(globs, path) {
			return false
		}
		return size >= cutoff
	}
	threshold := cq.directPutThreshold()
	if threshold <= 0 {
		// Unknown cutoff (cache cold): the historical behavior routes to
		// the extent plane rather than guessing classic.
		return true
	}
	return size >= threshold
}

// RetainShadowForLanding asks the success cleanup to keep this path's
// shadow generation: the handle that staged the landed image refused
// certification (newer local mutations), and its unchanged evicted parts
// still reference the shadow as their only restore source. The next
// staging overwrites the shadow and the next landing reclaims it.
func (cq *CommitQueue) RetainShadowForLanding(path string, gen staging.ContentGen) {
	if cq == nil {
		return
	}
	cq.retainedShadows.Store(path, gen)
}

// ConfigureBatchWrite enables a small writeback-only direct-PUT batch window.
// A non-positive window disables batching.
func (cq *CommitQueue) ConfigureBatchWrite(window time.Duration, maxItems int, maxBytes int64) {
	if cq == nil {
		return
	}
	if maxItems <= 0 {
		maxItems = defaultCommitBatchMaxItems
	}
	if maxItems > client.MaxBatchWriteItems {
		maxItems = client.MaxBatchWriteItems
	}
	if maxBytes <= 0 {
		maxBytes = defaultCommitBatchMaxBytes
	}
	if maxBytes > client.MaxBatchWriteBytes {
		maxBytes = client.MaxBatchWriteBytes
	}
	cq.mu.Lock()
	defer cq.mu.Unlock()
	if window <= 0 || maxItems <= 1 || maxBytes <= 0 {
		cq.batchWindow = 0
		cq.batchMaxItems = 0
		cq.batchMaxBytes = 0
		return
	}
	cq.batchWindow = window
	cq.batchMaxItems = maxItems
	cq.batchMaxBytes = maxBytes
}

// SetLayerRef pins the layer every entry commits into.
func (cq *CommitQueue) SetLayerRef(layerRef string) {
	if cq == nil {
		return
	}
	cq.mu.Lock()
	cq.layerRef = strings.TrimSpace(layerRef)
	cq.mu.Unlock()
}

// Enqueue adds a commit entry to the queue. Returns an error if the queue
// is full (backpressure).
func (cq *CommitQueue) Enqueue(entry *CommitEntry) error {
	cq.mu.Lock()
	defer cq.mu.Unlock()
	if cq.stopped {
		if cq.perf != nil {
			cq.perf.commitEnqueueError.add(1)
		}
		if cq.abandoned {
			return errLayerRolledBack
		}
		return fmt.Errorf("commit queue stopped")
	}
	if len(cq.queue) >= cq.maxPending {
		if cq.perf != nil {
			cq.perf.commitEnqueueError.add(1)
		}
		return fmt.Errorf("commit queue full (%d pending)", cq.maxPending)
	}
	cq.queue = append(cq.queue, entry)
	cq.addQueuedLocked(entry)
	cq.coalesceDirectParentQueuedLocked(entry)
	if cq.perf != nil {
		cq.perf.commitEnqueue.add(1)
	}

	if cq.shouldDelayZeroTruncateLocked(entry) {
		cq.delayZeroTruncateLocked(entry)
		return nil
	}
	entry.dispatched = true

	// Send to workers while holding the lock. The channel buffer is always
	// > maxPending, so this will not block as long as the backpressure
	// check above holds. Holding the lock prevents DrainAll from closing
	// workCh between our check and the send.
	cq.workCh <- entry
	return nil
}

// Pending returns the number of pending commits.
func (cq *CommitQueue) Pending() int {
	cq.mu.Lock()
	defer cq.mu.Unlock()
	return len(cq.queue)
}

// PendingStats returns the number of unique pending entries and the sum
// of their sizes. Used by unmount to display drain progress.
//
// Subtleties:
// 1. In-flight entries remain in cq.queue until the worker calls
// removeFromQueue *after* cleanup (see line 692). The same
// *CommitEntry pointer is stored in both cq.queue and cq.inFlight
// during that window.
// 2. Enqueue does NOT dedupe by Path (line 153 just appends). A path
// written + closed twice yields two distinct entries with the same
// Path string but different pointers.
//
// We dedupe by entry pointer: collapses (1) without collapsing (2).
func (cq *CommitQueue) PendingStats() (count int, bytes int64) {
	cq.mu.Lock()
	defer cq.mu.Unlock()
	seen := make(map[*CommitEntry]struct{}, len(cq.queue))
	for _, e := range cq.queue {
		if _, ok := seen[e]; ok {
			continue
		}
		seen[e] = struct{}{}
		count++
		bytes += e.Size
	}
	for _, e := range cq.inFlight {
		if _, ok := seen[e]; ok {
			continue
		}
		seen[e] = struct{}{}
		count++
		bytes += e.Size
	}
	for e := range cq.immediate {
		if _, ok := seen[e]; ok {
			continue
		}
		seen[e] = struct{}{}
		count++
		bytes += e.Size
	}
	return
}

// Snapshot captures the current queue depths.
func (cq *CommitQueue) Snapshot() CommitQueueSnapshot {
	if cq == nil {
		return CommitQueueSnapshot{}
	}
	cq.mu.Lock()
	defer cq.mu.Unlock()
	return cq.snapshotLocked()
}

// WaitIdle blocks until every queued entry has settled.
func (cq *CommitQueue) WaitIdle(ctx context.Context) error {
	if cq == nil {
		return nil
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		cq.mu.Lock()
		cq.forceAllDelayedLocked()
		snap := cq.snapshotLocked()
		cq.mu.Unlock()
		if snap.Pending == 0 && snap.InFlight == 0 && snap.Delayed == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// IsFull reports whether the queue has reached its backpressure limit.
func (cq *CommitQueue) IsFull() bool {
	cq.mu.Lock()
	defer cq.mu.Unlock()
	return len(cq.queue) >= cq.maxPending
}

// AbandonLayer halts the commit queue because the mounted layer was rolled
// back. New Enqueue calls return errLayerRolledBack. Queued (not-yet-
// dispatched) entries are canceled so workers remove them; their pending
// state is preserved on disk. In-flight uploads are left to finish — they
// will fail with 409 and fall to onCommitTerminalFailure, which marks them
// staging.PendingConflict. Unlike DrainAll this does NOT block (no wg.Wait()) so the
// caller is not stalled waiting on uploads to a now-dead layer.
func (cq *CommitQueue) AbandonLayer() {
	if cq == nil {
		return
	}
	cq.mu.Lock()
	if cq.stopped {
		// Already stopped (e.g. DrainAll ran first); just mark abandoned so
		// Enqueue returns the rolled-back sentinel instead of the generic
		// "commit queue stopped" message.
		cq.abandoned = true
		cq.mu.Unlock()
		return
	}
	cq.abandoned = true
	cq.stopped = true
	close(cq.workCh)
	// Cancel all queued (not-yet-dispatched) entries so the worker loop
	// removes them; their pending state is preserved on disk.
	for _, entry := range cq.queue {
		if entry != nil {
			entry.canceled = true
		}
	}
	cq.forceAllDelayedLocked()
	cq.queue = nil
	cq.queuedByPath = make(map[string]map[*CommitEntry]struct{})
	cq.mu.Unlock()
	// Do NOT wg.Wait() — let in-flight workers finish their current upload
	// (they will 409 → onCommitTerminalFailure → MarkConflict on their own).
}

// DrainAll stops accepting new entries and waits for all workers to finish.
func (cq *CommitQueue) DrainAll() {
	start := time.Now()
	defer func() {
		if cq.perf != nil {
			cq.perf.commitDrainCount.add(1)
			cq.perf.commitDrainTotalNS.add(uint64(time.Since(start)))
		}
	}()
	cq.mu.Lock()
	if cq.stopped {
		cq.mu.Unlock()
		cq.wg.Wait()
		return
	}
	cq.forceAllDelayedLocked()
	cq.stopped = true
	// Close workCh under the lock so no concurrent Enqueue can send after close.
	close(cq.workCh)
	cq.mu.Unlock()
	cq.wg.Wait()
}

// RecoverPending re-enqueues any locally persisted pending commits on startup.
func (cq *CommitQueue) RecoverPending() {
	if cq.index == nil {
		return
	}
	// Startup only: reconcile cold disk accounting before the first enqueue
	// lets workers mutate or remove any recovered payload.
	if cq.shadows != nil {
		cq.shadows.RecoverPendingBytes()
	}
	for path := range cq.index.ListUncommittedPaths() {
		entry := cq.recoveredEntry(path)
		if entry == nil {
			continue
		}
		if err := cq.Enqueue(entry); err != nil {
			safeLogPrintf("commit queue: recover enqueue failed for %s: %v", path, err)
		}
	}
}

// WaitPath blocks until any in-flight or queued commit for the given path
// completes (including post-commit cleanup). This prevents namespace
// operations from racing with background commits.
func (cq *CommitQueue) WaitPath(path string) {
	for {
		cq.mu.Lock()
		cq.forceDelayedPathLocked(path)
		_, inflight := cq.inFlight[path]
		queued := cq.hasQueuedPathLocked(path)
		immediate := cq.hasImmediatePathLocked(path)
		cq.mu.Unlock()
		if !inflight && !queued && !immediate {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// WaitPathTimeout is like WaitPath but returns false if the path is still
// busy after pollInterval. Returns true immediately if the path is idle.
// Callers should use this in a loop with their own deadline to bound wait time.
func (cq *CommitQueue) WaitPathTimeout(path string, pollInterval time.Duration) bool {
	cq.mu.Lock()
	cq.forceDelayedPathLocked(path)
	_, inflight := cq.inFlight[path]
	queued := cq.hasQueuedPathLocked(path)
	immediate := cq.hasImmediatePathLocked(path)
	cq.mu.Unlock()
	if !inflight && !queued && !immediate {
		return true
	}
	time.Sleep(pollInterval)
	return false
}

// WaitPathDeadline waits until the path is idle, polling at 50ms, and
// returns false only if it is still busy after timeout. WaitPathTimeout
// alone is a single check plus one sleep — a caller passing a 10s interval
// sleeps the whole 10s even when the path lands milliseconds later, so
// busy-path metadata ops (chmod/unlink/rename on just-written files) must
// use this deadline loop instead.
func (cq *CommitQueue) WaitPathDeadline(path string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for !cq.WaitPathTimeout(path, 0) {
		if !time.Now().Before(deadline) {
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
	return true
}

// ClearPathCommitErr retires a remembered terminal failure for a pathname
// after a publication of that name SUCCEEDED outside the queue (an inline
// PUT, the write-cache uploader, a synchronous commit). Without this a
// recreated or re-published file keeps inheriting its predecessor's error.
func (cq *CommitQueue) ClearPathCommitErr(path string) {
	if cq == nil || path == "" {
		return
	}
	cq.mu.Lock()
	delete(cq.terminalFailures, path)
	cq.mu.Unlock()
}

// WaitPathOutcome waits like WaitPathDeadline and then reports whether the
// path's last commit actually succeeded. A path can be idle because its
// entry terminally failed (the queue removed it) — sync-durability callers
// need that distinction.
//
// The failure itself is read from PathTerminalFailure, which also covers the
// durable PendingConflict marker a previous process left behind; a bare
// in-memory map would silently lose that across restarts.
func (cq *CommitQueue) WaitPathOutcome(path string, timeout time.Duration) error {
	if !cq.WaitPathDeadline(path, timeout) {
		return fmt.Errorf("commit for %s still in flight after %s", path, timeout)
	}
	return cq.PathTerminalFailure(path)
}

// HasPath reports whether a path is queued or currently in flight.
func (cq *CommitQueue) HasPath(path string) bool {
	if cq == nil {
		return false
	}
	cq.mu.Lock()
	defer cq.mu.Unlock()
	if _, inflight := cq.inFlight[path]; inflight {
		return true
	}
	return cq.hasQueuedPathLocked(path) || cq.hasImmediatePathLocked(path)
}

// WaitPrefix blocks until all in-flight or queued commits under the given
// prefix complete. Used by Rename to wait for descendant commits.
func (cq *CommitQueue) WaitPrefix(prefix string) {
	for {
		cq.mu.Lock()
		cq.forceDelayedPrefixLocked(prefix)
		found := false
		for p := range cq.inFlight {
			if strings.HasPrefix(p, prefix) {
				found = true
				break
			}
		}
		if !found {
			for _, e := range cq.queue {
				if strings.HasPrefix(e.Path, prefix) {
					found = true
					break
				}
			}
		}
		if !found {
			found = cq.hasImmediatePrefixLocked(prefix)
		}
		cq.mu.Unlock()
		if !found {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// WaitPrefixTimeout waits for all in-flight and queued entries under the
// given prefix to drain, or returns when the timeout elapses. Unlike
// WaitPrefix, it is bounded so callers like Rmdir do not block
// indefinitely if the commit queue is stuck or slow.
func (cq *CommitQueue) WaitPrefixTimeout(prefix string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		cq.mu.Lock()
		cq.forceDelayedPrefixLocked(prefix)
		found := false
		for p := range cq.inFlight {
			if strings.HasPrefix(p, prefix) {
				found = true
				break
			}
		}
		if !found {
			for _, e := range cq.queue {
				if strings.HasPrefix(e.Path, prefix) {
					found = true
					break
				}
			}
		}
		if !found {
			found = cq.hasImmediatePrefixLocked(prefix)
		}
		cq.mu.Unlock()
		if !found {
			return true // drained
		}
		if time.Now().After(deadline) {
			return false // timed out
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// CancelPath marks currently queued or in-flight entries for path as canceled,
// removes queued entries, and cleans up shadow/index state. Cancellation is
// entry-scoped so future files that reuse the same path (for example git's
// config.lock) are not poisoned by an old cancellation. It reports an error
// only when the path's local state could not be made non-replayable.
func (cq *CommitQueue) CancelPath(path string) error {
	return cq.cancelPath(path, false)
}

// CancelPathPreserveLocal cancels queued or in-flight uploads for path without
// removing the local shadow/index state. Unlink uses this when an open fd
// still serves the staged image: the upload must stop, but the bytes survive
// until the last close. The WAL done marker is still journaled (the name is
// gone) so a crash before that close cannot replay the staged frames.
func (cq *CommitQueue) CancelPathPreserveLocal(path string) error {
	return cq.cancelPath(path, true)
}

// CancelEntryInFlight cancels EXACTLY the given entry — the targeted
// counterpart of the pathname-wide CancelPath* family. Used by a staging
// that observed the unlink marking after its enqueue: the name may have
// been recreated by the time this runs, so nothing but this entry may be
// touched (no pathname sweep, no journal fence — the unlink's own cancel
// owns those).
func (cq *CommitQueue) CancelEntryInFlight(entry *CommitEntry) {
	if cq == nil || entry == nil {
		return
	}
	cq.mu.Lock()
	entry.canceled = true
	cancel := entry.cancelCommit
	if cancel == nil {
		// Not picked up by a worker yet: retire it from EVERY waitable
		// set — the queue slice, the immediate set, and the queuedByPath
		// index (WaitPath/WaitPathDeadline consult the index; leaving the
		// membership behind keeps the path busy forever, since a worker
		// that never sees the entry can never repair it).
		cq.stopDelayedLocked(entry)
		for i, e := range cq.queue {
			if e == entry {
				cq.queue = append(cq.queue[:i], cq.queue[i+1:]...)
				break
			}
		}
		delete(cq.immediate, entry)
		cq.removeQueuedLocked(entry)
	}
	cq.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// CancelPathIfInode cancels queued or in-flight uploads for path whose entries
// were staged by the given inode, leaving entries owned by other inodes (e.g.
// a replacement file that reused the pathname after an unlink) untouched.
// Local shadow/index state is always preserved — the caller performs its own
// generation-scoped cleanup.
func (cq *CommitQueue) CancelPathIfInode(path string, ino uint64) {
	if cq == nil || path == "" {
		return
	}
	var cancels []context.CancelFunc
	seen := make(map[*CommitEntry]struct{})
	markCanceled := func(e *CommitEntry) {
		if e == nil || e.Inode != ino {
			return
		}
		e.canceled = true
		cq.stopDelayedLocked(e)
		if _, ok := seen[e]; ok {
			return
		}
		seen[e] = struct{}{}
		if e.cancelCommit != nil {
			cancels = append(cancels, e.cancelCommit)
		}
		if e.cancelUpload != nil {
			cancels = append(cancels, e.cancelUpload)
		}
	}

	cq.mu.Lock()
	// The canceled inode may be the old incarnation of a path that has already
	// been recreated. Never let its successful-commit proof authorize a later
	// growth rebase for that pathname.
	delete(cq.landed, path)
	if e, ok := cq.inFlight[path]; ok {
		markCanceled(e)
	}
	for e := range cq.immediate {
		if e != nil && e.Path == path {
			markCanceled(e)
		}
	}
	if cq.queuedByPath != nil {
		for e := range cq.queuedByPath[path] {
			markCanceled(e)
		}
		if len(seen) > 0 {
			remaining := cq.queue[:0]
			for _, e := range cq.queue {
				if _, drop := seen[e]; drop {
					continue
				}
				remaining = append(remaining, e)
			}
			cq.queue = remaining
			for e := range seen {
				if set, ok := cq.queuedByPath[path]; ok {
					delete(set, e)
					if len(set) == 0 {
						delete(cq.queuedByPath, path)
					}
				}
			}
		}
	} else {
		remaining := cq.queue[:0]
		for _, e := range cq.queue {
			if e.Path == path && e.Inode == ino {
				markCanceled(e)
				continue
			}
			remaining = append(remaining, e)
		}
		cq.queue = remaining
	}
	cq.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
}

// CancelQueuedZeroTruncatePreserveLocal cancels queued, not in-flight,
// zero-byte overwrite entries for path without removing shadow/pending state.
// It returns true only when no in-flight or other queued same-path entry remains.
func (cq *CommitQueue) CancelQueuedZeroTruncatePreserveLocal(path string) bool {
	if cq == nil || path == "" {
		return true
	}
	cq.mu.Lock()
	defer cq.mu.Unlock()
	if cq.inFlight[path] != nil || cq.hasImmediatePathLocked(path) {
		return false
	}
	otherQueued := false
	remaining := cq.queue[:0]
	for _, entry := range cq.queue {
		if entry == nil || entry.Path != path {
			remaining = append(remaining, entry)
			continue
		}
		if isQueuedZeroTruncateEntry(entry) && cq.isDelayedLocked(entry) && !entry.dispatched {
			entry.canceled = true
			cq.stopDelayedLocked(entry)
			continue
		}
		otherQueued = true
		remaining = append(remaining, entry)
	}
	cq.queue = remaining
	if cq.queuedByPath != nil {
		cq.rebuildQueuedIndexLocked()
	}
	return !otherQueued
}

// CancelPrefix marks queued or in-flight entries under prefix as canceled,
// removes queued entries, and cleans up their shadow/index state. Cancellation
// is entry-scoped, so future entries under the same prefix are unaffected.
func (cq *CommitQueue) CancelPrefix(prefix string) {
	cq.mu.Lock()
	for path := range cq.landed {
		if strings.HasPrefix(path, prefix) {
			delete(cq.landed, path)
		}
	}
	var remaining []*CommitEntry
	var cancelled []string
	var cancels []context.CancelFunc
	seen := make(map[*CommitEntry]struct{})
	markCanceled := func(e *CommitEntry) {
		if e == nil {
			return
		}
		e.canceled = true
		cq.stopDelayedLocked(e)
		if _, ok := seen[e]; ok {
			return
		}
		seen[e] = struct{}{}
		if e.cancelCommit != nil {
			cancels = append(cancels, e.cancelCommit)
		}
		if e.cancelUpload != nil {
			cancels = append(cancels, e.cancelUpload)
		}
	}
	for p, e := range cq.inFlight {
		if strings.HasPrefix(p, prefix) {
			markCanceled(e)
			cancelled = append(cancelled, p)
		}
	}
	for e := range cq.immediate {
		if e != nil && strings.HasPrefix(e.Path, prefix) {
			markCanceled(e)
			cancelled = append(cancelled, e.Path)
		}
	}
	for _, e := range cq.queue {
		if strings.HasPrefix(e.Path, prefix) {
			markCanceled(e)
			cancelled = append(cancelled, e.Path)
		} else {
			remaining = append(remaining, e)
		}
	}
	cq.queue = remaining
	if cq.queuedByPath != nil {
		cq.rebuildQueuedIndexLocked()
	}
	cq.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
	for _, p := range cancelled {
		if cq.shadows != nil {
			cq.shadows.Remove(p)
		}
		if cq.index != nil {
			cq.index.Remove(p)
		}
		// Same pathname-incarnation rule as cancelPath: the subtree's names
		// are gone, so their terminal failures must not poison a recreator.
		cq.clearTerminalFailure(p)
		if cq.journal != nil {
			if err := cq.journal.Append(staging.JournalEntry{
				Op:   staging.JournalCommit,
				Path: p,
			}); err != nil {
				safeLogPrintf("commit queue: journal cancel marker failed for %s: %v", p, err)
			}
		}
	}
}

// RecoverPendingSync synchronously commits every recoverable pending entry,
// for use after the async queue has already been drained and stopped at
// unmount (issue #964): late FUSE Release arrivals and drain timeouts leave
// staged entries behind, and this path uploads them without needing live
// workers. Returns the number of successfully committed entries.
func (cq *CommitQueue) RecoverPendingSync(ctx context.Context) int {
	if cq == nil || cq.index == nil {
		return 0
	}
	committed := 0
	for path := range cq.index.ListUncommittedPaths() {
		if ctx.Err() != nil {
			break
		}
		entry := cq.recoveredEntry(path)
		if entry == nil {
			continue
		}
		if err := cq.CommitNow(ctx, entry); err != nil {
			safeLogPrintf("commit queue: late sync commit failed for %s: %v", path, err)
			continue
		}
		committed++
	}
	return committed
}

// CommitNow uploads an entry synchronously through the same commit path used
// by workers. It is used as a fallback when the async queue rejects an entry
// after local state has already moved to the final path.
func (cq *CommitQueue) CommitNow(ctx context.Context, entry *CommitEntry) error {
	if err := cq.rejectAbandonedLayer(); err != nil {
		return err
	}
	release, discard, err := cq.beginImmediateMutationCommit(ctx, entry, false)
	if err != nil {
		return err
	}
	if discard {
		cq.discardEntry(entry)
		return nil
	}
	if release != nil {
		defer release()
	}
	unlockPath := cq.lockPath(entry.Path)
	defer unlockPath()
	return cq.commitNowClaimedPathLocked(ctx, entry)
}

func isQueuedZeroTruncateEntry(entry *CommitEntry) bool {
	return entry != nil && entry.Kind == staging.PendingOverwrite && entry.Size == 0
}

func isBatchWriteUnsupported(err error) bool {
	var statusErr *client.StatusError
	if !errors.As(err, &statusErr) {
		return false
	}
	return statusErr.StatusCode == http.StatusNotFound || statusErr.StatusCode == http.StatusMethodNotAllowed
}

func batchWriteResultError(result client.BatchWriteResult) error {
	return &client.StatusError{StatusCode: result.Status, Message: result.Error}
}

func sleepWithCancel(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func committedRevisionForExpectedRevision(expectedRevision, committedRev int64) int64 {
	if committedRev > 0 {
		return committedRev
	}
	if revision, ok := committedRevisionFromExpectedRevision(expectedRevision); ok {
		return revision
	}
	return 0
}

func classifyCommitTerminalReason(lastErr error) string {
	switch {
	case lastErr == nil:
		return "conflict"
	case errors.Is(lastErr, errLayerRolledBack):
		return "abandoned"
	case errors.Is(lastErr, client.ErrConflict), errors.Is(lastErr, errCommitPayloadStale):
		return "conflict"
	default:
		return "upload_failure"
	}
}

func (cq *CommitQueue) layerRefSnapshot() string {
	if cq == nil {
		return ""
	}
	cq.mu.Lock()
	defer cq.mu.Unlock()
	return cq.layerRef
}

func (cq *CommitQueue) remotePath(localPath string) string {
	root := "/"
	if cq != nil && cq.remoteRoot != "" {
		root = cq.remoteRoot
	}
	return mountpath.ToRemote(root, localPath)
}

func (cq *CommitQueue) lockPath(path string) func() {
	if cq != nil && cq.PathLock != nil {
		return cq.PathLock(path)
	}
	return func() {}
}

func (cq *CommitQueue) shouldDelayZeroTruncateLocked(entry *CommitEntry) bool {
	if cq == nil || entry == nil || entry.canceled || !entry.CoalesceZeroTruncate || !isQueuedZeroTruncateEntry(entry) {
		return false
	}
	return cq.zeroTruncateDelay > 0
}

func (cq *CommitQueue) isDelayedLocked(entry *CommitEntry) bool {
	if cq == nil || entry == nil || cq.delayed == nil {
		return false
	}
	return cq.delayed[entry] != nil
}

func (cq *CommitQueue) delayZeroTruncateLocked(entry *CommitEntry) {
	if cq.delayed == nil {
		cq.delayed = make(map[*CommitEntry]*time.Timer)
	}
	delay := cq.zeroTruncateDelay
	cq.delayed[entry] = time.AfterFunc(delay, func() {
		cq.dispatchDelayed(entry)
	})
}

func (cq *CommitQueue) dispatchDelayed(entry *CommitEntry) {
	if cq == nil || entry == nil {
		return
	}
	cq.mu.Lock()
	defer cq.mu.Unlock()
	if cq.delayed == nil || cq.delayed[entry] == nil {
		return
	}
	delete(cq.delayed, entry)
	if cq.stopped || entry.canceled {
		return
	}
	entry.dispatched = true
	cq.workCh <- entry
}

func (cq *CommitQueue) stopDelayedLocked(entry *CommitEntry) {
	if cq == nil || entry == nil || cq.delayed == nil {
		return
	}
	if timer := cq.delayed[entry]; timer != nil {
		timer.Stop()
		delete(cq.delayed, entry)
	}
}

func (cq *CommitQueue) forceDelayedPathLocked(path string) {
	if cq == nil || path == "" || len(cq.delayed) == 0 {
		return
	}
	for entry, timer := range cq.delayed {
		if entry == nil || entry.Path != path {
			continue
		}
		timer.Stop()
		delete(cq.delayed, entry)
		if !cq.stopped && !entry.canceled {
			entry.dispatched = true
			cq.workCh <- entry
		}
	}
}

func (cq *CommitQueue) forceDelayedPrefixLocked(prefix string) {
	if cq == nil || prefix == "" || len(cq.delayed) == 0 {
		return
	}
	for entry, timer := range cq.delayed {
		if entry == nil || !strings.HasPrefix(entry.Path, prefix) {
			continue
		}
		timer.Stop()
		delete(cq.delayed, entry)
		if !cq.stopped && !entry.canceled {
			entry.dispatched = true
			cq.workCh <- entry
		}
	}
}

func (cq *CommitQueue) forceAllDelayedLocked() {
	if cq == nil || len(cq.delayed) == 0 {
		return
	}
	for entry, timer := range cq.delayed {
		timer.Stop()
		delete(cq.delayed, entry)
		if !cq.stopped && entry != nil && !entry.canceled {
			entry.dispatched = true
			cq.workCh <- entry
		}
	}
}

func (cq *CommitQueue) snapshotLocked() CommitQueueSnapshot {
	var snap CommitQueueSnapshot
	seen := make(map[*CommitEntry]struct{}, len(cq.queue))
	for _, e := range cq.queue {
		if e == nil {
			continue
		}
		if snap.FirstPath == "" {
			snap.FirstPath = e.Path
		}
		if _, ok := seen[e]; ok {
			continue
		}
		seen[e] = struct{}{}
		snap.Pending++
		snap.Bytes += e.Size
	}
	for _, e := range cq.inFlight {
		if e == nil {
			continue
		}
		if snap.FirstPath == "" {
			snap.FirstPath = e.Path
		}
		if _, ok := seen[e]; ok {
			continue
		}
		seen[e] = struct{}{}
		snap.Pending++
		snap.Bytes += e.Size
	}
	for e := range cq.immediate {
		if e == nil {
			continue
		}
		if snap.FirstPath == "" {
			snap.FirstPath = e.Path
		}
		if _, ok := seen[e]; ok {
			continue
		}
		seen[e] = struct{}{}
		snap.Pending++
		snap.Bytes += e.Size
	}
	for e := range cq.delayed {
		if e != nil && snap.FirstPath == "" {
			snap.FirstPath = e.Path
		}
	}
	snap.InFlight = len(cq.inFlight) + len(cq.immediate)
	snap.Delayed = len(cq.delayed)
	if cq.index != nil {
		var firstConflictPath string
		snap.Conflicts, snap.ConflictBytes, firstConflictPath = cq.index.ConflictSummary()
		if snap.FirstPath == "" {
			snap.FirstPath = firstConflictPath
		}
	}
	return snap
}

// recoveredEntry classifies and fences a recovered publication identically
// for startup and late shutdown recovery. Unusable bytes become conflicts.
func (cq *CommitQueue) recoveredEntry(path string) *CommitEntry {
	meta, ok := cq.index.GetMeta(path)
	if !ok || meta.LayerClean {
		return nil
	}
	if meta.Kind == staging.PendingConflict {
		// A conflict marker is preserved even without a shadow: the
		// transfer transaction stages one at the destination deliberately
		// (a crash mid-move must leave the renamed name refusing to certify
		// the image that never landed, with the bytes preserved under the
		// old name) — pruning it as "orphaned" would erase the only durable
		// witness. Entries WITH a shadow are the ordinary preserved-for-
		// manual-recovery case.
		safeLogPrintf("commit queue: skipping conflicted entry for %s (preserved for manual recovery)", path)
		return nil
	}
	if cq.shadows != nil && !cq.shadows.Has(path) {
		// Shadow file missing — prune orphaned pending index entry so
		// Lookup/GetAttr don't serve stale metadata.
		safeLogPrintf("commit queue: pruning orphaned pending entry for %s (shadow missing)", path)
		cq.index.RemoveIfGeneration(path, meta.Generation)
		return nil
	}
	shadowGen := cq.index.RecoverShadowSource(path, meta.Generation, cq.shadows)
	if meta.Kind == staging.PendingOverwrite && meta.BaseRev <= 0 && cq.layerRefSnapshot() == "" {
		safeLogPrintf("commit queue: skip legacy pending overwrite without base revision for %s", path)
		return nil
	}
	// The shadow file is the data actually uploaded; its size is
	// authoritative. Meta size can be stale (memory-only updates, or a
	// WAL-resurrected entry from an older fsync) and would mis-route the
	// direct-PUT vs multipart decision in uploadEntry.
	size := meta.Size
	if cq.shadows != nil {
		if shadowSize := cq.shadows.Size(path); shadowSize >= 0 {
			size = shadowSize
		}
	}
	entry := &CommitEntry{
		Path:              path,
		BaseRev:           meta.BaseRev,
		Size:              size,
		Kind:              meta.Kind,
		ShadowSpill:       meta.ShadowSpill,
		Mode:              meta.Mode,
		HasMode:           meta.HasMode,
		PayloadBaseRev:    meta.BaseRev,
		PayloadBaseRevSet: true,
		PendingIndexGen:   meta.Generation,
	}
	if cq.shadows != nil {
		entry.ShadowGen = shadowGen
		if entry.ShadowGen == 0 {
			safeLogPrintf("commit queue: skipping recovered pending entry for %s (shadow missing, short, or metadata replaced)", path)
			if _, err := cq.index.MarkConflictIfGeneration(path, meta.Generation); err != nil {
				safeLogPrintf("commit queue: mark recovered pending conflict failed for %s: %v", path, err)
			}
			return nil
		}
	}
	entry.recovered = true
	return entry
}

func (cq *CommitQueue) hasImmediatePathLocked(path string) bool {
	for entry := range cq.immediate {
		if entry != nil && entry.Path == path {
			return true
		}
	}
	return false
}

func (cq *CommitQueue) hasImmediatePrefixLocked(prefix string) bool {
	for entry := range cq.immediate {
		if entry != nil && strings.HasPrefix(entry.Path, prefix) {
			return true
		}
	}
	return false
}

// cancelPathEntries marks every queued, immediate, or in-flight entry for
// path canceled, drops the queued ones, and returns their cancel funcs for
// the caller to fire outside cq.mu.
func (cq *CommitQueue) cancelPathEntries(path string) []context.CancelFunc {
	var cancels []context.CancelFunc
	seen := make(map[*CommitEntry]struct{})
	markCanceled := func(e *CommitEntry) {
		if e == nil {
			return
		}
		e.canceled = true
		cq.stopDelayedLocked(e)
		if _, ok := seen[e]; ok {
			return
		}
		seen[e] = struct{}{}
		if e.cancelCommit != nil {
			cancels = append(cancels, e.cancelCommit)
		}
		if e.cancelUpload != nil {
			cancels = append(cancels, e.cancelUpload)
		}
	}

	cq.mu.Lock()
	defer cq.mu.Unlock()
	delete(cq.landed, path)
	// The remembered failure belongs to the PUBLICATION that failed, not to
	// the pathname: a cancel (unlink/rename/delete) retires it, or a
	// recreated file would inherit its predecessor's EIO on every Sync until
	// an unrelated queue success happened to clear it.
	if e, ok := cq.inFlight[path]; ok {
		markCanceled(e)
	}
	for e := range cq.immediate {
		if e != nil && e.Path == path {
			markCanceled(e)
		}
	}
	if cq.queuedByPath != nil {
		for e := range cq.queuedByPath[path] {
			markCanceled(e)
		}
	}
	if cq.queuedByPath == nil {
		remaining := cq.queue[:0]
		for _, e := range cq.queue {
			if e.Path == path {
				markCanceled(e)
				continue
			}
			remaining = append(remaining, e)
		}
		cq.queue = remaining
	} else if len(cq.queuedByPath[path]) > 0 {
		remaining := cq.queue[:0]
		for _, e := range cq.queue {
			if e.Path != path {
				remaining = append(remaining, e)
			}
		}
		cq.queue = remaining
		delete(cq.queuedByPath, path)
	}
	return cancels
}

// cancelPath is the shared Unlink/Rmdir cancel. It returns an error when the
// path's local state provably stays replayable — the marker could not be
// journaled AND the fallback cleanup could not remove the recovery source —
// so the caller must not acknowledge a successful unlink on the assumption
// that the void removal APIs below did their job. A successful cancel also
// drops the path's recorded terminal failure: the pathname incarnation ended,
// and a later reuse of the name (a renamed-in file) must not inherit an EIO
// for data the recreator never wrote. The failure-free error return keeps the
// record — the name survived, and so did its unlanded data.
func (cq *CommitQueue) cancelPath(path string, preserveLocal bool) error {
	// First pass without the path lock: an in-flight upload holds that lock
	// for its whole transfer, and aborting it here is what releases it.
	for _, cancel := range cq.cancelPathEntries(path) {
		cancel()
	}
	// Second pass under the same-path lock the staging path holds across
	// its shadow/pending publication and Enqueue: an entry that slipped in
	// between the two passes is canceled too, and the marker below lands at
	// a higher Seq than every frame published before it. A staging that
	// takes the lock after this returns sees the handle's unlink marking and
	// publishes nothing (stageAndEnqueueOpt).
	unlock := cq.lockPath(path)
	defer unlock()
	for _, cancel := range cq.cancelPathEntries(path) {
		cancel()
	}
	hadLocal := false
	if cq.shadows != nil && cq.shadows.Has(path) {
		hadLocal = true
		if !preserveLocal {
			cq.shadows.Remove(path)
		}
	}
	// The pending entry dies only once the recovery source is CONFIRMED
	// gone: it is the overlay lookups serve from, and a failed unlink must
	// leave the fsynced publication discoverable and retryable.
	pendingHad := false
	if cq.index != nil && cq.index.HasPending(path) {
		hadLocal = true
		pendingHad = true
	}
	dropPending := func() {
		if !preserveLocal && pendingHad && cq.index != nil {
			cq.index.Remove(path)
		}
	}
	// staging.Journal a "done" marker so crash recovery does not resurrect the
	// unlinked path from earlier fsync frames, and so those frames compact.
	// Only when local pending state existed, to avoid a WAL frame per unlink.
	// A preserve-local cancel journals it too: the in-memory shadow/index stay
	// for the anonymous fd's reads, but the name is gone. The facade's
	// unlink-of-open-file path defers the non-preserving cancel to the last
	// close plus a remote HEAD, and a crash inside that window replayed the
	// fsync'd frames and re-committed the unlinked file (#1065).
	// No WAL (or nothing staged): without frames there is nothing to fence,
	// and a journal-less queue keeps the preserve-local contract verbatim.
	if cq.journal == nil || !hadLocal {
		dropPending()
		cq.clearTerminalFailure(path)
		return nil
	}
	appendErr := cq.journal.Append(staging.JournalEntry{
		Op:   staging.JournalUnlink,
		Path: path,
	})
	if appendErr == nil {
		// The marker fences every frame published before it — the path
		// cannot replay even if the removal above silently failed.
		dropPending()
		cq.clearTerminalFailure(path)
		return nil
	}
	safeLogPrintf("commit queue: journal cancel marker failed for %s: %v", path, appendErr)
	if preserveLocal {
		// The retained shadow/index are safe only while the marker fences
		// their WAL frames: without it a crash replays them. Fall back to
		// the non-preserving cleanup below.
		if cq.shadows != nil {
			cq.shadows.Remove(path)
		}
	}
	// ShadowStore.Remove and PendingIndex.Remove discard their disk errors,
	// so verify with evidence instead of assuming: a read-only or failing
	// cache filesystem can reject the marker AND leave the durable shadow
	// behind, and replay would then upload the deleted name after a restart.
	if cq.shadows == nil || cq.shadows.VerifyRemoved(path) == nil {
		// Confirmed gone — finish the degrade-to-non-preserving cleanup
		// (the anonymous fd loses its staged image on a failing WAL, but
		// the unlink cannot resurrect).
		if pendingHad && cq.index != nil {
			cq.index.Remove(path)
		}
		cq.clearTerminalFailure(path)
		return nil
	}
	// The recovery source survived or its absence cannot be proven. Keep the
	// pending entry (the overlay stays discoverable), re-drive the upload
	// when the shadow is still stat-able so the fsynced publication is
	// retryable without a restart, and report the unlink as failed — the
	// caller must not acknowledge a deletion a restart would undo.
	if cq.shadows.Has(path) {
		if entry := cq.recoveredEntry(path); entry != nil {
			if enqErr := cq.Enqueue(entry); enqErr != nil {
				safeLogPrintf("commit queue: re-driving %s after the failed unlink rollback failed: %v", path, enqErr)
			}
		}
	}
	return fmt.Errorf("commit queue: unlink of %s is not crash-durable: WAL marker failed (%v) and the staged image could not be confirmed removed", path, appendErr)
}

// isEntryCanceled checks whether this specific entry was canceled by
// Unlink/Rmdir. It intentionally does not key by path; git repeatedly reuses
// config.lock, and an old cancellation must not affect a newer entry.
func (cq *CommitQueue) isEntryCanceled(entry *CommitEntry) bool {
	cq.mu.Lock()
	defer cq.mu.Unlock()
	if entry == nil {
		return false
	}
	return entry.canceled
}

func (cq *CommitQueue) isCanceledImmediateEntry(entry *CommitEntry) bool {
	if cq == nil || entry == nil {
		return false
	}
	cq.mu.Lock()
	defer cq.mu.Unlock()
	_, immediate := cq.immediate[entry]
	return immediate && entry.canceled
}

func (cq *CommitQueue) discardSupersededEntry(entry *CommitEntry) bool {
	if cq == nil || entry == nil || cq.IsSuperseded == nil || !cq.IsSuperseded(entry) {
		return false
	}
	cq.discardEntry(entry)
	return true
}

func (cq *CommitQueue) discardEntry(entry *CommitEntry) {
	if cq == nil || entry == nil {
		return
	}
	if cq.index != nil && entry.PendingIndexGen != 0 {
		cq.index.RemoveIfGeneration(entry.Path, entry.PendingIndexGen)
	}
	if cq.shadows != nil && entry.ShadowGen != 0 {
		cq.shadows.RemoveIfGeneration(entry.Path, entry.ShadowGen)
	}
	cq.removeFromQueue(entry)
	if cq.OnDiscard != nil {
		cq.OnDiscard(entry)
	}
	safeLogPrintf("commit queue: discarded superseded entry for %s (inode=%d seq=%d)", entry.Path, entry.Inode, entry.MutationSeq)
}

func (cq *CommitQueue) worker() {
	defer cq.wg.Done()
	for entry := range cq.workCh {
		cq.processEntry(entry)
	}
}

func (cq *CommitQueue) processEntry(entry *CommitEntry) {
	if cq.isEntryCanceled(entry) {
		cq.removeFromQueue(entry)
		safeLogPrintf("commit queue: skipping canceled entry for %s", entry.Path)
		return
	}
	if !cq.beginInFlight(entry) {
		cq.removeFromQueue(entry)
		safeLogPrintf("commit queue: entry for %s was canceled before in-flight", entry.Path)
		return
	}
	handled, deferred := cq.tryCommitBatch(entry)
	if !handled {
		cq.commitOne(entry)
		cq.endInFlight(entry)
	}
	for _, next := range deferred {
		cq.processEntry(next)
	}
}

func (cq *CommitQueue) beginInFlight(entry *CommitEntry) bool {
	if cq == nil || entry == nil {
		return false
	}
	for {
		cq.mu.Lock()
		if entry.canceled {
			cq.mu.Unlock()
			return false
		}
		if cq.inFlight == nil {
			cq.inFlight = make(map[string]*CommitEntry)
		}
		if cq.canBeginInFlightLocked(entry) {
			cq.inFlight[entry.Path] = entry
			cq.mu.Unlock()
			return true
		}
		cq.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

func (cq *CommitQueue) tryBeginInFlight(entry *CommitEntry) bool {
	if cq == nil || entry == nil {
		return false
	}
	cq.mu.Lock()
	defer cq.mu.Unlock()
	if entry.canceled {
		return false
	}
	if cq.inFlight == nil {
		cq.inFlight = make(map[string]*CommitEntry)
	}
	if !cq.canBeginInFlightLocked(entry) {
		return false
	}
	cq.inFlight[entry.Path] = entry
	return true
}

func (cq *CommitQueue) canBeginInFlightLocked(entry *CommitEntry) bool {
	if cq == nil || entry == nil || cq.inFlight[entry.Path] != nil {
		return false
	}
	if oldest := cq.oldestQueuedForPathLocked(entry.Path); oldest != nil && oldest != entry {
		return false
	}
	if entry.Inode == 0 || entry.MutationSeq == 0 {
		return true
	}
	for _, active := range cq.inFlight {
		if active != nil && active != entry && active.Inode == entry.Inode && active.MutationSeq != 0 {
			return false
		}
	}
	for active := range cq.immediate {
		if active != nil && !active.canceled && active != entry && active.Inode == entry.Inode && active.MutationSeq != 0 {
			return false
		}
	}
	for _, queued := range cq.queue {
		if queued == nil || queued.canceled || queued.Inode != entry.Inode || queued.MutationSeq == 0 {
			continue
		}
		return queued == entry
	}
	return true
}

func (cq *CommitQueue) oldestQueuedForPathLocked(path string) *CommitEntry {
	if cq == nil || path == "" {
		return nil
	}
	for _, queued := range cq.queue {
		if queued == nil || queued.Path != path || queued.canceled {
			continue
		}
		return queued
	}
	return nil
}

func (cq *CommitQueue) endInFlight(entry *CommitEntry) {
	if cq == nil || entry == nil {
		return
	}
	cq.mu.Lock()
	if cq.inFlight[entry.Path] == entry {
		delete(cq.inFlight, entry.Path)
	}
	cq.pruneLandedLocked()
	cq.mu.Unlock()
}

// commitOne uploads a single entry to the server with exponential backoff.
func (cq *CommitQueue) commitOne(entry *CommitEntry) {
	const maxRetries = 5
	const baseDelay = 200 * time.Millisecond
	const maxDelay = 30 * time.Second

	entryCtx, entryCancel := context.WithCancel(context.Background())
	cq.mu.Lock()
	if entry.canceled {
		cq.mu.Unlock()
		entryCancel()
		cq.removeFromQueue(entry)
		safeLogPrintf("commit queue: entry for %s was canceled before retry loop", entry.Path)
		return
	}
	entry.cancelCommit = entryCancel
	cq.mu.Unlock()
	defer func() {
		cq.mu.Lock()
		entry.cancelCommit = nil
		entry.cancelUpload = nil
		cq.mu.Unlock()
		entryCancel()
	}()

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Re-check cancelation between retries.
		if cq.isEntryCanceled(entry) {
			cq.removeFromQueue(entry)
			safeLogPrintf("commit queue: entry for %s was canceled during retry", entry.Path)
			return
		}

		if attempt > 0 {
			if cq.perf != nil {
				cq.perf.commitRetry.add(1)
			}
			delay := time.Duration(float64(baseDelay) * math.Pow(2, float64(attempt-1)))
			if delay > maxDelay {
				delay = maxDelay
			}
			if !sleepWithCancel(entryCtx, delay) {
				cq.removeFromQueue(entry)
				safeLogPrintf("commit queue: entry for %s was canceled during retry backoff", entry.Path)
				return
			}
		}

		timeout := uploadTimeout
		if entry.ShadowSpill {
			timeout = releaseTimeout(entry.Size)
		}
		ctx, cancel := context.WithTimeout(entryCtx, timeout)
		cq.mu.Lock()
		if entry.canceled {
			cq.mu.Unlock()
			cancel()
			cq.removeFromQueue(entry)
			safeLogPrintf("commit queue: entry for %s was canceled before upload", entry.Path)
			return
		}
		entry.cancelUpload = cancel
		cq.mu.Unlock()

		unlockPath := cq.lockPath(entry.Path)
		if cq.discardSupersededEntry(entry) || cq.discardLandedAncestor(entryCtx, entry) {
			unlockPath()
			return
		}
		committedRev, err := cq.uploadEntry(ctx, entry)
		cq.mu.Lock()
		entry.cancelUpload = nil
		cq.mu.Unlock()
		cancel()

		if err == nil {
			// Check if the entry was canceled while the upload was in flight.
			// If canceled, skip onCommitSuccess entirely — its path-scoped
			// cleanup (shadowStore.Remove() + pendingIndex.Remove()) would delete
			// a newer writer's staged data. The upload already succeeded on the
			// backend, so we just clean up the queue entry without touching
			// local shadow/pending state.
			if cq.isEntryCanceled(entry) {
				unlockPath()
				cq.removeFromQueue(entry)
				safeLogPrintf("commit queue: entry for %s was canceled after upload succeeded, skipping cleanup to preserve newer staged data", entry.Path)
				return
			}
			if err := cq.onCommitSuccess(entry, entry.BaseRev, committedRev); err == nil {
				unlockPath()
				return
			} else {
				unlockPath()
				safeLogPrintf("commit queue: post-upload attempt %d/%d failed for %s: %v", attempt+1, maxRetries, entry.Path, err)
				cq.onCommitPostUploadFailure(entry, err)
				return
			}
		}
		if cq.isEntryCanceled(entry) {
			unlockPath()
			cq.removeFromQueue(entry)
			safeLogPrintf("commit queue: entry for %s was canceled during upload", entry.Path)
			return
		}
		if errors.Is(err, errCommitPayloadStale) {
			// The resolver holds the path lock (taken above and kept through
			// this point), so a synchronous same-path writer cannot commit
			// newer bytes between its equality proof and its bookkeeping.
			if resolved, _ := cq.resolveStalePayloadAsIdempotent(entryCtx, entry); resolved {
				unlockPath()
				return
			}
			safeLogPrintf("commit queue: stale payload rejected for %s: %v", entry.Path, err)
			cq.onCommitTerminalFailure(entry, err)
			unlockPath()
			return
		}
		if errors.Is(err, client.ErrConflict) {
			// When the layer was rolled back, the 409 is a state conflict
			// (not a revision conflict); skip the Stat/HEAD auto-resolve
			// round-trip and go straight to terminal failure so the entry
			// is preserved as staging.PendingConflict.
			cq.mu.Lock()
			abandoned := cq.abandoned
			cq.mu.Unlock()
			if abandoned {
				safeLogPrintf("commit queue: layer abandoned, terminal failure for %s", entry.Path)
				cq.onCommitTerminalFailure(entry, fmt.Errorf("%w: %v", errLayerRolledBack, err))
				unlockPath()
				return
			}
			if entry.DisableAutoResolveLWW {
				safeLogPrintf("commit queue: fenced conflict for %s at base revision %d, keeping terminal", entry.Path, entry.BaseRev)
				cq.onCommitTerminalFailure(entry, err)
				unlockPath()
				return
			}
			if err := cq.validateEntryPayloadFreshCtx(entryCtx, entry); err != nil {
				if cq.isEntryCanceled(entry) {
					cq.removeFromQueue(entry)
					unlockPath()
					return
				}
				safeLogPrintf("commit queue: conflict auto-resolve rejected stale payload for %s: %v", entry.Path, err)
				cq.onCommitTerminalFailure(entry, err)
				unlockPath()
				return
			}
			safeLogPrintf("commit queue: conflict committing %s at base revision %d, attempting auto-resolve", entry.Path, entry.BaseRev)
			cq.tryAutoResolveConflict(entryCtx, entry)
			unlockPath()
			return
		}
		unlockPath()
		lastErr = err
		safeLogPrintf("commit queue: upload attempt %d/%d failed for %s: %v", attempt+1, maxRetries, entry.Path, err)
	}

	safeLogPrintf("commit queue: giving up on %s after %d retries: %v", entry.Path, maxRetries, lastErr)
	if errors.Is(lastErr, errCommitPostUpload) {
		cq.onCommitPostUploadFailure(entry, lastErr)
		return
	}
	cq.onCommitTerminalFailure(entry, lastErr)
}

func (cq *CommitQueue) batchConfigSnapshot() commitBatchConfig {
	if cq == nil {
		return commitBatchConfig{}
	}
	cq.mu.Lock()
	defer cq.mu.Unlock()
	return commitBatchConfig{
		window:   cq.batchWindow,
		maxItems: cq.batchMaxItems,
		maxBytes: cq.batchMaxBytes,
	}
}

func (cq *CommitQueue) tryCommitBatch(first *CommitEntry) (bool, []*CommitEntry) {
	cfg := cq.batchConfigSnapshot()
	if cfg.window <= 0 || cfg.maxItems <= 1 || cfg.maxBytes <= 0 || !cq.batchWriteEligible(first) {
		return false, nil
	}
	entries, deferred := cq.collectBatchEntries(first, cfg)
	if len(entries) < 2 {
		return false, deferred
	}
	cq.commitBatch(entries)
	return true, deferred
}

func (cq *CommitQueue) collectBatchEntries(first *CommitEntry, cfg commitBatchConfig) ([]*CommitEntry, []*CommitEntry) {
	entries := []*CommitEntry{first}
	deferred := []*CommitEntry{}
	seenPaths := map[string]struct{}{first.Path: {}}
	totalBytes := first.Size
	timer := time.NewTimer(cfg.window)
	defer timer.Stop()
	for len(entries) < cfg.maxItems && totalBytes < cfg.maxBytes {
		select {
		case <-timer.C:
			return entries, deferred
		case candidate, ok := <-cq.workCh:
			if !ok {
				return entries, deferred
			}
			if cq.isEntryCanceled(candidate) {
				cq.removeFromQueue(candidate)
				safeLogPrintf("commit queue: skipping canceled entry for %s", candidate.Path)
				continue
			}
			if !cq.canAddToBatch(candidate, seenPaths, totalBytes, cfg) {
				deferred = append(deferred, candidate)
				continue
			}
			if !cq.tryBeginInFlight(candidate) {
				deferred = append(deferred, candidate)
				continue
			}
			entries = append(entries, candidate)
			seenPaths[candidate.Path] = struct{}{}
			totalBytes += candidate.Size
		}
	}
	return entries, deferred
}

func (cq *CommitQueue) canAddToBatch(entry *CommitEntry, seenPaths map[string]struct{}, totalBytes int64, cfg commitBatchConfig) bool {
	if !cq.batchWriteEligible(entry) {
		return false
	}
	if _, ok := seenPaths[entry.Path]; ok {
		return false
	}
	return totalBytes+entry.Size <= cfg.maxBytes
}

func (cq *CommitQueue) batchWriteEligible(entry *CommitEntry) bool {
	if cq == nil || entry == nil || entry.canceled || entry.ShadowSpill || cq.shadows == nil || cq.client == nil {
		return false
	}
	if cq.layerRefSnapshot() != "" {
		return false
	}
	if entry.Kind == staging.PendingOverwrite && entry.BaseRev <= 0 {
		return false
	}
	if entry.Kind != staging.PendingNew && entry.Kind != staging.PendingOverwrite {
		return false
	}
	threshold := cq.directPutThreshold()
	return entry.Size == 0 || (threshold > 0 && entry.Size < threshold)
}

func (cq *CommitQueue) commitBatch(entries []*CommitEntry) {
	allEntries := append([]*CommitEntry(nil), entries...)
	entryCtx, entryCancel := context.WithCancel(context.Background())
	cq.mu.Lock()
	for _, entry := range allEntries {
		entry.cancelCommit = entryCancel
	}
	cq.mu.Unlock()
	defer func() {
		cq.mu.Lock()
		for _, entry := range allEntries {
			entry.cancelCommit = nil
			entry.cancelUpload = nil
		}
		cq.mu.Unlock()
		entryCancel()
	}()

	timeout := uploadTimeout
	for _, entry := range entries {
		if entry.ShadowSpill {
			timeout = releaseTimeout(entry.Size)
			break
		}
	}
	ctx, cancel := context.WithTimeout(entryCtx, timeout)
	cq.mu.Lock()
	for _, entry := range entries {
		if entry.canceled {
			continue
		}
		entry.cancelUpload = cancel
	}
	cq.mu.Unlock()
	unlockPaths := cq.lockBatchPaths(entries)
	active := entries[:0]
	for _, entry := range entries {
		if cq.discardSupersededEntry(entry) || cq.discardLandedAncestor(ctx, entry) {
			cq.endInFlight(entry)
			continue
		}
		active = append(active, entry)
	}
	entries = active
	if len(entries) == 0 {
		cancel()
		unlockPaths()
		return
	}
	if len(entries) == 1 {
		cancel()
		unlockPaths()
		cq.fallbackBatchEntries(entries)
		return
	}

	remoteItems, err := cq.prepareBatchWriteItems(ctx, entries)
	if err != nil {
		unlockPaths()
		cancel()
		if errors.Is(err, errCommitPayloadStale) {
			for _, entry := range entries {
				if entry == nil {
					continue
				}
				verr := cq.validateEntryPayloadFreshCtx(entryCtx, entry)
				if cq.isEntryCanceled(entry) {
					cq.removeFromQueue(entry)
					cq.endInFlight(entry)
					continue
				}
				if errors.Is(verr, errCommitPayloadStale) {
					// The batch released its path locks before this point, so
					// reacquire this entry's lock: the resolver's equality
					// proof and its success bookkeeping must not straddle a
					// synchronous same-path commit.
					unlockEntry := cq.lockPath(entry.Path)
					resolved, _ := cq.resolveStalePayloadAsIdempotent(entryCtx, entry)
					unlockEntry()
					if resolved {
						cq.endInFlight(entry)
						continue
					}
					safeLogPrintf("commit queue: stale batched payload rejected for %s: %v", entry.Path, verr)
					cq.onCommitTerminalFailure(entry, verr)
					cq.endInFlight(entry)
					continue
				}
				cq.fallbackBatchEntries([]*CommitEntry{entry})
			}
			return
		}
		cq.fallbackBatchEntries(entries)
		return
	}

	start := time.Now()
	results, err := cq.client.BatchWriteCtx(ctx, remoteItems)
	if cq.perf != nil {
		var bytes uint64
		for _, item := range remoteItems {
			bytes += uint64(len(item.Data))
		}
		cq.perf.recordRemoteOp(perfRemoteWrite, err, time.Since(start), bytes)
	}
	cq.mu.Lock()
	for _, entry := range entries {
		entry.cancelUpload = nil
	}
	cq.mu.Unlock()
	cancel()

	if err != nil {
		unlockPaths()
		if isBatchWriteUnsupported(err) {
			cq.ConfigureBatchWrite(0, 0, 0)
		}
		safeLogPrintf("commit queue: batch write failed for %d entries, falling back to single commits: %v", len(entries), err)
		cq.fallbackBatchEntries(entries)
		return
	}
	if len(results) != len(entries) {
		unlockPaths()
		safeLogPrintf("commit queue: batch write returned %d results for %d entries, falling back to single commits", len(results), len(entries))
		cq.fallbackBatchEntries(entries)
		return
	}
	type failedBatchEntry struct {
		entry *CommitEntry
		err   error
	}
	failed := make([]failedBatchEntry, 0)
	for i, result := range results {
		entry := entries[i]
		if cq.isEntryCanceled(entry) {
			cq.removeFromQueue(entry)
			cq.endInFlight(entry)
			safeLogPrintf("commit queue: entry for %s was canceled after batch upload, skipping cleanup to preserve newer staged data", entry.Path)
			continue
		}
		if result.OK() {
			if err := cq.onCommitSuccessWithOptions(entry, entry.BaseRev, result.Revision, remoteItems[i].HasMode); err != nil {
				safeLogPrintf("commit queue: batch post-upload failed for %s: %v", entry.Path, err)
				cq.onCommitPostUploadFailure(entry, err)
			}
			cq.endInFlight(entry)
			continue
		}
		resultErr := batchWriteResultError(result)
		failed = append(failed, failedBatchEntry{entry: entry, err: resultErr})
	}
	unlockPaths()
	for _, failure := range failed {
		entry := failure.entry
		resultErr := failure.err
		// A batch shares one transport cancellation context. Retry each failed
		// item through commitOne so cancellation of one path cannot poison a
		// sibling's conflict validation or auto-resolve with that shared context.
		safeLogPrintf("commit queue: batch write result failed for %s, retrying with an independent single commit: %v", entry.Path, resultErr)
		cq.commitOne(entry)
		cq.endInFlight(entry)
	}
}

func (cq *CommitQueue) prepareBatchWriteItems(ctx context.Context, entries []*CommitEntry) ([]client.BatchWriteItem, error) {
	items := make([]client.BatchWriteItem, len(entries))
	for i, entry := range entries {
		data, err := cq.readEntryPayloadCtx(ctx, entry)
		if err != nil {
			return nil, fmt.Errorf("read shadow %s: %w", entry.Path, err)
		}
		item := client.BatchWriteItem{
			Path:             cq.remotePath(entry.Path),
			ExpectedRevision: entry.BaseRev,
			Data:             data,
		}
		// Older batch endpoints may accept mode without enforcing chmod's
		// authorization policy. Without the advertised contract, commit content
		// only and let onCommitSuccessWithOptions apply the pending chmod.
		if cq.client.CachedBatchWriteModeSupported() && shouldApplyRemoteMode(entry.Kind, entry.HasMode, entry.Mode) {
			item.Mode = remoteChmodMode(entry.Mode & posixPermissionModeMask)
			item.HasMode = true
		}
		items[i] = item
	}
	return items, nil
}

func (cq *CommitQueue) lockBatchPaths(entries []*CommitEntry) func() {
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		paths = append(paths, entry.Path)
	}
	sort.Strings(paths)
	unlocks := make([]func(), 0, len(paths))
	var last string
	for _, path := range paths {
		if path == last {
			continue
		}
		last = path
		unlocks = append(unlocks, cq.lockPath(path))
	}
	return func() {
		for i := len(unlocks) - 1; i >= 0; i-- {
			unlocks[i]()
		}
	}
}

func (cq *CommitQueue) fallbackBatchEntries(entries []*CommitEntry) {
	for _, entry := range entries {
		if cq.isEntryCanceled(entry) {
			cq.removeFromQueue(entry)
			cq.endInFlight(entry)
			continue
		}
		cq.commitOne(entry)
		cq.endInFlight(entry)
	}
}

func (cq *CommitQueue) validateEntryPayloadFreshCtx(ctx context.Context, entry *CommitEntry) error {
	if entry == nil {
		return nil
	}
	payloadBaseRev := entry.payloadBaseRevision()
	watermark := cq.effectiveDurableWatermark(entry)
	if watermark > 0 && payloadBaseRev >= 0 && payloadBaseRev < watermark {
		if entry.growthRebaseRev > 0 {
			if entry.growthRebaseRev != watermark || entry.BaseRev != watermark ||
				entry.growthRebaseParentSnapshotID != entry.ParentSnapshotID {
				entry.DisableAutoResolveLWW = true
				return fmt.Errorf("%w: %s direct-child rebase was authorized at rev %d but durable watermark is rev %d",
					errCommitPayloadStale, entry.Path, entry.growthRebaseRev, watermark)
			}
		} else if cq.maybeRebaseGrownPayloadOntoWatermark(ctx, entry, watermark) {
			// Keep going: this is a later growth of the same path, not a
			// stale sibling snapshot. Authorization is recorded on entry so
			// nested payload reads do not repeat the remote identity proof.
		} else if ctx != nil && ctx.Err() != nil {
			// Cancellation is not evidence that the payload is stale. Preserve
			// it as a transient caller outcome so unlink/rollback does not turn
			// an intentionally canceled entry into staging.PendingConflict.
			return ctx.Err()
		} else {
			entry.DisableAutoResolveLWW = true
			return fmt.Errorf("%w: %s payload base rev %d is older than durable watermark rev %d",
				errCommitPayloadStale, entry.Path, payloadBaseRev, watermark)
		}
	}
	if entry.ShadowGen != 0 && cq != nil && cq.shadows != nil {
		if activeGen := cq.shadows.ActiveGeneration(entry.Path); activeGen != entry.ShadowGen {
			return fmt.Errorf("%w: %s shadow generation changed from %d to %d",
				errCommitPayloadStale, entry.Path, entry.ShadowGen, activeGen)
		}
	}
	return nil
}

// effectiveDurableWatermark returns the freshest known durable revision for
// entry.Path, combining the enqueue-time fence stored on the entry with the
// live filesystem tracker. It also writes the live watermark back to the entry
// so later conflict handling cannot auto-resolve with a stale snapshot.
func (cq *CommitQueue) effectiveDurableWatermark(entry *CommitEntry) int64 {
	if entry == nil {
		return 0
	}
	watermark := entry.DurableWatermarkRev
	if cq != nil && cq.DurableWatermark != nil {
		if live := cq.DurableWatermark(entry.Path); live > watermark {
			watermark = live
			entry.DurableWatermarkRev = live
		}
	}
	return watermark
}

func (cq *CommitQueue) readEntryPayloadCtx(ctx context.Context, entry *CommitEntry) ([]byte, error) {
	if entry == nil {
		return nil, fmt.Errorf("nil commit entry")
	}
	if err := cq.validateEntryPayloadFreshCtx(ctx, entry); err != nil {
		return nil, err
	}
	return cq.readEntryPayloadRaw(entry)
}

// readEntryPayloadRaw reads the staged payload without running the generation
// and watermark preflight, so callers decide for themselves whether the bytes
// are still the ones the entry is responsible for. It exists for the one case
// that must look at rejected bytes: proving that a payload the preflight
// called stale is byte-identical to the durable remote image, which makes the
// entry a redundant upload rather than a conflict.
func (cq *CommitQueue) readEntryPayloadRaw(entry *CommitEntry) ([]byte, error) {
	if entry == nil {
		return nil, fmt.Errorf("nil commit entry")
	}
	if entry.payloadBound {
		return entry.payload, nil
	}
	if cq == nil || cq.shadows == nil {
		return nil, fmt.Errorf("no shadow store")
	}
	data, err := cq.shadows.ReadAllIfGeneration(entry.Path, entry.ShadowGen)
	if err != nil {
		return nil, err
	}
	return entry.bindPayload(data), nil
}

func (cq *CommitQueue) entryUploadContext(parent context.Context, entry *CommitEntry, timeout time.Duration) (context.Context, context.CancelFunc, bool) {
	uploadCtx, uploadCancel := context.WithTimeout(parent, timeout)
	cq.mu.Lock()
	if entry.canceled {
		cq.mu.Unlock()
		uploadCancel()
		return nil, nil, false
	}
	entry.cancelUpload = uploadCancel
	cq.mu.Unlock()
	cleanup := func() {
		cq.mu.Lock()
		entry.cancelUpload = nil
		cq.mu.Unlock()
		uploadCancel()
	}
	return uploadCtx, cleanup, true
}

func (cq *CommitQueue) rejectAbandonedLayer() error {
	cq.mu.Lock()
	abandoned := cq.abandoned
	cq.mu.Unlock()
	if abandoned {
		return errLayerRolledBack
	}
	return nil
}

func (cq *CommitQueue) commitNowPathLocked(ctx context.Context, entry *CommitEntry) error {
	if err := cq.rejectAbandonedLayer(); err != nil {
		return err
	}
	release, discard, err := cq.beginImmediateMutationCommit(ctx, entry, true)
	if err != nil {
		return err
	}
	if discard {
		cq.discardEntry(entry)
		return nil
	}
	if release != nil {
		defer release()
	}
	return cq.commitNowClaimedPathLocked(ctx, entry)
}

func (cq *CommitQueue) commitNowClaimedPathLocked(ctx context.Context, entry *CommitEntry) error {
	if cq.isCanceledImmediateEntry(entry) {
		cq.removeFromQueue(entry)
		return nil
	}
	if cq.discardSupersededEntry(entry) || cq.discardLandedAncestor(ctx, entry) {
		return nil
	}
	committedRev, err := cq.uploadEntry(ctx, entry)
	if err != nil {
		if errors.Is(err, errCommitPayloadStale) {
			// Report a partial resolution: the bytes are durable, but if the
			// entry's post-commit bookkeeping (its pending chmod, for
			// instance) failed, a strict synchronous caller must not be told
			// durability succeeded.
			if resolved, postErr := cq.resolveStalePayloadAsIdempotent(ctx, entry); resolved {
				return postErr
			}
		}
		if cq.perf != nil {
			cq.perf.commitFailure.add(1)
		}
		return err
	}
	if cq.isCanceledImmediateEntry(entry) {
		cq.removeFromQueue(entry)
		return nil
	}
	if err := cq.onCommitSuccess(entry, entry.BaseRev, committedRev); err != nil {
		if cq.perf != nil {
			cq.perf.commitFailure.add(1)
		}
		return err
	}
	return nil
}

func (cq *CommitQueue) beginImmediateMutationCommit(ctx context.Context, entry *CommitEntry, pathLocked bool) (func(), bool, error) {
	if cq == nil || entry == nil || entry.Inode == 0 || entry.MutationSeq == 0 {
		return nil, false, nil
	}
	for {
		cq.mu.Lock()
		newer := false
		older := false
		samePathWorker := pathLocked && cq.inFlight[entry.Path] != nil
		for _, active := range cq.inFlight {
			if active == nil || active.canceled || active == entry || (pathLocked && active.Path == entry.Path) || active.Inode != entry.Inode || active.MutationSeq == 0 {
				continue
			}
			if active.MutationSeq > entry.MutationSeq {
				newer = true
			} else if active.MutationSeq <= entry.MutationSeq {
				older = true
			}
		}
		for active := range cq.immediate {
			if active == nil || active.canceled || active == entry || (pathLocked && active.Path == entry.Path) || active.Inode != entry.Inode || active.MutationSeq == 0 {
				continue
			}
			if active.MutationSeq > entry.MutationSeq {
				newer = true
			} else if active.MutationSeq <= entry.MutationSeq {
				older = true
			}
		}
		for _, queued := range cq.queue {
			if queued == nil || queued.canceled || queued == entry || (pathLocked && queued.Path == entry.Path) || queued.Inode != entry.Inode || queued.MutationSeq == 0 {
				continue
			}
			if samePathWorker && queued.MutationSeq < entry.MutationSeq {
				continue
			}
			if queued.MutationSeq > entry.MutationSeq {
				newer = true
			} else if queued.MutationSeq <= entry.MutationSeq {
				older = true
			}
		}
		if newer && cq.serializeMutationInodes {
			cq.mu.Unlock()
			return nil, true, nil
		}
		if !older && !newer && (pathLocked || cq.inFlight[entry.Path] == nil) {
			if cq.immediate == nil {
				cq.immediate = make(map[*CommitEntry]struct{})
			}
			cq.immediate[entry] = struct{}{}
			cq.mu.Unlock()
			return func() { cq.endImmediate(entry) }, false, nil
		}
		cq.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (cq *CommitQueue) endImmediate(entry *CommitEntry) {
	if cq == nil || entry == nil {
		return
	}
	cq.mu.Lock()
	delete(cq.immediate, entry)
	cq.pruneLandedLocked()
	cq.mu.Unlock()
}

// uploadEntry uploads entry data to the server. Returns (committedRev, error).
// committedRev > 0 only when the server-returned revision is available
// (direct PUT). Multipart/streaming uploads return 0 because the current
// complete response does not expose the exact committed revision.
func (cq *CommitQueue) uploadEntry(ctx context.Context, entry *CommitEntry) (int64, error) {
	if cq.shadows == nil {
		return 0, fmt.Errorf("no shadow store")
	}

	layerRef := cq.layerRefSnapshot()
	apiPath := cq.remotePath(entry.Path)
	if layerRef != "" {
		return cq.uploadLayerEntry(ctx, layerRef, entry, apiPath)
	}
	if err := cq.validateEntryPayloadFreshCtx(ctx, entry); err != nil {
		return 0, err
	}
	// Validation may safely authorize a base-zero staging.PendingNew direct child to
	// overwrite the exact parent revision this live queue just landed.
	expectedRevision := entry.BaseRev
	if entry.Kind == staging.PendingOverwrite && expectedRevision <= 0 {
		return 0, fmt.Errorf("missing base revision for overwrite: %s", entry.Path)
	}

	// Extent content layout: route large commits through the extent block
	// plane before the classic direct-PUT/multipart decision. Fresh
	// large files are taken over by extent sessions here; existing extent
	// files commit incrementally through PATCH dirty blocks.
	if committedRev, handled, err := cq.uploadExtentEntry(ctx, entry, apiPath, expectedRevision); handled {
		return committedRev, err
	}

	// ShadowSpill entries: stream directly from shadow file to avoid loading
	// multi-GiB files into memory. Uses io.SectionReader over the shadow fd.
	if entry.ShadowSpill {
		start := time.Now()
		committedRev, err := cq.uploadShadowSpill(ctx, entry, expectedRevision)
		if cq.perf != nil {
			var bytes uint64
			if entry.Size > 0 {
				bytes = uint64(entry.Size)
			}
			cq.perf.recordRemoteOp(perfRemoteWrite, err, time.Since(start), bytes)
		}
		return committedRev, err
	}

	// Non-ShadowSpill: read full content into memory.
	data, err := cq.readEntryPayloadCtx(ctx, entry)
	if err != nil {
		return 0, fmt.Errorf("read shadow: %w", err)
	}

	// Route based on entry.Size (metadata at enqueue time), NOT len(data).
	// Files under directPutThreshold use direct PUT to skip the multipart
	// initiate/presign/complete/finalize overhead (~440ms). When threshold
	// is 0 (no server value cached) we deliberately do not direct-PUT
	// non-empty files: the server may be configured below 50KB and would
	// reject the simple PUT. Zero-byte files keep direct PUT because V2
	// initiate rejects total_size=0.
	threshold := cq.directPutThreshold()
	useDirectPUT := entry.Size == 0 || (threshold > 0 && entry.Size < threshold)
	if useDirectPUT {
		start := time.Now()
		committedRev, err := cq.client.WriteCtxConditionalWithRevision(ctx, apiPath, data, expectedRevision)
		if cq.perf != nil {
			cq.perf.recordRemoteOp(perfRemoteWrite, err, time.Since(start), uint64(len(data)))
		}
		return committedRev, err
	}

	// Larger non-ShadowSpill files: multipart upload.
	start := time.Now()
	err = uploadBufferedRemoteFile(ctx, cq.client, apiPath, data, expectedRevision)
	if cq.perf != nil {
		cq.perf.recordRemoteOp(perfRemoteWrite, err, time.Since(start), uint64(len(data)))
	}
	return 0, err
}

func (cq *CommitQueue) uploadLayerEntry(ctx context.Context, layerRef string, entry *CommitEntry, apiPath string) (int64, error) {
	// The queue may have captured this generation before replay observed an
	// external tip. Recheck under the upload fence, not only during recovery.
	if cq.index != nil {
		if meta, ok := cq.index.GetMeta(entry.Path); ok && meta.Kind == staging.PendingConflict {
			entry.DisableAutoResolveLWW = true
			return 0, &client.StatusError{StatusCode: http.StatusConflict, Message: "local Layer publication conflicts with an observed remote tip"}
		}
	}
	if err := cq.validateEntryPayloadFreshCtx(ctx, entry); err != nil {
		return 0, err
	}
	// Read the main-namespace claim only after validation. Overlay upserts
	// themselves do not advance main revisions or produce landed proofs.
	// Layer entries use zero, not the base write API's -1 sentinel, for no claim.
	expectedRevision := max(entry.BaseRev, 0)
	if entry.Size > maxInlineLayerEntryBytes || entry.ShadowSpill {
		fd, actualSize, release, err := cq.shadows.OpenIfGeneration(entry.Path, entry.ShadowGen)
		if err != nil {
			return 0, fmt.Errorf("open shadow stream: %w", err)
		}
		defer release()
		defer func() { _ = fd.Close() }()
		if entry.Size != actualSize {
			return 0, fmt.Errorf("layer entry %s size mismatch: metadata=%d actual=%d", entry.Path, entry.Size, actualSize)
		}
		start := time.Now()
		uploaded, err := cq.client.UploadFSLayerFile(ctx, layerRef, apiPath, fd, actualSize, expectedRevision, entry.Mode, entry.HasMode)
		if err == nil {
			entry.LayerEntry = layerCacheIdentity(uploaded, layerRef)
		}
		if cq.perf != nil {
			cq.perf.recordRemoteOp(perfRemoteWrite, err, time.Since(start), uint64(actualSize))
		}
		return 0, err
	}
	data, err := cq.readEntryPayloadCtx(ctx, entry)
	if err != nil {
		return 0, fmt.Errorf("read shadow: %w", err)
	}
	if int64(len(data)) > maxInlineLayerEntryBytes {
		return 0, fmt.Errorf("layer entry %s is %d bytes; inline layer uploads are limited to %d bytes", entry.Path, len(data), maxInlineLayerEntryBytes)
	}
	actualSize := int64(len(data))
	if entry.Size != actualSize {
		return 0, fmt.Errorf("layer entry %s size mismatch: metadata=%d actual=%d", entry.Path, entry.Size, actualSize)
	}
	req := client.FSLayerEntryRequest{
		Path:         apiPath,
		Op:           "upsert",
		Kind:         "file",
		BaseRevision: expectedRevision,
		Content:      data,
		SizeBytes:    actualSize,
	}
	if entry.HasMode {
		req.Mode = entry.Mode & 0o777
	}
	start := time.Now()
	uploaded, err := cq.client.UpsertFSLayerEntry(ctx, layerRef, req)
	if err == nil {
		entry.LayerEntry = layerCacheIdentity(uploaded, layerRef)
	}
	if cq.perf != nil {
		cq.perf.recordRemoteOp(perfRemoteWrite, err, time.Since(start), uint64(len(data)))
	}
	return 0, err
}

func (cq *CommitQueue) removeFromQueue(entry *CommitEntry) {
	cq.mu.Lock()
	defer cq.mu.Unlock()
	cq.stopDelayedLocked(entry)
	for i, e := range cq.queue {
		if e == entry {
			cq.queue = append(cq.queue[:i], cq.queue[i+1:]...)
			cq.removeQueuedLocked(entry)
			cq.pruneLandedLocked()
			return
		}
	}
}

func (cq *CommitQueue) addQueuedLocked(entry *CommitEntry) {
	if cq.queuedByPath == nil || entry == nil {
		return
	}
	entries := cq.queuedByPath[entry.Path]
	if entries == nil {
		entries = make(map[*CommitEntry]struct{})
		cq.queuedByPath[entry.Path] = entries
	}
	entries[entry] = struct{}{}
}

func (cq *CommitQueue) removeQueuedLocked(entry *CommitEntry) {
	if cq.queuedByPath == nil || entry == nil {
		return
	}
	entries := cq.queuedByPath[entry.Path]
	if entries == nil {
		return
	}
	delete(entries, entry)
	if len(entries) == 0 {
		delete(cq.queuedByPath, entry.Path)
	}
}

func (cq *CommitQueue) hasQueuedPathLocked(path string) bool {
	if cq.queuedByPath != nil {
		return len(cq.queuedByPath[path]) > 0
	}
	for _, e := range cq.queue {
		if e.Path == path {
			return true
		}
	}
	return false
}

func (cq *CommitQueue) rebuildQueuedIndexLocked() {
	if cq.queuedByPath == nil {
		return
	}
	cq.queuedByPath = make(map[string]map[*CommitEntry]struct{}, len(cq.queue))
	for _, e := range cq.queue {
		cq.addQueuedLocked(e)
	}
}

func (cq *CommitQueue) onCommitSuccess(entry *CommitEntry, expectedRevision, committedRev int64) error {
	return cq.onCommitSuccessWithOptions(entry, expectedRevision, committedRev, false)
}

func (cq *CommitQueue) onCommitSuccessWithOptions(entry *CommitEntry, expectedRevision, committedRev int64, remoteModeAlreadyApplied bool) error {
	layerRef := cq.layerRefSnapshot()
	if layerRef == "" {
		committedRev = committedRevisionForExpectedRevision(expectedRevision, committedRev)
	}
	cq.rememberLanded(entry.Path, committedRev, entry.Size, cq.checksumLandedPayload(entry), entry.SnapshotID, entry.liveAncestors...)
	cq.recordCommitSeq(entry.Path)
	// The image landed remotely: a previously recorded terminal failure for
	// this path must not fail later durability barriers. Clear it before the
	// entry leaves the queue (same visibility window as rememberLanded).
	cq.clearTerminalFailure(entry.Path)
	if cq.OnUploaded != nil {
		cq.OnUploaded(entry, committedRev)
	}

	// The live pending meta carries the newest journaled mode (v1
	// setPendingMetadataMode): prefer it over the entry's enqueue-time
	// snapshot, which a newer chmod on an open handle (killpriv) has since
	// superseded — applying the stale snapshot would clobber it (chmod/12).
	applyMode, applyHasMode := entry.Mode, entry.HasMode
	if cq.index != nil {
		if meta, ok := cq.index.GetMeta(entry.Path); ok && meta.HasMode {
			applyMode, applyHasMode = meta.Mode, true
		}
	}
	if layerRef == "" && !remoteModeAlreadyApplied && shouldApplyRemoteMode(entry.Kind, applyHasMode, applyMode) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		var err error
		mode := applyMode & posixPermissionModeMask
		remoteMode := remoteChmodMode(mode)
		err = retryPostUploadMode(ctx, func() error {
			start := time.Now()
			applyErr := cq.client.ChmodCtx(ctx, cq.remotePath(entry.Path), remoteMode)
			if cq.perf != nil {
				cq.perf.recordRemoteOp(perfRemoteMutation, applyErr, time.Since(start), 0)
			}
			return applyErr
		})
		cancel()
		if err != nil {
			// Chmod is a best-effort metadata operation. The file data has
			// already been uploaded successfully. Under concurrent writes to
			// the same file, the server may have already moved to a newer
			// revision (or the file may have been replaced), causing the chmod
			// to fail with "not found". Treating this as a terminal failure
			// blocks the commit queue path indefinitely and stalls all
			// subsequent FUSE operations on that path. Instead, log the warning
			// and proceed — the mode will be applied by the next successful
			// commit for this path.
			if client.IsNotFound(err) {
				safeLogPrintf("commit queue: post-upload chmod not found for %s (concurrent write likely replaced it); proceeding without mode update", entry.Path)
			} else {
				return fmt.Errorf("%w: chmod %s to %o: %w", errCommitPostUpload, entry.Path, mode, err)
			}
		}
	}

	// Write durable commit record BEFORE cleaning up local state so that
	// crash recovery never re-uploads an already committed entry.
	if cq.journal != nil && layerRef == "" {
		if err := cq.journal.Append(staging.JournalEntry{
			Op:   staging.JournalCommit,
			Path: entry.Path,
		}); err != nil {
			safeLogPrintf("commit queue: journal commit marker failed for %s: %v (keeping local state)", entry.Path, err)
			cq.removeFromQueue(entry)
			return nil
		}
	}

	// Notify dat9fs to seed readCache + update inode revision before
	// cleaning up shadow (which is the data source for the cache seed).
	if cq.OnSuccess != nil {
		cq.OnSuccess(entry, committedRev)
	}

	// Clean up shadow and pending index. Layer mounts intentionally retain
	// both until the layer is committed or rolled back: pendingIndex remains
	// the local metadata source, and shadowStore remains the local data source
	// for large files that are not admitted to readCache.
	if cq.shadows != nil && cq.layerRefSnapshot() == "" {
		if keep, ok := cq.retainedShadows.LoadAndDelete(entry.Path); ok && keep == entry.ShadowGen {
			// The staging handle kept newer local bytes: the shadow stays
			// as the restore source for this landing's evicted parts.
			cq.index.RemoveIfGeneration(entry.Path, entry.PendingIndexGen)
			cq.removeFromQueue(entry)
			return nil
		}
		if entry.ShadowGen != 0 {
			cq.shadows.RemoveIfGeneration(entry.Path, entry.ShadowGen)
		}
	}
	if cq.index != nil && cq.layerRefSnapshot() == "" {
		if entry.PendingIndexGen != 0 {
			cq.index.RemoveIfGeneration(entry.Path, entry.PendingIndexGen)
		}
	}
	if cq.index != nil && cq.layerRefSnapshot() != "" {
		if err := cq.index.MarkLayerCommittedIfGeneration(entry.Path, entry.PendingIndexGen, committedRev, entry.Mode, entry.HasMode, entry.LayerEntry); err != nil {
			return err
		}
	}
	if cq.OnCleanup != nil {
		cq.OnCleanup(entry)
	}

	// Remove from queue AFTER all cleanup so WaitPath sees the entry
	// until bookkeeping is complete.
	cq.removeFromQueue(entry)

	if cq.perf != nil {
		cq.perf.commitSuccess.add(1)
	}
	safeLogPrintf("commit queue: successfully uploaded %s (%d bytes, rev=%d)", entry.Path, entry.Size, committedRev)
	return nil
}

// tryAutoResolveConflict attempts to resolve a 409 conflict automatically.
// It fetches the server's current revision and content, then:
// - If local shadow matches server content → idempotent (mark success)
// - If local shadow differs → LWW re-upload with the new revision
// - If re-upload also 409s or any step fails → fall back to terminal failure
//
// This covers ~80% of agent conflict scenarios (whole-file overwrites) without
// requiring 3-way merge. Max 1 retry to avoid write amplification.
func (cq *CommitQueue) tryAutoResolveConflict(entryCtx context.Context, entry *CommitEntry) {
	if cq.discardSupersededEntry(entry) {
		return
	}
	// Bail early if the file was deleted locally while queued.
	if cq.isEntryCanceled(entry) || entryCtx.Err() != nil {
		cq.removeFromQueue(entry)
		safeLogPrintf("commit queue: auto-resolve skipped for %s (canceled)", entry.Path)
		return
	}

	// Layer mounts upload through layer endpoints; Stat/Read here would
	// compare against the base FS namespace, so neither the idempotent
	// check nor LWW can be trusted. Keep layer conflicts terminal.
	if cq.layerRefSnapshot() != "" {
		safeLogPrintf("commit queue: auto-resolve unsupported for layer mount, terminal failure for %s", entry.Path)
		cq.finishResolveFailure(entryCtx, entry, nil)
		return
	}

	// ShadowSpill large files: full-memory ReadAll + bytes.Equal would OOM
	// for multi-GiB files, so no LWW. But the idempotent case (crash after
	// upload completed, before the local cleanup — the common large-file
	// crash window) is resolvable with a bounded-memory chunked compare.
	if entry.ShadowSpill {
		cq.tryResolveShadowSpillIdempotent(entryCtx, entry)
		return
	}

	if cq.shadows == nil {
		cq.finishResolveFailure(entryCtx, entry, nil)
		return
	}

	// Read local shadow content.
	localData, err := cq.readEntryPayloadCtx(entryCtx, entry)
	if err != nil {
		// Shadow may have been removed by a concurrent CancelPath/CancelPrefix
		// (Unlink/Rmdir). Treat as canceled rather than a true conflict.
		if cq.isEntryCanceled(entry) || entryCtx.Err() != nil {
			cq.removeFromQueue(entry)
			safeLogPrintf("commit queue: auto-resolve skipped for %s (canceled mid-read)", entry.Path)
			return
		}
		if errors.Is(err, errCommitPayloadStale) {
			safeLogPrintf("commit queue: auto-resolve rejected stale payload for %s: %v", entry.Path, err)
			cq.finishResolveFailure(entryCtx, entry, err)
			return
		}
		safeLogPrintf("commit queue: auto-resolve failed for %s: read shadow: %v", entry.Path, err)
		cq.finishResolveFailure(entryCtx, entry, err)
		return
	}
	apiPath := cq.remotePath(entry.Path)

	// Fetch server's current state: revision + content.
	// Use per-RPC timeouts so that a slow Read doesn't starve the Upload budget.
	statCtx, statCancel := context.WithTimeout(entryCtx, 10*time.Second)
	statStart := time.Now()
	stat, err := cq.client.StatCtx(statCtx, apiPath)
	statCancel()
	if cq.perf != nil {
		cq.perf.recordRemoteOp(perfRemoteStat, err, time.Since(statStart), 0)
	}
	if err != nil {
		if client.IsNotFound(err) {
			// 409 but no committed remote file: the conflict came from
			// upload-session state (e.g. a session orphaned by a crashed
			// client), not content — or the file was deleted remotely, in
			// which case LWW means the local write wins anyway. Retry once
			// as a create; terminal only if the retry also fails.
			if cq.isEntryCanceled(entry) || entryCtx.Err() != nil {
				cq.removeFromQueue(entry)
				safeLogPrintf("commit queue: auto-resolve skipped for %s (canceled before create retry)", entry.Path)
				return
			}
			safeLogPrintf("commit queue: conflict for %s but remote file not found, retrying upload as create", entry.Path)
			uploadCtx, uploadCancel, ok := cq.entryUploadContext(entryCtx, entry, releaseTimeout(int64(len(localData))))
			if !ok {
				cq.removeFromQueue(entry)
				safeLogPrintf("commit queue: auto-resolve skipped for %s (canceled before create retry upload)", entry.Path)
				return
			}
			uploadStart := time.Now()
			var committedRev int64
			var uploadErr error
			if len(localData) == 0 {
				committedRev, uploadErr = cq.client.WriteCtxConditionalWithRevision(uploadCtx, apiPath, localData, 0)
			} else {
				uploadErr = uploadBufferedRemoteFile(uploadCtx, cq.client, apiPath, localData, 0)
			}
			uploadCancel()
			if cq.perf != nil {
				cq.perf.recordRemoteOp(perfRemoteWrite, uploadErr, time.Since(uploadStart), uint64(len(localData)))
			}
			if cq.isEntryCanceled(entry) || entryCtx.Err() != nil {
				cq.removeFromQueue(entry)
				safeLogPrintf("commit queue: auto-resolve skipped for %s (canceled during create retry upload)", entry.Path)
				return
			}
			if uploadErr != nil {
				safeLogPrintf("commit queue: create retry failed for %s: %v", entry.Path, uploadErr)
				cq.finishResolveFailure(entryCtx, entry, uploadErr)
				return
			}
			safeLogPrintf("commit queue: auto-resolved conflict for %s via create retry (no remote file)", entry.Path)
			if err := cq.onCommitSuccess(entry, 0, committedRev); err != nil {
				cq.onCommitPostUploadFailure(entry, err)
			}
			return
		}
		safeLogPrintf("commit queue: auto-resolve failed for %s: stat: %v", entry.Path, err)
		cq.finishResolveFailure(entryCtx, entry, err)
		return
	}
	serverRev := stat.Revision

	readCtx, readCancel := context.WithTimeout(entryCtx, uploadTimeout)
	readStart := time.Now()
	serverData, err := cq.client.ReadCtx(readCtx, apiPath)
	readCancel()
	if cq.perf != nil {
		cq.perf.recordRemoteOp(perfRemoteRead, err, time.Since(readStart), uint64(len(serverData)))
	}
	if err != nil {
		safeLogPrintf("commit queue: auto-resolve failed for %s: read server: %v", entry.Path, err)
		cq.finishResolveFailure(entryCtx, entry, err)
		return
	}
	if cq.maybeRebaseGrownPayloadAgainstRemote(entry, serverRev, stat.Size, serverData) {
		cq.commitGrownPayload(entryCtx, entry)
		return
	}

	// Branch 1: idempotent — content already matches server.
	if bytes.Equal(localData, serverData) {
		safeLogPrintf("commit queue: auto-resolved conflict for %s (idempotent, content matches server rev %d)", entry.Path, serverRev)
		cq.finishIdempotentCommit(entryCtx, entry, serverRev)
		return
	}

	// Branch 2: LWW — re-upload local shadow with new base revision.
	if entry.DisableAutoResolveLWW {
		safeLogPrintf("commit queue: fenced conflict for %s will not LWW-rebase payload from base rev %d onto server rev %d", entry.Path, entry.payloadBaseRevision(), serverRev)
		cq.finishResolveFailure(entryCtx, entry, nil)
		return
	}
	if entry.PayloadBaseRevSet && entry.payloadBaseRevision() < serverRev {
		safeLogPrintf("commit queue: refusing LWW rebase for %s: payload base rev %d is older than server rev %d", entry.Path, entry.payloadBaseRevision(), serverRev)
		cq.finishResolveFailure(entryCtx, entry, nil)
		return
	}
	if err := cq.validateEntryPayloadFreshCtx(entryCtx, entry); err != nil {
		if cq.isEntryCanceled(entry) || entryCtx.Err() != nil {
			cq.removeFromQueue(entry)
			return
		}
		safeLogPrintf("commit queue: auto-resolve rejected stale payload for %s before LWW: %v", entry.Path, err)
		cq.finishResolveFailure(entryCtx, entry, err)
		return
	}
	if cq.discardSupersededEntry(entry) {
		return
	}
	// Re-check cancelation before the potentially expensive upload.
	if cq.isEntryCanceled(entry) || entryCtx.Err() != nil {
		cq.removeFromQueue(entry)
		safeLogPrintf("commit queue: auto-resolve aborted for %s before LWW upload (canceled)", entry.Path)
		return
	}
	safeLogPrintf("commit queue: auto-resolving conflict for %s via LWW (base rev %d → server rev %d)", entry.Path, entry.BaseRev, serverRev)
	uploadCtx, uploadCancel, ok := cq.entryUploadContext(entryCtx, entry, releaseTimeout(int64(len(localData))))
	if !ok {
		cq.removeFromQueue(entry)
		safeLogPrintf("commit queue: auto-resolve aborted for %s before LWW upload (canceled)", entry.Path)
		return
	}
	uploadStart := time.Now()
	err = uploadBufferedRemoteFile(uploadCtx, cq.client, apiPath, localData, serverRev)
	uploadCancel()
	if cq.perf != nil {
		cq.perf.recordRemoteOp(perfRemoteWrite, err, time.Since(uploadStart), uint64(len(localData)))
	}
	if err != nil {
		safeLogPrintf("commit queue: auto-resolve LWW re-upload failed for %s: %v", entry.Path, err)
		cq.finishResolveFailure(entryCtx, entry, err)
		return
	}
	if cq.isEntryCanceled(entry) || entryCtx.Err() != nil {
		cq.removeFromQueue(entry)
		safeLogPrintf("commit queue: auto-resolve skipped for %s (canceled during LWW upload)", entry.Path)
		return
	}

	safeLogPrintf("commit queue: auto-resolved conflict for %s via LWW (overwrote rev %d → new upload based on rev %d)", entry.Path, entry.BaseRev, serverRev)
	if err := cq.onCommitSuccess(entry, serverRev, 0); err != nil {
		cq.onCommitPostUploadFailure(entry, err)
	}
}

// tryResolveShadowSpillIdempotent resolves a 409 for a ShadowSpill entry only
// in the idempotent direction: if the server content already equals the local
// shadow byte-for-byte (crash after a completed upload, before local cleanup),
// the commit is marked successful. Any divergence is a terminal conflict —
// LWW re-upload is intentionally not attempted for large files.
func (cq *CommitQueue) tryResolveShadowSpillIdempotent(entryCtx context.Context, entry *CommitEntry) {
	if cq.shadows == nil || cq.client == nil {
		cq.finishResolveFailure(entryCtx, entry, nil)
		return
	}
	if err := cq.validateEntryPayloadFreshCtx(entryCtx, entry); err != nil {
		if cq.isEntryCanceled(entry) || entryCtx.Err() != nil {
			cq.removeFromQueue(entry)
			return
		}
		safeLogPrintf("commit queue: ShadowSpill stale payload rejected for %s: %v", entry.Path, err)
		cq.finishResolveFailure(entryCtx, entry, err)
		return
	}
	apiPath := cq.remotePath(entry.Path)

	statCtx, statCancel := context.WithTimeout(entryCtx, 10*time.Second)
	statStart := time.Now()
	stat, err := cq.client.StatCtx(statCtx, apiPath)
	statCancel()
	if cq.perf != nil {
		cq.perf.recordRemoteOp(perfRemoteStat, err, time.Since(statStart), 0)
	}
	if err != nil {
		if client.IsNotFound(err) {
			// 409 but no committed remote file: the conflict came from
			// upload-session state (e.g. a session orphaned by a crashed
			// client mid-multipart — exactly the crash-recovery window),
			// not content. Retry once as a create; terminal only if the
			// retry also fails.
			if cq.isEntryCanceled(entry) || entryCtx.Err() != nil {
				cq.removeFromQueue(entry)
				safeLogPrintf("commit queue: ShadowSpill auto-resolve skipped for %s (canceled before create retry)", entry.Path)
				return
			}
			safeLogPrintf("commit queue: ShadowSpill conflict for %s but remote file not found, retrying upload as create", entry.Path)
			uploadCtx, uploadCancel, ok := cq.entryUploadContext(entryCtx, entry, releaseTimeout(entry.Size))
			if !ok {
				cq.removeFromQueue(entry)
				safeLogPrintf("commit queue: ShadowSpill auto-resolve skipped for %s (canceled before create retry upload)", entry.Path)
				return
			}
			uploadStart := time.Now()
			committedRev, uploadErr := cq.uploadShadowSpill(uploadCtx, entry, 0)
			uploadCancel()
			if cq.perf != nil {
				var bytes uint64
				if entry.Size > 0 {
					bytes = uint64(entry.Size)
				}
				cq.perf.recordRemoteOp(perfRemoteWrite, uploadErr, time.Since(uploadStart), bytes)
			}
			if cq.isEntryCanceled(entry) || entryCtx.Err() != nil {
				cq.removeFromQueue(entry)
				safeLogPrintf("commit queue: ShadowSpill auto-resolve skipped for %s (canceled during create retry upload)", entry.Path)
				return
			}
			if uploadErr != nil {
				safeLogPrintf("commit queue: ShadowSpill create retry failed for %s: %v", entry.Path, uploadErr)
				cq.finishResolveFailure(entryCtx, entry, uploadErr)
				return
			}
			safeLogPrintf("commit queue: auto-resolved ShadowSpill conflict for %s via create retry (no remote file)", entry.Path)
			if err := cq.onCommitSuccess(entry, 0, committedRev); err != nil {
				cq.onCommitPostUploadFailure(entry, err)
			}
			return
		}
		safeLogPrintf("commit queue: ShadowSpill auto-resolve failed for %s: stat: %v", entry.Path, err)
		cq.finishResolveFailure(entryCtx, entry, err)
		return
	}

	fd, localSize, release, err := cq.shadows.OpenIfGeneration(entry.Path, entry.ShadowGen)
	if err != nil {
		// Shadow gone — likely canceled by a concurrent Unlink/Rmdir.
		if cq.isEntryCanceled(entry) || entryCtx.Err() != nil {
			cq.removeFromQueue(entry)
			safeLogPrintf("commit queue: ShadowSpill auto-resolve skipped for %s (canceled)", entry.Path)
			return
		}
		if errors.Is(err, errCommitPayloadStale) {
			safeLogPrintf("commit queue: ShadowSpill auto-resolve rejected stale payload for %s: %v", entry.Path, err)
		} else {
			safeLogPrintf("commit queue: ShadowSpill auto-resolve failed for %s: open shadow: %v", entry.Path, err)
		}
		cq.finishResolveFailure(entryCtx, entry, err)
		return
	}
	defer func() {
		if fd != nil {
			_ = fd.Close()
		}
		if release != nil {
			release()
		}
	}()
	if stat.Size != localSize {
		if localSize > stat.Size && stat.Size >= 0 && stat.Size <= maxLandedPayloadBytes {
			// Reuse the bounded, caller-cancelable identity read. A failed read
			// is not permission to overwrite, nor evidence of a genuine conflict.
			rev, size, body, readErr := cq.readRemoteSnapshot(entryCtx, entry.Path)
			if readErr != nil {
				cq.finishResolveFailure(entryCtx, entry, readErr)
				return
			}
			if cq.maybeRebaseGrownPayloadAgainstRemote(entry, rev, size, body) {
				_ = fd.Close()
				fd = nil
				release()
				release = nil
				cq.commitGrownPayload(entryCtx, entry)
				return
			}
		}
		cq.finishResolveFailure(entryCtx, entry, nil)
		return
	}

	buf := make([]byte, shadowSpillCompareChunk)
	for off := int64(0); off < localSize; off += shadowSpillCompareChunk {
		// Chunk-boundary cancel check bounds wasted work on multi-GiB
		// compares when the file is unlinked mid-resolve.
		if cq.isEntryCanceled(entry) || entryCtx.Err() != nil {
			cq.removeFromQueue(entry)
			safeLogPrintf("commit queue: ShadowSpill auto-resolve skipped for %s (canceled mid-compare)", entry.Path)
			return
		}
		n := int64(shadowSpillCompareChunk)
		if off+n > localSize {
			n = localSize - off
		}
		readCtx, readCancel := context.WithTimeout(entryCtx, uploadTimeout)
		readStart := time.Now()
		serverChunk, err := cq.client.ReadAtCtx(readCtx, apiPath, off, n)
		readCancel()
		if cq.perf != nil {
			cq.perf.recordRemoteOp(perfRemoteRead, err, time.Since(readStart), uint64(len(serverChunk)))
		}
		if err != nil {
			safeLogPrintf("commit queue: ShadowSpill auto-resolve failed for %s: read server @%d: %v", entry.Path, off, err)
			cq.finishResolveFailure(entryCtx, entry, err)
			return
		}
		ln, err := fd.ReadAt(buf[:n], off)
		if err != nil || int64(ln) != n {
			if cq.isEntryCanceled(entry) || entryCtx.Err() != nil {
				cq.removeFromQueue(entry)
				safeLogPrintf("commit queue: ShadowSpill auto-resolve skipped for %s (canceled mid-read)", entry.Path)
				return
			}
			safeLogPrintf("commit queue: ShadowSpill auto-resolve failed for %s: read shadow @%d: n=%d err=%v", entry.Path, off, ln, err)
			cq.finishResolveFailure(entryCtx, entry, err)
			return
		}
		if int64(len(serverChunk)) != n || !bytes.Equal(serverChunk, buf[:n]) {
			safeLogPrintf("commit queue: ShadowSpill conflict for %s is genuine (content differs @%d), terminal failure", entry.Path, off)
			cq.finishResolveFailure(entryCtx, entry, nil)
			return
		}
	}

	safeLogPrintf("commit queue: auto-resolved ShadowSpill conflict for %s (idempotent, %d bytes match server rev %d)", entry.Path, localSize, stat.Revision)
	// onCommitSuccess removes the shadow generation. Release the OpenIfGeneration
	// path lock first to avoid self-deadlock on generation-scoped cleanup.
	_ = fd.Close()
	fd = nil
	release()
	release = nil
	cq.finishIdempotentCommit(entryCtx, entry, stat.Revision)
}

// finishIdempotentCommit runs only after a successful equality read. Cancellation
// may have happened after that read returned, so recheck before removing staging.
func (cq *CommitQueue) finishIdempotentCommit(ctx context.Context, entry *CommitEntry, rev int64) {
	if cq.isEntryCanceled(entry) || ctx.Err() != nil {
		cq.removeFromQueue(entry)
		return
	}
	if err := cq.onCommitSuccess(entry, rev, rev); err != nil {
		cq.onCommitPostUploadFailure(entry, err)
	}
}

func (cq *CommitQueue) onCommitPostUploadFailure(entry *CommitEntry, err error) {
	if cq.perf != nil {
		cq.perf.commitFailure.add(1)
	}
	// The DATA landed but the post-upload work (deferred chmod, tags) did
	// not, so the requested outcome was never achieved. Record it for
	// outcome-aware waiters: a close-sync/strict-fsync caller that waited on
	// this path must not report success for a mode that was never committed.
	// The record is the same terminal-failure channel the data-failure path
	// uses — both mean "this path did not reach the requested state" — and it
	// is retired by the next successful commit of the path.
	if cq.terminalFailures == nil {
		cq.terminalFailures = make(map[string]pathCommitFailure)
	}
	cq.mu.Lock()
	if err == nil {
		err = errCommitPostUploadUnknown
	}
	cq.failureClock++
	cq.terminalFailures[entry.Path] = pathCommitFailure{err: err, seq: cq.failureClock}
	cq.pruneTerminalFailuresLocked()
	cq.mu.Unlock()
	cq.removeFromQueue(entry)
	safeLogPrintf("commit queue: post-upload failure for %s; local pending state preserved for retry: %v", entry.Path, err)
}

// errCommitPostUploadUnknown backs a post-upload failure that carried no
// error value: the requested metadata was not applied either way.
var errCommitPostUploadUnknown = errors.New("commit queue: post-upload work failed")

func (cq *CommitQueue) onCommitTerminalFailure(entry *CommitEntry, lastErr error) {
	// Mark the entry as conflicted in the pending index so that crash
	// recovery (RecoverPending) skips it instead of retrying forever.
	// Preserve both the shadow file and the pending metadata so the user
	// can recover their local edits manually — deleting them here would
	// silently discard the only durable copy of unsynchronised data.
	//
	// The conflict marker MUST be durable before we journal or dequeue;
	// otherwise a restart could re-enqueue the same upload.
	if cq.index != nil {
		if entry.PendingIndexGen != 0 {
			marked, err := cq.index.MarkConflictIfGeneration(entry.Path, entry.PendingIndexGen)
			if err != nil {
				safeLogPrintf("commit queue: failed to mark conflict for %s: %v (pending data preserved; in-memory queue entry removed)", entry.Path, err)
				cq.removeFromQueue(entry)
				cq.recordCommitTerminalFailure(entry, lastErr)
				return
			}
			if !marked {
				safeLogPrintf("commit queue: stale terminal failure for %s skipped conflict marker; newer pending generation exists", entry.Path)
				cq.removeFromQueue(entry)
				return
			}
		} else if err := cq.index.MarkConflict(entry.Path); err != nil {
			// Conflict marker not durable. The in-memory queue entry is still
			// removed; RecoverPending can retry from preserved pending/shadow.
			safeLogPrintf("commit queue: failed to mark conflict for %s: %v (pending data preserved; in-memory queue entry removed)", entry.Path, err)
			cq.removeFromQueue(entry)
			cq.recordCommitTerminalFailure(entry, lastErr)
			return
		}
	}
	if cq.journal != nil {
		if err := cq.journal.Append(staging.JournalEntry{
			Op:   staging.JournalCommit, // treated as "done" so recovery won't re-enqueue
			Path: entry.Path,
		}); err != nil {
			safeLogPrintf("commit queue: journal done marker failed for %s: %v", entry.Path, err)
		}
	}

	// Remove from queue AFTER all cleanup so WaitPath sees the entry
	// until bookkeeping is complete.
	cq.removeFromQueue(entry)
	cq.recordCommitTerminalFailure(entry, lastErr)
}

func (cq *CommitQueue) recordCommitTerminalFailure(entry *CommitEntry, lastErr error) {
	if cq.perf != nil {
		cq.perf.commitFailure.add(1)
		cq.perf.commitTerminalFailure.add(1)
	}
	// Record the failure before this worker returns (the entry is already
	// dequeued; endInFlight follows commitOne) so any durability barrier that
	// observes the path idle through WaitPath* necessarily sees the failure.
	if lastErr == nil {
		// The data still never landed remotely; report a concrete error to
		// barriers instead of a nil that would read as success.
		lastErr = errCommitTerminalUnknown
	}
	cq.mu.Lock()
	if cq.terminalFailures == nil {
		cq.terminalFailures = make(map[string]pathCommitFailure)
	}
	cq.failureClock++
	cq.terminalFailures[entry.Path] = pathCommitFailure{err: lastErr, seq: cq.failureClock}
	cq.pruneTerminalFailuresLocked()
	cq.mu.Unlock()
	// Alertable FUSE-local error. Collectors should match
	// "drive9: error event=commit_terminal_failure" on stderr / mount-log
	// files (typically ~/.cache/drive9/mount-logs/). The drive9 server never
	// sees this line; collect it outside the sandbox. lastErr may be nil.
	// reason= distinguishes retryable upload loss from concurrency conflict
	// and layer-abandonment so alert severity can differ.
	safeLogPrintf("drive9: error event=commit_terminal_failure path=%s reason=%s err=%v detail=local_data_preserved", entry.Path, classifyCommitTerminalReason(lastErr), lastErr)
}

// RecordTerminalFailureForPath records an in-memory terminal failure for path
// without a queue entry behind it. Callers use it when a failure happened
// outside the commit worker (a rename whose preserved payload could not be
// re-paired with its new name): barriers on the path must still answer EIO
// even though nothing can be re-driven from the queue. The record is
// standalone — the unlanded bytes stay under the old pathname, so this path
// usually carries no pending entry the pruner could treat as a durable
// fallback or as proof of staleness.
func (cq *CommitQueue) RecordTerminalFailureForPath(path string, err error) {
	if cq == nil {
		return
	}
	if err == nil {
		err = errCommitTerminalUnknown
	}
	cq.mu.Lock()
	if cq.terminalFailures == nil {
		cq.terminalFailures = make(map[string]pathCommitFailure)
	}
	cq.failureClock++
	cq.terminalFailures[path] = pathCommitFailure{err: err, seq: cq.failureClock, standalone: true}
	cq.pruneTerminalFailuresLocked()
	cq.mu.Unlock()
}

// errCommitTerminalUnknown backs PathTerminalFailure when a terminal commit
// failure carried no upload error: the data never landed either way, so the
// barrier must still report failure.
var errCommitTerminalUnknown = errors.New("commit queue: terminal commit failure without a recorded upload error")

// errCommitTerminalRecovered backs PathTerminalFailure's durable fallback: the
// in-memory record died with the previous process, but the conflict-marked
// pending entry it left behind still describes data that never landed.
var errCommitTerminalRecovered = errors.New("commit queue: pending entry preserved for manual recovery after a terminal commit failure")

// terminalFailuresLimit bounds terminalFailures the way landedLimit bounds
// landed. Eviction is only allowed for records the barrier can still
// re-derive: one whose path still carries a PendingConflict marker (the
// fallback below reports it), plus — for worker records only — one whose
// pending state is already gone (stale). A standalone record with no marker
// at its path must stay regardless: its bytes sit under a different (old)
// pathname, so the absence of pending state here is not staleness but the
// record's normal shape, and evicting it would let fsync claim durability for
// data that never landed.
const terminalFailuresLimit = 64

func (cq *CommitQueue) pruneTerminalFailuresLocked() {
	if cq.index == nil {
		// No durable marker backs any record; evicting one would lose the
		// only copy of an unresolved failure.
		return
	}
	for len(cq.terminalFailures) > terminalFailuresLimit {
		// Evict the oldest EVICTABLE record: one whose path still carries a
		// PendingConflict marker (the fallback in PathTerminalFailure keeps
		// answering it — standalone or not), or a worker record whose pending
		// state is already gone (stale). A record with a live non-conflict
		// entry, and a standalone record with no marker at all, are the only
		// witnesses of an unresolved failure and are skipped, not dropped.
		var oldestPath string
		var oldestSeq uint64
		first := true
		for path, failure := range cq.terminalFailures {
			meta, hasMeta := cq.index.GetMeta(path)
			if hasMeta && meta != nil && meta.Kind != staging.PendingConflict {
				continue // live non-conflict entry: the record is the only witness
			}
			markerBacked := hasMeta && meta != nil
			if failure.standalone && !markerBacked {
				continue // nothing at this path can re-derive a standalone record
			}
			if first || failure.seq < oldestSeq {
				oldestPath, oldestSeq, first = path, failure.seq, false
			}
		}
		if first {
			return // nothing evictable
		}
		delete(cq.terminalFailures, oldestPath)
	}
}

// clearTerminalFailure drops the recorded terminal failure for path: a newer
// commit landed, so later durability barriers must not keep reporting EIO.
func (cq *CommitQueue) clearTerminalFailure(path string) {
	cq.mu.Lock()
	delete(cq.terminalFailures, path)
	cq.mu.Unlock()
}

// CancelQueuedForRename stops every queued or in-flight entry for path and
// journals the rename's done marker fencing the path's earlier fsync frames.
// Unlike CancelPath the caller already holds the writeback path locks, so no
// staging or worker can publish in between — a worker blocked ahead of the
// upload aborts on the cancelled entry context once it gets the lock. Local
// shadow/pending state is preserved: the caller moves it to the new name,
// where the re-driven landing carries the durability obligation. Without the
// marker a restart would replay the old frames and re-upload the dead name.
func (cq *CommitQueue) CancelQueuedForRename(path string) error {
	if cq == nil {
		return nil
	}
	for _, cancel := range cq.cancelPathEntries(path) {
		cancel()
	}
	if cq.journal == nil {
		return nil
	}
	if err := cq.journal.Append(staging.JournalEntry{Op: staging.JournalUnlink, Path: path}); err != nil {
		return fmt.Errorf("journal rename marker for %s: %w", path, err)
	}
	return nil
}

// recordCommitSeq stamps path's commit sequence — every successful commit,
// unconditionally (see commitSeq). Guarded by cq.mu.
func (cq *CommitQueue) recordCommitSeq(path string) {
	cq.mu.Lock()
	defer cq.mu.Unlock()
	cq.commitClock++
	if cq.commitSeq == nil {
		cq.commitSeq = make(map[string]uint64)
	}
	cq.commitSeq[path] = cq.commitClock
}

// CommitSeq reports path's unconditional commit stamp (0 when none). Unlike
// LandedSeq this counts every completed upload: recovered/re-driven entries
// and payloads above the checksum limit included.
func (cq *CommitQueue) CommitSeq(path string) uint64 {
	if cq == nil {
		return 0
	}
	cq.mu.Lock()
	defer cq.mu.Unlock()
	return cq.commitSeq[path]
}

// CommitSeqsUnderPrefix snapshots every unconditional commit stamp under
// prefix.
func (cq *CommitQueue) CommitSeqsUnderPrefix(prefix string) map[string]uint64 {
	if cq == nil {
		return nil
	}
	cq.mu.Lock()
	defer cq.mu.Unlock()
	out := map[string]uint64{}
	for path, seq := range cq.commitSeq {
		if strings.HasPrefix(path, prefix) {
			out[path] = seq
		}
	}
	return out
}

// LandedSeq reports the landed landmark stamp for path (0 when none): the
// rename flow snapshots it before the transfer so the failure-record rekey
// can decline to move a record onto a destination whose re-driven upload
// has already landed — the landing fulfilled the obligation the record
// describes, and re-installing it after the fact would answer stale EIO
// forever.
func (cq *CommitQueue) LandedSeq(path string) uint64 {
	if cq == nil {
		return 0
	}
	cq.mu.Lock()
	defer cq.mu.Unlock()
	return cq.landed[path].lastUsed
}

// LandedSnapshotsForRename snapshots the commit stamps a rename's
// failure-record rekeys fence against: the destination name itself (no
// trailing slash, so a prefix scan alone misses it) plus every path under
// the destination prefix. These are the UNCONDITIONAL commit stamps — a
// re-driven upload that succeeds always advances them, whether or not the
// selective landed landmark was ever recorded (no snapshot ID, payloads
// above the checksum limit).
func (cq *CommitQueue) LandedSnapshotsForRename(newPath, newPrefix string) map[string]uint64 {
	if cq == nil {
		return nil
	}
	cq.mu.Lock()
	defer cq.mu.Unlock()
	out := map[string]uint64{}
	for path, seq := range cq.commitSeq {
		if path == newPath || strings.HasPrefix(path, newPrefix) {
			out[path] = seq
		}
	}
	return out
}

// LandedSeqsUnderPrefix snapshots every landed landmark stamp under prefix.
func (cq *CommitQueue) LandedSeqsUnderPrefix(prefix string) map[string]uint64 {
	if cq == nil {
		return nil
	}
	cq.mu.Lock()
	defer cq.mu.Unlock()
	out := map[string]uint64{}
	for path, landmark := range cq.landed {
		if strings.HasPrefix(path, prefix) {
			out[path] = landmark.lastUsed
		}
	}
	return out
}

// RenameTerminalFailure moves a completed rename's endpoint records with the
// object: the source's record (standalone ones included — their bytes sit
// under an even older name, and the retargeted handle's next barrier checks
// the NEW path) transfers to the destination, and the destination's previous
// record dies with its replaced incarnation. Clearing both the way the caller
// once did would strand a standalone witness at the dead source name.
func (cq *CommitQueue) RenameTerminalFailure(oldPath, newPath string, landedSnaps map[string]uint64) {
	if cq == nil || oldPath == newPath {
		return
	}
	cq.mu.Lock()
	defer cq.mu.Unlock()
	// The rekey runs after the transfer released its locks: the destination
	// worker may have landed the re-driven replacement in between, which
	// fulfills the source record's obligation (and clears the destination
	// record). Moving the record onto that landing would reinstall stale
	// EIO. A non-nil snapshot map arms the fence for the whole destination
	// universe (a path absent from the map simply had no landing yet: stamp
	// 0, and any landing since declines the move).
	if landedSnaps != nil && cq.commitSeq[newPath] > landedSnaps[newPath] {
		delete(cq.terminalFailures, oldPath)
		return
	}
	failure, ok := cq.terminalFailures[oldPath]
	dst, dstOK := cq.terminalFailures[newPath]
	delete(cq.terminalFailures, oldPath)
	delete(cq.terminalFailures, newPath)
	switch {
	case !ok:
		return
	case dstOK && dst.seq > failure.seq:
		// A newer failure was recorded at the destination meanwhile (the
		// re-driven upload terminally failed again): it is the live truth.
		cq.terminalFailures[newPath] = dst
	default:
		cq.terminalFailures[newPath] = failure
	}
}

// RenameTerminalFailuresPrefix rekeys every terminal-failure record under a
// renamed directory prefix. Standalone records have no pending entry at their
// own path (their bytes remain under the pre-move name), so neither the
// write-cache nor the pending-index prefix loop carries them — without this
// rekey, a retargeted handle's next strict Sync would check a dead name and
// certify data that never landed.
func (cq *CommitQueue) RenameTerminalFailuresPrefix(oldPrefix, newPrefix string, landedSnaps map[string]uint64) {
	if cq == nil || oldPrefix == newPrefix {
		return
	}
	cq.mu.Lock()
	defer cq.mu.Unlock()
	moved := make(map[string]pathCommitFailure, len(cq.terminalFailures))
	for path, failure := range cq.terminalFailures {
		if strings.HasPrefix(path, oldPrefix) {
			target := newPrefix + strings.TrimPrefix(path, oldPrefix)
			// Same landing fence as the endpoint rekey: a child's re-driven
			// upload that landed after the snapshot fulfilled that record's
			// obligation — drop the record instead of re-installing it over
			// the landing (absent from the map = no landing yet = stamp 0).
			if landedSnaps != nil && cq.commitSeq[target] > landedSnaps[target] {
				delete(cq.terminalFailures, path)
				continue
			}
			if dst, ok := cq.terminalFailures[target]; ok && dst.seq > failure.seq {
				delete(cq.terminalFailures, path)
				continue
			}
			moved[target] = failure
			delete(cq.terminalFailures, path)
		}
	}
	for path, failure := range moved {
		cq.terminalFailures[path] = failure
	}
}

// PathTerminalFailureSeq reports the monotonic stamp of the recorded terminal
// failure for path (0 when none). Callers capture it before a landing attempt
// and hand it to ClearTerminalFailureIfSeq so the landing cannot erase a
// failure recorded after it started.
func (cq *CommitQueue) PathTerminalFailureSeq(path string) uint64 {
	if cq == nil {
		return 0
	}
	cq.mu.Lock()
	defer cq.mu.Unlock()
	if failure, ok := cq.terminalFailures[path]; ok {
		return failure.seq
	}
	return 0
}

// ClearTerminalFailureIfSeq drops the recorded terminal failure only when it
// is still the one stamped seq: a record with a newer stamp belongs to a
// generation the just-landed image does not cover, and must keep answering
// EIO.
func (cq *CommitQueue) ClearTerminalFailureIfSeq(path string, seq uint64) {
	if cq == nil || seq == 0 {
		return
	}
	cq.mu.Lock()
	if failure, ok := cq.terminalFailures[path]; ok && failure.seq == seq {
		delete(cq.terminalFailures, path)
	}
	cq.mu.Unlock()
}

// ResolveSupersededConflict retires the durable conflict marker for path when
// a landed image provably contains it: supersedeGen is the pending-index
// generation the landing image composed over at staging time, and a conflict
// entry at or below that generation is obsolete once the image is remote. The
// generation fence keeps a newer staging's entry (and its preserved recovery
// bytes) untouched. The conflict entry's shadow bytes are intentionally left
// behind: the pending meta carries no shadow generation to fence a removal
// with, and a stale recovery copy is safe where a wrong removal is not.
func (cq *CommitQueue) ResolveSupersededConflict(path string, supersedeGen uint64) {
	if cq == nil || cq.index == nil || supersedeGen == 0 {
		return
	}
	meta, ok := cq.index.GetMeta(path)
	if !ok || meta == nil || meta.Kind != staging.PendingConflict {
		return
	}
	// A moved marker carries its own minted generation AND the source
	// generation of the failed image it describes (a rename move mints a
	// fresh generation without changing the image): a landing which
	// superseded EITHER generation recovered that image — most importantly
	// the source one every retained handle and pre-staged cache snapshot
	// recorded before the move.
	if meta.Generation > supersedeGen && (meta.SupersedesPendingGen == 0 || meta.SupersedesPendingGen > supersedeGen) {
		return
	}
	cq.index.RemoveIfGeneration(path, meta.Generation)
	safeLogPrintf("commit queue: resolved conflict marker for %s superseded by a landed image (gen %d <= %d)", path, meta.Generation, supersedeGen)
}

// PathTerminalFailure returns the most recent terminal commit failure recorded
// for path whose data never landed remotely (local pending state is
// preserved). Durability barriers consult it once the path goes idle: without
// it, a terminally failed path is indistinguishable from a landed commit and
// fsync would report success for data that exists only locally. When the
// in-memory record is absent (a fresh process, or eviction above the map
// bound), the durable PendingConflict marker onCommitTerminalFailure left
// behind answers instead: it survives restarts and re-enqueues nothing, so it
// is exactly the state a barrier must refuse to certify.
func (cq *CommitQueue) PathTerminalFailure(path string) error {
	if cq == nil {
		return nil
	}
	cq.mu.Lock()
	failure, ok := cq.terminalFailures[path]
	cq.mu.Unlock()
	if ok {
		return failure.err
	}
	if cq.index != nil {
		if meta, found := cq.index.GetMeta(path); found && meta != nil && meta.Kind == staging.PendingConflict {
			return errCommitTerminalRecovered
		}
	}
	return nil
}

// directPutThreshold returns the size limit below which commit queue workers
// use direct PUT (WriteCtxConditionalWithRevision) instead of multipart
// upload. Must match the server's inline_threshold — the server rejects
// simple PUTs for files at or above the threshold on S3-configured backends
// by requiring X-Dat9-Part-Checksums (multipart protocol). Read from the
// client's cached value to avoid surprise GET //status calls in the hot
// commit path; the FS layer is expected to have warmed the cache via the
// startup inlineThreshold call.
//
// Returns 0 when no server value has been negotiated yet. Callers that
// route on the result must treat 0 as "force multipart": the server may
// be configured below the historical 50KB default, and a fixed fallback
// would direct-PUT files the server then rejects. Multipart is always
// accepted.
func (cq *CommitQueue) directPutThreshold() int64 {
	if cq.client != nil {
		return cq.client.CachedSmallFileThreshold()
	}
	return 0
}

func (entry *CommitEntry) payloadBaseRevision() int64 {
	if entry == nil {
		return 0
	}
	if entry.PayloadBaseRevSet {
		return entry.PayloadBaseRev
	}
	return entry.BaseRev
}

func (entry *CommitEntry) bindPayload(data []byte) []byte {
	if entry == nil {
		return data
	}
	if !entry.payloadBound {
		entry.payload = append([]byte(nil), data...)
		entry.payloadBound = true
	}
	return entry.payload
}
