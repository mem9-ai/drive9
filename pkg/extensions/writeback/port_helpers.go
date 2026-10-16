package writeback

import (
	"context"
	"errors"
	"fmt"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

// maxCommitQueuePending bounds queued entries before Enqueue backpressures.
const maxCommitQueuePending = 500

// uploadTimeout bounds one queued upload.
const uploadTimeout = 60 * time.Second

// probeDeadline bounds the detached existence probe at open (v1's
// interrupt-safe mutations carried their own deadline once detached from
// the FUSE cancel channel).
const probeDeadline = 30 * time.Second

// Mode helpers (the staging package keeps its own unexported copies).
const (
	defaultRegularFileMode   uint32 = 0o644
	posixPermissionModeMask  uint32 = 0o7777
	remotePermissionModeMask uint32 = 0o777
)

// retryPostUploadMode retries a mode application that 404s against
// backend replica lag right after an upload.
const (
	postUploadModeAttempts     = 5
	postUploadModeInitialDelay = 10 * time.Millisecond
)

const (
	perfRemoteStat perfRemoteOp = iota
	perfRemoteRead
	perfRemoteWrite
	perfRemoteMutation
	perfRemoteOpCount
)

var errLayerRolledBack = errors.New("fs layer has been rolled back; remount required")

type safeLogValue struct{ value any }

type atomicUint64 struct{ v atomic.Uint64 }

type perfRemoteOp int

type fusePerfCounters struct {
	enabled               atomic.Bool
	forward               func(name string, err error, dur time.Duration, bytes uint64)
	commitRetry           atomicUint64
	commitFailure         atomicUint64
	commitSuccess         atomicUint64
	commitEnqueue         atomicUint64
	commitEnqueueError    atomicUint64
	commitDrainCount      atomicUint64
	commitDrainTotalNS    atomicUint64
	commitTerminalFailure atomicUint64

	uploaderSubmit       atomicUint64
	uploaderSuccess      atomicUint64
	uploaderFailure      atomicUint64
	uploaderSyncFallback atomicUint64
	uploaderDrainCount   atomicUint64
	uploaderDrainTotal   atomicUint64
}

// snapshot renders the commit counters the ported queue tests assert on.
type fusePerfSnapshotShim struct {
	Counters map[string]uint64
}

// String renders the wrapped value with control whitespace escaped.
func (v safeLogValue) String() string {
	switch t := v.value.(type) {
	case string:
		return strings.NewReplacer("\\n", "\\\\n", "\\r", "\\\\r", "\\t", "\\\\t").Replace(t)
	case error:
		return strings.NewReplacer("\\n", "\\\\n", "\\r", "\\\\r", "\\t", "\\\\t").Replace(t.Error())
	}
	return fmt.Sprint(v.value)
}

// releaseTimeout is the size-proportional upload budget for close-path
// commits (60s floor, 15min ceiling at 5MB/s).
func releaseTimeout(size int64) time.Duration {
	const (
		minTimeout = 60 * time.Second
		maxTimeout = 15 * time.Minute
		bandwidth  = 5 << 20 // 5 MB/s
	)
	t := time.Duration(size/bandwidth) * time.Second
	if t < minTimeout {
		t = minTimeout
	}
	if t > maxTimeout {
		t = maxTimeout
	}
	return t
}

func remoteChmodMode(mode uint32) uint32 {
	return mode & remotePermissionModeMask
}

func shouldApplyRemoteMode(kind staging.PendingKind, hasMode bool, mode uint32) bool {
	if !hasMode {
		return false
	}
	if kind == staging.PendingNew && mode&posixPermissionModeMask == defaultRegularFileMode {
		return false
	}
	return true
}

// committedRevisionFromExpectedRevision maps an expected-revision
// constraint to its committed form.
func committedRevisionFromExpectedRevision(expectedRevision int64) (int64, bool) {
	if expectedRevision < 0 {
		return 0, false
	}
	return expectedRevision + 1, true
}

// layerCacheIdentity extracts the layer incarnation an entry was staged
// against.
func layerCacheIdentity(entry *client.FSLayerEntry, fallbackLayer string) staging.LayerCacheIdentity {
	if entry == nil {
		return staging.LayerCacheIdentity{}
	}
	id := entry.LayerID
	if id == "" {
		id = fallbackLayer
	}
	return staging.LayerCacheIdentity{LayerID: id, EntrySeq: entry.EntrySeq}
}

func retryPostUploadMode(ctx context.Context, apply func() error) error {
	var lastErr error
	for attempt := 0; attempt < postUploadModeAttempts; attempt++ {
		lastErr = apply()
		if lastErr == nil {
			return nil
		}
		if !client.IsNotFound(lastErr) || attempt == postUploadModeAttempts-1 {
			return lastErr
		}
		delay := postUploadModeInitialDelay << attempt
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}

func safeLogPrintf(format string, args ...any) {
	escapeLogArgs(args)
	log.Printf(format, args...)
}

func escapeLogArgs(args []any) {
	for i, arg := range args {
		switch arg.(type) {
		case string:
			args[i] = safeLogValue{value: arg}
		case error:
			args[i] = safeLogValue{value: arg}
		}
	}
}

func (a *atomicUint64) add(n uint64) { a.v.Add(n) }

func (a *atomicUint64) load() uint64 { return a.v.Load() }

func (p *fusePerfCounters) snapshot() fusePerfSnapshotShim {
	shim := fusePerfSnapshotShim{Counters: map[string]uint64{}}
	if p == nil {
		return shim
	}
	shim.Counters["commit_retry"] = p.commitRetry.load()
	shim.Counters["commit_failure"] = p.commitFailure.load()
	shim.Counters["commit_success"] = p.commitSuccess.load()
	shim.Counters["commit_enqueue"] = p.commitEnqueue.load()
	shim.Counters["commit_enqueue_error"] = p.commitEnqueueError.load()
	shim.Counters["commit_drain_count"] = p.commitDrainCount.load()
	shim.Counters["commit_terminal_failure"] = p.commitTerminalFailure.load()
	return shim
}

func (p *fusePerfCounters) recordRemoteOp(op perfRemoteOp, err error, dur time.Duration, bytes uint64) {
	if p == nil {
		return
	}
	// Forward to the facade-owned remote-op feed when attached (v1
	// perfRecordRemote); the histogram internals live there.
	if p.forward != nil {
		p.forward(remoteOpName(op), err, dur, bytes)
	}
}

// remoteOpName maps the writeback remote families onto the perf counter
// names.
func remoteOpName(op perfRemoteOp) string {
	switch op {
	case perfRemoteStat:
		return "stat"
	case perfRemoteRead:
		return "read"
	case perfRemoteWrite:
		return "write"
	default:
		return "mutation"
	}
}

// setForward attaches the facade-owned remote-op recorder.
func (p *fusePerfCounters) setForward(fn func(name string, err error, dur time.Duration, bytes uint64)) {
	if p != nil {
		p.forward = fn
	}
}
