package server

import (
	"context"
	"hash/fnv"
	"sort"
	"sync/atomic"
	"time"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"go.uber.org/zap"
)

// jumpConsistentHash implements the standard jump consistent hashing
// algorithm (Lamping & Vee 2014). It maps a 64-bit key onto [0, numBuckets)
// with O(1) space and even distribution. When buckets are added/removed only
// ~1/n of keys remap, so tenant→pod ownership is stable across ring changes.
func jumpConsistentHash(key uint64, numBuckets int) int {
	if numBuckets <= 0 {
		return 0
	}
	var b, j int64 = -1, 0
	for j < int64(numBuckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1<<31)) / float64((key>>33)+1)))
	}
	return int(b)
}

// tenantHashKey derives a stable 64-bit key for a tenant ID using FNV-1a. The
// same tenant always maps to the same key regardless of process restart.
func tenantHashKey(tenantID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(tenantID))
	return h.Sum64()
}

// semanticShardResolver determines which pod in the cluster owns a given
// tenant for sharded work (semantic, file_gc, quota). It maintains a sorted
// ring of active pod IDs (refreshed from pod_registry every 5s) and uses jump
// consistent hashing over the ring index to pick the owner.
//
// In single-pod mode (no meta store or only one active pod), ownsTenant always
// returns true — this pod owns all sharded work.
type semanticShardResolver struct {
	metaStore *meta.Store
	selfID    string

	// refreshInterval is how often the active pod ring is refreshed.
	refreshInterval time.Duration

	// pods is the current sorted ring of active pod IDs. Stored as an atomic
	// pointer so ownsTenant reads it lock-free.
	pods atomic.Pointer[[]string]

	cancel context.CancelFunc
	done   chan struct{}
}

const (
	defaultShardRefreshInterval = 5 * time.Second
)

// newSemanticShardResolver creates a shard resolver. selfID is this pod's
// identifier in pod_registry. When metaStore is nil or selfID is empty, the
// resolver operates in single-pod mode (owns everything).
func newSemanticShardResolver(metaStore *meta.Store, selfID string, refreshInterval time.Duration) *semanticShardResolver {
	if refreshInterval <= 0 {
		refreshInterval = defaultShardRefreshInterval
	}
	r := &semanticShardResolver{
		metaStore:       metaStore,
		selfID:          selfID,
		refreshInterval: refreshInterval,
	}
	return r
}

// Start performs an initial synchronous refresh and launches the background
// refresh loop. The initial refresh must complete before live traffic so
// ownsTenant has a valid ring on startup.
func (r *semanticShardResolver) Start(ctx context.Context) {
	if r == nil || r.metaStore == nil || r.cancel != nil {
		return
	}
	r.refresh(ctx)
	loopCtx, cancel := context.WithCancel(backgroundWithTrace(ctx))
	r.cancel = cancel
	r.done = make(chan struct{})
	go r.refreshLoop(loopCtx)
}

// Stop cancels the refresh loop and waits for it to exit.
func (r *semanticShardResolver) Stop() {
	if r == nil || r.cancel == nil {
		return
	}
	r.cancel()
	<-r.done
	r.cancel = nil
}

func (r *semanticShardResolver) refreshLoop(ctx context.Context) {
	defer close(r.done)
	ticker := time.NewTicker(r.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.refresh(ctx)
		}
	}
}

func (r *semanticShardResolver) refresh(ctx context.Context) {
	if r.metaStore == nil {
		return
	}
	pods, err := r.metaStore.ListAllActivePodIDs(ctx)
	if err != nil {
		logger.Warn(ctx, "shard_resolver_refresh_failed", zap.Error(err))
		return
	}
	sort.Strings(pods)
	r.pods.Store(&pods)
	logger.Debug(ctx, "shard_resolver_refreshed",
		zap.Int("active_pods", len(pods)),
		zap.String("self_id", r.selfID))
}

// ownsTenant reports whether this pod owns sharded work (semantic/file_gc/
// quota) for the given tenant. Ownership is determined by jump consistent hash
// over the active pod ring.
//
// Single-pod mode (no meta store, empty selfID, or ring with ≤1 pod) always
// returns true: this pod owns everything.
func (r *semanticShardResolver) ownsTenant(tenantID string) bool {
	if r == nil || r.metaStore == nil || r.selfID == "" {
		return true
	}
	podsPtr := r.pods.Load()
	if podsPtr == nil {
		// Ring not yet refreshed: treat as single-pod (owns everything) to
		// avoid dropping work during startup. The safety-net scan catches
		// any work this pod skipped while the ring was empty.
		return true
	}
	pods := *podsPtr
	if len(pods) <= 1 {
		return true
	}
	idx := jumpConsistentHash(tenantHashKey(tenantID), len(pods))
	return pods[idx] == r.selfID
}

// ownsTenantFn returns ownsTenant as a function value, suitable for wiring
// into the poller and worker. The returned function reads the current ring
// snapshot on each call so it picks up ring refreshes automatically.
func (r *semanticShardResolver) ownsTenantFn() func(string) bool {
	if r == nil {
		return func(string) bool { return true }
	}
	return r.ownsTenant
}
