package server

import (
	"context"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
)

// defaultSemanticShardRefreshInterval is how often the shard resolver queries
// pod_registry to refresh the active pod ring. A shorter interval reacts
// faster to pod add/remove; a longer interval reduces meta DB load. The outbox
// durable signal guarantees no work is lost during the transition window.
const defaultSemanticShardRefreshInterval = 5 * time.Second

// semanticShardResolver determines whether the current pod owns a given
// tenant for semantic task processing. It periodically queries pod_registry
// for the set of active pods, builds a sorted ring, and assigns tenants to
// pods via jump consistent hashing — minimizing tenant migration when pods
// are added or removed (~1/N tenants move).
//
// The resolver is safe for concurrent use. The ownsTenant closure captures
// a snapshot of the ring so callers never block on a refresh.
type semanticShardResolver struct {
	metaStore *meta.Store
	podID     string
	refresh   time.Duration

	mu       sync.RWMutex
	ring     []string // sorted active pod IDs
	myRank   int      // index of this pod in ring, -1 if not found
	numShard int      // len(ring)
}

// newSemanticShardResolver creates a shard resolver. If podID is empty or
// metaStore is nil, ownsTenant always returns true (single-pod / fallback mode).
func newSemanticShardResolver(metaStore *meta.Store, podID string, refresh time.Duration) *semanticShardResolver {
	if refresh <= 0 {
		refresh = defaultSemanticShardRefreshInterval
	}
	r := &semanticShardResolver{
		metaStore: metaStore,
		podID:     podID,
		refresh:   refresh,
		myRank:    -1,
	}
	// In single-pod mode (no metaStore or no podID), always own all tenants.
	if metaStore == nil || podID == "" {
		r.numShard = 1
		r.myRank = 0
	}
	return r
}

// Start launches the background refresh goroutine and performs an initial
// synchronous refresh so the ring is populated before live traffic. It blocks
// until the first refresh completes (or fails, in which case the resolver
// falls back to owning everything).
func (r *semanticShardResolver) Start(ctx context.Context) {
	if r == nil || r.metaStore == nil || r.podID == "" {
		return
	}
	// Synchronous initial refresh so the ring is ready before traffic.
	r.refreshOnce(ctx)
	go r.loop(ctx)
}

func (r *semanticShardResolver) loop(ctx context.Context) {
	ticker := time.NewTicker(r.refresh)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.refreshOnce(ctx)
		}
	}
}

func (r *semanticShardResolver) refreshOnce(ctx context.Context) {
	podIDs, err := r.metaStore.ListAllActivePodIDs(ctx)
	if err != nil {
		logger.Warn(ctx, "semantic_shard_refresh_failed", zap.Error(err))
		return
	}
	// ListAllActivePodIDs already returns sorted by pod_id.
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ring = podIDs
	r.numShard = len(podIDs)
	r.myRank = sort.SearchStrings(podIDs, r.podID)
	if r.myRank < len(podIDs) && podIDs[r.myRank] == r.podID {
		// Found — rank is correct.
	} else {
		// This pod is not yet in the active set; own nothing until registered.
		r.myRank = -1
	}
	logger.Info(ctx, "semantic_shard_refreshed",
		zap.Int("num_pods", r.numShard),
		zap.Int("my_rank", r.myRank))
}

// ownsTenant reports whether the current pod is responsible for processing
// semantic tasks for the given tenant. In single-pod mode it always returns
// true. When the pod is not yet registered in the ring, it returns false to
// avoid racing with a fresh registration — the outbox signal is durable so
// the task will be claimed once the pod appears in the ring.
func (r *semanticShardResolver) ownsTenant(tenantID string) bool {
	if r == nil {
		return true
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.numShard <= 0 || r.myRank < 0 {
		// No ring or not registered: in single-pod mode own everything;
		// otherwise own nothing (waiting for registration).
		if r.metaStore == nil || r.podID == "" {
			return true
		}
		return false
	}
	h := tenantHash(tenantID)
	bucket := jumpConsistentHash(h, uint64(r.numShard))
	return int(bucket) == r.myRank
}

// ownsTenant returns the ownsTenant closure suitable for injection into the
// semantic worker and outbox poller.
func (r *semanticShardResolver) ownsTenantFn() func(string) bool {
	if r == nil {
		return func(string) bool { return true }
	}
	return r.ownsTenant
}

// tenantHash returns a 64-bit hash of the tenant ID using FNV-1a.
func tenantHash(tenantID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(tenantID))
	return h.Sum64()
}

// jumpConsistentHash implements the Jump Consistent Hash algorithm from
// "A Fast, Minimal Memory, Consistent Hashing Algorithm" (Lamping & Veach,
// 2014). Given a key and numBuckets, it returns a bucket in [0, numBuckets).
// When numBuckets increases, only ~1/numBuckets of keys move to a new bucket,
// minimizing tenant migration on pod add/remove.
func jumpConsistentHash(key uint64, numBuckets uint64) uint64 {
	var b, j int64
	b = -1
	j = 0
	for j < int64(numBuckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1<<31)) / float64((key>>33)+1)))
	}
	return uint64(b)
}