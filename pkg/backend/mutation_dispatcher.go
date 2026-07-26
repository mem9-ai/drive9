package backend

import (
	"context"
	"database/sql"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
)

const (
	// mutationDispatcherShards is the number of shard workers. Each tenant
	// maps to exactly one shard (FNV-1a 32 of tenantID mod K), so per-tenant
	// FIFO is preserved while each batch still aggregates across tenants:
	// under uniform load per-tenant queues almost never accumulate, so only
	// cross-tenant grouping reduces the number of metadb transactions.
	mutationDispatcherShards = 4
	// mutationDispatcherShardQueueSize buffers items per shard. A full
	// buffer blocks the enqueue path (under mutationMu), applying the same
	// backpressure the per-backend 256-slot queue applied before.
	mutationDispatcherShardQueueSize = 4096
	// mutationDispatcherMaxBatchExtra caps how many extra items a shard
	// worker drains non-blocking after the first blocking receive, bounding
	// a batch transaction to at most 128 mutation applies plus one batch
	// mark statement.
	mutationDispatcherMaxBatchExtra = 127
)

// quotaMutationItem is one durably-logged central quota mutation awaiting
// async apply. The batch transaction runs on store (captured from the owning
// backend at enqueue time); backend is retained for the post-apply
// bookkeeping (quota cache invalidation, pending-delta clearing, metrics) and
// for mutationWG accounting — the owning backend outlives its items because
// Close waits on mutationWG.
type quotaMutationItem struct {
	backend        *Dat9Backend
	store          MetaQuotaStore
	tenantID       string
	tidbCloudOrgID string
	mutationType   string
	logID          int64
	pending        quotaPendingDeltas
	apply          func(context.Context, *sql.Tx) (quotaCounterDeltas, error)
	// raw is a pre-built closure submitted through the legacy enqueueMutation
	// API (unit tests). Raw items bypass batching and run one by one.
	raw func(context.Context)
}

// mutationDispatcher is the process-global cross-tenant quota mutation
// apply engine. K shard workers each own a buffered channel; a shard worker
// blocks on the first item, then non-blockingly drains up to
// mutationDispatcherMaxBatchExtra more and applies the whole batch in a
// single metadb transaction (per underlying store identity), collapsing
// what used to be one begin+commit pair per file mutation.
type mutationDispatcher struct {
	shards [mutationDispatcherShards]chan quotaMutationItem
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

var (
	mutationDispatcherMu   sync.Mutex
	mutationDispatcherInst *mutationDispatcher
)

// StartMutationDispatcher launches the process-global shard workers.
// Idempotent. Called from server wiring in multi-tenant mode
// (startNotifyInfrastructure); unit tests start it explicitly and stop it via
// t.Cleanup. When the dispatcher is not running, enqueueMutationItem applies
// inline — the pre-dispatcher synchronous behavior.
func StartMutationDispatcher() {
	mutationDispatcherMu.Lock()
	defer mutationDispatcherMu.Unlock()
	if mutationDispatcherInst != nil {
		return
	}
	// The worker context is a traced background context, never a request
	// context: mutation apply must outlive the request that logged it.
	ctx, cancel := context.WithCancel(backgroundWithTrace())
	d := &mutationDispatcher{cancel: cancel}
	for i := range d.shards {
		d.shards[i] = make(chan quotaMutationItem, mutationDispatcherShardQueueSize)
		metrics.RecordMutationDispatcherQueue(i, 0, mutationDispatcherShardQueueSize)
		d.wg.Add(1)
		go d.runShard(ctx, i, d.shards[i])
	}
	mutationDispatcherInst = d
}

// StopMutationDispatcher stops the shard workers and waits for them to exit.
// Idempotent. It must run only after every backend has been Closed (backend
// Close waits for its queued items, so shard channels are already drained);
// the server binary defers it right after pool.Close for that reason. Any
// item still queued is finished on a fresh background context before exit —
// and would otherwise be recovered by MutationReplayWorker from the durable
// log.
func StopMutationDispatcher() {
	mutationDispatcherMu.Lock()
	d := mutationDispatcherInst
	mutationDispatcherInst = nil
	mutationDispatcherMu.Unlock()
	if d == nil {
		return
	}
	d.cancel()
	d.wg.Wait()
}

func currentMutationDispatcher() *mutationDispatcher {
	mutationDispatcherMu.Lock()
	defer mutationDispatcherMu.Unlock()
	return mutationDispatcherInst
}

// enqueue blocks until the tenant's shard buffer accepts the item; this is
// the intended backpressure point (callers hold mutationMu, so per-tenant
// FIFO matches durable log_id order).
func (d *mutationDispatcher) enqueue(item quotaMutationItem) {
	shard := int(mutationDispatcherShardIndex(item.tenantID))
	start := time.Now()
	d.shards[shard] <- item
	metrics.RecordMutationDispatcherEnqueueBlocked(shard, time.Since(start))
	metrics.RecordMutationDispatcherQueue(shard, len(d.shards[shard]), cap(d.shards[shard]))
}

func mutationDispatcherShardIndex(tenantID string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(tenantID))
	return h.Sum32() % mutationDispatcherShards
}

func (d *mutationDispatcher) runShard(ctx context.Context, shard int, ch chan quotaMutationItem) {
	defer d.wg.Done()
	for {
		select {
		case <-ctx.Done():
			d.drainShard(ch)
			metrics.RecordMutationDispatcherQueue(shard, 0, cap(ch))
			return
		case first := <-ch:
			batch := collectMutationBatch(ch, first)
			metrics.RecordMutationDispatcherQueue(shard, len(ch), cap(ch))
			processMutationBatch(ctx, batch)
		}
	}
}

// drainShard finishes whatever is still queued after shutdown, on a fresh
// background context (the worker context is already cancelled).
func (d *mutationDispatcher) drainShard(ch chan quotaMutationItem) {
	ctx := backgroundWithTrace()
	for {
		select {
		case first := <-ch:
			processMutationBatch(ctx, collectMutationBatch(ch, first))
		default:
			return
		}
	}
}

func collectMutationBatch(ch chan quotaMutationItem, first quotaMutationItem) []quotaMutationItem {
	batch := make([]quotaMutationItem, 0, mutationDispatcherMaxBatchExtra+1)
	batch = append(batch, first)
	for len(batch) < mutationDispatcherMaxBatchExtra+1 {
		select {
		case item := <-ch:
			batch = append(batch, item)
		default:
			return batch
		}
	}
	return batch
}

// processMutationBatch applies one shard batch. Raw items (legacy
// enqueueMutation closures, test-only) run individually in received order.
// Batchable items are grouped by store identity (QuotaStoreIdentity) —
// preserving received order within each group — because one transaction can
// only run on one underlying store; groups with a single item take the
// legacy per-item path, larger groups share one transaction.
func processMutationBatch(ctx context.Context, batch []quotaMutationItem) {
	metrics.RecordMutationDispatcherBatch(len(batch), false)
	groups := make([]mutationStoreGroup, 0, 1)
	groupIndexByIdentity := make(map[any]int)
	for i := range batch {
		item := &batch[i]
		if item.raw != nil {
			item.raw(ctx)
			item.backend.mutationWG.Done()
			continue
		}
		identity := item.store.QuotaStoreIdentity()
		idx, ok := groupIndexByIdentity[identity]
		if !ok {
			idx = len(groups)
			groupIndexByIdentity[identity] = idx
			groups = append(groups, mutationStoreGroup{store: item.store})
		}
		groups[idx].items = append(groups[idx].items, item)
	}
	for i := range groups {
		processMutationStoreGroup(ctx, groups[i].store, groups[i].items)
	}
}

// mutationStoreGroup is one identity group's share of a shard batch. store
// is the first item's handle: adapters over the same underlying store are
// interchangeable because item apply closures receive the tx and delegate,
// so any handle of the group can host the transaction.
type mutationStoreGroup struct {
	store MetaQuotaStore
	items []*quotaMutationItem
}

// processMutationStoreGroup applies one store-identity group's share of a
// shard batch in a single transaction: every item's apply in received order
// (create/overwrite mutations on the same file are non-commutative;
// per-tenant FIFO is guaranteed by mutationMu plus one shard per tenant),
// then one batch mark.
//
// Applies return their tenant_quota_usage counter deltas instead of
// executing them inline; the runner aggregates them per tenant (items are
// cross-tenant) and flushes them after the last apply in SORTED tenant_id
// order — a deterministic global lock order so two pods' batch transactions
// cannot deadlock each other by locking the per-tenant hot rows in different
// orders. Deferring the counter updates to the end of the tx ("hot row
// last") shrinks the lock-hold window on the single per-tenant
// tenant_quota_usage row from the whole batch to just before commit, and N
// mutations for one tenant collapse into one UPDATE.
//
// On any failure — including InnoDB killing the batch as a cross-pod
// deadlock victim (two pods' batch transactions lock tenant_file_meta rows
// in different orders) or the batch mark detecting an already-applied row —
// every item falls back to the legacy per-item path: applyQuotaMutation
// opens its own transaction per item, logs per failure, and leaves failed
// items pending for MutationReplayWorker. This preserves per-item failure
// isolation for poison items and guarantees deadlock losers still progress.
func processMutationStoreGroup(ctx context.Context, store MetaQuotaStore, items []*quotaMutationItem) {
	if len(items) == 1 {
		item := items[0]
		item.backend.applyQuotaMutation(ctx, item.mutationType, item.logID, item.pending, item.apply)
		item.backend.mutationWG.Done()
		return
	}
	ids := make([]int64, len(items))
	for i, item := range items {
		ids[i] = item.logID
	}
	err := store.InTx(ctx, func(tx *sql.Tx) error {
		deltasByTenant := make(map[string]quotaCounterDeltas)
		for _, item := range items {
			deltas, err := item.apply(ctx, tx)
			if err != nil {
				return err
			}
			agg := deltasByTenant[item.tenantID]
			agg.add(deltas)
			deltasByTenant[item.tenantID] = agg
		}
		tenantIDs := make([]string, 0, len(deltasByTenant))
		for tenantID := range deltasByTenant {
			tenantIDs = append(tenantIDs, tenantID)
		}
		sort.Strings(tenantIDs)
		for _, tenantID := range tenantIDs {
			if err := applyQuotaCounterDeltasTx(store, tx, tenantID, deltasByTenant[tenantID]); err != nil {
				return err
			}
		}
		return store.MarkMutationsAppliedTx(tx, ids)
	})
	if err == nil {
		for _, item := range items {
			item.backend.finishMutationItemOK(item)
		}
		return
	}
	logger.Warn(ctx, "central_quota_mutation_batch_apply_failed",
		zap.Int("batch_size", len(items)),
		zap.Error(err))
	metrics.RecordMutationDispatcherBatch(0, true)
	for _, item := range items {
		item.backend.applyQuotaMutation(ctx, item.mutationType, item.logID, item.pending, item.apply)
		item.backend.mutationWG.Done()
	}
}

// finishMutationItemOK mirrors the success tail of applyQuotaMutation after
// the batch transaction commits, and releases the item's mutationWG slot.
func (b *Dat9Backend) finishMutationItemOK(item *quotaMutationItem) {
	defer b.mutationWG.Done()
	if b.quotaUsageCache != nil {
		b.quotaUsageCache.invalidate()
	}
	b.clearPendingCentralMutationDeltas(item.pending.storageDelta, item.pending.fileDelta, item.pending.mediaDelta)
	b.recordTenantOperation("central_quota", item.mutationType, "ok", 0)
}
