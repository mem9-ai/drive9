package backend

import (
	"context"
)

// startMutationWorker enrolls the backend in async central quota mutation
// apply. Enqueued mutations are handed to the process-global mutation
// dispatcher (mutation_dispatcher.go), which batches applies cross-tenant
// into single metadb transactions while preserving per-tenant FIFO (one
// shard per tenant). When the dispatcher is not running (unit tests,
// drive9-server-local) or the backend is closing, items apply inline,
// exactly like the pre-dispatcher per-backend queue fallback did.
//
// Cross-instance ordering is not guaranteed; see logAndEnqueueMutation.
// The durable quota_mutation_log plus MutationReplayWorker provides the
// convergence backstop.
//
// Called once from SetMetaQuotaStore when server quota is active.
func (b *Dat9Backend) startMutationWorker() {
	b.mutationMu.Lock()
	defer b.mutationMu.Unlock()
	b.mutationWorkerOn = true
	b.mutationClosed = false
}

// enqueueMutation submits a raw mutation apply closure. This is the
// pre-dispatcher per-backend queue API kept for existing unit tests;
// production code enqueues structured items via logAndEnqueueMutation.
//
// Callers hold mutationMu (or are otherwise serialized), so the closed check
// and mutationWG.Add stay ordered against stopMutationWorker's Wait.
func (b *Dat9Backend) enqueueMutation(ctx context.Context, fn func(context.Context)) {
	b.enqueueMutationItem(ctx, quotaMutationItem{backend: b, tenantID: b.tenantID, raw: fn})
}

// enqueueMutationItem routes the item to the tenant's dispatcher shard, or
// applies it inline when async apply is unavailable (dispatcher not started,
// backend closing, or worker never started). The mutation log entry has
// already been durably written by the caller (logQuotaMutation), so if the
// process crashes before the item is applied, MutationReplayWorker recovers
// it.
//
// Callers must hold mutationMu: mutationWG.Add must not start once
// stopMutationWorker has set mutationClosed and entered Wait.
func (b *Dat9Backend) enqueueMutationItem(ctx context.Context, item quotaMutationItem) {
	d := currentMutationDispatcher()
	if !b.mutationWorkerOn || b.mutationClosed || d == nil {
		b.applyQuotaMutationItem(ctx, &item)
		return
	}
	b.mutationWG.Add(1)
	d.enqueue(item)
}

// applyQuotaMutationItem applies an item synchronously (the inline fallback).
func (b *Dat9Backend) applyQuotaMutationItem(ctx context.Context, item *quotaMutationItem) {
	if item.raw != nil {
		item.raw(ctx)
		return
	}
	b.applyQuotaMutation(ctx, item.mutationType, item.logID, item.pending, item.apply)
}

// stopMutationWorker blocks new async enqueues (they fall back to inline
// apply) and waits for already-enqueued items to be fully processed by the
// dispatcher — including post-apply bookkeeping — so Close never abandons
// in-flight quota mutations. Idempotent.
func (b *Dat9Backend) stopMutationWorker() {
	b.mutationMu.Lock()
	if !b.mutationWorkerOn {
		b.mutationMu.Unlock()
		return
	}
	// Set under mutationMu: enqueueMutationItem checks mutationClosed under
	// the same mutex, so no mutationWG.Add can start once Wait begins.
	b.mutationClosed = true
	b.mutationMu.Unlock()
	b.mutationWG.Wait()
	b.mutationMu.Lock()
	b.mutationWorkerOn = false
	b.mutationMu.Unlock()
}
