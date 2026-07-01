package backend

import (
	"context"
)

const (
	// mutationQueueSize is the buffer size for the async mutation channel.
	// Sized to absorb short bursts without blocking the write path. A single
	// worker drains the channel to preserve per-tenant FIFO ordering
	// (UpsertFileMetaTx is not commutative across create/overwrite).
	//
	// The in-process mutation queue provides per-tenant FIFO ordering only
	// within a single backend instance. In multi-pod deployments, cross-pod
	// ordering is not guaranteed (see logAndEnqueueMutation comment). The
	// durable quota_mutation_log plus MutationReplayWorker provides the
	// convergence backstop.
	mutationQueueSize = 256
)

// startMutationWorker initializes the async mutation queue and a single
// sequencing worker. A single worker guarantees that within this backend
// instance, per-tenant mutation apply order matches the log insertion order
// (which is serialized by the caller's mutationMu + logQuotaMutation).
// Cross-instance ordering is not guaranteed; see logAndEnqueueMutation.
//
// Called once from SetMetaQuotaStore when server quota is active.
func (b *Dat9Backend) startMutationWorker() {
	if b.mutationQueue != nil {
		return // already started
	}
	ctx, cancel := context.WithCancel(context.Background())
	b.mutationStop = cancel
	b.mutationQueue = make(chan func(context.Context), mutationQueueSize)
	b.mutationWG.Add(1)
	go b.drainMutations(ctx)
}

func (b *Dat9Backend) drainMutations(ctx context.Context) {
	defer b.mutationWG.Done()
	for {
		select {
		case <-ctx.Done():
			// Drain remaining items before exiting.
			for {
				select {
				case fn := <-b.mutationQueue:
					fn(ctx)
				default:
					return
				}
			}
		case fn := <-b.mutationQueue:
			fn(ctx)
		}
	}
}

// enqueueMutation submits a mutation apply function to the async queue.
// The mutation log entry has already been durably written by the caller
// (logQuotaMutation), so if this enqueue blocks or the process crashes
// before the worker runs, MutationReplayWorker recovers the pending entry.
//
// If the queue is not wired (tests), the function runs inline.
// The channel send blocks if the buffer is full, preserving FIFO ordering;
// the 256-slot buffer makes blocking extremely unlikely in practice.
func (b *Dat9Backend) enqueueMutation(ctx context.Context, fn func(context.Context)) {
	if b.mutationQueue == nil {
		// No async queue — run inline (test/fallback path).
		fn(ctx)
		return
	}
	b.mutationQueue <- fn
}

// stopMutationWorker shuts down the async mutation queue and waits for
// all pending mutations to drain.
func (b *Dat9Backend) stopMutationWorker() {
	if b.mutationStop != nil {
		b.mutationStop()
		b.mutationWG.Wait()
		b.mutationStop = nil
		b.mutationQueue = nil
	}
}
