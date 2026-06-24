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
	// DEPRECATED: The in-process mutation queue provides per-tenant FIFO ordering
	// only within a single backend instance. In multi-pod deployments, cross-pod
	// ordering is not guaranteed (see logAndEnqueueMutation comment). The newer
	// quota_outbox_worker.go uses FOR UPDATE SKIP LOCKED + lease for multi-pod
	// safe claim-based processing. The mutation replay worker (leader-gated since
	// PR #601) provides a convergence backstop for the durable mutation log.
	// This queue remains as the primary in-process apply path for same-pod writes.
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
	b.mutationQueue = make(chan func(), mutationQueueSize)
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
					fn()
				default:
					return
				}
			}
		case fn := <-b.mutationQueue:
			fn()
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
func (b *Dat9Backend) enqueueMutation(fn func()) {
	if b.mutationQueue == nil {
		// No async queue — run inline (test/fallback path).
		fn()
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
