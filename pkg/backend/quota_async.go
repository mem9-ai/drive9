package backend

import (
	"context"

	"github.com/mem9-ai/drive9/pkg/logger"
	"go.uber.org/zap"
)

const (
	// mutationQueueSize is the buffer size for the async mutation channel.
	// Sized to absorb short bursts without blocking. If the buffer is full,
	// the mutation is applied inline (same as before) so no mutation is lost.
	mutationQueueSize = 256
	// mutationWorkers is the number of goroutines draining the mutation queue.
	mutationWorkers = 2
)

// startMutationWorker initializes the async mutation queue and workers.
// Called once from SetMetaQuotaStore when server quota is active.
func (b *Dat9Backend) startMutationWorker() {
	if b.mutationQueue != nil {
		return // already started
	}
	ctx, cancel := context.WithCancel(context.Background())
	b.mutationStop = cancel
	b.mutationQueue = make(chan func(), mutationQueueSize)
	for i := 0; i < mutationWorkers; i++ {
		b.mutationWG.Add(1)
		go b.drainMutations(ctx)
	}
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

// enqueueMutation submits a mutation function to the async queue.
// If the queue is full, the mutation is executed inline to ensure it is
// never dropped. The mutation log + replay worker provides crash recovery.
func (b *Dat9Backend) enqueueMutation(fn func()) {
	if b.mutationQueue == nil {
		// No async queue — run inline (test/fallback path).
		fn()
		return
	}
	select {
	case b.mutationQueue <- fn:
		// Enqueued for async execution.
	default:
		// Queue full — apply inline to avoid dropping.
		logger.Warn(context.Background(), "mutation_queue_full_inline_fallback",
			zap.String("tenant_id", b.tenantID))
		fn()
	}
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
