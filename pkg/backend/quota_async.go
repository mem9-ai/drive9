package backend

import (
	"context"
)

const (
	// mutationQueueSize is the buffer size for the async mutation channel.
	// Sized to absorb short bursts. The channel accepts mutationEntry structs
	// (not closures) so the write hot path does zero DB work — the background
	// worker performs both the durable log INSERT and the quota-state apply.
	mutationQueueSize = 256
)

// startMutationWorker initializes the async mutation queue and a single
// sequencing worker. Called once from SetMetaQuotaStore when server quota is
// active.
func (b *Dat9Backend) startMutationWorker() {
	if b.mutationQueue != nil {
		return // already started
	}
	ctx, cancel := context.WithCancel(context.Background())
	b.mutationStop = cancel
	b.mutationQueue = make(chan mutationEntry, mutationQueueSize)
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
				case entry := <-b.mutationQueue:
					b.processOneMutation(context.Background(), entry)
				default:
					return
				}
			}
		case entry := <-b.mutationQueue:
			b.processOneMutation(context.Background(), entry)
		}
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
