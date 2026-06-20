package backend

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEnqueueMutation_Async(t *testing.T) {
	b := &Dat9Backend{}
	b.startMutationWorker()
	defer b.stopMutationWorker()

	var executed atomic.Int64
	for i := 0; i < 10; i++ {
		b.enqueueMutation(func() {
			executed.Add(1)
		})
	}

	// Wait for all mutations to drain.
	require.Eventually(t, func() bool {
		return executed.Load() == 10
	}, 2*time.Second, 10*time.Millisecond)
}

func TestEnqueueMutation_InlineFallback(t *testing.T) {
	// No mutation worker started — should run inline.
	b := &Dat9Backend{}
	var executed atomic.Int64
	b.enqueueMutation(func() {
		executed.Add(1)
	})
	require.Equal(t, int64(1), executed.Load())
}

func TestEnqueueMutation_QueueFull_InlineFallback(t *testing.T) {
	b := &Dat9Backend{}
	// Create a tiny queue and don't start workers so it fills up.
	b.mutationQueue = make(chan func(), 1)

	var executed atomic.Int64

	// First enqueue goes into the channel.
	b.enqueueMutation(func() { executed.Add(1) })
	// Second enqueue finds channel full → runs inline.
	b.enqueueMutation(func() { executed.Add(1) })

	// The inline one should have executed immediately.
	require.Equal(t, int64(1), executed.Load())
}

func TestStopMutationWorker_DrainsPending(t *testing.T) {
	b := &Dat9Backend{}
	b.startMutationWorker()

	var executed atomic.Int64
	for i := 0; i < 50; i++ {
		b.enqueueMutation(func() {
			executed.Add(1)
		})
	}

	b.stopMutationWorker()
	require.Equal(t, int64(50), executed.Load())
}

func TestStopMutationWorker_Idempotent(t *testing.T) {
	b := &Dat9Backend{}
	b.startMutationWorker()
	b.stopMutationWorker()
	b.stopMutationWorker() // Should not panic.
}
