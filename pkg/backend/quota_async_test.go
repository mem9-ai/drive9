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

func TestEnqueueMutation_FIFO_SingleWorker(t *testing.T) {
	b := &Dat9Backend{}
	b.startMutationWorker()
	defer b.stopMutationWorker()

	// Verify FIFO: record execution order.
	var order []int
	done := make(chan struct{})
	const n = 20
	for i := 0; i < n; i++ {
		i := i
		b.enqueueMutation(func() {
			order = append(order, i)
			if len(order) == n {
				close(done)
			}
		})
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for mutations")
	}

	for i := 0; i < n; i++ {
		require.Equal(t, i, order[i], "mutation %d executed out of order", i)
	}
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
