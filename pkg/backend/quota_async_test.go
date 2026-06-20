package backend

import (
	"sync"
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

// TestMutationMu_ConcurrentOrdering verifies that mutationMu serializes
// concurrent log+enqueue operations so the worker applies mutations in
// log_id order even when goroutines interleave. Without the mutex,
// goroutine A could log log_id=1, stall, goroutine B logs log_id=2 and
// enqueues first, causing the worker to apply log_id=2 before log_id=1.
func TestMutationMu_ConcurrentOrdering(t *testing.T) {
	b := &Dat9Backend{}
	b.startMutationWorker()
	defer b.stopMutationWorker()

	const n = 100
	var (
		mu    sync.Mutex
		order []int
	)
	done := make(chan struct{})

	// Simulate concurrent writers: each goroutine acquires mutationMu,
	// records its sequence number, and enqueues — exactly what
	// logAndEnqueueMutation does.
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(seq int) {
			defer wg.Done()
			b.mutationMu.Lock()
			// Under mutationMu: "log" (record seq) + "enqueue" (channel send)
			// are atomic, so enqueue order = seq order.
			mySeq := seq
			b.enqueueMutation(func() {
				mu.Lock()
				order = append(order, mySeq)
				if len(order) == n {
					close(done)
				}
				mu.Unlock()
			})
			b.mutationMu.Unlock()
		}(i)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for mutations")
	}
	wg.Wait()

	// Verify the worker processed mutations in the order they were
	// enqueued (which, under mutationMu, equals log_id order).
	// Because goroutines acquire the mutex in arbitrary order, we don't
	// know which goroutine gets seq=0,1,2... But we DO know that the
	// enqueue order under the mutex is strictly sequential, and the
	// single worker preserves that FIFO. So order[i] must be
	// monotonically increasing.
	for i := 1; i < len(order); i++ {
		require.Greater(t, order[i], order[i-1],
			"apply order[%d]=%d should be > order[%d]=%d (log_id FIFO violated)",
			i, order[i], i-1, order[i-1])
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
