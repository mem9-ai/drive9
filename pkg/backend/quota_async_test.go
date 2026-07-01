package backend

import (
	"context"
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
		b.enqueueMutation(func(context.Context) {
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
	b.enqueueMutation(func(context.Context) {
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
		b.enqueueMutation(func(context.Context) {
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
		resultMu sync.Mutex
		applied  []int // log_ids in worker apply order
		nextID   int   // simulates auto-increment log_id
	)
	done := make(chan struct{})

	// Simulate concurrent writers: each goroutine acquires mutationMu,
	// assigns a log_id (simulating InsertMutationLog's auto-increment),
	// and enqueues — exactly what logAndEnqueueMutation does.
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.mutationMu.Lock()
			// Under mutationMu: assign log_id + enqueue are atomic.
			// This models InsertMutationLog returning sequential IDs.
			logID := nextID
			nextID++
			b.enqueueMutation(func(context.Context) {
				resultMu.Lock()
				applied = append(applied, logID)
				if len(applied) == n {
					close(done)
				}
				resultMu.Unlock()
			})
			b.mutationMu.Unlock()
		}()
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for mutations")
	}
	wg.Wait()

	// Verify: worker applied mutations in strict log_id order.
	// Because log_id is assigned inside mutationMu and enqueue happens
	// before unlock, channel order = log_id order. Single worker
	// preserves FIFO, so applied[i] must equal i.
	require.Len(t, applied, n)
	for i := 0; i < n; i++ {
		require.Equal(t, i, applied[i],
			"apply order[%d]=%d, want %d (log_id FIFO violated)",
			i, applied[i], i)
	}
}

func TestStopMutationWorker_DrainsPending(t *testing.T) {
	b := &Dat9Backend{}
	b.startMutationWorker()

	var executed atomic.Int64
	for i := 0; i < 50; i++ {
		b.enqueueMutation(func(context.Context) {
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
