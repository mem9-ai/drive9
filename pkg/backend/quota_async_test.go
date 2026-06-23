package backend

import (
	"database/sql"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// noopMutationEntry returns a mutationEntry whose apply is a counter bump.
func noopMutationEntry(counter *atomic.Int64) mutationEntry {
	return mutationEntry{
		mutationType: "test",
		payload:      fileCreateMutationData{FileID: "f1"},
		apply: func(tx *sql.Tx) error {
			counter.Add(1)
			return nil
		},
	}
}

func TestEnqueueMutationEntry_Async(t *testing.T) {
	b := &Dat9Backend{}
	b.startMutationWorker()
	defer b.stopMutationWorker()

	// Without metaStore, processOneMutation short-circuits (no-op).
	// Verify the entry is dequeued and processed (even if it's a no-op).
	var enqueued atomic.Int64
	for i := 0; i < 10; i++ {
		entry := mutationEntry{
			mutationType: "test",
			payload:      fileCreateMutationData{FileID: "f1"},
			apply: func(tx *sql.Tx) error {
				enqueued.Add(1)
				return nil
			},
		}
		b.mutationQueue <- entry
	}

	require.Eventually(t, func() bool {
		return len(b.mutationQueue) == 0
	}, 2*time.Second, 10*time.Millisecond, "queue should drain")
}

func TestEnqueueMutationEntry_NonBlocking_DropOnFull(t *testing.T) {
	b := &Dat9Backend{}
	// Create a tiny queue to easily fill it.
	b.mutationQueue = make(chan mutationEntry, 2)
	// Don't start worker — entries stay in queue.

	var counter atomic.Int64
	// Fill the queue.
	b.mutationQueue <- noopMutationEntry(&counter)
	b.mutationQueue <- noopMutationEntry(&counter)

	// Third entry should be dropped (non-blocking).
	b.enqueueMutationEntry(t.Context(), noopMutationEntry(&counter))

	// Queue should still have exactly 2 entries.
	require.Equal(t, 2, len(b.mutationQueue))
}

func TestEnqueueMutationEntry_InlineFallback(t *testing.T) {
	// No mutation worker started, no metaStore — should run inline
	// (processOneMutation short-circuits because metaStore is nil).
	b := &Dat9Backend{}
	entry := noopMutationEntry(new(atomic.Int64))
	// Should not panic.
	b.enqueueMutationEntry(t.Context(), entry)
}

func TestStopMutationWorker_DrainsPending(t *testing.T) {
	b := &Dat9Backend{}
	b.startMutationWorker()

	// Push entries directly to channel — they will be processed on drain.
	for i := 0; i < 50; i++ {
		b.mutationQueue <- mutationEntry{
			mutationType: "test",
			payload:      fileCreateMutationData{FileID: "f1"},
			apply:        func(tx *sql.Tx) error { return nil },
		}
	}

	b.stopMutationWorker()
	// After stop, queue should be nil (drained + cleaned up).
	require.Nil(t, b.mutationQueue)
}

func TestStopMutationWorker_Idempotent(t *testing.T) {
	b := &Dat9Backend{}
	b.startMutationWorker()
	b.stopMutationWorker()
	b.stopMutationWorker() // Should not panic.
}
