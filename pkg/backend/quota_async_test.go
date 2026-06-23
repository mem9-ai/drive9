package backend

import (
	"context"
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
	// Verify the entry is dequeued (queue drains).
	for i := 0; i < 10; i++ {
		b.mutationQueue <- mutationEntry{
			mutationType: "test",
			payload:      fileCreateMutationData{FileID: "f1"},
			apply:        func(tx *sql.Tx) error { return nil },
		}
	}

	require.Eventually(t, func() bool {
		return len(b.mutationQueue) == 0
	}, 2*time.Second, 10*time.Millisecond, "queue should drain")
}

func TestEnqueueMutationEntry_NonBlocking_DropOnFull(t *testing.T) {
	fake := newFakeMetaQuotaStore()
	b := &Dat9Backend{
		tenantID:  "tenant-drop",
		metaStore: fake,
	}
	// Create a tiny queue and don't start worker — entries stay in queue.
	b.mutationQueue = make(chan mutationEntry, 2)

	var counter atomic.Int64
	// Fill the queue.
	b.mutationQueue <- noopMutationEntry(&counter)
	b.mutationQueue <- noopMutationEntry(&counter)

	// Third entry should be dropped (non-blocking).
	droppedEntry := mutationEntry{
		mutationType: "file_create",
		payload:      fileCreateMutationData{FileID: "dropped-file", SizeBytes: 999},
		apply: func(tx *sql.Tx) error {
			return b.metaStore.IncrStorageBytesTx(tx, b.tenantID, 999)
		},
	}
	b.enqueueMutationEntry(t.Context(), droppedEntry)

	// Queue should still have exactly 2 entries.
	require.Equal(t, 2, len(b.mutationQueue))

	// Verify the dropped entry was NOT logged or applied.
	require.Len(t, fake.mutations, 0, "dropped entry must not be logged")
	usage := fake.usage["tenant-drop"]
	if usage != nil {
		require.Equal(t, int64(0), usage.StorageBytes, "dropped entry must not be applied")
	}
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

	for i := 0; i < 50; i++ {
		b.mutationQueue <- mutationEntry{
			mutationType: "test",
			payload:      fileCreateMutationData{FileID: "f1"},
			apply:        func(tx *sql.Tx) error { return nil },
		}
	}

	b.stopMutationWorker()
	require.Nil(t, b.mutationQueue)
}

func TestStopMutationWorker_Idempotent(t *testing.T) {
	b := &Dat9Backend{}
	b.startMutationWorker()
	b.stopMutationWorker()
	b.stopMutationWorker() // Should not panic.
}

// TestProcessOneMutation_LogApplyMark verifies that processOneMutation
// calls InsertMutationLog, the apply function, and MarkMutationAppliedTx
// using a fakeMetaQuotaStore (defined in quota_migration_test.go).
func TestProcessOneMutation_LogApplyMark(t *testing.T) {
	fake := newFakeMetaQuotaStore()
	b := &Dat9Backend{
		tenantID:  "tenant-test",
		metaStore: fake,
	}

	var applyCalled atomic.Bool
	entry := mutationEntry{
		mutationType: "file_create",
		payload: fileCreateMutationData{
			FileID:    "file-1",
			SizeBytes: 1024,
			IsMedia:   false,
		},
		apply: func(tx *sql.Tx) error {
			applyCalled.Store(true)
			return b.metaStore.IncrStorageBytesTx(tx, b.tenantID, 1024)
		},
	}

	b.processOneMutation(context.Background(), entry)

	require.True(t, applyCalled.Load(), "apply function should be called")

	// Verify mutation log was inserted and marked applied.
	// Note: mutationStatus locks fake.mu internally, so we must NOT hold
	// fake.mu when calling it.
	require.Len(t, fake.mutations, 1, "one mutation log entry expected")
	require.Equal(t, "file_create", fake.mutations[0].typ)
	require.Equal(t, "applied", fake.mutationStatus(fake.mutations[0].id))

	// Verify storage bytes were incremented.
	fake.mu.Lock()
	usage := fake.usage["tenant-test"]
	fake.mu.Unlock()
	require.NotNil(t, usage)
	require.Equal(t, int64(1024), usage.StorageBytes)
}

// TestProcessOneMutation_WorkerDrain verifies that the background worker
// calls processOneMutation with a real metaStore for each enqueued entry.
func TestProcessOneMutation_WorkerDrain(t *testing.T) {
	fake := newFakeMetaQuotaStore()
	b := &Dat9Backend{
		tenantID:  "tenant-test",
		metaStore: fake,
	}
	b.startMutationWorker()

	const n = 5
	for i := 0; i < n; i++ {
		b.enqueueMutationEntry(context.Background(), mutationEntry{
			mutationType: "file_create",
			payload: fileCreateMutationData{
				FileID:    "file-1",
				SizeBytes: 100,
			},
			apply: func(tx *sql.Tx) error {
				return b.metaStore.IncrStorageBytesTx(tx, b.tenantID, 100)
			},
		})
	}

	b.stopMutationWorker()

	require.Len(t, fake.mutations, n, "all mutations should be logged")
	for _, entry := range fake.mutations {
		require.Equal(t, "applied", fake.mutationStatus(entry.id))
	}
	fake.mu.Lock()
	require.Equal(t, int64(n*100), fake.usage["tenant-test"].StorageBytes)
	fake.mu.Unlock()
}

// TestSyncCentralFileCreate_AsyncPath verifies the full production path:
// syncCentralFileCreate enqueues to the async channel, and the background
// worker processes it through InsertMutationLog → apply → MarkMutationApplied.
func TestSyncCentralFileCreate_AsyncPath(t *testing.T) {
	fake := newFakeMetaQuotaStore()
	b := &Dat9Backend{
		quotaSource: QuotaSourceServer,
	}
	// Use the production wiring path to start the worker.
	b.SetMetaQuotaStore("tenant-test", fake)
	require.NotNil(t, b.mutationQueue, "SetMetaQuotaStore should start mutation worker")

	b.syncCentralFileCreate(context.Background(), "file-abc", 2048, "image/png")

	b.stopMutationWorker()

	require.Len(t, fake.mutations, 1)
	require.Equal(t, "file_create", fake.mutations[0].typ)
	require.Equal(t, "applied", fake.mutationStatus(fake.mutations[0].id))

	fake.mu.Lock()
	usage := fake.usage["tenant-test"]
	fm := fake.fileMeta[metaKey("tenant-test", "file-abc")]
	fake.mu.Unlock()

	require.NotNil(t, usage)
	require.Equal(t, int64(2048), usage.StorageBytes)
	require.Equal(t, int64(1), usage.MediaFileCount)
	require.NotNil(t, fm)
	require.Equal(t, int64(2048), fm.SizeBytes)
	require.True(t, fm.IsMedia)
}

// TestSyncCentralLLMCostRecord_Synchronous verifies that LLM cost records
// are processed synchronously (not enqueued to the async channel).
func TestSyncCentralLLMCostRecord_Synchronous(t *testing.T) {
	fake := newFakeMetaQuotaStore()
	b := &Dat9Backend{
		tenantID:  "tenant-test",
		metaStore: fake,
	}
	// Start worker with tiny queue — if LLM cost went through queue,
	// it would be enqueued there.
	b.mutationQueue = make(chan mutationEntry, 1)

	b.syncCentralLLMCostRecord(context.Background(), "img_extract_text", "task-1", 500, 100, "tokens")

	// Queue should be empty — LLM cost bypasses the queue.
	require.Equal(t, 0, len(b.mutationQueue), "LLM cost should not use async queue")

	// Verify it was logged and applied.
	require.Len(t, fake.mutations, 1)
	require.Equal(t, "llm_cost_record", fake.mutations[0].typ)
	require.Equal(t, "applied", fake.mutationStatus(fake.mutations[0].id))
}
