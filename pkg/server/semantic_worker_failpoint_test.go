//go:build failpoint

package server

import (
	"errors"
	"testing"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/pingcap/failpoint"

	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/semantic"
)

const semanticWorkerBeforeFinalizeFailpoint = "github.com/mem9-ai/dat9/pkg/server/semanticWorkerBeforeFinalize"

func TestSemanticWorkerFinalizeAckSkipsWhenLeaseOwnershipLostAtBoundary(t *testing.T) {
	_, b := newTestServerWithSemanticWorker(t, staticSemanticEmbedder{vec: []float32{0.1, 0.2, 0.3}}, SemanticWorkerOptions{
		Workers:         1,
		PollInterval:    10 * time.Millisecond,
		RecoverInterval: 50 * time.Millisecond,
		LeaseDuration:   5 * time.Second,
	})
	if _, err := b.Write("/docs/finalize-ack.txt", []byte("hello finalize ack"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	nf := mustServerFile(t, b, "/docs/finalize-ack.txt")

	if err := failpoint.EnableCall(semanticWorkerBeforeFinalizeFailpoint,
		func(_ string, store *datastore.Store, task *semantic.Task, action, reason, result string) {
			if task == nil || task.ResourceID != nf.FileID || action != string(semanticTaskActionAck) || reason != "written" || result != "ok" {
				return
			}
			if _, err := store.DB().Exec(`UPDATE semantic_tasks SET receipt = ? WHERE task_id = ?`, "lost-receipt", task.TaskID); err != nil {
				panic(err)
			}
		},
	); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = failpoint.Disable(semanticWorkerBeforeFinalizeFailpoint)
	})

	waitForEmbeddingRevision(t, b, "/docs/finalize-ack.txt", 1, 3*time.Second)
	waitForTaskStatusByResource(t, b, nf.FileID, string(semantic.TaskProcessing), 3*time.Second)

	task := mustGetServerSemanticTaskByResource(t, b, nf.FileID)
	if task.Status != string(semantic.TaskProcessing) {
		t.Fatalf("task status=%q, want %q", task.Status, semantic.TaskProcessing)
	}
	if task.AttemptCount != 1 {
		t.Fatalf("attempt_count=%d, want 1", task.AttemptCount)
	}
}

func TestSemanticWorkerFinalizeRetrySkipsWhenLeaseOwnershipLostAtBoundary(t *testing.T) {
	_, b := newTestServerWithSemanticWorker(t, staticSemanticEmbedder{err: errors.New("embed failed")}, SemanticWorkerOptions{
		Workers:         1,
		PollInterval:    10 * time.Millisecond,
		RecoverInterval: 50 * time.Millisecond,
		LeaseDuration:   5 * time.Second,
		RetryBaseDelay:  20 * time.Millisecond,
	})
	if _, err := b.Write("/docs/finalize-retry.txt", []byte("hello finalize retry"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	nf := mustServerFile(t, b, "/docs/finalize-retry.txt")

	if err := failpoint.EnableCall(semanticWorkerBeforeFinalizeFailpoint,
		func(_ string, store *datastore.Store, task *semantic.Task, action, reason, result string) {
			if task == nil || task.ResourceID != nf.FileID || action != string(semanticTaskActionRetry) || result != "embed_error" {
				return
			}
			if _, err := store.DB().Exec(`UPDATE semantic_tasks SET receipt = ? WHERE task_id = ?`, "lost-receipt", task.TaskID); err != nil {
				panic(err)
			}
		},
	); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = failpoint.Disable(semanticWorkerBeforeFinalizeFailpoint)
	})

	waitForTaskStatusByResource(t, b, nf.FileID, string(semantic.TaskProcessing), 3*time.Second)

	task := mustGetServerSemanticTaskByResource(t, b, nf.FileID)
	if task.Status != string(semantic.TaskProcessing) {
		t.Fatalf("task status=%q, want %q", task.Status, semantic.TaskProcessing)
	}
	if task.AttemptCount != 1 {
		t.Fatalf("attempt_count=%d, want 1", task.AttemptCount)
	}
	if task.LastError != "" {
		t.Fatalf("last_error=%q, want empty because retry finalize was skipped", task.LastError)
	}
}

func mustGetServerSemanticTaskByResource(t *testing.T, b *backend.Dat9Backend, resourceID string) serverSemanticTaskState {
	t.Helper()
	var task serverSemanticTaskState
	err := b.Store().DB().QueryRow(`SELECT task_id, task_type, resource_id, status, attempt_count, COALESCE(last_error, '')
		FROM semantic_tasks WHERE resource_id = ? AND resource_version = 1`, resourceID).Scan(
		&task.TaskID,
		&task.TaskType,
		&task.ResourceID,
		&task.Status,
		&task.AttemptCount,
		&task.LastError,
	)
	if err != nil {
		t.Fatalf("get semantic task by resource %s: %v", resourceID, err)
	}
	return task
}

func waitForTaskStatusByResource(t *testing.T, b *backend.Dat9Backend, resourceID, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var status string
		err := b.Store().DB().QueryRow(`SELECT status FROM semantic_tasks WHERE resource_id = ? AND resource_version = 1`, resourceID).Scan(&status)
		if err == nil && status == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	var status string
	if err := b.Store().DB().QueryRow(`SELECT status FROM semantic_tasks WHERE resource_id = ? AND resource_version = 1`, resourceID).Scan(&status); err != nil {
		t.Fatalf("wait task status query by resource: %v", err)
	}
	t.Fatalf("resource %s status=%q, want %q", resourceID, status, want)
}
