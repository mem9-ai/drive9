package datastore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mem9-ai/dat9/pkg/semantic"
)

func newSemanticTask(taskID, resourceID string, version int64, availableAt, createdAt time.Time) *semantic.Task {
	return &semantic.Task{
		TaskID:          taskID,
		TaskType:        semantic.TaskTypeEmbed,
		ResourceID:      resourceID,
		ResourceVersion: version,
		Status:          semantic.TaskQueued,
		MaxAttempts:     3,
		AvailableAt:     availableAt.UTC(),
		CreatedAt:       createdAt.UTC(),
		UpdatedAt:       createdAt.UTC(),
		PayloadJSON:     []byte(`{"resource_id":"` + resourceID + `"}`),
	}
}

func TestSemanticTaskLifecycle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	created, err := s.EnqueueSemanticTask(ctx, newSemanticTask("task-1", "file-1", 1, now, now))
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("expected task to be created")
	}

	claimed, found, err := s.ClaimSemanticTask(ctx, now.Add(time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected claim to find queued task")
	}
	if claimed.TaskID != "task-1" || claimed.AttemptCount != 1 {
		t.Fatalf("unexpected claim result: %+v", claimed)
	}
	if claimed.Receipt == "" || claimed.LeasedAt == nil || claimed.LeaseUntil == nil {
		t.Fatalf("claim did not populate lease fields: %+v", claimed)
	}

	if err := s.AckSemanticTask(ctx, claimed.TaskID, claimed.Receipt); err != nil {
		t.Fatal(err)
	}

	task := mustGetSemanticTask(t, s, claimed.TaskID)
	if task.Status != semantic.TaskSucceeded {
		t.Fatalf("status=%q, want %q", task.Status, semantic.TaskSucceeded)
	}
	if task.CompletedAt == nil {
		t.Fatal("expected completed_at after ack")
	}
	if task.Receipt != "" || task.LeasedAt != nil || task.LeaseUntil != nil {
		t.Fatalf("ack should clear lease fields: %+v", task)
	}
}

func TestSemanticTaskDuplicateEnqueueReturnsFalse(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Unix(1711600100, 0).UTC()

	if created, err := s.EnqueueSemanticTask(ctx, newSemanticTask("task-1", "file-1", 7, now, now)); err != nil {
		t.Fatal(err)
	} else if !created {
		t.Fatal("first enqueue should create task")
	}

	created, err := s.EnqueueSemanticTask(ctx, newSemanticTask("task-2", "file-1", 7, now.Add(time.Second), now.Add(time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatal("duplicate enqueue should not create a second task")
	}

	if count := countSemanticTasks(t, s); count != 1 {
		t.Fatalf("semantic task count=%d, want 1", count)
	}
}

func TestSemanticTaskAckWrongReceiptFails(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	if _, err := s.EnqueueSemanticTask(ctx, newSemanticTask("task-1", "file-1", 1, now, now)); err != nil {
		t.Fatal(err)
	}
	claimed, found, err := s.ClaimSemanticTask(ctx, now.Add(time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected claim to find task")
	}

	err = s.AckSemanticTask(ctx, claimed.TaskID, "wrong-receipt")
	if !errors.Is(err, semantic.ErrTaskLeaseMismatch) {
		t.Fatalf("ack error=%v, want %v", err, semantic.ErrTaskLeaseMismatch)
	}
}

func TestSemanticTaskClaimOrderByAvailableAtThenCreatedAt(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	base := time.Unix(1711600300, 0).UTC()

	tasks := []*semantic.Task{
		newSemanticTask("task-late", "file-late", 1, base.Add(10*time.Second), base.Add(10*time.Second)),
		newSemanticTask("task-b", "file-b", 1, base, base.Add(2*time.Second)),
		newSemanticTask("task-a", "file-a", 1, base, base.Add(time.Second)),
	}
	for _, task := range tasks {
		if _, err := s.EnqueueSemanticTask(ctx, task); err != nil {
			t.Fatal(err)
		}
	}

	for _, want := range []string{"task-a", "task-b", "task-late"} {
		claimed, found, err := s.ClaimSemanticTask(ctx, base.Add(20*time.Second), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Fatalf("expected claim to find %s", want)
		}
		if claimed.TaskID != want {
			t.Fatalf("claimed task=%q, want %q", claimed.TaskID, want)
		}
	}
}

func TestSemanticTaskRetryAndRecoverExpired(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC()

	if _, err := s.EnqueueSemanticTask(ctx, newSemanticTask("task-1", "file-1", 1, base, base)); err != nil {
		t.Fatal(err)
	}
	claimed, found, err := s.ClaimSemanticTask(ctx, base.Add(time.Second), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected claim to find task")
	}

	retryAt := base.Add(5 * time.Second)
	if err := s.RetrySemanticTask(ctx, claimed.TaskID, claimed.Receipt, retryAt, "temporary failure"); err != nil {
		t.Fatal(err)
	}

	if _, found, err := s.ClaimSemanticTask(ctx, base.Add(4*time.Second), time.Second); err != nil {
		t.Fatal(err)
	} else if found {
		t.Fatal("task should not be claimable before retry_at")
	}

	reclaimed, found, err := s.ClaimSemanticTask(ctx, retryAt.Add(time.Second), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected task to be claimable at retry_at")
	}
	if reclaimed.AttemptCount != 2 {
		t.Fatalf("attempt_count=%d, want 2", reclaimed.AttemptCount)
	}

	recoveredAt := retryAt.Add(3 * time.Second)
	recovered, err := s.RecoverExpiredSemanticTasks(ctx, recoveredAt, 0)
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 1 {
		t.Fatalf("recovered=%d, want 1", recovered)
	}

	recoveredTask, found, err := s.ClaimSemanticTask(ctx, recoveredAt.Add(time.Second), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected recovered task to become claimable again")
	}
	if recoveredTask.AttemptCount != 3 {
		t.Fatalf("attempt_count=%d, want 3", recoveredTask.AttemptCount)
	}
	if err := s.RetrySemanticTask(ctx, recoveredTask.TaskID, recoveredTask.Receipt, retryAt.Add(3*time.Second), "permanent failure"); err != nil {
		t.Fatal(err)
	}

	task := mustGetSemanticTask(t, s, recoveredTask.TaskID)
	if task.Status != semantic.TaskDeadLettered {
		t.Fatalf("status=%q, want %q", task.Status, semantic.TaskDeadLettered)
	}
	if task.CompletedAt == nil {
		t.Fatal("expected completed_at for dead-lettered task")
	}
}

func TestSemanticTaskRecoverAfterReopen(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	base := time.Unix(1711600500, 0).UTC()

	if _, err := s.EnqueueSemanticTask(ctx, newSemanticTask("task-1", "file-1", 1, base, base)); err != nil {
		t.Fatal(err)
	}
	if _, found, err := s.ClaimSemanticTask(ctx, base, time.Second); err != nil {
		t.Fatal(err)
	} else if !found {
		t.Fatal("expected initial claim to succeed")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	recovered, err := reopened.RecoverExpiredSemanticTasks(ctx, base.Add(2*time.Second), 0)
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 1 {
		t.Fatalf("recovered=%d, want 1", recovered)
	}

	claimed, found, err := reopened.ClaimSemanticTask(ctx, base.Add(2*time.Second), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected recovered task after reopen")
	}
	if claimed.TaskID != "task-1" || claimed.AttemptCount != 2 {
		t.Fatalf("unexpected reclaimed task: %+v", claimed)
	}
}

func TestEnsureSemanticTaskQueuedRequeuesSucceededTask(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	if _, err := s.EnqueueSemanticTask(ctx, newSemanticTask("task-1", "file-1", 1, now, now)); err != nil {
		t.Fatal(err)
	}
	claimed, found, err := s.ClaimSemanticTask(ctx, now.Add(time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected claim to find task")
	}
	if err := s.AckSemanticTask(ctx, claimed.TaskID, claimed.Receipt); err != nil {
		t.Fatal(err)
	}

	queued, err := s.EnsureSemanticTaskQueued(ctx, &semantic.Task{
		TaskID:          "task-requeue",
		TaskType:        semantic.TaskTypeEmbed,
		ResourceID:      "file-1",
		ResourceVersion: 1,
		Status:          semantic.TaskQueued,
		MaxAttempts:     3,
		AvailableAt:     now.Add(2 * time.Second),
		CreatedAt:       now.Add(2 * time.Second),
		UpdatedAt:       now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !queued {
		t.Fatal("expected ensure to requeue succeeded task")
	}

	task := mustGetSemanticTask(t, s, claimed.TaskID)
	if task.Status != semantic.TaskQueued {
		t.Fatalf("status=%q, want %q", task.Status, semantic.TaskQueued)
	}
	if task.CompletedAt != nil {
		t.Fatal("completed_at should be cleared when re-queued")
	}
	if task.Receipt != "" || task.LeasedAt != nil || task.LeaseUntil != nil {
		t.Fatalf("lease fields should be cleared when re-queued: %+v", task)
	}
}

func mustGetSemanticTask(t *testing.T, s *Store, taskID string) *semantic.Task {
	t.Helper()
	row := s.DB().QueryRow(`SELECT task_id, task_type, resource_id, resource_version, status,
		attempt_count, max_attempts, receipt, leased_at, lease_until, available_at,
		payload_json, last_error, created_at, updated_at, completed_at
		FROM semantic_tasks WHERE task_id = ?`, taskID)
	task, err := scanSemanticTask(row)
	if err != nil {
		t.Fatalf("get semantic task %s: %v", taskID, err)
	}
	return task
}

func countSemanticTasks(t *testing.T, s *Store) int {
	t.Helper()
	var count int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM semantic_tasks`).Scan(&count); err != nil {
		t.Fatalf("count semantic tasks: %v", err)
	}
	return count
}
