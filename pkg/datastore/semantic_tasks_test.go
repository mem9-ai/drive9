package datastore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mem9-ai/dat9/pkg/semantic"
)

func newSemanticTask(taskID, resourceID string, version int64, availableAt, createdAt time.Time) *semantic.Task {
	return newSemanticTaskOfType(semantic.TaskTypeEmbed, taskID, resourceID, version, availableAt, createdAt)
}

func newSemanticTaskOfType(taskType semantic.TaskType, taskID, resourceID string, version int64, availableAt, createdAt time.Time) *semantic.Task {
	return &semantic.Task{
		TaskID:          taskID,
		TaskType:        taskType,
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

func TestClaimSemanticTaskSkipsOlderDisallowedTask(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	base := time.Unix(1711600350, 0).UTC()

	tasks := []*semantic.Task{
		newSemanticTaskOfType(semantic.TaskTypeEmbed, "task-embed", "file-embed", 1, base, base),
		newSemanticTaskOfType(semantic.TaskTypeImgExtractText, "task-img", "file-img", 1, base.Add(time.Second), base.Add(time.Second)),
	}
	for _, task := range tasks {
		if _, err := s.EnqueueSemanticTask(ctx, task); err != nil {
			t.Fatal(err)
		}
	}

	claimed, found, err := s.ClaimSemanticTask(ctx, base.Add(5*time.Second), time.Minute, semantic.TaskTypeImgExtractText)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected filtered claim to find image task")
	}
	if claimed.TaskID != "task-img" {
		t.Fatalf("claimed task=%q, want %q", claimed.TaskID, "task-img")
	}

	embed := mustGetSemanticTask(t, s, "task-embed")
	if embed.Status != semantic.TaskQueued {
		t.Fatalf("embed task status=%q, want %q", embed.Status, semantic.TaskQueued)
	}
}

func TestClaimSemanticTaskReturnsNotFoundWhenOnlyDisallowedTasksExist(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	base := time.Unix(1711600360, 0).UTC()

	if _, err := s.EnqueueSemanticTask(ctx, newSemanticTaskOfType(semantic.TaskTypeEmbed, "task-embed", "file-embed", 1, base, base)); err != nil {
		t.Fatal(err)
	}

	claimed, found, err := s.ClaimSemanticTask(ctx, base.Add(time.Second), time.Minute, semantic.TaskTypeImgExtractText)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatalf("unexpected claimed task: %+v", claimed)
	}

	task := mustGetSemanticTask(t, s, "task-embed")
	if task.Status != semantic.TaskQueued {
		t.Fatalf("task status=%q, want %q", task.Status, semantic.TaskQueued)
	}
}

func TestClaimSemanticTaskSupportsMultipleTaskTypes(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	base := time.Unix(1711600370, 0).UTC()

	tasks := []*semantic.Task{
		newSemanticTaskOfType(semantic.TaskTypeEmbed, "task-embed", "file-embed", 1, base, base),
		newSemanticTaskOfType(semantic.TaskTypeImgExtractText, "task-img", "file-img", 1, base.Add(time.Second), base.Add(time.Second)),
	}
	for _, task := range tasks {
		if _, err := s.EnqueueSemanticTask(ctx, task); err != nil {
			t.Fatal(err)
		}
	}

	claimed, found, err := s.ClaimSemanticTask(ctx, base.Add(5*time.Second), time.Minute, semantic.TaskTypeImgExtractText, semantic.TaskTypeEmbed)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected filtered claim to find a task")
	}
	if claimed.TaskID != "task-embed" {
		t.Fatalf("claimed task=%q, want %q", claimed.TaskID, "task-embed")
	}
}

func TestClaimSemanticTaskWithoutFiltersClaimsAnyTask(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	base := time.Unix(1711600380, 0).UTC()

	if _, err := s.EnqueueSemanticTask(ctx, newSemanticTask("task-1", "file-1", 1, base, base)); err != nil {
		t.Fatal(err)
	}

	claimed, found, err := s.ClaimSemanticTask(ctx, base.Add(time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected unfiltered claim to find task")
	}
	if claimed.TaskID != "task-1" {
		t.Fatalf("claimed task=%q, want %q", claimed.TaskID, "task-1")
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

func TestObserveSemanticTasksSummarizesQueueState(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC()

	if _, err := s.EnqueueSemanticTask(ctx, newSemanticTask("task-queued", "file-queued", 1, base.Add(-3*time.Second), base)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnqueueSemanticTask(ctx, newSemanticTask("task-future", "file-future", 1, base.Add(30*time.Second), base.Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnqueueSemanticTask(ctx, newSemanticTask("task-processing", "file-processing", 1, base.Add(-5*time.Second), base.Add(2*time.Second))); err != nil {
		t.Fatal(err)
	}
	processingTask, found, err := s.ClaimSemanticTask(ctx, base.Add(time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected processing task to be claimed")
	}

	if _, err := s.EnqueueSemanticTask(ctx, newSemanticTask("task-dead", "file-dead", 1, base.Add(-4*time.Second), base.Add(3*time.Second))); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`UPDATE semantic_tasks SET max_attempts = 1 WHERE task_id = ?`, "task-dead"); err != nil {
		t.Fatal(err)
	}
	deadTask, found, err := s.ClaimSemanticTask(ctx, base.Add(2*time.Second), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected dead-letter task to be claimed")
	}
	if err := s.RetrySemanticTask(ctx, deadTask.TaskID, deadTask.Receipt, base.Add(3*time.Second), "permanent failure"); err != nil {
		t.Fatal(err)
	}

	obs, err := s.ObserveSemanticTasks(ctx, base.Add(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if obs.Queued != 2 {
		t.Fatalf("queued=%d, want 2", obs.Queued)
	}
	if obs.Processing != 1 {
		t.Fatalf("processing=%d, want 1", obs.Processing)
	}
	if obs.DeadLettered != 1 {
		t.Fatalf("dead_lettered=%d, want 1", obs.DeadLettered)
	}
	if obs.OldestClaimableAvailableAt == nil {
		t.Fatal("expected oldest claimable available_at")
	}
	wantOldest := base.Add(-3 * time.Second).Round(time.Millisecond)
	if !obs.OldestClaimableAvailableAt.Equal(wantOldest) {
		t.Fatalf("oldest claimable=%s, want %s", obs.OldestClaimableAvailableAt.UTC(), wantOldest.UTC())
	}

	task := mustGetSemanticTask(t, s, processingTask.TaskID)
	if task.Status != semantic.TaskProcessing {
		t.Fatalf("processing task status=%q, want %q", task.Status, semantic.TaskProcessing)
	}
}

func TestObserveSemanticTasksSkipsFutureOnlyQueueLag(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	base := time.Unix(1711600700, 0).UTC()

	if _, err := s.EnqueueSemanticTask(ctx, newSemanticTask("task-future", "file-future", 1, base.Add(10*time.Second), base)); err != nil {
		t.Fatal(err)
	}

	obs, err := s.ObserveSemanticTasks(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	if obs.Queued != 1 {
		t.Fatalf("queued=%d, want 1", obs.Queued)
	}
	if obs.Processing != 0 {
		t.Fatalf("processing=%d, want 0", obs.Processing)
	}
	if obs.DeadLettered != 0 {
		t.Fatalf("dead_lettered=%d, want 0", obs.DeadLettered)
	}
	if obs.OldestClaimableAvailableAt != nil {
		t.Fatalf("oldest claimable=%s, want nil", obs.OldestClaimableAvailableAt.UTC())
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
