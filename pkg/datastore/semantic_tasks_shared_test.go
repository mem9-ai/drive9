package datastore

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/semantic"
)

// runSemanticTaskCoreScenario exercises the semantic task queue flow
// (enqueue, duplicate suppression, claim, ack, requeue, retry, recover,
// dead-letter, observe) against a store. It is run against both schema
// shapes to prove behavioral parity.
func runSemanticTaskCoreScenario(t *testing.T, store *Store, idPrefix string) {
	t.Helper()
	ctx := context.Background()
	base := time.Now().UTC()

	// Enqueue creates; a second enqueue for the same resource tuple is a
	// no-op even with a different task id.
	created, err := store.EnqueueSemanticTask(ctx, newSemanticTask(idPrefix+"-task-1", idPrefix+"-file-1", 1, base, base))
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("expected task to be created")
	}
	created, err = store.EnqueueSemanticTask(ctx, newSemanticTask(idPrefix+"-task-dup", idPrefix+"-file-1", 1, base.Add(time.Second), base.Add(time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatal("duplicate enqueue should not create a second task")
	}

	// Claim → ack. A wrong receipt is a lease mismatch.
	claimed, found, err := store.ClaimSemanticTask(ctx, base.Add(time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected claim to find queued task")
	}
	if claimed.TaskID != idPrefix+"-task-1" || claimed.AttemptCount != 1 {
		t.Fatalf("unexpected claim result: %+v", claimed)
	}
	if claimed.Receipt == "" || claimed.LeasedAt == nil || claimed.LeaseUntil == nil {
		t.Fatalf("claim did not populate lease fields: %+v", claimed)
	}
	if err := store.AckSemanticTask(ctx, claimed.TaskID, "wrong-receipt"); !errors.Is(err, semantic.ErrTaskLeaseMismatch) {
		t.Fatalf("ack error=%v, want %v", err, semantic.ErrTaskLeaseMismatch)
	}
	if err := store.AckSemanticTask(ctx, claimed.TaskID, claimed.Receipt); err != nil {
		t.Fatal(err)
	}
	task := mustGetSharedSemanticTask(t, store, claimed.TaskID)
	if task.Status != semantic.TaskSucceeded || task.CompletedAt == nil {
		t.Fatalf("ack should succeed the task with completed_at: %+v", task)
	}

	// EnsureSemanticTaskQueued re-queues the terminal task in place.
	queued, err := store.EnsureSemanticTaskQueued(ctx, &semantic.Task{
		TaskID:          idPrefix + "-task-requeue",
		TaskType:        semantic.TaskTypeEmbed,
		ResourceID:      idPrefix + "-file-1",
		ResourceVersion: 1,
		Status:          semantic.TaskQueued,
		MaxAttempts:     3,
		AvailableAt:     base.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !queued {
		t.Fatal("expected ensure to requeue succeeded task")
	}
	task = mustGetSharedSemanticTask(t, store, claimed.TaskID)
	if task.Status != semantic.TaskQueued || task.CompletedAt != nil || task.Receipt != "" {
		t.Fatalf("requeue should clear terminal/lease fields: %+v", task)
	}

	// Retry → recover → dead-letter on a fresh task (max attempts 3).
	if _, err := store.EnqueueSemanticTask(ctx, newSemanticTask(idPrefix+"-task-2", idPrefix+"-file-2", 1, base, base)); err != nil {
		t.Fatal(err)
	}
	claimed2, found, err := store.ClaimSemanticTask(ctx, base.Add(time.Second), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !found || claimed2.TaskID != idPrefix+"-task-2" || claimed2.AttemptCount != 1 {
		t.Fatalf("unexpected claim result: found=%v task=%+v", found, claimed2)
	}
	retryAt := base.Add(5 * time.Second)
	if err := store.RetrySemanticTask(ctx, claimed2.TaskID, claimed2.Receipt, retryAt, "temporary failure"); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.ClaimSemanticTask(ctx, base.Add(4*time.Second), time.Second); err != nil {
		t.Fatal(err)
	} else if found {
		t.Fatal("task should not be claimable before retry_at")
	}
	reclaimed, found, err := store.ClaimSemanticTask(ctx, retryAt.Add(time.Second), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !found || reclaimed.AttemptCount != 2 {
		t.Fatalf("expected reclaim at retry_at with attempt 2: found=%v task=%+v", found, reclaimed)
	}
	recoveredAt := retryAt.Add(3 * time.Second)
	recovered, err := store.RecoverExpiredSemanticTasks(ctx, recoveredAt, 0)
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 1 {
		t.Fatalf("recovered=%d, want 1", recovered)
	}
	recoveredTask, found, err := store.ClaimSemanticTask(ctx, recoveredAt.Add(time.Second), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !found || recoveredTask.AttemptCount != 3 {
		t.Fatalf("expected recovered task with attempt 3: found=%v task=%+v", found, recoveredTask)
	}
	if err := store.RetrySemanticTask(ctx, recoveredTask.TaskID, recoveredTask.Receipt, recoveredAt.Add(3*time.Second), "permanent failure"); err != nil {
		t.Fatal(err)
	}
	task = mustGetSharedSemanticTask(t, store, recoveredTask.TaskID)
	if task.Status != semantic.TaskDeadLettered || task.CompletedAt == nil {
		t.Fatalf("max-attempt retry should dead-letter: %+v", task)
	}

	// Type-filtered claim skips older tasks of other types.
	if _, err := store.EnqueueSemanticTask(ctx, newSemanticTaskOfType(semantic.TaskTypeImgExtractText, idPrefix+"-task-img", idPrefix+"-file-img", 1, base, base)); err != nil {
		t.Fatal(err)
	}
	filtered, found, err := store.ClaimSemanticTask(ctx, base.Add(time.Minute), time.Minute, semantic.TaskTypeImgExtractText)
	if err != nil {
		t.Fatal(err)
	}
	if !found || filtered.TaskID != idPrefix+"-task-img" {
		t.Fatalf("filtered claim = found=%v task=%+v, want %s", found, filtered, idPrefix+"-task-img")
	}
	if err := store.AckSemanticTask(ctx, filtered.TaskID, filtered.Receipt); err != nil {
		t.Fatal(err)
	}

	// Queue observation is scoped to this store: one future queued task and
	// one dead-lettered task, no currently claimable work.
	obs, err := store.ObserveSemanticTasks(ctx, base.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if obs.Queued != 1 || obs.Processing != 0 || obs.DeadLettered != 1 {
		t.Fatalf("observation = %+v, want queued=1 processing=0 dead_lettered=1", obs)
	}
	if obs.OldestClaimableAvailableAt != nil {
		t.Fatalf("oldest claimable = %s, want nil (only a future task is queued)", obs.OldestClaimableAvailableAt.UTC())
	}
}

// runFileGCTaskCoreScenario exercises the file GC task flow (enqueue,
// duplicate suppression, get, claim, ack, retry, recover, S3 ref listing)
// against a store. It is run against both schema shapes to prove parity.
func runFileGCTaskCoreScenario(t *testing.T, store *Store, idPrefix string) {
	t.Helper()
	ctx := context.Background()
	base := time.Now().UTC()

	task1 := &FileGCTask{
		TaskID:      idPrefix + "-gc-1",
		FileID:      idPrefix + "-file-1",
		StorageType: StorageS3,
		StorageRef:  "s3://bucket/" + idPrefix + "-blob-1",
		SizeBytes:   42,
		ContentType: "text/plain",
		AvailableAt: base,
	}
	inserted, err := store.EnqueueFileGCTaskTx(store.DB(), task1)
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("expected file gc task to be created")
	}
	inserted, err = store.EnqueueFileGCTaskTx(store.DB(), task1)
	if err != nil {
		t.Fatal(err)
	}
	if inserted {
		t.Fatal("duplicate file gc enqueue should be a no-op")
	}
	got, err := store.GetFileGCTaskByFileID(ctx, task1.FileID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != FileGCTaskQueued || got.StorageRef != task1.StorageRef || got.SizeBytes != 42 {
		t.Fatalf("unexpected file gc task: %+v", got)
	}
	count, err := store.CountQueuedFileGCTasks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("queued file gc count=%d, want 1", count)
	}

	claimed, found, err := store.ClaimFileGCTask(ctx, base.Add(time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !found || claimed.TaskID != task1.TaskID || claimed.AttemptCount != 1 || claimed.Receipt == "" {
		t.Fatalf("unexpected claim result: found=%v task=%+v", found, claimed)
	}
	if err := store.AckFileGCTask(ctx, claimed.TaskID, "wrong-receipt"); !errors.Is(err, ErrFileGCTaskLeaseMismatch) {
		t.Fatalf("ack error=%v, want %v", err, ErrFileGCTaskLeaseMismatch)
	}
	if err := store.AckFileGCTask(ctx, claimed.TaskID, claimed.Receipt); err != nil {
		t.Fatal(err)
	}
	got, err = store.GetFileGCTaskByFileID(ctx, task1.FileID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != FileGCTaskSucceeded || got.CompletedAt == nil {
		t.Fatalf("ack should succeed the file gc task: %+v", got)
	}

	// Retry → recover on a second task; file GC retries indefinitely
	// (max_attempts 0), so the final retry requeues instead of dead-lettering.
	task2 := &FileGCTask{
		TaskID:      idPrefix + "-gc-2",
		FileID:      idPrefix + "-file-2",
		StorageType: StorageS3,
		StorageRef:  "s3://bucket/" + idPrefix + "-blob-2",
		SizeBytes:   7,
		AvailableAt: base,
	}
	if _, err := store.EnqueueFileGCTaskTx(store.DB(), task2); err != nil {
		t.Fatal(err)
	}
	claimed2, found, err := store.ClaimFileGCTask(ctx, base.Add(time.Second), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !found || claimed2.TaskID != task2.TaskID {
		t.Fatalf("unexpected claim result: found=%v task=%+v", found, claimed2)
	}
	retryAt := base.Add(5 * time.Second)
	if err := store.RetryFileGCTask(ctx, claimed2.TaskID, claimed2.Receipt, retryAt, "temporary failure"); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.ClaimFileGCTask(ctx, base.Add(4*time.Second), time.Second); err != nil {
		t.Fatal(err)
	} else if found {
		t.Fatal("file gc task should not be claimable before retry_at")
	}
	reclaimed, found, err := store.ClaimFileGCTask(ctx, retryAt.Add(time.Second), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !found || reclaimed.AttemptCount != 2 {
		t.Fatalf("expected reclaim with attempt 2: found=%v task=%+v", found, reclaimed)
	}
	recoveredAt := retryAt.Add(3 * time.Second)
	recovered, err := store.RecoverExpiredFileGCTasks(ctx, recoveredAt, 0)
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 1 {
		t.Fatalf("recovered=%d, want 1", recovered)
	}
	recoveredTask, found, err := store.ClaimFileGCTask(ctx, recoveredAt.Add(time.Second), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !found || recoveredTask.AttemptCount != 3 {
		t.Fatalf("expected recovered task with attempt 3: found=%v task=%+v", found, recoveredTask)
	}
	if err := store.RetryFileGCTask(ctx, recoveredTask.TaskID, recoveredTask.Receipt, recoveredAt.Add(3*time.Second), "still failing"); err != nil {
		t.Fatal(err)
	}
	got, err = store.GetFileGCTaskByFileID(ctx, task2.FileID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != FileGCTaskQueued {
		t.Fatalf("file gc retry with max_attempts=0 should requeue: %+v", got)
	}

	// S3 ref listing paginates by storage_ref cursor.
	refs, cursor, err := store.ListFileGCTaskS3Refs(ctx, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 || refs[0].StorageRef != task1.StorageRef || cursor != task1.StorageRef {
		t.Fatalf("first ref page = %+v cursor=%q, want [%s]", refs, cursor, task1.StorageRef)
	}
	refs, cursor, err = store.ListFileGCTaskS3Refs(ctx, cursor, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 || refs[0].StorageRef != task2.StorageRef || cursor != "" {
		t.Fatalf("second ref page = %+v cursor=%q, want [%s]", refs, cursor, task2.StorageRef)
	}
}

// mustGetSharedSemanticTask reads one task row through the store's scope so
// the same helper works against both schema shapes.
func mustGetSharedSemanticTask(t *testing.T, store *Store, taskID string) *semantic.Task {
	t.Helper()
	row := store.DB().QueryRow(`SELECT task_id, task_type, resource_id, resource_version, status,
		attempt_count, max_attempts, receipt, leased_at, lease_until, available_at,
		payload_json, last_error, created_at, updated_at, completed_at
		FROM semantic_tasks WHERE `+store.scope.And(`task_id = ?`), store.scope.Args(taskID)...)
	task, err := scanSemanticTask(row)
	if err != nil {
		t.Fatalf("get semantic task %s: %v", taskID, err)
	}
	return task
}

// TestSemanticTasksSharedShapeParity runs the same task-queue scenario used
// by the standalone tests against the shared (fs_id) schema shape.
func TestSemanticTasksSharedShapeParity(t *testing.T) {
	installSharedCoreFSSchema(t)
	store := newSharedStore(t, 4300001)
	runSemanticTaskCoreScenario(t, store, "shr-parity")
	runFileGCTaskCoreScenario(t, store, "shr-parity")
}

// TestSemanticTasksSharedShapeCrossTenantIsolation proves semantic task rows
// of one fs_id are invisible to another fs_id on the same shared table, and
// that the same task_id/resource tuple can coexist under both fs_ids.
func TestSemanticTasksSharedShapeCrossTenantIsolation(t *testing.T) {
	installSharedCoreFSSchema(t)
	ctx := context.Background()
	storeA := newSharedStore(t, 4300002)
	storeB := newSharedStore(t, 4300003)
	base := time.Now().UTC()

	// Same task_id and resource tuple under both fs_ids must coexist.
	for _, store := range []*Store{storeA, storeB} {
		created, err := store.EnqueueSemanticTask(ctx, newSemanticTask("task-iso", "file-iso", 1, base, base))
		if err != nil {
			t.Fatal(err)
		}
		if !created {
			t.Fatal("expected per-tenant task rows to coexist")
		}
	}
	// Duplicate suppression stays inside the fs_id: re-enqueueing A's tuple
	// with a different task id is a no-op for A and must not touch B's row.
	created, err := storeA.EnqueueSemanticTask(ctx, newSemanticTask("task-iso-dup", "file-iso", 1, base, base))
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatal("duplicate enqueue within A should not create a second task")
	}

	// Each store claims only its own identical row.
	claimedA, found, err := storeA.ClaimSemanticTask(ctx, base.Add(time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !found || claimedA.TaskID != "task-iso" {
		t.Fatalf("A claim = found=%v task=%+v", found, claimedA)
	}
	claimedB, found, err := storeB.ClaimSemanticTask(ctx, base.Add(time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !found || claimedB.TaskID != "task-iso" || claimedB.AttemptCount != 1 {
		t.Fatalf("B claim = found=%v task=%+v, want its own untouched row", found, claimedB)
	}

	// B cannot ack with A's receipt; A's row is unaffected.
	if err := storeB.AckSemanticTask(ctx, "task-iso", claimedA.Receipt); !errors.Is(err, semantic.ErrTaskLeaseMismatch) {
		t.Fatalf("B ack with A receipt err=%v, want %v", err, semantic.ErrTaskLeaseMismatch)
	}
	if err := storeA.AckSemanticTask(ctx, "task-iso", claimedA.Receipt); err != nil {
		t.Fatal(err)
	}
	if task := mustGetSharedSemanticTask(t, storeA, "task-iso"); task.Status != semantic.TaskSucceeded {
		t.Fatalf("A task status=%q, want %q", task.Status, semantic.TaskSucceeded)
	}

	// Recover never crosses fs_id: A has no expired processing task, B does.
	recovered, err := storeA.RecoverExpiredSemanticTasks(ctx, base.Add(2*time.Minute), 0)
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 0 {
		t.Fatalf("A recovered=%d, want 0", recovered)
	}
	recovered, err = storeB.RecoverExpiredSemanticTasks(ctx, base.Add(2*time.Minute), 0)
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 1 {
		t.Fatalf("B recovered=%d, want 1", recovered)
	}

	// Queue observation is scoped per fs_id.
	obsA, err := storeA.ObserveSemanticTasks(ctx, base.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if obsA.Queued != 0 || obsA.Processing != 0 || obsA.DeadLettered != 0 {
		t.Fatalf("A observation = %+v, want all zero", obsA)
	}
	obsB, err := storeB.ObserveSemanticTasks(ctx, base.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if obsB.Queued != 1 || obsB.Processing != 0 || obsB.DeadLettered != 0 {
		t.Fatalf("B observation = %+v, want queued=1", obsB)
	}
}

// TestFileGCTasksSharedShapeCrossTenantIsolation proves file GC rows of one
// fs_id are invisible to another fs_id, and that the same file_id/task_id can
// coexist under both fs_ids.
func TestFileGCTasksSharedShapeCrossTenantIsolation(t *testing.T) {
	installSharedCoreFSSchema(t)
	ctx := context.Background()
	storeA := newSharedStore(t, 4300004)
	storeB := newSharedStore(t, 4300005)
	base := time.Now().UTC()

	// Same file_id (and therefore default task_id) under both fs_ids must
	// coexist; the storage refs differ so listing scoping is observable.
	for i, store := range []*Store{storeA, storeB} {
		inserted, err := store.EnqueueFileGCTaskTx(store.DB(), &FileGCTask{
			FileID:      "file-gc-iso",
			StorageType: StorageS3,
			StorageRef:  fmt.Sprintf("s3://bucket/iso-blob-%c", 'a'+i),
			SizeBytes:   int64(10 + i),
			AvailableAt: base,
		})
		if err != nil {
			t.Fatal(err)
		}
		if !inserted {
			t.Fatal("expected per-tenant file gc rows to coexist")
		}
	}
	gotA, err := storeA.GetFileGCTaskByFileID(ctx, "file-gc-iso")
	if err != nil {
		t.Fatal(err)
	}
	gotB, err := storeB.GetFileGCTaskByFileID(ctx, "file-gc-iso")
	if err != nil {
		t.Fatal(err)
	}
	if gotA.StorageRef != "s3://bucket/iso-blob-a" || gotB.StorageRef != "s3://bucket/iso-blob-b" {
		t.Fatalf("cross-tenant file gc rows mixed up: A=%+v B=%+v", gotA, gotB)
	}
	for name, store := range map[string]*Store{"A": storeA, "B": storeB} {
		count, err := store.CountQueuedFileGCTasks(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("%s queued file gc count=%d, want 1", name, count)
		}
	}

	// A claims and acks its own row; B's identical row stays queued.
	claimedA, found, err := storeA.ClaimFileGCTask(ctx, base.Add(time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !found || claimedA.StorageRef != "s3://bucket/iso-blob-a" {
		t.Fatalf("A claim = found=%v task=%+v", found, claimedA)
	}
	if err := storeA.AckFileGCTask(ctx, claimedA.TaskID, claimedA.Receipt); err != nil {
		t.Fatal(err)
	}
	gotB, err = storeB.GetFileGCTaskByFileID(ctx, "file-gc-iso")
	if err != nil {
		t.Fatal(err)
	}
	if gotB.Status != FileGCTaskQueued || gotB.AttemptCount != 0 {
		t.Fatalf("B row changed by A's claim/ack: %+v", gotB)
	}

	// S3 ref listing never crosses fs_id.
	refsA, _, err := storeA.ListFileGCTaskS3Refs(ctx, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(refsA) != 1 || refsA[0].StorageRef != "s3://bucket/iso-blob-a" {
		t.Fatalf("A refs = %+v, want only iso-blob-a", refsA)
	}
	refsB, _, err := storeB.ListFileGCTaskS3Refs(ctx, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(refsB) != 1 || refsB[0].StorageRef != "s3://bucket/iso-blob-b" {
		t.Fatalf("B refs = %+v, want only iso-blob-b", refsB)
	}

	// B cannot retry A's completed task id: B's own row is claimed below and
	// A's row stays succeeded. Recover never crosses fs_id either.
	claimedB, found, err := storeB.ClaimFileGCTask(ctx, base.Add(time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !found || claimedB.StorageRef != "s3://bucket/iso-blob-b" {
		t.Fatalf("B claim = found=%v task=%+v", found, claimedB)
	}
	recovered, err := storeA.RecoverExpiredFileGCTasks(ctx, base.Add(2*time.Minute), 0)
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 0 {
		t.Fatalf("A recovered=%d, want 0", recovered)
	}
	recovered, err = storeB.RecoverExpiredFileGCTasks(ctx, base.Add(2*time.Minute), 0)
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 1 {
		t.Fatalf("B recovered=%d, want 1", recovered)
	}
	if err := storeB.RetryFileGCTask(ctx, claimedA.TaskID, claimedA.Receipt, base.Add(3*time.Minute), "cross"); !errors.Is(err, ErrFileGCTaskLeaseMismatch) {
		t.Fatalf("B retry with A receipt err=%v, want %v", err, ErrFileGCTaskLeaseMismatch)
	}
	gotA, err = storeA.GetFileGCTaskByFileID(ctx, "file-gc-iso")
	if err != nil {
		t.Fatal(err)
	}
	if gotA.Status != FileGCTaskSucceeded {
		t.Fatalf("A row changed by B's retry: %+v", gotA)
	}
}

// TestSemanticTasksSharedShapeStoresFsID asserts every task row written by
// the core scenarios carries the scope's fs_id as its row key.
func TestSemanticTasksSharedShapeStoresFsID(t *testing.T) {
	installSharedCoreFSSchema(t)
	const fsID int64 = 4300006
	store := newSharedStore(t, fsID)
	runSemanticTaskCoreScenario(t, store, "fsid-sem")
	runFileGCTaskCoreScenario(t, store, "fsid-gc")

	for _, tbl := range []string{"semantic_tasks", "file_gc_tasks"} {
		var got int64
		err := store.DB().QueryRow("SELECT COUNT(*) FROM "+tbl+" WHERE fs_id != ?", fsID).Scan(&got)
		if err != nil {
			t.Fatalf("count %s rows with foreign fs_id: %v", tbl, err)
		}
		if got != 0 {
			t.Fatalf("%s has %d rows with fs_id != %d", tbl, got, fsID)
		}
		var total int64
		if err := store.DB().QueryRow("SELECT COUNT(*) FROM " + tbl).Scan(&total); err != nil {
			t.Fatalf("count %s: %v", tbl, err)
		}
		if total == 0 {
			t.Fatalf("%s is empty; scenario should have written rows", tbl)
		}
	}
}
