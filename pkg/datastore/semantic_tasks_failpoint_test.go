//go:build failpoint

package datastore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pingcap/failpoint"

	"github.com/mem9-ai/dat9/pkg/semantic"
)

const semanticTaskLeaseNowFailpoint = "github.com/mem9-ai/dat9/pkg/datastore/semanticTaskLeaseNow"

func enableSemanticTaskLeaseNowFailpoint(t *testing.T, now time.Time) {
	t.Helper()
	if err := failpoint.Enable(semanticTaskLeaseNowFailpoint, `return("`+now.UTC().Format(time.RFC3339Nano)+`")`); err != nil {
		t.Fatalf("enable failpoint %s: %v", semanticTaskLeaseNowFailpoint, err)
	}
	t.Cleanup(func() {
		_ = failpoint.Disable(semanticTaskLeaseNowFailpoint)
	})
}

func TestSemanticTaskAckFailsWhenLeaseExpiresAtInjectedNow(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	base := time.Unix(1711601000, 0).UTC()

	if _, err := s.EnqueueSemanticTask(ctx, newSemanticTask("task-ack-expired", "file-1", 1, base, base)); err != nil {
		t.Fatal(err)
	}
	claimed, found, err := s.ClaimSemanticTask(ctx, base, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected claim to find task")
	}

	enableSemanticTaskLeaseNowFailpoint(t, base.Add(2*time.Minute))
	err = s.AckSemanticTask(ctx, claimed.TaskID, claimed.Receipt)
	if !errors.Is(err, semantic.ErrTaskLeaseMismatch) {
		t.Fatalf("ack error=%v, want %v", err, semantic.ErrTaskLeaseMismatch)
	}

	task := mustGetSemanticTask(t, s, claimed.TaskID)
	if task.Status != semantic.TaskProcessing {
		t.Fatalf("status=%q, want %q", task.Status, semantic.TaskProcessing)
	}
}

func TestSemanticTaskRetryFailsWhenLeaseExpiresAtInjectedNow(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	base := time.Unix(1711601100, 0).UTC()

	if _, err := s.EnqueueSemanticTask(ctx, newSemanticTask("task-retry-expired", "file-1", 1, base, base)); err != nil {
		t.Fatal(err)
	}
	claimed, found, err := s.ClaimSemanticTask(ctx, base, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected claim to find task")
	}

	enableSemanticTaskLeaseNowFailpoint(t, base.Add(2*time.Minute))
	err = s.RetrySemanticTask(ctx, claimed.TaskID, claimed.Receipt, time.Time{}, "temporary failure")
	if !errors.Is(err, semantic.ErrTaskLeaseMismatch) {
		t.Fatalf("retry error=%v, want %v", err, semantic.ErrTaskLeaseMismatch)
	}

	task := mustGetSemanticTask(t, s, claimed.TaskID)
	if task.Status != semantic.TaskProcessing {
		t.Fatalf("status=%q, want %q", task.Status, semantic.TaskProcessing)
	}
}

func TestRecoverExpiredSemanticTasksUsesInjectedNow(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	base := time.Unix(1711601200, 0).UTC()

	for _, task := range []*semantic.Task{
		newSemanticTask("task-expired", "file-expired", 1, base, base),
		newSemanticTask("task-active", "file-active", 1, base, base.Add(time.Second)),
	} {
		if _, err := s.EnqueueSemanticTask(ctx, task); err != nil {
			t.Fatal(err)
		}
	}

	expiredClaim, found, err := s.ClaimSemanticTask(ctx, base, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !found || expiredClaim.TaskID != "task-expired" {
		t.Fatalf("first claim=%+v found=%v, want task-expired", expiredClaim, found)
	}

	activeClaim, found, err := s.ClaimSemanticTask(ctx, base, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !found || activeClaim.TaskID != "task-active" {
		t.Fatalf("second claim=%+v found=%v, want task-active", activeClaim, found)
	}

	enableSemanticTaskLeaseNowFailpoint(t, base.Add(2*time.Minute))
	recovered, err := s.RecoverExpiredSemanticTasks(ctx, time.Time{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 1 {
		t.Fatalf("recovered=%d, want 1", recovered)
	}

	expiredTask := mustGetSemanticTask(t, s, "task-expired")
	if expiredTask.Status != semantic.TaskQueued {
		t.Fatalf("expired task status=%q, want %q", expiredTask.Status, semantic.TaskQueued)
	}
	activeTask := mustGetSemanticTask(t, s, "task-active")
	if activeTask.Status != semantic.TaskProcessing {
		t.Fatalf("active task status=%q, want %q", activeTask.Status, semantic.TaskProcessing)
	}
}
