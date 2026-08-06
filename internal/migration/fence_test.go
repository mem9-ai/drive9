package migration

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newFenceWorker(t *testing.T) (*Worker, *Startup, *checkpointFake, *httptest.Server) {
	t.Helper()
	backend := &workerServer{target: &memoryTarget{nodes: make(map[string]memoryTargetNode)}, checkpoint: &checkpointFake{}, caps: allWorkerCapabilities()}
	server := httptest.NewServer(http.HandlerFunc(backend.handler))
	startup := newWorkerStartup(t, t.TempDir(), server)
	startup.Phase = PhaseDualWriteRepairing
	worker, err := NewWorker(context.Background(), startup)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	if err := worker.DeepRecovery(context.Background()); err != nil {
		server.Close()
		t.Fatal(err)
	}
	if _, err := worker.VerifyFull(context.Background()); err != nil {
		server.Close()
		t.Fatal(err)
	}
	return worker, startup, backend.checkpoint, server
}

func TestCutoverFenceSuccessDuplicateAndRestart(t *testing.T) {
	worker, startup, fake, server := newFenceWorker(t)
	defer server.Close()
	checkpoint, err := worker.PrepareCutover(context.Background())
	if err != nil || !checkpoint.FenceIntent || !checkpoint.FenceComplete || checkpoint.HighestPhase != PhaseCutoverReady {
		t.Fatalf("checkpoint=%+v err=%v", checkpoint, err)
	}
	if worker.State().Phase != PhaseCutoverReady || !worker.statusOutput().FenceComplete {
		t.Fatalf("fenced state=%+v", worker.statusOutput())
	}
	if err := worker.Round(context.Background(), RoundModeFast); !errors.Is(err, ErrIllegalAction) {
		t.Fatalf("post-intent Round error=%v", err)
	}
	fake.mu.Lock()
	writes := fake.writes
	fake.mu.Unlock()
	if _, err := worker.PrepareCutover(context.Background()); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	if fake.writes != writes {
		t.Fatalf("duplicate fence wrote %d -> %d", writes, fake.writes)
	}
	fake.mu.Unlock()
	restarted, err := NewWorker(context.Background(), startup)
	if err != nil || restarted.recovery.WritesAllowed || restarted.State().Phase != PhaseCutoverReady || !restarted.fenceComplete.Load() {
		t.Fatalf("restart=%+v err=%v", restarted, err)
	}
}

func TestCutoverFenceFailureSplitAndRecovery(t *testing.T) {
	t.Run("before intent resumes", func(t *testing.T) {
		worker, _, _, server := newFenceWorker(t)
		defer server.Close()
		worker.checkpoint.beforeWrite = func(Checkpoint) error { return errors.New("before intent") }
		if _, err := worker.PrepareCutover(context.Background()); err == nil || worker.writesFenced.Load() {
			t.Fatalf("before-intent err=%v fenced=%v", err, worker.writesFenced.Load())
		}
		worker.checkpoint.beforeWrite = nil
		if err := worker.Round(context.Background(), RoundModeFast); err != nil {
			t.Fatalf("repair did not resume: %v", err)
		}
	})
	t.Run("after intent recovers only forward", func(t *testing.T) {
		worker, startup, _, server := newFenceWorker(t)
		defer server.Close()
		worker.checkpoint.afterWrite = func(Checkpoint) error { return errors.New("after intent") }
		if _, err := worker.PrepareCutover(context.Background()); err == nil || !worker.fenceIntent.Load() || !worker.writesFenced.Load() {
			t.Fatalf("after-intent err=%v status=%+v", err, worker.statusOutput())
		}
		if err := worker.Round(context.Background(), RoundModeFast); !errors.Is(err, ErrIllegalAction) {
			t.Fatalf("post-intent Round error=%v", err)
		}
		worker.checkpoint.afterWrite = nil
		restarted, err := NewWorker(context.Background(), startup)
		if err != nil || !restarted.recovery.FenceRecoveryOnly {
			t.Fatalf("restart recovery=%+v err=%v", restarted, err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- restarted.Run(ctx) }()
		waitFor(t, restarted.fenceComplete.Load)
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("complete recovery err=%v", err)
		}
	})
	t.Run("complete retry", func(t *testing.T) {
		worker, _, _, server := newFenceWorker(t)
		defer server.Close()
		worker.checkpoint.beforeWrite = func(checkpoint Checkpoint) error {
			if checkpoint.FenceComplete {
				return errors.New("complete failure")
			}
			return nil
		}
		if _, err := worker.PrepareCutover(context.Background()); err == nil || !worker.fenceIntent.Load() || worker.fenceComplete.Load() {
			t.Fatalf("complete failure err=%v status=%+v", err, worker.statusOutput())
		}
		worker.checkpoint.beforeWrite = nil
		if _, err := worker.PrepareCutover(context.Background()); err != nil || !worker.fenceComplete.Load() {
			t.Fatalf("complete retry err=%v", err)
		}
	})
}

func TestCutoverRejectsEarlyAttentionAndStaleVerification(t *testing.T) {
	worker, _, _, server := newFenceWorker(t)
	defer server.Close()
	worker.state.SetAttention(true)
	if _, err := worker.PrepareCutover(context.Background()); !errors.Is(err, ErrIllegalAction) {
		t.Fatalf("Attention gate error=%v", err)
	}
	worker.state.SetAttention(false)
	if err := worker.Round(context.Background(), RoundModeFast); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.PrepareCutover(context.Background()); !errors.Is(err, ErrIllegalAction) {
		t.Fatalf("stale verification error=%v", err)
	}
}
