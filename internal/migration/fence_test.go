package migration

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newFenceWorker(t *testing.T) (*Worker, *Startup, *checkpointFake, *httptest.Server) {
	worker, startup, checkpoint, _, server := newFenceWorkerWithTarget(t)
	return worker, startup, checkpoint, server
}

func newFenceWorkerWithTarget(t *testing.T) (*Worker, *Startup, *checkpointFake, *memoryTarget, *httptest.Server) {
	t.Helper()
	target := &memoryTarget{nodes: make(map[string]memoryTargetNode)}
	backend := &workerServer{target: target, checkpoint: &checkpointFake{}, caps: allWorkerCapabilities()}
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
	return worker, startup, backend.checkpoint, target, server
}

func TestPrepareCutoverCancellationWhileWaitingForGateDoesNotFence(t *testing.T) {
	worker, _, fake, server := newFenceWorker(t)
	defer server.Close()
	if err := worker.controlGate.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer worker.controlGate.Release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := worker.PrepareCutover(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled cutover error=%v", err)
	}
	if worker.fenceIntent.Load() || worker.writesFenced.Load() {
		t.Fatalf("canceled cutover changed fence state: %+v", worker.statusOutput())
	}
	fake.mu.Lock()
	writes := fake.writes
	fake.mu.Unlock()
	if writes != 1 {
		t.Fatalf("canceled cutover checkpoint writes=%d, want startup write only", writes)
	}
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
	duplicateWrites := fake.writes
	fake.mu.Unlock()
	if duplicateWrites != writes {
		t.Fatalf("duplicate fence wrote %d -> %d", writes, duplicateWrites)
	}
	restarted, err := NewWorker(context.Background(), startup)
	if err != nil || restarted.recovery.WritesAllowed || restarted.State().Phase != PhaseCutoverReady || !restarted.fenceComplete.Load() {
		t.Fatalf("restart=%+v err=%v", restarted, err)
	}
}

func TestConfigMapCutoverRequestRunsFreshVerificationAndFence(t *testing.T) {
	_, startup, _, server := newFenceWorker(t)
	defer server.Close()
	startup.Phase = PhaseCutoverReady

	worker, err := NewWorker(context.Background(), startup)
	if err != nil {
		t.Fatal(err)
	}
	before := worker.statusOutput()
	if before.StartupPhase != PhaseCutoverReady || before.Phase != PhaseDualWriteRepairing || before.RecoveryComplete || before.Verification.Status != "" || before.FenceIntent || before.FenceComplete {
		t.Fatalf("pre-cutover status=%+v", before)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	waitFor(t, worker.fenceComplete.Load)
	after := worker.statusOutput()
	if after.Phase != PhaseCutoverReady || after.StartupPhase != PhaseCutoverReady || after.Verification.Status != "passed" || !after.FenceIntent || !after.FenceComplete {
		cancel()
		<-done
		t.Fatalf("automatic cutover status=%+v", after)
	}
	if _, err := worker.PrepareCutover(context.Background()); err != nil {
		cancel()
		<-done
		t.Fatalf("manual cutover after automatic completion: %v", err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("automatic cutover Worker error=%v", err)
	}
}

func TestConfigMapCutoverRequestRetriesTransientRecoveryFailure(t *testing.T) {
	_, startup, _, target, server := newFenceWorkerWithTarget(t)
	defer server.Close()
	startup.Phase = PhaseCutoverReady

	worker, err := NewWorker(context.Background(), startup)
	if err != nil {
		t.Fatal(err)
	}
	target.mu.Lock()
	target.failListCount = 1
	target.failListStatus = http.StatusServiceUnavailable
	target.mu.Unlock()
	retries := 0
	worker.retryWait = func(context.Context, time.Duration) error {
		retries++
		return nil
	}

	if err := worker.runRequestedCutover(context.Background()); err != nil {
		t.Fatalf("automatic cutover did not retry transient recovery failure: %v", err)
	}
	if retries != 1 || !worker.fenceComplete.Load() {
		t.Fatalf("automatic cutover retries=%d status=%+v", retries, worker.statusOutput())
	}
}

func TestConfigMapCutoverRequestFailsClosedWhenFreshVerificationFails(t *testing.T) {
	_, startup, _, server := newFenceWorker(t)
	defer server.Close()
	if err := os.WriteFile(filepath.Join(startup.Job.Source.Root, "unsafe-mode"), []byte("content"), 0o600); err != nil {
		t.Fatal(err)
	}
	startup.Phase = PhaseCutoverReady

	worker, err := NewWorker(context.Background(), startup)
	if err != nil {
		t.Fatal(err)
	}
	err = worker.Run(context.Background())
	if err == nil {
		t.Fatal("automatic cutover succeeded after failed verification")
	}
	status := worker.statusOutput()
	if status.Phase != PhaseDualWriteRepairing || status.Verification.Status != "failed" || status.FenceIntent || status.FenceComplete || !status.Conditions.Attention {
		t.Fatalf("failed automatic cutover status=%+v error=%v", status, err)
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
	t.Run("complete response loss is adopted", func(t *testing.T) {
		worker, _, _, server := newFenceWorker(t)
		defer server.Close()
		worker.checkpoint.afterWrite = func(checkpoint Checkpoint) error {
			if checkpoint.FenceComplete {
				return errors.New("lost complete response")
			}
			return nil
		}
		checkpoint, err := worker.PrepareCutover(context.Background())
		if err != nil || !checkpoint.FenceComplete || !worker.fenceComplete.Load() || worker.State().Phase != PhaseCutoverReady {
			t.Fatalf("adopted checkpoint=%+v status=%+v err=%v", checkpoint, worker.statusOutput(), err)
		}
	})
	t.Run("complete post-write read loss is reconciled", func(t *testing.T) {
		worker, _, fake, server := newFenceWorker(t)
		defer server.Close()
		fake.mu.Lock()
		fake.failGetAtWrite = 3
		fake.mu.Unlock()
		checkpoint, err := worker.PrepareCutover(context.Background())
		if err != nil || !checkpoint.FenceComplete || !worker.fenceComplete.Load() {
			t.Fatalf("reconciled checkpoint=%+v status=%+v err=%v", checkpoint, worker.statusOutput(), err)
		}
	})
}

func TestCutoverFenceIntentOutcomeUnknownStaysFencedAndRaisesAttention(t *testing.T) {
	worker, startup, fake, server := newFenceWorker(t)
	defer server.Close()
	worker.checkpoint.afterWrite = func(checkpoint Checkpoint) error {
		if checkpoint.FenceIntent && !checkpoint.FenceComplete {
			return errors.New("lost intent response")
		}
		return nil
	}
	fake.mu.Lock()
	fake.failGetAtWrite = 2
	fake.mu.Unlock()

	if _, err := worker.PrepareCutover(context.Background()); err == nil {
		t.Fatal("ambiguous fence intent unexpectedly succeeded")
	}
	status := worker.statusOutput()
	if !worker.writesFenced.Load() || status.FenceIntent || status.FenceComplete || !status.Conditions.Attention || status.AttentionReason != "fence_intent_outcome_unknown" {
		t.Fatalf("ambiguous intent status=%+v writes_fenced=%v", status, worker.writesFenced.Load())
	}
	if err := worker.Round(context.Background(), RoundModeFast); !errors.Is(err, ErrIllegalAction) {
		t.Fatalf("ambiguous intent allowed a write Round: %v", err)
	}

	worker.checkpoint.afterWrite = nil
	restarted, err := NewWorker(context.Background(), startup)
	if err != nil || !restarted.recovery.FenceRecoveryOnly {
		t.Fatalf("restart did not recover durable intent: recovery=%+v err=%v", restarted.recovery, err)
	}
}

func TestFenceRecoverySerializesWithControlCutover(t *testing.T) {
	worker, startup, _, targetServer := newFenceWorker(t)
	defer targetServer.Close()
	worker.checkpoint.afterWrite = func(checkpoint Checkpoint) error {
		if checkpoint.FenceIntent && !checkpoint.FenceComplete {
			return errors.New("lost fence intent response")
		}
		return nil
	}
	if _, err := worker.PrepareCutover(context.Background()); err == nil {
		t.Fatal("fence intent response-loss injection did not fail")
	}
	restarted, err := NewWorker(context.Background(), startup)
	if err != nil || !restarted.recovery.FenceRecoveryOnly {
		t.Fatalf("restart recovery=%+v err=%v", restarted.recovery, err)
	}

	writeStarted := make(chan struct{}, 2)
	releaseWrite := make(chan struct{})
	restarted.checkpoint.beforeWrite = func(checkpoint Checkpoint) error {
		if checkpoint.FenceComplete {
			writeStarted <- struct{}{}
			<-releaseWrite
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	socket := testControlSocket(t)
	controlServer, err := startControl(ctx, socket, restarted)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- restarted.Run(ctx) }()
	select {
	case <-writeStarted:
	case <-time.After(time.Second):
		cancel()
		controlServer.close()
		t.Fatal("startup fence recovery did not reach Checkpoint completion")
	}
	restarted.controlGate.initialize()
	gateHeldByRecovery := true
	select {
	case <-restarted.controlGate.token:
		gateHeldByRecovery = false
		restarted.controlGate.token <- struct{}{}
	default:
	}
	controlDone := make(chan error, 1)
	go func() {
		controlDone <- Control(context.Background(), socket, ControlRequest{Command: "prepare-drive9-cutover"}, io.Discard)
	}()
	close(releaseWrite)
	if err := <-controlDone; err != nil {
		cancel()
		controlServer.close()
		t.Fatal(err)
	}
	waitFor(t, restarted.fenceComplete.Load)
	cancel()
	if err := <-runDone; err != nil {
		controlServer.close()
		t.Fatal(err)
	}
	controlServer.close()
	if !gateHeldByRecovery {
		t.Fatal("startup fence recovery entered Checkpoint completion without the control Gate")
	}
}

func TestCutoverRejectsAttentionButAcceptsPassedVerificationAfterFastRound(t *testing.T) {
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
	if checkpoint, err := worker.PrepareCutover(context.Background()); err != nil || !checkpoint.FenceComplete {
		t.Fatalf("cutover after converged fast Round checkpoint=%+v err=%v", checkpoint, err)
	}
}
