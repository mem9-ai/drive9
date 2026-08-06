package migration

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestVerifyFullUnfilteredIdempotentAndRestartLocal(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a"), []byte("same"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := &memoryTarget{nodes: map[string]memoryTargetNode{"a": {data: []byte("same"), revision: 1, mode: 0o100644, resourceID: "id"}}}
	now := time.Now()
	worker, server := newDualWorker(t, root, target, time.Minute, &now)
	defer server.Close()
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	worker.state.setRepairMtimeFloor(now.Add(time.Hour))
	reads := 0
	worker.scanner.afterRead = func(string) { reads++ }
	result, err := worker.VerifyFull(context.Background())
	if err != nil || result.Status != "passed" || result.SourceCount != 1 || reads == 0 || worker.State().Phase != PhaseDualWriteRepairing {
		t.Fatalf("verification=%+v reads=%d phase=%s err=%v", result, reads, worker.State().Phase, err)
	}
	reads = 0
	if _, err := worker.VerifyFull(context.Background()); err != nil || reads != 0 {
		t.Fatalf("idempotent verify reads=%d err=%v", reads, err)
	}
	restarted, restartedServer := newDualWorker(t, root, target, time.Minute, &now)
	defer restartedServer.Close()
	if restarted.State().Verification.Status != "" {
		t.Fatalf("restart inherited verification=%+v", restarted.State().Verification)
	}
}

func TestVerifyFullFailureOverlapAndControl(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a"), []byte("missing"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := &memoryTarget{nodes: make(map[string]memoryTargetNode)}
	now := time.Now()
	worker, server := newDualWorker(t, root, target, time.Minute, &now)
	defer server.Close()
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	result, err := worker.VerifyFull(context.Background())
	if !errors.Is(err, ErrVerificationFailed) || result.Status != "failed" || result.MismatchCount == 0 {
		t.Fatalf("failed verification=%+v err=%v", result, err)
	}
	if _, err := worker.VerifyFull(context.Background()); !errors.Is(err, ErrVerificationFailed) {
		t.Fatalf("failed result was not idempotent: %v", err)
	}
	worker.state.mu.Lock()
	worker.state.verification = VerificationState{Status: "running"}
	worker.state.mu.Unlock()
	if _, err := worker.VerifyFull(context.Background()); !errors.Is(err, ErrIllegalAction) {
		t.Fatalf("overlap error=%v", err)
	}
	worker.state.mu.Lock()
	worker.state.verification = VerificationState{}
	worker.state.phase = PhaseSyncing
	worker.state.mu.Unlock()
	if _, err := worker.VerifyFull(context.Background()); !errors.Is(err, ErrIllegalAction) {
		t.Fatalf("phase error=%v", err)
	}
	if err := worker.handleControl(context.Background(), &bytes.Buffer{}, ControlRequest{Command: "verify-full"}); !errors.Is(err, ErrIllegalAction) {
		t.Fatalf("control phase error=%v", err)
	}
}

func TestVerifyFullInterruptedRecordsFailure(t *testing.T) {
	now := time.Now()
	worker, server := newDualWorker(t, t.TempDir(), &memoryTarget{nodes: make(map[string]memoryTargetNode)}, time.Minute, &now)
	defer server.Close()
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := worker.VerifyFull(ctx)
	if !errors.Is(err, context.Canceled) || result.Status != "failed" {
		t.Fatalf("interrupted verification=%+v err=%v", result, err)
	}
}
