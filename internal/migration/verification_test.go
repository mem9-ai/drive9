package migration

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
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
	if err := os.WriteFile(filepath.Join(root, "a"), []byte("missing"), 0o600); err != nil {
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

func TestVerifyFullInterruptedAttemptCanBeRetried(t *testing.T) {
	now := time.Now()
	worker, server := newDualWorker(t, t.TempDir(), &memoryTarget{nodes: make(map[string]memoryTargetNode)}, time.Minute, &now)
	defer server.Close()
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := worker.VerifyFull(ctx)
	if !errors.Is(err, context.Canceled) || result.Status != "" {
		t.Fatalf("interrupted verification=%+v err=%v", result, err)
	}
	if result, err = worker.VerifyFull(context.Background()); err != nil || result.Status != "passed" {
		t.Fatalf("retry after interruption=%+v err=%v", result, err)
	}
}

func TestVerifyFullRetriesMoreThanFourSourceChanges(t *testing.T) {
	root := t.TempDir()
	filePath := filepath.Join(root, "a")
	if err := os.WriteFile(filePath, []byte("same"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := &memoryTarget{nodes: map[string]memoryTargetNode{"a": {data: []byte("same"), revision: 1, mode: 0o100644, resourceID: "id"}}}
	now := time.Now()
	worker, server := newDualWorker(t, root, target, time.Minute, &now)
	defer server.Close()
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	changes := 0
	worker.scanner.afterRead = func(string) {
		if changes < 5 {
			changes++
			if err := os.WriteFile(filePath, []byte("same"), 0o644); err != nil {
				t.Error(err)
			}
		}
	}
	worker.retryWait = func(context.Context, time.Duration) error { return nil }
	result, err := worker.VerifyFull(context.Background())
	if err != nil || result.Status != "passed" || changes != 5 {
		t.Fatalf("verification=%+v changes=%d err=%v", result, changes, err)
	}
}

func TestVerifyFullRetriesThrottleServerAndNetworkFailures(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
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
			target.mu.Lock()
			target.failListCount, target.failListStatus = 1, status
			target.mu.Unlock()
			worker.retryWait = func(context.Context, time.Duration) error { return nil }
			result, err := worker.VerifyFull(context.Background())
			if err != nil || result.Status != "passed" {
				t.Fatalf("verification=%+v err=%v", result, err)
			}
		})
	}

	t.Run("network", func(t *testing.T) {
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
		goodInventory := worker.inventory
		closed := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		closedURL := closed.URL
		closed.Close()
		badInventory, err := NewTargetScanner(client.New(closedURL, "key"), "/data")
		if err != nil {
			t.Fatal(err)
		}
		worker.inventory = badInventory
		worker.retryWait = func(context.Context, time.Duration) error {
			worker.inventory = goodInventory
			return nil
		}
		result, err := worker.VerifyFull(context.Background())
		if err != nil || result.Status != "passed" {
			t.Fatalf("verification=%+v err=%v", result, err)
		}
	})
}

func TestVerifyFullRefreshesCredentialAfterAuthenticationFailure(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "a"), []byte("same"), 0o644); err != nil {
				t.Fatal(err)
			}
			backend := &workerServer{
				target:     &memoryTarget{nodes: map[string]memoryTargetNode{"a": {data: []byte("same"), revision: 1, mode: 0o100644, resourceID: "id"}}},
				checkpoint: &checkpointFake{}, caps: allWorkerCapabilities(), rejectAuth: make(map[string]bool), rejectStatus: status,
			}
			server := httptest.NewServer(http.HandlerFunc(backend.handler))
			defer server.Close()
			startup := newWorkerStartup(t, root, server)
			startup.Phase = PhaseDualWriteRepairing
			worker, err := NewWorker(context.Background(), startup)
			if err != nil {
				t.Fatal(err)
			}
			if err := worker.DeepRecovery(context.Background()); err != nil {
				t.Fatal(err)
			}
			var rotate sync.Once
			backend.mu.Lock()
			backend.rejectAuth["Bearer first-key"] = true
			backend.onReject = func() {
				rotate.Do(func() {
					if err := os.WriteFile(startup.Credential.path, []byte("rotated-key\n"), 0o600); err != nil {
						t.Error(err)
					}
				})
			}
			backend.mu.Unlock()
			worker.retryWait = func(context.Context, time.Duration) error { return nil }

			result, err := worker.VerifyFull(context.Background())
			if err != nil || result.Status != "passed" {
				t.Fatalf("verification=%+v err=%v", result, err)
			}
			backend.mu.Lock()
			lastAuth := backend.auth[len(backend.auth)-1]
			backend.mu.Unlock()
			if lastAuth != "Bearer rotated-key" {
				t.Fatalf("verification retained old credential: %q", lastAuth)
			}
		})
	}
}

func TestVerifyFullWaitsForGraceRepairBeforeCompleting(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a"), []byte("new"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := &memoryTarget{nodes: make(map[string]memoryTargetNode)}
	now := time.Now()
	worker, server := newDualWorker(t, root, target, time.Minute, &now)
	defer server.Close()
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	worker.retryWait = func(context.Context, time.Duration) error {
		now = now.Add(time.Minute + time.Second)
		return nil
	}
	result, err := worker.VerifyFull(context.Background())
	if err != nil || result.Status != "passed" {
		t.Fatalf("verification=%+v err=%v", result, err)
	}
	target.mu.Lock()
	data := append([]byte(nil), target.nodes["a"].data...)
	target.mu.Unlock()
	if string(data) != "new" {
		t.Fatalf("verification did not complete grace repair: %q", data)
	}
}

func TestVerifyFullLongGraceDoesNotSetAttention(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a"), []byte("new"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := &memoryTarget{nodes: make(map[string]memoryTargetNode)}
	now := time.Now()
	grace := maximumGracePeriod
	worker, server := newDualWorker(t, root, target, grace, &now)
	defer server.Close()
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	waits := 0
	sawAttention := false
	worker.retryWait = func(context.Context, time.Duration) error {
		if worker.State().Conditions.Attention {
			sawAttention = true
		}
		waits++
		if waits == 1 {
			now = now.Add(attentionAfter + time.Second)
		} else {
			now = now.Add(grace)
		}
		return nil
	}
	result, err := worker.VerifyFull(context.Background())
	if err != nil || result.Status != "passed" {
		t.Fatalf("verification=%+v err=%v", result, err)
	}
	if sawAttention || worker.State().Conditions.Attention {
		t.Fatal("healthy Grace wait raised Attention")
	}
}

func TestVerifyFullRetriesExpectedSourceChurnBeforePublishingResult(t *testing.T) {
	root := t.TempDir()
	filePath := filepath.Join(root, "a")
	if err := os.WriteFile(filePath, []byte("same"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := &memoryTarget{nodes: map[string]memoryTargetNode{"a": {data: []byte("same"), revision: 1, mode: 0o100644, resourceID: "id"}}}
	now := time.Now()
	worker, server := newDualWorker(t, root, target, time.Minute, &now)
	defer server.Close()
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	removed := false
	worker.scanner.beforeEntry = func(name string) {
		if !removed && name == filePath {
			removed = true
			_ = os.Remove(name)
		}
	}
	result, err := worker.VerifyFull(context.Background())
	if err != nil || result.Status != "passed" || !removed {
		t.Fatalf("verification after churn=%+v removed=%v err=%v", result, removed, err)
	}
}
