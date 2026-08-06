package migration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestControlStatusDiffPermissionsAndUnavailable(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "secret-path"), []byte("sensitive-body-xyz"), 0o644); err != nil {
		t.Fatal(err)
	}
	worker, targetServer := newRoundWorker(t, root, &memoryTarget{nodes: make(map[string]memoryTargetNode)})
	defer targetServer.Close()
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	worker.state.BeginRound("diagnostic", RoundModeFull, time.Now())
	if err := worker.state.PublishRound(Round{ID: "diagnostic", Mode: RoundModeFull, ScanComplete: true, Findings: []Finding{{Path: "/secret-path", Kind: FindingContent, Severity: SeverityBlocker}}}); err != nil {
		t.Fatal(err)
	}
	socket := testControlSocket(t)
	ctx, cancel := context.WithCancel(context.Background())
	server, err := startControl(ctx, socket, worker)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(socket)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode=%v err=%v", info.Mode().Perm(), err)
	}
	var status bytes.Buffer
	if err := Control(context.Background(), socket, ControlRequest{Command: "status", Output: "json"}, &status); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(status.String(), "secret-path") || strings.Contains(status.String(), "sensitive-body-xyz") {
		t.Fatalf("status leaked path/content: %s", status.String())
	}
	var decoded StatusOutput
	if err := json.Unmarshal(status.Bytes(), &decoded); err != nil || decoded.Phase != PhaseSyncing || decoded.Current.ID == "" {
		t.Fatalf("status=%s err=%v", status.String(), err)
	}
	var diff bytes.Buffer
	if err := Control(context.Background(), socket, ControlRequest{Command: "diff", Output: "jsonl", Limit: 1}, &diff); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(diff.String(), "/secret-path") {
		t.Fatalf("explicit diff omitted path=%q", diff.String())
	}
	cancel()
	server.close()
	if err := Control(context.Background(), socket, ControlRequest{Command: "status"}, &status); !errors.Is(err, ErrControlUnavailable) {
		t.Fatalf("unavailable error=%v", err)
	}
}

func TestControlConcurrentReadsAndSerializedMutations(t *testing.T) {
	worker, targetServer := newRoundWorker(t, t.TempDir(), &memoryTarget{nodes: make(map[string]memoryTargetNode)})
	defer targetServer.Close()
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	socket := testControlSocket(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server, err := startControl(ctx, socket, worker)
	if err != nil {
		t.Fatal(err)
	}
	defer server.close()
	var wait sync.WaitGroup
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			var output bytes.Buffer
			if err := Control(ctx, socket, ControlRequest{Command: "status", Output: "json"}, &output); err != nil {
				t.Error(err)
			}
		}()
	}
	wait.Wait()
	for _, command := range []string{"verify-full", "prepare-drive9-cutover"} {
		if err := Control(ctx, socket, ControlRequest{Command: command}, &bytes.Buffer{}); !errors.Is(err, ErrIllegalAction) {
			t.Fatalf("%s error=%v", command, err)
		}
	}
}

func TestControlRejectsInvalidProtocolAndCoversStatusBranches(t *testing.T) {
	worker, targetServer := newRoundWorker(t, t.TempDir(), &memoryTarget{nodes: make(map[string]memoryTargetNode)})
	defer targetServer.Close()
	worker.startup = &Startup{Phase: PhaseDualWriteRepairing}
	worker.reporter = newEventReporter(worker.api, 1)
	worker.recovery = &Recovery{Record: CheckpointRecord{Checkpoint: Checkpoint{FenceIntent: true}}}
	worker.fenceIntent.Store(true)
	worker.state.SetAttention(true)
	status := worker.statusOutput()
	if status.StartupPhase != PhaseDualWriteRepairing || !status.FenceIntent || status.AttentionReason == "" {
		t.Fatalf("status branches=%+v", status)
	}
	for _, request := range []ControlRequest{{Command: "status"}, {Command: "diff", Output: "jsonl", Limit: -1}, {Command: "unknown"}} {
		if err := worker.handleControl(context.Background(), io.Discard, request); !errors.Is(err, ErrIllegalAction) {
			t.Fatalf("request=%+v error=%v", request, err)
		}
	}
	if err := worker.handleControl(context.Background(), io.Discard, ControlRequest{Command: "diff", Output: "jsonl"}); err != nil {
		t.Fatal(err)
	}
	socket := testControlSocket(t)
	ctx, cancel := context.WithCancel(context.Background())
	server, err := startControl(ctx, socket, worker)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); server.close() }()
	if _, err := startControl(ctx, socket, worker); err == nil {
		t.Fatal("duplicate socket listener succeeded")
	}
	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = conn.Write([]byte("{\"command\":\"status\",\"unknown\":true}\n"))
	response, _ := io.ReadAll(conn)
	_ = conn.Close()
	if !bytes.Contains(response, []byte(`"code":1`)) {
		t.Fatalf("invalid response=%s", response)
	}
}

func TestAttentionThresholdAndSuccessfulRecheck(t *testing.T) {
	target := &memoryTarget{nodes: make(map[string]memoryTargetNode)}
	worker, targetServer := newRoundWorker(t, t.TempDir(), target)
	defer targetServer.Close()
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	worker.now = func() time.Time { return time.Unix(0, now.Load()) }
	target.mu.Lock()
	target.failList, target.listHit = true, make(chan struct{}, 4)
	target.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	<-target.listHit
	waitFor(t, func() bool { return worker.State().Current.FailureClass == "scan" })
	now.Add(int64(attentionAfter))
	<-target.listHit
	now.Add(int64(attentionAfter))
	<-target.listHit
	waitFor(t, func() bool { return worker.State().Conditions.Attention })
	target.mu.Lock()
	target.failList = false
	target.mu.Unlock()
	<-target.listHit
	waitFor(t, func() bool { return !worker.State().Conditions.Attention })
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func waitFor(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !ready() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !ready() {
		t.Fatal("condition did not become true")
	}
}

func testControlSocket(t *testing.T) string {
	t.Helper()
	path := fmt.Sprintf("/tmp/drive9-migration-%d.sock", time.Now().UnixNano())
	t.Cleanup(func() { _ = os.Remove(path) })
	return path
}
