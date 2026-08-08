package migration

import (
	"bufio"
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

func TestStartControlReclaimsStaleSocket(t *testing.T) {
	socket := testControlSocket(t)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	unixListener, ok := listener.(*net.UnixListener)
	if !ok {
		t.Fatalf("listener type=%T", listener)
	}
	unixListener.SetUnlinkOnClose(false)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(socket)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("stale socket mode=%v", info.Mode())
	}

	ctx, cancel := context.WithCancel(context.Background())
	server, err := startControl(ctx, socket, &Worker{})
	if err != nil {
		cancel()
		t.Fatalf("reclaim stale socket: %v", err)
	}
	cancel()
	server.close()
}

func TestStartControlDoesNotRemoveNonSocketPath(t *testing.T) {
	socket := testControlSocket(t)
	const contents = "not-a-socket"
	if err := os.WriteFile(socket, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := startControl(context.Background(), socket, &Worker{}); err == nil {
		t.Fatal("non-socket path was replaced")
	}
	got, err := os.ReadFile(socket)
	if err != nil || string(got) != contents {
		t.Fatalf("non-socket contents=%q err=%v", got, err)
	}
}

func TestStatusReportsPendingAndBlockedUploadInFlight(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := &memoryTarget{
		nodes: make(map[string]memoryTargetNode), putStarted: make(chan struct{}, 1), putRelease: make(chan struct{}),
	}
	worker, server := newRoundWorker(t, root, target)
	defer server.Close()
	worker.apply.onOperationStart = worker.state.beginOperation
	worker.apply.onOperationDone = worker.state.endOperation

	done := make(chan error, 1)
	go func() { done <- worker.Round(context.Background(), RoundModeFull) }()
	select {
	case <-target.putStarted:
	case <-time.After(time.Second):
		t.Fatal("upload did not start")
	}
	status := worker.statusOutput()
	if status.PendingRepairs != 1 || status.InFlight != 1 {
		t.Fatalf("blocked upload status=%+v", status)
	}
	close(target.putRelease)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	status = worker.statusOutput()
	if status.PendingRepairs != 0 || status.InFlight != 0 {
		t.Fatalf("completed upload status=%+v", status)
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

func TestControlMutationWaitsForRoundAndCancellationBeforeAcceptanceStopsIt(t *testing.T) {
	now := time.Now()
	worker, targetServer := newDualWorker(t, t.TempDir(), &memoryTarget{nodes: make(map[string]memoryTargetNode)}, time.Minute, &now)
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

	if err := worker.controlGate.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		result <- Control(context.Background(), socket, ControlRequest{Command: "verify-full"}, io.Discard)
	}()
	select {
	case err := <-result:
		worker.controlGate.Release()
		t.Fatalf("mutation did not wait for active Round: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	worker.controlGate.Release()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if worker.State().Verification.Status != "passed" {
		t.Fatalf("waited verification=%+v", worker.State().Verification)
	}

	worker.state.mu.Lock()
	worker.state.verification = VerificationState{}
	worker.state.mu.Unlock()
	if err := worker.controlGate.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	callCtx, callCancel := context.WithCancel(context.Background())
	result = make(chan error, 1)
	go func() {
		result <- Control(callCtx, socket, ControlRequest{Command: "verify-full"}, io.Discard)
	}()
	time.Sleep(20 * time.Millisecond)
	callCancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		worker.controlGate.Release()
		t.Fatalf("pre-acceptance cancellation error=%v", err)
	}
	worker.controlGate.Release()
	time.Sleep(20 * time.Millisecond)
	if status := worker.State().Verification.Status; status != "" {
		t.Fatalf("canceled verification executed later: status=%q", status)
	}
}

func TestSerialGateDoesNotAcquireWithAlreadyCanceledContext(t *testing.T) {
	var gate serialGate
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range 100 {
		if err := gate.Acquire(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled Gate acquisition error=%v", err)
		}
	}
	if err := gate.Acquire(context.Background()); err != nil {
		t.Fatalf("canceled acquisition consumed Gate token: %v", err)
	}
	gate.Release()
}

func TestControlClientSeparatesHandshakeFromOperationLifetime(t *testing.T) {
	priorDeadline := controlIODeadline
	controlIODeadline = 10 * time.Millisecond
	t.Cleanup(func() { controlIODeadline = priorDeadline })

	socket := testControlSocket(t)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	done := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			done <- acceptErr
			return
		}
		defer func() { _ = conn.Close() }()
		var request ControlRequest
		if decodeErr := json.NewDecoder(conn).Decode(&request); decodeErr != nil {
			done <- decodeErr
			return
		}
		if _, writeErr := io.WriteString(conn, "{\"type\":\"accepted\",\"command\":\"verify-full\"}\n"); writeErr != nil {
			done <- writeErr
			return
		}
		time.Sleep(3 * controlIODeadline)
		_, writeErr := io.WriteString(conn, "{\"status\":\"passed\"}\n{\"type\":\"terminal\",\"command\":\"verify-full\",\"ok\":true}\n")
		done <- writeErr
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var output bytes.Buffer
	if err := Control(ctx, socket, ControlRequest{Command: "verify-full"}, &output); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "accepted") || !strings.Contains(output.String(), "passed") {
		t.Fatalf("response=%q", output.String())
	}
}

func TestControlRequiresTypedTerminalResponse(t *testing.T) {
	t.Run("zero response", func(t *testing.T) {
		for _, request := range []ControlRequest{
			{Command: "status", Output: "json"},
			{Command: "verify-full"},
			{Command: "prepare-drive9-cutover"},
		} {
			socket := fakeControlResponder(t, func(net.Conn) {})
			if err := Control(context.Background(), socket, request, io.Discard); !errors.Is(err, ErrControlUnavailable) {
				t.Fatalf("%s zero-response error=%v", request.Command, err)
			}
		}
	})

	t.Run("malformed payload", func(t *testing.T) {
		socket := fakeControlResponder(t, func(conn net.Conn) {
			_, _ = io.WriteString(conn, "{\"bogus\":true}\n{\"type\":\"terminal\",\"command\":\"status\",\"ok\":true}\n")
		})
		if err := Control(context.Background(), socket, ControlRequest{Command: "status", Output: "json"}, io.Discard); err == nil {
			t.Fatal("malformed status payload succeeded")
		}
	})

	t.Run("response loss after acceptance", func(t *testing.T) {
		socket := fakeControlResponder(t, func(conn net.Conn) {
			_, _ = io.WriteString(conn, "{\"type\":\"accepted\",\"command\":\"prepare-drive9-cutover\"}\n")
		})
		if err := Control(context.Background(), socket, ControlRequest{Command: "prepare-drive9-cutover"}, io.Discard); !errors.Is(err, ErrControlOutcomeUnknown) {
			t.Fatalf("accepted response-loss error=%v", err)
		}
	})

	t.Run("wrong terminal command", func(t *testing.T) {
		socket := fakeControlResponder(t, func(conn net.Conn) {
			_, _ = io.WriteString(conn, "{\"phase\":\"SYNCING\"}\n{\"type\":\"terminal\",\"command\":\"diff\",\"ok\":true}\n")
		})
		if err := Control(context.Background(), socket, ControlRequest{Command: "status", Output: "json"}, io.Discard); err == nil {
			t.Fatal("mismatched terminal command succeeded")
		}
	})
}

func TestAcceptedCutoverContinuesAfterCallerLosesResponse(t *testing.T) {
	worker, _, _, targetServer := newFenceWorker(t)
	defer targetServer.Close()
	started, release := make(chan struct{}), make(chan struct{})
	worker.checkpoint.beforeWrite = func(checkpoint Checkpoint) error {
		if checkpoint.FenceIntent && !checkpoint.FenceComplete {
			close(started)
			<-release
		}
		return nil
	}
	socket := testControlSocket(t)
	ctx, cancel := context.WithCancel(context.Background())
	server, err := startControl(ctx, socket, worker)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); server.close() }()
	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewEncoder(conn).Encode(ControlRequest{Command: "prepare-drive9-cutover"}); err != nil {
		t.Fatal(err)
	}
	accepted, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil || !bytes.Contains(accepted, []byte(`"type":"accepted"`)) {
		t.Fatalf("accepted frame=%s err=%v", accepted, err)
	}
	<-started
	_ = conn.Close()
	close(release)
	waitFor(t, func() bool { return worker.fenceComplete.Load() })
	if worker.State().Phase != PhaseCutoverReady {
		t.Fatalf("lost response interrupted accepted cutover: %+v", worker.statusOutput())
	}
}

func TestControlEmptyDiffRequiresTerminalButKeepsStdoutEmpty(t *testing.T) {
	worker, targetServer := newRoundWorker(t, t.TempDir(), &memoryTarget{nodes: make(map[string]memoryTargetNode)})
	defer targetServer.Close()
	socket := testControlSocket(t)
	ctx, cancel := context.WithCancel(context.Background())
	server, err := startControl(ctx, socket, worker)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); server.close() }()
	var output bytes.Buffer
	if err := Control(context.Background(), socket, ControlRequest{Command: "diff", Output: "jsonl"}, &output); err != nil {
		t.Fatal(err)
	}
	if output.Len() != 0 {
		t.Fatalf("empty diff output=%q", output.String())
	}
}

func TestControlVerificationCanOutliveHandshakeDeadline(t *testing.T) {
	priorDeadline := controlIODeadline
	controlIODeadline = 10 * time.Millisecond
	t.Cleanup(func() { controlIODeadline = priorDeadline })

	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := &memoryTarget{nodes: map[string]memoryTargetNode{
		"file": {data: []byte("content"), revision: 1, mode: 0o100644, resourceID: "file"},
	}}
	now := time.Now()
	worker, targetServer := newDualWorker(t, root, target, time.Minute, &now)
	defer targetServer.Close()
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	worker.scanner.beforeEntry = func(string) { time.Sleep(3 * controlIODeadline) }

	socket := testControlSocket(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server, err := startControl(ctx, socket, worker)
	if err != nil {
		t.Fatal(err)
	}
	defer server.close()
	callCtx, callCancel := context.WithTimeout(context.Background(), time.Second)
	defer callCancel()
	var output bytes.Buffer
	if err := Control(callCtx, socket, ControlRequest{Command: "verify-full"}, &output); err != nil {
		t.Fatal(err)
	}
	if worker.State().Verification.Status != "passed" {
		t.Fatalf("verification=%+v output=%s", worker.State().Verification, output.String())
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

func TestStatusIncludesSecretFreeJobMapping(t *testing.T) {
	worker := &Worker{
		state: NewState(PhaseSyncing),
		startup: &Startup{
			Job: Job{
				VolumeID: "vol-001", NodeName: "node-a",
				Target: TargetConfig{SpaceRef: "space-a", Prefix: "/tenant-a"},
			},
			Space: SpaceConfig{CredentialRef: "owner-key"},
		},
	}

	status := worker.statusOutput()
	if status.VolumeID != "vol-001" || status.NodeName != "node-a" || status.SpaceRef != "space-a" || status.Prefix != "/tenant-a" || status.CredentialRef != "owner-key" {
		t.Fatalf("status mapping=%+v", status)
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

func fakeControlResponder(t *testing.T, respond func(net.Conn)) string {
	t.Helper()
	socket := testControlSocket(t)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		var request ControlRequest
		if json.NewDecoder(conn).Decode(&request) != nil {
			return
		}
		respond(conn)
	}()
	return socket
}
