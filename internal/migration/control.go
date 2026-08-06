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
	"sync"
	"time"
)

const (
	DefaultControlSocket = "/run/drive9-migration/control.sock"
	controlLimit         = 64 << 10
	controlDeadline      = 5 * time.Second
)

type ControlRequest struct {
	Command string `json:"command"`
	Output  string `json:"output,omitempty"`
	Type    string `json:"type,omitempty"`
	Limit   int    `json:"limit,omitempty"`
}

type StatusOutput struct {
	Phase            Phase               `json:"phase"`
	StartupPhase     Phase               `json:"startup_phase"`
	RecoveryComplete bool                `json:"recovery_complete"`
	Current          RoundStatus         `json:"round"`
	Conditions       Conditions          `json:"conditions"`
	RepairMtimeFloor *time.Time          `json:"repair_mtime_floor,omitempty"`
	SourceCount      int                 `json:"source_count"`
	Findings         map[FindingKind]int `json:"finding_counts"`
	GraceCandidates  int                 `json:"grace_candidates"`
	CASRetry         int                 `json:"cas_retry"`
	InFlight         int                 `json:"in_flight"`
	Reporter         ReporterSnapshot    `json:"event_reporter"`
	Verification     VerificationState   `json:"full_verification"`
	FenceIntent      bool                `json:"fence_intent"`
	FenceComplete    bool                `json:"fence_complete"`
	AttentionReason  string              `json:"attention_reason,omitempty"`
}

type controlServer struct {
	listener net.Listener
	path     string
	worker   *Worker
	once     sync.Once
	wait     sync.WaitGroup
}

func startControl(ctx context.Context, path string, worker *Worker) (*controlServer, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = listener.Close()
		return nil, err
	}
	server := &controlServer{listener: listener, path: path, worker: worker}
	server.wait.Add(1)
	go server.serve()
	go func() { <-ctx.Done(); server.close() }()
	return server, nil
}

func (s *controlServer) serve() {
	defer s.wait.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.wait.Add(1)
		go func() { defer s.wait.Done(); s.handle(conn) }()
	}
}

func (s *controlServer) handle(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(controlDeadline))
	line, err := bufio.NewReader(io.LimitReader(conn, controlLimit+1)).ReadBytes('\n')
	if err != nil || len(line) > controlLimit {
		writeControlError(conn, errors.New("invalid or oversized control request"))
		return
	}
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.DisallowUnknownFields()
	var request ControlRequest
	if decoder.Decode(&request) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		writeControlError(conn, errors.New("invalid control request"))
		return
	}
	mutation := request.Command == "verify-full" || request.Command == "prepare-drive9-cutover"
	if mutation {
		s.worker.controlMu.Lock()
		defer s.worker.controlMu.Unlock()
	}
	ctx, cancel := context.WithTimeout(context.Background(), controlDeadline)
	defer cancel()
	if err := s.worker.handleControl(ctx, conn, request); err != nil {
		writeControlError(conn, err)
	}
}

func (s *controlServer) close() {
	s.once.Do(func() {
		_ = s.listener.Close()
		s.wait.Wait()
		_ = os.Remove(s.path)
	})
}

func (w *Worker) handleControl(ctx context.Context, output io.Writer, request ControlRequest) error {
	switch request.Command {
	case "status":
		if request.Output != "json" {
			return ErrIllegalAction
		}
		return json.NewEncoder(output).Encode(w.statusOutput())
	case "diff":
		if request.Output != "jsonl" || request.Limit < 0 {
			return ErrIllegalAction
		}
		snapshot, written := w.state.Snapshot(), 0
		if snapshot.LastComplete == nil {
			return nil
		}
		for _, finding := range snapshot.LastComplete.Findings {
			if request.Type != "" && request.Type != string(finding.Kind) {
				continue
			}
			if err := json.NewEncoder(output).Encode(finding); err != nil {
				return err
			}
			written++
			if request.Limit > 0 && written == request.Limit {
				break
			}
		}
		return nil
	case "verify-full":
		result, err := w.VerifyFull(ctx)
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(result)
	case "prepare-drive9-cutover":
		result, err := w.PrepareCutover(ctx)
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(result)
	default:
		return ErrIllegalAction
	}
}

func (w *Worker) statusOutput() StatusOutput {
	snapshot := w.state.Snapshot()
	status := StatusOutput{Phase: snapshot.Phase, StartupPhase: snapshot.Phase, RecoveryComplete: snapshot.RecoveryComplete, Current: snapshot.Current, Conditions: snapshot.Conditions, RepairMtimeFloor: snapshot.RepairMtimeFloor, GraceCandidates: len(snapshot.Grace), CASRetry: len(snapshot.Retry), InFlight: snapshot.ActiveOperations, Verification: snapshot.Verification, Findings: make(map[FindingKind]int)}
	if w.startup != nil {
		status.StartupPhase = w.startup.Phase
	}
	if snapshot.LastComplete != nil {
		status.SourceCount = len(snapshot.LastComplete.Source)
		for _, finding := range snapshot.LastComplete.Findings {
			status.Findings[finding.Kind]++
		}
	}
	if w.reporter != nil {
		status.Reporter = w.reporter.snapshot()
	}
	status.FenceIntent, status.FenceComplete = w.fenceIntent.Load(), w.fenceComplete.Load()
	if snapshot.Conditions.Attention {
		status.AttentionReason = snapshot.Current.FailureClass
		if status.AttentionReason == "" {
			status.AttentionReason = "unsafe_or_persistent_blocker"
		}
	}
	return status
}

type controlFailure struct {
	Error string `json:"error"`
	Code  int    `json:"code"`
}

func writeControlError(output io.Writer, err error) {
	code := 1
	if errors.Is(err, ErrIllegalAction) || errors.Is(err, ErrInvalidPhase) {
		code = 2
	}
	_ = json.NewEncoder(output).Encode(controlFailure{Error: err.Error(), Code: code})
}

func Control(ctx context.Context, socket string, request ControlRequest, output io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, controlDeadline)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", socket)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrControlUnavailable, err)
	}
	defer func() { _ = conn.Close() }()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return err
	}
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		line := scanner.Bytes()
		var failure controlFailure
		if json.Unmarshal(line, &failure) == nil && failure.Error != "" {
			if failure.Code == 2 {
				return fmt.Errorf("%w: %s", ErrIllegalAction, failure.Error)
			}
			return errors.New(failure.Error)
		}
		if output != nil {
			_, _ = output.Write(append(append([]byte(nil), line...), '\n'))
		}
	}
	return scanner.Err()
}
