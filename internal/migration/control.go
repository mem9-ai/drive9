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
)

var controlIODeadline = 5 * time.Second

type ControlRequest struct {
	Command string `json:"command"`
	Output  string `json:"output,omitempty"`
	Type    string `json:"type,omitempty"`
	Limit   int    `json:"limit,omitempty"`
}

const (
	controlFrameAccepted = "accepted"
	controlFrameTerminal = "terminal"
)

type controlFrame struct {
	Type    string `json:"type"`
	Command string `json:"command"`
	OK      bool   `json:"ok,omitempty"`
	Error   string `json:"error,omitempty"`
	Code    int    `json:"code,omitempty"`
}

type serialGate struct {
	once  sync.Once
	token chan struct{}
}

func (g *serialGate) initialize() {
	g.once.Do(func() {
		g.token = make(chan struct{}, 1)
		g.token <- struct{}{}
	})
}

func (g *serialGate) Acquire(ctx context.Context) error {
	g.initialize()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.token:
		if err := ctx.Err(); err != nil {
			g.token <- struct{}{}
			return err
		}
		return nil
	}
}

func (g *serialGate) Release() {
	g.initialize()
	g.token <- struct{}{}
}

type StatusOutput struct {
	Phase            Phase               `json:"phase"`
	StartupPhase     Phase               `json:"startup_phase"`
	RecoveryComplete bool                `json:"recovery_complete"`
	Current          RoundStatus         `json:"round"`
	Conditions       Conditions          `json:"conditions"`
	RepairMtimeFloor *time.Time          `json:"repair_mtime_floor,omitempty"`
	SourceCount      int                 `json:"source_count"`
	CandidateCounts  CandidateCounts     `json:"candidate_counts"`
	Findings         map[FindingKind]int `json:"finding_counts"`
	GraceCandidates  int                 `json:"grace_candidates"`
	CASRetry         int                 `json:"cas_retry"`
	Backlog          int                 `json:"backlog"`
	PendingRepairs   int                 `json:"pending_repairs"`
	InFlight         int                 `json:"in_flight"`
	Reporter         ReporterSnapshot    `json:"event_reporter"`
	Verification     VerificationState   `json:"full_verification"`
	FenceIntent      bool                `json:"fence_intent"`
	FenceComplete    bool                `json:"fence_complete"`
	AttentionReason  string              `json:"attention_reason,omitempty"`
}

type controlServer struct {
	ctx      context.Context
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
	server := &controlServer{ctx: ctx, listener: listener, path: path, worker: worker}
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
	_ = conn.SetReadDeadline(time.Now().Add(controlIODeadline))
	line, err := bufio.NewReader(io.LimitReader(conn, controlLimit+1)).ReadBytes('\n')
	if err != nil || len(line) > controlLimit {
		_ = writeControlTerminal(conn, "", errors.New("invalid or oversized control request"))
		return
	}
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.DisallowUnknownFields()
	var request ControlRequest
	if decoder.Decode(&request) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		_ = writeControlTerminal(conn, "", errors.New("invalid control request"))
		return
	}
	_ = conn.SetReadDeadline(time.Time{})
	output := controlDeadlineWriter{conn: conn}
	if isControlMutation(request.Command) {
		waitCtx, cancelWait := context.WithCancel(s.ctx)
		go func() {
			var extra [1]byte
			_, _ = conn.Read(extra[:])
			cancelWait()
		}()
		if err := s.worker.controlGate.Acquire(waitCtx); err != nil {
			cancelWait()
			return
		}
		cancelWait()
		defer s.worker.controlGate.Release()
		if err := writeControlFrame(output, controlFrame{Type: controlFrameAccepted, Command: request.Command}); err != nil {
			return
		}
	}
	err = s.worker.handleControlLocked(s.ctx, output, request)
	_ = writeControlTerminal(output, request.Command, err)
}

type controlDeadlineWriter struct {
	conn net.Conn
}

func (w controlDeadlineWriter) Write(body []byte) (int, error) {
	if err := w.conn.SetWriteDeadline(time.Now().Add(controlIODeadline)); err != nil {
		return 0, err
	}
	return w.conn.Write(body)
}

func (s *controlServer) close() {
	s.once.Do(func() {
		_ = s.listener.Close()
		s.wait.Wait()
		_ = os.Remove(s.path)
	})
}

func (w *Worker) handleControl(ctx context.Context, output io.Writer, request ControlRequest) error {
	if isControlMutation(request.Command) {
		if err := w.controlGate.Acquire(ctx); err != nil {
			return err
		}
		defer w.controlGate.Release()
	}
	return w.handleControlLocked(ctx, output, request)
}

func (w *Worker) handleControlLocked(ctx context.Context, output io.Writer, request ControlRequest) error {
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
		result, err := w.verifyFullLocked(ctx)
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(result)
	case "prepare-drive9-cutover":
		result, err := w.prepareCutoverLocked(ctx)
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
	status := StatusOutput{Phase: snapshot.Phase, StartupPhase: snapshot.Phase, RecoveryComplete: snapshot.RecoveryComplete, Current: snapshot.Current, Conditions: snapshot.Conditions, RepairMtimeFloor: snapshot.RepairMtimeFloor, CandidateCounts: snapshot.CandidateCounts, GraceCandidates: len(snapshot.Grace), CASRetry: len(snapshot.Retry), Backlog: len(snapshot.Retry), PendingRepairs: snapshot.PendingRepairs, InFlight: snapshot.ActiveOperations, Verification: snapshot.Verification, Findings: make(map[FindingKind]int)}
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

func isControlMutation(command string) bool {
	return command == "verify-full" || command == "prepare-drive9-cutover"
}

func controlErrorCode(err error) int {
	code := 1
	if errors.Is(err, ErrIllegalAction) || errors.Is(err, ErrInvalidPhase) {
		code = 2
	}
	return code
}

func writeControlFrame(output io.Writer, frame controlFrame) error {
	return json.NewEncoder(output).Encode(frame)
}

func writeControlTerminal(output io.Writer, command string, err error) error {
	frame := controlFrame{Type: controlFrameTerminal, Command: command, OK: err == nil}
	if err != nil {
		frame.Error, frame.Code = err.Error(), controlErrorCode(err)
	}
	return writeControlFrame(output, frame)
}

func Control(ctx context.Context, socket string, request ControlRequest, output io.Writer) error {
	dialCtx, cancelDial := context.WithTimeout(ctx, controlIODeadline)
	conn, err := (&net.Dialer{}).DialContext(dialCtx, "unix", socket)
	cancelDial()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrControlUnavailable, err)
	}
	defer func() { _ = conn.Close() }()
	stopCancel := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopCancel()
	if err := conn.SetWriteDeadline(time.Now().Add(controlIODeadline)); err != nil {
		return err
	}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return err
	}
	_ = conn.SetWriteDeadline(time.Time{})
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetReadDeadline(deadline)
	}
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	accepted, terminal, payloads := false, false, 0
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		var header struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(line, &header) == nil && (header.Type == controlFrameAccepted || header.Type == controlFrameTerminal) {
			var frame controlFrame
			if err := decodeControlJSON(line, &frame); err != nil {
				return fmt.Errorf("invalid control frame: %w", err)
			}
			if frame.Command != request.Command || terminal {
				return errors.New("control response command or ordering mismatch")
			}
			switch frame.Type {
			case controlFrameAccepted:
				if !isControlMutation(request.Command) || accepted || payloads != 0 || frame.OK || frame.Error != "" || frame.Code != 0 {
					return errors.New("invalid control acceptance frame")
				}
				accepted = true
			case controlFrameTerminal:
				if isControlMutation(request.Command) && !accepted {
					return errors.New("mutation terminated before acceptance")
				}
				if !frame.OK {
					return controlFrameError(frame)
				}
				if frame.Error != "" || frame.Code != 0 || !validControlPayloadCount(request.Command, payloads) {
					return errors.New("invalid successful control terminal")
				}
				terminal = true
			}
			continue
		}
		if terminal {
			return errors.New("control response continued after terminal")
		}
		if err := validateControlPayload(request.Command, line); err != nil {
			return err
		}
		payloads++
		if output != nil {
			if _, err := output.Write(append(line, '\n')); err != nil {
				return err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		if accepted {
			return fmt.Errorf("%w: %v", ErrControlOutcomeUnknown, err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("%w: %v", ErrControlUnavailable, err)
	}
	if !terminal {
		if accepted {
			return ErrControlOutcomeUnknown
		}
		return fmt.Errorf("%w: terminal response missing", ErrControlUnavailable)
	}
	return nil
}

func decodeControlJSON(body []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("trailing JSON data")
	}
	return nil
}

func validateControlPayload(command string, body []byte) error {
	switch command {
	case "status":
		status := &StatusOutput{}
		if err := decodeControlJSON(body, status); err != nil {
			return fmt.Errorf("invalid status response: %w", err)
		}
		if phaseRank(status.Phase) == 0 || phaseRank(status.StartupPhase) == 0 {
			return errors.New("invalid status phase")
		}
		return nil
	case "diff":
		finding := &Finding{}
		if err := decodeControlJSON(body, finding); err != nil {
			return fmt.Errorf("invalid diff response: %w", err)
		}
		if finding.Kind == "" || finding.Severity != SeverityWarning && finding.Severity != SeverityBlocker {
			return errors.New("invalid diff finding")
		}
		return nil
	case "verify-full":
		verification := &VerificationState{}
		if err := decodeControlJSON(body, verification); err != nil {
			return fmt.Errorf("invalid verify-full response: %w", err)
		}
		if verification.Status != "passed" && verification.Status != "failed" {
			return errors.New("invalid verify-full status")
		}
		return nil
	case "prepare-drive9-cutover":
		checkpoint := &Checkpoint{}
		if err := decodeControlJSON(body, checkpoint); err != nil {
			return fmt.Errorf("invalid cutover response: %w", err)
		}
		if err := validateCheckpoint(*checkpoint); err != nil || !checkpoint.FenceComplete || checkpoint.HighestPhase != PhaseCutoverReady {
			return errors.New("invalid cutover checkpoint")
		}
		return nil
	default:
		return ErrIllegalAction
	}
}

func validControlPayloadCount(command string, count int) bool {
	if command == "diff" {
		return true
	}
	return count == 1
}

func controlFrameError(frame controlFrame) error {
	if frame.Error == "" || frame.Code == 0 {
		return errors.New("invalid failed control terminal")
	}
	switch frame.Code {
	case 2:
		return fmt.Errorf("%w: %s", ErrIllegalAction, frame.Error)
	default:
		return errors.New(frame.Error)
	}
}
