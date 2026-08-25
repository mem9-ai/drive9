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
	"syscall"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

const (
	DefaultControlSocket = "/run/drive9-migration/control.sock"
	controlLimit         = 64 << 10
)

var controlIODeadline = 5 * time.Second

type ControlRequest struct {
	Command string `json:"command"`
	JobID   string `json:"job_id,omitempty"`
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
	JobID            string              `json:"job_id,omitempty"`
	VolumeID         string              `json:"volume_id,omitempty"`
	NodeName         string              `json:"node_name,omitempty"`
	EBSRoot          string              `json:"ebs_root,omitempty"`
	Subpath          string              `json:"subpath,omitempty"`
	SpaceRef         string              `json:"space_ref,omitempty"`
	Prefix           string              `json:"prefix,omitempty"`
	CredentialRef    string              `json:"credential_ref,omitempty"`
	RuntimeState     RuntimeState        `json:"runtime_state,omitempty"`
	RuntimeAttempts  int                 `json:"runtime_attempts,omitempty"`
	RuntimeError     string              `json:"runtime_error,omitempty"`
	Phase            Phase               `json:"phase,omitempty"`
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

type EBSStatusOutput struct {
	VolumeID string         `json:"volume_id"`
	NodeName string         `json:"node_name"`
	EBSRoot  string         `json:"ebs_root"`
	Jobs     []StatusOutput `json:"jobs"`
}

type controlTarget interface {
	prepareControl(context.Context, ControlRequest) (func(), error)
	handlePreparedControl(context.Context, io.Writer, ControlRequest) error
}

type controlServer struct {
	ctx      context.Context
	listener net.Listener
	path     string
	target   controlTarget
	once     sync.Once
	wait     sync.WaitGroup
}

func startControl(ctx context.Context, path string, target controlTarget) (*controlServer, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	listener, err := listenControlSocket(path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = listener.Close()
		return nil, err
	}
	server := &controlServer{ctx: ctx, listener: listener, path: path, target: target}
	server.wait.Add(1)
	go server.serve()
	go func() { <-ctx.Done(); server.close() }()
	return server, nil
}

func listenControlSocket(path string) (net.Listener, error) {
	listener, listenErr := net.Listen("unix", path)
	if listenErr == nil || !errors.Is(listenErr, syscall.EADDRINUSE) {
		return listener, listenErr
	}

	staleInfo, err := os.Lstat(path)
	if err != nil || staleInfo.Mode()&os.ModeSocket == 0 {
		return nil, listenErr
	}
	conn, dialErr := net.DialTimeout("unix", path, 250*time.Millisecond)
	if dialErr == nil {
		_ = conn.Close()
		return nil, listenErr
	}
	if !errors.Is(dialErr, syscall.ECONNREFUSED) {
		return nil, listenErr
	}
	currentInfo, err := os.Lstat(path)
	if err != nil || !os.SameFile(staleInfo, currentInfo) || currentInfo.Mode()&os.ModeSocket == 0 {
		return nil, listenErr
	}
	if err := os.Remove(path); err != nil {
		return nil, fmt.Errorf("remove stale control socket %q: %w", path, err)
	}
	return net.Listen("unix", path)
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
		release, err := s.target.prepareControl(waitCtx, request)
		if err != nil {
			cancelWait()
			_ = writeControlTerminal(output, request.Command, err)
			return
		}
		cancelWait()
		defer release()
		if err := writeControlFrame(output, controlFrame{Type: controlFrameAccepted, Command: request.Command}); err != nil {
			return
		}
	}
	err = s.target.handlePreparedControl(s.ctx, output, request)
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

func (w *Worker) prepareControl(ctx context.Context, request ControlRequest) (func(), error) {
	if !isControlMutation(request.Command) {
		return func() {}, nil
	}
	if err := w.controlGate.Acquire(ctx); err != nil {
		return nil, err
	}
	return w.controlGate.Release, nil
}

func (w *Worker) handlePreparedControl(ctx context.Context, output io.Writer, request ControlRequest) error {
	return w.handleControlLocked(ctx, output, request)
}

func (m *Manager) prepareControl(ctx context.Context, request ControlRequest) (func(), error) {
	if !isControlMutation(request.Command) {
		return func() {}, nil
	}
	if request.JobID == "" {
		return nil, ErrIllegalAction
	}
	snapshot, ok := m.snapshot(request.JobID)
	if !ok || snapshot.Worker == nil || snapshot.State != RuntimeRunning {
		return nil, ErrIllegalAction
	}
	if err := snapshot.Worker.controlGate.Acquire(ctx); err != nil {
		return nil, err
	}
	current, ok := m.snapshot(request.JobID)
	if !ok || current.Worker != snapshot.Worker || current.State != RuntimeRunning {
		snapshot.Worker.controlGate.Release()
		return nil, ErrIllegalAction
	}
	return snapshot.Worker.controlGate.Release, nil
}

func (m *Manager) handlePreparedControl(ctx context.Context, output io.Writer, request ControlRequest) error {
	if request.Command == "status" {
		if request.Output != "json" {
			return ErrIllegalAction
		}
		status, err := m.statusOutput(request.JobID)
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(status)
	}
	if request.JobID == "" {
		return ErrIllegalAction
	}
	snapshot, ok := m.snapshot(request.JobID)
	if !ok || snapshot.Worker == nil || snapshot.State != RuntimeRunning {
		return ErrIllegalAction
	}
	return snapshot.Worker.handleControlLocked(ctx, output, request)
}

func (m *Manager) statusOutput(jobID string) (EBSStatusOutput, error) {
	snapshots, err := m.snapshots(jobID)
	if err != nil {
		return EBSStatusOutput{}, err
	}
	status := EBSStatusOutput{
		VolumeID: m.startup.Source.VolumeID, NodeName: m.startup.Source.NodeName,
		EBSRoot: m.startup.Source.Root, Jobs: make([]StatusOutput, 0, len(snapshots)),
	}
	for _, snapshot := range snapshots {
		var job StatusOutput
		if snapshot.Worker != nil {
			job = snapshot.Worker.statusOutput()
		} else {
			job = startupStatusOutput(snapshot.Startup)
		}
		job.RuntimeState = snapshot.State
		job.RuntimeAttempts = snapshot.Attempts
		job.RuntimeError = snapshot.Error
		if snapshot.Error != "" {
			job.AttentionReason = snapshot.Error
		}
		status.Jobs = append(status.Jobs, job)
	}
	return status, nil
}

func startupStatusOutput(startup *Startup) StatusOutput {
	status := StatusOutput{Findings: make(map[FindingKind]int)}
	if startup == nil {
		return status
	}
	status.JobID = startup.Job.JobID
	status.VolumeID = startup.Job.VolumeID
	status.NodeName = startup.Job.NodeName
	status.EBSRoot = startup.Job.EBSRoot
	status.Subpath = startup.Job.Subpath
	status.SpaceRef = startup.Job.Target.SpaceRef
	status.Prefix = startup.Job.Target.Prefix
	status.CredentialRef = startup.Space.CredentialRef
	status.StartupPhase = startup.Phase
	return status
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
		status.JobID = w.startup.Job.JobID
		status.StartupPhase = w.startup.Phase
		status.VolumeID = w.startup.Job.VolumeID
		status.NodeName = w.startup.Job.NodeName
		status.EBSRoot = w.startup.Job.EBSRoot
		status.Subpath = w.startup.Job.Subpath
		status.SpaceRef = w.startup.Job.Target.SpaceRef
		status.Prefix = w.startup.Job.Target.Prefix
		status.CredentialRef = w.startup.Space.CredentialRef
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
		frame.Error, frame.Code = controlErrorMessage(err), controlErrorCode(err)
	}
	return writeControlFrame(output, frame)
}

func controlErrorMessage(err error) string {
	switch {
	case errors.Is(err, ErrIllegalAction):
		return ErrIllegalAction.Error()
	case errors.Is(err, ErrInvalidPhase):
		return ErrInvalidPhase.Error()
	case errors.Is(err, context.Canceled):
		return context.Canceled.Error()
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded.Error()
	case errors.Is(err, ErrVerificationFailed):
		return ErrVerificationFailed.Error()
	case errors.Is(err, ErrControlOutcomeUnknown):
		return ErrControlOutcomeUnknown.Error()
	}
	var status *client.StatusError
	if errors.As(err, &status) {
		return fmt.Sprintf("drive9 request failed: HTTP %d", status.StatusCode)
	}
	return "migration control operation failed"
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
				if !frame.OK {
					return controlFrameError(frame)
				}
				if isControlMutation(request.Command) && !accepted {
					return errors.New("mutation succeeded before acceptance")
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
		var fields map[string]json.RawMessage
		if err := decodeControlJSON(body, &fields); err != nil {
			return fmt.Errorf("invalid status response: %w", err)
		}
		if _, ok := fields["jobs"]; ok {
			status := &EBSStatusOutput{}
			if err := decodeControlJSON(body, status); err != nil {
				return fmt.Errorf("invalid EBS status response: %w", err)
			}
			if status.VolumeID == "" || status.NodeName == "" || status.EBSRoot == "" || len(status.Jobs) == 0 {
				return errors.New("invalid EBS status identity")
			}
			for _, job := range status.Jobs {
				if err := validateStatusOutput(job); err != nil {
					return err
				}
			}
			return nil
		}
		status := &StatusOutput{}
		if err := decodeControlJSON(body, status); err != nil {
			return fmt.Errorf("invalid status response: %w", err)
		}
		return validateStatusOutput(*status)
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

func validateStatusOutput(status StatusOutput) error {
	if status.RuntimeState != "" && status.RuntimeState != RuntimeInitializing && status.RuntimeState != RuntimeRetrying && status.RuntimeState != RuntimeRunning && status.RuntimeState != RuntimeStopped {
		return errors.New("invalid runtime state")
	}
	if status.RuntimeState == RuntimeInitializing || status.RuntimeState == RuntimeRetrying || status.RuntimeState == RuntimeStopped {
		if status.JobID == "" || phaseRank(status.StartupPhase) == 0 {
			return errors.New("invalid startup-only status")
		}
		return nil
	}
	if phaseRank(status.Phase) == 0 || phaseRank(status.StartupPhase) == 0 {
		return errors.New("invalid status phase")
	}
	return nil
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
