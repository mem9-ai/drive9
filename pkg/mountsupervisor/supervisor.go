// Package mountsupervisor provides in-binary supervision for drive9 FUSE mounts.
package mountsupervisor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	drive9fuse "github.com/mem9-ai/drive9/pkg/fuse"
	"github.com/mem9-ai/drive9/pkg/mountstate"
)

// Config configures a mount supervisor.
type Config struct {
	MountPoint     string
	Executable     string
	WorkerArgs     []string // args after executable (e.g. mount --foreground --supervised ...)
	Env            []string
	LogPath        string
	Stdout         io.Writer
	Stderr         io.Writer
	MaxRestarts    int           // default 5
	RestartWindow  time.Duration // default 10m
	StableReset    time.Duration // default 10m
	HealthInterval time.Duration // default 10s
	HealthTimeout  time.Duration // default 5s
	HealthFailures int           // default 3
	StopTimeout    time.Duration // default 60s
	BackoffMax     time.Duration // default 30s
	AlertWebhook   string
	AlertFile      string
	// SanitizedArgs stored for ensure (no secrets).
	SanitizedArgs []string
	Server        string
	RemoteRoot    string
	Profile       string
	LocalRoot     string
	PackPaths     []string
	// ReadyNotify is closed once the first worker is ready (optional).
	ReadyNotify chan<- struct{}
	// Now/Sleep allow tests to inject clocks.
	Now   func() time.Time
	Sleep func(time.Duration)
}

// Run blocks as the supervisor until intentional stop or permanent failure.
func Run(cfg Config) error {
	s, err := newSupervisor(cfg)
	if err != nil {
		return err
	}
	defer s.unlock()
	return s.loop()
}

type supervisor struct {
	cfg Config

	mu             sync.Mutex
	state          mountstate.SupervisorState
	stopRequested  bool
	workerCmd      *exec.Cmd
	workerWait     chan exitResult
	restarts       []time.Time
	permanentFails int
	backoff        time.Duration
	lockFile       *os.File

	healthFails int
	lastHealth  string
	lastHealthErr string
}

type exitResult struct {
	err  error
	code int
}

func newSupervisor(cfg Config) (*supervisor, error) {
	cfg = applyDefaults(cfg)
	if strings.TrimSpace(cfg.MountPoint) == "" {
		return nil, fmt.Errorf("mountsupervisor: mountpoint required")
	}
	if strings.TrimSpace(cfg.Executable) == "" {
		exe, err := os.Executable()
		if err != nil {
			return nil, fmt.Errorf("mountsupervisor: locate executable: %w", err)
		}
		cfg.Executable = exe
	}
	if len(cfg.WorkerArgs) == 0 {
		return nil, fmt.Errorf("mountsupervisor: worker args required")
	}
	lock, err := acquireLock(cfg.MountPoint)
	if err != nil {
		return nil, err
	}
	creation, _ := mountstate.ProcessCreationTime(os.Getpid())
	now := cfg.Now()
	s := &supervisor{
		cfg:      cfg,
		lockFile: lock,
		backoff:  time.Second,
		state: mountstate.SupervisorState{
			PID:          os.Getpid(),
			CreationTime: creation,
			MountPoint:   cfg.MountPoint,
			State:        mountstate.SupervisorStateStarting,
			StartedAt:    now,
			LogPath:      cfg.LogPath,
			Args:         append([]string(nil), cfg.SanitizedArgs...),
			Server:       cfg.Server,
			RemoteRoot:   cfg.RemoteRoot,
			Profile:      cfg.Profile,
		},
	}
	_ = s.persist()
	return s, nil
}

func applyDefaults(cfg Config) Config {
	if cfg.MaxRestarts <= 0 {
		cfg.MaxRestarts = 5
	}
	if cfg.RestartWindow <= 0 {
		cfg.RestartWindow = 10 * time.Minute
	}
	if cfg.StableReset <= 0 {
		cfg.StableReset = 10 * time.Minute
	}
	if cfg.HealthInterval <= 0 {
		cfg.HealthInterval = 10 * time.Second
	}
	if cfg.HealthTimeout <= 0 {
		cfg.HealthTimeout = 5 * time.Second
	}
	if cfg.HealthFailures <= 0 {
		cfg.HealthFailures = 3
	}
	if cfg.StopTimeout <= 0 {
		cfg.StopTimeout = 60 * time.Second
	}
	if cfg.BackoffMax <= 0 {
		cfg.BackoffMax = 30 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Sleep == nil {
		cfg.Sleep = time.Sleep
	}
	if cfg.Stdout == nil {
		cfg.Stdout = os.Stdout
	}
	if cfg.Stderr == nil {
		cfg.Stderr = os.Stderr
	}
	return cfg
}

func (s *supervisor) unlock() {
	if s.lockFile != nil {
		_ = s.lockFile.Close()
		_ = os.Remove(mountstate.SupervisorLockPath(s.cfg.MountPoint))
		s.lockFile = nil
	}
}

func (s *supervisor) loop() error {
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	// Best-effort clean before first spawn.
	if _, err := drive9fuse.EnsureCleanMountpoint(s.cfg.MountPoint); err != nil {
		s.logf("pre-start clean: %v", err)
	}

	readyOnce := sync.Once{}
	notifyReady := func() {
		if s.cfg.ReadyNotify == nil {
			return
		}
		readyOnce.Do(func() { close(s.cfg.ReadyNotify) })
	}

	for {
		if s.isStopRequested() {
			return s.shutdownClean()
		}
		if mountstate.StopTokenPresent(s.cfg.MountPoint) {
			s.requestStop("stop token present")
			return s.shutdownClean()
		}

		if s.circuitOpen() {
			// Enter circuit-open once: clean, persist, alert — then idle.
			s.mu.Lock()
			alreadyOpen := s.state.State == mountstate.SupervisorStateCircuitOpen
			s.mu.Unlock()
			if !alreadyOpen {
				s.setState(mountstate.SupervisorStateCircuitOpen)
				_, _ = drive9fuse.EnsureCleanMountpoint(s.cfg.MountPoint)
				s.alert("mount_circuit_open", "restart budget exhausted")
			}
			// Wait for stop signal or stop token (no per-tick webhook/probe spam).
			select {
			case <-sigCh:
				s.requestStop("signal while circuit open")
				return s.shutdownClean()
			case <-time.After(2 * time.Second):
				if mountstate.StopTokenPresent(s.cfg.MountPoint) {
					s.requestStop("stop token while circuit open")
					return s.shutdownClean()
				}
			}
			continue
		}

		if err := s.spawnWorker(); err != nil {
			s.permanentFails++
			s.logf("spawn worker failed: %v", err)
			if s.permanentFails >= 3 {
				s.setState(mountstate.SupervisorStateFailed)
				s.alert("mount_gave_up", err.Error())
				_, _ = drive9fuse.EnsureCleanMountpoint(s.cfg.MountPoint)
				return giveUpError(fmt.Sprintf("spawn failed permanently: %v", err))
			}
			s.cfg.Sleep(s.nextBackoff())
			continue
		}

		// Wait until worker ready or dies.
		if err := s.waitWorkerReady(sigCh); err != nil {
			if s.isStopRequested() {
				return s.shutdownClean()
			}
			code, reason := s.classifyExit(err)
			s.recordExit(code, reason)
			if s.isStopRequested() || mountstate.StopTokenPresent(s.cfg.MountPoint) {
				return s.shutdownClean()
			}
			if code == drive9fuse.ExitOK {
				return s.shutdownClean()
			}
			if code == drive9fuse.ExitStartupPermanent || code == drive9fuse.ExitUsage {
				s.permanentFails++
				if s.permanentFails >= 3 {
					s.setState(mountstate.SupervisorStateFailed)
					s.alert("mount_gave_up", reason)
					_, _ = drive9fuse.EnsureCleanMountpoint(s.cfg.MountPoint)
					return giveUpError(reason)
				}
				s.noteRestart(reason)
				s.alert("mount_restart", reason)
				_, _ = drive9fuse.EnsureCleanMountpoint(s.cfg.MountPoint)
				s.cfg.Sleep(s.nextBackoff())
				continue
			}
			// Transient / abnormal / unknown (including bare exit 1) → restart.
			s.noteRestart(reason)
			s.alert("mount_restart", reason)
			_, _ = drive9fuse.EnsureCleanMountpoint(s.cfg.MountPoint)
			s.cfg.Sleep(s.nextBackoff())
			continue
		}

		s.permanentFails = 0
		s.backoff = time.Second
		s.setState(mountstate.SupervisorStateRunning)
		notifyReady()
		s.logf("worker ready pid=%d", s.state.WorkerPID)

		// Running loop: wait for worker exit, stop signal, or health failure.
		workerExit, stop := s.superviseRunning(sigCh)
		if stop {
			return s.shutdownClean()
		}
		code, reason := s.classifyExit(workerExit)
		s.recordExit(code, reason)

		// Intent-first: if we are stopping, never restart.
		if s.isStopRequested() || mountstate.StopTokenPresent(s.cfg.MountPoint) {
			return s.shutdownClean()
		}

		// Exit 0 while still RUNNING: verify mount inactive; if still active, treat as abnormal.
		if code == 0 {
			if active, _ := drive9fuse.ActiveMountPoint(s.cfg.MountPoint); active {
				if probeErr := drive9fuse.ProbeMountPointReady(s.cfg.MountPoint); probeErr != nil {
					code = drive9fuse.ExitServeAbnormal
					reason = "worker exit 0 but mount broken: " + probeErr.Error()
				} else {
					// Live healthy mount with dead worker is unexpected — restart.
					code = drive9fuse.ExitServeAbnormal
					reason = "worker exit 0 but mount still active"
				}
			} else if !s.shouldRestart(code) {
				return s.shutdownClean()
			}
		}

		if code == drive9fuse.ExitOK && !s.shouldRestart(code) {
			return s.shutdownClean()
		}
		if code == drive9fuse.ExitStartupPermanent || code == drive9fuse.ExitUsage {
			s.permanentFails++
			if s.permanentFails >= 3 {
				s.setState(mountstate.SupervisorStateFailed)
				s.alert("mount_gave_up", reason)
				_, _ = drive9fuse.EnsureCleanMountpoint(s.cfg.MountPoint)
				return giveUpError(reason)
			}
		} else if !s.shouldRestart(code) {
			return s.shutdownClean()
		}

		s.noteRestart(reason)
		_, _ = drive9fuse.EnsureCleanMountpoint(s.cfg.MountPoint)
		s.alert("mount_restart", reason)
		s.cfg.Sleep(s.nextBackoff())
	}
}

func (s *supervisor) superviseRunning(sigCh <-chan os.Signal) (error, bool) {
	healthTicker := time.NewTicker(s.cfg.HealthInterval)
	defer healthTicker.Stop()
	started := s.cfg.Now()

	for {
		select {
		case sig := <-sigCh:
			s.logf("received signal %v", sig)
			s.requestStop("signal")
			s.stopWorker()
			return nil, true
		case res := <-s.workerWait:
			s.clearWorker()
			// Stable reset if long uptime.
			if s.cfg.Now().Sub(started) >= s.cfg.StableReset {
				s.mu.Lock()
				s.restarts = nil
				s.state.RestartCount = 0
				s.mu.Unlock()
			}
			return res.err, false
		case <-healthTicker.C:
			if s.isStopRequested() {
				s.stopWorker()
				return nil, true
			}
			if mountstate.StopTokenPresent(s.cfg.MountPoint) {
				s.requestStop("stop token")
				s.stopWorker()
				return nil, true
			}
			if !s.healthOK() {
				s.healthFails++
				s.setHealth("fail", s.lastHealthErr)
				if s.healthFails >= s.cfg.HealthFailures {
					s.logf("health failed %d times; restarting worker: %s", s.healthFails, s.lastHealthErr)
					s.alert("mount_unhealthy", s.lastHealthErr)
					// Kill worker without setting stopRequested so we restart.
					s.killWorkerForHealth()
					// Wait for exit.
					select {
					case res := <-s.workerWait:
						s.clearWorker()
						if res.err == nil {
							res.err = drive9fuse.ExitUnhealthyErr(s.lastHealthErr)
						}
						return res.err, false
					case <-time.After(s.cfg.StopTimeout + 5*time.Second):
						s.clearWorker()
						return drive9fuse.ExitUnhealthyErr("health kill timeout"), false
					}
				}
			} else {
				s.healthFails = 0
				s.setHealth("ok", "")
			}
		}
	}
}

func (s *supervisor) healthOK() bool {
	s.mu.Lock()
	pid := s.state.WorkerPID
	creation := s.state.WorkerCreation
	s.mu.Unlock()
	if pid <= 0 || !processAlive(pid, creation) {
		s.lastHealthErr = "worker process not alive"
		return false
	}
	// Bound probe with timeout via channel.
	done := make(chan error, 1)
	go func() {
		done <- drive9fuse.ProbeMountPointReady(s.cfg.MountPoint)
	}()
	select {
	case err := <-done:
		if err != nil {
			s.lastHealthErr = err.Error()
			return false
		}
		return true
	case <-time.After(s.cfg.HealthTimeout):
		s.lastHealthErr = fmt.Sprintf("health probe timed out after %s", s.cfg.HealthTimeout)
		return false
	}
}

func (s *supervisor) spawnWorker() error {
	s.setState(mountstate.SupervisorStateStarting)
	cmd := exec.Command(s.cfg.Executable, s.cfg.WorkerArgs...)
	cmd.Env = s.cfg.Env
	if cmd.Env == nil {
		cmd.Env = os.Environ()
	}
	// Tag supervisor pid for worker / umount.
	cmd.Env = append(cmd.Env, fmt.Sprintf("DRIVE9_MOUNT_SUPERVISOR_PID=%d", os.Getpid()))
	cmd.Stdout = s.cfg.Stdout
	cmd.Stderr = s.cfg.Stderr
	cmd.Stdin = nil
	// Worker must NOT setsid — remain child of supervisor.
	if err := cmd.Start(); err != nil {
		return err
	}
	waitCh := make(chan exitResult, 1)
	go func() {
		err := cmd.Wait()
		code := 0
		if err != nil {
			var ee *exec.ExitError
			if errors.As(err, &ee) {
				code = ee.ExitCode()
			} else {
				code = 1
			}
		}
		waitCh <- exitResult{err: err, code: code}
	}()
	creation, _ := mountstate.ProcessCreationTime(cmd.Process.Pid)
	s.mu.Lock()
	s.workerCmd = cmd
	s.workerWait = waitCh
	s.state.WorkerPID = cmd.Process.Pid
	s.state.WorkerCreation = creation
	s.mu.Unlock()
	_ = s.persist()
	_ = s.writeAuthoritativeProcessState()
	return nil
}

func (s *supervisor) waitWorkerReady(sigCh <-chan os.Signal) error {
	deadline := s.cfg.Now().Add(30 * time.Second)
	for s.cfg.Now().Before(deadline) {
		select {
		case <-sigCh:
			s.requestStop("signal during ready")
			s.stopWorker()
			return errors.New("stop during ready")
		case res := <-s.workerWait:
			s.clearWorker()
			if res.err == nil {
				return errors.New("worker exited before ready")
			}
			return res.err
		default:
		}
		if mountstate.StopTokenPresent(s.cfg.MountPoint) {
			s.requestStop("stop token during ready")
			s.stopWorker()
			return errors.New("stop during ready")
		}
		if err := drive9fuse.ProbeMountPointReady(s.cfg.MountPoint); err == nil {
			// Probe passed: publish supervisor-owned process state and treat as ready.
			_ = s.writeAuthoritativeProcessState()
			return nil
		}
		s.cfg.Sleep(200 * time.Millisecond)
	}
	// Timeout — kill worker.
	s.killWorkerForHealth()
	select {
	case res := <-s.workerWait:
		s.clearWorker()
		if res.err != nil {
			return res.err
		}
		return fmt.Errorf("worker ready timeout")
	case <-time.After(5 * time.Second):
		s.clearWorker()
		return fmt.Errorf("worker ready timeout")
	}
}

func (s *supervisor) writeAuthoritativeProcessState() error {
	s.mu.Lock()
	st := s.state
	s.mu.Unlock()
	// Prefer reading worker-written state for secrets/control socket.
	base, _, err := mountstate.ReadProcessState(s.cfg.MountPoint)
	if err != nil {
		base = mountstate.ProcessState{}
	}
	creation, _ := mountstate.ProcessCreationTime(os.Getpid())
	base.PID = os.Getpid()
	base.CreationTime = creation
	base.Role = mountstate.RoleSupervisor
	base.Supervise = true
	base.SupervisorPID = os.Getpid()
	base.SupervisorCreationTime = creation
	base.WorkerPID = st.WorkerPID
	base.MountPoint = s.cfg.MountPoint
	base.LogPath = s.cfg.LogPath
	base.StatusPath = mountstate.SupervisorStatePath(s.cfg.MountPoint)
	base.StopTokenPath = mountstate.StopTokenPath(s.cfg.MountPoint)
	base.Args = append([]string(nil), s.cfg.SanitizedArgs...)
	if base.Server == "" {
		base.Server = s.cfg.Server
	}
	if base.RemoteRoot == "" {
		base.RemoteRoot = s.cfg.RemoteRoot
	}
	if base.Profile == "" {
		base.Profile = s.cfg.Profile
	}
	if base.LocalRoot == "" {
		base.LocalRoot = s.cfg.LocalRoot
	}
	if len(base.PackPaths) == 0 && len(s.cfg.PackPaths) > 0 {
		base.PackPaths = append([]string(nil), s.cfg.PackPaths...)
	}
	// When the supervised worker skips the pidfile, recover credentials from env
	// so umount --pack and status tooling still work.
	if base.APIKey == "" && base.Token == "" {
		for _, kv := range s.cfg.Env {
			if after, ok := strings.CutPrefix(kv, "DRIVE9_API_KEY="); ok && after != "" {
				base.APIKey = after
				base.CredentialKind = mountstate.CredentialKindAPIKey
			}
			if after, ok := strings.CutPrefix(kv, "DRIVE9_VAULT_TOKEN="); ok && after != "" {
				base.Token = after
				base.CredentialKind = mountstate.CredentialKindToken
			}
			// Also accept token env used by the CLI vault path if present.
			if after, ok := strings.CutPrefix(kv, "DRIVE9_TOKEN="); ok && after != "" && base.Token == "" {
				base.Token = after
				base.CredentialKind = mountstate.CredentialKindToken
			}
		}
	}
	if base.Component == "" {
		base.Component = "drive9-fuse-supervisor"
	}
	if base.MountKind == "" {
		base.MountKind = mountstate.MountKindFUSE
	}
	if base.StartedAt == "" {
		base.StartedAt = s.cfg.Now().UTC().Format(time.RFC3339Nano)
	}
	// Control socket path is deterministic; always record it so drain/status work
	// when the supervised worker skips writing process state.
	if base.ControlSocket == "" {
		base.ControlSocket = mountstate.ControlSocketPath(s.cfg.MountPoint)
	}
	st.ControlSocket = base.ControlSocket
	s.mu.Lock()
	s.state.ControlSocket = base.ControlSocket
	s.mu.Unlock()
	_, err = mountstate.WriteProcessState(s.cfg.MountPoint, base)
	return err
}

func (s *supervisor) stopWorker() {
	s.mu.Lock()
	cmd := s.workerCmd
	waitCh := s.workerWait
	// Take ownership of wait channel so a second stopWorker/shutdownClean
	// cannot re-drain the same channel for StopTimeout.
	s.workerWait = nil
	s.workerCmd = nil
	s.state.WorkerPID = 0
	s.state.WorkerCreation = 0
	s.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	if waitCh == nil {
		return
	}
	select {
	case <-waitCh:
		return
	case <-time.After(s.cfg.StopTimeout):
		_ = cmd.Process.Kill()
		select {
		case <-waitCh:
		case <-time.After(5 * time.Second):
		}
	}
}

func (s *supervisor) killWorkerForHealth() {
	s.mu.Lock()
	cmd := s.workerCmd
	s.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	// Grace uses full --stop-timeout, then SIGKILL.
	s.cfg.Sleep(s.cfg.StopTimeout)
	s.mu.Lock()
	cmd = s.workerCmd
	s.mu.Unlock()
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}

func (s *supervisor) clearWorker() {
	s.mu.Lock()
	s.workerCmd = nil
	s.workerWait = nil
	s.state.WorkerPID = 0
	s.state.WorkerCreation = 0
	s.mu.Unlock()
}

func (s *supervisor) shutdownClean() error {
	s.setState(mountstate.SupervisorStateStopping)
	// stopWorker is idempotent: takes ownership of waitCh and reaps once.
	s.stopWorker()
	// Force clean mount on stop.
	if active, _ := drive9fuse.ActiveMountPoint(s.cfg.MountPoint); active {
		drive9fuse.ForceUnmount(s.cfg.MountPoint)
	}
	_ = mountstate.ClearStopToken(s.cfg.MountPoint)
	s.setState(mountstate.SupervisorStateStopped)
	_ = mountstate.ClearSupervisorState(s.cfg.MountPoint)
	// Remove pid file we own.
	_ = os.Remove(mountstate.PIDFilePath(s.cfg.MountPoint))
	s.alert("mount_stopped", "intentional stop")
	return nil
}

func (s *supervisor) requestStop(reason string) {
	s.mu.Lock()
	s.stopRequested = true
	s.state.StopRequested = true
	s.mu.Unlock()
	_ = mountstate.WriteStopToken(s.cfg.MountPoint, reason)
	_ = s.persist()
	s.logf("stop requested: %s", reason)
}

func (s *supervisor) isStopRequested() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopRequested
}

func (s *supervisor) shouldRestart(code int) bool {
	if s.isStopRequested() || mountstate.StopTokenPresent(s.cfg.MountPoint) {
		return false
	}
	// Intent-first: only clean intentional stop, usage, and permanent config
	// failures suppress restart. Bare exit 1 (legacy force-quit / unclassified)
	// is restartable when no stop token is present so transient startup
	// failures cannot silently kill supervision.
	switch code {
	case drive9fuse.ExitOK, drive9fuse.ExitUsage, drive9fuse.ExitStartupPermanent:
		return false
	default:
		return true
	}
}

func (s *supervisor) classifyExit(err error) (int, string) {
	if err == nil {
		return 0, "clean exit"
	}
	var me *drive9fuse.MountExitError
	if errors.As(err, &me) {
		return me.ExitCode(), me.Error()
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code := ee.ExitCode()
		// Signal deaths often appear as 128+sig or negative on some systems.
		if status, ok := ee.Sys().(syscall.WaitStatus); ok {
			if status.Signaled() {
				return 128 + int(status.Signal()), fmt.Sprintf("signal: %s", status.Signal())
			}
		}
		if code < 0 {
			return 1, err.Error()
		}
		return code, fmt.Sprintf("exit code %d", code)
	}
	return 1, err.Error()
}

func (s *supervisor) recordExit(code int, reason string) {
	s.mu.Lock()
	s.state.LastExitCode = code
	s.state.LastExitReason = reason
	s.mu.Unlock()
	_ = s.persist()
	s.logf("worker exit code=%d reason=%s", code, reason)
}

func (s *supervisor) noteRestart(reason string) {
	now := s.cfg.Now()
	s.mu.Lock()
	s.restarts = append(s.restarts, now)
	// Drop old entries outside window.
	cut := now.Add(-s.cfg.RestartWindow)
	n := 0
	for _, t := range s.restarts {
		if t.After(cut) {
			s.restarts[n] = t
			n++
		}
	}
	s.restarts = s.restarts[:n]
	s.state.RestartCount = len(s.restarts)
	s.state.LastRestartAt = now
	s.state.WindowStart = cut
	s.state.State = mountstate.SupervisorStateRestarting
	count := len(s.restarts)
	s.mu.Unlock()
	_ = s.persist()
	s.logf("scheduling restart (%d in window) reason=%s", count, reason)
}

func (s *supervisor) circuitOpen() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.cfg.Now()
	cut := now.Add(-s.cfg.RestartWindow)
	n := 0
	for _, t := range s.restarts {
		if t.After(cut) {
			n++
		}
	}
	return n >= s.cfg.MaxRestarts
}

func (s *supervisor) nextBackoff() time.Duration {
	s.mu.Lock()
	b := s.backoff
	if s.backoff < s.cfg.BackoffMax {
		s.backoff *= 2
		if s.backoff > s.cfg.BackoffMax {
			s.backoff = s.cfg.BackoffMax
		}
	}
	s.mu.Unlock()
	// jitter ±20%
	j := 1.0 + (rand.Float64()*0.4 - 0.2)
	d := time.Duration(float64(b) * j)
	if d < time.Second {
		d = time.Second
	}
	return d
}

func (s *supervisor) setState(state string) {
	s.mu.Lock()
	s.state.State = state
	s.mu.Unlock()
	_ = s.persist()
}

func (s *supervisor) setHealth(status, errMsg string) {
	s.mu.Lock()
	s.state.LastHealth = status
	s.state.LastHealthAt = s.cfg.Now().UTC()
	s.state.LastHealthError = errMsg
	s.lastHealth = status
	s.mu.Unlock()
	_ = s.persist()
}

func (s *supervisor) persist() error {
	s.mu.Lock()
	st := s.state
	s.mu.Unlock()
	return mountstate.WriteSupervisorState(s.cfg.MountPoint, st)
}

func (s *supervisor) logf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	_, _ = fmt.Fprintf(s.cfg.Stderr, "drive9: supervisor: %s\n", msg)
}

func (s *supervisor) alert(event, detail string) {
	payload := map[string]any{
		"event":      event,
		"mountpoint": s.cfg.MountPoint,
		"detail":     detail,
		"ts":         s.cfg.Now().UTC().Format(time.RFC3339Nano),
		"pid":        os.Getpid(),
	}
	data, _ := json.Marshal(payload)
	if path := strings.TrimSpace(s.cfg.AlertFile); path != "" {
		f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err == nil {
			_, _ = f.Write(append(data, '\n'))
			_ = f.Close()
		}
	}
	url := strings.TrimSpace(s.cfg.AlertWebhook)
	if url == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(data)))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		_ = resp.Body.Close()
	}
}

func processAlive(pid int, creation uint64) bool {
	if pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	if err := proc.Signal(syscall.Signal(0)); err != nil {
		if errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH) {
			return false
		}
		// EPERM means process exists.
		if errors.Is(err, syscall.EPERM) {
			// fall through to creation check
		} else {
			return false
		}
	}
	if creation == 0 {
		return true
	}
	got, err := mountstate.ProcessCreationTime(pid)
	if err != nil {
		return false
	}
	return got == creation
}

// ResetCircuit clears restart budget for ensure --reset style recovery.
func ResetCircuit(mountPoint string) error {
	st, _, err := mountstate.ReadSupervisorState(mountPoint)
	if err != nil {
		return err
	}
	st.RestartCount = 0
	st.State = mountstate.SupervisorStateStarting
	st.LastExitReason = ""
	return mountstate.WriteSupervisorState(mountPoint, st)
}

// StatusSnapshot merges supervisor + process + probe for mount status.
type StatusSnapshot struct {
	MountPoint     string `json:"mount_point"`
	State          string `json:"state"`
	Healthy        bool   `json:"healthy"`
	SupervisorPID  int    `json:"supervisor_pid,omitempty"`
	WorkerPID      int    `json:"worker_pid,omitempty"`
	Restarts       int    `json:"restarts"`
	LastExitCode   int    `json:"last_exit_code,omitempty"`
	LastExitReason string `json:"last_exit_reason,omitempty"`
	LastHealth     string `json:"last_health,omitempty"`
	LastHealthErr  string `json:"last_health_error,omitempty"`
	LogPath        string `json:"log_path,omitempty"`
	ProbeError     string `json:"probe_error,omitempty"`
	StopRequested  bool   `json:"stop_requested,omitempty"`
	Supervised     bool   `json:"supervised"`
}

func CollectStatus(mountPoint string) StatusSnapshot {
	snap := StatusSnapshot{MountPoint: mountPoint}
	if st, _, err := mountstate.ReadSupervisorState(mountPoint); err == nil {
		snap.Supervised = true
		snap.State = st.State
		snap.SupervisorPID = st.PID
		snap.WorkerPID = st.WorkerPID
		snap.Restarts = st.RestartCount
		snap.LastExitCode = st.LastExitCode
		snap.LastExitReason = st.LastExitReason
		snap.LastHealth = st.LastHealth
		snap.LastHealthErr = st.LastHealthError
		snap.LogPath = st.LogPath
		snap.StopRequested = st.StopRequested
	}
	if ps, _, err := mountstate.ReadProcessState(mountPoint); err == nil {
		if snap.SupervisorPID == 0 && ps.SupervisorPID > 0 {
			snap.SupervisorPID = ps.SupervisorPID
		}
		if snap.WorkerPID == 0 {
			if ps.WorkerPID > 0 {
				snap.WorkerPID = ps.WorkerPID
			} else if ps.Role != mountstate.RoleSupervisor {
				snap.WorkerPID = ps.PID
			}
		}
		if snap.LogPath == "" {
			snap.LogPath = ps.LogPath
		}
		if ps.Supervise {
			snap.Supervised = true
		}
	}
	// Surface last worker exit reason even after the process is gone.
	if snap.LastExitReason == "" {
		if er, _, err := mountstate.ReadExitReason(mountPoint); err == nil {
			snap.LastExitCode = er.Code
			snap.LastExitReason = er.Reason
			if er.Detail != "" {
				if snap.LastExitReason != "" {
					snap.LastExitReason = snap.LastExitReason + ": " + er.Detail
				} else {
					snap.LastExitReason = er.Detail
				}
			}
		}
	}
	if err := drive9fuse.ProbeMountPointReady(mountPoint); err != nil {
		snap.ProbeError = err.Error()
		snap.Healthy = false
		if snap.State == "" {
			snap.State = "unhealthy"
		}
	} else {
		snap.Healthy = true
		if snap.State == "" {
			snap.State = "running"
		}
	}
	return snap
}

// EnsureClean is a thin wrapper for CLI.
func EnsureClean(mountPoint string) (bool, error) {
	return drive9fuse.EnsureCleanMountpoint(mountPoint)
}

// WorkerArgsFromSanitized rebuilds worker argv for ensure when supervisor is dead.
func WorkerArgsFromSanitized(sanitized []string) []string {
	out := append([]string(nil), sanitized...)
	// Ensure --foreground and --supervised present.
	hasFG, hasSup := false, false
	for _, a := range out {
		if a == "--foreground" {
			hasFG = true
		}
		if a == "--supervised" {
			hasSup = true
		}
	}
	if !hasFG {
		out = append([]string{"--foreground"}, out...)
	}
	if !hasSup {
		out = append(out, "--supervised")
	}
	return out
}


