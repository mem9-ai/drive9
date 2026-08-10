package mountstate

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Supervisor process / role labels.
const (
	RoleWorker     = "worker"
	RoleSupervisor = "supervisor"

	SupervisorStateStarting    = "starting"
	SupervisorStateRunning     = "running"
	SupervisorStateRestarting  = "restarting"
	SupervisorStateStopping    = "stopping"
	SupervisorStateCircuitOpen = "circuit_open"
	SupervisorStateStopped     = "stopped"
	SupervisorStateFailed      = "failed"
)

// SupervisorState is the supervisor sidecar status JSON.
type SupervisorState struct {
	Role            string    `json:"role,omitempty"`
	PID             int       `json:"pid"`
	CreationTime    uint64    `json:"creation_time,omitempty"`
	MountPoint      string    `json:"mount_point"`
	State           string    `json:"state"`
	WorkerPID       int       `json:"worker_pid,omitempty"`
	WorkerCreation  uint64    `json:"worker_creation_time,omitempty"`
	RestartCount    int       `json:"restart_count"`
	WindowStart     time.Time `json:"window_start,omitempty"`
	LastExitCode    int       `json:"last_exit_code,omitempty"`
	LastExitReason  string    `json:"last_exit_reason,omitempty"`
	LastHealth      string    `json:"last_health,omitempty"`
	LastHealthAt    time.Time `json:"last_health_at,omitempty"`
	LastHealthError string    `json:"last_health_error,omitempty"`
	LastRestartAt   time.Time `json:"last_restart_at,omitempty"`
	StartedAt       time.Time `json:"started_at,omitempty"`
	StopRequested   bool      `json:"stop_requested,omitempty"`
	LogPath         string    `json:"log_path,omitempty"`
	ControlSocket   string    `json:"control_socket,omitempty"`
	Args            []string  `json:"args,omitempty"`
	Server          string    `json:"server,omitempty"`
	RemoteRoot      string    `json:"remote_root,omitempty"`
	Profile         string    `json:"profile,omitempty"`
	LocalRoot       string    `json:"local_root,omitempty"`
	PackPaths       []string  `json:"pack_paths,omitempty"`
}

func SupervisorStatePath(mountPoint string) string {
	return filepath.Join(stateDir(), "drive9-mount-"+hash8(canonicalMountPoint(mountPoint))+".supervise.json")
}

func StopTokenPath(mountPoint string) string {
	return filepath.Join(stateDir(), "drive9-mount-"+hash8(canonicalMountPoint(mountPoint))+".stop")
}

func SupervisorLockPath(mountPoint string) string {
	return filepath.Join(stateDir(), "drive9-mount-"+hash8(canonicalMountPoint(mountPoint))+".supervise.lock")
}

// legacyTempStatePath is the pre-UID-scoped location under bare os.TempDir().
func legacyTempStatePath(suffix, mountPoint string) string {
	return filepath.Join(os.TempDir(), "drive9-mount-"+hash8(canonicalMountPoint(mountPoint))+suffix)
}

func WriteSupervisorState(mountPoint string, state SupervisorState) error {
	state.Role = RoleSupervisor
	if state.MountPoint == "" {
		state.MountPoint = canonicalMountPoint(mountPoint)
	}
	if err := ensureStateDir(); err != nil {
		return err
	}
	path := SupervisorStatePath(mountPoint)
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal supervisor state: %w", err)
	}
	return writeFileAtomic(path, append(data, '\n'), 0o600)
}

func ReadSupervisorState(mountPoint string) (SupervisorState, string, error) {
	path := SupervisorStatePath(mountPoint)
	data, err := os.ReadFile(path)
	if err != nil && os.IsNotExist(err) {
		// Upgrade-compat: mounts started before UID-scoped state dirs.
		legacy := legacyTempStatePath(".supervise.json", mountPoint)
		if b, lerr := os.ReadFile(legacy); lerr == nil {
			data, path, err = b, legacy, nil
		}
	}
	if err != nil {
		return SupervisorState{}, path, err
	}
	var state SupervisorState
	if err := json.Unmarshal(data, &state); err != nil {
		return SupervisorState{}, path, fmt.Errorf("read supervisor state %s: %w", path, err)
	}
	return state, path, nil
}

// WriteStopToken marks intentional stop for a supervised mount.
func WriteStopToken(mountPoint string, reason string) error {
	if reason == "" {
		reason = "umount"
	}
	payload := map[string]any{
		"reason": reason,
		"ts":     time.Now().UTC().Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if err := ensureStateDir(); err != nil {
		return err
	}
	return writeFileAtomic(StopTokenPath(mountPoint), append(data, '\n'), 0o600)
}

// ReadStopTokenTime returns the token timestamp when present.
func ReadStopTokenTime(mountPoint string) (time.Time, bool) {
	for _, path := range []string{StopTokenPath(mountPoint), legacyTempStatePath(".stop", mountPoint)} {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var payload struct {
			TS string `json:"ts"`
		}
		if json.Unmarshal(data, &payload) != nil || payload.TS == "" {
			return time.Time{}, true
		}
		ts, err := time.Parse(time.RFC3339Nano, payload.TS)
		if err != nil {
			ts, err = time.Parse(time.RFC3339, payload.TS)
		}
		if err != nil {
			return time.Time{}, true
		}
		return ts, true
	}
	return time.Time{}, false
}

func StopTokenPresent(mountPoint string) bool {
	if _, err := os.Stat(StopTokenPath(mountPoint)); err == nil {
		return true
	}
	_, err := os.Stat(legacyTempStatePath(".stop", mountPoint))
	return err == nil
}

func ClearStopToken(mountPoint string) error {
	var first error
	for _, path := range []string{StopTokenPath(mountPoint), legacyTempStatePath(".stop", mountPoint)} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) && first == nil {
			first = err
		}
	}
	return first
}

func ClearSupervisorState(mountPoint string) error {
	var first error
	for _, path := range []string{SupervisorStatePath(mountPoint), legacyTempStatePath(".supervise.json", mountPoint)} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) && first == nil {
			first = err
		}
	}
	return first
}
