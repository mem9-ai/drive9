package mountstate

import (
	"crypto/sha256"
	"encoding/hex"
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
	Role             string    `json:"role,omitempty"`
	PID              int       `json:"pid"`
	CreationTime     uint64    `json:"creation_time,omitempty"`
	MountPoint       string    `json:"mount_point"`
	State            string    `json:"state"`
	WorkerPID        int       `json:"worker_pid,omitempty"`
	WorkerCreation   uint64    `json:"worker_creation_time,omitempty"`
	RestartCount     int       `json:"restart_count"`
	WindowStart      time.Time `json:"window_start,omitempty"`
	LastExitCode     int       `json:"last_exit_code,omitempty"`
	LastExitReason   string    `json:"last_exit_reason,omitempty"`
	LastHealth       string    `json:"last_health,omitempty"`
	LastHealthAt     time.Time `json:"last_health_at,omitempty"`
	LastHealthError  string    `json:"last_health_error,omitempty"`
	LastRestartAt    time.Time `json:"last_restart_at,omitempty"`
	StartedAt        time.Time `json:"started_at,omitempty"`
	StopRequested    bool      `json:"stop_requested,omitempty"`
	LogPath          string    `json:"log_path,omitempty"`
	ControlSocket    string    `json:"control_socket,omitempty"`
	Args             []string  `json:"args,omitempty"`
	Server           string    `json:"server,omitempty"`
	RemoteRoot       string    `json:"remote_root,omitempty"`
	Profile          string    `json:"profile,omitempty"`
	LocalRoot        string    `json:"local_root,omitempty"`
	PackPaths        []string  `json:"pack_paths,omitempty"`
}

func SupervisorStatePath(mountPoint string) string {
	return filepath.Join(os.TempDir(), "drive9-mount-"+hash8(canonicalMountPoint(mountPoint))+".supervise.json")
}

func StopTokenPath(mountPoint string) string {
	return filepath.Join(os.TempDir(), "drive9-mount-"+hash8(canonicalMountPoint(mountPoint))+".stop")
}

func SupervisorLockPath(mountPoint string) string {
	return filepath.Join(os.TempDir(), "drive9-mount-"+hash8(canonicalMountPoint(mountPoint))+".supervise.lock")
}

func WriteSupervisorState(mountPoint string, state SupervisorState) error {
	state.Role = RoleSupervisor
	if state.MountPoint == "" {
		state.MountPoint = canonicalMountPoint(mountPoint)
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
	if err != nil {
		return SupervisorState{}, path, err
	}
	var state SupervisorState
	if err := json.Unmarshal(data, &state); err != nil {
		return SupervisorState{}, path, fmt.Errorf("read supervisor state %s: %w", path, err)
	}
	return state, path, nil
}

func ClearSupervisorState(mountPoint string) error {
	path := SupervisorStatePath(mountPoint)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
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
	return writeFileAtomic(StopTokenPath(mountPoint), append(data, '\n'), 0o600)
}

func StopTokenPresent(mountPoint string) bool {
	_, err := os.Stat(StopTokenPath(mountPoint))
	return err == nil
}

func ClearStopToken(mountPoint string) error {
	path := StopTokenPath(mountPoint)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func hash8(canonical string) string {
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:8])
}
