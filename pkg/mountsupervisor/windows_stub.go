//go:build windows

package mountsupervisor

import (
	"fmt"
	"io"
	"time"
)

// Config is accepted on Windows so shared CLI files compile. Run is unsupported.
type Config struct {
	MountPoint          string
	Executable          string
	WorkerArgs          []string
	Env                 []string
	LogPath             string
	Stdout              io.Writer
	Stderr              io.Writer
	MaxRestarts         int
	RestartWindow       time.Duration
	StableReset         time.Duration
	HealthInterval      time.Duration
	HealthTimeout       time.Duration
	HealthFailures      int
	StopTimeout         time.Duration
	BackoffMax          time.Duration
	AlertWebhook        string
	AlertFile           string
	SanitizedArgs       []string
	Server              string
	RemoteRoot          string
	Profile             string
	MountKind           string
	LocalRoot           string
	PackPaths           []string
	Adopt               bool
	AdoptWorkerPID      int
	AdoptWorkerCreation uint64
	ReadyNotify         chan<- struct{}
	Now                 func() time.Time
	Sleep               func(time.Duration)
}

func Run(Config) error {
	return fmt.Errorf("mount supervision is not supported on windows")
}

type StatusSnapshot struct {
	MountPoint         string `json:"mount_point"`
	State              string `json:"state"`
	Healthy            bool   `json:"healthy"`
	SupervisorPID      int    `json:"supervisor_pid,omitempty"`
	SupervisorCreation uint64 `json:"supervisor_creation_time,omitempty"`
	WorkerPID          int    `json:"worker_pid,omitempty"`
	WorkerCreation     uint64 `json:"worker_creation_time,omitempty"`
	Restarts           int    `json:"restarts"`
	LastExitCode       int    `json:"last_exit_code,omitempty"`
	LastExitReason     string `json:"last_exit_reason,omitempty"`
	LastHealth         string `json:"last_health,omitempty"`
	LastHealthErr      string `json:"last_health_error,omitempty"`
	LogPath            string `json:"log_path,omitempty"`
	ProbeError         string `json:"probe_error,omitempty"`
	StopRequested      bool   `json:"stop_requested,omitempty"`
	Supervised         bool   `json:"supervised"`
}

func CollectStatus(mountPoint string) StatusSnapshot {
	return StatusSnapshot{MountPoint: mountPoint, State: "unsupported", ProbeError: "fuse supervision is not supported on windows"}
}

func SupervisorIdentityAlive(StatusSnapshot) bool { return false }
func WorkerIdentityAlive(StatusSnapshot) bool     { return false }

type EnsureAction string

const (
	EnsureAlreadyHealthy EnsureAction = "already_healthy"
	EnsureAdopt          EnsureAction = "adopt"
	EnsureRemount        EnsureAction = "remount"
)

func DecideEnsureAction(StatusSnapshot, bool) EnsureAction { return EnsureRemount }

func ResetCircuit(string) error { return fmt.Errorf("mount supervision is not supported on windows") }

func EnsureClean(string) (bool, error) { return false, nil }

func WorkerArgsFromSanitized(sanitized []string) []string {
	return append([]string(nil), sanitized...)
}
