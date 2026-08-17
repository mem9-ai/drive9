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
	return supervisorStatePathForCanonical(canonicalMountPoint(mountPoint))
}

func supervisorStatePathForCanonical(canonical string) string {
	return filepath.Join(stateDir(), "drive9-mount-"+hash8(canonical)+".supervise.json")
}

func StopTokenPath(mountPoint string) string {
	return stopTokenPathForCanonical(canonicalMountPoint(mountPoint))
}

func stopTokenPathForCanonical(canonical string) string {
	return filepath.Join(stateDir(), "drive9-mount-"+hash8(canonical)+".stop")
}

func SupervisorLockPath(mountPoint string) string {
	return filepath.Join(stateDir(), "drive9-mount-"+hash8(canonicalMountPoint(mountPoint))+".supervise.lock")
}

// legacyTempStatePath is the pre-UID-scoped location under bare os.TempDir().
func legacyTempStatePath(suffix, mountPoint string) string {
	return legacyTempStatePathForCanonical(suffix, canonicalMountPoint(mountPoint))
}

func legacyTempStatePathForCanonical(suffix, canonical string) string {
	return filepath.Join(os.TempDir(), "drive9-mount-"+hash8(canonical)+suffix)
}

func supervisorStateReadPaths(mountPoint string) (primary string, alts []string) {
	primary = SupervisorStatePath(mountPoint)
	alts = append(alts, legacyTempStatePath(".supervise.json", mountPoint))
	if trustedFilePresent(primary) {
		return primary, nil
	}
	for _, canon := range resolvedCanonicalIfMissing(mountPoint) {
		alts = append(alts,
			supervisorStatePathForCanonical(canon),
			legacyTempStatePathForCanonical(".supervise.json", canon),
		)
	}
	return primary, alts
}

func stopTokenReadPaths(mountPoint string) []string {
	primary := StopTokenPath(mountPoint)
	legacy := legacyTempStatePath(".stop", mountPoint)
	out := []string{primary, legacy}
	if trustedFilePresent(primary) || trustedFilePresent(legacy) {
		return out
	}
	seen := map[string]struct{}{primary: {}, legacy: {}}
	add := func(p string) {
		if _, ok := seen[p]; ok {
			return
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	for _, canon := range resolvedCanonicalIfMissing(mountPoint) {
		add(stopTokenPathForCanonical(canon))
		add(legacyTempStatePathForCanonical(".stop", canon))
	}
	return out
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
	primary, alts := supervisorStateReadPaths(mountPoint)
	data, path, err := readTrustedFileCandidates(primary, alts)
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
	_, err := WriteStopTokenReceipt(mountPoint, reason)
	return err
}

// WriteStopTokenReceipt writes a stop token and returns the exact timestamp
// stored in the file. Callers that must not clear a successor token should
// later pass this receipt to ClearStopTokenIf.
func WriteStopTokenReceipt(mountPoint string, reason string) (time.Time, error) {
	if reason == "" {
		reason = "umount"
	}
	ts := time.Now().UTC()
	payload := map[string]any{
		"reason": reason,
		"ts":     ts.Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return time.Time{}, err
	}
	if err := ensureStateDir(); err != nil {
		return time.Time{}, err
	}
	if err := writeFileAtomic(StopTokenPath(mountPoint), append(data, '\n'), 0o600); err != nil {
		return time.Time{}, err
	}
	return ts, nil
}

func parseStopTokenTS(data []byte) (time.Time, bool) {
	var payload struct {
		TS string `json:"ts"`
	}
	if json.Unmarshal(data, &payload) != nil || payload.TS == "" {
		return time.Time{}, true // present but unparseable
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

// ReadStopTokenTime returns the token timestamp when a trusted token is present.
// Untrusted legacy/bare /tmp tokens (wrong owner, world-writable) are ignored.
func ReadStopTokenTime(mountPoint string) (time.Time, bool) {
	for _, path := range stopTokenReadPaths(mountPoint) {
		data, err := readTrustedFile(path)
		if err != nil {
			continue
		}
		return parseStopTokenTS(data)
	}
	return time.Time{}, false
}

func StopTokenPresent(mountPoint string) bool {
	for _, path := range stopTokenReadPaths(mountPoint) {
		if trustedFilePresent(path) {
			return true
		}
	}
	return false
}

func ClearStopToken(mountPoint string) error {
	return clearStopTokenMatching(mountPoint, time.Time{}, false)
}

// ClearStopTokenIf removes a trusted stop token only when its timestamp equals
// receipt (CAS). A successor token with a different ts is left intact.
func ClearStopTokenIf(mountPoint string, receipt time.Time) error {
	if receipt.IsZero() {
		return nil
	}
	return clearStopTokenMatching(mountPoint, receipt, true)
}

func clearStopTokenMatching(mountPoint string, receipt time.Time, matchReceipt bool) error {
	var first error
	for _, path := range stopTokenReadPaths(mountPoint) {
		if !trustedFilePresent(path) {
			if info, err := os.Lstat(path); err == nil {
				if uid, ok := stateDirOwnerUID(info); ok && uid == os.Getuid() && !matchReceipt {
					if err := os.Remove(path); err != nil && !os.IsNotExist(err) && first == nil {
						first = err
					}
				}
			}
			continue
		}
		if matchReceipt {
			data, err := readTrustedFile(path)
			if err != nil {
				continue
			}
			ts, ok := parseStopTokenTS(data)
			if !ok || ts.IsZero() || !ts.Equal(receipt) {
				continue
			}
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) && first == nil {
			first = err
		}
	}
	return first
}

func ClearSupervisorState(mountPoint string) error {
	var first error
	paths := []string{SupervisorStatePath(mountPoint), legacyTempStatePath(".supervise.json", mountPoint)}
	for _, canon := range resolvedCanonicalIfMissing(mountPoint) {
		paths = append(paths,
			supervisorStatePathForCanonical(canon),
			legacyTempStatePathForCanonical(".supervise.json", canon),
		)
	}
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) && first == nil {
			first = err
		}
	}
	return first
}
