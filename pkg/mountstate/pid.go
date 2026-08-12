package mountstate

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type ProcessState struct {
	PID                    int      `json:"pid"`
	CreationTime           uint64   `json:"creation_time,omitempty"`
	Component              string   `json:"component,omitempty"`
	MountKind              string   `json:"mount_kind,omitempty"`
	MountPoint             string   `json:"mount_point,omitempty"`
	RemoteRoot             string   `json:"remote_root,omitempty"`
	Profile                string   `json:"profile,omitempty"`
	LocalRoot              string   `json:"local_root,omitempty"`
	Server                 string   `json:"server,omitempty"`
	PackPaths              []string `json:"pack_paths,omitempty"`
	CredentialKind         string   `json:"credential_kind,omitempty"`
	APIKey                 string   `json:"api_key,omitempty"`
	Token                  string   `json:"token,omitempty"`
	ProfileDir             string   `json:"profile_dir,omitempty"`
	PerfSamplesPath        string   `json:"perf_samples_path,omitempty"`
	PerfInterval           string   `json:"perf_interval,omitempty"`
	PerfMaxSamples         int      `json:"perf_max_samples,omitempty"`
	PerfMaxSampleFiles     int      `json:"perf_max_sample_files,omitempty"`
	PerfMaxProfileFiles    int      `json:"perf_max_profile_files,omitempty"`
	PprofAddr              string   `json:"pprof_addr,omitempty"`
	StartedAt              string   `json:"started_at,omitempty"`
	HeapProfilePath        string   `json:"heap_profile_path,omitempty"`
	ControlSocket          string   `json:"control_socket,omitempty"`
	Role                   string   `json:"role,omitempty"` // worker | supervisor
	SupervisorPID          int      `json:"supervisor_pid,omitempty"`
	SupervisorCreationTime uint64   `json:"supervisor_creation_time,omitempty"`
	WorkerPID              int      `json:"worker_pid,omitempty"`
	LogPath                string   `json:"log_path,omitempty"`
	StatusPath             string   `json:"status_path,omitempty"`
	StopTokenPath          string   `json:"stop_token_path,omitempty"`
	Supervise              bool     `json:"supervise,omitempty"`
	Args                   []string `json:"args,omitempty"` // sanitized; no secrets
}

const (
	CredentialKindAPIKey = "api_key"
	CredentialKindToken  = "token"

	MountKindFUSE   = "fuse"
	MountKindVault  = "vault"
	MountKindWebDAV = "webdav"
)

func PIDFilePath(mountPoint string) string {
	// Legacy location kept for cross-version umount of mounts started before
	// UID-scoped state dirs. New writes still use this path.
	return filepath.Join(os.TempDir(), "drive9-mount-"+hash8(canonicalMountPoint(mountPoint))+".pid")
}

func ControlSocketPath(mountPoint string) string {
	canonical := canonicalMountPoint(mountPoint)
	uid := currentUID()
	sum := sha256.Sum256([]byte(uid + "\x00" + canonical))
	return filepath.Join(controlSocketDir(uid), "drive9-mount-"+hex.EncodeToString(sum[:8])+".sock")
}

func currentUID() string {
	uid := os.Getuid()
	if uid < 0 {
		return "unknown"
	}
	return strconv.Itoa(uid)
}

func controlSocketDir(uid string) string {
	if runtimeDir := strings.TrimSpace(os.Getenv("XDG_RUNTIME_DIR")); runtimeDir != "" && filepath.IsAbs(runtimeDir) {
		return runtimeDir
	}
	return filepath.Join(os.TempDir(), "drive9-"+uid)
}

// stateDir is a UID-scoped 0700 directory for supervisor state/lock/stop/exit
// files so other users cannot pre-create predictable paths in a sticky /tmp.
func stateDir() string {
	return controlSocketDir(currentUID())
}

// EnsureStateDir creates and validates the UID-scoped state directory used for
// supervisor lock/state/stop/exit files. Callers that open lock files must use
// this instead of raw MkdirAll so squatted/wrong-owner dirs fail closed.
func EnsureStateDir() error {
	return ensureStateDir()
}

func ensureStateDir() error {
	dir := stateDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return validateStateDir(dir)
}

// validateStateDir fails closed on symlink / wrong-owner / unrepairable mode
// state directories so a pre-created predictable path in sticky /tmp cannot
// intercept supervisor state, stop tokens, locks, or exit files.
func validateStateDir(dir string) error {
	info, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("state dir %s: %w", dir, err)
	}
	if err := checkStateDirInfo(dir, info); err != nil {
		return err
	}
	// POSIX mode bits are not meaningful on Windows (dirs report 0777);
	// only enforce 0700 repair when the platform exposes POSIX UIDs.
	if _, ok := stateDirOwnerUID(info); !ok {
		return nil
	}
	if info.Mode().Perm() != 0o700 {
		if err := os.Chmod(dir, 0o700); err != nil {
			return fmt.Errorf("chmod state dir %s: %w", dir, err)
		}
		info, err = os.Lstat(dir)
		if err != nil {
			return fmt.Errorf("state dir %s: %w", dir, err)
		}
		if info.Mode().Perm() != 0o700 {
			return fmt.Errorf("state dir %s: mode %o after chmod, want 700", dir, info.Mode().Perm())
		}
	}
	return nil
}

// checkStateDirInfo validates directory-ness and ownership for a state dir.
// Owner check is skipped when the platform does not expose POSIX UIDs.
func checkStateDirInfo(dir string, info os.FileInfo) error {
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("state dir %s: not a directory (mode %s)", dir, info.Mode())
	}
	if uid, ok := stateDirOwnerUID(info); ok && uid != os.Getuid() {
		return fmt.Errorf("state dir %s: owned by uid %d, want %d", dir, uid, os.Getuid())
	}
	return nil
}

// readTrustedFile reads a regular file only when it is owned by the current
// user (when the platform exposes UIDs) and is not group/world-writable.
// Symlinks and non-regular files are rejected so bare /tmp or legacy paths
// cannot be used as a forged trust root for identity / stop intent.
func readTrustedFile(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if err := checkTrustedFileInfo(path, info); err != nil {
		return nil, err
	}
	return os.ReadFile(path)
}

// checkTrustedFileInfo is the pure ownership/mode gate for identity state files.
func checkTrustedFileInfo(path string, info os.FileInfo) error {
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("state file %s: symlink not allowed", path)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("state file %s: not a regular file (mode %s)", path, info.Mode())
	}
	uid, hasUID := stateDirOwnerUID(info)
	if hasUID && uid != os.Getuid() {
		return fmt.Errorf("state file %s: owned by uid %d, want %d", path, uid, os.Getuid())
	}
	// Reject group/world-writable files on POSIX (sticky /tmp squat). On
	// Windows, writable files report mode 0666 and owner UIDs are unavailable;
	// skip the mode gate there (mirrors stateDirOwnerUID opt-out).
	if hasUID && info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("state file %s: mode %o is group/world-writable", path, info.Mode().Perm())
	}
	return nil
}

// trustedFilePresent reports whether path exists as a trusted regular state file.
func trustedFilePresent(path string) bool {
	info, err := os.Lstat(path)
	if err != nil {
		return false
	}
	return checkTrustedFileInfo(path, info) == nil
}

// canonicalMountPoint normalizes mountpoint for hashing without EvalSymlinks.
// Symlink resolution can hang forever on a wedged FUSE endpoint; control-plane
// path lookup must not perform mountpoint I/O.
func canonicalMountPoint(mountPoint string) string {
	canonical := filepath.Clean(mountPoint)
	if abs, err := filepath.Abs(canonical); err == nil {
		canonical = abs
	}
	return canonical
}

func hash8(canonical string) string {
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:8])
}

func WritePID(mountPoint string, pid int) (string, error) {
	if pid <= 0 {
		return "", fmt.Errorf("invalid pid %d", pid)
	}
	path := PIDFilePath(mountPoint)
	// 0600: identity files must not be group/world-writable (trust root).
	if err := writeFileAtomic(path, []byte(strconv.Itoa(pid)+"\n"), 0o600); err != nil {
		return "", err
	}
	return path, nil
}

func WriteProcessState(mountPoint string, state ProcessState) (string, error) {
	if state.PID <= 0 {
		return "", fmt.Errorf("invalid pid %d", state.PID)
	}
	path := PIDFilePath(mountPoint)
	data, err := json.Marshal(state)
	if err != nil {
		return "", fmt.Errorf("marshal process state: %w", err)
	}
	if err := writeFileAtomic(path, append(data, '\n'), 0o600); err != nil {
		return "", err
	}
	return path, nil
}

func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	tmp, err := createTempFile(dir, "."+base+".tmp-", perm)
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := replaceFile(tmpPath, path); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func createTempFile(dir, prefix string, perm os.FileMode) (*os.File, error) {
	var lastErr error
	for range 100 {
		var suffix [8]byte
		if _, err := rand.Read(suffix[:]); err != nil {
			return nil, err
		}
		name := filepath.Join(dir, prefix+hex.EncodeToString(suffix[:]))
		f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, perm)
		if err == nil {
			return f, nil
		}
		if !os.IsExist(err) {
			return nil, err
		}
		lastErr = err
	}
	return nil, fmt.Errorf("create temporary pid file: %w", lastErr)
}

func ReadPID(mountPoint string) (int, string, error) {
	state, path, err := ReadProcessState(mountPoint)
	if err != nil {
		return 0, path, err
	}
	return state.PID, path, nil
}

func ReadProcessState(mountPoint string) (ProcessState, string, error) {
	path := PIDFilePath(mountPoint)
	data, err := readTrustedFile(path)
	if err != nil {
		return ProcessState{}, path, err
	}
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return ProcessState{}, path, fmt.Errorf("read pid file %s: empty file", path)
	}
	if strings.HasPrefix(trimmed, "{") {
		var state ProcessState
		if err := json.Unmarshal([]byte(trimmed), &state); err != nil {
			return ProcessState{}, path, fmt.Errorf("read pid file %s: %w", path, err)
		}
		if state.PID <= 0 {
			return ProcessState{}, path, fmt.Errorf("read pid file %s: invalid pid %d", path, state.PID)
		}
		return state, path, nil
	}
	pid, err := strconv.Atoi(trimmed)
	if err != nil {
		return ProcessState{}, path, fmt.Errorf("read pid file %s: %w", path, err)
	}
	if pid <= 0 {
		return ProcessState{}, path, fmt.Errorf("read pid file %s: invalid pid %d", path, pid)
	}
	return ProcessState{PID: pid}, path, nil
}
