package mountstate

import (
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
	PID          int    `json:"pid"`
	CreationTime uint64 `json:"creation_time,omitempty"`
}

func PIDFilePath(mountPoint string) string {
	canonical := canonicalMountPoint(mountPoint)
	sum := sha256.Sum256([]byte(canonical))
	return filepath.Join(os.TempDir(), "drive9-mount-"+hex.EncodeToString(sum[:8])+".pid")
}

func canonicalMountPoint(mountPoint string) string {
	canonical := filepath.Clean(mountPoint)
	if abs, err := filepath.Abs(canonical); err == nil {
		canonical = abs
	}
	if resolved, err := filepath.EvalSymlinks(canonical); err == nil {
		canonical = resolved
	}
	return canonical
}

func WritePID(mountPoint string, pid int) (string, error) {
	if pid <= 0 {
		return "", fmt.Errorf("invalid pid %d", pid)
	}
	path := PIDFilePath(mountPoint)
	if err := os.WriteFile(path, []byte(strconv.Itoa(pid)+"\n"), 0o644); err != nil {
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
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		return "", err
	}
	return path, nil
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
	data, err := os.ReadFile(path)
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
