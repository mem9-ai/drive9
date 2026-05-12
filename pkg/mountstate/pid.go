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
	if err := writeFileAtomic(path, []byte(strconv.Itoa(pid)+"\n"), 0o644); err != nil {
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
	if err := writeFileAtomic(path, append(data, '\n'), 0o644); err != nil {
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
