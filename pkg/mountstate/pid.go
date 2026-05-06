package mountstate

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

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

func ReadPID(mountPoint string) (int, string, error) {
	path := PIDFilePath(mountPoint)
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, path, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, path, fmt.Errorf("read pid file %s: %w", path, err)
	}
	if pid <= 0 {
		return 0, path, fmt.Errorf("read pid file %s: invalid pid %d", path, pid)
	}
	return pid, path, nil
}
