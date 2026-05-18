// Package safety validates generated paths before the harness touches them.
package safety

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/mem9-ai/dat9/pkg/pathutil"
)

var (
	ErrInvalidRoot          = errors.New("invalid root")
	ErrInvalidGeneratedPath = errors.New("invalid generated path")
	ErrExistingMountpoint   = errors.New("existing mountpoint")
)

func ValidateRoot(name, p string) (string, error) {
	if p == "" || !strings.HasPrefix(p, "/") {
		return "", fmt.Errorf("%w: %s must be absolute", ErrInvalidRoot, name)
	}
	clean := filepath.Clean(p)
	if strings.Contains(clean, "..") {
		return "", fmt.Errorf("%w: %s contains traversal", ErrInvalidRoot, name)
	}
	return clean, nil
}

func CaseRemoteRoot(base, suffix string) (string, error) {
	if err := validateSuffix("remote_root_suffix", suffix); err != nil {
		return "", err
	}
	root, err := pathutil.Canonicalize(path.Join(base, suffix))
	if err != nil {
		return "", fmt.Errorf("%w: remote root: %v", ErrInvalidGeneratedPath, err)
	}
	baseClean, err := pathutil.Canonicalize(base)
	if err != nil {
		return "", fmt.Errorf("%w: remote base: %v", ErrInvalidRoot, err)
	}
	if root == baseClean || !strings.HasPrefix(root, strings.TrimRight(baseClean, "/")+"/") {
		return "", fmt.Errorf("%w: remote root %q escapes %q", ErrInvalidGeneratedPath, root, baseClean)
	}
	return root, nil
}

func Mountpoint(root, runID, suffix string) (string, error) {
	if err := validateSuffix("mountpoint_suffix", suffix); err != nil {
		return "", err
	}
	name := "drive9-agent-" + runID + "-" + suffix
	mp := filepath.Join(root, name)
	cleanRoot := filepath.Clean(root)
	cleanMP := filepath.Clean(mp)
	if cleanMP == cleanRoot || !strings.HasPrefix(cleanMP, cleanRoot+string(os.PathSeparator)) {
		return "", fmt.Errorf("%w: mountpoint %q escapes %q", ErrInvalidGeneratedPath, cleanMP, cleanRoot)
	}
	return cleanMP, nil
}

func ValidateMountpointAvailable(mp string) error {
	if mounted, err := IsMounted(mp); err != nil {
		return fmt.Errorf("%w: mount status check failed for %s: %v", ErrExistingMountpoint, mp, err)
	} else if mounted {
		return fmt.Errorf("%w: %s is already mounted", ErrExistingMountpoint, mp)
	}
	info, err := os.Lstat(mp)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("%w: inspect %s: %v", ErrExistingMountpoint, mp, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: %s is a symlink", ErrExistingMountpoint, mp)
	}
	if !info.IsDir() {
		return fmt.Errorf("%w: %s is not a directory", ErrExistingMountpoint, mp)
	}
	entries, err := os.ReadDir(mp)
	if err != nil {
		return fmt.Errorf("%w: read %s: %v", ErrExistingMountpoint, mp, err)
	}
	if len(entries) != 0 {
		return fmt.Errorf("%w: %s is non-empty", ErrExistingMountpoint, mp)
	}
	return nil
}

func IsMounted(mp string) (bool, error) {
	clean := filepath.Clean(mp)
	if runtime.GOOS == "linux" {
		b, err := os.ReadFile("/proc/mounts")
		if err != nil {
			return false, err
		}
		for _, line := range strings.Split(string(b), "\n") {
			fields := strings.Fields(line)
			if len(fields) >= 2 && fields[1] == clean {
				return true, nil
			}
		}
		return false, nil
	}
	out, err := exec.Command("mount").Output()
	if err != nil {
		return false, err
	}
	return strings.Contains(string(out), " on "+clean+" "), nil
}

func validateSuffix(field, suffix string) error {
	if suffix == "" || strings.HasPrefix(suffix, "/") || strings.Contains(suffix, "/") ||
		strings.Contains(suffix, "\\") || suffix == "." || suffix == ".." || strings.Contains(suffix, "..") {
		return fmt.Errorf("%w: invalid %s %q", ErrInvalidGeneratedPath, field, suffix)
	}
	return nil
}
