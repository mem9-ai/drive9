package fuse

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

// ProbeMountPointReady confirms the mountpoint is an active, usable FUSE mount.
// It is safe to call from a supervisor process (not from inside the FUSE worker
// as the primary hang detector).
func ProbeMountPointReady(mountPoint string) error {
	return probeMountPointReady(mountPoint)
}

// ActiveMountPoint reports whether path is currently an active mount.
func ActiveMountPoint(path string) (bool, error) {
	return activeMountPoint(path)
}

// ForceUnmount force-unmounts a FUSE mountpoint (graceful-death path).
func ForceUnmount(mountPoint string) {
	forceUnmount(mountPoint)
}

// ForceUnmountLazy force-unmounts using lazy detach where available.
// Prefer this on abnormal death / supervisor cleanup paths.
func ForceUnmountLazy(mountPoint string) {
	forceUnmountLazy(mountPoint)
}

// forceUnmountLazy prefers lazy/force detach variants for death cleanup.
func forceUnmountLazy(mountpoint string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var cmd *exec.Cmd
	if runtime.GOOS == "darwin" {
		cmd = exec.CommandContext(ctx, "diskutil", "unmount", "force", mountpoint)
	} else {
		if _, err := exec.LookPath("fusermount3"); err == nil {
			cmd = exec.CommandContext(ctx, "fusermount3", "-uz", mountpoint)
		} else if _, err := exec.LookPath("fusermount"); err == nil {
			cmd = exec.CommandContext(ctx, "fusermount", "-uz", mountpoint)
		} else {
			cmd = exec.CommandContext(ctx, "umount", "-l", mountpoint)
		}
	}
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			fmt.Fprintf(os.Stderr, "drive9: force unmount (lazy) timed out after 5s\n")
		} else {
			fmt.Fprintf(os.Stderr, "drive9: force unmount (lazy) failed: %v; retrying standard force\n", err)
			forceUnmount(mountpoint)
		}
	}
}

// EnsureCleanMountpoint force-unmounts a stale (broken) mount if present.
// Returns true if a cleanup action was taken.
func EnsureCleanMountpoint(mountPoint string) (cleaned bool, err error) {
	mountPoint = strings.TrimSpace(mountPoint)
	if mountPoint == "" {
		return false, fmt.Errorf("empty mountpoint")
	}
	active, activeErr := activeMountPoint(mountPoint)
	if activeErr != nil {
		if os.IsNotExist(activeErr) {
			return false, nil
		}
		if isTransportBroken(activeErr) {
			forceUnmountLazy(mountPoint)
			return true, nil
		}
		return false, activeErr
	}
	if !active {
		return false, nil
	}
	if probeErr := probeMountPointReady(mountPoint); probeErr != nil {
		forceUnmountLazy(mountPoint)
		return true, nil
	}
	return false, nil
}

func isTransportBroken(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, sub := range []string{
		"transport endpoint is not connected",
		"connection aborted",
		"software caused connection abort",
		"input/output error",
		"enotconn",
		"econnaborted",
	} {
		if strings.Contains(msg, sub) {
			return true
		}
	}
	return false
}
