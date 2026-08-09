package fuse

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
)

// ProbeMountPointReady confirms the mountpoint is an active, usable FUSE mount
// via root readdir. Prefer ProbeMountLocalHealth for destructive supervisor
// decisions: readdir can reach the remote backend after cache expiry.
func ProbeMountPointReady(mountPoint string) error {
	return probeMountPointReady(mountPoint)
}

// ProbeMountLocalHealth checks local FUSE endpoint health without remote List.
// It verifies an active/not-transport-broken mount and, when the control socket
// exists, a backend-independent control-socket ping. Owner PID liveness is
// checked by callers (supervisor healthOK / EnsureClean ownerAlive).
// Backend outages must not fail this probe.
func ProbeMountLocalHealth(mountPoint string) error {
	mountPoint = strings.TrimSpace(mountPoint)
	if mountPoint == "" {
		return fmt.Errorf("empty mountpoint")
	}
	done := make(chan error, 1)
	go func() {
		done <- probeMountLocalHealthOnce(mountPoint)
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(probeMountPointReadyTimeout):
		return fmt.Errorf("local health probe timed out after %s", probeMountPointReadyTimeout)
	}
}

func probeMountLocalHealthOnce(mountPoint string) error {
	active, err := activeMountPoint(mountPoint)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("mountpoint does not exist")
		}
		if isTransportBroken(err) {
			return err
		}
		// Unknown stat error: treat as unhealthy for local liveness.
		return err
	}
	if !active {
		return fmt.Errorf("mountpoint is not an active mount")
	}
	// Control socket ping is backend-independent (separate goroutine in worker).
	// Use a short dial budget so a stuck socket cannot dominate health latency.
	sock := mountstate.ControlSocketPath(mountPoint)
	if _, statErr := os.Stat(sock); statErr == nil {
		const pingTimeout = 2 * time.Second
		ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
		defer cancel()
		if _, pingErr := mountcontrol.RequestStatus(ctx, sock, pingTimeout); pingErr != nil {
			return fmt.Errorf("control socket ping: %w", pingErr)
		}
	}
	return nil
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
// Destructive cleanup is local-only: transport-broken endpoints, or active
// mounts whose recorded owner process is dead. Backend readdir failures on a
// live owned mount must NOT trigger force-unmount (design: backend outage ≠
// FUSE death). When force-unmount is attempted but the endpoint remains,
// returns a non-nil error.
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
			if stillBroken := mountStillNeedsClean(mountPoint); stillBroken {
				return true, fmt.Errorf("force unmount did not clear broken mountpoint %s", mountPoint)
			}
			return true, nil
		}
		return false, activeErr
	}
	if !active {
		return false, nil
	}
	// Mount is kernel-active. Only force-unmount if the recorded owner is dead
	// (orphan stale mount). Live owner + readdir/backend failure is degraded,
	// not cleanup.
	if ownerAlive(mountPoint) {
		return false, nil
	}
	forceUnmountLazy(mountPoint)
	if mountStillNeedsClean(mountPoint) {
		return true, fmt.Errorf("force unmount did not clear mountpoint %s", mountPoint)
	}
	return true, nil
}

func ownerAlive(mountPoint string) bool {
	pid := 0
	var creation uint64
	if st, _, err := mountstate.ReadSupervisorState(mountPoint); err == nil {
		if st.WorkerPID > 0 {
			pid, creation = st.WorkerPID, st.WorkerCreation
		} else if st.PID > 0 {
			pid, creation = st.PID, st.CreationTime
		}
	}
	if pid == 0 {
		ps, _, err := mountstate.ReadProcessState(mountPoint)
		if err != nil {
			return false
		}
		if ps.WorkerPID > 0 {
			pid = ps.WorkerPID
		} else {
			pid = ps.PID
			creation = ps.CreationTime
		}
	}
	if pid <= 0 {
		return false
	}
	got, err := mountstate.ProcessCreationTime(pid)
	if err != nil || got == 0 {
		return false
	}
	if creation == 0 {
		return true
	}
	return got == creation
}

func mountStillNeedsClean(mountPoint string) bool {
	active, err := activeMountPoint(mountPoint)
	if err != nil {
		// NotExist → clean. Any other error (including ENOTCONN) → still needs work.
		return !os.IsNotExist(err)
	}
	return active
}

// IsTransportBroken reports whether err indicates a still-mounted but unusable
// FUSE endpoint (ENOTCONN / EIO / etc.). Callers should treat these as active
// for cleanup purposes rather than "already unmounted".
func IsTransportBroken(err error) bool {
	return isTransportBroken(err)
}

func isTransportBroken(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, sub := range []string{
		"transport endpoint is not connected",
		"socket is not connected", // Darwin ENOTCONN wording
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
