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
// Unbounded: may hang on a wedged FUSE endpoint. Prefer
// ActiveMountPointBounded for supervisor cleanup / ensure paths.
func ActiveMountPoint(path string) (bool, error) {
	return activeMountPoint(path)
}

// cleanupActiveMountTimeout bounds Stat used by destructive cleanup.
// Wedged FUSE daemons can block os.Stat indefinitely; cleanup must not.
var cleanupActiveMountTimeout = 2 * time.Second

// activeMountProbe is the underlying active-mount check. Tests inject hangs.
var activeMountProbe = activeMountPoint

// ActiveMountPointBounded is ActiveMountPoint with a hard timeout for cleanup
// and recovery paths that must not stall on a wedged FUSE endpoint.
func ActiveMountPointBounded(path string) (bool, error) {
	return activeMountPointBounded(path)
}

func activeMountPointBounded(path string) (bool, error) {
	type result struct {
		active bool
		err    error
	}
	// Capture probe in the caller goroutine so test cleanups that restore
	// activeMountProbe cannot race a still-running timed-out probe goroutine.
	probe := activeMountProbe
	timeout := cleanupActiveMountTimeout
	ch := make(chan result, 1)
	go func() {
		// A timeout abandons this goroutine; if the underlying Stat is wedged
		// it may leak until the endpoint recovers (same tradeoff as health probes).
		a, e := probe(path)
		ch <- result{a, e}
	}()
	select {
	case r := <-ch:
		return r.active, r.err
	case <-time.After(timeout):
		return false, fmt.Errorf("active mount check timed out after %s", timeout)
	}
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
// After FUSE daemon SIGKILL, fusermount can fail with EACCES on the dead
// endpoint; always fall through to umount -l so remount can succeed.
func forceUnmountLazy(mountpoint string) {
	if runtime.GOOS == "darwin" {
		_ = runUnmountCmd(5*time.Second, "diskutil", "unmount", "force", mountpoint)
		return
	}
	// Linux: try fusermount lazy first (updates mtab), then kernel lazy.
	tried := false
	if _, err := exec.LookPath("fusermount3"); err == nil {
		tried = true
		if runUnmountCmd(5*time.Second, "fusermount3", "-uz", mountpoint) == nil && !mountStillNeedsClean(mountpoint) {
			return
		}
	}
	if _, err := exec.LookPath("fusermount"); err == nil {
		tried = true
		if runUnmountCmd(5*time.Second, "fusermount", "-uz", mountpoint) == nil && !mountStillNeedsClean(mountpoint) {
			return
		}
	}
	// umount -l is the reliable last resort for orphan FUSE superblocks.
	if _, err := exec.LookPath("umount"); err == nil {
		if err := runUnmountCmd(5*time.Second, "umount", "-l", mountpoint); err != nil && tried {
			fmt.Fprintf(os.Stderr, "drive9: force unmount (lazy) exhausted fusermount+umount -l: %v\n", err)
		}
		return
	}
	if tried {
		// No umount binary — fall back to non-lazy fusermount.
		forceUnmount(mountpoint)
	}
}

// runUnmountCmd runs an unmount helper with a deadline. Stderr is inherited so
// fusermount diagnostics remain visible in supervisor logs.
func runUnmountCmd(timeout time.Duration, name string, args ...string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil && ctx.Err() == context.DeadlineExceeded {
		fmt.Fprintf(os.Stderr, "drive9: %s %v timed out after %s\n", name, args, timeout)
	}
	return err
}

// EnsureCleanMountpoint force-unmounts a stale (broken) mount if present.
// Destructive cleanup is local-only:
//   - transport-broken endpoints (ENOTCONN/EIO/EACCES-after-daemon-death), or
//   - active mounts whose recorded owner process is dead.
//
// Timeout / unknown probe errors first pass the ownerAlive identity gate: a
// slow-but-live owned mount is degraded, not stale, and must not be detached
// (design: backend outage / slow getattr ≠ FUSE death). Backend readdir
// failures on a live owned mount must NOT trigger force-unmount. When
// force-unmount is attempted but the endpoint remains, returns a non-nil error.
func EnsureCleanMountpoint(mountPoint string) (cleaned bool, err error) {
	mountPoint = strings.TrimSpace(mountPoint)
	if mountPoint == "" {
		return false, fmt.Errorf("empty mountpoint")
	}
	// Always use the bounded probe: cleanup is on the supervisor critical path
	// and must not hang forever on a wedged FUSE stat(2).
	active, activeErr := activeMountPointBounded(mountPoint)
	if activeErr != nil {
		if os.IsNotExist(activeErr) {
			return false, nil
		}
		// Transport-broken: kernel-mounted dead endpoint — detach immediately.
		// Timeout/unknown: may be a slow-but-live FUSE daemon; only detach when
		// the recorded owner (worker PID+creation) is dead/missing.
		if !isTransportBroken(activeErr) && ownerAlive(mountPoint) {
			return false, nil
		}
		forceUnmountLazy(mountPoint)
		if stillBroken := mountStillNeedsClean(mountPoint); stillBroken {
			return true, fmt.Errorf("force unmount did not clear broken mountpoint %s: %v", mountPoint, activeErr)
		}
		return true, nil
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
		// FUSE endpoint is owned by the worker, never the supervisor process.
		// WorkerPID==0 means the worker is gone (or not yet recorded); do not
		// treat the live supervisor as owner or EnsureClean will refuse to
		// detach orphans after kill -9.
		if st.WorkerPID > 0 {
			pid, creation = st.WorkerPID, st.WorkerCreation
		} else {
			return false
		}
	}
	if pid == 0 {
		ps, _, err := mountstate.ReadProcessState(mountPoint)
		if err != nil {
			return false
		}
		if ps.WorkerPID > 0 {
			pid = ps.WorkerPID
		} else if ps.Role == mountstate.RoleSupervisor || ps.Supervise {
			// Supervised mount with no worker identity → orphan.
			return false
		} else {
			// Unsupervised single-process mount: PID is the FUSE daemon.
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
	active, err := activeMountPointBounded(mountPoint)
	if err != nil {
		// NotExist → clean. Timeout / ENOTCONN / other → still needs work.
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
		// After FUSE daemon SIGKILL some kernels surface EACCES/EPERM on the
		// mountpoint instead of ENOTCONN; still needs force/lazy unmount.
		"permission denied",
		"operation not permitted",
		"eacces",
		"eperm",
	} {
		if strings.Contains(msg, sub) {
			return true
		}
	}
	return false
}
