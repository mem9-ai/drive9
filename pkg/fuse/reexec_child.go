package fuse

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// reexecRecvFdTimeout bounds how long the child waits to receive the FUSE
// device fd from the parent after connecting. It mirrors the parent's default
// AcceptTimeout so a dead/stalled parent cannot leave the child blocked forever
// in recvFd. It is a var (not const) so tests can shorten it.
var reexecRecvFdTimeout = 30 * time.Second

// IsReexecChild returns true if the current process was spawned by a reexec
// parent. The parent sets DRIVE9_REEXEC_SOCK to the Unix socket path.
func IsReexecChild() bool {
	return os.Getenv("DRIVE9_REEXEC_SOCK") != ""
}

// ReexecChildConfig holds the environment parsed from reexec env vars.
type ReexecChildConfig struct {
	SockPath   string
	MountPoint string
	Version    int
}

// ParseReexecChildEnv reads reexec configuration from environment variables
// set by the parent process. Returns an error if the env vars are missing or
// invalid.
func ParseReexecChildEnv() (ReexecChildConfig, error) {
	sockPath := os.Getenv("DRIVE9_REEXEC_SOCK")
	if sockPath == "" {
		return ReexecChildConfig{}, fmt.Errorf("DRIVE9_REEXEC_SOCK not set")
	}
	mountPoint := os.Getenv("DRIVE9_REEXEC_MOUNT")
	if mountPoint == "" {
		return ReexecChildConfig{}, fmt.Errorf("DRIVE9_REEXEC_MOUNT not set")
	}
	versionStr := os.Getenv("DRIVE9_REEXEC_VERSION")
	if versionStr == "" {
		return ReexecChildConfig{}, fmt.Errorf("DRIVE9_REEXEC_VERSION not set")
	}
	version, err := strconv.Atoi(versionStr)
	if err != nil {
		return ReexecChildConfig{}, fmt.Errorf("DRIVE9_REEXEC_VERSION invalid: %w", err)
	}
	return ReexecChildConfig{
		SockPath:   sockPath,
		MountPoint: mountPoint,
		Version:    version,
	}, nil
}

// ReexecChildHandshake connects to the parent's Unix socket, receives the
// FUSE device fd via SCM_RIGHTS, and validates the protocol version.
// On success the caller owns the returned fd. The returned conn must be
// kept open and used to send the accept message after serving starts.
func ReexecChildHandshake(cfg ReexecChildConfig) (fd int, conn *net.UnixConn, err error) {
	if cfg.Version != ReexecProtocolVersion {
		return -1, nil, fmt.Errorf("reexec child: protocol version mismatch: parent=%d, child=%d",
			cfg.Version, ReexecProtocolVersion)
	}

	rawConn, err := net.Dial("unix", cfg.SockPath)
	if err != nil {
		return -1, nil, fmt.Errorf("reexec child: connect to parent: %w", err)
	}

	unixConn := rawConn.(*net.UnixConn)
	// Bound the SCM_RIGHTS wait: if the parent dies or stalls between accepting
	// the connection and sending the fd, recvFd would otherwise block forever,
	// leaving a stray reexec child. A deadline makes the child fail fast so
	// supervisors can retry or report the failed handoff.
	if err := unixConn.SetReadDeadline(time.Now().Add(reexecRecvFdTimeout)); err != nil {
		_ = unixConn.Close()
		return -1, nil, fmt.Errorf("reexec child: set recv deadline: %w", err)
	}
	fd, err = recvFd(unixConn)
	if err != nil {
		_ = unixConn.Close()
		return -1, nil, fmt.Errorf("reexec child: receive fd: %w", err)
	}
	// Clear the deadline: the same conn is reused to send the accept message
	// after serving starts, which happens well after this bounded wait.
	if err := unixConn.SetReadDeadline(time.Time{}); err != nil {
		_ = unixConn.Close()
		return -1, nil, fmt.Errorf("reexec child: clear recv deadline: %w", err)
	}

	return fd, unixConn, nil
}

// ReexecChildImportAndServe imports the FUSE device fd, creates a server,
// starts serving, and validates the mount is working.
//
// On success, the returned server is already serving in a background
// goroutine. The caller must call SendReexecAccept on parentConn after
// establishing full operational ownership (control socket, pidfile, etc.)
// to signal the parent to exit.
//
// On failure, an error is returned and the caller should exit the
// process (the OS will close the fd and stop the Serve goroutine).
//
// IMPORTANT: failure paths must NOT call server.Unmount() because the
// server was created via ImportFd (not Mount). Unmount() would call
// fusermount -u, destroying the parent's mount and preventing rollback.
// Instead, we let the child process exit — the OS closes the fd, which
// causes Serve() to get EBADF/ENODEV and exit its read loop.
func ReexecChildImportAndServe(fd int, mountPoint string, rawFS gofuse.RawFileSystem, opts *gofuse.MountOptions) (*gofuse.Server, error) {
	server, err := gofuse.ImportFd(rawFS, mountPoint, fd, opts)
	if err != nil {
		// ImportFd already closes fd on failure (documented contract), so we
		// must NOT close it again — a double-close can hit an unrelated
		// descriptor if the number was reused in between.
		return nil, fmt.Errorf("reexec child: import fd: %w", err)
	}

	// Start serving in background. Serve() will close the fd when it exits.
	go server.Serve()

	// Probe the mount point to verify the FUSE connection is working.
	// The probe stat is queued by the kernel until Serve() starts reading
	// the fd. Use a timeout to avoid hanging forever if Serve() fails to
	// start (e.g. child crashes between goroutine spawn and first read).
	probeDone := make(chan error, 1)
	go func() {
		_, err := os.Stat(mountPoint)
		probeDone <- err
	}()

	select {
	case err := <-probeDone:
		if err != nil {
			teardownImportedServer(server, fd)
			return nil, fmt.Errorf("reexec child: mount probe failed: %w", err)
		}
	case <-time.After(10 * time.Second):
		teardownImportedServer(server, fd)
		return nil, fmt.Errorf("reexec child: mount probe timed out after 10s")
	}

	return server, nil
}

// teardownImportedServer stops a server started via ImportFd after a
// post-Serve failure (probe failed/timed out), honoring the
// ReexecChildImportAndServe contract that failures must not leave the
// handed-off FUSE connection alive.
//
// It must NOT call server.Unmount(): that runs `fusermount -u` and would
// destroy the parent's still-live mount, defeating rollback. Instead it closes
// the FUSE device fd, which makes the Serve() read loop fail with ENODEV/EBADF
// and exit. Serve() then closes ms.mountFd itself; that second close targets an
// already-closed descriptor and returns EBADF, which is harmless here because
// this function opens no new fds between the close and Wait(), so the number
// cannot have been reused. Wait() blocks until the Serve goroutine has fully
// exited so the caller can return knowing the connection is released.
func teardownImportedServer(server *gofuse.Server, fd int) {
	_ = syscall.Close(fd)
	server.Wait()
}

// SendReexecAccept sends the accept message to the parent process over
// the handshake connection. This must be called only after the child has
// fully established operational ownership (control socket, pidfile, etc.)
// so the parent can safely release its resources without racing the child.
func SendReexecAccept(parentConn *net.UnixConn) error {
	msg := reexecAcceptMsg{
		Accept:  true,
		Version: ReexecProtocolVersion,
	}
	if err := json.NewEncoder(parentConn).Encode(&msg); err != nil {
		return fmt.Errorf("reexec child: send accept: %w", err)
	}
	return nil
}
