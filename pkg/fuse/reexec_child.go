package fuse

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

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
	fd, err = recvFd(unixConn)
	if err != nil {
		unixConn.Close()
		return -1, nil, fmt.Errorf("reexec child: receive fd: %w", err)
	}

	return fd, unixConn, nil
}

// ReexecChildImportAndServe imports the FUSE device fd, creates a server,
// starts serving, validates the mount is working, and sends the accept
// message back to the parent through parentConn.
//
// On success, the returned server is already serving in a background
// goroutine. On failure, the fd is closed and an error is returned.
func ReexecChildImportAndServe(fd int, mountPoint string, rawFS gofuse.RawFileSystem, opts *gofuse.MountOptions, parentConn *net.UnixConn) (*gofuse.Server, error) {
	server, err := gofuse.ImportFd(rawFS, mountPoint, fd, opts)
	if err != nil {
		syscall.Close(fd)
		return nil, fmt.Errorf("reexec child: import fd: %w", err)
	}

	// Start serving in background.
	serveDone := make(chan struct{})
	go func() {
		server.Serve()
		close(serveDone)
	}()

	// Probe the mount point to verify the FUSE connection is working.
	if _, err := os.Stat(mountPoint); err != nil {
		// Probe failed — stop the server. Don't call Unmount since we
		// imported the fd (did not mount); just let Serve drain.
		// The parent will rollback when it doesn't receive accept.
		return nil, fmt.Errorf("reexec child: mount probe failed: %w", err)
	}

	// Send accept to parent.
	msg := reexecAcceptMsg{
		Accept:  true,
		Version: ReexecProtocolVersion,
	}
	if err := json.NewEncoder(parentConn).Encode(&msg); err != nil {
		return nil, fmt.Errorf("reexec child: send accept: %w", err)
	}

	return server, nil
}
