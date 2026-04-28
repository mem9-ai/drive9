package cli

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
)

// MountMode selects the filesystem mount backend.
type MountMode string

const (
	MountModeAuto   MountMode = "auto"
	MountModeFUSE   MountMode = "fuse"
	MountModeWebDAV MountMode = "webdav"
)

// ParseMountMode validates the --mode flag value.
func ParseMountMode(s string) (MountMode, error) {
	switch MountMode(s) {
	case MountModeAuto, MountModeFUSE, MountModeWebDAV:
		return MountMode(s), nil
	default:
		return "", fmt.Errorf("invalid mount mode %q (must be auto, fuse, or webdav)", s)
	}
}

// ResolveMountMode picks the concrete mode from "auto" by probing the system.
// On macOS auto prefers WebDAV (zero-install); on Linux auto prefers FUSE.
func ResolveMountMode(mode MountMode, goos string, lookPath func(string) (string, error)) MountMode {
	if mode != MountModeAuto {
		return mode
	}
	if goos == "darwin" {
		if hasFUSE(lookPath) {
			return MountModeFUSE
		}
		return MountModeWebDAV
	}
	// Linux and others: FUSE is the default.
	return MountModeFUSE
}

// hasFUSE checks whether macFUSE (or FUSE-T) is usable on the current system.
func hasFUSE(lookPath func(string) (string, error)) bool {
	// macFUSE installs mount_macfuse; FUSE-T installs mount_fusefs.
	for _, bin := range []string{"mount_macfuse", "mount_fusefs"} {
		if _, err := lookPath(bin); err == nil {
			return true
		}
	}
	return false
}

// WebDAVMountHandler is the interface that the pkg/webdav adapter must satisfy.
// It returns an http.Handler that serves WebDAV requests backed by a drive9 client.
// This is the contract between CLI lifecycle (this file) and the adapter (pkg/webdav).
type WebDAVMountHandler interface {
	http.Handler
}

// webdavMount starts a local WebDAV server bound to 127.0.0.1, invokes
// mount_webdav to attach it to mountPoint, and blocks until SIGINT/SIGTERM.
func webdavMount(c *client.Client, mountPoint string, handler http.Handler) error {
	// Bind to a random available port on loopback.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("webdav: listen: %w", err)
	}
	defer func() { _ = ln.Close() }()

	addr := ln.Addr().(*net.TCPAddr)
	serverURL := fmt.Sprintf("http://127.0.0.1:%d", addr.Port)

	srv := &http.Server{Handler: handler}

	// Start HTTP server in background.
	srvErr := make(chan error, 1)
	go func() {
		srvErr <- srv.Serve(ln)
	}()

	// Brief pause to let the server start accepting connections.
	time.Sleep(50 * time.Millisecond)

	// Ensure mount point exists.
	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		_ = srv.Close()
		return fmt.Errorf("webdav: create mount point: %w", err)
	}

	// Invoke mount_webdav.
	mountCmd, err := webdavMountCmd(runtime.GOOS, serverURL, mountPoint)
	if err != nil {
		_ = srv.Close()
		return err
	}
	mountCmd.Stdout = os.Stderr
	mountCmd.Stderr = os.Stderr
	if err := mountCmd.Run(); err != nil {
		_ = srv.Close()
		return fmt.Errorf("webdav: mount_webdav failed: %w", err)
	}

	fmt.Fprintf(os.Stderr, "dat9: mounted on %s via WebDAV (%s)\n", mountPoint, serverURL)

	// Signal handling for graceful shutdown.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Fprintf(os.Stderr, "\ndat9: unmounting %s...\n", mountPoint)

		// Second signal forces exit.
		go func() {
			<-sigCh
			fmt.Fprintf(os.Stderr, "dat9: forced exit\n")
			os.Exit(1)
		}()

		// Unmount first, then stop server.
		webdavUnmount(runtime.GOOS, mountPoint)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	// Block until server exits (from signal handler shutdown or error).
	if err := <-srvErr; err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("webdav: server error: %w", err)
	}
	return nil
}

// webdavMountCmd builds the OS command to mount a WebDAV URL at mountPoint.
func webdavMountCmd(goos, serverURL, mountPoint string) (*exec.Cmd, error) {
	switch goos {
	case "darwin":
		// -S suppresses auth UI dialogs and auto-unmounts on disconnect.
		return exec.Command("mount_webdav", "-S", serverURL, mountPoint), nil
	case "linux":
		// Linux: try davfs2 if available.
		if _, err := exec.LookPath("mount.davfs"); err == nil {
			return exec.Command("mount.davfs", serverURL, mountPoint), nil
		}
		return nil, fmt.Errorf("webdav: no WebDAV mount utility found on Linux (install davfs2)")
	default:
		return nil, fmt.Errorf("webdav: unsupported OS %q", goos)
	}
}

// webdavUnmount detaches the WebDAV mount.
func webdavUnmount(goos, mountPoint string) {
	var cmd *exec.Cmd
	switch goos {
	case "darwin":
		cmd = exec.Command("umount", mountPoint)
	default:
		cmd = exec.Command("umount", mountPoint)
	}
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "dat9: webdav unmount failed: %v\n", err)
	}
}
