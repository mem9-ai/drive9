package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
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

// ResolveMountMode picks the concrete mode from "auto".
// On macOS auto always uses WebDAV for zero-install onboarding; FUSE is only
// selected there when explicitly requested with --mode=fuse.
func ResolveMountMode(mode MountMode, goos string, lookPath func(string) (string, error)) MountMode {
	_ = lookPath
	if mode != MountModeAuto {
		return mode
	}
	if goos == "darwin" {
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
func webdavMount(c *client.Client, mountPoint string) error {
	return webdavMountWithDeps(c, mountPoint, webdavMountDeps{
		goos:      runtime.GOOS,
		signals:   signalChannel(),
		runMount:  runWebDAVMountCmd,
		unmount:   webdavUnmount,
		exit:      os.Exit,
		newPrefix: newWebDAVNoncePrefix,
	})
}

type webdavMountDeps struct {
	goos      string
	signals   <-chan os.Signal
	runMount  func(goos, serverURL, mountPoint string) error
	unmount   func(goos, mountPoint string)
	exit      func(code int)
	newPrefix func() (string, error)
}

func webdavMountWithDeps(c *client.Client, mountPoint string, deps webdavMountDeps) error {
	if deps.goos == "" {
		deps.goos = runtime.GOOS
	}
	if deps.signals == nil {
		deps.signals = signalChannel()
	}
	if deps.runMount == nil {
		deps.runMount = runWebDAVMountCmd
	}
	if deps.unmount == nil {
		deps.unmount = webdavUnmount
	}
	if deps.exit == nil {
		deps.exit = os.Exit
	}
	if deps.newPrefix == nil {
		deps.newPrefix = newWebDAVNoncePrefix
	}

	// Bind to a random available port on loopback.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("webdav: listen: %w", err)
	}
	defer func() { _ = ln.Close() }()

	prefix, err := deps.newPrefix()
	if err != nil {
		return err
	}
	handler, err := newWebDAVHandler(c, prefix)
	if err != nil {
		return fmt.Errorf("webdav: %w", err)
	}

	addr := ln.Addr().(*net.TCPAddr)
	serverURL := fmt.Sprintf("http://127.0.0.1:%d%s/", addr.Port, prefix)
	srv := &http.Server{Handler: handler}

	// Start HTTP server in background.
	srvErr := make(chan error, 1)
	go func() {
		srvErr <- srv.Serve(ln)
	}()

	readyCtx, readyCancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := waitForWebDAVReady(readyCtx, serverURL); err != nil {
		readyCancel()
		_ = srv.Close()
		select {
		case serveErr := <-srvErr:
			if serveErr != nil && serveErr != http.ErrServerClosed {
				return fmt.Errorf("webdav: server error: %w", serveErr)
			}
		default:
		}
		return err
	}
	readyCancel()

	// Ensure mount point exists.
	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		_ = srv.Close()
		return fmt.Errorf("webdav: create mount point: %w", err)
	}

	// Invoke mount_webdav.
	if err := deps.runMount(deps.goos, serverURL, mountPoint); err != nil {
		_ = srv.Close()
		return fmt.Errorf("webdav: mount_webdav failed: %w", err)
	}

	fmt.Fprintf(os.Stderr, "dat9: mounted on %s via WebDAV (%s)\n", mountPoint, serverURL)

	// Signal handling for graceful shutdown.
	go func() {
		<-deps.signals
		fmt.Fprintf(os.Stderr, "\ndat9: unmounting %s...\n", mountPoint)

		// Second signal forces exit.
		go func() {
			<-deps.signals
			fmt.Fprintf(os.Stderr, "dat9: forced exit\n")
			deps.exit(1)
		}()

		// Unmount first, then stop server.
		deps.unmount(deps.goos, mountPoint)
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

func signalChannel() <-chan os.Signal {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	return sigCh
}

func runWebDAVMountCmd(goos, serverURL, mountPoint string) error {
	mountCmd, err := webdavMountCmd(goos, serverURL, mountPoint)
	if err != nil {
		return err
	}
	mountCmd.Stdout = os.Stderr
	mountCmd.Stderr = os.Stderr
	return mountCmd.Run()
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

func newWebDAVNoncePrefix() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("webdav: generate nonce prefix: %w", err)
	}
	return "/_drive9_" + hex.EncodeToString(raw[:]), nil
}

func waitForWebDAVReady(ctx context.Context, serverURL string) error {
	client := http.Client{Timeout: 200 * time.Millisecond}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodOptions, serverURL, nil)
		if err != nil {
			return fmt.Errorf("webdav: readiness request: %w", err)
		}
		resp, err := client.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode < 500 {
				return nil
			}
			lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
		} else {
			lastErr = err
		}

		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("webdav: server not ready: %w", lastErr)
			}
			return fmt.Errorf("webdav: server not ready: %w", ctx.Err())
		case <-ticker.C:
		}
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
