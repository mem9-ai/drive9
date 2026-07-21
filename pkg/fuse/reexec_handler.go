package fuse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

// ErrReexecInProgress is returned by Reexec when another reexec attempt is
// already holding the guard. Callers should compare with errors.Is.
var ErrReexecInProgress = errors.New("reexec: already in progress")

// ReexecConfig holds parameters for a binary reexec attempt.
type ReexecConfig struct {
	// BinaryPath is the path to the new binary. If empty, os.Executable() is
	// used (self-reexec).
	BinaryPath string

	// MountPoint is the FUSE mount directory.
	MountPoint string

	// DrainTimeout is the maximum time to wait for Drain before aborting.
	DrainTimeout time.Duration

	// QuiesceTimeout is the maximum time to wait for in-flight FUSE requests
	// to complete during quiesce.
	QuiesceTimeout time.Duration

	// AcceptTimeout is the maximum time to wait for the new process to send
	// an accept message after receiving the fd.
	AcceptTimeout time.Duration
}

func (c *ReexecConfig) drainTimeout() time.Duration {
	if c.DrainTimeout > 0 {
		return c.DrainTimeout
	}
	return 30 * time.Second
}

func (c *ReexecConfig) quiesceTimeout() time.Duration {
	if c.QuiesceTimeout > 0 {
		return c.QuiesceTimeout
	}
	return 10 * time.Second
}

func (c *ReexecConfig) acceptTimeout() time.Duration {
	if c.AcceptTimeout > 0 {
		return c.AcceptTimeout
	}
	return 30 * time.Second
}

// reexecAcceptMsg is the JSON message sent by the new process after it has
// successfully imported the fd and started serving.
type reexecAcceptMsg struct {
	Accept  bool `json:"accept"`
	Version int  `json:"version"`
}

// ReexecResult describes the outcome of a reexec attempt.
type ReexecResult struct {
	// Accepted is true if the new process accepted the fd and is now serving.
	Accepted bool

	// Err is non-nil if the attempt failed.
	Err error
}

// Reexec performs a clean-state binary reexec. It implements the full V0
// handshake sequence:
//
//  1. Acquire the reexec guard (prevent concurrent attempts).
//  2. Drain dirty state (while still serving normally).
//  3. Quiesce FUSE dispatch (block new requests, wait inflight → 0).
//  4. Run ReexecPreflight (verify clean state under quiesce).
//  5. ExportFd (dup the FUSE device fd).
//  6. Create a Unix socket, spawn the child process.
//  7. Send the fd to the child via SCM_RIGHTS.
//  8. Wait for the child's accept message (or timeout/crash → rollback).
//
// On success, the caller should exit gracefully (without unmounting).
// On failure, the quiesce barrier is removed and the old process resumes.
func (fs *Dat9FS) Reexec(cfg ReexecConfig) ReexecResult {
	// Step 1: acquire the reexec guard (must be first to prevent concurrent
	// attempts even when preconditions fail).
	if !fs.reexecGuard.tryAcquire() {
		return ReexecResult{Err: ErrReexecInProgress}
	}
	defer fs.reexecGuard.release()

	if fs.server == nil {
		return ReexecResult{Err: fmt.Errorf("reexec: no FUSE server")}
	}

	// Resolve binary path.
	binaryPath := cfg.BinaryPath
	if binaryPath == "" {
		var err error
		binaryPath, err = os.Executable()
		if err != nil {
			return ReexecResult{Err: fmt.Errorf("reexec: resolve executable: %w", err)}
		}
	}

	// Step 2: drain dirty state (still serving normally).
	drainCtx, drainCancel := context.WithTimeout(context.Background(), cfg.drainTimeout())
	defer drainCancel()
	drainResp := fs.Drain(drainCtx)
	if drainResp.Error != "" {
		return ReexecResult{Err: fmt.Errorf("reexec: drain failed: %s", drainResp.Error)}
	}

	// Step 3: quiesce FUSE dispatch.
	quiesceCtx, quiesceCancel := context.WithTimeout(context.Background(), cfg.quiesceTimeout())
	defer quiesceCancel()
	token, err := fs.server.Quiesce(quiesceCtx)
	if err != nil {
		return ReexecResult{Err: fmt.Errorf("reexec: quiesce failed: %w", err)}
	}
	// On failure paths, resume quiesce so the old process continues serving.
	// On success, do NOT resume — the old server must stay quiesced so it
	// doesn't race the new process on the same FUSE connection fd.
	accepted := false
	defer func() {
		if !accepted {
			token.Resume()
		}
	}()

	// Step 4: preflight check (under quiesce — no new mutations).
	preflight := fs.ReexecPreflight()
	if !preflight.OK {
		return ReexecResult{Err: fmt.Errorf("reexec: preflight refused: %v", preflight.Refusals)}
	}

	// Step 5: export the FUSE fd.
	fd, err := fs.server.ExportFd()
	if err != nil {
		return ReexecResult{Err: fmt.Errorf("reexec: export fd: %w", err)}
	}
	defer func() {
		// Close our dup if we didn't hand it off successfully.
		_ = syscall.Close(fd)
	}()

	// Step 6: create Unix socket and spawn child.
	sockDir, err := os.MkdirTemp("", "drive9-reexec-*")
	if err != nil {
		return ReexecResult{Err: fmt.Errorf("reexec: create temp dir: %w", err)}
	}
	defer func() { _ = os.RemoveAll(sockDir) }()
	sockPath := filepath.Join(sockDir, "reexec.sock")

	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		return ReexecResult{Err: fmt.Errorf("reexec: listen: %w", err)}
	}
	defer func() { _ = listener.Close() }()

	// Spawn the child process with reexec-specific env vars.
	// os.Args[1:] is passed through intentionally — the child's main()
	// calls IsReexecChild() early and takes the reexec import path,
	// skipping any one-shot flags (e.g. --migrate). The DRIVE9_REEXEC_*
	// env vars are the real control channel; args are preserved so the
	// child's normal flag parser sees the same mount config.
	child := exec.Command(binaryPath, os.Args[1:]...)
	child.Stdout = os.Stdout
	child.Stderr = os.Stderr
	child.Env = append(os.Environ(),
		"DRIVE9_REEXEC_SOCK="+sockPath,
		"DRIVE9_REEXEC_MOUNT="+cfg.MountPoint,
		fmt.Sprintf("DRIVE9_REEXEC_VERSION=%d", ReexecProtocolVersion),
	)
	// Re-inject the live credential snapshot so the child can rebuild its
	// Drive9 client after fd import. The parent's ResolveCredentials already
	// unset DRIVE9_SERVER/DRIVE9_API_KEY/DRIVE9_VAULT_TOKEN from its own
	// environment (the "strip DRIVE9_* before exec" mitigation), so inheriting
	// only os.Environ() would leave a child that was started with explicit or
	// env credentials unable to authenticate. fs.opts holds the credential this
	// mount is bound to for its lifetime (Invariant #3); pass it through.
	child.Env = append(child.Env, reexecCredentialEnv(fs.opts)...)

	if err := child.Start(); err != nil {
		return ReexecResult{Err: fmt.Errorf("reexec: start child: %w", err)}
	}

	// Monitor child exit in a goroutine.
	childDone := make(chan error, 1)
	go func() {
		childDone <- child.Wait()
	}()

	// Step 7: accept connection from child and send fd.
	acceptDeadline := time.Now().Add(cfg.acceptTimeout())
	_ = listener.(*net.UnixListener).SetDeadline(acceptDeadline)

	// Accept in a goroutine so we can also observe childDone: if the child
	// exits before dialing the socket (e.g. an incompatible child rejecting
	// DRIVE9_REEXEC_VERSION, or a startup crash), a blocking Accept would wait
	// out the full AcceptTimeout while the mount stays quiesced. Selecting on
	// childDone lets the old process resume immediately instead.
	type acceptResult struct {
		conn net.Conn
		err  error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		c, err := listener.Accept()
		acceptCh <- acceptResult{conn: c, err: err}
	}()

	var conn net.Conn
	select {
	case res := <-acceptCh:
		if res.err != nil {
			_ = child.Process.Kill()
			<-childDone
			return ReexecResult{Err: fmt.Errorf("reexec: accept connection: %w", res.err)}
		}
		conn = res.conn
	case childErr := <-childDone:
		// Child is gone before connecting. Unblock the Accept goroutine by
		// closing the listener, then drain its result so the goroutine exits.
		_ = listener.Close()
		if res := <-acceptCh; res.conn != nil {
			_ = res.conn.Close()
		}
		return ReexecResult{Err: fmt.Errorf("reexec: child exited before connecting: %w", childErr)}
	}
	defer func() { _ = conn.Close() }()

	unixConn := conn.(*net.UnixConn)
	if err := sendFd(unixConn, fd); err != nil {
		_ = child.Process.Kill()
		<-childDone
		return ReexecResult{Err: fmt.Errorf("reexec: send fd: %w", err)}
	}

	// Step 8: wait for accept message from child.
	//
	// Reset the read deadline after the fd is sent so AcceptTimeout governs
	// only the "wait for the child to import/probe the fd and send its accept
	// message" phase, not the earlier connect wait. Reusing the single
	// acceptDeadline would let a slow-connecting child burn the read budget
	// and trigger a premature rollback.
	_ = conn.SetReadDeadline(time.Now().Add(cfg.acceptTimeout()))
	decoder := json.NewDecoder(conn)
	var msg reexecAcceptMsg
	if err := decoder.Decode(&msg); err != nil {
		_ = child.Process.Kill()
		<-childDone
		return ReexecResult{Err: fmt.Errorf("reexec: read accept: %w", err)}
	}

	if !msg.Accept {
		_ = child.Process.Kill()
		<-childDone
		return ReexecResult{Err: fmt.Errorf("reexec: child rejected handoff")}
	}

	if msg.Version != ReexecProtocolVersion {
		_ = child.Process.Kill()
		<-childDone
		return ReexecResult{Err: fmt.Errorf("reexec: protocol version mismatch: want %d, got %d", ReexecProtocolVersion, msg.Version)}
	}

	// Success — new process is serving. Mark accepted so deferred cleanup
	// does NOT resume quiesce (which would race the new process on the fd).
	// Our dup fd will be closed by the deferred syscall.Close, which is fine
	// since the child has its own copy via SCM_RIGHTS.
	accepted = true
	return ReexecResult{Accepted: true}
}

// reexecCredentialEnv builds the DRIVE9_SERVER/DRIVE9_API_KEY/DRIVE9_VAULT_TOKEN
// env entries the child needs to rebuild its Drive9 client. APIKey and Token are
// mutually exclusive (Invariant #3), so at most one credential var is emitted.
// Empty fields are omitted so we never plant a blank credential that would
// shadow a config-context fallback in the child's ResolveCredentials.
func reexecCredentialEnv(opts *MountOptions) []string {
	if opts == nil {
		return nil
	}
	var env []string
	if opts.Server != "" {
		env = append(env, "DRIVE9_SERVER="+opts.Server)
	}
	if opts.Token != "" {
		env = append(env, "DRIVE9_VAULT_TOKEN="+opts.Token)
	} else if opts.APIKey != "" {
		env = append(env, "DRIVE9_API_KEY="+opts.APIKey)
	}
	return env
}
