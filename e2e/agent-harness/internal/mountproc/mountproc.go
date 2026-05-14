// Package mountproc runs harness commands and tracks mount processes.
package mountproc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

var (
	ErrMountTimeout          = errors.New("mount startup timeout")
	ErrUnmountTimeout        = errors.New("unmount timeout")
	ErrUnexpectedProcessExit = errors.New("unexpected process exit")
)

type Result struct {
	ID       string
	Args     []string
	Stdout   string
	Stderr   string
	ExitCode int
	Duration time.Duration
	Err      error
}

type Env struct {
	Server string
	APIKey string
}

type Mount struct {
	ID           string
	Cmd          *exec.Cmd
	Mountpoint   string
	LogPath      string
	ProcessGroup int
}

func Run(ctx context.Context, id string, env Env, dir string, name string, args ...string) Result {
	start := time.Now()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Env = withEnv(os.Environ(), env)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	exit := 0
	if err != nil {
		exit = 1
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			exit = ee.ExitCode()
		}
	}
	return Result{ID: id, Args: append([]string{name}, args...), Stdout: stdout.String(), Stderr: stderr.String(), ExitCode: exit, Duration: time.Since(start), Err: err}
}

func StartMount(ctx context.Context, id string, env Env, drive9Bin, remoteRoot, mountpoint, syncMode, logPath string) (*Mount, error) {
	if err := os.MkdirAll(mountpoint, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return nil, err
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	args := []string{"mount", "--mode", "fuse", "--perf-counters"}
	if syncMode != "" {
		args = append(args, "--sync-mode", syncMode)
	}
	args = append(args, ":"+remoteRoot, mountpoint)
	cmd := exec.CommandContext(ctx, drive9Bin, args...)
	cmd.Env = withEnv(os.Environ(), env)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	setProcessGroup(cmd)
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return nil, err
	}
	pgid := processGroupID(cmd)
	go func() {
		_ = cmd.Wait()
		_ = logFile.Close()
	}()
	return &Mount{ID: id, Cmd: cmd, Mountpoint: mountpoint, LogPath: logPath, ProcessGroup: pgid}, nil
}

func WaitMounted(ctx context.Context, mountpoint string, check func(string) (bool, error)) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		mounted, err := check(mountpoint)
		if err == nil && mounted {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %s", ErrMountTimeout, mountpoint)
		case <-ticker.C:
		}
	}
}

func Stop(ctx context.Context, env Env, drive9Bin, mountpoint string) Result {
	return Run(ctx, "umount", env, "", drive9Bin, "umount", "--timeout", "30s", mountpoint)
}

func KillMount(m *Mount) error {
	if m == nil || m.Cmd == nil || m.Cmd.Process == nil {
		return nil
	}
	return killProcessGroup(m)
}

func withEnv(base []string, env Env) []string {
	out := append([]string{}, base...)
	if env.Server != "" {
		out = append(out, "DRIVE9_SERVER="+env.Server)
	}
	if env.APIKey != "" {
		out = append(out, "DRIVE9_API_KEY="+env.APIKey)
	}
	return out
}
