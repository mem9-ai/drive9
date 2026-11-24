//go:build windows

package git

import (
	"errors"
	"os"
	"os/exec"
)

func configureGitCommandGroup(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	cmd.WaitDelay = gitSubprocessWaitDelay
	cmd.Cancel = func() error { return killGitCommandGroup(cmd) }
}

func killGitCommandGroup(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return os.ErrProcessDone
	}
	if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	return nil
}
