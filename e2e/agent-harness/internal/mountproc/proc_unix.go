//go:build !windows

package mountproc

import (
	"os/exec"
	"syscall"
)

func setProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func processGroupID(cmd *exec.Cmd) int {
	if cmd == nil || cmd.Process == nil {
		return 0
	}
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		return cmd.Process.Pid
	}
	return pgid
}

func killProcessGroup(m *Mount) error {
	if m.ProcessGroup > 0 {
		return syscall.Kill(-m.ProcessGroup, syscall.SIGTERM)
	}
	return m.Cmd.Process.Kill()
}

