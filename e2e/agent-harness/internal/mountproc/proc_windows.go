//go:build windows

package mountproc

import "os/exec"

func setProcessGroup(_ *exec.Cmd) {}

func processGroupID(cmd *exec.Cmd) int {
	if cmd == nil || cmd.Process == nil {
		return 0
	}
	return cmd.Process.Pid
}

func killProcessGroup(m *Mount) error {
	return m.Cmd.Process.Kill()
}

