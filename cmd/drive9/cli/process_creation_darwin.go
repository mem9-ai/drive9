//go:build darwin

package cli

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func processCreationTimeByPID(pid int) (uint64, error) {
	if pid <= 0 {
		return 0, fmt.Errorf("invalid mount process pid %d", pid)
	}
	kp, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return 0, err
	}
	if kp == nil || int(kp.Proc.P_pid) != pid {
		return 0, os.ErrNotExist
	}
	start := kp.Proc.P_starttime
	if start.Sec < 0 || start.Usec < 0 {
		return 0, fmt.Errorf("invalid process start time for pid %d", pid)
	}
	return uint64(start.Sec)*1_000_000_000 + uint64(start.Usec)*1_000, nil
}
