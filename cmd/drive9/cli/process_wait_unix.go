//go:build !windows

package cli

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"
)

func processAliveImpl(pid int) bool {
	if pid <= 0 {
		return false
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = process.Signal(syscall.Signal(0))
	if err == nil {
		return true
	}
	if errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH) {
		return false
	}
	if errors.Is(err, syscall.EPERM) {
		return true
	}
	return false
}

func waitForProcessExitByPID(pid int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if !processAliveImpl(pid) {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("drive9 umount: mount process pid %d still running after %s", pid, timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
