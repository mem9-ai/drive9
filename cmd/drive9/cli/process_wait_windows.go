//go:build windows

package cli

import (
	"fmt"
	"math"
	"syscall"
	"time"
)

func processAliveImpl(pid int) bool {
	if pid <= 0 {
		return false
	}
	handle, err := syscall.OpenProcess(syscall.SYNCHRONIZE, false, uint32(pid))
	if err != nil {
		return false
	}
	defer syscall.CloseHandle(handle)
	status, err := syscall.WaitForSingleObject(handle, 0)
	if err != nil {
		return false
	}
	return status == syscall.WAIT_TIMEOUT
}

func waitForProcessExitByPID(pid int, timeout time.Duration) error {
	if pid <= 0 {
		return fmt.Errorf("invalid mount process pid %d", pid)
	}
	handle, err := syscall.OpenProcess(syscall.SYNCHRONIZE, false, uint32(pid))
	if err != nil {
		return nil
	}
	defer syscall.CloseHandle(handle)

	waitMillis := uint32(syscall.INFINITE)
	if timeout > 0 {
		millis := timeout / time.Millisecond
		if millis <= 0 {
			millis = 1
		}
		if millis > time.Duration(math.MaxUint32) {
			waitMillis = syscall.INFINITE
		} else {
			waitMillis = uint32(millis)
		}
	}

	status, err := syscall.WaitForSingleObject(handle, waitMillis)
	if err != nil {
		return fmt.Errorf("drive9 umount: wait for mount process pid %d exit: %w", pid, err)
	}
	switch status {
	case syscall.WAIT_OBJECT_0:
		return nil
	case syscall.WAIT_TIMEOUT:
		return fmt.Errorf("drive9 umount: mount process pid %d still running after %s", pid, timeout)
	default:
		return fmt.Errorf("drive9 umount: wait for mount process pid %d exit returned status %d", pid, status)
	}
}
