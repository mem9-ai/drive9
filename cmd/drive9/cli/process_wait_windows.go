//go:build windows

package cli

import (
	"errors"
	"fmt"
	"math"
	"time"

	"golang.org/x/sys/windows"

	"github.com/mem9-ai/dat9/pkg/mountstate"
)

const processQueryLimitedInformation = 0x1000

func processAliveImpl(pid int) bool {
	if pid <= 0 {
		return false
	}
	handle, err := windows.OpenProcess(windows.SYNCHRONIZE|processQueryLimitedInformation, false, uint32(pid))
	if err != nil {
		return false
	}
	defer windows.CloseHandle(handle)
	status, err := windows.WaitForSingleObject(handle, 0)
	if err != nil {
		return false
	}
	return status == uint32(windows.WAIT_TIMEOUT)
}

func waitForProcessExitByPID(pid int, timeout time.Duration) error {
	if pid <= 0 {
		return fmt.Errorf("invalid mount process pid %d", pid)
	}
	handle, err := windows.OpenProcess(windows.SYNCHRONIZE|processQueryLimitedInformation, false, uint32(pid))
	if err != nil {
		return nil
	}
	defer windows.CloseHandle(handle)

	waitMillis := uint32(windows.INFINITE)
	if timeout > 0 {
		millis := timeout / time.Millisecond
		if millis <= 0 {
			millis = 1
		}
		if millis > time.Duration(math.MaxUint32) {
			waitMillis = windows.INFINITE
		} else {
			waitMillis = uint32(millis)
		}
	}

	status, err := windows.WaitForSingleObject(handle, waitMillis)
	if err != nil {
		return fmt.Errorf("drive9 umount: wait for mount process pid %d exit: %w", pid, err)
	}
	switch status {
	case windows.WAIT_OBJECT_0:
		return nil
	case uint32(windows.WAIT_TIMEOUT):
		return fmt.Errorf("drive9 umount: mount process pid %d still running after %s", pid, timeout)
	default:
		return fmt.Errorf("drive9 umount: wait for mount process pid %d exit returned status %d", pid, status)
	}
}

func processCreationTimeByPID(pid int) (uint64, error) {
	if pid <= 0 {
		return 0, fmt.Errorf("invalid mount process pid %d", pid)
	}
	handle, err := windows.OpenProcess(processQueryLimitedInformation, false, uint32(pid))
	if err != nil {
		return 0, err
	}
	defer windows.CloseHandle(handle)

	var creationTime, exitTime, kernelTime, userTime windows.Filetime
	if err := windows.GetProcessTimes(handle, &creationTime, &exitTime, &kernelTime, &userTime); err != nil {
		return 0, err
	}
	return uint64(creationTime.HighDateTime)<<32 | uint64(creationTime.LowDateTime), nil
}

func terminateMountProcess(state mountstate.ProcessState, waitTimeout time.Duration) error {
	if state.PID <= 0 {
		return fmt.Errorf("invalid mount process pid %d", state.PID)
	}
	if state.CreationTime == 0 {
		return fmt.Errorf("%w: mount pid file for pid %d is missing ownership metadata", errMountProcessStateUnsafe, state.PID)
	}

	handle, err := windows.OpenProcess(windows.SYNCHRONIZE|windows.PROCESS_TERMINATE|processQueryLimitedInformation, false, uint32(state.PID))
	if err != nil {
		return fmt.Errorf("%w: mount process pid %d is no longer running", errMountProcessStateStale, state.PID)
	}
	defer windows.CloseHandle(handle)

	var creationTime, exitTime, kernelTime, userTime windows.Filetime
	if err := windows.GetProcessTimes(handle, &creationTime, &exitTime, &kernelTime, &userTime); err != nil {
		return fmt.Errorf("drive9 umount: inspect mount process pid %d: %w", state.PID, err)
	}
	actualCreationTime := uint64(creationTime.HighDateTime)<<32 | uint64(creationTime.LowDateTime)
	if actualCreationTime != state.CreationTime {
		return fmt.Errorf("%w: mount process pid %d no longer matches recorded ownership", errMountProcessStateStale, state.PID)
	}

	status, err := windows.WaitForSingleObject(handle, 0)
	if err == nil && status == uint32(windows.WAIT_OBJECT_0) {
		return nil
	}

	err = windows.TerminateProcess(handle, 1)
	if err != nil {
		if errors.Is(err, windows.ERROR_ACCESS_DENIED) {
			status, waitErr := windows.WaitForSingleObject(handle, 0)
			if waitErr == nil && status == uint32(windows.WAIT_OBJECT_0) {
				return nil
			}
		}
		return fmt.Errorf("drive9 umount: terminate mount process pid %d: %w", state.PID, err)
	}
	if waitTimeout > 0 {
		return waitForProcessExitHandle(handle, state.PID, waitTimeout)
	}
	return nil
}

func waitForProcessExitHandle(handle windows.Handle, pid int, timeout time.Duration) error {
	waitMillis := uint32(windows.INFINITE)
	if timeout > 0 {
		millis := timeout / time.Millisecond
		if millis <= 0 {
			millis = 1
		}
		if millis > time.Duration(math.MaxUint32) {
			waitMillis = windows.INFINITE
		} else {
			waitMillis = uint32(millis)
		}
	}

	status, err := windows.WaitForSingleObject(handle, waitMillis)
	if err != nil {
		return fmt.Errorf("drive9 umount: wait for mount process pid %d exit: %w", pid, err)
	}
	switch status {
	case windows.WAIT_OBJECT_0:
		return nil
	case uint32(windows.WAIT_TIMEOUT):
		return fmt.Errorf("drive9 umount: mount process pid %d still running after %s", pid, timeout)
	default:
		return fmt.Errorf("drive9 umount: wait for mount process pid %d exit returned status %d", pid, status)
	}
}
