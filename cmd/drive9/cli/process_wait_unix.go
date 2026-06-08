//go:build !windows

package cli

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/mem9-ai/dat9/pkg/mountstate"
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

func terminateMountProcess(state mountstate.ProcessState, waitTimeout time.Duration) error {
	if state.PID <= 0 {
		return fmt.Errorf("invalid mount process pid %d", state.PID)
	}
	if state.MountKind == mountstate.MountKindWebDAV {
		if state.CreationTime == 0 {
			return fmt.Errorf("%w: mount pid file for pid %d is missing ownership metadata", errMountProcessStateUnsafe, state.PID)
		}
		actualCreationTime, err := processCreationTimeByPID(state.PID)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
				return fmt.Errorf("%w: mount process pid %d is no longer running", errMountProcessStateStale, state.PID)
			}
			return fmt.Errorf("drive9 umount: inspect mount process pid %d: %w", state.PID, err)
		}
		if actualCreationTime == 0 {
			return fmt.Errorf("%w: mount process pid %d has no ownership metadata", errMountProcessStateUnsafe, state.PID)
		}
		if actualCreationTime != state.CreationTime {
			return fmt.Errorf("%w: mount process pid %d no longer matches recorded ownership", errMountProcessStateStale, state.PID)
		}
	}
	return terminateProcess(state.PID, waitTimeout)
}
