//go:build windows

package mountstate

import (
	"fmt"

	"golang.org/x/sys/windows"
)

// ProcessCreationTime returns a Windows process start identity (FILETIME ticks).
func ProcessCreationTime(pid int) (uint64, error) {
	if pid <= 0 {
		return 0, fmt.Errorf("invalid pid %d", pid)
	}
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		return 0, err
	}
	defer func() { _ = windows.CloseHandle(handle) }()

	var creationTime, exitTime, kernelTime, userTime windows.Filetime
	if err := windows.GetProcessTimes(handle, &creationTime, &exitTime, &kernelTime, &userTime); err != nil {
		return 0, err
	}
	v := uint64(creationTime.HighDateTime)<<32 | uint64(creationTime.LowDateTime)
	if v == 0 {
		return 0, fmt.Errorf("inspect pid %d: empty creation time", pid)
	}
	return v, nil
}
