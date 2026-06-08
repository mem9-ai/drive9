//go:build !windows && !darwin && !linux

package cli

import "fmt"

func processCreationTimeByPID(pid int) (uint64, error) {
	if pid <= 0 {
		return 0, fmt.Errorf("invalid mount process pid %d", pid)
	}
	return 0, fmt.Errorf("process creation time inspection is unsupported on this platform")
}
