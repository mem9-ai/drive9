//go:build !linux && !darwin && !windows

package mountstate

import "fmt"

// ProcessCreationTime is best-effort on unsupported platforms.
func ProcessCreationTime(pid int) (uint64, error) {
	if pid <= 0 {
		return 0, fmt.Errorf("invalid pid %d", pid)
	}
	return 0, nil
}
