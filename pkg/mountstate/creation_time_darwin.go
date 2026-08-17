//go:build darwin

package mountstate

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// ProcessCreationTime returns a platform-specific process start identity for pid.
// On Darwin we use FNV-1a of `ps -o lstart=` (stable for the process lifetime).
func ProcessCreationTime(pid int) (uint64, error) {
	if pid <= 0 {
		return 0, fmt.Errorf("invalid pid %d", pid)
	}
	out, err := exec.Command("ps", "-o", "lstart=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, err
	}
	lstart := strings.TrimSpace(string(out))
	if lstart == "" {
		return 0, fmt.Errorf("inspect pid %d: empty lstart", pid)
	}
	var h uint64 = 14695981039346656037
	for i := 0; i < len(lstart); i++ {
		h ^= uint64(lstart[i])
		h *= 1099511628211
	}
	if h == 0 {
		h = 1
	}
	return h, nil
}
