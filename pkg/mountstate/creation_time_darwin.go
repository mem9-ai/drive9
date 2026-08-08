//go:build darwin

package mountstate

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// ProcessCreationTime returns a platform-specific process start identity for pid.
// On Darwin we use ps etime/lstart seconds via sysctl-friendly ps output.
func ProcessCreationTime(pid int) (uint64, error) {
	if pid <= 0 {
		return 0, fmt.Errorf("invalid pid %d", pid)
	}
	// %l is start time as number of seconds since epoch on modern macOS ps.
	out, err := exec.Command("ps", "-o", "lstart=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, err
	}
	// Hash of lstart string is not numeric; use /bin/ps -o etimes if available.
	out2, err2 := exec.Command("ps", "-o", "etimes=", "-p", strconv.Itoa(pid)).Output()
	if err2 == nil {
		s := strings.TrimSpace(string(out2))
		if s != "" {
			if v, err := strconv.ParseUint(s, 10, 64); err == nil {
				// etimes grows; invert-ish using a large base so identity is unique enough
				// for ownership checks within process lifetime. Better: use lstart string hash.
				_ = v
			}
		}
	}
	// Stable-enough identity: FNV of lstart text.
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
