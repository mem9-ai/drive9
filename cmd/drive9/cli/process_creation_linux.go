//go:build linux

package cli

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func processCreationTimeByPID(pid int) (uint64, error) {
	if pid <= 0 {
		return 0, fmt.Errorf("invalid mount process pid %d", pid)
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, err
	}
	stat := string(data)
	commEnd := strings.LastIndex(stat, ")")
	if commEnd < 0 || commEnd+2 >= len(stat) {
		return 0, fmt.Errorf("inspect mount process pid %d: malformed /proc stat", pid)
	}
	fields := strings.Fields(stat[commEnd+2:])
	if len(fields) <= 19 {
		return 0, fmt.Errorf("inspect mount process pid %d: short /proc stat", pid)
	}
	startTime, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("inspect mount process pid %d start time: %w", pid, err)
	}
	return startTime, nil
}
