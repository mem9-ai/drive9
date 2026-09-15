package fuse

import (
	"path/filepath"
	"strconv"
	"strings"
)

// procMountInfoPath is the authoritative kernel mount table on Linux. Tests
// redirect it to a fixture file; other platforms never read it.
var procMountInfoPath = "/proc/self/mountinfo"

// mountTableCandidates returns the paths a kernel mount-table entry may spell
// for mountPoint: the cleaned absolute path and its symlink resolution (the
// kernel reports the mountpoint dentry path, without symlink shortcuts).
func mountTableCandidates(mountPoint string) map[string]bool {
	abs, err := filepath.Abs(mountPoint)
	if err != nil {
		return nil
	}
	abs = filepath.Clean(abs)
	candidates := map[string]bool{abs: true}
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		if resolved = filepath.Clean(resolved); resolved != abs {
			candidates[resolved] = true
		}
	}
	return candidates
}

// parseMountInfoMountPoints returns the decoded mount-point field (field 5)
// of every /proc/self/mountinfo line. Raw spaces never occur inside
// mountinfo fields — they are always octal-escaped — so plain field splitting
// is safe.
func parseMountInfoMountPoints(data []byte) []string {
	var mounts []string
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		mounts = append(mounts, decodeMountInfoField(fields[4]))
	}
	return mounts
}

// decodeMountInfoField decodes the kernel's octal escaping used in
// /proc/self/mountinfo fields: \040 (space), \011 (tab), \012 (newline),
// \134 (backslash).
func decodeMountInfoField(s string) string {
	if !strings.ContainsRune(s, '\\') {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		if s[i] == '\\' && i+3 < len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(v))
				i += 4
				continue
			}
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}
