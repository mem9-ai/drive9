//go:build !windows

package extent

import "syscall"

// POSIX lock types in the JuiceFS meta protocol; see pkg/meta/utils_*.go in the
// fork. Windows has no syscall.F_*, so the values live in platform files.
const (
	memLockUnlock = uint32(syscall.F_UNLCK)
	memLockWrite  = uint32(syscall.F_WRLCK)
)
