//go:build !windows

package datastore

import "syscall"

// POSIX lock types in the JuiceFS meta protocol. Windows has no syscall.F_*,
// so the values live in platform files that mirror JuiceFS's own
// pkg/meta/utils_{unix,windows}.go convention.
const (
	jfsLockUnlock = uint32(syscall.F_UNLCK)
	jfsLockWrite  = uint32(syscall.F_WRLCK)
)
