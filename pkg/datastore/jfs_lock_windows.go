//go:build windows

package datastore

// POSIX lock types in the JuiceFS meta protocol, using the Windows values from
// JuiceFS's pkg/meta/utils_windows.go (F_UNLCK=1, F_WRLCK=3). The Windows CLI
// never serves extent metadata; this only keeps pkg/datastore compiling.
const (
	jfsLockUnlock uint32 = 1
	jfsLockWrite  uint32 = 3
)
