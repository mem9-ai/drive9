//go:build windows

package extent

// POSIX lock types in the JuiceFS meta protocol, using the Windows values from
// JuiceFS's pkg/meta/utils_windows.go (F_UNLCK=1, F_WRLCK=3). The Windows CLI
// never runs the extent data plane; this only keeps pkg/extent compiling.
const (
	memLockUnlock uint32 = 1
	memLockWrite  uint32 = 3
)
