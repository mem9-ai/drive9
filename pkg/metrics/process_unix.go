//go:build unix

package metrics

import "syscall"

// maxFileDescriptors returns the soft RLIMIT_NOFILE on Unix platforms.
func maxFileDescriptors() (uint64, bool) {
	var rl syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl); err != nil {
		return 0, false
	}
	return uint64(rl.Cur), true
}
