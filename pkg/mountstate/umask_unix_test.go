//go:build !windows

package mountstate

import "syscall"

func setUmask(mask int) int {
	return syscall.Umask(mask)
}
