//go:build !windows

package mountstate

import (
	"os"
	"syscall"
)

func stateDirOwnerUID(info os.FileInfo) (int, bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return int(st.Uid), true
}
