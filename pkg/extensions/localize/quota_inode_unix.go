//go:build !windows

package localize

import (
	iofs "io/fs"
	"os"
	"strconv"
	"syscall"
)

// fileIdentity is the (device, inode) pair that identifies one backing file
// through several hardlink names.
type fileIdentity struct {
	dev uint64
	ino uint64
}

// linkedFileIdentity reports the file's identity when it has more than one
// hardlink, so the usage walk can count its bytes once.
func linkedFileIdentity(info iofs.FileInfo) (fileIdentity, bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Nlink <= 1 {
		return fileIdentity{}, false
	}
	return fileIdentity{dev: uint64(st.Dev), ino: uint64(st.Ino)}, true
}

// fileReservationKey returns the reservation key for an open descriptor (its
// device+inode, so a rename or a hardlink alias keeps addressing the same
// file), with the path as the fallback when the platform exposes no identity.
func fileReservationKey(f *os.File, rel string) (string, bool) {
	info, err := f.Stat()
	if err != nil {
		return "", false
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "", false
	}
	return "ino:" + strconv.FormatUint(uint64(st.Dev), 10) + ":" + strconv.FormatUint(uint64(st.Ino), 10), true
}
