//go:build windows

package localize

import iofs "io/fs"

// fileIdentity is unused on Windows: the platform exposes no stable inode
// identity through FileInfo, so the usage walk counts every name.
type fileIdentity struct {
	dev uint64
	ino uint64
}

// linkedFileIdentity always reports "no shared identity", so a hardlinked
// file is counted once per name (the conservative direction for a quota).
func linkedFileIdentity(iofs.FileInfo) (fileIdentity, bool) {
	return fileIdentity{}, false
}

// fileReservationKey always reports "no identity" on Windows: FileInfo exposes
// no stable file identity there, so the path key is used.
func fileReservationKey(iofs.FileInfo) (string, bool) { return "", false }
