//go:build windows

package mountstate

import "os"

// stateDirOwnerUID is unavailable on Windows; skip ownership validation.
func stateDirOwnerUID(info os.FileInfo) (int, bool) {
	return 0, false
}
