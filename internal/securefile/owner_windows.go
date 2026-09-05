//go:build windows

package securefile

import "os"

func ownedByEffectiveUser(os.FileInfo) bool {
	return false
}
