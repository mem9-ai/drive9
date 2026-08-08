//go:build windows

package mountsupervisor

import (
	"fmt"
	"os"

	"github.com/mem9-ai/drive9/pkg/mountstate"
)

func acquireLock(mountPoint string) (*os.File, error) {
	path := mountstate.SupervisorLockPath(mountPoint)
	// Exclusive create-ish lock: open with exclusive share mode is not portable
	// here; best-effort open is enough for Windows non-FUSE builds.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("mountsupervisor: open lock: %w", err)
	}
	return f, nil
}
