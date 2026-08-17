//go:build windows

package mountsupervisor

import (
	"fmt"
	"os"

	"github.com/mem9-ai/drive9/pkg/mountstate"
)

func acquireLock(mountPoint string) (*os.File, error) {
	path := mountstate.SupervisorLockPath(mountPoint)
	if err := mountstate.EnsureStateDir(); err != nil {
		return nil, fmt.Errorf("mountsupervisor: state dir: %w", err)
	}
	// Exclusive create-ish lock: open with exclusive share mode is not portable
	// here; best-effort open is enough for Windows non-FUSE builds.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("mountsupervisor: open lock: %w", err)
	}
	return f, nil
}

// TryExclusive acquires the lock file and runs fn. Windows has no flock;
// exclusive create-ish open is best-effort.
func TryExclusive(mountPoint string, fn func() error) (acquired bool, err error) {
	f, err := acquireLock(mountPoint)
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()
	if fn != nil {
		return true, fn()
	}
	return true, nil
}
