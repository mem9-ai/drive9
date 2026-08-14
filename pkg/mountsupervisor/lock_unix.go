//go:build !windows

package mountsupervisor

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/mem9-ai/drive9/pkg/mountstate"
)

func acquireLock(mountPoint string) (*os.File, error) {
	path := mountstate.SupervisorLockPath(mountPoint)
	// Fail closed on squatted / wrong-owner / unrepairable state directories.
	if err := mountstate.EnsureStateDir(); err != nil {
		return nil, fmt.Errorf("mountsupervisor: state dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("mountsupervisor: open lock: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		// EWOULDBLOCK/EAGAIN = contention; other errors are real lock failures
		// (unsupported FS, EINTR, etc.) and must not look like "already held".
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, fmt.Errorf("mountsupervisor: another supervisor already holds %s", mountPoint)
		}
		return nil, fmt.Errorf("mountsupervisor: lock %s: %w", path, err)
	}
	return f, nil
}

// TryExclusive acquires the supervisor flock non-blocking and runs fn only if
// this process owns the lock. acquired=false means another supervisor holds it
// (do not treat as an error). The lock is released after fn returns.
func TryExclusive(mountPoint string, fn func() error) (acquired bool, err error) {
	f, err := acquireLock(mountPoint)
	if err != nil {
		if strings.Contains(err.Error(), "already holds") {
			return false, nil
		}
		return false, err
	}
	defer func() { _ = f.Close() }()
	if fn != nil {
		return true, fn()
	}
	return true, nil
}
