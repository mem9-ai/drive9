//go:build !windows

package mountsupervisor

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/mem9-ai/drive9/pkg/mountstate"
)

func acquireLock(mountPoint string) (*os.File, error) {
	path := mountstate.SupervisorLockPath(mountPoint)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("mountsupervisor: create lock dir: %w", err)
	}
	_ = os.Chmod(filepath.Dir(path), 0o700)
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
