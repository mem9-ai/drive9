//go:build !windows

package mountsupervisor

import (
	"fmt"
	"os"
	"syscall"

	"github.com/mem9-ai/drive9/pkg/mountstate"
)

func acquireLock(mountPoint string) (*os.File, error) {
	path := mountstate.SupervisorLockPath(mountPoint)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("mountsupervisor: open lock: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("mountsupervisor: another supervisor already holds %s", mountPoint)
	}
	return f, nil
}
