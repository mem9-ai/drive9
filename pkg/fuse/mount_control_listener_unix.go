//go:build !windows

package fuse

import (
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"golang.org/x/sys/unix"

	"github.com/mem9-ai/drive9/pkg/mountstate"
)

// listenMountControl owns the pathname until the server has joined all
// handlers, removed its socket and closed lock. Never unlink a live listener;
// only ECONNREFUSED proves that a leftover Unix socket has no listener.
func listenMountControl(path string) (_ *net.UnixListener, _ *os.File, err error) {
	if err := mountstate.EnsureStateDir(); err != nil {
		return nil, nil, err
	}
	fd, err := unix.Open(path+".lock", unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0o600)
	if err != nil {
		return nil, nil, err
	}
	lock := os.NewFile(uintptr(fd), path+".lock")
	defer func() {
		if err != nil {
			_ = lock.Close()
		}
	}()
	info, err := lock.Stat()
	if err != nil {
		return nil, nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return nil, nil, errors.New("invalid mount control lock file")
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, nil, fmt.Errorf("mount control already owned: %w", err)
	}
	info, err = os.Lstat(path)
	if err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, nil, errors.New("mount control path is not a socket")
		}
		conn, dialErr := net.DialTimeout("unix", path, time.Second)
		if dialErr == nil {
			_ = conn.Close()
			return nil, nil, errors.New("mount control listener is still active")
		}
		if !errors.Is(dialErr, unix.ECONNREFUSED) {
			return nil, nil, fmt.Errorf("cannot establish stale mount control socket: %w", dialErr)
		}
		now, err := os.Lstat(path)
		if err != nil || !os.SameFile(info, now) {
			return nil, nil, errors.New("mount control socket changed during recovery")
		}
		if err := os.Remove(path); err != nil {
			return nil, nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, nil, err
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = listener.Close()
		return nil, nil, err
	}
	return listener, lock, nil
}
