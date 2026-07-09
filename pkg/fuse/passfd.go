package fuse

import (
	"fmt"
	"net"
	"syscall"
)

// sendFd sends a file descriptor over a Unix domain socket connection using
// SCM_RIGHTS. The fd is not closed by this function; the caller retains
// ownership.
func sendFd(conn *net.UnixConn, fd int) error {
	rights := syscall.UnixRights(fd)
	// We must send at least one byte of data alongside the control message.
	_, _, err := conn.WriteMsgUnix([]byte{0}, rights, nil)
	if err != nil {
		return fmt.Errorf("sendfd: %w", err)
	}
	return nil
}

// recvFd receives a file descriptor from a Unix domain socket connection
// using SCM_RIGHTS. The caller owns the returned fd and must close it.
func recvFd(conn *net.UnixConn) (int, error) {
	buf := make([]byte, 1)
	// UnixRights builds a 4-byte payload per fd. Use a generous buffer for
	// the control message header + one fd.
	oob := make([]byte, 64) // enough for one SCM_RIGHTS fd on any platform
	_, oobn, _, _, err := conn.ReadMsgUnix(buf, oob)
	if err != nil {
		return -1, fmt.Errorf("recvfd: %w", err)
	}
	msgs, err := syscall.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return -1, fmt.Errorf("recvfd: parse control message: %w", err)
	}
	for _, msg := range msgs {
		fds, err := syscall.ParseUnixRights(&msg)
		if err != nil {
			continue
		}
		if len(fds) > 0 {
			// Close any extra fds beyond the first.
			for i := 1; i < len(fds); i++ {
				_ = syscall.Close(fds[i])
			}
			return fds[0], nil
		}
	}
	return -1, fmt.Errorf("recvfd: no fd in control message")
}
