package fuse

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestSendRecvFd(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "test.sock")

	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()

	// Create a temp file to use as our fd.
	tmpFile, err := os.CreateTemp(dir, "fd-test-*")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	if _, err := tmpFile.WriteString("hello fd"); err != nil {
		t.Fatalf("write: %v", err)
	}
	origFd := int(tmpFile.Fd())

	// Send fd in a goroutine.
	errCh := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = conn.Close() }()
		errCh <- sendFd(conn.(*net.UnixConn), origFd)
	}()

	// Receive fd.
	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	recvdFd, err := recvFd(conn.(*net.UnixConn))
	if err != nil {
		t.Fatalf("recvFd: %v", err)
	}
	defer func() {
		// Don't close origFd — tmpFile.Close() handles that.
		// But do close the received dup.
		_ = os.NewFile(uintptr(recvdFd), "received").Close()
	}()

	if sendErr := <-errCh; sendErr != nil {
		t.Fatalf("sendFd: %v", sendErr)
	}

	// Verify the received fd can read the same file.
	recvFile := os.NewFile(uintptr(recvdFd), "received")
	if _, err := recvFile.Seek(0, 0); err != nil {
		t.Fatalf("seek: %v", err)
	}
	buf := make([]byte, 100)
	n, err := recvFile.Read(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got := string(buf[:n]); got != "hello fd" {
		t.Fatalf("read content: want %q, got %q", "hello fd", got)
	}
	_ = tmpFile.Close()
}
