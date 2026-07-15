//go:build linux

package fuse

import (
	"syscall"
	"testing"
)

func TestReadOnlyDirectMountFlags(t *testing.T) {
	want := uintptr(syscall.MS_NODEV | syscall.MS_NOSUID | syscall.MS_RDONLY)
	if got := readOnlyDirectMountFlags(); got != want {
		t.Fatalf("read-only direct mount flags = %#x, want %#x", got, want)
	}
}
