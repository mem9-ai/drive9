//go:build linux

package cli

import (
	"testing"

	"golang.org/x/sys/unix"
)

func TestHardenMountCredentialProcessMakesLinuxProcessNonDumpable(t *testing.T) {
	previous, err := unix.PrctlRetInt(unix.PR_GET_DUMPABLE, 0, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Prctl(unix.PR_SET_DUMPABLE, uintptr(previous), 0, 0, 0) })
	if err := hardenMountCredentialProcess(); err != nil {
		t.Fatal(err)
	}
	current, err := unix.PrctlRetInt(unix.PR_GET_DUMPABLE, 0, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if current != 0 {
		t.Fatalf("dumpable = %d, want 0", current)
	}
}
