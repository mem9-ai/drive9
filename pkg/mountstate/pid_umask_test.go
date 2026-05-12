//go:build !windows

package mountstate

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWritePIDHonorsUmask(t *testing.T) {
	mountPoint := filepath.Join(t.TempDir(), "mnt")
	oldUmask := setUmask(0o077)
	t.Cleanup(func() { _ = setUmask(oldUmask) })

	path, err := WritePID(mountPoint, 12345)
	if err != nil {
		t.Fatalf("WritePID: %v", err)
	}
	defer func() { _ = os.Remove(path) }()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat pid file: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("pid file mode = %o, want 600 with umask 077", got)
	}
}
