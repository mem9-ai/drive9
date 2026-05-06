package mountstate

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestPIDFilePathCanonicalizesMountPoint(t *testing.T) {
	dir := t.TempDir()
	oldwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	defer func() {
		if err := os.Chdir(oldwd); err != nil {
			t.Fatalf("restore wd: %v", err)
		}
	}()
	if err := os.Mkdir(filepath.Join(dir, "mnt"), 0o755); err != nil {
		t.Fatalf("mkdir mountpoint: %v", err)
	}

	relPath := PIDFilePath("mnt/../mnt")
	absPath := PIDFilePath(filepath.Join(dir, "mnt"))
	if relPath != absPath {
		t.Fatalf("PIDFilePath relative = %q, absolute = %q", relPath, absPath)
	}
	if !strings.HasPrefix(relPath, os.TempDir()) {
		t.Fatalf("PIDFilePath = %q, want temp-dir path", relPath)
	}
}

func TestWriteReadPID(t *testing.T) {
	mountPoint := filepath.Join(t.TempDir(), "mnt")
	pid := 12345

	path, err := WritePID(mountPoint, pid)
	if err != nil {
		t.Fatalf("WritePID: %v", err)
	}
	defer func() { _ = os.Remove(path) }()

	got, gotPath, err := ReadPID(mountPoint)
	if err != nil {
		t.Fatalf("ReadPID: %v", err)
	}
	if got != pid {
		t.Fatalf("ReadPID pid = %d, want %d", got, pid)
	}
	if gotPath != path {
		t.Fatalf("ReadPID path = %q, want %q", gotPath, path)
	}
}

func TestReadPIDRejectsInvalidFile(t *testing.T) {
	mountPoint := filepath.Join(t.TempDir(), "mnt")
	path := PIDFilePath(mountPoint)
	if err := os.WriteFile(path, []byte("not-a-pid\n"), 0o644); err != nil {
		t.Fatalf("write pid file: %v", err)
	}
	defer func() { _ = os.Remove(path) }()

	_, _, err := ReadPID(mountPoint)
	if err == nil {
		t.Fatal("expected error for invalid pid file")
	}
	if strings.Contains(err.Error(), strconv.Itoa(os.Getpid())) {
		t.Fatalf("ReadPID error = %v, unexpectedly used current pid", err)
	}
}
