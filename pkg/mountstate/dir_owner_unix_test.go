//go:build !windows

package mountstate

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestEnsureStateDirRejectsSymlink(t *testing.T) {
	base := t.TempDir()
	real := filepath.Join(base, "real")
	if err := os.MkdirAll(real, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_RUNTIME_DIR", link)
	if err := ensureStateDir(); err == nil {
		t.Fatal("expected symlink reject")
	}
}

type fakeDirInfo struct {
	mode os.FileMode
	sys  any
}

func (f fakeDirInfo) Name() string       { return "fake" }
func (f fakeDirInfo) Size() int64        { return 0 }
func (f fakeDirInfo) Mode() os.FileMode  { return f.mode }
func (f fakeDirInfo) ModTime() time.Time { return time.Time{} }
func (f fakeDirInfo) IsDir() bool        { return f.mode.IsDir() }
func (f fakeDirInfo) Sys() any           { return f.sys }

func TestCheckStateDirInfoRejectsWrongOwner(t *testing.T) {
	self := os.Getuid()
	okInfo := fakeDirInfo{
		mode: os.ModeDir | 0o700,
		sys:  &syscall.Stat_t{Uid: uint32(self)},
	}
	if err := checkStateDirInfo("/tmp/fake", okInfo); err != nil {
		t.Fatalf("matching owner: %v", err)
	}
	badInfo := fakeDirInfo{
		mode: os.ModeDir | 0o700,
		sys:  &syscall.Stat_t{Uid: uint32(self + 1)},
	}
	if err := checkStateDirInfo("/tmp/fake", badInfo); err == nil {
		t.Fatal("wrong owner must fail closed")
	}
}
