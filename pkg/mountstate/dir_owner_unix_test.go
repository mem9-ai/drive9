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

func TestCheckTrustedFileInfoRejectsWrongOwnerUID(t *testing.T) {
	self := os.Getuid()
	ok := fakeDirInfo{
		mode: 0o600, // regular file
		sys:  &syscall.Stat_t{Uid: uint32(self)},
	}
	// Mode without ModeDir → IsDir false; Mode() without ModeType regular?
	// os.FileMode for regular file is just perm bits; IsRegular is ModeType==0.
	if err := checkTrustedFileInfo("/tmp/f", ok); err != nil {
		t.Fatalf("matching owner regular: %v", err)
	}
	bad := fakeDirInfo{
		mode: 0o600,
		sys:  &syscall.Stat_t{Uid: uint32(self + 1)},
	}
	if err := checkTrustedFileInfo("/tmp/f", bad); err == nil {
		t.Fatal("wrong owner state file must fail closed")
	}
	wide := fakeDirInfo{
		mode: 0o666,
		sys:  &syscall.Stat_t{Uid: uint32(self)},
	}
	if err := checkTrustedFileInfo("/tmp/f", wide); err == nil {
		t.Fatal("world-writable state file must fail closed")
	}
}

func TestReadSupervisorStateRejectsUntrustedLegacy(t *testing.T) {
	mp := filepath.Join(t.TempDir(), "mnt")
	t.Setenv("XDG_RUNTIME_DIR", filepath.Join(t.TempDir(), "xdg"))
	// No UID-scoped state; only world-writable legacy.
	legacy := legacyTempStatePath(".supervise.json", mp)
	payload := []byte(`{"pid":1,"mount_point":"` + mp + `","state":"running"}` + "\n")
	if err := os.WriteFile(legacy, payload, 0o666); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(legacy, 0o666); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(legacy) })
	_, _, err := ReadSupervisorState(mp)
	if err == nil {
		t.Fatal("untrusted legacy supervise.json must be rejected")
	}
}
