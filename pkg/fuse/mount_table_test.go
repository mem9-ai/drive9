package fuse

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestDecodeMountInfoField(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"/mnt/drive9", "/mnt/drive9"},
		{"/mnt/with\\040space", "/mnt/with space"},
		{"/mnt/with\\011tab", "/mnt/with\ttab"},
		{"/mnt/with\\012newline", "/mnt/with\nnewline"},
		{"/mnt/with\\134backslash", "/mnt/with\\backslash"},
		{"/mnt/trailing\\", "/mnt/trailing\\"},
		{"/mnt/short\\04", "/mnt/short\\04"},
		{"/mnt/\\134040escaped", "/mnt/\\040escaped"},
	}
	for _, tc := range cases {
		if got := decodeMountInfoField(tc.in); got != tc.want {
			t.Errorf("decodeMountInfoField(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestParseMountInfoMountPoints(t *testing.T) {
	data := []byte("36 35 98:0 /mnt1 /mnt/drive9 rw,relatime - fuse.drive9 fuse.drive9 rw,user_id=1000,group_id=1000\n" +
		"37 36 0:31 / /mnt/with\\040space rw - fuse.drive9 fuse.drive9 rw\n" +
		"malformed line\n" +
		"\n" +
		"38 35 0:32 / /mnt/other rw - ext4 /dev/sda1 rw\n")
	got := parseMountInfoMountPoints(data)
	want := []string{"/mnt/drive9", "/mnt/with space", "/mnt/other"}
	if len(got) != len(want) {
		t.Fatalf("parsed %d mount points (%v), want %d (%v)", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("mount point %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestMountTableCandidates(t *testing.T) {
	dir := t.TempDir()
	candidates := mountTableCandidates(dir)
	if !candidates[filepath.Clean(dir)] {
		t.Fatalf("candidates %v missing %q", candidates, dir)
	}

	// A symlinked path must map to its resolution too: the kernel reports the
	// mountpoint dentry path, not the symlink shortcut.
	real := filepath.Join(t.TempDir(), "real")
	if err := os.MkdirAll(real, 0o755); err != nil {
		t.Fatal(err)
	}
	realResolved, err := filepath.EvalSymlinks(real)
	if err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(real, link); err != nil {
		t.Skipf("symlink: %v", err)
	}
	candidates = mountTableCandidates(link)
	if !candidates[realResolved] {
		t.Fatalf("candidates %v missing resolved %q", candidates, realResolved)
	}
}

func TestKernelMountTableHasUnlistedPath(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("mountinfo-backed check is Linux-only")
	}
	listed, err := kernelMountTableHas(filepath.Join(t.TempDir(), "definitely-not-mounted"))
	if err != nil {
		t.Fatalf("kernelMountTableHas: %v", err)
	}
	if listed {
		t.Fatal("expected unlisted path to report not listed")
	}
}

func TestKernelMountTableHasFixture(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("mountinfo-backed check is Linux-only")
	}
	mp := t.TempDir()
	fixture := t.TempDir()
	line := "36 35 98:0 /mnt1 " + mp + " rw,relatime - fuse.drive9 fuse.drive9 rw,user_id=1000,group_id=1000\n"
	if err := os.WriteFile(filepath.Join(fixture, "mountinfo"), []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	old := procMountInfoPath
	procMountInfoPath = filepath.Join(fixture, "mountinfo")
	t.Cleanup(func() { procMountInfoPath = old })

	listed, err := kernelMountTableHas(mp)
	if err != nil {
		t.Fatalf("kernelMountTableHas: %v", err)
	}
	if !listed {
		t.Fatalf("expected %q to be listed via fixture", mp)
	}
	if KernelMountTableHas(mp) != true {
		t.Fatal("KernelMountTableHas should honor the fixture")
	}

	procMountInfoPath = filepath.Join(fixture, "missing")
	// An unreadable kernel table is indeterminate on Linux and must fail
	// closed: report still-listed so umount never forgives it as "cleared".
	if !KernelMountTableHas(mp) {
		t.Fatal("unreadable mount table should conservatively report still listed")
	}
}
