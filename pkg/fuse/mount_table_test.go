package fuse

import (
	"errors"
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
	candidates, conclusive := mountTableCandidates(dir)
	if !conclusive {
		t.Fatal("resolution of an existing plain path must be conclusive")
	}
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
	candidates, conclusive = mountTableCandidates(link)
	if !conclusive {
		t.Fatal("resolution of an existing symlink must be conclusive")
	}
	if !candidates[realResolved] {
		t.Fatalf("candidates %v missing resolved %q", candidates, realResolved)
	}
}

// TestMountTableCandidatesResolutionFailureIsInconclusive guards #928: when
// symlink resolution fails or times out, the candidate set may be missing
// the spelling the kernel actually lists, so the result must be marked
// inconclusive (callers fail closed on a non-match). A missing path is the
// exception — it cannot hide a symlink spelling — and stays conclusive.
func TestMountTableCandidatesResolutionFailureIsInconclusive(t *testing.T) {
	link := filepath.Join(t.TempDir(), "mp")
	if err := os.Symlink(filepath.Join(t.TempDir(), "target"), link); err != nil {
		t.Skipf("symlink: %v", err)
	}
	stubEvalSymlinks(t, func(p string) (string, error) {
		return "", errors.New("stat: endpoint wedged")
	})
	candidates, conclusive := mountTableCandidates(link)
	if conclusive {
		t.Fatal("wedged resolution must be inconclusive")
	}
	if len(candidates) != 1 || !candidates[filepath.Clean(link)] {
		t.Fatalf("candidates = %v, want only the unresolved abs spelling", candidates)
	}

	stubEvalSymlinks(t, func(string) (string, error) {
		return "", os.ErrNotExist
	})
	candidates, conclusive = mountTableCandidates(link)
	if !conclusive {
		t.Fatal("ENOENT resolution is conclusive: no symlink spelling can exist")
	}
	if len(candidates) != 1 || !candidates[filepath.Clean(link)] {
		t.Fatalf("candidates = %v, want only the unresolved abs spelling", candidates)
	}
}

func stubEvalSymlinks(t *testing.T, fn func(string) (string, error)) {
	t.Helper()
	old := evalSymlinks
	evalSymlinks = fn
	t.Cleanup(func() { evalSymlinks = old })
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

// TestKernelMountTableHasUnresolvedSpellingFailsClosed guards the #928
// symlink-spelling hole: when the kernel lists the post-symlink dentry path
// but resolution could not compute it, a non-match must still report the
// mount as listed instead of a false clearance.
func TestKernelMountTableHasUnresolvedSpellingFailsClosed(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("mountinfo-backed check is Linux-only")
	}
	target := t.TempDir()
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink: %v", err)
	}
	fixture := t.TempDir()
	// The kernel table lists the post-symlink dentry path only.
	line := "36 35 98:0 /mnt1 " + target + " rw,relatime - fuse.drive9 fuse.drive9 rw,user_id=1000,group_id=1000\n"
	if err := os.WriteFile(filepath.Join(fixture, "mountinfo"), []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	old := procMountInfoPath
	procMountInfoPath = filepath.Join(fixture, "mountinfo")
	t.Cleanup(func() { procMountInfoPath = old })

	// Wedged resolution: only the unresolved link spelling is available, it
	// does not match the listed target path — must fail closed.
	stubEvalSymlinks(t, func(string) (string, error) {
		return "", errors.New("stat: endpoint wedged")
	})
	listed, err := kernelMountTableHas(link)
	if err != nil {
		t.Fatalf("kernelMountTableHas: %v", err)
	}
	if !listed {
		t.Fatal("inconclusive resolution with no candidate match must report still listed")
	}
	if !KernelMountTableHas(link) {
		t.Fatal("KernelMountTableHas must fail closed on inconclusive resolution")
	}

	// ENOENT is conclusive (no symlink spelling can exist): a no-match is a
	// real clearance.
	stubEvalSymlinks(t, func(string) (string, error) {
		return "", os.ErrNotExist
	})
	listed, err = kernelMountTableHas(link)
	if err != nil {
		t.Fatalf("kernelMountTableHas: %v", err)
	}
	if listed {
		t.Fatal("conclusive ENOENT resolution with no candidate match should report not listed")
	}
}

// TestMountTableProbeFailedTimeoutFailsClosedWithoutReprobe: a bounded-probe
// timeout must not be retried — a wedged stat times out again and a
// recovered endpoint could flip the conservative answer.
func TestMountTableProbeFailedTimeoutFailsClosedWithoutReprobe(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("mountTableProbeFailed is only wired on the stat-probe platforms")
	}
	old := activeMountProbe
	calls := 0
	activeMountProbe = func(string) (bool, error) {
		calls++
		return false, errActiveMountProbeTimeout
	}
	t.Cleanup(func() { activeMountProbe = old })

	// Pre-seed the counter: kernelMountTableHas on these platforms runs the
	// probe once; the timeout must not trigger a second run.
	if !mountTableProbeFailed("/mnt/drive9", errActiveMountProbeTimeout) {
		t.Fatal("probe timeout must conservatively report still listed")
	}
	if calls != 0 {
		t.Fatalf("bounded probe re-run %d times after a timeout, want 0", calls)
	}
}
