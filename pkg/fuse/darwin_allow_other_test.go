package fuse

import (
	"os"
	"slices"
	"testing"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// A shared macOS mount needs the kernel to check mode bits. macFUSE arms that check
// only when the mount passes default_permissions: without it every authorization is
// forwarded to this daemon as an ACCESS message, and the daemon's group class reads
// the caller's supplementary groups from /proc/<pid>/status — which macOS does not
// have — so it cannot tell a member from a non-member. Passing defer_permissions as
// well (the pre-fix shape) left the kernel out entirely, and the daemon gate was
// skipped whenever --allow-other was set, so nothing enforced the mode bits.
//
// A private mount is different: the kernel already restricts it to the mounting user,
// and defer_permissions keeps the per-operation ACCESS upcall (and its chain stat) off
// the hot path.
func TestDarwinAllowOtherArmsKernelPermissions(t *testing.T) {
	shared := newGoFuseMountOptionsFor("darwin", MountConfig{AllowOther: true})
	if !slices.Contains(shared.Options, "default_permissions") {
		t.Fatalf("darwin allowOther: default_permissions missing from %v; macFUSE would forward every check to FS.Access", shared.Options)
	}
	if slices.Contains(shared.Options, "defer_permissions") {
		t.Fatalf("darwin allowOther: defer_permissions present in %v; the kernel would not check", shared.Options)
	}
	if !slices.Contains(shared.Options, "local") {
		t.Fatalf("darwin allowOther: local option missing from %v", shared.Options)
	}

	private := newGoFuseMountOptionsFor("darwin", MountConfig{})
	if slices.Contains(private.Options, "default_permissions") {
		t.Fatalf("darwin private: default_permissions present in %v", private.Options)
	}
	if !slices.Contains(private.Options, "defer_permissions") || !slices.Contains(private.Options, "local") {
		t.Fatalf("darwin private: options = %v, want defer_permissions and local", private.Options)
	}
}

// The Linux contract is unchanged, and the daemon gate is derived from the same option
// set the mount uses, so the two cannot drift apart.
func TestAllowOtherKernelEnforcementContract(t *testing.T) {
	cases := []struct {
		goos       string
		allowOther bool
		want       bool
	}{
		{"linux", true, true},
		{"linux", false, false},
		{"darwin", true, true},
		{"darwin", false, false},
		{"freebsd", true, false},
		{"windows", true, false},
	}
	for _, tc := range cases {
		opts := newGoFuseMountOptionsFor(tc.goos, MountConfig{AllowOther: tc.allowOther})
		if got := slices.Contains(opts.Options, "default_permissions"); got != tc.want {
			t.Fatalf("%s allowOther=%t: default_permissions present=%t, want %t (options %v)", tc.goos, tc.allowOther, got, tc.want, opts.Options)
		}
	}
}

// The daemon gate, where it is used, must not grant a group class from guessed
// membership: with /proc unavailable the answer stays "not a member", which is why
// macOS shared mounts rely on the kernel instead.
func TestPOSIXAccessDeniesGroupClassWithoutMembership(t *testing.T) {
	restore := readProcessStatusFile
	readProcessStatusFile = func(string) ([]byte, error) { return nil, os.ErrNotExist }
	t.Cleanup(func() { readProcessStatusFile = restore })

	caller := fs.Caller{Uid: 1001, Gid: 1002, Pid: 4242}
	if hasPOSIXAccess(caller, 1000, 2000, 0o640, 0b100) {
		t.Fatal("0640 file granted read to a caller outside the owning group")
	}
	if !hasPOSIXAccess(caller, 1000, 2000, 0o644, 0b100) {
		t.Fatal("0644 file denied read to other")
	}
	if !hasPOSIXAccess(caller, 1001, 2000, 0o600, 0b110) {
		t.Fatal("owner denied")
	}
}
