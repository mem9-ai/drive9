package fuse

import "testing"

// Locks the Linux mount contract from #1006/#1020 — mounts pass --allow-other,
// whose default_permissions binding provides the kernel-side ext4-parity EACCES.
// The platform table in TestAllowOtherKernelEnforcementContract asserts the same
// option for every branch; this keeps the Linux-only expectations explicit.
func TestNewGoFuseMountOptionsDefaultPermissions(t *testing.T) {
	linux := newGoFuseMountOptionsFor("linux", MountConfig{AllowOther: true})
	if !hasMountOptionForTest(linux.Options, "default_permissions") {
		t.Errorf("linux AllowOther=true: default_permissions missing from %v", linux.Options)
	}
	bare := newGoFuseMountOptionsFor("linux", MountConfig{})
	if hasMountOptionForTest(bare.Options, "default_permissions") {
		t.Errorf("linux AllowOther=false: default_permissions unexpectedly present in %v", bare.Options)
	}
	readOnly := newGoFuseMountOptionsFor("linux", MountConfig{AllowOther: true, ReadOnly: true})
	if !readOnly.AllowOther {
		t.Error("AllowOther flag not propagated")
	}
	if !hasMountOptionForTest(readOnly.Options, "ro") {
		t.Errorf("linux ReadOnly=true: ro missing from %v", readOnly.Options)
	}
}
