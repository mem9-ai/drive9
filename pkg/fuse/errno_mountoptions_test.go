package fuse

import (
	"runtime"
	"testing"
)

// Port of v1's TestNewGoFuseMountOptionsDefaultPermissions: locks the Linux
// mount contract from #1006/#1020 — mounts pass --allow-other, whose
// default_permissions binding provides the kernel-side ext4-parity EACCES.
func TestNewGoFuseMountOptionsDefaultPermissions(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("mount options differ per platform; asserted on linux")
	}
	opts := newGoFuseMountOptions(MountConfig{AllowOther: true})
	found := false
	for _, o := range opts.Options {
		if o == "default_permissions" {
			found = true
		}
	}
	if !found {
		t.Errorf("AllowOther=true: default_permissions missing from %v", opts.Options)
	}

	bare := newGoFuseMountOptions(MountConfig{})
	for _, o := range bare.Options {
		if o == "default_permissions" {
			t.Errorf("AllowOther=false: default_permissions unexpectedly present in %v", bare.Options)
		}
	}
	opts2 := newGoFuseMountOptions(MountConfig{AllowOther: true, ReadOnly: true})
	if !opts2.AllowOther {
		t.Errorf("AllowOther flag not propagated")
	}
	foundRo := false
	for _, o := range opts2.Options {
		if o == "ro" {
			foundRo = true
		}
	}
	if !foundRo {
		t.Errorf("ReadOnly=true: ro missing from %v", opts2.Options)
	}
}
