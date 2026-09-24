package fuse

import "testing"

func TestValidateMountPlatformRejectsWindowsLocalState(t *testing.T) {
	tests := []struct {
		name string
		opts *MountOptions
	}{
		{name: "gate off local root", opts: &MountOptions{LocalRoot: `C:\\drive9\\local`}},
		{name: "promotion enabled", opts: &MountOptions{EnableSynchronousPromotion: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateMountPlatform("windows", tt.opts); err == nil {
				t.Fatal("validateMountPlatform windows error = nil, want unsupported")
			}
		})
	}
}

func TestValidateMountPlatformAllowsSupportedConfigurations(t *testing.T) {
	tests := []struct {
		goos string
		opts *MountOptions
	}{
		{goos: "windows", opts: &MountOptions{}},
		{goos: "linux", opts: &MountOptions{LocalRoot: "/tmp/drive9-local"}},
		{goos: "darwin", opts: &MountOptions{LocalRoot: "/tmp/drive9-local", EnableSynchronousPromotion: true}},
	}
	for _, tt := range tests {
		if err := validateMountPlatform(tt.goos, tt.opts); err != nil {
			t.Fatalf("validateMountPlatform(%q, %+v) error = %v", tt.goos, tt.opts, err)
		}
	}
}
