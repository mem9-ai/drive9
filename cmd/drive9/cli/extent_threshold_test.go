package cli

import "testing"

// TestConsumeMountExtentLargeFileThreshold validates the takeover knob
// before it reaches the MiB→bytes shift: negatives and overflow-wrapping
// values are configuration errors on both the flag and env paths.
func TestConsumeMountExtentLargeFileThreshold(t *testing.T) {
	t.Setenv(EnvMountExtentLargeFileThresholdMB, "")
	if got, err := consumeMountExtentLargeFileThreshold(10); err != nil || got != 10 {
		t.Fatalf("flag default = %d %v", got, err)
	}
	if got, err := consumeMountExtentLargeFileThreshold(-1); err == nil || got != 0 {
		t.Fatalf("negative flag accepted: %d %v", got, err)
	}
	if got, err := consumeMountExtentLargeFileThreshold(int64(1) << 45); err == nil || got != 0 {
		t.Fatalf("overflowing flag accepted: %d %v", got, err)
	}
	t.Setenv(EnvMountExtentLargeFileThresholdMB, "off")
	if got, err := consumeMountExtentLargeFileThreshold(10); err != nil || got != 0 {
		t.Fatalf("off = %d %v", got, err)
	}
	t.Setenv(EnvMountExtentLargeFileThresholdMB, "4")
	if got, err := consumeMountExtentLargeFileThreshold(10); err != nil || got != 4 {
		t.Fatalf("env override = %d %v", got, err)
	}
	t.Setenv(EnvMountExtentLargeFileThresholdMB, "-3")
	if got, err := consumeMountExtentLargeFileThreshold(10); err == nil || got != 0 {
		t.Fatalf("negative env accepted: %d %v", got, err)
	}
	t.Setenv(EnvMountExtentLargeFileThresholdMB, "99999999999999999999")
	if got, err := consumeMountExtentLargeFileThreshold(10); err == nil || got != 0 {
		t.Fatalf("overflowing env accepted: %d %v", got, err)
	}
}
