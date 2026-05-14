package safety

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestCaseRemoteRoot(t *testing.T) {
	got, err := CaseRemoteRoot("/agent-run", "case-a")
	if err != nil {
		t.Fatal(err)
	}
	if got != "/agent-run/case-a" {
		t.Fatalf("root = %q", got)
	}
	if _, err := CaseRemoteRoot("/agent-run", "../bad"); !errors.Is(err, ErrInvalidGeneratedPath) {
		t.Fatalf("err = %v, want ErrInvalidGeneratedPath", err)
	}
}

func TestValidateMountpointAvailableRejectsNonEmpty(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "mnt")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "x"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := ValidateMountpointAvailable(dir); !errors.Is(err, ErrExistingMountpoint) {
		t.Fatalf("err = %v, want ErrExistingMountpoint", err)
	}
}
