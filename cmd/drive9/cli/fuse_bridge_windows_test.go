//go:build windows

package cli

import (
	"strings"
	"testing"
)

func TestMountVaultImplWindowsReportsCLIAlternative(t *testing.T) {
	err := mountVaultImpl(&vaultMountOptions{})
	if err == nil {
		t.Fatal("expected windows vault mount to be unsupported")
	}
	got := err.Error()
	if !strings.Contains(got, "drive9 vault commands") {
		t.Fatalf("error = %q, want drive9 vault command guidance", got)
	}
	if !strings.Contains(got, "vault API") {
		t.Fatalf("error = %q, want vault API guidance", got)
	}
}

func TestMountFuseImplWindowsReportsWebDAVAlternative(t *testing.T) {
	err := mountFuseImpl(&mountFuseOptions{})
	if err == nil {
		t.Fatal("expected windows fuse mount to be unsupported")
	}
	got := err.Error()
	if !strings.Contains(got, "drive9 mount") || !strings.Contains(got, "--mode=fuse") {
		t.Fatalf("error = %q, want explicit WebDAV mount guidance", got)
	}
	if !strings.Contains(got, "WebDAV") {
		t.Fatalf("error = %q, want WebDAV guidance", got)
	}
}