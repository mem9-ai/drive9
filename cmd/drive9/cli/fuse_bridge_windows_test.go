//go:build windows

package cli

import (
	"strings"
	"testing"
)

func TestFsMountCmdWindowsFuseReportsUnsupportedBeforeCredentialResolution(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)

	err := fsMountCmd([]string{"--mode=fuse", `X:\\drive9`})
	if err == nil {
		t.Fatal("expected windows fuse mount to be unsupported")
	}
	got := err.Error()
	if !strings.Contains(got, "WebDAV") || !strings.Contains(got, "--mode=fuse") {
		t.Fatalf("error = %q, want explicit Windows WebDAV guidance", got)
	}
	if strings.Contains(got, "owner API key or delegated token required") {
		t.Fatalf("error = %q, should not surface credential resolution failure", got)
	}
	if strings.Contains(got, "drive9 server URL required") {
		t.Fatalf("error = %q, should not surface server resolution failure", got)
	}
}

func TestFsMountCmdWindowsFuseStillRejectsExplicitEmptyAPIKey(t *testing.T) {
	err := fsMountCmd([]string{"--mode=fuse", "--api-key=", `X:\\drive9`})
	if err == nil {
		t.Fatal("expected explicit empty api-key to fail")
	}
	got := err.Error()
	if !strings.Contains(got, "--api-key was given an empty value") {
		t.Fatalf("error = %q, want empty api-key validation", got)
	}
	if strings.Contains(got, "WebDAV") {
		t.Fatalf("error = %q, want empty-flag validation before Windows unsupported guidance", got)
	}
}

func TestVaultMountCmdWindowsReportsUnsupportedBeforeCredentialResolution(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)

	err := VaultMountCmd([]string{`X:\\drive9-vault`})
	if err == nil {
		t.Fatal("expected windows vault mount to be unsupported")
	}
	got := err.Error()
	if !strings.Contains(got, "drive9 vault commands") || !strings.Contains(got, "vault API") {
		t.Fatalf("error = %q, want Windows vault guidance", got)
	}
	if strings.Contains(got, "owner API key or delegated token required") {
		t.Fatalf("error = %q, should not surface credential resolution failure", got)
	}
	if strings.Contains(got, "drive9 server URL required") {
		t.Fatalf("error = %q, should not surface server resolution failure", got)
	}
}

func TestVaultMountCmdWindowsStillRejectsExplicitEmptyAPIKey(t *testing.T) {
	err := VaultMountCmd([]string{"--api-key=", `X:\\drive9-vault`})
	if err == nil {
		t.Fatal("expected explicit empty api-key to fail")
	}
	got := err.Error()
	if !strings.Contains(got, "--api-key was given an empty value") {
		t.Fatalf("error = %q, want empty api-key validation", got)
	}
	if strings.Contains(got, "vault API") {
		t.Fatalf("error = %q, want empty-flag validation before Windows unsupported guidance", got)
	}
}

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
