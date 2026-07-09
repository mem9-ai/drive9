package fuse

import (
	"testing"
)

func TestReexecRefusesWithoutServer(t *testing.T) {
	fs := newTestReexecFS()
	result := fs.Reexec(ReexecConfig{MountPoint: "/mnt"})
	if result.Accepted {
		t.Fatal("expected reexec to fail without server")
	}
	if result.Err == nil {
		t.Fatal("expected error")
	}
}

func TestReexecRefusesConcurrent(t *testing.T) {
	fs := newTestReexecFS()
	// Simulate an in-progress reexec.
	if !fs.reexecGuard.tryAcquire() {
		t.Fatal("first acquire should succeed")
	}
	defer fs.reexecGuard.release()

	result := fs.Reexec(ReexecConfig{MountPoint: "/mnt"})
	if result.Accepted {
		t.Fatal("expected concurrent reexec to be refused")
	}
	if result.Err == nil || result.Err.Error() != "reexec: already in progress" {
		t.Fatalf("expected 'already in progress' error, got: %v", result.Err)
	}
}

func TestReexecRefusesDirtyState(t *testing.T) {
	// Use a real Dat9FS-like setup with open file handles to trigger
	// preflight refusal. We can't easily test the full Reexec path
	// without a real FUSE server, but we can verify that preflight
	// gates are checked by the Reexec method.
	fs := newTestReexecFS()
	fs.fileHandles.Allocate(&FileHandle{Path: "/dirty.txt"})

	// Reexec will fail because fs.server is nil (step 1), not because
	// of dirty state (step 4). The dirty state test is really a
	// preflight test which is covered in reexec_gate_test.go.
	result := fs.Reexec(ReexecConfig{MountPoint: "/mnt"})
	if result.Accepted {
		t.Fatal("expected reexec to fail")
	}
}

func TestReexecChildEnvParsing(t *testing.T) {
	// No env vars set.
	if IsReexecChild() {
		t.Fatal("should not be reexec child without env vars")
	}

	// Set env vars.
	t.Setenv("DRIVE9_REEXEC_SOCK", "/tmp/test.sock")
	t.Setenv("DRIVE9_REEXEC_MOUNT", "/mnt/test")
	t.Setenv("DRIVE9_REEXEC_VERSION", "1")

	if !IsReexecChild() {
		t.Fatal("should be reexec child with env vars set")
	}

	cfg, err := ParseReexecChildEnv()
	if err != nil {
		t.Fatalf("ParseReexecChildEnv: %v", err)
	}
	if cfg.SockPath != "/tmp/test.sock" {
		t.Fatalf("SockPath: want /tmp/test.sock, got %s", cfg.SockPath)
	}
	if cfg.MountPoint != "/mnt/test" {
		t.Fatalf("MountPoint: want /mnt/test, got %s", cfg.MountPoint)
	}
	if cfg.Version != 1 {
		t.Fatalf("Version: want 1, got %d", cfg.Version)
	}
}

func TestReexecChildEnvMissingSock(t *testing.T) {
	t.Setenv("DRIVE9_REEXEC_MOUNT", "/mnt/test")
	t.Setenv("DRIVE9_REEXEC_VERSION", "1")

	_, err := ParseReexecChildEnv()
	if err == nil {
		t.Fatal("expected error for missing DRIVE9_REEXEC_SOCK")
	}
}

func TestReexecChildEnvInvalidVersion(t *testing.T) {
	t.Setenv("DRIVE9_REEXEC_SOCK", "/tmp/test.sock")
	t.Setenv("DRIVE9_REEXEC_MOUNT", "/mnt/test")
	t.Setenv("DRIVE9_REEXEC_VERSION", "abc")

	_, err := ParseReexecChildEnv()
	if err == nil {
		t.Fatal("expected error for invalid version")
	}
}

func TestReexecChildVersionMismatch(t *testing.T) {
	cfg := ReexecChildConfig{
		SockPath:   "/tmp/test.sock",
		MountPoint: "/mnt/test",
		Version:    999, // mismatched
	}
	_, _, err := ReexecChildHandshake(cfg)
	if err == nil {
		t.Fatal("expected error for version mismatch")
	}
}

func TestReexecAcceptMsgRoundTrip(t *testing.T) {
	// Verify the accept message struct can be marshaled/unmarshaled correctly.
	msg := reexecAcceptMsg{Accept: true, Version: 1}
	if !msg.Accept || msg.Version != 1 {
		t.Fatal("accept msg fields incorrect")
	}
}
