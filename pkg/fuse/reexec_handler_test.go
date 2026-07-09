package fuse

import (
	"encoding/json"
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

func TestReexecGuardAcquireReleaseCycle(t *testing.T) {
	fs := newTestReexecFS()
	// First acquire succeeds.
	if !fs.reexecGuard.tryAcquire() {
		t.Fatal("first acquire should succeed")
	}
	// Second acquire fails.
	if fs.reexecGuard.tryAcquire() {
		t.Fatal("second acquire should fail")
	}
	// After release, acquire succeeds again.
	fs.reexecGuard.release()
	if !fs.reexecGuard.tryAcquire() {
		t.Fatal("acquire after release should succeed")
	}
	fs.reexecGuard.release()
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
	orig := reexecAcceptMsg{Accept: true, Version: ReexecProtocolVersion}
	data, err := json.Marshal(&orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded reexecAcceptMsg
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Accept != orig.Accept {
		t.Fatalf("Accept: want %v, got %v", orig.Accept, decoded.Accept)
	}
	if decoded.Version != orig.Version {
		t.Fatalf("Version: want %d, got %d", orig.Version, decoded.Version)
	}
}

func TestReexecAcceptMsgRejectRoundTrip(t *testing.T) {
	orig := reexecAcceptMsg{Accept: false, Version: ReexecProtocolVersion}
	data, err := json.Marshal(&orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded reexecAcceptMsg
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Accept != false {
		t.Fatal("Accept should be false")
	}
}

func TestReexecChildEnvMissingMount(t *testing.T) {
	t.Setenv("DRIVE9_REEXEC_SOCK", "/tmp/test.sock")
	t.Setenv("DRIVE9_REEXEC_VERSION", "1")

	_, err := ParseReexecChildEnv()
	if err == nil {
		t.Fatal("expected error for missing DRIVE9_REEXEC_MOUNT")
	}
}

func TestReexecChildEnvMissingVersion(t *testing.T) {
	t.Setenv("DRIVE9_REEXEC_SOCK", "/tmp/test.sock")
	t.Setenv("DRIVE9_REEXEC_MOUNT", "/mnt/test")

	_, err := ParseReexecChildEnv()
	if err == nil {
		t.Fatal("expected error for missing DRIVE9_REEXEC_VERSION")
	}
}

func TestReexecChildModeDetection(t *testing.T) {
	// Verify IsReexecChild gates the child path.
	if IsReexecChild() {
		t.Fatal("should not be reexec child without env vars")
	}

	t.Setenv("DRIVE9_REEXEC_SOCK", "/tmp/reexec.sock")
	if !IsReexecChild() {
		t.Fatal("should be reexec child when DRIVE9_REEXEC_SOCK is set")
	}
}

func TestReexecProtocolVersionConsistency(t *testing.T) {
	// Verify that the protocol version constant is used consistently in
	// accept messages and child config parsing.
	if ReexecProtocolVersion < 1 {
		t.Fatal("ReexecProtocolVersion must be >= 1")
	}
	msg := reexecAcceptMsg{Accept: true, Version: ReexecProtocolVersion}
	data, err := json.Marshal(&msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded reexecAcceptMsg
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Version != ReexecProtocolVersion {
		t.Fatalf("version mismatch after round-trip: want %d, got %d", ReexecProtocolVersion, decoded.Version)
	}
}
