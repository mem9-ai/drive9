package fuse

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReexecChildHandshakeRecvFdTimeout(t *testing.T) {
	// Parent accepts the connection but never sends the fd. The child's
	// bounded recvFd wait must fail fast instead of blocking forever.
	//
	// Use os.MkdirTemp (short base path) rather than t.TempDir(): macOS caps
	// Unix socket paths near 104 bytes and t.TempDir()'s long names overflow it.
	sockDir, err := os.MkdirTemp("", "reexec-recvfd-*")
	if err != nil {
		t.Fatalf("mkdir temp: %v", err)
	}
	defer func() { _ = os.RemoveAll(sockDir) }()
	sockPath := filepath.Join(sockDir, "s.sock")
	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()

	// Accept and hold the connection open without ever sending an fd.
	acceptedCh := make(chan net.Conn, 1)
	go func() {
		c, err := listener.Accept()
		if err != nil {
			acceptedCh <- nil
			return
		}
		acceptedCh <- c
	}()

	orig := reexecRecvFdTimeout
	reexecRecvFdTimeout = 100 * time.Millisecond
	defer func() { reexecRecvFdTimeout = orig }()

	cfg := ReexecChildConfig{
		SockPath:   sockPath,
		MountPoint: "/mnt/test",
		Version:    ReexecProtocolVersion,
	}

	done := make(chan struct{})
	var handshakeErr error
	go func() {
		_, _, handshakeErr = ReexecChildHandshake(cfg)
		close(done)
	}()

	select {
	case <-done:
		if handshakeErr == nil {
			t.Fatal("expected recvFd to fail on stalled parent, got nil error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ReexecChildHandshake blocked past the recv deadline")
	}

	if c := <-acceptedCh; c != nil {
		_ = c.Close()
	}
}

func TestReexecCredentialEnv(t *testing.T) {
	tests := []struct {
		name string
		opts *MountOptions
		want []string
	}{
		{
			name: "nil opts",
			opts: nil,
			want: nil,
		},
		{
			name: "server and api key",
			opts: &MountOptions{Server: "https://api.drive9.ai", APIKey: "drive9_owner"},
			want: []string{"DRIVE9_SERVER=https://api.drive9.ai", "DRIVE9_API_KEY=drive9_owner"},
		},
		{
			name: "server and token",
			opts: &MountOptions{Server: "https://api.drive9.ai", Token: "jwt.abc.def"},
			want: []string{"DRIVE9_SERVER=https://api.drive9.ai", "DRIVE9_VAULT_TOKEN=jwt.abc.def"},
		},
		{
			name: "token wins over api key (mutually exclusive)",
			opts: &MountOptions{Server: "s", APIKey: "k", Token: "t"},
			want: []string{"DRIVE9_SERVER=s", "DRIVE9_VAULT_TOKEN=t"},
		},
		{
			name: "empty fields omitted",
			opts: &MountOptions{},
			want: nil,
		},
		{
			name: "api key without server",
			opts: &MountOptions{APIKey: "k"},
			want: []string{"DRIVE9_API_KEY=k"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := reexecCredentialEnv(tt.opts)
			if len(got) != len(tt.want) {
				t.Fatalf("len = %d, want %d (got %v)", len(got), len(tt.want), got)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Errorf("env[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

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
	if result.Err == nil || !errors.Is(result.Err, ErrReexecInProgress) {
		t.Fatalf("expected ErrReexecInProgress, got: %v", result.Err)
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
