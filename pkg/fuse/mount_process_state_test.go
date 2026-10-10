package fuse

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountstate"
)

func TestMountProcessStateOmitsCredentialsWhenRequested(t *testing.T) {
	const apiKey = "sk-runtime-secret"
	const token = "runtime-delegated-secret"
	const server = "https://signed-preview-secret.example"
	state := newMountProcessState(&MountOptions{
		Server: server, APIKey: apiKey, Token: token, RemoteRoot: "/workspace", OmitProcessStateCredentials: true,
	}, "/mnt/runtime", 42, mountstate.CredentialKindAPIKey, "/tmp/control.sock", time.Unix(1, 0).UTC())

	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), apiKey) || strings.Contains(string(raw), token) || strings.Contains(string(raw), server) ||
		state.APIKey != "" || state.Token != "" || state.Server != "" {
		t.Fatalf("process state leaked credential: %s", raw)
	}
	if state.MountPoint != "/mnt/runtime" || state.ControlSocket == "" || state.CredentialKind != mountstate.CredentialKindAPIKey {
		t.Fatalf("process state lost non-secret control metadata: %#v", state)
	}
}

func TestMountProcessStateFixtureContainsNoCredentialBytes(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	const apiKey = "sk-runtime-fixture-secret"
	const token = "runtime-fixture-token"
	const server = "https://signed-preview-fixture.example"
	mountPoint := t.TempDir()
	state := newMountProcessState(&MountOptions{
		Server: server, APIKey: apiKey, Token: token, RemoteRoot: "/repo", OmitProcessStateCredentials: true,
	}, mountPoint, 42, mountstate.CredentialKindAPIKey, "/tmp/runtime-control.sock", time.Unix(1, 0).UTC())
	state.PID = os.Getpid()
	path, err := mountstate.WriteProcessState(mountPoint, state)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(path) })
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if strings.Contains(text, apiKey) || strings.Contains(text, token) || strings.Contains(text, server) {
		t.Fatalf("persisted mount fixture leaked credential bytes: %s", text)
	}
	for _, want := range []string{mountPoint, "/repo", "/tmp/runtime-control.sock"} {
		if !strings.Contains(text, want) {
			t.Fatalf("persisted mount fixture lost control metadata %q: %s", want, text)
		}
	}
}

func TestMountProcessStateKeepsCredentialsByDefault(t *testing.T) {
	state := newMountProcessState(&MountOptions{Server: "https://drive9.example", Token: "delegated-token"}, "/mnt/default", 42, mountstate.CredentialKindToken, "", time.Unix(1, 0).UTC())
	if state.Token != "delegated-token" || state.Server != "https://drive9.example" {
		t.Fatalf("default process state token/server = %q/%q", state.Token, state.Server)
	}
}

func TestMountReadyMessageOmitsServer(t *testing.T) {
	const server = "https://signed-preview-secret.example"
	message := mountReadyMessage(&MountOptions{
		Server: server, MountPoint: "/mnt/runtime", ReadOnly: true, WritePolicy: WritePolicyWriteSync,
	}, "actor-1", "/cache", "/shadow")
	if strings.Contains(message, server) || strings.Contains(message, "server:") {
		t.Fatalf("mount ready message leaked server endpoint: %q", message)
	}
	for _, want := range []string{"/mnt/runtime", "actor-1", "readonly: true", "write_policy: write-sync", "/cache", "/shadow"} {
		if !strings.Contains(message, want) {
			t.Fatalf("mount ready message lost %q: %q", want, message)
		}
	}
}

func TestMountCredentialErrorRedactsFailureAndPreservesExitCode(t *testing.T) {
	const server = "https://signed-preview-secret.example"
	const apiKey = "runtime-secret-key"
	const token = "runtime-secret-token"
	original := ExitStartupTransientErr("cannot reach "+server, fmt.Errorf("request %s with %s/%s failed", server, apiKey, token))
	got := redactMountCredentialError(original, &MountOptions{Server: server, APIKey: apiKey, Token: token})
	if strings.Contains(got.Error(), server) || strings.Contains(got.Error(), apiKey) || strings.Contains(got.Error(), token) {
		t.Fatalf("mount failure leaked credential-bearing endpoint: %q", got)
	}
	var exitErr *MountExitError
	if !errors.As(got, &exitErr) || exitErr.ExitCode() != ExitStartupTransient || exitErr.Reason != ExitReasonStartupTransient {
		t.Fatalf("redacted mount failure lost exit classification: %#v", got)
	}
	if !strings.Contains(got.Error(), "<redacted>") {
		t.Fatalf("redacted mount failure omitted redaction marker: %q", got)
	}
}
