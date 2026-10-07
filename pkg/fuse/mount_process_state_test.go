package fuse

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountstate"
)

func TestMountProcessStateOmitsCredentialsWhenRequested(t *testing.T) {
	const apiKey = "sk-runtime-secret"
	const token = "runtime-delegated-secret"
	state := newMountProcessState(&MountOptions{
		APIKey: apiKey, Token: token, RemoteRoot: "/workspace", OmitProcessStateCredentials: true,
	}, "/mnt/runtime", 42, mountstate.CredentialKindAPIKey, "/tmp/control.sock", time.Unix(1, 0).UTC())

	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), apiKey) || strings.Contains(string(raw), token) || state.APIKey != "" || state.Token != "" {
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
	mountPoint := t.TempDir()
	state := newMountProcessState(&MountOptions{
		APIKey: apiKey, Token: token, RemoteRoot: "/repo", OmitProcessStateCredentials: true,
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
	if strings.Contains(text, apiKey) || strings.Contains(text, token) {
		t.Fatalf("persisted mount fixture leaked credential bytes: %s", text)
	}
	for _, want := range []string{mountPoint, "/repo", "/tmp/runtime-control.sock"} {
		if !strings.Contains(text, want) {
			t.Fatalf("persisted mount fixture lost control metadata %q: %s", want, text)
		}
	}
}

func TestMountProcessStateKeepsCredentialsByDefault(t *testing.T) {
	state := newMountProcessState(&MountOptions{Token: "delegated-token"}, "/mnt/default", 42, mountstate.CredentialKindToken, "", time.Unix(1, 0).UTC())
	if state.Token != "delegated-token" {
		t.Fatalf("default process state token = %q", state.Token)
	}
}
