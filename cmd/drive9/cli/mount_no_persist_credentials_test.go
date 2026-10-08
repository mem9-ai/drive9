package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

type mountNoPersistExitError struct {
	message string
	code    int
}

func (e mountNoPersistExitError) Error() string { return e.message }
func (e mountNoPersistExitError) ExitCode() int { return e.code }

func TestMountNoPersistCredentialsPropagatesToFuse(t *testing.T) {
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)
	stubMountProfileAppendLogProbe(t)
	oldMountFuse := mountFuse
	t.Cleanup(func() { mountFuse = oldMountFuse })

	var got *mountFuseOptions
	mountFuse = func(opts *mountFuseOptions) error {
		copied := *opts
		got = &copied
		return nil
	}
	if err := fsMountCmd([]string{
		"--mode=fuse", "--server=https://drive9.example", "--api-key=sk-runtime-secret",
		"--profile=none", "--no-persist-credentials", t.TempDir(),
	}); err != nil {
		t.Fatalf("fsMountCmd: %v", err)
	}
	if got == nil || !got.OmitProcessStateCredentials {
		t.Fatalf("mount options = %#v, want omitted process-state credentials", got)
	}
}

func TestMountNoPersistCredentialsRejectsSupervisor(t *testing.T) {
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)
	err := MountCmd([]string{
		"--mode=fuse", "--server=https://drive9.example", "--api-key=sk-runtime-secret",
		"--profile=none", "--no-persist-credentials", t.TempDir(),
	})
	if err == nil || !strings.Contains(err.Error(), "requires --foreground or --no-supervise") {
		t.Fatalf("error = %v", err)
	}
}

func TestMountNoPersistCredentialsRedactsCapabilityPreflightError(t *testing.T) {
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)
	server := "https://preview.example.test/?token=preview-secret"
	apiKey := "drive9-owner-secret"
	oldProbe := mountExtentXattrSupported
	t.Cleanup(func() { mountExtentXattrSupported = oldProbe })
	mountExtentXattrSupported = func(context.Context, string, string, string) error {
		return fmt.Errorf("GET %s failed with %s", server, apiKey)
	}

	err := fsMountCmd([]string{
		"--mode=fuse", "--server=" + server, "--api-key=" + apiKey,
		"--profile=extent", "--require-extent-xattr-v1", "--no-persist-credentials", t.TempDir(),
	})
	if err == nil {
		t.Fatal("expected capability preflight error")
	}
	if strings.Contains(err.Error(), server) || strings.Contains(err.Error(), apiKey) {
		t.Fatalf("capability preflight error leaked credentials: %q", err)
	}
	if !strings.Contains(err.Error(), "GET <redacted> failed with <redacted>") {
		t.Fatalf("capability preflight error = %q", err)
	}
}

func TestMountNoPersistCredentialsRedactsMountError(t *testing.T) {
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)
	stubMountProfileAppendLogProbe(t)
	server := "https://preview.example.test/?token=preview-secret"
	apiKey := "drive9-owner-secret"
	sentinel := errors.New("mount failed")
	oldMountFuse := mountFuse
	t.Cleanup(func() { mountFuse = oldMountFuse })
	mountFuse = func(*mountFuseOptions) error {
		return fmt.Errorf("mount stderr included %s and %s: %w", server, apiKey, sentinel)
	}

	err := fsMountCmd([]string{
		"--mode=fuse", "--server=" + server, "--api-key=" + apiKey,
		"--profile=none", "--no-persist-credentials", t.TempDir(),
	})
	if err == nil {
		t.Fatal("expected mount error")
	}
	if strings.Contains(err.Error(), server) || strings.Contains(err.Error(), apiKey) {
		t.Fatalf("mount error leaked credentials: %q", err)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("redacted mount error lost unwrap identity: %v", err)
	}
}

func TestRedactMountCommandCredentialsPreservesWrappedExitCode(t *testing.T) {
	server := "https://preview.example.test/?token=preview-secret"
	apiKey := "drive9-owner-secret"
	token := "drive9-delegated-secret"
	original := mountNoPersistExitError{
		message: fmt.Sprintf("server=%s api_key=%s token=%s", server, apiKey, token),
		code:    42,
	}
	err := redactMountCommandCredentials(fmt.Errorf("outer: %w", original), server, apiKey, token)
	if strings.Contains(err.Error(), server) || strings.Contains(err.Error(), apiKey) || strings.Contains(err.Error(), token) {
		t.Fatalf("wrapped error leaked credentials: %q", err)
	}
	var originalType mountNoPersistExitError
	if !errors.As(err, &originalType) {
		t.Fatalf("redacted error lost unwrap type: %v", err)
	}
	type exitCoder interface{ ExitCode() int }
	code, ok := err.(exitCoder)
	if !ok || code.ExitCode() != 42 {
		t.Fatalf("redacted exit code = %v, %v", code, ok)
	}
}
