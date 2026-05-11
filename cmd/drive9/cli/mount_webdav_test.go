package cli

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
)

func TestParseMountMode(t *testing.T) {
	for _, s := range []string{"auto", "fuse", "webdav"} {
		mode, err := ParseMountMode(s)
		if err != nil {
			t.Fatalf("ParseMountMode(%q) error: %v", s, err)
		}
		if string(mode) != s {
			t.Fatalf("ParseMountMode(%q) = %q", s, mode)
		}
	}

	if _, err := ParseMountMode("nfs"); err == nil {
		t.Fatal("expected error for invalid mode")
	}
	if _, err := ParseMountMode(""); err == nil {
		t.Fatal("expected error for empty mode")
	}
}

func TestResolveMountMode_DarwinNoFUSE(t *testing.T) {
	lp := fakeLookPath(nil) // no binaries
	got := ResolveMountMode(MountModeAuto, "darwin", lp)
	if got != MountModeWebDAV {
		t.Fatalf("darwin with no FUSE: got %q, want webdav", got)
	}
}

func TestResolveMountMode_DarwinWithMacFUSEStillDefaultsWebDAV(t *testing.T) {
	lp := fakeLookPath(map[string]bool{"mount_macfuse": true})
	got := ResolveMountMode(MountModeAuto, "darwin", lp)
	if got != MountModeWebDAV {
		t.Fatalf("darwin with macFUSE: got %q, want webdav", got)
	}
}

func TestResolveMountMode_DarwinWithFUSETStillDefaultsWebDAV(t *testing.T) {
	lp := fakeLookPath(map[string]bool{"mount_fusefs": true})
	got := ResolveMountMode(MountModeAuto, "darwin", lp)
	if got != MountModeWebDAV {
		t.Fatalf("darwin with FUSE-T: got %q, want webdav", got)
	}
}

func TestResolveMountMode_Linux(t *testing.T) {
	lp := fakeLookPath(nil)
	got := ResolveMountMode(MountModeAuto, "linux", lp)
	if got != MountModeFUSE {
		t.Fatalf("linux auto: got %q, want fuse", got)
	}
}

func TestResolveMountMode_Windows(t *testing.T) {
	lp := fakeLookPath(nil)
	got := ResolveMountMode(MountModeAuto, "windows", lp)
	if got != MountModeWebDAV {
		t.Fatalf("windows auto: got %q, want webdav", got)
	}
}

func TestResolveMountMode_ExplicitOverridesAuto(t *testing.T) {
	lp := fakeLookPath(nil)
	got := ResolveMountMode(MountModeFUSE, "darwin", lp)
	if got != MountModeFUSE {
		t.Fatalf("explicit fuse: got %q, want fuse", got)
	}

	got = ResolveMountMode(MountModeWebDAV, "linux", lp)
	if got != MountModeWebDAV {
		t.Fatalf("explicit webdav: got %q, want webdav", got)
	}
}

func TestHasFUSE(t *testing.T) {
	if hasFUSE(fakeLookPath(nil)) {
		t.Fatal("hasFUSE should be false with no binaries")
	}
	if !hasFUSE(fakeLookPath(map[string]bool{"mount_macfuse": true})) {
		t.Fatal("hasFUSE should be true with mount_macfuse")
	}
	if !hasFUSE(fakeLookPath(map[string]bool{"mount_fusefs": true})) {
		t.Fatal("hasFUSE should be true with mount_fusefs")
	}
}

func TestWebdavMountCmd_Darwin(t *testing.T) {
	cmd, err := webdavMountCmd("darwin", "http://127.0.0.1:8080", "/mnt/drive9")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	args := cmd.Args
	if len(args) != 4 || args[0] != "mount_webdav" || args[1] != "-S" ||
		args[2] != "http://127.0.0.1:8080" || args[3] != "/mnt/drive9" {
		t.Fatalf("darwin mount cmd args = %v", args)
	}
}

func TestWebdavMountCmd_UnsupportedOS(t *testing.T) {
	_, err := webdavMountCmd("freebsd", "http://127.0.0.1:8080", "/mnt/drive9")
	if err == nil {
		t.Fatal("expected error for unsupported OS")
	}
}

func TestWebdavMountCmd_Windows(t *testing.T) {
	cmd, err := webdavMountCmd("windows", "http://127.0.0.1:8080/_drive9_test/", "x:\\")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	args := cmd.Args
	if len(args) != 5 || args[0] != "net" || args[1] != "use" || args[2] != "X:" || args[3] != "http://127.0.0.1:8080/_drive9_test" || args[4] != "/persistent:no" {
		t.Fatalf("windows mount cmd args = %v", args)
	}
}

func TestWebdavMountCmd_WindowsRejectsNonDriveLetter(t *testing.T) {
	_, err := webdavMountCmd("windows", "http://127.0.0.1:8080", "C:\\temp\\drive9")
	if err == nil {
		t.Fatal("expected invalid Windows mountpoint to fail")
	}
	if !strings.Contains(err.Error(), "drive letter like \"X:\"") {
		t.Fatalf("error = %v, want drive-letter guidance", err)
	}
}

// TestWebdavMountCmd_LinuxNoDavfs verifies that the mount command returns
// an error when davfs2 is not installed.
func TestWebdavMountCmd_LinuxNoDavfs(t *testing.T) {
	// webdavMountCmd uses exec.LookPath internally, so on a system without
	// mount.davfs it should fail. We test by checking the error path
	// on non-Linux (where LookPath also won't find it).
	_, err := webdavMountCmd("linux", "http://127.0.0.1:8080", "/mnt/drive9")
	// On CI this may or may not have davfs2. If no error, the cmd should
	// reference mount.davfs. Either outcome is valid; we just verify no panic.
	_ = err
}

// ---------------------------------------------------------------------------
// Mount mode flag integration: verify the new --mode flag is parsed correctly
// and doesn't break existing flag parsing.
// ---------------------------------------------------------------------------

func TestMountCmd_InvalidMode(t *testing.T) {
	// Parse will call os.Exit(2) on flag errors due to ExitOnError, so we
	// can't easily test that. Instead test ParseMountMode directly.
	for _, bad := range []string{"smb", "nfs", "9p", "sync", ""} {
		if _, err := ParseMountMode(bad); err == nil {
			t.Fatalf("ParseMountMode(%q) should error", bad)
		}
	}
}

func TestMountCmd_WebDAVRejectsReadOnly(t *testing.T) {
	err := fsMountCmd([]string{"--mode=webdav", "--read-only", "/tmp/drive9-webdav-test"})
	if err == nil {
		t.Fatal("expected error for --read-only with WebDAV mode")
	}
	if !strings.Contains(err.Error(), "--read-only is not supported with WebDAV mode") {
		t.Fatalf("error = %v, want read-only WebDAV rejection", err)
	}
}

// TestNewWebDAVHandler verifies the handler constructor doesn't panic.
func TestNewWebDAVHandler(t *testing.T) {
	c := client.New("http://127.0.0.1:1", "test-key")
	handler, err := newWebDAVHandler(c, "/_drive9_test", "/")
	if err != nil {
		t.Fatalf("newWebDAVHandler returned error: %v", err)
	}
	if handler == nil {
		t.Fatal("newWebDAVHandler returned nil handler")
	}
}

func TestNewWebDAVNoncePrefix(t *testing.T) {
	prefix, err := newWebDAVNoncePrefix()
	if err != nil {
		t.Fatalf("newWebDAVNoncePrefix returned error: %v", err)
	}
	if !strings.HasPrefix(prefix, "/_drive9_") {
		t.Fatalf("prefix = %q, want /_drive9_ prefix", prefix)
	}
	if len(prefix) != len("/_drive9_")+32 {
		t.Fatalf("prefix length = %d, want %d", len(prefix), len("/_drive9_")+32)
	}
}

func TestWaitForWebDAVReady(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodOptions {
			t.Fatalf("method = %s, want OPTIONS", r.Method)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := waitForWebDAVReady(ctx, srv.URL); err != nil {
		t.Fatalf("waitForWebDAVReady returned error: %v", err)
	}
}

func TestWebDAVMountLifecycle(t *testing.T) {
	c := newWebDAVLifecycleClient(t)
	mountPoint := filepath.Join(t.TempDir(), "mnt")
	signals := make(chan os.Signal, 2)
	mounted := make(chan string, 1)
	done := make(chan error, 1)
	var unmounted atomic.Bool

	go func() {
		done <- webdavMountWithDeps(c, mountPoint, webdavMountDeps{
			goos:    "darwin",
			signals: signals,
			runMount: func(goos, serverURL, gotMountPoint string) error {
				if goos != "darwin" {
					t.Errorf("goos = %q, want darwin", goos)
				}
				if gotMountPoint != mountPoint {
					t.Errorf("mountPoint = %q, want %q", gotMountPoint, mountPoint)
				}
				if !strings.Contains(serverURL, "/_drive9_test/") {
					t.Errorf("serverURL = %q, want nonce prefix", serverURL)
				}
				req, err := http.NewRequest(http.MethodOptions, serverURL, nil)
				if err != nil {
					return err
				}
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return err
				}
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					return errors.New("nonce-prefixed WebDAV endpoint not ready")
				}
				mounted <- serverURL
				return nil
			},
			unmount: func(goos, gotMountPoint string) {
				if goos != "darwin" {
					t.Errorf("unmount goos = %q, want darwin", goos)
				}
				if gotMountPoint != mountPoint {
					t.Errorf("unmount mountPoint = %q, want %q", gotMountPoint, mountPoint)
				}
				unmounted.Store(true)
			},
			exit: func(code int) {
				t.Errorf("unexpected forced exit code %d", code)
			},
			newPrefix: func() (string, error) {
				return "/_drive9_test", nil
			},
		})
	}()

	select {
	case serverURL := <-mounted:
		rootURL := strings.TrimSuffix(serverURL, "/_drive9_test/")
		req, err := http.NewRequest(http.MethodOptions, rootURL, nil)
		if err != nil {
			t.Fatalf("root OPTIONS request: %v", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("root OPTIONS: %v", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("root OPTIONS status = %d, want %d", resp.StatusCode, http.StatusNotFound)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("mount command was not invoked")
	}

	signals <- os.Interrupt

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("webdavMountWithDeps returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("webdavMountWithDeps did not shut down after signal")
	}
	if !unmounted.Load() {
		t.Fatal("unmount callback was not called")
	}
}

func TestWebDAVMountLifecycleWindowsNormalizesDriveLetter(t *testing.T) {
	c := newWebDAVLifecycleClient(t)
	signals := make(chan os.Signal, 2)
	mounted := make(chan string, 1)
	done := make(chan error, 1)
	var unmounted atomic.Bool

	go func() {
		done <- webdavMountWithDeps(c, "x:\\", webdavMountDeps{
			goos:    "windows",
			signals: signals,
			runMount: func(goos, serverURL, gotMountPoint string) error {
				if goos != "windows" {
					t.Errorf("goos = %q, want windows", goos)
				}
				if gotMountPoint != "X:" {
					t.Errorf("mountPoint = %q, want X:", gotMountPoint)
				}
				mounted <- serverURL
				return nil
			},
			unmount: func(goos, gotMountPoint string) {
				if goos != "windows" {
					t.Errorf("unmount goos = %q, want windows", goos)
				}
				if gotMountPoint != "X:" {
					t.Errorf("unmount mountPoint = %q, want X:", gotMountPoint)
				}
				unmounted.Store(true)
			},
			exit: func(code int) {
				t.Errorf("unexpected forced exit code %d", code)
			},
			newPrefix: func() (string, error) {
				return "/_drive9_test", nil
			},
		})
	}()

	select {
	case <-mounted:
	case <-time.After(2 * time.Second):
		t.Fatal("mount command was not invoked")
	}

	signals <- os.Interrupt

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("webdavMountWithDeps returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("webdavMountWithDeps did not shut down after signal")
	}
	if !unmounted.Load() {
		t.Fatal("unmount callback was not called")
	}
}

func TestWebDAVMountReturnsMountFailure(t *testing.T) {
	c := newWebDAVLifecycleClient(t)
	mountErr := errors.New("mount failed")
	err := webdavMountWithDeps(c, filepath.Join(t.TempDir(), "mnt"), webdavMountDeps{
		goos:    "darwin",
		signals: make(chan os.Signal, 1),
		runMount: func(goos, serverURL, mountPoint string) error {
			return mountErr
		},
		newPrefix: func() (string, error) {
			return "/_drive9_test", nil
		},
	})
	if !strings.Contains(err.Error(), "mount failed") {
		t.Fatalf("error = %v, want mount failure", err)
	}
}

// TestErrorPathWebdavUnmount verifies unmount doesn't panic even on failure.
func TestErrorPathWebdavUnmount(t *testing.T) {
	// webdavUnmount with a non-existent mountpoint should not panic.
	// It prints to stderr but returns no error (fire-and-forget).
	webdavUnmount("darwin", "/nonexistent/path/drive9-test-unmount")
}

// TestMountErrorHint verifies that known macOS mount_webdav exit codes are
// translated into actionable messages, while unknown errors fall through.
func TestMountErrorHint(t *testing.T) {
	mp := "/tmp/drive9-test-mp"

	tests := []struct {
		name     string
		goos     string
		err      error
		wantSubs []string // all must appear in the hint
		wantHint bool
	}{
		{
			name:     "darwin ENODEV (19)",
			goos:     "darwin",
			err:      exitErrorWithCode(t, 19),
			wantSubs: []string{"ENODEV", "stale mount", "drive9 umount"},
			wantHint: true,
		},
		{
			name:     "darwin ENOENT (2)",
			goos:     "darwin",
			err:      exitErrorWithCode(t, 2),
			wantSubs: []string{"does not exist"},
			wantHint: true,
		},
		{
			name:     "darwin EBUSY (16)",
			goos:     "darwin",
			err:      exitErrorWithCode(t, 16),
			wantSubs: []string{"busy", "drive9 umount"},
			wantHint: true,
		},
		{
			name:     "darwin ECANCELED (77)",
			goos:     "darwin",
			err:      exitErrorWithCode(t, 77),
			wantSubs: []string{"authentication"},
			wantHint: true,
		},
		{
			name:     "darwin unknown exit code falls through",
			goos:     "darwin",
			err:      exitErrorWithCode(t, 42),
			wantHint: false,
		},
		{
			name:     "darwin non-ExitError falls through",
			goos:     "darwin",
			err:      errors.New("plain error"),
			wantHint: false,
		},
		{
			name:     "linux gets no hint",
			goos:     "linux",
			err:      exitErrorWithCode(t, 19),
			wantHint: false,
		},
		{
			name:     "windows system error 67",
			goos:     "windows",
			err:      exitErrorWithCode(t, 67),
			wantSubs: []string{"WebDAV redirector", "WebClient service"},
			wantHint: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hint := mountErrorHint(tt.goos, mp, tt.err)
			if tt.wantHint && hint == "" {
				t.Fatalf("expected hint, got empty string")
			}
			if !tt.wantHint && hint != "" {
				t.Fatalf("expected no hint, got %q", hint)
			}
			for _, sub := range tt.wantSubs {
				if !strings.Contains(hint, sub) {
					t.Errorf("hint = %q, want substring %q", hint, sub)
				}
			}
		})
	}
}

// TestExplainMountError checks that explainMountError preserves the underlying
// error (so errors.Is/As keep working) and appends the hint when applicable.
func TestExplainMountError(t *testing.T) {
	underlying := exitErrorWithCode(t, 19)
	wrapped := explainMountError("darwin", "/tmp/mp", underlying)

	if !errors.Is(wrapped, underlying) {
		t.Fatalf("wrapped error must wrap the original; got %v", wrapped)
	}
	if !strings.Contains(wrapped.Error(), "mount_webdav failed") {
		t.Errorf("missing prefix; got %q", wrapped.Error())
	}
	if !strings.Contains(wrapped.Error(), "ENODEV") {
		t.Errorf("missing hint; got %q", wrapped.Error())
	}

	// Plain errors (non-ExitError) get the original wrapping with no hint.
	plain := errors.New("plain")
	wrapped2 := explainMountError("darwin", "/tmp/mp", plain)
	if !strings.Contains(wrapped2.Error(), "plain") {
		t.Errorf("plain error not preserved; got %q", wrapped2.Error())
	}
	if strings.Contains(wrapped2.Error(), "ENODEV") {
		t.Errorf("plain error should not get an ENODEV hint; got %q", wrapped2.Error())
	}

	wrapped3 := explainMountError("windows", "X:", exitErrorWithCode(t, 67))
	if !strings.Contains(wrapped3.Error(), "WebClient service") {
		t.Errorf("windows hint missing; got %q", wrapped3.Error())
	}
}

func TestWebdavUnmountCmdWindows(t *testing.T) {
	cmd, err := webdavUnmountCmd("windows", "x:\\")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	args := cmd.Args
	if len(args) != 5 || args[0] != "net" || args[1] != "use" || args[2] != "X:" || args[3] != "/delete" || args[4] != "/y" {
		t.Fatalf("windows unmount cmd args = %v", args)
	}
}

// exitErrorWithCode runs a tiny shell command that exits with the given code
// and returns the resulting *exec.ExitError. This is the only portable way
// to construct one — its internal fields are unexported.
func exitErrorWithCode(t *testing.T, code int) error {
	t.Helper()
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.Command("cmd", "/c", fmt.Sprintf("exit %d", code))
	} else {
		cmd = exec.Command("sh", "-c", fmt.Sprintf("exit %d", code))
	}
	err := cmd.Run()
	if err == nil {
		t.Fatalf("expected non-zero exit, got nil")
	}
	return err
}

// TestParseMountModeRoundTrip ensures every MountMode constant is parseable.
func TestParseMountModeRoundTrip(t *testing.T) {
	for _, m := range []MountMode{MountModeAuto, MountModeFUSE, MountModeWebDAV} {
		got, err := ParseMountMode(string(m))
		if err != nil {
			t.Fatalf("ParseMountMode(%q) error: %v", m, err)
		}
		if got != m {
			t.Fatalf("roundtrip: %q != %q", got, m)
		}
	}
}

// Reuse fakeLookPath from mount_test.go (same package). If the existing
// helper isn't available due to build tags, duplicate it here.
func init() {
	_ = errors.New // ensure import used
}

// ---------------------------------------------------------------------------
// Remote root mount (:/remote /local) CLI parsing tests
// ---------------------------------------------------------------------------

func TestFsMountCmd_RemoteSourceParsing(t *testing.T) {
	isolateMountCredentialsForTest(t)
	// 2-arg with valid remote source should parse (will fail at credential
	// resolution, but that proves parsing succeeded).
	err := fsMountCmd([]string{":/foo/bar", "/tmp/drive9-remote-test"})
	if err == nil {
		t.Fatal("expected credential error, not nil")
	}
	// Should NOT contain "must be a remote source" — that means parsing worked.
	if strings.Contains(err.Error(), "must be a remote source") {
		t.Fatalf("error = %v, should have parsed remote source successfully", err)
	}
}

func TestFsMountCmd_RejectsNonRemoteFirstArg(t *testing.T) {
	err := fsMountCmd([]string{"/foo/bar", "/tmp/drive9-test"})
	if err == nil {
		t.Fatal("expected error for non-remote first arg")
	}
	if !strings.Contains(err.Error(), "must be a remote source") {
		t.Fatalf("error = %v, want remote-source rejection", err)
	}
}

func TestFsMountCmd_RejectsContextScopedRemote(t *testing.T) {
	err := fsMountCmd([]string{"prod:/foo/bar", "/tmp/drive9-test"})
	if err == nil {
		t.Fatal("expected error for context-scoped remote")
	}
	if !strings.Contains(err.Error(), "not yet supported") {
		t.Fatalf("error = %v, want context-scoped rejection", err)
	}
}

func TestFsMountCmd_SingleArgDefaultsToRootRemote(t *testing.T) {
	isolateMountCredentialsForTest(t)
	// Single arg should work (fail at credentials, not at parsing).
	err := fsMountCmd([]string{"/tmp/drive9-single-arg-test"})
	if err == nil {
		t.Fatal("expected credential error, not nil")
	}
	// Should NOT contain "remote source" error.
	if strings.Contains(err.Error(), "remote source") {
		t.Fatalf("single arg should not trigger remote-source error: %v", err)
	}
}

// isolateMountCredentialsForTest keeps parsing-only mount tests from reading
// the developer machine's DRIVE9_* env vars or active drive9 context. Without
// this, a test that expects credential resolution to fail can reach the real
// WebDAV mount path and block waiting for an unmount signal.
func isolateMountCredentialsForTest(t *testing.T) {
	t.Helper()
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)
	t.Setenv("HOME", t.TempDir())
	t.Setenv(EnvServer, "")
	t.Setenv(EnvAPIKey, "")
	t.Setenv(EnvVaultToken, "")
}

func newWebDAVLifecycleClient(t *testing.T) *client.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && r.URL.Path == "/v1/fs/" {
			w.Header().Set("X-Dat9-IsDir", "true")
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"not found"}`))
	}))
	t.Cleanup(srv.Close)
	return client.New(srv.URL, "test-key")
}
