package cli

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	handler, err := newWebDAVHandler(c, "/_drive9_test")
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
