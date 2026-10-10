package cli

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Existing mount tests stub the mount itself and do not exercise status I/O.
func stubMountProfileAppendLogProbe(t *testing.T) {
	t.Helper()
	old := mountProfileAppendLogSupported
	t.Cleanup(func() { mountProfileAppendLogSupported = old })
	mountProfileAppendLogSupported = func(context.Context, string, string, string) bool { return true }
}

func setupMountProfileAppendLogTest(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("Drive9 FUSE is unavailable on Windows")
	}
	t.Setenv("HOME", t.TempDir())
	setResolverEnv(t, nil)
	t.Setenv(EnvMountAppendLogPatterns, "")
	t.Setenv(EnvMountLocalOnlyPatterns, "")
	t.Setenv(EnvMountRemoteOnlyPatterns, "")
	t.Setenv("DRIVE9_MOUNT_SUPERVISE", "")
}

func captureProfileMountOptions(t *testing.T) **mountFuseOptions {
	t.Helper()
	var got *mountFuseOptions
	old := mountFuse
	t.Cleanup(func() { mountFuse = old })
	mountFuse = func(opts *mountFuseOptions) error {
		copy := *opts
		got = &copy
		return nil
	}
	return &got
}

func TestMountProfileAppendLogCapabilities(t *testing.T) {
	for _, tc := range []struct {
		name   string
		body   string
		status int
		want   bool
	}{
		{"supported", `{"storage_capabilities":{"append_log_v1":true},"migration_capabilities":{},"max_upload_bytes":1000000,"inline_threshold":50000}`, 200, true},
		{"false", `{"storage_capabilities":{"append_log_v1":false}}`, 200, false},
		{"empty", `{"storage_capabilities":{}}`, 200, false},
		{"missing", `{}`, 200, false},
		{"invalid-json", `{`, 200, false},
		{"invalid-thresholds", `{"storage_capabilities":{"append_log_v1":true},"migration_capabilities":{}}`, 200, false},
		{"server-error", `{}`, 503, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setupMountProfileAppendLogTest(t)
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path != "/v1/status" || r.Header.Get("Authorization") != "Bearer sk-test" {
					t.Errorf("unexpected status request: %s, auth=%q", r.URL.Path, r.Header.Get("Authorization"))
				}
				w.WriteHeader(tc.status)
				_, _ = fmt.Fprint(w, tc.body)
			}))
			t.Cleanup(srv.Close)
			got := captureProfileMountOptions(t)
			stderr, err := captureStderrE(t, func() error {
				return MountCmd([]string{"--foreground", "--mode=fuse", "--server", srv.URL, "--api-key=sk-test", t.TempDir()})
			})
			if err != nil || *got == nil {
				t.Fatalf("mount = %v, opts = %v", err, *got)
			}
			var want []string
			if tc.want {
				want = []string{"**/*-wal"}
			}
			if !reflect.DeepEqual((*got).AppendLogPatterns, want) {
				t.Fatalf("patterns = %v, want %v", (*got).AppendLogPatterns, want)
			}
			wantWarnings := 1
			if tc.want {
				wantWarnings = 0
			}
			if n := strings.Count(stderr, "[append-log] ignored:"); n != wantWarnings {
				t.Fatalf("warnings = %d, stderr = %q", n, stderr)
			}
			if calls.Load() != 1 {
				t.Fatalf("status requests = %d, want 1", calls.Load())
			}
		})
	}
}

func TestMountProfileAppendLogSources(t *testing.T) {
	for _, supported := range []bool{true, false} {
		t.Run(fmt.Sprint(supported), func(t *testing.T) {
			setupMountProfileAppendLogTest(t)
			writeTestProfile(t, "custom", "[append-log]\n**/profile.log\n**/shared.log\n")
			t.Setenv(EnvMountAppendLogPatterns, "**/env.log\n**/shared.log")
			setResolverEnv(t, map[string]string{EnvVaultToken: "delegated-test"})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer delegated-test" {
					t.Errorf("wrong delegated identity: %q", r.Header.Get("Authorization"))
				}
				_, _ = fmt.Fprintf(w, `{"storage_capabilities":{"append_log_v1":%t}}`, supported)
			}))
			t.Cleanup(srv.Close)
			got := captureProfileMountOptions(t)
			err := MountCmd([]string{"--foreground", "--mode=fuse", "--server", srv.URL, "--profile=custom", "--append-log=**/flag.log", "--append-log=**/shared.log", t.TempDir()})
			if err != nil || *got == nil {
				t.Fatalf("mount = %v, opts = %v", err, *got)
			}
			want := []string{"**/env.log", "**/shared.log", "**/flag.log"}
			if supported {
				want = []string{"**/profile.log", "**/shared.log", "**/env.log", "**/flag.log"}
			}
			if !reflect.DeepEqual((*got).AppendLogPatterns, want) {
				t.Fatalf("patterns = %v, want %v", (*got).AppendLogPatterns, want)
			}
		})
	}
}

func TestMountProfileAppendLogSkipsProbe(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		wantError  bool
	}{
		{"none", "", false},
		{"custom-empty", "[local]\n**/scratch/**\n", false},
		{"custom-invalid", "[append-log]\n**/../wal\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setupMountProfileAppendLogTest(t)
			if tc.body != "" {
				writeTestProfile(t, tc.name, tc.body)
			}
			old := mountProfileAppendLogSupported
			t.Cleanup(func() { mountProfileAppendLogSupported = old })
			mountProfileAppendLogSupported = func(context.Context, string, string, string) bool {
				t.Fatal("unexpected capability probe")
				return false
			}
			got := captureProfileMountOptions(t)
			stderr, err := captureStderrE(t, func() error {
				return MountCmd([]string{"--foreground", "--mode=fuse", "--server=https://unused.invalid", "--api-key=sk-test", "--profile=" + tc.name, "--append-log=**/explicit.log", t.TempDir()})
			})
			if (err != nil) != tc.wantError {
				t.Fatalf("mount error = %v", err)
			}
			if strings.Contains(stderr, "[append-log] ignored:") {
				t.Fatalf("unexpected warning: %s", stderr)
			}
			if !tc.wantError && (*got == nil || !reflect.DeepEqual((*got).AppendLogPatterns, []string{"**/explicit.log"})) {
				t.Fatalf("explicit rules lost: %v", *got)
			}
		})
	}
}

func TestMountProfileAppendLogProbeFailures(t *testing.T) {
	for _, kind := range []string{"timeout", "connection"} {
		t.Run(kind, func(t *testing.T) {
			setupMountProfileAppendLogTest(t)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
			t.Cleanup(srv.Close)
			if kind == "connection" {
				srv.Close()
			}
			old := mountProfileAppendLogSupported
			t.Cleanup(func() { mountProfileAppendLogSupported = old })
			mountProfileAppendLogSupported = func(ctx context.Context, server, apiKey, token string) bool {
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > 5*time.Second {
					t.Fatal("mount preflight needs a bounded deadline")
				}
				ctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
				defer cancel()
				return old(ctx, server, apiKey, token)
			}
			got := captureProfileMountOptions(t)
			stderr, err := captureStderrE(t, func() error {
				return MountCmd([]string{"--foreground", "--mode=fuse", "--server", srv.URL, "--api-key=sk-test", t.TempDir()})
			})
			if err != nil || *got == nil || len((*got).AppendLogPatterns) != 0 {
				t.Fatalf("mount = %v, opts = %v", err, *got)
			}
			if strings.Count(stderr, "[append-log] ignored:") != 1 {
				t.Fatalf("missing warning: %q", stderr)
			}
		})
	}
}

func TestMountProfileAppendLogWarningBeforeStart(t *testing.T) {
	for _, mode := range []string{"foreground", "background", "legacy", "supervise-foreground", "worker"} {
		t.Run(mode, func(t *testing.T) {
			setupMountProfileAppendLogTest(t)
			oldProbe := mountProfileAppendLogSupported
			oldFuse, oldBackground, oldSupervised, oldForeground := mountFuse, startMountBackground, startMountSupervisedBackground, runSuperviseForeground
			t.Cleanup(func() {
				mountProfileAppendLogSupported = oldProbe
				mountFuse = oldFuse
				startMountBackground = oldBackground
				startMountSupervisedBackground = oldSupervised
				runSuperviseForeground = oldForeground
			})
			probed := false
			mountProfileAppendLogSupported = func(context.Context, string, string, string) bool { probed = true; return false }
			stderr, err := os.CreateTemp(t.TempDir(), "stderr")
			if err != nil {
				t.Fatal(err)
			}
			oldStderr := os.Stderr
			os.Stderr = stderr
			t.Cleanup(func() { os.Stderr = oldStderr; _ = stderr.Close() })
			stopped := errors.New("mount start reached")
			check := func(actual string) error {
				if actual != mode || !probed {
					t.Fatalf("start = %s, probe = %t", actual, probed)
				}
				data, err := os.ReadFile(stderr.Name())
				if err != nil {
					t.Fatal(err)
				}
				want := 1
				if mode == "worker" {
					want = 0
				}
				if n := strings.Count(string(data), "[append-log] ignored:"); n != want {
					t.Fatalf("warnings before start = %d, want %d: %s", n, want, data)
				}
				return stopped
			}
			mountFuse = func(opts *mountFuseOptions) error {
				if len(opts.AppendLogPatterns) != 0 {
					t.Fatal("unsupported profile rules reached FUSE")
				}
				if opts.Supervised {
					return check("worker")
				}
				return check("foreground")
			}
			startMountBackground = func(mountBackgroundRequest) error { return check("legacy") }
			startMountSupervisedBackground = func(mountSuperviseStartRequest) error { return check("background") }
			runSuperviseForeground = func(mountSuperviseStartRequest) error { return check("supervise-foreground") }
			args := []string{"--mode=fuse", "--server=https://unused.invalid", "--api-key=sk-test"}
			switch mode {
			case "foreground":
				args = append(args, "--foreground")
			case "legacy":
				args = append(args, "--no-supervise")
			case "supervise-foreground":
				args = append(args, "--supervise-foreground")
			case "worker":
				args = append(args, "--foreground", "--supervised")
			}
			if err := MountCmd(append(args, t.TempDir())); !errors.Is(err, stopped) {
				t.Fatalf("mount error = %v", err)
			}
		})
	}
}

func TestMountProfileAppendLogWorkerReloads(t *testing.T) {
	setupMountProfileAppendLogTest(t)
	writeTestProfile(t, "custom", "[append-log]\n**/original.log\n")
	t.Setenv(EnvMountAppendLogPatterns, "**/env.log")
	var supported atomic.Bool
	supported.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, `{"storage_capabilities":{"append_log_v1":%t}}`, supported.Load())
	}))
	t.Cleanup(srv.Close)
	var req mountSuperviseStartRequest
	old := startMountSupervisedBackground
	t.Cleanup(func() { startMountSupervisedBackground = old })
	startMountSupervisedBackground = func(r mountSuperviseStartRequest) error { req = r; return nil }
	got := captureProfileMountOptions(t)
	if err := MountCmd([]string{"--mode=fuse", "--server", srv.URL, "--api-key=sk-test", "--profile=custom", t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	if *got != nil {
		t.Fatal("parent mounted in process")
	}
	if strings.Contains(strings.Join(req.OriginalArgs, " "), "original.log") {
		t.Fatal("profile pattern materialized as flag")
	}
	if !containsString(req.OriginalArgs, "--append-log=**/env.log") {
		t.Fatal("env pattern not snapshotted")
	}
	if err := os.WriteFile(profileConfigPath("custom"), []byte("[append-log]\n**/updated.log\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	args := workerArgsForSupervise(req.OriginalArgs)
	for _, enabled := range []bool{true, false} {
		supported.Store(enabled)
		setResolverEnv(t, map[string]string{EnvServer: req.Server, EnvAPIKey: req.APIKey})
		stderr, err := captureStderrE(t, func() error { return MountCmd(args[1:]) })
		if err != nil || *got == nil {
			t.Fatalf("worker mount = %v, opts = %v", err, *got)
		}
		want := []string{"**/env.log"}
		if enabled {
			want = []string{"**/updated.log", "**/env.log"}
		}
		if !reflect.DeepEqual((*got).AppendLogPatterns, want) {
			t.Fatalf("worker patterns = %v, want %v", (*got).AppendLogPatterns, want)
		}
		if strings.Contains(stderr, "[append-log] ignored:") {
			t.Fatalf("worker repeated warning: %s", stderr)
		}
	}
}
