package cli

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
	"github.com/mem9-ai/dat9/pkg/mountstate"
)

func fakeLookPath(binMap map[string]bool) func(string) (string, error) {
	return func(name string) (string, error) {
		if binMap[name] {
			return "/usr/bin/" + name, nil
		}
		return "", errors.New("not found")
	}
}

func TestUmountArgvDarwin(t *testing.T) {
	got, err := umountArgv("darwin", fakeLookPath(nil), "/mnt/drive9")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"umount", "/mnt/drive9"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("argv = %v, want %v", got, want)
	}
}

func TestUmountArgvPrefersFusermount3(t *testing.T) {
	got, err := umountArgv("linux", fakeLookPath(map[string]bool{
		"fusermount3": true,
		"fusermount":  true,
		"umount":      true,
	}), "/mnt/drive9")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"fusermount3", "-u", "/mnt/drive9"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("argv = %v, want %v", got, want)
	}
}

func TestUmountArgvFallsBackToFusermount(t *testing.T) {
	got, err := umountArgv("linux", fakeLookPath(map[string]bool{
		"fusermount": true,
	}), "/mnt/drive9")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"fusermount", "-u", "/mnt/drive9"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("argv = %v, want %v", got, want)
	}
}

func TestUmountArgvFallsBackToUmount(t *testing.T) {
	got, err := umountArgv("linux", fakeLookPath(map[string]bool{
		"umount": true,
	}), "/mnt/drive9")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"umount", "/mnt/drive9"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("argv = %v, want %v", got, want)
	}
}

func TestUmountArgvNoBinary(t *testing.T) {
	_, err := umountArgv("linux", fakeLookPath(nil), "/mnt/drive9")
	if err == nil {
		t.Fatal("expected error when no unmount binaries are available")
	}
}

func TestUmountArgvWindows(t *testing.T) {
	got, err := umountArgv("windows", fakeLookPath(nil), "x:\\")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"net", "use", "X:", "/delete", "/y"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("argv = %v, want %v", got, want)
	}
}

func TestUmountArgvWindowsRejectsNonDriveLetter(t *testing.T) {
	_, err := umountArgv("windows", fakeLookPath(nil), "C:\\temp\\drive9")
	if err == nil {
		t.Fatal("expected invalid Windows mountpoint to fail")
	}
	if !strings.Contains(err.Error(), "drive letter like \"X:\"") {
		t.Fatalf("error = %v, want drive-letter guidance", err)
	}
}

func TestRunUmountWaitsForMountProcessExit(t *testing.T) {
	now := time.Unix(100, 0)
	runCalls := 0
	aliveCalls := 0
	deps := umountDeps{
		goos:     "linux",
		lookPath: fakeLookPath(map[string]bool{"fusermount3": true}),
		run: func(argv []string) error {
			runCalls++
			want := []string{"fusermount3", "-u", "/mnt/drive9"}
			if !reflect.DeepEqual(argv, want) {
				t.Fatalf("argv = %v, want %v", argv, want)
			}
			return nil
		},
		readPID: func(mountPoint string) (int, string, error) {
			if mountPoint != "/mnt/drive9" {
				t.Fatalf("mountPoint = %q", mountPoint)
			}
			return 1234, "/tmp/drive9.pid", nil
		},
		pidAlive: func(pid int) bool {
			if pid != 1234 {
				t.Fatalf("pid = %d", pid)
			}
			aliveCalls++
			return aliveCalls < 3
		},
		now:       func() time.Time { return now },
		sleep:     func(d time.Duration) { now = now.Add(d) },
		printErrf: func(string, ...any) {},
	}

	if err := runUmount([]string{"/mnt/drive9"}, deps); err != nil {
		t.Fatalf("runUmount: %v", err)
	}
	if runCalls != 1 {
		t.Fatalf("runCalls = %d, want 1", runCalls)
	}
	if aliveCalls != 3 {
		t.Fatalf("aliveCalls = %d, want 3", aliveCalls)
	}
}

func TestRunUmountReturnsTimeoutWhenMountProcessStillAlive(t *testing.T) {
	now := time.Unix(100, 0)
	deps := umountDeps{
		goos:     "linux",
		lookPath: fakeLookPath(map[string]bool{"fusermount3": true}),
		run:      func([]string) error { return nil },
		readPID:  func(string) (int, string, error) { return 1234, "/tmp/drive9.pid", nil },
		pidAlive: func(int) bool { return true },
		now:      func() time.Time { return now },
		sleep:    func(d time.Duration) { now = now.Add(d) },
		printErrf: func(format string, args ...any) {
			if !strings.Contains(format, "still running") {
				t.Fatalf("printErrf format = %q", format)
			}
		},
	}

	err := runUmount([]string{"--timeout", "250ms", "/mnt/drive9"}, deps)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !strings.Contains(err.Error(), "still running") {
		t.Fatalf("error = %v, want still running", err)
	}
	if !strings.Contains(err.Error(), "/tmp/drive9.pid") {
		t.Fatalf("error = %v, want pid file path", err)
	}
}

func TestRunUmountDoesNotBlockOnStalePID(t *testing.T) {
	aliveCalls := 0
	deps := umountDeps{
		goos:     "linux",
		lookPath: fakeLookPath(map[string]bool{"fusermount3": true}),
		run:      func([]string) error { return nil },
		readProcessState: func(string) (mountstate.ProcessState, string, error) {
			return mountstate.ProcessState{}, "", errors.New("readProcessState should not be called on non-Windows")
		},
		readPID: func(string) (int, string, error) { return 1234, "/tmp/drive9.pid", nil },
		pidAlive: func(int) bool {
			aliveCalls++
			return false
		},
		now:       time.Now,
		sleep:     func(time.Duration) { t.Fatal("sleep should not be called for stale pid") },
		printErrf: func(string, ...any) {},
	}

	if err := runUmount([]string{"/mnt/drive9"}, deps); err != nil {
		t.Fatalf("runUmount: %v", err)
	}
	if aliveCalls != 1 {
		t.Fatalf("aliveCalls = %d, want 1", aliveCalls)
	}
}

func TestRunUmountNoPIDFileReturnsSuccess(t *testing.T) {
	deps := umountDeps{
		goos:     "linux",
		lookPath: fakeLookPath(map[string]bool{"fusermount3": true}),
		run:      func([]string) error { return nil },
		readProcessState: func(string) (mountstate.ProcessState, string, error) {
			return mountstate.ProcessState{}, "", errors.New("readProcessState should not be called on non-Windows")
		},
		readPID:   func(string) (int, string, error) { return 0, "/tmp/drive9.pid", os.ErrNotExist },
		terminate: func(int, time.Duration) error { t.Fatal("terminate should not be called without pid file"); return nil },
		remove:    func(string) error { t.Fatal("remove should not be called without pid file"); return nil },
		pidAlive:  func(int) bool { t.Fatal("pidAlive should not be called without pid file"); return false },
		now:       time.Now,
		sleep:     func(time.Duration) {},
		printErrf: func(string, ...any) {},
	}

	if err := runUmount([]string{"/mnt/drive9"}, deps); err != nil {
		t.Fatalf("runUmount: %v", err)
	}
}

func TestRunUmountNonWindowsTimeoutZeroSkipsPIDRead(t *testing.T) {
	deps := umountDeps{
		goos:     "linux",
		lookPath: fakeLookPath(map[string]bool{"fusermount3": true}),
		run:      func([]string) error { return nil },
		readProcessState: func(string) (mountstate.ProcessState, string, error) {
			return mountstate.ProcessState{}, "", errors.New("readProcessState should not be called on non-Windows")
		},
		readPID: func(string) (int, string, error) {
			t.Fatal("readPID should not be called when timeout is zero on non-Windows")
			return 0, "", nil
		},
		terminate: func(int, time.Duration) error {
			t.Fatal("terminate should not be called on non-Windows timeout-zero path")
			return nil
		},
		remove: func(string) error {
			t.Fatal("remove should not be called on non-Windows timeout-zero path")
			return nil
		},
		pidAlive: func(int) bool {
			t.Fatal("pidAlive should not be called on non-Windows timeout-zero path")
			return false
		},
		now:       time.Now,
		sleep:     func(time.Duration) {},
		printErrf: func(string, ...any) {},
	}

	if err := runUmount([]string{"--timeout", "0", "/mnt/drive9"}, deps); err != nil {
		t.Fatalf("runUmount: %v", err)
	}
}

func TestRunUmountWindowsTerminatesMountProcess(t *testing.T) {
	now := time.Unix(100, 0)
	runCalls := 0
	terminateCalls := 0
	removeCalls := 0
	deps := umountDeps{
		goos:     "windows",
		lookPath: fakeLookPath(nil),
		run: func(argv []string) error {
			runCalls++
			want := []string{"net", "use", "X:", "/delete", "/y"}
			if !reflect.DeepEqual(argv, want) {
				t.Fatalf("argv = %v, want %v", argv, want)
			}
			return nil
		},
		readProcessState: func(mountPoint string) (mountstate.ProcessState, string, error) {
			if mountPoint != "X:\\" {
				t.Fatalf("mountPoint = %q, want %q", mountPoint, "X:\\")
			}
			return mountstate.ProcessState{PID: 4321, CreationTime: 99}, "C:/tmp/drive9-webdav.pid", nil
		},
		terminateState: func(state mountstate.ProcessState, waitTimeout time.Duration) error {
			terminateCalls++
			if state.PID != 4321 {
				t.Fatalf("pid = %d, want 4321", state.PID)
			}
			if state.CreationTime != 99 {
				t.Fatalf("creationTime = %d, want 99", state.CreationTime)
			}
			if waitTimeout != 60*time.Second {
				t.Fatalf("waitTimeout = %s, want %s", waitTimeout, 60*time.Second)
			}
			return nil
		},
		remove: func(path string) error {
			removeCalls++
			if path != "C:/tmp/drive9-webdav.pid" {
				t.Fatalf("path = %q, want %q", path, "C:/tmp/drive9-webdav.pid")
			}
			return nil
		},
		pidAlive: func(pid int) bool {
			t.Fatalf("pidAlive should not be called on Windows; terminate handles the wait path")
			return false
		},
		now:       func() time.Time { return now },
		sleep:     func(d time.Duration) { now = now.Add(d) },
		printErrf: func(string, ...any) {},
	}

	if err := runUmount([]string{"x:\\"}, deps); err != nil {
		t.Fatalf("runUmount: %v", err)
	}
	if runCalls != 1 {
		t.Fatalf("runCalls = %d, want 1", runCalls)
	}
	if terminateCalls != 1 {
		t.Fatalf("terminateCalls = %d, want 1", terminateCalls)
	}
	if removeCalls != 1 {
		t.Fatalf("removeCalls = %d, want 1", removeCalls)
	}
}

func TestRunUmountWindowsStillCleansUpAfterUnmountFailure(t *testing.T) {
	now := time.Unix(100, 0)
	runErr := errors.New("net use failed")
	runCalls := 0
	terminateCalls := 0
	removeCalls := 0
	deps := umountDeps{
		goos:     "windows",
		lookPath: fakeLookPath(nil),
		run: func(argv []string) error {
			runCalls++
			want := []string{"net", "use", "X:", "/delete", "/y"}
			if !reflect.DeepEqual(argv, want) {
				t.Fatalf("argv = %v, want %v", argv, want)
			}
			return runErr
		},
		readProcessState: func(mountPoint string) (mountstate.ProcessState, string, error) {
			if mountPoint != "X:\\" {
				t.Fatalf("mountPoint = %q, want %q", mountPoint, "X:\\")
			}
			return mountstate.ProcessState{PID: 4321, CreationTime: 99}, "C:/tmp/drive9-webdav.pid", nil
		},
		terminateState: func(state mountstate.ProcessState, waitTimeout time.Duration) error {
			terminateCalls++
			if state.PID != 4321 {
				t.Fatalf("pid = %d, want 4321", state.PID)
			}
			if state.CreationTime != 99 {
				t.Fatalf("creationTime = %d, want 99", state.CreationTime)
			}
			if waitTimeout != 60*time.Second {
				t.Fatalf("waitTimeout = %s, want %s", waitTimeout, 60*time.Second)
			}
			return nil
		},
		remove: func(path string) error {
			removeCalls++
			if path != "C:/tmp/drive9-webdav.pid" {
				t.Fatalf("path = %q, want %q", path, "C:/tmp/drive9-webdav.pid")
			}
			return nil
		},
		pidAlive:  func(int) bool { t.Fatal("pidAlive should not be called on Windows"); return false },
		now:       func() time.Time { return now },
		sleep:     func(d time.Duration) { now = now.Add(d) },
		printErrf: func(string, ...any) {},
	}

	err := runUmount([]string{"x:\\"}, deps)
	if !errors.Is(err, runErr) {
		t.Fatalf("err = %v, want %v", err, runErr)
	}
	if runCalls != 1 {
		t.Fatalf("runCalls = %d, want 1", runCalls)
	}
	if terminateCalls != 1 {
		t.Fatalf("terminateCalls = %d, want 1", terminateCalls)
	}
	if removeCalls != 1 {
		t.Fatalf("removeCalls = %d, want 1", removeCalls)
	}
}

func TestTerminateProcessWaitsAfterSuccessfulKill(t *testing.T) {
	oldWaitForProcessExit := waitForProcessExit
	t.Cleanup(func() { waitForProcessExit = oldWaitForProcessExit })

	called := false
	waitForProcessExit = func(pid int, timeout time.Duration) error {
		called = true
		if pid <= 0 {
			t.Fatalf("pid = %d, want > 0", pid)
		}
		if timeout != 50*time.Millisecond {
			t.Fatalf("timeout = %s, want %s", timeout, 50*time.Millisecond)
		}
		return nil
	}

	cmd := exec.Command("cmd", "/c", "pause")
	if runtime.GOOS != "windows" {
		cmd = exec.Command("sh", "-c", "sleep 30")
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper process: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()

	if err := terminateProcess(cmd.Process.Pid, 50*time.Millisecond); err != nil {
		t.Fatalf("terminateProcess: %v", err)
	}
	if !called {
		t.Fatal("waitForProcessExit was not called after successful kill")
	}
	_, _ = cmd.Process.Wait()
}

func TestRunUmountWindowsStalePIDFileDoesNotTerminateProcess(t *testing.T) {
	runCalls := 0
	removeCalls := 0
	deps := umountDeps{
		goos:     "windows",
		lookPath: fakeLookPath(nil),
		run: func(argv []string) error {
			runCalls++
			want := []string{"net", "use", "X:", "/delete", "/y"}
			if !reflect.DeepEqual(argv, want) {
				t.Fatalf("argv = %v, want %v", argv, want)
			}
			return nil
		},
		readProcessState: func(mountPoint string) (mountstate.ProcessState, string, error) {
			if mountPoint != "X:\\" {
				t.Fatalf("mountPoint = %q, want %q", mountPoint, "X:\\")
			}
			return mountstate.ProcessState{PID: 9999, CreationTime: 123}, "C:/tmp/drive9-webdav.pid", nil
		},
		terminateState: func(state mountstate.ProcessState, waitTimeout time.Duration) error {
			if state.PID != 9999 {
				t.Fatalf("pid = %d, want 9999", state.PID)
			}
			return fmt.Errorf("%w: pid reused", errMountProcessStateStale)
		},
		remove: func(path string) error {
			removeCalls++
			if path != "C:/tmp/drive9-webdav.pid" {
				t.Fatalf("path = %q, want %q", path, "C:/tmp/drive9-webdav.pid")
			}
			return nil
		},
		now:       time.Now,
		sleep:     func(time.Duration) {},
		printErrf: func(string, ...any) {},
	}

	if err := runUmount([]string{"x:\\"}, deps); err != nil {
		t.Fatalf("runUmount: %v", err)
	}
	if runCalls != 1 {
		t.Fatalf("runCalls = %d, want 1", runCalls)
	}
	if removeCalls != 1 {
		t.Fatalf("removeCalls = %d, want 1", removeCalls)
	}
}

func TestRunUmountWindowsPermissionErrorKeepsPIDFile(t *testing.T) {
	runCalls := 0
	removeCalls := 0
	inspectErr := errors.New("access is denied")
	deps := umountDeps{
		goos:     "windows",
		lookPath: fakeLookPath(nil),
		run: func(argv []string) error {
			runCalls++
			want := []string{"net", "use", "X:", "/delete", "/y"}
			if !reflect.DeepEqual(argv, want) {
				t.Fatalf("argv = %v, want %v", argv, want)
			}
			return nil
		},
		readProcessState: func(mountPoint string) (mountstate.ProcessState, string, error) {
			if mountPoint != "X:\\" {
				t.Fatalf("mountPoint = %q, want %q", mountPoint, "X:\\")
			}
			return mountstate.ProcessState{PID: 4321, CreationTime: 99}, "C:/tmp/drive9-webdav.pid", nil
		},
		terminateState: func(state mountstate.ProcessState, waitTimeout time.Duration) error {
			if state.PID != 4321 {
				t.Fatalf("pid = %d, want 4321", state.PID)
			}
			if state.CreationTime != 99 {
				t.Fatalf("creationTime = %d, want 99", state.CreationTime)
			}
			return inspectErr
		},
		remove: func(path string) error {
			removeCalls++
			if path != "C:/tmp/drive9-webdav.pid" {
				t.Fatalf("path = %q, want %q", path, "C:/tmp/drive9-webdav.pid")
			}
			return nil
		},
		now:       time.Now,
		sleep:     func(time.Duration) {},
		printErrf: func(string, ...any) {},
	}

	err := runUmount([]string{"x:\\"}, deps)
	if err == nil {
		t.Fatal("expected permission error")
	}
	if !errors.Is(err, inspectErr) {
		t.Fatalf("err = %v, want wrapped %v", err, inspectErr)
	}
	if runCalls != 1 {
		t.Fatalf("runCalls = %d, want 1", runCalls)
	}
	if removeCalls != 0 {
		t.Fatalf("removeCalls = %d, want 0", removeCalls)
	}
}

// TestResolveMountCredentials_OwnerFromResolver binds a mount to an owner
// API key sourced from the resolver (no --api-key flag). Asserts that
// apiKey routes through MountOptions.APIKey and token stays empty, which
// in pkg/fuse.Mount dispatches to client.New (tenantAuthMiddleware path).
func TestResolveMountCredentials_OwnerFromResolver(t *testing.T) {
	r := ResolvedCredentials{
		Kind:   CredentialOwner,
		Server: "https://owner.example",
		APIKey: "sk-owner",
	}
	server, apiKey, token, err := resolveMountCredentials(r, "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if server != "https://owner.example" {
		t.Fatalf("server = %q", server)
	}
	if apiKey != "sk-owner" {
		t.Fatalf("apiKey = %q, want sk-owner", apiKey)
	}
	if token != "" {
		t.Fatalf("token = %q, want empty (owner path)", token)
	}
}

// TestResolveMountCredentials_DelegatedFromResolver binds a mount to a
// delegated JWT sourced from the resolver (active context or
// DRIVE9_VAULT_TOKEN). Asserts that token routes through MountOptions.Token
// and apiKey stays empty, which in pkg/fuse.Mount dispatches to
// client.NewWithToken (capabilityAuthMiddleware path).
func TestResolveMountCredentials_DelegatedFromResolver(t *testing.T) {
	r := ResolvedCredentials{
		Kind:   CredentialDelegated,
		Server: "https://delegated.example",
		Token:  "jwt-aaa.bbb.ccc",
	}
	server, apiKey, token, err := resolveMountCredentials(r, "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if server != "https://delegated.example" {
		t.Fatalf("server = %q", server)
	}
	if apiKey != "" {
		t.Fatalf("apiKey = %q, want empty (delegated path)", apiKey)
	}
	if token != "jwt-aaa.bbb.ccc" {
		t.Fatalf("token = %q, want jwt-aaa.bbb.ccc", token)
	}
}

// TestResolveMountCredentials_Invariant6Snapshot is the CLI-layer half of
// Invariant #6: the credential captured at mount time MUST NOT be
// retroactively overridden by a later resolver snapshot (e.g. `ctx use`
// between call and mount). We simulate by taking two independent
// snapshots and asserting both were captured as-of their respective
// resolver states. There is no shared mutable state the second call can
// mutate into the first.
func TestResolveMountCredentials_Invariant6Snapshot(t *testing.T) {
	first := ResolvedCredentials{Kind: CredentialDelegated, Server: "https://s.example", Token: "jwt-original"}
	_, _, tok1, err := resolveMountCredentials(first, "", "")
	if err != nil {
		t.Fatalf("first: %v", err)
	}

	// Simulate `ctx use other-context` happening between two mount
	// attempts. Since the helper is pure, the second call cannot affect
	// the first's result - the first mount's binding is already frozen
	// into its returned triple.
	second := ResolvedCredentials{Kind: CredentialOwner, Server: "https://s.example", APIKey: "sk-rotated"}
	_, api2, tok2, err := resolveMountCredentials(second, "", "")
	if err != nil {
		t.Fatalf("second: %v", err)
	}

	if tok1 != "jwt-original" {
		t.Fatalf("first mount token = %q, want jwt-original (Invariant #6: first mount binding is frozen)", tok1)
	}
	if api2 != "sk-rotated" || tok2 != "" {
		t.Fatalf("second mount (apiKey=%q, token=%q) want (sk-rotated, empty)", api2, tok2)
	}
}

// TestResolveMountCredentials_FlagAPIKeyBeatsResolver documents that an
// explicit --api-key flag forces the owner path even when the resolver
// would otherwise return a delegated token. The flag is owner-only by
// construction; there is no --token flag.
func TestResolveMountCredentials_FlagAPIKeyBeatsResolver(t *testing.T) {
	r := ResolvedCredentials{
		Kind:   CredentialDelegated,
		Server: "https://s.example",
		Token:  "jwt-should-be-ignored",
	}
	_, apiKey, token, err := resolveMountCredentials(r, "", "sk-flag-owner")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if apiKey != "sk-flag-owner" {
		t.Fatalf("apiKey = %q, want sk-flag-owner (flag wins)", apiKey)
	}
	if token != "" {
		t.Fatalf("token = %q, want empty when --api-key given", token)
	}
}

// TestResolveMountCredentials_MissingCredential rejects mounts where
// neither a flag nor resolver produced a credential. Mount must refuse
// rather than silently succeed against a public endpoint.
func TestResolveMountCredentials_MissingCredential(t *testing.T) {
	r := ResolvedCredentials{Server: "https://s.example"} // Kind=CredentialNone
	_, _, _, err := resolveMountCredentials(r, "", "")
	if err == nil {
		t.Fatal("expected error when no credential is available")
	}
}

// TestResolveMountCredentials_MissingServer rejects mounts with no
// server URL (neither flag, env, nor config).
func TestResolveMountCredentials_MissingServer(t *testing.T) {
	r := ResolvedCredentials{Kind: CredentialOwner, APIKey: "sk-owner"}
	_, _, _, err := resolveMountCredentials(r, "", "")
	if err == nil {
		t.Fatal("expected error when no server URL is available")
	}
}

func TestMountCmdPassesLegacyDirStatFallbackOption(t *testing.T) {
	oldMountFuse := mountFuse
	t.Cleanup(func() { mountFuse = oldMountFuse })

	var got *mountFuseOptions
	mountFuse = func(opts *mountFuseOptions) error {
		copied := *opts
		got = &copied
		return nil
	}

	err := MountCmd([]string{
		"--mode", "fuse",
		"--server", "https://drive9.example",
		"--api-key", "sk-test",
		"--legacy-dir-stat-fallback",
		":/repo",
		t.TempDir(),
	})
	if err != nil {
		t.Fatalf("MountCmd: %v", err)
	}
	if got == nil {
		t.Fatal("mountFuse was not called")
	}
	if !got.LegacyDirStatFallback {
		t.Fatal("LegacyDirStatFallback = false, want true")
	}
	if got.RemoteRoot != "/repo" {
		t.Fatalf("RemoteRoot = %q, want /repo", got.RemoteRoot)
	}
}

func TestMountCmdLeavesLegacyDirStatFallbackDisabledByDefault(t *testing.T) {
	oldMountFuse := mountFuse
	t.Cleanup(func() { mountFuse = oldMountFuse })

	var got *mountFuseOptions
	mountFuse = func(opts *mountFuseOptions) error {
		copied := *opts
		got = &copied
		return nil
	}

	err := MountCmd([]string{
		"--mode", "fuse",
		"--server", "https://drive9.example",
		"--api-key", "sk-test",
		t.TempDir(),
	})
	if err != nil {
		t.Fatalf("MountCmd: %v", err)
	}
	if got == nil {
		t.Fatal("mountFuse was not called")
	}
	if got.LegacyDirStatFallback {
		t.Fatal("LegacyDirStatFallback = true, want false")
	}
}

func TestMountCmdPassesTrustLocalEventsOption(t *testing.T) {
	oldMountFuse := mountFuse
	t.Cleanup(func() { mountFuse = oldMountFuse })

	var got *mountFuseOptions
	mountFuse = func(opts *mountFuseOptions) error {
		copied := *opts
		got = &copied
		return nil
	}

	err := MountCmd([]string{
		"--mode", "fuse",
		"--server", "https://drive9.example",
		"--api-key", "sk-test",
		"--trust-process-local-events",
		t.TempDir(),
	})
	if err != nil {
		t.Fatalf("MountCmd: %v", err)
	}
	if got == nil {
		t.Fatal("mountFuse was not called")
	}
	if !got.TrustLocalEvents {
		t.Fatal("TrustLocalEvents = false, want true")
	}
}

func TestMountCmdLeavesTrustLocalEventsDisabledByDefault(t *testing.T) {
	oldMountFuse := mountFuse
	t.Cleanup(func() { mountFuse = oldMountFuse })

	var got *mountFuseOptions
	mountFuse = func(opts *mountFuseOptions) error {
		copied := *opts
		got = &copied
		return nil
	}

	err := MountCmd([]string{
		"--mode", "fuse",
		"--server", "https://drive9.example",
		"--api-key", "sk-test",
		t.TempDir(),
	})
	if err != nil {
		t.Fatalf("MountCmd: %v", err)
	}
	if got == nil {
		t.Fatal("mountFuse was not called")
	}
	if got.TrustLocalEvents {
		t.Fatal("TrustLocalEvents = true, want false")
	}
}

func TestMountCmdRejectsTrustLocalEventsWithWebDAV(t *testing.T) {
	err := MountCmd([]string{
		"--mode", "webdav",
		"--server", "https://drive9.example",
		"--api-key", "sk-test",
		"--trust-process-local-events",
		t.TempDir(),
	})
	if err == nil || !strings.Contains(err.Error(), "--trust-process-local-events") {
		t.Fatalf("MountCmd error = %v, want trust-process-local-events validation error", err)
	}
}

func TestMountCmdPassesReadCacheMaxFileOption(t *testing.T) {
	oldMountFuse := mountFuse
	t.Cleanup(func() { mountFuse = oldMountFuse })

	var got *mountFuseOptions
	mountFuse = func(opts *mountFuseOptions) error {
		copied := *opts
		got = &copied
		return nil
	}

	err := MountCmd([]string{
		"--mode", "fuse",
		"--server", "https://drive9.example",
		"--api-key", "sk-test",
		"--read-cache-max-file-mb", "4",
		t.TempDir(),
	})
	if err != nil {
		t.Fatalf("MountCmd: %v", err)
	}
	if got == nil {
		t.Fatal("mountFuse was not called")
	}
	if got.ReadCacheMaxFileBytes != 4<<20 {
		t.Fatalf("ReadCacheMaxFileBytes = %d, want %d", got.ReadCacheMaxFileBytes, int64(4<<20))
	}
}

func TestMountCmdRejectsZeroDiskReadCacheFreeRatio(t *testing.T) {
	err := MountCmd([]string{
		"--mode", "fuse",
		"--server", "https://drive9.example",
		"--api-key", "sk-test",
		"--disk-read-cache-free-ratio", "0",
		t.TempDir(),
	})
	if err == nil || !strings.Contains(err.Error(), "--disk-read-cache-free-ratio") {
		t.Fatalf("MountCmd error = %v, want disk-read-cache-free-ratio validation error", err)
	}
}

func TestMountCmdPassesReadConcurrencyOption(t *testing.T) {
	oldMountFuse := mountFuse
	t.Cleanup(func() { mountFuse = oldMountFuse })

	var got *mountFuseOptions
	mountFuse = func(opts *mountFuseOptions) error {
		copied := *opts
		got = &copied
		return nil
	}

	err := MountCmd([]string{
		"--mode", "fuse",
		"--server", "https://drive9.example",
		"--api-key", "sk-test",
		"--read-concurrency", "12",
		t.TempDir(),
	})
	if err != nil {
		t.Fatalf("MountCmd: %v", err)
	}
	if got == nil {
		t.Fatal("mountFuse was not called")
	}
	if got.ReadConcurrency != 12 {
		t.Fatalf("ReadConcurrency = %d, want 12", got.ReadConcurrency)
	}
}

func TestMountCmdPassesParallelReadOptions(t *testing.T) {
	oldMountFuse := mountFuse
	t.Cleanup(func() { mountFuse = oldMountFuse })

	var got *mountFuseOptions
	mountFuse = func(opts *mountFuseOptions) error {
		copied := *opts
		got = &copied
		return nil
	}

	err := MountCmd([]string{
		"--mode", "fuse",
		"--server", "https://drive9.example",
		"--api-key", "sk-test",
		"--parallel-read-concurrency", "3",
		"--parallel-read-block-size-mb", "2",
		t.TempDir(),
	})
	if err != nil {
		t.Fatalf("MountCmd: %v", err)
	}
	if got == nil {
		t.Fatal("mountFuse was not called")
	}
	if got.ParallelReadConcurrency != 3 {
		t.Fatalf("ParallelReadConcurrency = %d, want 3", got.ParallelReadConcurrency)
	}
	if got.ParallelReadBlockSize != 2<<20 {
		t.Fatalf("ParallelReadBlockSize = %d, want %d", got.ParallelReadBlockSize, int64(2<<20))
	}
}

func TestMountCmdPassesUploadConcurrencyOption(t *testing.T) {
	oldMountFuse := mountFuse
	t.Cleanup(func() { mountFuse = oldMountFuse })

	var got *mountFuseOptions
	mountFuse = func(opts *mountFuseOptions) error {
		copied := *opts
		got = &copied
		return nil
	}

	err := MountCmd([]string{
		"--mode", "fuse",
		"--server", "https://drive9.example",
		"--api-key", "sk-test",
		"--upload-concurrency", "16",
		t.TempDir(),
	})
	if err != nil {
		t.Fatalf("MountCmd: %v", err)
	}
	if got == nil {
		t.Fatal("mountFuse was not called")
	}
	if got.UploadConcurrency != 16 {
		t.Fatalf("UploadConcurrency = %d, want 16", got.UploadConcurrency)
	}
}

func TestMountCmdRejectsInvalidReadConcurrency(t *testing.T) {
	err := MountCmd([]string{
		"--mode", "fuse",
		"--server", "https://drive9.example",
		"--api-key", "sk-test",
		"--read-concurrency", "0",
		t.TempDir(),
	})
	if err == nil || !strings.Contains(err.Error(), "--read-concurrency") {
		t.Fatalf("MountCmd error = %v, want read-concurrency validation error", err)
	}
}

func TestMountCmdRejectsInvalidParallelReadConcurrency(t *testing.T) {
	err := MountCmd([]string{
		"--mode", "fuse",
		"--server", "https://drive9.example",
		"--api-key", "sk-test",
		"--parallel-read-concurrency", "0",
		t.TempDir(),
	})
	if err == nil || !strings.Contains(err.Error(), "--parallel-read-concurrency") {
		t.Fatalf("MountCmd error = %v, want parallel-read-concurrency validation error", err)
	}
}

func TestMountCmdRejectsInvalidParallelReadBlockSize(t *testing.T) {
	err := MountCmd([]string{
		"--mode", "fuse",
		"--server", "https://drive9.example",
		"--api-key", "sk-test",
		"--parallel-read-block-size-mb", "0",
		t.TempDir(),
	})
	if err == nil || !strings.Contains(err.Error(), "--parallel-read-block-size-mb") {
		t.Fatalf("MountCmd error = %v, want parallel-read-block-size validation error", err)
	}
}

func TestMountCmdRejectsInvalidUploadConcurrency(t *testing.T) {
	err := MountCmd([]string{
		"--mode", "fuse",
		"--server", "https://drive9.example",
		"--api-key", "sk-test",
		"--upload-concurrency", "0",
		t.TempDir(),
	})
	if err == nil || !strings.Contains(err.Error(), "--upload-concurrency") {
		t.Fatalf("MountCmd error = %v, want upload-concurrency validation error", err)
	}
}

func TestMountCmdPassesFuseSyncReadOption(t *testing.T) {
	oldMountFuse := mountFuse
	t.Cleanup(func() { mountFuse = oldMountFuse })

	var got *mountFuseOptions
	mountFuse = func(opts *mountFuseOptions) error {
		copied := *opts
		got = &copied
		return nil
	}

	err := MountCmd([]string{
		"--mode", "fuse",
		"--server", "https://drive9.example",
		"--api-key", "sk-test",
		"--fuse-sync-read",
		t.TempDir(),
	})
	if err != nil {
		t.Fatalf("MountCmd: %v", err)
	}
	if got == nil {
		t.Fatal("mountFuse was not called")
	}
	if !got.SyncRead {
		t.Fatal("SyncRead = false, want true")
	}
}

func TestMountCmdMapsDurabilityOption(t *testing.T) {
	oldMountFuse := mountFuse
	t.Cleanup(func() { mountFuse = oldMountFuse })

	tests := []struct {
		name       string
		args       []string
		wantSync   fuseSyncMode
		wantPolicy fuseWritePolicy
	}{
		{
			name:       "default",
			wantSync:   fuseSyncModeAuto,
			wantPolicy: fuseWritePolicyWriteBack,
		},
		{
			name:       "interactive",
			args:       []string{"--durability", "interactive"},
			wantSync:   fuseSyncModeInteractive,
			wantPolicy: fuseWritePolicyWriteBack,
		},
		{
			name:       "fsync",
			args:       []string{"--durability", "fsync"},
			wantSync:   fuseSyncModeStrict,
			wantPolicy: fuseWritePolicyWriteBack,
		},
		{
			name:       "close-sync",
			args:       []string{"--durability", "close-sync"},
			wantSync:   fuseSyncModeStrict,
			wantPolicy: fuseWritePolicyCloseSync,
		},
		{
			name:       "write-sync",
			args:       []string{"--durability", "write-sync"},
			wantSync:   fuseSyncModeStrict,
			wantPolicy: fuseWritePolicyWriteSync,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got *mountFuseOptions
			mountFuse = func(opts *mountFuseOptions) error {
				copied := *opts
				got = &copied
				return nil
			}

			args := []string{
				"--mode", "fuse",
				"--server", "https://drive9.example",
				"--api-key", "sk-test",
			}
			args = append(args, tt.args...)
			args = append(args, t.TempDir())

			if err := MountCmd(args); err != nil {
				t.Fatalf("MountCmd: %v", err)
			}
			if got == nil {
				t.Fatal("mountFuse was not called")
			}
			if got.SyncMode != tt.wantSync || got.WritePolicy != tt.wantPolicy {
				t.Fatalf("durability mapped to sync=%q policy=%q, want sync=%q policy=%q", got.SyncMode, got.WritePolicy, tt.wantSync, tt.wantPolicy)
			}
		})
	}
}

func TestMountCmdLeavesDefaultTTLsUnsetForFuseDefaults(t *testing.T) {
	oldMountFuse := mountFuse
	t.Cleanup(func() { mountFuse = oldMountFuse })

	var got *mountFuseOptions
	mountFuse = func(opts *mountFuseOptions) error {
		copied := *opts
		got = &copied
		return nil
	}

	err := MountCmd([]string{
		"--mode", "fuse",
		"--server", "https://drive9.example",
		"--api-key", "sk-test",
		t.TempDir(),
	})
	if err != nil {
		t.Fatalf("MountCmd: %v", err)
	}
	if got == nil {
		t.Fatal("mountFuse was not called")
	}
	if got.DirTTL != 0 || got.AttrTTL != 0 || got.EntryTTL != 0 {
		t.Fatalf("default TTLs = dir %v attr %v entry %v, want all unset", got.DirTTL, got.AttrTTL, got.EntryTTL)
	}
	if got.LookupRetryCount != 0 || got.LookupRetryTimeout != 0 {
		t.Fatalf("default lookup retry = count %d timeout %v, want all unset", got.LookupRetryCount, got.LookupRetryTimeout)
	}
}

func TestMountCmdPreservesExplicitTTLs(t *testing.T) {
	oldMountFuse := mountFuse
	t.Cleanup(func() { mountFuse = oldMountFuse })

	var got *mountFuseOptions
	mountFuse = func(opts *mountFuseOptions) error {
		copied := *opts
		got = &copied
		return nil
	}

	err := MountCmd([]string{
		"--mode", "fuse",
		"--server", "https://drive9.example",
		"--api-key", "sk-test",
		"--profile", "interactive",
		"--dir-ttl", "5s",
		"--attr-ttl", "6s",
		"--entry-ttl", "7s",
		t.TempDir(),
	})
	if err != nil {
		t.Fatalf("MountCmd: %v", err)
	}
	if got == nil {
		t.Fatal("mountFuse was not called")
	}
	if got.DirTTL != 5*time.Second {
		t.Fatalf("DirTTL = %v, want 5s", got.DirTTL)
	}
	if got.AttrTTL != 6*time.Second {
		t.Fatalf("AttrTTL = %v, want 6s", got.AttrTTL)
	}
	if got.EntryTTL != 7*time.Second {
		t.Fatalf("EntryTTL = %v, want 7s", got.EntryTTL)
	}
}

func TestMountCmdCodingAgentProfilePassesPolicyOptions(t *testing.T) {
	oldMountFuse := mountFuse
	t.Cleanup(func() { mountFuse = oldMountFuse })

	var got *mountFuseOptions
	mountFuse = func(opts *mountFuseOptions) error {
		copied := *opts
		got = &copied
		return nil
	}

	localRoot := t.TempDir()
	err := MountCmd([]string{
		"--mode", "fuse",
		"--server", "https://drive9.example",
		"--api-key", "sk-test",
		"--profile", "coding-agent",
		"--local-root", " " + localRoot + " ",
		"--no-auto-unpack",
		"--local-only", "**/.custom-cache/**",
		"--remote-only", "**/node_modules/keep/**",
		t.TempDir(),
	})
	if err != nil {
		t.Fatalf("MountCmd: %v", err)
	}
	if got == nil {
		t.Fatal("mountFuse was not called")
	}
	if got.Profile != "coding-agent" {
		t.Fatalf("Profile = %q, want coding-agent", got.Profile)
	}
	if got.LocalRoot != localRoot {
		t.Fatalf("LocalRoot = %q, want %q", got.LocalRoot, localRoot)
	}
	wantLocalOnly := append(builtinCodingAgentLocalOnlyPatterns(), "**/.custom-cache/**")
	if !reflect.DeepEqual(got.LocalOnlyPatterns, wantLocalOnly) {
		t.Fatalf("LocalOnlyPatterns = %v, want %v", got.LocalOnlyPatterns, wantLocalOnly)
	}
	if !reflect.DeepEqual(got.RemoteOnlyPatterns, []string{"**/node_modules/keep/**"}) {
		t.Fatalf("RemoteOnlyPatterns = %v", got.RemoteOnlyPatterns)
	}
}

func TestMountCmdCodingAgentProfileGeneratesDefaultLocalRoot(t *testing.T) {
	oldMountFuse := mountFuse
	t.Cleanup(func() { mountFuse = oldMountFuse })

	t.Setenv("HOME", t.TempDir())
	var got *mountFuseOptions
	mountFuse = func(opts *mountFuseOptions) error {
		copied := *opts
		got = &copied
		return nil
	}

	err := MountCmd([]string{
		"--mode", "fuse",
		"--server", "https://drive9.example",
		"--api-key", "sk-test",
		"--profile", "coding-agent",
		t.TempDir(),
	})
	if err != nil {
		t.Fatalf("MountCmd: %v", err)
	}
	if got == nil {
		t.Fatal("mountFuse was not called")
	}
	want, err := defaultMountLocalRoot("https://drive9.example", "/", mountCredentialCacheKey("sk-test", ""))
	if err != nil {
		t.Fatalf("defaultMountLocalRoot: %v", err)
	}
	if got.LocalRoot != want {
		t.Fatalf("LocalRoot = %q, want default %q", got.LocalRoot, want)
	}
	if strings.Contains(got.LocalRoot, "coding-agent") {
		t.Fatalf("LocalRoot = %q, should not include profile name", got.LocalRoot)
	}
}

func TestMountCmdLocalPolicyFlagsRequireOverlayProfile(t *testing.T) {
	err := MountCmd([]string{
		"--mode", "fuse",
		"--server", "https://drive9.example",
		"--api-key", "sk-test",
		"--profile", "none",
		"--local-only", "**/.git/**",
		t.TempDir(),
	})
	if err == nil || !strings.Contains(err.Error(), "overlay profile") {
		t.Fatalf("error = %v, want overlay profile validation error", err)
	}
}

func TestMountCmdLocalRootMustBeAbsolute(t *testing.T) {
	err := MountCmd([]string{
		"--mode", "fuse",
		"--server", "https://drive9.example",
		"--api-key", "sk-test",
		"--profile", "coding-agent",
		"--local-root", "relative/root",
		t.TempDir(),
	})
	if err == nil || !strings.Contains(err.Error(), "--local-root") {
		t.Fatalf("error = %v, want local root validation error", err)
	}
}

func TestMountCmdLocalPolicyPatternsRejectUnsafePaths(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		want    string
	}{
		{name: "backslash", pattern: `**\\.git\\**`, want: "path contains backslash"},
		{name: "dotdot", pattern: "**/../.git/**", want: `path contains ".." segment`},
		{name: "dot", pattern: "**/./.git/**", want: `path contains "." segment`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := MountCmd([]string{
				"--mode", "fuse",
				"--server", "https://drive9.example",
				"--api-key", "sk-test",
				"--profile", "coding-agent",
				"--local-root", t.TempDir(),
				"--local-only", test.pattern,
				t.TempDir(),
			})
			if err == nil || !strings.Contains(err.Error(), "invalid local policy pattern") || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want invalid policy pattern containing %q", err, test.want)
			}
		})
	}
}

func TestValidateLookupRetryFlags(t *testing.T) {
	if err := validateLookupRetryFlags(0, 0, false, false); err != nil {
		t.Fatalf("omitted lookup retry flags should be allowed: %v", err)
	}

	if err := validateLookupRetryFlags(2, 250*time.Millisecond, true, true); err != nil {
		t.Fatalf("validateLookupRetryFlags() unexpected error: %v", err)
	}

	if err := validateLookupRetryFlags(0, 0, true, false); err != nil {
		t.Fatalf("count=0 should be allowed to disable retries: %v", err)
	}

	if err := validateLookupRetryFlags(2, 0, true, true); err == nil || !strings.Contains(err.Error(), "--lookup-retry-timeout") {
		t.Fatalf("timeout=0 error = %v, want timeout validation error", err)
	}

	if err := validateLookupRetryFlags(-1, 250*time.Millisecond, true, true); err == nil || !strings.Contains(err.Error(), "--lookup-retry-count") {
		t.Fatalf("count=-1 error = %v, want count validation error", err)
	}

	if err := validateLookupRetryFlags(2, -time.Millisecond, true, true); err == nil || !strings.Contains(err.Error(), "--lookup-retry-timeout") {
		t.Fatalf("timeout<0 error = %v, want timeout validation error", err)
	}
}

func TestValidateReadDirPrefetchFlags(t *testing.T) {
	if err := validateReadDirPrefetchFlags(32, 50_000, 1<<20, time.Second); err != nil {
		t.Fatalf("validateReadDirPrefetchFlags() unexpected error: %v", err)
	}

	if err := validateReadDirPrefetchFlags(0, 50_000, 1<<20, time.Second); err == nil || !strings.Contains(err.Error(), "--readdir-prefetch-max-files") {
		t.Fatalf("max-files=0 error = %v, want max-files validation error", err)
	}
	if err := validateReadDirPrefetchFlags(32, 0, 1<<20, time.Second); err == nil || !strings.Contains(err.Error(), "--readdir-prefetch-max-file-bytes") {
		t.Fatalf("max-file-bytes=0 error = %v, want max-file-bytes validation error", err)
	}
	if err := validateReadDirPrefetchFlags(32, 50_000, 0, time.Second); err == nil || !strings.Contains(err.Error(), "--readdir-prefetch-max-bytes") {
		t.Fatalf("max-bytes=0 error = %v, want max-bytes validation error", err)
	}
	if err := validateReadDirPrefetchFlags(32, 50_000, 1<<20, 0); err == nil || !strings.Contains(err.Error(), "--readdir-prefetch-timeout") {
		t.Fatalf("timeout=0 error = %v, want timeout validation error", err)
	}
}

func TestNormalizeLookupRetryCount(t *testing.T) {
	count := lookupRetryCountFlagValue(false, 0)
	if count != 0 {
		t.Fatalf("omitted count = %d, want 0 for FUSE default", count)
	}

	count = normalizeLookupRetryCount(2)
	if count != 2 {
		t.Fatalf("normalized positive count = %d, want 2", count)
	}

	count = normalizeLookupRetryCount(0)
	if count != -1 {
		t.Fatalf("count=0 normalization = %d, want -1", count)
	}
}

// ---------------------------------------------------------------------------
// Row A - only the CURRENT backend keyword ("vault") is special. All other
// bare-word first positionals (not :/path remote sources) are rejected when
// two args are given, since the 2-arg form requires a remote source prefix.
// ---------------------------------------------------------------------------

func TestMountCmd_BareWordFirstArgRejectsNonRemoteSource(t *testing.T) {
	for _, s := range []string{"kv", "s3", "gcs", "nfs", "mnt", "tmp", "vaultdir", "data"} {
		err := MountCmd([]string{s, "/mnt/x"})
		if err == nil {
			t.Fatalf("%q: expected error for non-remote first arg", s)
		}
		if got := err.Error(); !strings.Contains(got, "must be a remote source") {
			t.Fatalf("%q: error = %q, want remote-source rejection", s, got)
		}
	}
}

func TestMountCmd_VaultStillDispatchesSeparately(t *testing.T) {
	err := MountCmd([]string{"vault", "/mnt/a", "/mnt/b"})
	if err == nil {
		t.Fatal("expected vault subcommand arity error")
	}
	if got := err.Error(); !strings.Contains(got, "drive9 mount vault: exactly one mountpoint required") {
		t.Fatalf("error = %q, want vault-specific arity rejection", got)
	}
}

func TestRemoteRootError_404ShowsHint(t *testing.T) {
	err := remoteRootError("/data", &client.StatusError{StatusCode: http.StatusNotFound, Message: "not found"})
	got := err.Error()
	if !strings.Contains(got, "does not exist") {
		t.Fatalf("error = %q, want 'does not exist'", got)
	}
	if !strings.Contains(got, "drive9 fs mkdir :/data") {
		t.Fatalf("error = %q, want mkdir hint", got)
	}
	if !strings.Contains(got, "drive9 mount :/data") {
		t.Fatalf("error = %q, want retry hint", got)
	}
}

func TestRemoteRootError_Non404WrapsOriginal(t *testing.T) {
	origErr := &client.StatusError{StatusCode: http.StatusInternalServerError, Message: "server error"}
	err := remoteRootError("/data", origErr)
	got := err.Error()
	if strings.Contains(got, "does not exist") {
		t.Fatalf("500 error should not show 404 hint: %q", got)
	}
	if !strings.Contains(got, "server error") {
		t.Fatalf("error = %q, want original message wrapped", got)
	}
}

func TestValidateRemoteRoot_StatFailsListSucceeds(t *testing.T) {
	// Backend where Stat (HEAD) on directories is unsupported but List works.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			// Stat unsupported for directories on this backend (non-404).
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if r.URL.Query().Has("list") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"entries":[]}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	c := client.New(srv.URL, "")
	err := validateRemoteRoot(c, "/data")
	if err != nil {
		t.Fatalf("expected success when Stat fails but List succeeds: %v", err)
	}
}

func TestValidateRemoteRoot_BothFailWith404(t *testing.T) {
	// Both Stat (HEAD) and List return 404.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := client.New(srv.URL, "")
	err := validateRemoteRoot(c, "/data")
	if err == nil {
		t.Fatal("expected error when both Stat and List return 404")
	}
	got := err.Error()
	if !strings.Contains(got, "does not exist") {
		t.Fatalf("error = %q, want 404 hint", got)
	}
	if !strings.Contains(got, "drive9 fs mkdir :/data") {
		t.Fatalf("error = %q, want mkdir guidance", got)
	}
}

func TestValidateRemoteRoot_Stat404TrustedOverList(t *testing.T) {
	// Stat (HEAD) returns 404 - we trust it immediately and show the hint,
	// even if List would succeed (server returns empty entries for
	// non-existent paths).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		// List would succeed with empty entries
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"entries":[]}`))
	}))
	defer srv.Close()

	c := client.New(srv.URL, "")
	err := validateRemoteRoot(c, "/data")
	if err == nil {
		t.Fatal("expected error when Stat returns 404")
	}
	got := err.Error()
	if !strings.Contains(got, "does not exist") {
		t.Fatalf("error = %q, want 404 hint", got)
	}
	if !strings.Contains(got, "drive9 fs mkdir :/data") {
		t.Fatalf("error = %q, want mkdir guidance", got)
	}
}

func TestValidateRemoteRoot_StatNon404ListFails(t *testing.T) {
	// Stat (HEAD) returns non-404 error (unsupported), List also fails with 500.
	// Should show List error, not 404 hint.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"timeout"}`))
	}))
	defer srv.Close()

	c := client.New(srv.URL, "")
	err := validateRemoteRoot(c, "/data")
	if err == nil {
		t.Fatal("expected error")
	}
	got := err.Error()
	if strings.Contains(got, "does not exist") {
		t.Fatalf("non-404 Stat + List 500 should NOT show 404 hint: %q", got)
	}
	if !strings.Contains(got, "timeout") {
		t.Fatalf("error = %q, want List error message", got)
	}
}
