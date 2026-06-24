package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestVersionStringUsesDrive9Component(t *testing.T) {
	got := versionString()
	if !strings.Contains(got, "component: drive9\n") {
		t.Fatalf("versionString() missing component line: %q", got)
	}
}

func TestStartCPUProfileFromEnv(t *testing.T) {
	profilePath := filepath.Join(t.TempDir(), "drive9.cpu.pprof")
	t.Setenv("DRIVE9_PROF_CPU_PROFILE", profilePath)

	stopCPUProfile, err := startCPUProfileFromEnv()
	if err != nil {
		t.Fatalf("startCPUProfileFromEnv: %v", err)
	}

	deadline := time.Now().Add(20 * time.Millisecond)
	for time.Now().Before(deadline) {
	}

	stopCPUProfile()

	info, err := os.Stat(profilePath)
	if err != nil {
		t.Fatalf("Stat(profile): %v", err)
	}
	if info.Size() == 0 {
		t.Fatalf("profile file is empty: %s", profilePath)
	}
}

func TestDispatchLongHelpFlagShowsUsage(t *testing.T) {
	origExit := exitFunc
	origStderr := os.Stderr
	origStop := cpuProfileStop
	t.Cleanup(func() {
		exitFunc = origExit
		os.Stderr = origStderr
		cpuProfileStop = origStop
	})
	cpuProfileStop = func() {}

	var exitCodes []int
	exitFunc = func(code int) { exitCodes = append(exitCodes, code) }

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stderr = w
	done := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		done <- buf.String()
	}()

	dispatch("--help", nil)

	_ = w.Close()
	stderr := <-done

	if len(exitCodes) != 1 || exitCodes[0] != 0 {
		t.Fatalf("exit codes = %v, want [0] for explicit --help", exitCodes)
	}
	if strings.Contains(stderr, "unknown command") {
		t.Fatalf("stderr = %q, want --help to show usage directly", stderr)
	}
	for _, want := range []string{
		"usage: drive9 <command> [arguments]",
		"create [--name NAME] [--region-code CODE] [--server URL] [--json]",
		"delete [--server URL] [--api-key KEY] [--json]",
		"admin <tenant|quota> <command> [flags]",
		"ctx show [--json] [--reveal]",
		"ctx use <name>",
		"token <issue|revoke>",
		"journal <new|append|cat|find|verify>",
		"region list [--json] [--manifest-url URL]",
		"profile show [profile]",
		"mount [flags] [:/remote] <mountpoint>",
		"mount drain [--timeout duration] [--json] <mountpoint>",
		"mount vault [flags] <mountpoint>",
		"umount <mountpoint>",
		"doctor fuse",
		"update [--check]",
		"-h, -help, --help",
	} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("stderr = %q, want it to contain %q", stderr, want)
		}
	}
}

func TestDispatchHelpCommandShowsVisualTreeHelp(t *testing.T) {
	origExit := exitFunc
	origStdout := os.Stdout
	origStderr := os.Stderr
	origStop := cpuProfileStop
	t.Cleanup(func() {
		exitFunc = origExit
		os.Stdout = origStdout
		os.Stderr = origStderr
		cpuProfileStop = origStop
	})
	cpuProfileStop = func() {}

	var exitCodes []int
	exitFunc = func(code int) { exitCodes = append(exitCodes, code) }

	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	os.Stdout = stdoutW
	os.Stderr = stderrW

	stdoutDone := make(chan string, 1)
	stderrDone := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, stdoutR)
		stdoutDone <- buf.String()
	}()
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, stderrR)
		stderrDone <- buf.String()
	}()

	dispatch("help", []string{"--no-pager", "--color=never"})

	_ = stdoutW.Close()
	_ = stderrW.Close()
	stdout := <-stdoutDone
	stderr := <-stderrDone

	if len(exitCodes) != 1 || exitCodes[0] != 0 {
		t.Fatalf("exit codes = %v, want [0]", exitCodes)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty stderr", stderr)
	}
	for _, want := range []string{
		"drive9 <command> [args] [flags]",
		"drive9 fs <command> [arguments]",
		"drive9 git <clone|worktree|hydrate>",
		"Anonymous mode transfers data management rights to PingCAP",
		"--tidbcloud-public-key KEY",
		"--tidbcloud-private-key KEY",
		"drive9 region list [--json] [--manifest-url URL]",
		"REGION CODE, CLOUD PROVIDER, REGION, MODE, SERVER",
		"--manifest-url URL",
		"Connection:",
		"--read-cache-max-file-mb MB",
		"--parallel-read-concurrency N",
		"--local-only PATTERN",
		"--checkpoint REF",
		"--perf-cpu-interval DURATION",
		"drive9 mount drain [--timeout duration] [--json] <mountpoint>",
		"drive9 mount drain ./mnt --timeout=30s",
		"drive9 mount vault [flags] <mountpoint>",
		"--pack-path PATH",
		"drive9 update [--check] [--force] [--base-url URL]",
		"less -R -S",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("stdout = %q, want it to contain %q", stdout, want)
		}
	}
	if strings.Contains(stdout, "\x1b[") {
		t.Fatalf("stdout contains ANSI escapes despite --color=never: %q", stdout)
	}
}

func TestDispatchHelpCommandUsage(t *testing.T) {
	origExit := exitFunc
	origStdout := os.Stdout
	origStderr := os.Stderr
	origStop := cpuProfileStop
	t.Cleanup(func() {
		exitFunc = origExit
		os.Stdout = origStdout
		os.Stderr = origStderr
		cpuProfileStop = origStop
	})
	cpuProfileStop = func() {}

	var exitCodes []int
	exitFunc = func(code int) { exitCodes = append(exitCodes, code) }

	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	os.Stdout = stdoutW
	os.Stderr = stderrW

	stdoutDone := make(chan string, 1)
	stderrDone := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, stdoutR)
		stdoutDone <- buf.String()
	}()
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, stderrR)
		stderrDone <- buf.String()
	}()

	dispatch("help", []string{"--help"})

	_ = stdoutW.Close()
	_ = stderrW.Close()
	stdout := <-stdoutDone
	stderr := <-stderrDone

	if len(exitCodes) != 1 || exitCodes[0] != 0 {
		t.Fatalf("exit codes = %v, want [0]", exitCodes)
	}
	if !strings.HasPrefix(stdout, "usage: drive9 help") {
		t.Fatalf("stdout = %q, want help usage", stdout)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty stderr", stderr)
	}
}

func TestDispatchHelpCommandPlainShowsClassicUsage(t *testing.T) {
	stdout, stderr, exitCodes := captureDispatchOutput(t, "help", []string{"--plain"})

	if len(exitCodes) != 1 || exitCodes[0] != 0 {
		t.Fatalf("exit codes = %v, want [0]", exitCodes)
	}
	if stdout != "" {
		t.Fatalf("stdout = %q, want empty stdout", stdout)
	}
	for _, want := range []string{
		"usage: drive9 <command> [arguments]",
		"help [--plain] [--no-pager] [--color=auto|always|never]",
		"-h, -help, --help",
	} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("stderr = %q, want it to contain %q", stderr, want)
		}
	}
}

func TestDispatchHelpCommandUnknownOptionFails(t *testing.T) {
	stdout, stderr, exitCodes := captureDispatchOutput(t, "help", []string{"--bogus"})

	if len(exitCodes) != 1 || exitCodes[0] != 1 {
		t.Fatalf("exit codes = %v, want [1]", exitCodes)
	}
	if stdout != "" {
		t.Fatalf("stdout = %q, want empty stdout", stdout)
	}
	for _, want := range []string{
		`help: unknown help option "--bogus"`,
		"usage: drive9 help [--plain] [--no-pager] [--color=auto|always|never]",
	} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("stderr = %q, want it to contain %q", stderr, want)
		}
	}
}

func TestRenderDrive9VisualHelpColor(t *testing.T) {
	out := renderDrive9VisualHelp(true)
	if !strings.Contains(out, "\x1b[") {
		t.Fatalf("renderDrive9VisualHelp(true) missing ANSI color escapes")
	}
	for _, want := range []string{"drive9", "create", "fs", "git", "less", "-R", "-S"} {
		if !strings.Contains(out, want) {
			t.Fatalf("rendered help missing %q:\n%s", want, out)
		}
	}
}

func TestIsPagerClosedPipe(t *testing.T) {
	for _, err := range []error{
		io.ErrClosedPipe,
		&os.PathError{Op: "write", Path: "less", Err: syscall.EPIPE},
	} {
		if !isPagerClosedPipe(err) {
			t.Fatalf("isPagerClosedPipe(%v) = false, want true", err)
		}
	}
}

func captureDispatchOutput(t *testing.T, cmd string, args []string) (string, string, []int) {
	t.Helper()

	origExit := exitFunc
	origStdout := os.Stdout
	origStderr := os.Stderr
	origStop := cpuProfileStop
	t.Cleanup(func() {
		exitFunc = origExit
		os.Stdout = origStdout
		os.Stderr = origStderr
		cpuProfileStop = origStop
	})
	cpuProfileStop = func() {}

	var exitCodes []int
	exitFunc = func(code int) { exitCodes = append(exitCodes, code) }

	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	os.Stdout = stdoutW
	os.Stderr = stderrW

	stdoutDone := make(chan string, 1)
	stderrDone := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, stdoutR)
		stdoutDone <- buf.String()
	}()
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, stderrR)
		stderrDone <- buf.String()
	}()

	dispatch(cmd, args)

	_ = stdoutW.Close()
	_ = stderrW.Close()
	return <-stdoutDone, <-stderrDone, exitCodes
}

func TestDispatchSubcommandHelpShowsUsageWithoutFatalPrefix(t *testing.T) {
	for _, tc := range []struct {
		name      string
		cmd       string
		args      []string
		firstLine string
	}{
		{
			name:      "ctx",
			cmd:       "ctx",
			args:      []string{"--help"},
			firstLine: "usage: drive9 ctx <show|add|import|fork|ls|use|rm>",
		},
		{
			name:      "ctx rm",
			cmd:       "ctx",
			args:      []string{"rm", "--help"},
			firstLine: "usage: drive9 ctx rm <name>",
		},
		{
			name:      "journal",
			cmd:       "journal",
			args:      []string{"--help"},
			firstLine: "usage: drive9 journal <new|append|cat|find|verify>",
		},
		{
			name:      "vault",
			cmd:       "vault",
			args:      []string{"--help"},
			firstLine: "usage drive9 vault <set|get|put|with|ls|rm|grant|revoke|audit>",
		},
		{
			name:      "region",
			cmd:       "region",
			args:      []string{"--help"},
			firstLine: "usage: drive9 region <list|ls>",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())

			origExit := exitFunc
			origStdout := os.Stdout
			origStderr := os.Stderr
			origStop := cpuProfileStop
			t.Cleanup(func() {
				exitFunc = origExit
				os.Stdout = origStdout
				os.Stderr = origStderr
				cpuProfileStop = origStop
			})
			cpuProfileStop = func() {}

			var exitCodes []int
			exitFunc = func(code int) { exitCodes = append(exitCodes, code) }

			stdoutR, stdoutW, err := os.Pipe()
			if err != nil {
				t.Fatalf("stdout pipe: %v", err)
			}
			stderrR, stderrW, err := os.Pipe()
			if err != nil {
				t.Fatalf("stderr pipe: %v", err)
			}
			os.Stdout = stdoutW
			os.Stderr = stderrW

			stdoutDone := make(chan string, 1)
			stderrDone := make(chan string, 1)
			go func() {
				var buf bytes.Buffer
				_, _ = io.Copy(&buf, stdoutR)
				stdoutDone <- buf.String()
			}()
			go func() {
				var buf bytes.Buffer
				_, _ = io.Copy(&buf, stderrR)
				stderrDone <- buf.String()
			}()

			dispatch(tc.cmd, tc.args)

			_ = stdoutW.Close()
			_ = stderrW.Close()
			stdout := <-stdoutDone
			stderr := <-stderrDone

			if len(exitCodes) != 0 {
				t.Errorf("exit codes = %v, want no fatal/usage exit", exitCodes)
			}
			if !strings.HasPrefix(stdout, tc.firstLine) {
				t.Errorf("stdout = %q, want first line %q", stdout, tc.firstLine)
			}
			if stderr != "" {
				t.Errorf("stderr = %q, want empty stderr for explicit help", stderr)
			}
		})
	}
}

// V2b: `drive9 vault <sub>` MUST route to the vault handler with args forwarded
// verbatim (no shell parsing, no arg mangling). This is the positive half of
// the hard-cut contract: the new verb name is live.
func TestDispatchVaultVerbReachesHandler(t *testing.T) {
	origHandler := vaultHandler
	origExit := exitFunc
	t.Cleanup(func() {
		vaultHandler = origHandler
		exitFunc = origExit
	})
	exitFunc = func(int) {} // swallow any fatal/usage exit

	var gotArgs []string
	called := false
	vaultHandler = func(args []string) error {
		called = true
		gotArgs = args
		return nil
	}

	dispatch("vault", []string{"ls", "--json"})

	if !called {
		t.Fatal("vault handler was not invoked for `drive9 vault ...`")
	}
	want := []string{"ls", "--json"}
	if len(gotArgs) != len(want) {
		t.Fatalf("args = %v, want %v", gotArgs, want)
	}
	for i := range want {
		if gotArgs[i] != want[i] {
			t.Fatalf("args[%d] = %q, want %q", i, gotArgs[i], want[i])
		}
	}
}

func TestDispatchTokenVerbReachesHandler(t *testing.T) {
	origHandler := tokenHandler
	origExit := exitFunc
	t.Cleanup(func() {
		tokenHandler = origHandler
		exitFunc = origExit
	})
	exitFunc = func(int) {}

	var gotArgs []string
	called := false
	tokenHandler = func(args []string) error {
		called = true
		gotArgs = args
		return nil
	}

	dispatch("token", []string{"issue", "--subject", "vm0"})

	if !called {
		t.Fatal("token handler was not invoked for `drive9 token ...`")
	}
	want := []string{"issue", "--subject", "vm0"}
	if len(gotArgs) != len(want) {
		t.Fatalf("args = %v, want %v", gotArgs, want)
	}
	for i := range want {
		if gotArgs[i] != want[i] {
			t.Fatalf("args[%d] = %q, want %q", i, gotArgs[i], want[i])
		}
	}
}

func TestDispatchDeleteVerbReachesHandler(t *testing.T) {
	origHandler := deleteHandler
	origExit := exitFunc
	t.Cleanup(func() {
		deleteHandler = origHandler
		exitFunc = origExit
	})
	exitFunc = func(int) {}

	var gotArgs []string
	called := false
	deleteHandler = func(args []string) error {
		called = true
		gotArgs = args
		return nil
	}

	dispatch("delete", []string{"--json"})

	if !called {
		t.Fatal("delete handler was not invoked for `drive9 delete ...`")
	}
	want := []string{"--json"}
	if len(gotArgs) != len(want) {
		t.Fatalf("args = %v, want %v", gotArgs, want)
	}
	for i := range want {
		if gotArgs[i] != want[i] {
			t.Fatalf("args[%d] = %q, want %q", i, gotArgs[i], want[i])
		}
	}
}

func TestDispatchAdminVerbReachesHandler(t *testing.T) {
	origHandler := adminHandler
	origExit := exitFunc
	t.Cleanup(func() {
		adminHandler = origHandler
		exitFunc = origExit
	})
	exitFunc = func(int) {}

	var gotArgs []string
	called := false
	adminHandler = func(args []string) error {
		called = true
		gotArgs = args
		return nil
	}

	dispatch("admin", []string{"quota", "get", "--json"})

	if !called {
		t.Fatal("admin handler was not invoked for `drive9 admin ...`")
	}
	want := []string{"quota", "get", "--json"}
	if len(gotArgs) != len(want) {
		t.Fatalf("args = %v, want %v", gotArgs, want)
	}
	for i := range want {
		if gotArgs[i] != want[i] {
			t.Fatalf("args[%d] = %q, want %q", i, gotArgs[i], want[i])
		}
	}
}

func TestDispatchQuotaVerbIsRejected(t *testing.T) {
	origHandler := adminHandler
	origExit := exitFunc
	origStderr := os.Stderr
	origStop := cpuProfileStop
	t.Cleanup(func() {
		adminHandler = origHandler
		exitFunc = origExit
		os.Stderr = origStderr
		cpuProfileStop = origStop
	})
	cpuProfileStop = func() {}

	handlerCalled := false
	adminHandler = func(args []string) error {
		handlerCalled = true
		return nil
	}
	var exitCodes []int
	exitFunc = func(code int) { exitCodes = append(exitCodes, code) }

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stderr = w
	done := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		done <- buf.String()
	}()

	dispatch("quota", []string{"get"})

	_ = w.Close()
	stderr := <-done
	if handlerCalled {
		t.Fatal("admin handler was invoked for old top-level `drive9 quota ...` command")
	}
	if !strings.Contains(stderr, `drive9: unknown command "quota"`) {
		t.Fatalf("stderr = %q, want it to contain `drive9: unknown command \"quota\"`", stderr)
	}
	found2 := false
	for _, c := range exitCodes {
		if c == 2 {
			found2 = true
			break
		}
	}
	if !found2 {
		t.Fatalf("exit codes = %v, want exit(2) from usage() after unknown command", exitCodes)
	}
}

func TestDispatchRegionVerbReachesHandler(t *testing.T) {
	origHandler := regionHandler
	origExit := exitFunc
	t.Cleanup(func() {
		regionHandler = origHandler
		exitFunc = origExit
	})
	exitFunc = func(int) {}

	var gotArgs []string
	called := false
	regionHandler = func(args []string) error {
		called = true
		gotArgs = args
		return nil
	}

	dispatch("region", []string{"list", "--json"})

	if !called {
		t.Fatal("region handler was not invoked for `drive9 region ...`")
	}
	want := []string{"list", "--json"}
	if len(gotArgs) != len(want) {
		t.Fatalf("args = %v, want %v", gotArgs, want)
	}
	for i := range want {
		if gotArgs[i] != want[i] {
			t.Fatalf("args[%d] = %q, want %q", i, gotArgs[i], want[i])
		}
	}
}

func TestDispatchDoctorVerbReachesHandler(t *testing.T) {
	origHandler := doctorHandler
	origExit := exitFunc
	t.Cleanup(func() {
		doctorHandler = origHandler
		exitFunc = origExit
	})
	exitFunc = func(int) {}

	var gotArgs []string
	called := false
	doctorHandler = func(args []string) error {
		called = true
		gotArgs = args
		return nil
	}

	dispatch("doctor", []string{"fuse", "--mountpoint", "/mnt/drive9"})

	if !called {
		t.Fatal("doctor handler was not invoked for `drive9 doctor ...`")
	}
	want := []string{"fuse", "--mountpoint", "/mnt/drive9"}
	if len(gotArgs) != len(want) {
		t.Fatalf("args = %v, want %v", gotArgs, want)
	}
	for i := range want {
		if gotArgs[i] != want[i] {
			t.Fatalf("args[%d] = %q, want %q", i, gotArgs[i], want[i])
		}
	}
}

func TestDispatchUpdateVerbReachesHandler(t *testing.T) {
	origHandler := updateHandler
	origExit := exitFunc
	t.Cleanup(func() {
		updateHandler = origHandler
		exitFunc = origExit
	})
	exitFunc = func(int) {}

	var gotArgs []string
	called := false
	updateHandler = func(args []string) error {
		called = true
		gotArgs = args
		return nil
	}

	dispatch("update", []string{"--check"})

	if !called {
		t.Fatal("update handler was not invoked for `drive9 update ...`")
	}
	want := []string{"--check"}
	if len(gotArgs) != len(want) {
		t.Fatalf("args = %v, want %v", gotArgs, want)
	}
	for i := range want {
		if gotArgs[i] != want[i] {
			t.Fatalf("args[%d] = %q, want %q", i, gotArgs[i], want[i])
		}
	}
}

func TestDispatchJournalVerbReachesHandler(t *testing.T) {
	origHandler := journalHandler
	origExit := exitFunc
	t.Cleanup(func() {
		journalHandler = origHandler
		exitFunc = origExit
	})
	exitFunc = func(int) {}

	var gotArgs []string
	called := false
	journalHandler = func(args []string) error {
		called = true
		gotArgs = args
		return nil
	}

	dispatch("journal", []string{"find", "-t", "tool.call.completed"})

	if !called {
		t.Fatal("journal handler was not invoked for `drive9 journal ...`")
	}
	want := []string{"find", "-t", "tool.call.completed"}
	if len(gotArgs) != len(want) {
		t.Fatalf("args = %v, want %v", gotArgs, want)
	}
	for i := range want {
		if gotArgs[i] != want[i] {
			t.Fatalf("args[%d] = %q, want %q", i, gotArgs[i], want[i])
		}
	}
}

func TestDispatchGitVerbReachesHandler(t *testing.T) {
	origHandler := gitHandler
	origExit := exitFunc
	t.Cleanup(func() {
		gitHandler = origHandler
		exitFunc = origExit
	})
	exitFunc = func(int) {}

	var gotArgs []string
	called := false
	gitHandler = func(args []string) error {
		called = true
		gotArgs = args
		return nil
	}

	dispatch("git", []string{"clone", "--fast", "https://github.com/mem9-ai/drive9", "/mnt/drive9/repo"})

	if !called {
		t.Fatal("git handler was not invoked for `drive9 git ...`")
	}
	want := []string{"clone", "--fast", "https://github.com/mem9-ai/drive9", "/mnt/drive9/repo"}
	if len(gotArgs) != len(want) {
		t.Fatalf("args = %v, want %v", gotArgs, want)
	}
	for i := range want {
		if gotArgs[i] != want[i] {
			t.Fatalf("args[%d] = %q, want %q", i, gotArgs[i], want[i])
		}
	}
}

func TestDispatchPackVerbReachesHandler(t *testing.T) {
	origHandler := packHandler
	origExit := exitFunc
	t.Cleanup(func() {
		packHandler = origHandler
		exitFunc = origExit
	})
	exitFunc = func(int) {}

	var gotArgs []string
	called := false
	packHandler = func(args []string) error {
		called = true
		gotArgs = args
		return nil
	}

	dispatch("pack", []string{":/packs/archive.tar.gz", ".git"})

	if !called {
		t.Fatal("pack handler was not invoked for `drive9 pack ...`")
	}
	want := []string{":/packs/archive.tar.gz", ".git"}
	if len(gotArgs) != len(want) {
		t.Fatalf("args = %v, want %v", gotArgs, want)
	}
	for i := range want {
		if gotArgs[i] != want[i] {
			t.Fatalf("args[%d] = %q, want %q", i, gotArgs[i], want[i])
		}
	}
}

func TestDispatchUnpackVerbReachesHandler(t *testing.T) {
	origHandler := unpackHandler
	origExit := exitFunc
	t.Cleanup(func() {
		unpackHandler = origHandler
		exitFunc = origExit
	})
	exitFunc = func(int) {}

	var gotArgs []string
	called := false
	unpackHandler = func(args []string) error {
		called = true
		gotArgs = args
		return nil
	}

	dispatch("unpack", []string{":/packs/archive.tar.gz", "--local-root", "/tmp/drive9-local"})

	if !called {
		t.Fatal("unpack handler was not invoked for `drive9 unpack ...`")
	}
	want := []string{":/packs/archive.tar.gz", "--local-root", "/tmp/drive9-local"}
	if len(gotArgs) != len(want) {
		t.Fatalf("args = %v, want %v", gotArgs, want)
	}
	for i := range want {
		if gotArgs[i] != want[i] {
			t.Fatalf("args[%d] = %q, want %q", i, gotArgs[i], want[i])
		}
	}
}

func TestDispatchProfileVerbReachesHandler(t *testing.T) {
	origHandler := profileHandler
	origExit := exitFunc
	t.Cleanup(func() {
		profileHandler = origHandler
		exitFunc = origExit
	})
	exitFunc = func(int) {}

	var gotArgs []string
	called := false
	profileHandler = func(args []string) error {
		called = true
		gotArgs = args
		return nil
	}

	dispatch("profile", []string{"show", "coding-agent"})

	if !called {
		t.Fatal("profile handler was not invoked for `drive9 profile ...`")
	}
	want := []string{"show", "coding-agent"}
	if len(gotArgs) != len(want) {
		t.Fatalf("args = %v, want %v", gotArgs, want)
	}
	for i := range want {
		if gotArgs[i] != want[i] {
			t.Fatalf("args[%d] = %q, want %q", i, gotArgs[i], want[i])
		}
	}
}

// V2b hard-cut (G-V2b-1 / G-V2b-3): `drive9 secret <sub>` MUST NOT reach the
// vault handler and MUST NOT get a bespoke rename hint — it falls into the
// generic `unknown command` path shared with any typo. This pins the "no
// silent alias, no deferred-MUST deadline" policy: the old verb is simply
// not a command anymore.
func TestDispatchSecretVerbIsRejected(t *testing.T) {
	origHandler := vaultHandler
	origExit := exitFunc
	origStderr := os.Stderr
	origStop := cpuProfileStop
	t.Cleanup(func() {
		vaultHandler = origHandler
		exitFunc = origExit
		os.Stderr = origStderr
		cpuProfileStop = origStop
	})

	cpuProfileStop = func() {}

	handlerCalled := false
	vaultHandler = func(args []string) error {
		handlerCalled = true
		return nil
	}

	var exitCodes []int
	exitFunc = func(code int) { exitCodes = append(exitCodes, code) }

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stderr = w
	done := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		done <- buf.String()
	}()

	dispatch("secret", []string{"ls"})

	_ = w.Close()
	stderr := <-done

	if handlerCalled {
		t.Fatal("vault handler was invoked for `drive9 secret ...` — hard-cut violated (G-V2b-1)")
	}
	if !strings.Contains(stderr, `drive9: unknown command "secret"`) {
		t.Fatalf("stderr = %q, want it to contain `drive9: unknown command \"secret\"`", stderr)
	}
	// No bespoke rename hint allowed (G-V2b-3): `secret` must look like any
	// other typo, not like a grandfathered-in deprecated verb.
	lowered := strings.ToLower(stderr)
	for _, banned := range []string{"rename", "renamed", "alias", "legacy", "deprecated", "use `vault`", "use vault"} {
		if strings.Contains(lowered, strings.ToLower(banned)) {
			t.Fatalf("stderr contains bespoke rename hint %q — G-V2b-3 forbids alias-style fallback: %q", banned, stderr)
		}
	}
	// usage() exits with code 2; default branch prints the unknown-command
	// line and then calls usage(). The exact exit sequence is: one call from
	// usage() itself. We assert exit code 2 appeared.
	found2 := false
	for _, c := range exitCodes {
		if c == 2 {
			found2 = true
			break
		}
	}
	if !found2 {
		t.Fatalf("exit codes = %v, want exit(2) from usage() after unknown command", exitCodes)
	}
}

// V2b hard-cut is a pure removal: the generic `unknown command` path MUST
// treat `secret` and any other unknown string (like `xyz`) with the same
// framing. If the two diverge, it means someone smuggled a rename-aware
// branch back in.
func TestDispatchSecretVerbSameAsOtherUnknown(t *testing.T) {
	origExit := exitFunc
	origStderr := os.Stderr
	origStop := cpuProfileStop
	t.Cleanup(func() {
		exitFunc = origExit
		os.Stderr = origStderr
		cpuProfileStop = origStop
	})
	cpuProfileStop = func() {}
	exitFunc = func(int) {}

	capture := func(verb string) string {
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatalf("os.Pipe: %v", err)
		}
		os.Stderr = w
		done := make(chan string, 1)
		go func() {
			var buf bytes.Buffer
			_, _ = io.Copy(&buf, r)
			done <- buf.String()
		}()
		dispatch(verb, nil)
		_ = w.Close()
		return <-done
	}

	secretOut := capture("secret")
	xyzOut := capture("xyz-typo")

	// Framing must be identical except for the quoted verb name itself. If
	// `secret` had a bespoke branch, its output would have extra text.
	normalized := strings.Replace(secretOut, `"secret"`, `"xyz-typo"`, 1)
	if normalized != xyzOut {
		t.Fatalf("`secret` path diverges from generic unknown-command path.\n  secret (normalized): %q\n  xyz-typo           : %q", normalized, xyzOut)
	}
}

func TestExitWithCodeStopsCPUProfile(t *testing.T) {
	origStop := cpuProfileStop
	origExit := exitFunc
	t.Cleanup(func() {
		cpuProfileStop = origStop
		exitFunc = origExit
	})

	stopped := false
	exitCode := -1
	cpuProfileStop = func() { stopped = true }
	exitFunc = func(code int) { exitCode = code }

	exitWithCode(7)

	if !stopped {
		t.Fatal("expected exitWithCode to stop CPU profiling")
	}
	if exitCode != 7 {
		t.Fatalf("exit code = %d, want 7", exitCode)
	}
}
