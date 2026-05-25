package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
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
		"ctx show [--json] [--reveal]",
		"ctx use <name>",
		"token <issue|revoke>",
		"journal <new|append|cat|find|verify>",
		"perf <collect|summarize>",
		"mount [flags] [:/remote] <mountpoint>",
		"mount vault [flags] <mountpoint>",
		"doctor fuse",
		"-h, --help, help",
	} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("stderr = %q, want it to contain %q", stderr, want)
		}
	}
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

func TestDispatchPerfVerbReachesHandler(t *testing.T) {
	origHandler := perfHandler
	origExit := exitFunc
	t.Cleanup(func() {
		perfHandler = origHandler
		exitFunc = origExit
	})
	exitFunc = func(int) {}

	var gotArgs []string
	called := false
	perfHandler = func(args []string) error {
		called = true
		gotArgs = args
		return nil
	}

	dispatch("perf", []string{"summarize", "--input", "perf.jsonl"})

	if !called {
		t.Fatal("perf handler was not invoked for `drive9 perf ...`")
	}
	want := []string{"summarize", "--input", "perf.jsonl"}
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
