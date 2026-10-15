package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/cmd/drive9/cli"
	"github.com/mem9-ai/drive9/pkg/buildinfo"
	"github.com/mem9-ai/drive9/pkg/telemetry"
)

func TestResolveTelemetryCommandEligibility(t *testing.T) {
	tests := []struct {
		name         string
		args         []string
		wantEligible bool
		wantPath     string
		wantFlags    string
	}{
		{name: "commandless", args: nil},
		{name: "help flag", args: []string{"--help"}},
		{name: "help command", args: []string{"help"}},
		{name: "version", args: []string{"--version"}},
		{name: "update", args: []string{"update", "--check"}},
		{name: "unknown command", args: []string{"nope"}},
		{name: "unknown subcommand", args: []string{"fs", "nope"}},
		{name: "fs parent only", args: []string{"fs"}},
		{name: "subcommand help", args: []string{"fs", "ls", "--help"}},
		{name: "internal mount supervise", args: []string{"mount", "supervise", "--ready-file", "x"}},
		{name: "internal supervised worker", args: []string{"mount", "--foreground", "--supervised", ":/", "/mnt"}},
		{name: "internal supervise foreground", args: []string{"mount", "--supervise-foreground", ":/", "/mnt"}},
		{name: "fs ls", args: []string{"fs", "ls", "-l", ":/"}, wantEligible: true, wantPath: "drive9 fs ls", wantFlags: "l"},
		{name: "fs cat flags without values", args: []string{"fs", "cat", "--json", ":/secret/path"}, wantEligible: true, wantPath: "drive9 fs cat", wantFlags: "json"},
		{name: "ctx default show", args: []string{"ctx"}, wantEligible: true, wantPath: "drive9 ctx show"},
		{name: "ctx list alias", args: []string{"ctx", "list", "--json"}, wantEligible: true, wantPath: "drive9 ctx ls", wantFlags: "json"},
		{name: "fs layer create", args: []string{"fs", "layer", "create", "--json", "base"}, wantEligible: true, wantPath: "drive9 fs layer create", wantFlags: "json"},
		{name: "fs layer ls alias", args: []string{"fs", "layer", "ls"}, wantEligible: true, wantPath: "drive9 fs layer list"},
		{name: "mount leaf with path", args: []string{"mount", "--mode=fuse", ":/", "./mnt"}, wantEligible: true, wantPath: "drive9 mount", wantFlags: "mode"},
		{name: "mount vault", args: []string{"mount", "vault", "./mnt"}, wantEligible: true, wantPath: "drive9 mount vault"},
		{name: "git worktree add", args: []string{"git", "worktree", "add", "--fast", "a", "b"}, wantEligible: true, wantPath: "drive9 git worktree add", wantFlags: "fast"},
		{name: "admin tenant pool create flattened", args: []string{"admin", "tenant", "pool", "create", "--pool-size", "2"}, wantEligible: true, wantPath: "drive9 admin tenant pool-create", wantFlags: "pool-size"},
		{name: "stop at double dash", args: []string{"vault", "with", "/n/vault/x", "--", "echo", "--secret"}, wantEligible: true, wantPath: "drive9 vault with"},
		// "version" and "help" are only special in command position: a value or
		// a path that happens to spell them must still be reported.
		{name: "value that looks like the version command", args: []string{"fs", "find", "-name", "version", ":/"}, wantEligible: true, wantPath: "drive9 fs find", wantFlags: "name"},
		{name: "value that looks like help", args: []string{"fs", "grep", "help", ":/"}, wantEligible: true, wantPath: "drive9 fs grep"},
		{name: "context named version", args: []string{"ctx", "use", "version"}, wantEligible: true, wantPath: "drive9 ctx use"},
		// admin leaf parsers treat a bare "help" operand as a help request.
		{name: "admin leaf help operand", args: []string{"admin", "tenant", "create", "help"}},
		{name: "admin nested leaf help operand", args: []string{"admin", "tenant", "pool", "create", "help"}},
		{name: "admin object backend help operand", args: []string{"admin", "object-backend", "add", "help"}},
		{name: "admin help consumed as a flag value", args: []string{"admin", "tenant", "create", "--server", "help"}, wantEligible: true, wantPath: "drive9 admin tenant create", wantFlags: "server"},
		{name: "admin normal invocation", args: []string{"admin", "tenant", "create", "--json"}, wantEligible: true, wantPath: "drive9 admin tenant create", wantFlags: "json"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := resolveTelemetryCommand(test.args)
			if got.eligible != test.wantEligible {
				t.Fatalf("eligible = %t, want %t path=%q", got.eligible, test.wantEligible, got.commandPath)
			}
			if !test.wantEligible {
				if got.commandPath != "" {
					t.Fatalf("ineligible command path = %q", got.commandPath)
				}
				return
			}
			if got.commandPath != test.wantPath {
				t.Fatalf("command path = %q, want %q", got.commandPath, test.wantPath)
			}
			if test.wantFlags != "" {
				gotFlags := strings.Join(telemetryFlagNames(got.flagArgs), ",")
				if gotFlags != test.wantFlags {
					t.Fatalf("flag names = %q, want %q", gotFlags, test.wantFlags)
				}
			}
			if strings.Contains(got.commandPath, "/secret/path") {
				t.Fatalf("command path leaked user input: %q", got.commandPath)
			}
		})
	}
}

func TestTelemetryFlagNamesNeverRecordFlagValues(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "value that starts with a dash",
			args: []string{"--subject", "-secret-project"},
			want: "subject",
		},
		{
			name: "value that starts with a double dash",
			args: []string{"--description", "--private-note"},
			want: "description",
		},
		{
			name: "value with inline equals",
			args: []string{"--manifest-url=-internal-host"},
			want: "manifest-url",
		},
		{
			name: "positional arguments are never flags",
			args: []string{"-l", ":/secret", "./mnt"},
			want: "l",
		},
		{
			name: "flags after an inline value still count",
			args: []string{"--json=true", "--long"},
			want: "json,long",
		},
		{
			name: "flags separated by values still count",
			args: []string{"-name", "report", "-tag", "k=v"},
			want: "name,tag",
		},
		{
			name: "everything after a terminator is ignored",
			args: []string{"-l", "--", "--secret"},
			want: "l",
		},
		{
			// A dash-prefixed token that is not a flag this CLI defines is user
			// input — here a pattern the command would reject — never a name.
			name: "unknown flag-shaped token is dropped",
			args: []string{"-my-private-pattern"},
			want: "",
		},
		{
			name: "mistyped flag name is dropped",
			args: []string{"--api-key-secret123"},
			want: "",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := strings.Join(telemetryFlagNames(test.args), ",")
			if got != test.want {
				t.Fatalf("flag names = %q, want %q", got, test.want)
			}
		})
	}
}

func TestWithCLITelemetryReportsUnknownPlacementForContextScopedCommands(t *testing.T) {
	for _, test := range []struct {
		name         string
		args         []string
		wantProvider string
		wantRegion   string
		wantSource   string
	}{
		{
			name:         "active context",
			args:         []string{"fs", "cat", ":/file"},
			wantProvider: "aws",
			wantRegion:   "aws-ap-southeast-1",
			wantSource:   "default",
		},
		{
			name:       "named context",
			args:       []string{"fs", "cat", "prod:/file"},
			wantSource: "unknown",
		},
		{
			name:         "object store location keeps the active context",
			args:         []string{"fs", "cat", "s3://bucket/key"},
			wantProvider: "aws",
			wantRegion:   "aws-ap-southeast-1",
			wantSource:   "default",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			t.Setenv("DRIVE9_TELEMETRY", "on")
			t.Setenv("DRIVE9_ALLOW_TEST_ENDPOINTS", "1")
			t.Setenv(cli.EnvAPIKey, "")
			t.Setenv(cli.EnvVaultToken, "")
			writeConfigFile(t, home, `{"current_context":"dev","contexts":{"dev":{"type":"owner","api_key":"k","server":"https://s","cloud_provider":"aws","region":"aws-ap-southeast-1"}}}`)

			requests := make(chan []byte, 1)
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				body, _ := io.ReadAll(request.Body)
				requests <- body
				writer.WriteHeader(http.StatusAccepted)
			}))
			defer server.Close()
			t.Setenv("DRIVE9_TEST_TELEMETRY_ENDPOINT", server.URL+"/v1/telemetry/batch")

			origVersion, origSource := buildinfo.Version, buildinfo.InstallSource
			t.Cleanup(func() { buildinfo.Version, buildinfo.InstallSource = origVersion, origSource })
			buildinfo.Version = "0.2.0"
			buildinfo.InstallSource = "local"

			origExit, origStop := exitFunc, cpuProfileStop
			t.Cleanup(func() { exitFunc, cpuProfileStop = origExit, origStop })
			cpuProfileStop = func() {}
			exitFunc = func(int) {}

			withCLITelemetry(test.args, func() {})

			var payload struct {
				Events []struct {
					CloudProvider string `json:"cloud_provider"`
					RegionCode    string `json:"region_code"`
					ProfileSource string `json:"profile_source"`
				} `json:"events"`
			}
			select {
			case body := <-requests:
				if err := json.Unmarshal(body, &payload); err != nil {
					t.Fatal(err)
				}
			default:
				t.Fatal("no telemetry event was sent")
			}
			event := payload.Events[0]
			if event.CloudProvider != test.wantProvider || event.RegionCode != test.wantRegion || event.ProfileSource != test.wantSource {
				t.Fatalf("placement = %q/%q/%q, want %q/%q/%q", event.CloudProvider, event.RegionCode, event.ProfileSource, test.wantProvider, test.wantRegion, test.wantSource)
			}
		})
	}
}

func TestWithCLITelemetrySendsAllowlistedEventAndDoesNotChangeExit(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("DRIVE9_TELEMETRY", "on")
	t.Setenv("DRIVE9_ALLOW_TEST_ENDPOINTS", "1")
	t.Setenv("CI", "")
	t.Setenv("GITHUB_ACTIONS", "")

	origVersion := buildinfo.Version
	origSource := buildinfo.InstallSource
	origEndpoint := buildinfo.TelemetryEndpoint
	t.Cleanup(func() {
		buildinfo.Version = origVersion
		buildinfo.InstallSource = origSource
		buildinfo.TelemetryEndpoint = origEndpoint
	})
	buildinfo.Version = "0.2.0"
	buildinfo.InstallSource = "local"

	requests := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/v1/telemetry/batch" {
			t.Errorf("unexpected request %s %s", request.Method, request.URL.Path)
		}
		body, _ := io.ReadAll(request.Body)
		requests <- body
		writer.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	t.Setenv("DRIVE9_TEST_TELEMETRY_ENDPOINT", server.URL+"/v1/telemetry/batch")

	origExit := exitFunc
	origStop := cpuProfileStop
	t.Cleanup(func() {
		exitFunc = origExit
		cpuProfileStop = origStop
	})
	cpuProfileStop = func() {}
	var exitCodes []int
	exitFunc = func(code int) { exitCodes = append(exitCodes, code) }

	withCLITelemetry([]string{"fs", "ls", "-l", ":/secret"}, func() {
		exitWithCode(2)
	})

	if len(exitCodes) != 1 || exitCodes[0] != 2 {
		t.Fatalf("exit codes = %v, want [2]", exitCodes)
	}
	select {
	case body := <-requests:
		if strings.Contains(string(body), ":/secret") {
			t.Fatalf("payload leaked path: %s", body)
		}
		var payload struct {
			Events []struct {
				CommandPath string   `json:"command_path"`
				FlagNames   []string `json:"flag_names"`
				ExitCode    int      `json:"exit_code"`
				EventName   string   `json:"event_name"`
			} `json:"events"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatal(err)
		}
		if len(payload.Events) != 1 || payload.Events[0].CommandPath != "drive9 fs ls" || payload.Events[0].ExitCode != 2 || payload.Events[0].EventName != "drive9.command.finished" {
			t.Fatalf("unexpected payload: %s", body)
		}
		if strings.Join(payload.Events[0].FlagNames, ",") != "l" {
			t.Fatalf("flag names = %#v", payload.Events[0].FlagNames)
		}
	default:
		t.Fatal("eligible command did not send telemetry")
	}

	idPath := filepath.Join(home, ".drive9", ".telemetry-installation-id")
	data, err := os.ReadFile(idPath)
	if err != nil {
		t.Fatalf("read installation ID: %v", err)
	}
	if !strings.HasPrefix(strings.TrimSpace(string(data)), "drive9_") {
		t.Fatalf("installation ID = %q", data)
	}
}

func TestWithCLITelemetryHonorsConfigOptInAndEnvironmentOverride(t *testing.T) {
	for _, test := range []struct {
		name       string
		config     string
		env        string
		wantEvents int
	}{
		{name: "config opt in", config: `{"telemetry":{"enabled":true}}`, wantEvents: 1},
		{name: "config opt out", config: `{"telemetry":{"enabled":false}}`, wantEvents: 0},
		{name: "no preference", config: `{"contexts":{}}`, wantEvents: 0},
		{name: "malformed config fails closed", config: `{"telemetry":`, wantEvents: 0},
		{name: "environment overrides config opt out", config: `{"telemetry":{"enabled":false}}`, env: "on", wantEvents: 1},
		{name: "environment overrides config opt in", config: `{"telemetry":{"enabled":true}}`, env: "off", wantEvents: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			t.Setenv("DRIVE9_ALLOW_TEST_ENDPOINTS", "1")
			if test.env == "" {
				t.Setenv("DRIVE9_TELEMETRY", "")
			} else {
				t.Setenv("DRIVE9_TELEMETRY", test.env)
			}
			writeConfigFile(t, home, test.config)

			requests := make(chan []byte, 4)
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				body, _ := io.ReadAll(request.Body)
				requests <- body
				writer.WriteHeader(http.StatusAccepted)
			}))
			defer server.Close()
			t.Setenv("DRIVE9_TEST_TELEMETRY_ENDPOINT", server.URL+"/v1/telemetry/batch")

			origVersion := buildinfo.Version
			origSource := buildinfo.InstallSource
			t.Cleanup(func() {
				buildinfo.Version = origVersion
				buildinfo.InstallSource = origSource
			})
			buildinfo.Version = "0.2.0"
			buildinfo.InstallSource = "local"

			origExit := exitFunc
			origStop := cpuProfileStop
			t.Cleanup(func() {
				exitFunc = origExit
				cpuProfileStop = origStop
			})
			cpuProfileStop = func() {}
			exitFunc = func(int) {}

			withCLITelemetry([]string{"region", "list", "--json"}, func() {})

			got := 0
			for {
				select {
				case <-requests:
					got++
					continue
				default:
				}
				break
			}
			if got != test.wantEvents {
				t.Fatalf("events = %d, want %d", got, test.wantEvents)
			}
		})
	}
}

func TestWithCLITelemetryExcludedCommandsDoNotReadState(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("DRIVE9_TELEMETRY", "on")
	t.Setenv("DRIVE9_ALLOW_TEST_ENDPOINTS", "1")
	t.Setenv("DRIVE9_TEST_TELEMETRY_ENDPOINT", "https://example.invalid/v1/telemetry/batch")

	config := `{"telemetry":` // deliberately invalid: reading it must not be attempted
	writeConfigFile(t, home, config)

	origExit := exitFunc
	origStop := cpuProfileStop
	t.Cleanup(func() {
		exitFunc = origExit
		cpuProfileStop = origStop
	})
	cpuProfileStop = func() {}
	exitFunc = func(int) {}

	for _, args := range [][]string{
		{"--help"},
		{"update", "--check"},
		{"version"},
		{"fs", "ls", "--help"},
		{"mount", "supervise", "--mountpoint", "/mnt"},
		{"mount", "--supervised", ":/", "/mnt"},
	} {
		withCLITelemetry(args, func() {})
		got, err := os.ReadFile(filepath.Join(home, ".drive9", "config"))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != config {
			t.Fatalf("excluded %v rewrote config", args)
		}
		if _, err := os.Stat(telemetry.InstallationIDPath(home)); !os.IsNotExist(err) {
			t.Fatalf("excluded %v created identity", args)
		}
	}
}

func TestWithCLITelemetryDeliveryFailureDoesNotChangeResult(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("DRIVE9_TELEMETRY", "on")
	t.Setenv("DRIVE9_ALLOW_TEST_ENDPOINTS", "1")
	t.Setenv("DRIVE9_TEST_TELEMETRY_ENDPOINT", "http://127.0.0.1:1/v1/telemetry/batch")

	origVersion := buildinfo.Version
	origSource := buildinfo.InstallSource
	t.Cleanup(func() {
		buildinfo.Version = origVersion
		buildinfo.InstallSource = origSource
	})
	buildinfo.Version = "0.2.0"
	buildinfo.InstallSource = "local"

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

	withCLITelemetry([]string{"region", "list", "--json"}, func() {})
	if len(exitCodes) != 0 {
		t.Fatalf("delivery failure changed exit: %v", exitCodes)
	}
}

// TestCommandHandlersExitThroughTheHook guards the invariant that command
// handlers never call os.Exit directly: usage failures must stay observable by
// the telemetry hook, which is what makes exit codes reportable at all.
func TestCommandHandlersExitThroughTheHook(t *testing.T) {
	t.Run("flag parse failure carries exit code 2", func(t *testing.T) {
		err := cli.MountCmd([]string{"--not-a-flag"})
		var usage cli.UsageError
		if !errors.As(err, &usage) {
			t.Fatalf("error = %v, want a cli.UsageError", err)
		}
		if usage.ExitCode() != 2 || !usage.Reported() {
			t.Fatalf("usage error = %+v, want exit code 2 and already reported", usage)
		}
	})

	t.Run("missing argument exits through the installed hook", func(t *testing.T) {
		var recorded []int
		restore := cli.SetExitHook(func(code int) { recorded = append(recorded, code) })
		t.Cleanup(restore)

		_ = cli.VaultMountCmd(nil)

		if len(recorded) != 1 || recorded[0] != 2 {
			t.Fatalf("exit codes = %v, want [2]", recorded)
		}
	})
}

func writeConfigFile(t *testing.T, home, content string) {
	t.Helper()
	dir := filepath.Join(home, ".drive9")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
