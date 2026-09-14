package telemetry

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestStartResolutionAndStateCreation(t *testing.T) {
	enabled := true
	disabled := false
	tests := []struct {
		name        string
		version     string
		env         map[string]string
		preference  *bool
		wantSession bool
	}{
		{name: "release default", version: "0.2.0", wantSession: false},
		{name: "release default in CI", version: "0.2.0", env: map[string]string{"CI": "true"}, wantSession: false},
		{name: "development default", version: "dev", wantSession: false},
		{name: "recorded disable", version: "0.2.0", preference: &disabled, wantSession: false},
		{name: "recorded enable", version: "0.2.0", preference: &enabled, wantSession: true},
		{name: "recorded enable on development build", version: "dev", preference: &enabled, wantSession: true},
		{name: "environment enable on release build", version: "0.2.0", env: map[string]string{EnvironmentVariable: "on"}, wantSession: true},
		{name: "environment enable overrides development", version: "dev", env: map[string]string{EnvironmentVariable: "yes"}, wantSession: true},
		{name: "environment enable overrides recorded disable", version: "0.2.0", env: map[string]string{EnvironmentVariable: "true"}, preference: &disabled, wantSession: true},
		{name: "environment disable overrides recorded enable", version: "0.2.0", env: map[string]string{EnvironmentVariable: "no"}, preference: &enabled, wantSession: false},
		{name: "empty environment value falls back to preference", version: "0.2.0", env: map[string]string{EnvironmentVariable: ""}, preference: &enabled, wantSession: true},
		{name: "invalid environment fails closed", version: "0.2.0", env: map[string]string{EnvironmentVariable: "sometimes"}, preference: &enabled, wantSession: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			home := t.TempDir()
			environment := test.env
			if environment == nil {
				environment = map[string]string{}
			}
			session := Start(Config{
				Eligible:      true,
				HomeDir:       home,
				Endpoint:      "https://telemetry.example.test/v1/telemetry/batch",
				Version:       test.version,
				InstallSource: "archive",
				Environment:   environment,
				Preference:    recordedPreference(test.preference),
			})
			if (session != nil) != test.wantSession {
				t.Fatalf("session present = %t, want %t", session != nil, test.wantSession)
			}
			_, err := os.Stat(InstallationIDPath(home))
			if test.wantSession && err != nil {
				t.Fatalf("installation ID was not created: %v", err)
			}
			if !test.wantSession && !os.IsNotExist(err) {
				t.Fatalf("disabled telemetry created installation ID: %v", err)
			}
		})
	}
}

func TestStartShortCircuitsBeforeStateReads(t *testing.T) {
	for _, test := range []struct {
		name     string
		eligible bool
		endpoint string
		env      map[string]string
	}{
		{name: "ineligible", endpoint: "https://telemetry.example.test", env: map[string]string{EnvironmentVariable: "on"}},
		{name: "missing endpoint", eligible: true, env: map[string]string{EnvironmentVariable: "on"}},
		{name: "explicit disable", eligible: true, endpoint: "https://telemetry.example.test", env: map[string]string{EnvironmentVariable: "off"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			home := t.TempDir()
			before := "invalid-identity\n"
			writeTestFile(t, InstallationIDPath(home), before, 0o600)
			preferenceReads := 0
			session := Start(Config{
				Eligible:      test.eligible,
				HomeDir:       home,
				Endpoint:      test.endpoint,
				Version:       "0.2.0",
				InstallSource: "archive",
				Environment:   test.env,
				Preference: func() (bool, bool) {
					preferenceReads++
					return true, true
				},
			})
			if session != nil {
				t.Fatal("expected telemetry to stay disabled")
			}
			if preferenceReads != 0 {
				t.Fatalf("preference was read %d times before enablement was settled", preferenceReads)
			}
			assertFileContent(t, InstallationIDPath(home), before)
		})
	}
}

func TestMissingPreferenceFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name       string
		preference func() (bool, bool)
	}{
		{name: "unrecorded", preference: func() (bool, bool) { return false, false }},
		{name: "unreadable", preference: nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			home := t.TempDir()
			session := Start(Config{
				Eligible:      true,
				HomeDir:       home,
				Endpoint:      "https://telemetry.example.test",
				Version:       "0.2.0",
				InstallSource: "archive",
				Environment:   map[string]string{},
				Preference:    test.preference,
			})
			if session != nil {
				t.Fatal("telemetry was enabled without a recorded opt-in")
			}
			if _, err := os.Stat(InstallationIDPath(home)); !os.IsNotExist(err) {
				t.Fatalf("disabled telemetry created identity: %v", err)
			}
		})
	}
}

func TestDefaultDisabledKeepsStateUntouched(t *testing.T) {
	for _, installSource := range []string{"archive", "github-release", "homebrew", "scoop", "source", "local", "unknown", ""} {
		t.Run("install_source="+installSource, func(t *testing.T) {
			home := t.TempDir()
			cfg := Config{Eligible: true, HomeDir: home, Endpoint: "https://telemetry.example.test", Version: "0.2.0", InstallSource: installSource, Environment: map[string]string{}}
			if session := Start(cfg); session != nil {
				t.Fatalf("install source %q enabled telemetry without opt-in", installSource)
			}
			if _, err := os.Stat(InstallationIDPath(home)); !os.IsNotExist(err) {
				t.Fatalf("install source %q created identity: %v", installSource, err)
			}
		})
	}
}

func TestOptInCreatesIdentityAndUserCanResetIt(t *testing.T) {
	home := t.TempDir()
	cfg := Config{Eligible: true, HomeDir: home, Endpoint: "https://telemetry.example.test", Version: "0.2.0", InstallSource: "github-release", Environment: map[string]string{EnvironmentVariable: "on"}}
	first := Start(cfg)
	if first == nil {
		t.Fatal("environment opt-in should enable telemetry")
	}
	if err := os.Remove(InstallationIDPath(home)); err != nil {
		t.Fatal(err)
	}
	second := Start(cfg)
	if second == nil || second.installationID == first.installationID {
		t.Fatalf("identity reset did not create a new ID: first=%q second=%v", first.installationID, second)
	}
}

func TestInvalidEnvironmentOverrideFailsClosedWithRedactedDebug(t *testing.T) {
	home := t.TempDir()
	identity := "invalid-identity\n"
	writeTestFile(t, InstallationIDPath(home), identity, 0o600)
	var diagnostic strings.Builder
	session := Start(Config{
		Eligible:      true,
		HomeDir:       home,
		Endpoint:      "https://telemetry.example.test",
		Version:       "0.2.0",
		InstallSource: "archive",
		Environment:   map[string]string{EnvironmentVariable: "secret-invalid-value"},
		Preference:    func() (bool, bool) { return true, true },
		Debug:         true,
		DebugWriter:   &diagnostic,
	})
	if session != nil {
		t.Fatal("invalid override should disable telemetry")
	}
	if diagnostic.Len() != 0 {
		t.Fatalf("unexpected debug diagnostic %q", diagnostic.String())
	}
	assertFileContent(t, InstallationIDPath(home), identity)
}

func TestInstallationIDIsPrivateStableAndRaceSafe(t *testing.T) {
	home := t.TempDir()
	const workers = 32
	ids := make(chan string, workers)
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			session := Start(Config{
				Eligible:      true,
				HomeDir:       home,
				Endpoint:      "https://telemetry.example.test",
				Version:       "0.2.0",
				InstallSource: "archive",
				Environment:   map[string]string{EnvironmentVariable: "on"},
			})
			if session == nil {
				ids <- ""
				return
			}
			ids <- session.installationID
		}()
	}
	wait.Wait()
	close(ids)
	var expected string
	for id := range ids {
		if !installationIDPattern.MatchString(id) {
			t.Fatalf("invalid installation ID %q", id)
		}
		if expected == "" {
			expected = id
		} else if id != expected {
			t.Fatalf("concurrent initialization returned %q and %q", expected, id)
		}
	}
	data, err := os.ReadFile(InstallationIDPath(home))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(data)) != expected {
		t.Fatalf("persisted ID = %q, want %q", data, expected)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(InstallationIDPath(home))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("installation ID mode = %o, want 0600", info.Mode().Perm())
		}
	}
}

func TestInstallationIDPathThatCannotBeReadFailsClosed(t *testing.T) {
	t.Run("malformed", func(t *testing.T) {
		home := t.TempDir()
		before := "drive9_not-valid!\n"
		writeTestFile(t, InstallationIDPath(home), before, 0o600)
		if session := Start(enabledConfig(home)); session != nil {
			t.Fatal("malformed identity should disable telemetry")
		}
		assertFileContent(t, InstallationIDPath(home), before)
	})
	t.Run("unreadable state", func(t *testing.T) {
		home := t.TempDir()
		if err := os.MkdirAll(InstallationIDPath(home), 0o700); err != nil {
			t.Fatal(err)
		}
		if session := Start(enabledConfig(home)); session != nil {
			t.Fatal("unreadable identity state should disable telemetry")
		}
		info, err := os.Stat(InstallationIDPath(home))
		if err != nil || !info.IsDir() {
			t.Fatalf("identity state was overwritten: info=%v err=%v", info, err)
		}
	})
	t.Run("dangling symlink", func(t *testing.T) {
		home := t.TempDir()
		dir := filepath.Join(home, dirName)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join(dir, "missing"), InstallationIDPath(home)); err != nil {
			t.Fatal(err)
		}
		if session := Start(enabledConfig(home)); session != nil {
			t.Fatalf("dangling symlink produced a session with ID %q", session.installationID)
		}
	})
	t.Run("symlink to a valid identity", func(t *testing.T) {
		home := t.TempDir()
		dir := filepath.Join(home, dirName)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(home, "elsewhere")
		writeTestFile(t, target, "drive9_0123456789abcdefghijkl\n", 0o600)
		if err := os.Symlink(target, InstallationIDPath(home)); err != nil {
			t.Fatal(err)
		}
		if session := Start(enabledConfig(home)); session != nil {
			t.Fatal("symlinked identity should not be adopted")
		}
	})
}

func TestFinishSendsOnlyAllowlistedEventFields(t *testing.T) {
	requests := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/v1/telemetry/batch" {
			t.Errorf("unexpected request %s %s", request.Method, request.URL.Path)
		}
		if got := request.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q", got)
		}
		if got := request.Header.Get("User-Agent"); got != "drive9/0.2.0" {
			t.Errorf("User-Agent = %q", got)
		}
		body, _ := io.ReadAll(request.Body)
		requests <- body
		writer.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	home := t.TempDir()
	cfg := enabledConfig(home)
	cfg.Endpoint = server.URL + "/v1/telemetry/batch"
	cfg.Now = func() time.Time { return time.Date(2026, 7, 28, 1, 2, 3, 0, time.UTC) }
	session := Start(cfg)
	session.Finish(EventInput{
		CommandPath:   "drive9 fs cat",
		FlagNames:     []string{"json", "api-key", "server", "json", "Not A Flag"},
		ExitCode:      2,
		Duration:      182 * time.Millisecond,
		CloudProvider: "aws",
		RegionCode:    "aws-ap-southeast-1",
		ProfileSource: "explicit",
	})
	body := <-requests
	for _, prohibited := range []string{"SELECT secret_value", "private-key-value", "tenant-123", "profile-name", "/secret/path", "Not A Flag"} {
		if strings.Contains(string(body), prohibited) {
			t.Fatalf("payload contains prohibited value %q: %s", prohibited, body)
		}
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["schema_version"].(float64) != 2 {
		t.Fatalf("schema_version = %#v", decoded["schema_version"])
	}
	events := decoded["events"].([]any)
	if len(events) != 1 {
		t.Fatalf("events = %#v", events)
	}
	event := events[0].(map[string]any)
	wantKeys := []string{
		"anonymous_installation_id", "arch", "cli_version", "cloud_provider",
		"command_path", "duration_ms", "event_id", "event_name", "exit_code",
		"flag_names", "install_source", "occurred_at", "os", "profile_source",
		"region_code",
	}
	if len(event) != len(wantKeys) {
		t.Fatalf("event has %d fields, want %d: %#v", len(event), len(wantKeys), event)
	}
	for _, key := range wantKeys {
		if _, ok := event[key]; !ok {
			t.Fatalf("event is missing %q: %#v", key, event)
		}
	}
	if event["event_name"] != "drive9.command.finished" || event["command_path"] != "drive9 fs cat" {
		t.Fatalf("unexpected event: %#v", event)
	}
	if event["exit_code"].(float64) != 2 || event["duration_ms"].(float64) != 182 {
		t.Fatalf("unexpected outcome fields: %#v", event)
	}
	if !strings.HasPrefix(event["anonymous_installation_id"].(string), "drive9_") {
		t.Fatalf("installation id = %v", event["anonymous_installation_id"])
	}
	flags := event["flag_names"].([]any)
	if len(flags) != 3 || flags[0] != "api-key" || flags[1] != "json" || flags[2] != "server" {
		t.Fatalf("flag names were not canonicalized: %#v", flags)
	}
}

func TestFinishDropsEventsTheIngestionContractWouldReject(t *testing.T) {
	requests := make(chan []byte, 4)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		requests <- body
		writer.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	for _, test := range []struct {
		name   string
		config func(*Config)
		input  EventInput
	}{
		{name: "path deeper than the contract allows", input: EventInput{CommandPath: "drive9 admin tenant pool create"}},
		{name: "path containing user input", input: EventInput{CommandPath: "drive9 fs cat /secret"}},
		{name: "unsupported operating system", config: func(cfg *Config) { cfg.OS = "plan9x" }, input: EventInput{CommandPath: "drive9 fs ls"}},
		{name: "unsupported architecture", config: func(cfg *Config) { cfg.Arch = "sparc" }, input: EventInput{CommandPath: "drive9 fs ls"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := enabledConfig(t.TempDir())
			cfg.Endpoint = server.URL
			if test.config != nil {
				test.config(&cfg)
			}
			Start(cfg).Finish(test.input)
			select {
			case got := <-requests:
				t.Fatalf("unsupported event was sent: %s", got)
			default:
			}
		})
	}
}

func TestFinishClampsAndNormalizesFields(t *testing.T) {
	requests := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		requests <- body
		writer.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	home := t.TempDir()
	cfg := enabledConfig(home)
	cfg.Endpoint = server.URL
	cfg.Version = "-not-a-version"
	Start(cfg).Finish(EventInput{
		CommandPath: "drive9 admin tenant pool-create",
		ExitCode:    9001,
		Duration:    -time.Second,
	})
	body := <-requests
	var payload struct {
		Events []struct {
			CLIVersion string   `json:"cli_version"`
			ExitCode   int      `json:"exit_code"`
			DurationMS int64    `json:"duration_ms"`
			FlagNames  []string `json:"flag_names"`
		} `json:"events"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	event := payload.Events[0]
	if event.CLIVersion != "unknown" {
		t.Fatalf("cli_version = %q", event.CLIVersion)
	}
	if event.ExitCode != 255 || event.DurationMS != 0 || event.FlagNames == nil || len(event.FlagNames) != 0 {
		t.Fatalf("unexpected event: %s", body)
	}
}

func TestNormalizedRegionUsesAllowlist(t *testing.T) {
	if got := normalizedRegion("aws", "us-east-1"); got != "aws-us-east-1" {
		t.Fatalf("composed region = %q", got)
	}
	if got := normalizedRegion("aws", "custom-lab"); got != "unknown" {
		t.Fatalf("unknown region = %q", got)
	}
}

func TestDeliveryFailuresAreSilentByDefault(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusTooManyRequests, http.StatusInternalServerError} {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.WriteHeader(status)
			_, _ = writer.Write([]byte("sensitive backend response"))
		}))
		home := t.TempDir()
		cfg := enabledConfig(home)
		cfg.Endpoint = server.URL
		var debug strings.Builder
		cfg.DebugWriter = &debug
		Start(cfg).Finish(EventInput{CommandPath: "drive9 fs ls"})
		server.Close()
		if debug.Len() != 0 {
			t.Fatalf("status %d produced normal output: %q", status, debug.String())
		}
	}
}

func TestDeliveryDoesNotFollowRedirects(t *testing.T) {
	redirected := false
	sink := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		redirected = true
		writer.WriteHeader(http.StatusAccepted)
	}))
	defer sink.Close()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", sink.URL)
		writer.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	cfg := enabledConfig(t.TempDir())
	cfg.Endpoint = server.URL
	Start(cfg).Finish(EventInput{CommandPath: "drive9 fs ls"})
	if redirected {
		t.Fatal("telemetry followed a redirect away from its configured endpoint")
	}
}

func TestDeliveryTimeoutIsBounded(t *testing.T) {
	if deliveryTimeout > 1500*time.Millisecond {
		t.Fatalf("delivery timeout = %s, want a bounded exit-path budget", deliveryTimeout)
	}
}

func enabledConfig(home string) Config {
	return Config{
		Eligible:      true,
		HomeDir:       home,
		Endpoint:      "https://telemetry.example.test/v1/telemetry/batch",
		Version:       "0.2.0",
		OS:            "linux",
		Arch:          "amd64",
		InstallSource: "archive",
		Environment:   map[string]string{EnvironmentVariable: "on"},
	}
}

func recordedPreference(value *bool) func() (bool, bool) {
	if value == nil {
		return func() (bool, bool) { return false, false }
	}
	return func() (bool, bool) { return *value, true }
}

func writeTestFile(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
}

func assertFileContent(t *testing.T, path, want string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != want {
		t.Fatalf("%s changed: got %q, want %q", path, data, want)
	}
}

func TestClaimInstallationIDPathIsExclusive(t *testing.T) {
	path := filepath.Join(t.TempDir(), installationIDFile)
	claimed, err := claimInstallationIDPath(path)
	if err != nil || !claimed {
		t.Fatalf("first claim = %t, %v; want true, nil", claimed, err)
	}
	claimed, err = claimInstallationIDPath(path)
	if err != nil || claimed {
		t.Fatalf("second claim = %t, %v; want false, nil", claimed, err)
	}
	if info, err := os.Stat(path); err != nil {
		t.Fatal(err)
	} else if info.Mode().Perm() != 0o600 && runtime.GOOS != "windows" {
		t.Fatalf("claimed file mode = %o, want 0600", info.Mode().Perm())
	}
}
