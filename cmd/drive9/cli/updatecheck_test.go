package cli

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestShouldCheckForUpdate_DevVersion(t *testing.T) {
	setTerminal(t, true)
	if ShouldCheckForUpdate("dev", "fs") {
		t.Fatalf("expected false for dev version")
	}
}

func TestShouldCheckForUpdate_EmptyVersion(t *testing.T) {
	setTerminal(t, true)
	if ShouldCheckForUpdate("", "fs") {
		t.Fatalf("expected false for empty version")
	}
}

func TestShouldCheckForUpdate_UnknownVersion(t *testing.T) {
	setTerminal(t, true)
	if ShouldCheckForUpdate("unknown", "fs") {
		t.Fatalf("expected false for unknown version")
	}
}

func TestShouldCheckForUpdate_ValidVersion(t *testing.T) {
	setTerminal(t, true)
	clearCIEnv(t)
	if !ShouldCheckForUpdate("v0.8.1", "fs") {
		t.Fatalf("expected true for valid semver version")
	}
}

func TestShouldCheckForUpdate_ValidVersionWithoutV(t *testing.T) {
	setTerminal(t, true)
	clearCIEnv(t)
	if !ShouldCheckForUpdate("0.8.1", "fs") {
		t.Fatalf("expected true for valid version without v prefix")
	}
}

func TestShouldCheckForUpdate_SHAVersion(t *testing.T) {
	setTerminal(t, true)
	clearCIEnv(t)
	if !ShouldCheckForUpdate("abc1234", "fs") {
		t.Fatalf("expected true for 7-char SHA release version")
	}
}

func TestShouldCheckForUpdate_ExplicitDisable(t *testing.T) {
	setTerminal(t, true)
	t.Setenv("DRIVE9_NO_UPDATE_CHECK", "1")
	if ShouldCheckForUpdate("v0.8.1", "fs") {
		t.Fatalf("expected false when DRIVE9_NO_UPDATE_CHECK is set")
	}
}

func TestShouldCheckForUpdate_CI(t *testing.T) {
	setTerminal(t, true)
	t.Setenv("CI", "true")
	if ShouldCheckForUpdate("v0.8.1", "fs") {
		t.Fatalf("expected false in CI")
	}
}

func TestShouldCheckForUpdate_GitHubActions(t *testing.T) {
	setTerminal(t, true)
	clearCIEnv(t)
	t.Setenv("GITHUB_ACTIONS", "true")
	if ShouldCheckForUpdate("v0.8.1", "fs") {
		t.Fatalf("expected false in GitHub Actions")
	}
}

func TestShouldCheckForUpdate_ExcludedMount(t *testing.T) {
	setTerminal(t, true)
	clearCIEnv(t)
	if ShouldCheckForUpdate("v0.8.1", "mount") {
		t.Fatalf("expected false for excluded command mount")
	}
}

func TestShouldCheckForUpdate_ExcludedUmount(t *testing.T) {
	setTerminal(t, true)
	clearCIEnv(t)
	if ShouldCheckForUpdate("v0.8.1", "umount") {
		t.Fatalf("expected false for excluded command umount")
	}
}

func TestShouldCheckForUpdate_ExcludedCompletion(t *testing.T) {
	setTerminal(t, true)
	clearCIEnv(t)
	if ShouldCheckForUpdate("v0.8.1", "completion") {
		t.Fatalf("expected false for excluded command completion")
	}
}

func TestShouldCheckForUpdate_ExcludedDaemon(t *testing.T) {
	setTerminal(t, true)
	clearCIEnv(t)
	if ShouldCheckForUpdate("v0.8.1", "daemon") {
		t.Fatalf("expected false for excluded command daemon")
	}
}

func TestShouldCheckForUpdate_NonTTY(t *testing.T) {
	setTerminal(t, false)
	clearCIEnv(t)
	if ShouldCheckForUpdate("v0.8.1", "fs") {
		t.Fatalf("expected false for non-TTY")
	}
}

func TestIsNewerVersion(t *testing.T) {
	tests := []struct {
		latest, current string
		want            bool
	}{
		{"v0.8.3", "v0.8.1", true},
		{"v0.8.1", "v0.8.3", false},
		{"v0.8.1", "v0.8.1", false},
		{"v0.10.0", "v0.9.0", true},  // semver, not string compare
		{"v1.0.0", "v0.99.99", true},  // major version bump
		{"0.8.3", "0.8.1", true},      // without v prefix
		{"v0.8.3", "0.8.1", true},     // mixed prefix
		{"invalid", "v0.8.1", false},  // invalid latest
		{"v0.8.3", "invalid", false},  // invalid current
		{"", "v0.8.1", false},
		{"v0.8.1", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.latest+"_vs_"+tt.current, func(t *testing.T) {
			got := isNewerVersion(tt.latest, tt.current)
			if got != tt.want {
				t.Fatalf("isNewerVersion(%q, %q) = %v, want %v", tt.latest, tt.current, got, tt.want)
			}
		})
	}
}

func TestIsSHAVersion(t *testing.T) {
	tests := []struct {
		v    string
		want bool
	}{
		{"abc1234", true},   // 7-char hex
		{"deadbeef", true},  // 8-char hex
		{"0123456789ab", true}, // 12-char hex
		{"abc123", false},   // too short (6)
		{"ABC1234", false},  // uppercase
		{"ghijklm", false},  // non-hex
		{"v0.8.1", false},   // semver
		{"dev", false},
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.v, func(t *testing.T) {
			got := isSHAVersion(tt.v)
			if got != tt.want {
				t.Fatalf("isSHAVersion(%q) = %v, want %v", tt.v, got, tt.want)
			}
		})
	}
}

func TestIsUpdateAvailable(t *testing.T) {
	tests := []struct {
		name            string
		latest, current string
		want            bool
	}{
		{"semver newer", "v0.9.0", "v0.8.1", true},
		{"semver same", "v0.8.1", "v0.8.1", false},
		{"semver older", "v0.7.0", "v0.8.1", false},
		{"sha current, semver latest", "v0.9.0", "abc1234", true},
		{"sha current, invalid latest", "not-a-version", "abc1234", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isUpdateAvailable(tt.latest, tt.current)
			if got != tt.want {
				t.Fatalf("isUpdateAvailable(%q, %q) = %v, want %v", tt.latest, tt.current, got, tt.want)
			}
		})
	}
}

func TestCheckForUpdate_NetworkFailure(t *testing.T) {
	// Point to a URL that will fail.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	withTempState(t)
	old := updateLatestURL
	updateLatestURL = srv.URL
	defer func() { updateLatestURL = old }()

	// Should return nil, not panic.
	result := CheckForUpdate(context.Background(), "v0.8.1")
	if result != nil {
		t.Fatalf("expected nil on network failure, got %+v", result)
	}
}

func TestCheckForUpdate_NetworkFailure_ThrottlesRetry(t *testing.T) {
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	withTempState(t)
	old := updateLatestURL
	updateLatestURL = srv.URL
	defer func() { updateLatestURL = old }()

	// First call fetches.
	_ = CheckForUpdate(context.Background(), "v0.8.1")
	if callCount != 1 {
		t.Fatalf("expected 1 fetch, got %d", callCount)
	}

	// Second call within TTL should NOT refetch.
	_ = CheckForUpdate(context.Background(), "v0.8.1")
	if callCount != 1 {
		t.Fatalf("expected throttled (still 1 fetch), got %d", callCount)
	}
}

func TestCheckForUpdate_InvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not json"))
	}))
	defer srv.Close()

	withTempState(t)
	old := updateLatestURL
	updateLatestURL = srv.URL
	defer func() { updateLatestURL = old }()

	result := CheckForUpdate(context.Background(), "v0.8.1")
	if result != nil {
		t.Fatalf("expected nil on invalid JSON, got %+v", result)
	}
}

func TestCheckForUpdate_PlainTextVersion(t *testing.T) {
	// The release workflow may publish a plain text "version" file instead
	// of JSON. fetchLatestRelease should fall back to treating the body as
	// a version string.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("v0.9.0\n"))
	}))
	defer srv.Close()

	withTempState(t)
	old := updateLatestURL
	updateLatestURL = srv.URL
	defer func() { updateLatestURL = old }()

	result := CheckForUpdate(context.Background(), "v0.8.1")
	if result == nil {
		t.Fatalf("expected non-nil result for plain text version")
	}
	if result.Version != "v0.9.0" {
		t.Fatalf("got version %q, want %q", result.Version, "v0.9.0")
	}
}

func TestCheckForUpdate_EmptyBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{}"))
	}))
	defer srv.Close()

	withTempState(t)
	old := updateLatestURL
	updateLatestURL = srv.URL
	defer func() { updateLatestURL = old }()

	result := CheckForUpdate(context.Background(), "v0.8.1")
	if result != nil {
		t.Fatalf("expected nil on empty body, got %+v", result)
	}
}

func TestCheckForUpdate_NewerVersion(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ReleaseInfo{
			Version: "v0.9.0",
			URL:     "https://github.com/mem9-ai/drive9/releases/tag/v0.9.0",
		})
	}))
	defer srv.Close()

	withTempState(t)
	old := updateLatestURL
	updateLatestURL = srv.URL
	defer func() { updateLatestURL = old }()

	result := CheckForUpdate(context.Background(), "v0.8.1")
	if result == nil {
		t.Fatalf("expected non-nil result for newer version")
	}
	if result.Version != "v0.9.0" {
		t.Fatalf("got version %q, want %q", result.Version, "v0.9.0")
	}
}

func TestCheckForUpdate_SHACurrentVersion(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ReleaseInfo{Version: "v0.9.0"})
	}))
	defer srv.Close()

	withTempState(t)
	old := updateLatestURL
	updateLatestURL = srv.URL
	defer func() { updateLatestURL = old }()

	// SHA version should see any semver as an update.
	result := CheckForUpdate(context.Background(), "abc1234")
	if result == nil {
		t.Fatalf("expected update notice for SHA version")
	}
	if result.Version != "v0.9.0" {
		t.Fatalf("got version %q, want %q", result.Version, "v0.9.0")
	}
}

func TestCheckForUpdate_SameVersion(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ReleaseInfo{Version: "v0.8.1"})
	}))
	defer srv.Close()

	withTempState(t)
	old := updateLatestURL
	updateLatestURL = srv.URL
	defer func() { updateLatestURL = old }()

	result := CheckForUpdate(context.Background(), "v0.8.1")
	if result != nil {
		t.Fatalf("expected nil for same version, got %+v", result)
	}
}

func TestCheckForUpdate_OlderVersion(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ReleaseInfo{Version: "v0.7.0"})
	}))
	defer srv.Close()

	withTempState(t)
	old := updateLatestURL
	updateLatestURL = srv.URL
	defer func() { updateLatestURL = old }()

	result := CheckForUpdate(context.Background(), "v0.8.1")
	if result != nil {
		t.Fatalf("expected nil for older version, got %+v", result)
	}
}

func TestCheckForUpdate_TTLRespected(t *testing.T) {
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount++
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ReleaseInfo{Version: "v0.9.0"})
	}))
	defer srv.Close()

	stateDir := withTempState(t)
	old := updateLatestURL
	updateLatestURL = srv.URL
	defer func() { updateLatestURL = old }()

	// First check: should fetch.
	r1 := CheckForUpdate(context.Background(), "v0.8.1")
	if r1 == nil {
		t.Fatalf("expected non-nil result on first check")
	}
	if callCount != 1 {
		t.Fatalf("expected 1 fetch, got %d", callCount)
	}

	// Second check: should use cache, not fetch.
	r2 := CheckForUpdate(context.Background(), "v0.8.1")
	if r2 == nil {
		t.Fatalf("expected non-nil result from cache")
	}
	if callCount != 1 {
		t.Fatalf("expected no additional fetch (still 1), got %d", callCount)
	}

	// Manually expire the TTL.
	path := filepath.Join(stateDir, "drive9", "update-check.json")
	state := readUpdateState(path)
	if state == nil {
		t.Fatalf("expected non-nil state after write")
	}
	state.LastCheckedAt = time.Now().Add(-25 * time.Hour)
	writeUpdateState(path, state)

	// Third check: TTL expired, should fetch again.
	r3 := CheckForUpdate(context.Background(), "v0.8.1")
	if r3 == nil {
		t.Fatalf("expected non-nil result after TTL expiry")
	}
	if callCount != 2 {
		t.Fatalf("expected 2 fetches after TTL expiry, got %d", callCount)
	}
}

func TestCheckForUpdate_SkippedVersion(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ReleaseInfo{Version: "v0.9.0"})
	}))
	defer srv.Close()

	stateDir := withTempState(t)
	old := updateLatestURL
	updateLatestURL = srv.URL
	defer func() { updateLatestURL = old }()

	// Pre-seed state with skipped version matching latest.
	path := filepath.Join(stateDir, "drive9", "update-check.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(&updateState{
		SkippedVersion: "v0.9.0",
	})
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("failed to write state: %v", err)
	}

	result := CheckForUpdate(context.Background(), "v0.8.1")
	if result != nil {
		t.Fatalf("skipped version should not be returned, got %+v", result)
	}
}

func TestCheckForUpdate_SkipOldVersionStillPromptsNewer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ReleaseInfo{Version: "v0.9.1"})
	}))
	defer srv.Close()

	stateDir := withTempState(t)
	old := updateLatestURL
	updateLatestURL = srv.URL
	defer func() { updateLatestURL = old }()

	// Pre-seed state: user skipped v0.9.0, but v0.9.1 is now latest.
	path := filepath.Join(stateDir, "drive9", "update-check.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(&updateState{
		SkippedVersion: "v0.9.0",
	})
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("failed to write state: %v", err)
	}

	result := CheckForUpdate(context.Background(), "v0.8.1")
	if result == nil {
		t.Fatalf("newer version beyond skipped should still prompt")
	}
	if result.Version != "v0.9.1" {
		t.Fatalf("got version %q, want %q", result.Version, "v0.9.1")
	}
}

func TestCheckForUpdate_CorruptStateFile(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ReleaseInfo{Version: "v0.9.0"})
	}))
	defer srv.Close()

	stateDir := withTempState(t)
	old := updateLatestURL
	updateLatestURL = srv.URL
	defer func() { updateLatestURL = old }()

	// Write corrupt state file.
	path := filepath.Join(stateDir, "drive9", "update-check.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("not json {{{"), 0o600); err != nil {
		t.Fatalf("failed to write corrupt state: %v", err)
	}

	// Should not panic; should fetch and return result.
	result := CheckForUpdate(context.Background(), "v0.8.1")
	if result == nil {
		t.Fatalf("expected non-nil result after corrupt state")
	}
	if result.Version != "v0.9.0" {
		t.Fatalf("got version %q, want %q", result.Version, "v0.9.0")
	}
}

func TestCheckForUpdate_InvalidVersionInResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ReleaseInfo{Version: "not-a-version"})
	}))
	defer srv.Close()

	withTempState(t)
	old := updateLatestURL
	updateLatestURL = srv.URL
	defer func() { updateLatestURL = old }()

	result := CheckForUpdate(context.Background(), "v0.8.1")
	if result != nil {
		t.Fatalf("expected nil for invalid version in response, got %+v", result)
	}
}

func TestCheckForUpdate_ContextCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(5 * time.Second) // Simulate slow response.
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	withTempState(t)
	old := updateLatestURL
	updateLatestURL = srv.URL
	defer func() { updateLatestURL = old }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	result := CheckForUpdate(ctx, "v0.8.1")
	if result != nil {
		t.Fatalf("expected nil on cancelled context, got %+v", result)
	}
}

func TestPrintUpdateNotice_NonInteractive(t *testing.T) {
	// Capture stderr output.
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create pipe: %v", err)
	}

	oldStderr := os.Stderr
	os.Stderr = w

	// Non-interactive: isTerminal for stdin returns false.
	oldIsTerminal := isTerminal
	isTerminal = func(f *os.File) bool {
		return f != os.Stdin // stderr=true, stdout=true, stdin=false
	}
	defer func() {
		os.Stderr = oldStderr
		isTerminal = oldIsTerminal
	}()

	rel := &ReleaseInfo{Version: "v0.9.0", URL: "https://example.com"}
	PrintUpdateNotice(rel, "v0.8.1")

	_ = w.Close()
	buf := make([]byte, 4096)
	n, _ := r.Read(buf)
	output := string(buf[:n])

	// Banner must show current and latest versions.
	if !strings.Contains(output, "0.8.1") {
		t.Fatalf("expected output to contain current version %q, got %q", "0.8.1", output)
	}
	if !strings.Contains(output, "0.9.0") {
		t.Fatalf("expected output to contain latest version %q, got %q", "0.9.0", output)
	}
	// Non-blocking box banner: must contain box drawing characters and
	// the "Update available:" / "Run:" lines.
	if !strings.Contains(output, "Update available:") {
		t.Fatalf("expected output to contain %q, got %q", "Update available:", output)
	}
	if !strings.Contains(output, "Run:") {
		t.Fatalf("expected output to contain %q, got %q", "Run:", output)
	}
	if !strings.Contains(output, "╭") || !strings.Contains(output, "╰") {
		t.Fatalf("expected box drawing characters in output, got %q", output)
	}
	// Release notes URL should be included.
	if !strings.Contains(output, "Release notes:") {
		t.Fatalf("expected output to contain release notes URL, got %q", output)
	}
}

func TestPrintUpdateNotice_Nil(t *testing.T) {
	// Should not panic.
	PrintUpdateNotice(nil, "v0.8.1")
}

func TestAtomicStateWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "drive9", "update-check.json")

	state := &updateState{
		LastCheckedAt:  time.Now().UTC(),
		LatestVersion:  "v0.9.0",
		LatestURL:      "https://example.com",
		SkippedVersion: "v0.8.5",
	}
	writeUpdateState(path, state)

	// Verify it was written correctly.
	got := readUpdateState(path)
	if got == nil {
		t.Fatalf("expected non-nil state after write")
	}
	if got.LatestVersion != "v0.9.0" {
		t.Fatalf("got LatestVersion %q, want %q", got.LatestVersion, "v0.9.0")
	}
	if got.SkippedVersion != "v0.8.5" {
		t.Fatalf("got SkippedVersion %q, want %q", got.SkippedVersion, "v0.8.5")
	}

	// Verify no temp files left behind.
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatalf("failed to read dir: %v", err)
	}
	for _, e := range entries {
		if strings.Contains(e.Name(), ".tmp") {
			t.Fatalf("temp file %s should be cleaned up", e.Name())
		}
	}
}

func TestIsValidSemver(t *testing.T) {
	tests := []struct {
		v    string
		want bool
	}{
		{"v0.8.1", true},
		{"0.8.1", true},
		{"v1.0.0", true},
		{"v0.8.1-rc1", true},
		{"dev", false},
		{"unknown", false},
		{"", false},
		{"not-a-version", false},
	}
	for _, tt := range tests {
		t.Run(tt.v, func(t *testing.T) {
			got := isValidSemver(tt.v)
			if got != tt.want {
				t.Fatalf("isValidSemver(%q) = %v, want %v", tt.v, got, tt.want)
			}
		})
	}
}

// --- Test helpers ---

// setTerminal overrides isTerminal for the duration of the test.
func setTerminal(t *testing.T, value bool) {
	t.Helper()
	old := isTerminal
	isTerminal = func(_ *os.File) bool { return value }
	t.Cleanup(func() { isTerminal = old })
}

// clearCIEnv unsets all CI environment variables for the test.
func clearCIEnv(t *testing.T) {
	t.Helper()
	for _, v := range []string{
		"CI", "GITHUB_ACTIONS", "JENKINS_URL", "TRAVIS",
		"CIRCLECI", "GITLAB_CI", "BUILDKITE", "TF_BUILD",
		"CODEBUILD_BUILD_ID", "CODESPACES",
		"DRIVE9_NO_UPDATE_CHECK",
	} {
		// t.Setenv sets the var for the test duration and restores on cleanup.
		// Setting to "" effectively unsets it for os.Getenv("...") != "" checks.
		t.Setenv(v, "")
	}
}

// withTempState sets XDG_CONFIG_HOME to a temp dir and returns the path.
func withTempState(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)
	return dir
}
