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

	"github.com/stretchr/testify/require"
)

func TestShouldCheckForUpdate_DevVersion(t *testing.T) {
	setTerminal(t, true)
	require.False(t, ShouldCheckForUpdate("dev", "fs"))
}

func TestShouldCheckForUpdate_EmptyVersion(t *testing.T) {
	setTerminal(t, true)
	require.False(t, ShouldCheckForUpdate("", "fs"))
}

func TestShouldCheckForUpdate_UnknownVersion(t *testing.T) {
	setTerminal(t, true)
	require.False(t, ShouldCheckForUpdate("unknown", "fs"))
}

func TestShouldCheckForUpdate_ValidVersion(t *testing.T) {
	setTerminal(t, true)
	clearCIEnv(t)
	require.True(t, ShouldCheckForUpdate("v0.8.1", "fs"))
}

func TestShouldCheckForUpdate_ValidVersionWithoutV(t *testing.T) {
	setTerminal(t, true)
	clearCIEnv(t)
	require.True(t, ShouldCheckForUpdate("0.8.1", "fs"))
}

func TestShouldCheckForUpdate_ExplicitDisable(t *testing.T) {
	setTerminal(t, true)
	t.Setenv("DRIVE9_NO_UPDATE_CHECK", "1")
	require.False(t, ShouldCheckForUpdate("v0.8.1", "fs"))
}

func TestShouldCheckForUpdate_CI(t *testing.T) {
	setTerminal(t, true)
	t.Setenv("CI", "true")
	require.False(t, ShouldCheckForUpdate("v0.8.1", "fs"))
}

func TestShouldCheckForUpdate_GitHubActions(t *testing.T) {
	setTerminal(t, true)
	clearCIEnv(t)
	t.Setenv("GITHUB_ACTIONS", "true")
	require.False(t, ShouldCheckForUpdate("v0.8.1", "fs"))
}

func TestShouldCheckForUpdate_ExcludedMount(t *testing.T) {
	setTerminal(t, true)
	clearCIEnv(t)
	require.False(t, ShouldCheckForUpdate("v0.8.1", "mount"))
}

func TestShouldCheckForUpdate_ExcludedUmount(t *testing.T) {
	setTerminal(t, true)
	clearCIEnv(t)
	require.False(t, ShouldCheckForUpdate("v0.8.1", "umount"))
}

func TestShouldCheckForUpdate_ExcludedCompletion(t *testing.T) {
	setTerminal(t, true)
	clearCIEnv(t)
	require.False(t, ShouldCheckForUpdate("v0.8.1", "completion"))
}

func TestShouldCheckForUpdate_ExcludedDaemon(t *testing.T) {
	setTerminal(t, true)
	clearCIEnv(t)
	require.False(t, ShouldCheckForUpdate("v0.8.1", "daemon"))
}

func TestShouldCheckForUpdate_NonTTY(t *testing.T) {
	setTerminal(t, false)
	clearCIEnv(t)
	require.False(t, ShouldCheckForUpdate("v0.8.1", "fs"))
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
			require.Equal(t, tt.want, isNewerVersion(tt.latest, tt.current))
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
	require.Nil(t, result)
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
	require.Nil(t, result)
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
	require.Nil(t, result)
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
	require.NotNil(t, result)
	require.Equal(t, "v0.9.0", result.Version)
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
	require.Nil(t, result)
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
	require.Nil(t, result)
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
	require.NotNil(t, r1)
	require.Equal(t, 1, callCount)

	// Second check: should use cache, not fetch.
	r2 := CheckForUpdate(context.Background(), "v0.8.1")
	require.NotNil(t, r2)
	require.Equal(t, 1, callCount) // No additional fetch.

	// Manually expire the TTL.
	path := filepath.Join(stateDir, "drive9", "update-check.json")
	state := readUpdateState(path)
	require.NotNil(t, state)
	state.LastCheckedAt = time.Now().Add(-25 * time.Hour)
	writeUpdateState(path, state)

	// Third check: TTL expired, should fetch again.
	r3 := CheckForUpdate(context.Background(), "v0.8.1")
	require.NotNil(t, r3)
	require.Equal(t, 2, callCount)
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
	require.NoError(t, os.WriteFile(path, data, 0o600))

	result := CheckForUpdate(context.Background(), "v0.8.1")
	require.Nil(t, result, "skipped version should not be returned")
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
	require.NoError(t, os.WriteFile(path, data, 0o600))

	result := CheckForUpdate(context.Background(), "v0.8.1")
	require.NotNil(t, result, "newer version beyond skipped should still prompt")
	require.Equal(t, "v0.9.1", result.Version)
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
	require.NoError(t, os.WriteFile(path, []byte("not json {{{"), 0o600))

	// Should not panic; should fetch and return result.
	result := CheckForUpdate(context.Background(), "v0.8.1")
	require.NotNil(t, result)
	require.Equal(t, "v0.9.0", result.Version)
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
	require.Nil(t, result)
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
	require.Nil(t, result)
}

func TestPrintUpdateNotice_NonInteractive(t *testing.T) {
	// Capture stderr output.
	r, w, err := os.Pipe()
	require.NoError(t, err)

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

	w.Close()
	buf := make([]byte, 4096)
	n, _ := r.Read(buf)
	output := string(buf[:n])

	require.Contains(t, output, "0.8.1")
	require.Contains(t, output, "0.9.0")
	require.Contains(t, output, "To update:")
	// Should NOT contain interactive choices.
	require.NotContains(t, output, "Choose [1/2]")
}

func TestPrintUpdateNotice_Nil(t *testing.T) {
	// Should not panic.
	PrintUpdateNotice(nil, "v0.8.1")
}

func TestReadChoice(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"1\n", "1"},
		{"2\n", "2"},
		{" 1 \n", "1"},
		{"\n", ""},
		{"", ""},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			r := strings.NewReader(tt.input)
			require.Equal(t, tt.want, readChoice(r))
		})
	}
}

func TestAtomicStateWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "drive9", "update-check.json")

	state := &updateState{
		LastCheckedAt: time.Now().UTC(),
		LatestVersion: "v0.9.0",
		LatestURL:     "https://example.com",
		SkippedVersion: "v0.8.5",
	}
	writeUpdateState(path, state)

	// Verify it was written correctly.
	got := readUpdateState(path)
	require.NotNil(t, got)
	require.Equal(t, "v0.9.0", got.LatestVersion)
	require.Equal(t, "v0.8.5", got.SkippedVersion)

	// Verify no temp file left behind.
	_, err := os.Stat(path + ".tmp")
	require.True(t, os.IsNotExist(err), "temp file should be cleaned up")
}

func TestSkipVersion(t *testing.T) {
	stateDir := withTempState(t)

	skipVersion("v0.9.0")

	path := filepath.Join(stateDir, "drive9", "update-check.json")
	state := readUpdateState(path)
	require.NotNil(t, state)
	require.Equal(t, "v0.9.0", state.SkippedVersion)
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
			require.Equal(t, tt.want, isValidSemver(tt.v))
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
		t.Setenv(v, "")
		os.Unsetenv(v)
	}
}

// withTempState sets XDG_CONFIG_HOME to a temp dir and returns the path.
func withTempState(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)
	return dir
}
