// Package cli provides the Drive9 CLI implementation.
//
// updatecheck.go implements a non-blocking CLI update checker inspired by
// gh CLI (github.com/cli/cli) and npm. It checks for newer Drive9 releases
// and displays a non-blocking banner after command execution completes.
//
// Design principles:
//   - Never block or delay the user's command.
//   - Never prompt interactively — display a banner and let the user decide.
//   - All output goes to stderr; never pollute stdout.
//   - Silently skip on any failure (network, parse, file I/O).
//   - Only show on interactive TTY; CI and scripts see nothing.
//   - Use semver comparison, never string comparison.
package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"golang.org/x/mod/semver"
)

// updateCheckTTL is the minimum interval between network checks.
const updateCheckTTL = 24 * time.Hour

// updateCheckTimeout is the HTTP request timeout for fetching latest version.
const updateCheckTimeout = 1 * time.Second

// updateLatestURL is the endpoint that returns the latest release metadata.
// Override via DRIVE9_UPDATE_URL for testing or internal mirrors. This only
// affects the version-check fetch URL; the install command shown to the user
// is always the default public URL.
//
// The release workflow (.github/workflows/release-cli.yml) publishes a plain
// text "version" file at site/releases/version. When this endpoint returns
// a plain text body (not JSON), we treat it as a version-only response.
var updateLatestURL = "https://drive9.ai/releases/latest.json"

// ReleaseInfo holds metadata about a release fetched from the update endpoint.
type ReleaseInfo struct {
	Version string `json:"version"`
	URL     string `json:"url"`
}

// updateState persists check/skip state between CLI invocations.
type updateState struct {
	LastCheckedAt  time.Time `json:"last_checked_at"`
	LatestVersion  string    `json:"latest_version,omitempty"`
	LatestURL      string    `json:"latest_url,omitempty"`
	SkippedVersion string    `json:"skipped_version,omitempty"`
}

// excludedCommands are commands that should never show update prompts,
// even on interactive TTY. These are long-running, daemon-like, or
// machine-output commands.
var excludedCommands = map[string]bool{
	"mount":      true,
	"umount":     true,
	"completion": true,
	"__complete": true,
	"daemon":     true,
}

// ShouldCheckForUpdate decides whether update checking is appropriate
// for the current execution context. Returns false for CI, non-TTY,
// dev builds, excluded commands, or when explicitly disabled.
func ShouldCheckForUpdate(currentVersion, command string) bool {
	// Explicit opt-out.
	if os.Getenv("DRIVE9_NO_UPDATE_CHECK") != "" {
		return false
	}

	// CI detection (same set as gh CLI).
	if isCI() {
		return false
	}

	// Excluded commands.
	if excludedCommands[command] {
		return false
	}

	// Dev/unknown builds cannot be compared. The release workflow sets
	// VERSION to a 7-char SHA (not semver), so also allow those through —
	// they represent real release builds that should still see update
	// notices when a semver-tagged release becomes available.
	if !isValidSemver(currentVersion) && !isSHAVersion(currentVersion) {
		return false
	}

	// All update output goes to stderr; only check stderr TTY.
	if !isTerminal(os.Stderr) {
		return false
	}

	return true
}

// CheckForUpdate checks whether a newer version is available.
// It respects a 24h TTL cache and returns nil on any error or
// if no update is available. Safe to call from a goroutine.
func CheckForUpdate(ctx context.Context, currentVersion string) *ReleaseInfo {
	stateFilePath := updateStateFilePath()

	// Check TTL: if we checked recently, use cached state.
	state := readUpdateState(stateFilePath)
	if state != nil && time.Since(state.LastCheckedAt) < updateCheckTTL {
		// Use cached version if available and newer.
		if state.LatestVersion != "" && isUpdateAvailable(state.LatestVersion, currentVersion) {
			if state.SkippedVersion != "" && state.SkippedVersion == state.LatestVersion {
				return nil
			}
			return &ReleaseInfo{
				Version: state.LatestVersion,
				URL:     state.LatestURL,
			}
		}
		return nil
	}

	// Fetch latest release info.
	rel := fetchLatestRelease(ctx)

	// Always record a check timestamp, even on failure, so we don't
	// reattempt on every CLI invocation during an outage.
	newState := &updateState{
		LastCheckedAt: time.Now().UTC(),
	}
	if state != nil {
		newState.SkippedVersion = state.SkippedVersion
	}
	if rel != nil {
		newState.LatestVersion = rel.Version
		newState.LatestURL = rel.URL
	}
	writeUpdateState(stateFilePath, newState)

	if rel == nil {
		return nil
	}

	// Check if this version should be skipped.
	if newState.SkippedVersion != "" && newState.SkippedVersion == rel.Version {
		return nil
	}

	if isUpdateAvailable(rel.Version, currentVersion) {
		return rel
	}
	return nil
}

// isUpdateAvailable returns true when the user should be notified about
// a new release. For SHA-based release builds, any valid semver latest
// version is considered an update (we can't compare SHAs to semver).
func isUpdateAvailable(latest, current string) bool {
	if isSHAVersion(current) {
		return isValidSemver(latest)
	}
	return isNewerVersion(latest, current)
}

// PrintUpdateNotice displays a non-blocking update banner on stderr after
// command execution completes. The banner shows the available version and
// the command to run — it never prompts or waits for user input.
//
// Style follows the npm/gh CLI pattern: a visible box that doesn't interrupt
// the user's workflow.
func PrintUpdateNotice(rel *ReleaseInfo, currentVersion string) {
	if rel == nil {
		return
	}

	installCmd := installCommand()
	cur := trimV(currentVersion)
	next := trimV(rel.Version)

	// Build the banner lines.
	lines := []string{
		fmt.Sprintf("Update available: %s → %s", cur, next),
		fmt.Sprintf("Run: %s", installCmd),
	}
	if rel.URL != "" {
		lines = append(lines, fmt.Sprintf("Release notes: %s", rel.URL))
	}

	// Calculate box width (widest line + 4 for "│  " + "  │" padding).
	maxLen := 0
	for _, l := range lines {
		if len(l) > maxLen {
			maxLen = len(l)
		}
	}
	width := maxLen + 4 // 2 chars padding on each side

	// Draw the box.
	top := "╭" + strings.Repeat("─", width) + "╮"
	bot := "╰" + strings.Repeat("─", width) + "╯"

	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, top)
	for _, l := range lines {
		pad := width - len(l) - 2 // subtract left padding "  "
		if pad < 0 {
			pad = 0
		}
		fmt.Fprintf(os.Stderr, "│  %s%s│\n", l, strings.Repeat(" ", pad))
	}
	fmt.Fprintln(os.Stderr, bot)
	fmt.Fprintln(os.Stderr)
}

// --- Internal helpers ---

func isCI() bool {
	ciEnvVars := []string{
		"CI",
		"GITHUB_ACTIONS",
		"JENKINS_URL",
		"TRAVIS",
		"CIRCLECI",
		"GITLAB_CI",
		"BUILDKITE",
		"TF_BUILD",
		"CODEBUILD_BUILD_ID",
		"CODESPACES",
	}
	for _, v := range ciEnvVars {
		if os.Getenv(v) != "" {
			return true
		}
	}
	return false
}

func isValidSemver(v string) bool {
	if v == "" || v == "dev" || v == "unknown" {
		return false
	}
	sv := ensureVPrefix(v)
	return semver.IsValid(sv)
}

// isSHAVersion returns true if v looks like a short git SHA (hex, 7-12 chars),
// which is what the release workflow sets as VERSION.
func isSHAVersion(v string) bool {
	if len(v) < 7 || len(v) > 12 {
		return false
	}
	for _, c := range v {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func isNewerVersion(latest, current string) bool {
	l := ensureVPrefix(latest)
	c := ensureVPrefix(current)
	if !semver.IsValid(l) || !semver.IsValid(c) {
		return false
	}
	return semver.Compare(l, c) > 0
}

func ensureVPrefix(v string) string {
	if !strings.HasPrefix(v, "v") {
		return "v" + v
	}
	return v
}

func trimV(v string) string {
	return strings.TrimPrefix(v, "v")
}

// installScriptURL returns the install script URL. Separated from the
// shell command construction to avoid interpolating env vars into shell
// pipelines.
func installScriptURL() string {
	return "https://drive9.ai/install.sh"
}

func installCommand() string {
	if isWindows() {
		return fmt.Sprintf("powershell -Command \"irm %s | iex\"", installScriptURL())
	}
	return fmt.Sprintf("curl -fsSL %s | sh", installScriptURL())
}

func updateStateFilePath() string {
	if dir := os.Getenv("XDG_CONFIG_HOME"); dir != "" {
		return filepath.Join(dir, "drive9", "update-check.json")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".config", "drive9", "update-check.json")
}

func readUpdateState(path string) *updateState {
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var s updateState
	if err := json.Unmarshal(data, &s); err != nil {
		return nil
	}
	return &s
}

// writeUpdateState persists state using atomic write (write-to-temp + rename).
// Errors are silently ignored — update check state is best-effort.
func writeUpdateState(path string, s *updateState) {
	if path == "" || s == nil {
		return
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return
	}
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return
	}
	// Atomic write: create temp file in the same directory (avoids
	// cross-device rename failures), then rename over the target.
	tmpFile, err := os.CreateTemp(dir, ".update-check-*.tmp")
	if err != nil {
		return
	}
	tmpPath := tmpFile.Name()
	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
	}
}

func fetchLatestRelease(ctx context.Context) *ReleaseInfo {
	url := updateLatestURL
	if envURL := os.Getenv("DRIVE9_UPDATE_URL"); envURL != "" {
		url = envURL
	}

	ctx, cancel := context.WithTimeout(ctx, updateCheckTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return nil
	}

	// Try JSON first; fall back to plain text version string.
	// The release workflow publishes either a JSON object with
	// {"version":"...", "url":"..."} or a plain text version file.
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil || len(body) == 0 {
		return nil
	}

	var rel ReleaseInfo
	if err := json.Unmarshal(body, &rel); err != nil {
		// Plain text fallback: treat the entire body as a version string.
		rel.Version = strings.TrimSpace(string(body))
	}

	if rel.Version == "" {
		return nil
	}

	// Validate that version is valid semver.
	if !isValidSemver(rel.Version) {
		return nil
	}

	return &rel
}

// isWindows reports whether the current platform is Windows.
// Extracted as a var for testability.
var isWindows = func() bool {
	return runtime.GOOS == "windows"
}

// isTerminal checks if a file descriptor is a terminal.
// Extracted for testability.
var isTerminal = func(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}
