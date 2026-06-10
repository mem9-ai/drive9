// Package cli provides the Drive9 CLI implementation.
//
// updatecheck.go implements a non-blocking CLI update checker inspired by
// gh CLI (github.com/cli/cli). It checks for newer Drive9 releases and
// optionally prompts the user after command execution completes.
//
// Design principles:
//   - Never block or delay the user's command.
//   - All output goes to stderr; never pollute stdout.
//   - Silently skip on any failure (network, parse, file I/O).
//   - Only prompt on interactive TTY; CI and scripts see nothing.
//   - Use semver comparison, never string comparison.
package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/mod/semver"
)

// updateCheckTTL is the minimum interval between network checks.
const updateCheckTTL = 24 * time.Hour

// updateCheckTimeout is the HTTP request timeout for fetching latest version.
const updateCheckTimeout = 1 * time.Second

// updateLatestURL is the endpoint that returns the latest release metadata.
// Override via DRIVE9_UPDATE_URL for testing.
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

	// Dev/unknown builds cannot be compared.
	if !isValidSemver(currentVersion) {
		return false
	}

	// Both stdout and stderr must be TTY.
	if !isTerminal(os.Stdout) || !isTerminal(os.Stderr) {
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
		if state.LatestVersion != "" && isNewerVersion(state.LatestVersion, currentVersion) {
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
	if rel == nil {
		return nil
	}

	// Persist state (atomic write).
	newState := &updateState{
		LastCheckedAt: time.Now().UTC(),
		LatestVersion: rel.Version,
		LatestURL:     rel.URL,
	}
	// Preserve skipped_version from previous state.
	if state != nil {
		newState.SkippedVersion = state.SkippedVersion
	}
	writeUpdateState(stateFilePath, newState)

	// Check if this version should be skipped.
	if newState.SkippedVersion != "" && newState.SkippedVersion == rel.Version {
		return nil
	}

	if isNewerVersion(rel.Version, currentVersion) {
		return rel
	}
	return nil
}

// PrintUpdateNotice displays the update notice on stderr after
// command execution. If stdin is a TTY, offers interactive choices.
func PrintUpdateNotice(rel *ReleaseInfo, currentVersion string) {
	if rel == nil {
		return
	}

	installCmd := installCommand()

	fmt.Fprintf(os.Stderr, "\nA new Drive9 CLI is available: %s → %s\n",
		trimV(currentVersion), trimV(rel.Version))

	if rel.URL != "" {
		fmt.Fprintf(os.Stderr, "Release notes: %s\n", rel.URL)
	}

	// Interactive prompt only if stdin is also a TTY.
	if isTerminal(os.Stdin) {
		fmt.Fprintf(os.Stderr, "\n  1. Update now\n  2. Skip this version\n\nChoose [1/2]: ")

		choice := readChoice(os.Stdin)
		switch choice {
		case "1":
			executeUpdate(installCmd)
		case "2":
			skipVersion(rel.Version)
			fmt.Fprintf(os.Stderr, "Skipped %s. You won't be reminded about this version.\n", trimV(rel.Version))
		default:
			// No valid choice (timeout, EOF, etc.) — just show the command.
			fmt.Fprintf(os.Stderr, "\nTo update later: %s\n", installCmd)
		}
	} else {
		// Non-interactive: just print the update command.
		fmt.Fprintf(os.Stderr, "To update: %s\n", installCmd)
	}
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

func installCommand() string {
	if url := os.Getenv("DRIVE9_UPDATE_URL"); url != "" {
		return fmt.Sprintf("curl -fsSL %s | sh", url)
	}
	return "curl -fsSL https://drive9.ai/install.sh | sh"
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
	// Atomic write: write to temp file, then rename.
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return
	}
	_ = os.Rename(tmp, path)
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
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return nil
	}

	var rel ReleaseInfo
	if err := json.NewDecoder(resp.Body).Decode(&rel); err != nil {
		return nil
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

func readChoice(r io.Reader) string {
	scanner := bufio.NewScanner(r)
	if scanner.Scan() {
		return strings.TrimSpace(scanner.Text())
	}
	return ""
}

func executeUpdate(installCmd string) {
	fmt.Fprintf(os.Stderr, "\nWill run: %s\n", installCmd)
	fmt.Fprintf(os.Stderr, "Press Enter to continue, or Ctrl+C to cancel...")

	// Wait for Enter (second confirmation).
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()

	fmt.Fprintln(os.Stderr)
	cmd := exec.Command("sh", "-c", installCmd)
	cmd.Stdout = os.Stderr // Update output goes to stderr
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin

	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Update failed: %v\n", err)
		fmt.Fprintf(os.Stderr, "You can try manually: %s\n", installCmd)
	}
}

func skipVersion(version string) {
	path := updateStateFilePath()
	state := readUpdateState(path)
	if state == nil {
		state = &updateState{}
	}
	state.SkippedVersion = version
	writeUpdateState(path, state)
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
