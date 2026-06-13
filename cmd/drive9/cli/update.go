package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/buildinfo"
)

const (
	defaultUpdateBaseURL = "https://drive9.ai"
	updateCheckInterval  = 24 * time.Hour
	autoUpdateTimeout    = 1500 * time.Millisecond
	updateCommandTimeout = 2 * time.Minute
	maxUpdateFileBytes   = 200 << 20
)

type updateDeps struct {
	baseURL           string
	currentVersion    string
	goos              string
	goarch            string
	now               func() time.Time
	executable        func() (string, error)
	httpClient        *http.Client
	stdout            io.Writer
	stderr            io.Writer
	replaceExecutable func(string, string) error
}

type updateCache struct {
	LastCheckedAt time.Time `json:"last_checked_at"`
	LatestVersion string    `json:"latest_version,omitempty"`
	LatestURL     string    `json:"latest_url,omitempty"`
}

func defaultUpdateDeps() updateDeps {
	baseURL := strings.TrimSpace(os.Getenv("DRIVE9_UPDATE_BASE_URL"))
	if baseURL == "" {
		baseURL = defaultUpdateBaseURL
	}
	return updateDeps{
		baseURL:           baseURL,
		currentVersion:    buildinfo.Version,
		goos:              runtime.GOOS,
		goarch:            runtime.GOARCH,
		now:               time.Now,
		executable:        os.Executable,
		httpClient:        &http.Client{Timeout: updateCommandTimeout},
		stdout:            os.Stdout,
		stderr:            os.Stderr,
		replaceExecutable: replaceExecutableFile,
	}
}

// Update updates the running drive9 CLI binary in place.
func Update(args []string) error {
	return updateWithDeps(args, defaultUpdateDeps())
}

func updateWithDeps(args []string, deps updateDeps) error {
	deps = fillUpdateDeps(deps)
	if len(args) > 0 && IsHelpArg(args[0]) {
		_, _ = fmt.Fprintln(deps.stdout, updateUsage())
		return nil
	}

	fs := flag.NewFlagSet("update", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	checkOnly := fs.Bool("check", false, "check for the latest version without installing it")
	force := fs.Bool("force", false, "reinstall the latest binary even when versions match")
	baseURL := fs.String("base-url", deps.baseURL, "release base URL")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("%w\n%s", err, updateUsage())
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected argument %q\n%s", fs.Arg(0), updateUsage())
	}
	deps.baseURL = normalizeUpdateBaseURL(*baseURL)

	ctx, cancel := context.WithTimeout(context.Background(), updateCommandTimeout)
	defer cancel()

	latestVersion, err := fetchLatestVersion(ctx, deps)
	if err != nil {
		return err
	}
	artifact := updateArtifactName(deps.goos, deps.goarch)
	latestURL := releaseURL(deps.baseURL, artifact)
	_ = writeUpdateCache(updateCache{
		LastCheckedAt: deps.now().UTC(),
		LatestVersion: latestVersion,
		LatestURL:     latestURL,
	})

	if *checkOnly {
		if versionsDiffer(deps.currentVersion, latestVersion) {
			_, _ = fmt.Fprintf(deps.stdout, "drive9 update available: %s -> %s\n", displayVersion(deps.currentVersion), latestVersion)
		} else {
			_, _ = fmt.Fprintf(deps.stdout, "drive9 is up to date (%s)\n", displayVersion(deps.currentVersion))
		}
		return nil
	}
	if !*force && !versionsDiffer(deps.currentVersion, latestVersion) {
		_, _ = fmt.Fprintf(deps.stdout, "drive9 is already up to date (%s)\n", displayVersion(deps.currentVersion))
		return nil
	}

	target, err := currentExecutablePath(deps)
	if err != nil {
		return err
	}
	checksums, err := fetchReleaseChecksums(ctx, deps)
	if err != nil {
		return err
	}
	want, ok := checksums[artifact]
	if !ok {
		return fmt.Errorf("checksum for %s not found in %s", artifact, releaseURL(deps.baseURL, "checksums.txt"))
	}

	tmpPath, err := downloadUpdateBinary(ctx, deps, latestURL, target, want)
	if err != nil {
		return err
	}
	installed := false
	defer func() {
		if !installed {
			_ = os.Remove(tmpPath)
		}
	}()

	if err := deps.replaceExecutable(tmpPath, target); err != nil {
		return fmt.Errorf("replace %s: %w; rerun with sufficient permissions or install drive9 into a user-writable directory", target, err)
	}
	installed = true

	_, _ = fmt.Fprintf(deps.stdout, "drive9 updated: %s -> %s\n", displayVersion(deps.currentVersion), latestVersion)
	return nil
}

func updateUsage() string {
	return `usage: drive9 update [--check] [--force] [--base-url URL]
  --check          check for a newer release without installing
  --force          reinstall latest release even when versions match
  --base-url URL   release base URL (default https://drive9.ai)`
}

// MaybeNotifyUpdate prints a cached update reminder, then refreshes the cache
// when the previous check is stale. Refresh results are intentionally silent so
// a newly discovered version is reported on the next CLI invocation.
func MaybeNotifyUpdate(currentVersion string) {
	maybeNotifyUpdateWithDeps(fillUpdateDeps(updateDeps{currentVersion: currentVersion}))
}

func maybeNotifyUpdateWithDeps(deps updateDeps) {
	deps = fillUpdateDeps(deps)
	if updateCheckDisabled() {
		return
	}
	if updateCachePath() == "" {
		return
	}

	cache, _ := readUpdateCache()
	if shouldShowUpdateNotice(deps.currentVersion, cache.LatestVersion) {
		_, _ = fmt.Fprintf(deps.stderr, "\ndrive9 update available: %s -> %s\nRun `drive9 update` to install the latest binary.\n", displayVersion(deps.currentVersion), cache.LatestVersion)
	}
	if !updateCheckDue(cache, deps.now()) {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), autoUpdateTimeout)
	defer cancel()
	latestVersion, err := fetchLatestVersion(ctx, deps)
	if err != nil {
		cache.LastCheckedAt = deps.now().UTC()
		_ = writeUpdateCache(cache)
		return
	}
	artifact := updateArtifactName(deps.goos, deps.goarch)
	_ = writeUpdateCache(updateCache{
		LastCheckedAt: deps.now().UTC(),
		LatestVersion: latestVersion,
		LatestURL:     releaseURL(deps.baseURL, artifact),
	})
}

func fillUpdateDeps(deps updateDeps) updateDeps {
	if deps.baseURL == "" {
		deps.baseURL = strings.TrimSpace(os.Getenv("DRIVE9_UPDATE_BASE_URL"))
	}
	if deps.baseURL == "" {
		deps.baseURL = defaultUpdateBaseURL
	}
	deps.baseURL = normalizeUpdateBaseURL(deps.baseURL)
	if deps.currentVersion == "" {
		deps.currentVersion = buildinfo.Version
	}
	if deps.goos == "" {
		deps.goos = runtime.GOOS
	}
	if deps.goarch == "" {
		deps.goarch = runtime.GOARCH
	}
	if deps.now == nil {
		deps.now = time.Now
	}
	if deps.executable == nil {
		deps.executable = os.Executable
	}
	if deps.httpClient == nil {
		deps.httpClient = &http.Client{Timeout: updateCommandTimeout}
	}
	if deps.stdout == nil {
		deps.stdout = os.Stdout
	}
	if deps.stderr == nil {
		deps.stderr = os.Stderr
	}
	if deps.replaceExecutable == nil {
		deps.replaceExecutable = replaceExecutableFile
	}
	return deps
}

func updateCheckDisabled() bool {
	if isTruthy(os.Getenv("DRIVE9_NO_UPDATE_CHECK")) {
		return true
	}
	if isFalsey(os.Getenv("DRIVE9_UPDATE_CHECK")) {
		return true
	}
	return false
}

func isTruthy(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func isFalsey(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "0", "false", "no", "off":
		return true
	default:
		return false
	}
}

func updateCheckDue(cache updateCache, now time.Time) bool {
	if cache.LastCheckedAt.IsZero() {
		return true
	}
	if now.Before(cache.LastCheckedAt) {
		return true
	}
	return now.Sub(cache.LastCheckedAt) >= updateCheckInterval
}

func shouldShowUpdateNotice(currentVersion, latestVersion string) bool {
	if isDevVersion(currentVersion) {
		return false
	}
	return versionsDiffer(currentVersion, latestVersion)
}

func isDevVersion(version string) bool {
	switch normalizeVersion(version) {
	case "", "dev", "unknown":
		return true
	default:
		return false
	}
}

func versionsDiffer(currentVersion, latestVersion string) bool {
	current := normalizeVersion(currentVersion)
	latest := normalizeVersion(latestVersion)
	if current == "" || latest == "" {
		return false
	}
	return current != latest
}

func normalizeVersion(version string) string {
	version = strings.TrimSpace(version)
	version = strings.TrimPrefix(version, "v")
	return version
}

func displayVersion(version string) string {
	version = strings.TrimSpace(version)
	if version == "" {
		return "unknown"
	}
	return version
}

func fetchLatestVersion(ctx context.Context, deps updateDeps) (string, error) {
	body, err := httpGetBytes(ctx, deps.httpClient, releaseURL(deps.baseURL, "version"), 4096)
	if err != nil {
		return "", fmt.Errorf("fetch latest drive9 version: %w", err)
	}
	version := strings.TrimSpace(string(body))
	if version == "" {
		return "", errors.New("fetch latest drive9 version: empty response")
	}
	return version, nil
}

func fetchReleaseChecksums(ctx context.Context, deps updateDeps) (map[string]string, error) {
	body, err := httpGetBytes(ctx, deps.httpClient, releaseURL(deps.baseURL, "checksums.txt"), 1<<20)
	if err != nil {
		return nil, fmt.Errorf("fetch release checksums: %w", err)
	}
	checksums, err := parseChecksums(body)
	if err != nil {
		return nil, err
	}
	if len(checksums) == 0 {
		return nil, errors.New("fetch release checksums: empty checksum list")
	}
	return checksums, nil
}

func httpGetBytes(ctx context.Context, client *http.Client, url string, limit int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("GET %s: HTTP %d", url, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("GET %s: response too large", url)
	}
	return body, nil
}

func parseChecksums(data []byte) (map[string]string, error) {
	checksums := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return nil, fmt.Errorf("invalid checksum line %q", line)
		}
		sum := strings.ToLower(fields[0])
		if len(sum) != sha256.Size*2 {
			return nil, fmt.Errorf("invalid checksum for %s", fields[1])
		}
		if _, err := hex.DecodeString(sum); err != nil {
			return nil, fmt.Errorf("invalid checksum for %s: %w", fields[1], err)
		}
		name := strings.TrimPrefix(fields[1], "*")
		checksums[name] = sum
		checksums[filepath.Base(name)] = sum
	}
	return checksums, nil
}

func downloadUpdateBinary(ctx context.Context, deps updateDeps, url, targetPath, wantSHA string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := deps.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("download %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", fmt.Errorf("download %s: HTTP %d", url, resp.StatusCode)
	}

	targetInfo, err := os.Stat(targetPath)
	if err != nil {
		return "", fmt.Errorf("stat current executable %s: %w", targetPath, err)
	}
	mode := targetInfo.Mode().Perm()
	if mode == 0 {
		mode = 0o755
	}

	tmpFile, err := os.CreateTemp(filepath.Dir(targetPath), ".drive9-update-*")
	if err != nil {
		return "", fmt.Errorf("create update temp file next to %s: %w", targetPath, err)
	}
	tmpPath := tmpFile.Name()
	keep := false
	defer func() {
		_ = tmpFile.Close()
		if !keep {
			_ = os.Remove(tmpPath)
		}
	}()

	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmpFile, hash), io.LimitReader(resp.Body, maxUpdateFileBytes+1))
	if err != nil {
		return "", fmt.Errorf("write update temp file: %w", err)
	}
	if n > maxUpdateFileBytes {
		return "", fmt.Errorf("download %s: response too large", url)
	}
	gotSHA := hex.EncodeToString(hash.Sum(nil))
	if gotSHA != strings.ToLower(wantSHA) {
		return "", fmt.Errorf("checksum mismatch for %s: got %s, want %s", updateArtifactName(deps.goos, deps.goarch), gotSHA, wantSHA)
	}
	if err := tmpFile.Chmod(mode); err != nil {
		return "", fmt.Errorf("chmod update temp file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return "", fmt.Errorf("close update temp file: %w", err)
	}
	keep = true
	return tmpPath, nil
}

func currentExecutablePath(deps updateDeps) (string, error) {
	path, err := deps.executable()
	if err != nil {
		return "", fmt.Errorf("locate current executable: %w", err)
	}
	if path == "" {
		return "", errors.New("locate current executable: empty path")
	}
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		path = resolved
	}
	if abs, err := filepath.Abs(path); err == nil {
		path = abs
	}
	return path, nil
}

func updateArtifactName(goos, goarch string) string {
	name := "drive9-" + goos + "-" + goarch
	if goos == "windows" {
		name += ".exe"
	}
	return name
}

func normalizeUpdateBaseURL(baseURL string) string {
	return strings.TrimRight(strings.TrimSpace(baseURL), "/")
}

func releaseURL(baseURL, name string) string {
	return normalizeUpdateBaseURL(baseURL) + "/releases/" + strings.TrimLeft(name, "/")
}

func updateCachePath() string {
	dir := configDir()
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, "update-check.json")
}

func readUpdateCache() (updateCache, error) {
	path := updateCachePath()
	if path == "" {
		return updateCache{}, errors.New("cannot determine update cache path")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return updateCache{}, err
	}
	var cache updateCache
	if err := json.Unmarshal(data, &cache); err != nil {
		return updateCache{}, err
	}
	return cache, nil
}

func writeUpdateCache(cache updateCache) error {
	path := updateCachePath()
	if path == "" {
		return errors.New("cannot determine update cache path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o600)
}
