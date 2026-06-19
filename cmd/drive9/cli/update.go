package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/buildinfo"
)

const (
	defaultUpdateBaseURL = "https://drive9.ai"
	updateCommandTimeout = 2 * time.Minute
	maxUpdateFileBytes   = 200 << 20
)

type updateDeps struct {
	baseURL              string
	currentVersion       string
	goos                 string
	goarch               string
	executable           func() (string, error)
	httpClient           *http.Client
	stdout               io.Writer
	stderr               io.Writer
	allowInsecureBaseURL bool
	preflightReplace     func() error
	replaceExecutable    func(string, string) error
}

func defaultUpdateDeps() updateDeps {
	return fillUpdateDeps(updateDeps{})
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
	if err := fs.Parse(normalizeHelpFlags(args, fs)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = fmt.Fprintln(deps.stdout, updateUsage())
			return nil
		}
		return fmt.Errorf("%w\n%s", err, updateUsage())
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected argument %q\n%s", fs.Arg(0), updateUsage())
	}
	deps.baseURL = normalizeUpdateBaseURL(*baseURL)
	if err := validateUpdateBaseURL(deps.baseURL, deps.allowInsecureBaseURL); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), updateCommandTimeout)
	defer cancel()

	latestVersion, err := fetchLatestVersion(ctx, deps)
	if err != nil {
		return err
	}
	artifact := updateArtifactName(deps.goos, deps.goarch)
	latestURL := releaseURL(deps.baseURL, artifact)
	relation := compareReleaseVersions(deps.currentVersion, latestVersion)

	if *checkOnly {
		switch relation {
		case releaseNewer:
			_, _ = fmt.Fprintf(deps.stdout, "drive9 update available: %s -> %s\n", displayVersion(deps.currentVersion), latestVersion)
		case releaseSame:
			_, _ = fmt.Fprintf(deps.stdout, "drive9 is up to date (%s)\n", displayVersion(deps.currentVersion))
		case releaseOlder:
			_, _ = fmt.Fprintf(deps.stdout, "drive9 latest release %s is older than current %s\n", latestVersion, displayVersion(deps.currentVersion))
		default:
			_, _ = fmt.Fprintf(deps.stdout, "drive9 latest release %s cannot be compared with current %s\n", latestVersion, displayVersion(deps.currentVersion))
		}
		return nil
	}
	if !*force {
		switch relation {
		case releaseNewer:
		case releaseSame:
			_, _ = fmt.Fprintf(deps.stdout, "drive9 is already up to date (%s)\n", displayVersion(deps.currentVersion))
			return nil
		case releaseOlder:
			_, _ = fmt.Fprintf(deps.stdout, "drive9 latest release %s is older than current %s; use --force to install it\n", latestVersion, displayVersion(deps.currentVersion))
			return nil
		default:
			_, _ = fmt.Fprintf(deps.stdout, "drive9 latest release %s cannot be compared with current %s; use --force to install it\n", latestVersion, displayVersion(deps.currentVersion))
			return nil
		}
	}
	if err := deps.preflightReplace(); err != nil {
		return err
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
  --base-url URL   release base URL (default https://drive9.ai)` + updatePlatformUsageNote()
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
	if deps.preflightReplace == nil {
		deps.preflightReplace = preflightReplaceExecutableFile
	}
	if deps.replaceExecutable == nil {
		deps.replaceExecutable = replaceExecutableFile
	}
	return deps
}

func normalizeVersion(version string) string {
	version = strings.TrimSpace(version)
	version = strings.TrimPrefix(version, "v")
	return version
}

type releaseRelation int

const (
	releaseUnknown releaseRelation = iota
	releaseSame
	releaseNewer
	releaseOlder
)

type semverVersion struct {
	major int
	minor int
	patch int
	pre   []string
}

func compareReleaseVersions(currentVersion, latestVersion string) releaseRelation {
	current := normalizeVersion(currentVersion)
	latest := normalizeVersion(latestVersion)
	if current == "" || latest == "" {
		return releaseUnknown
	}
	if current == latest {
		return releaseSame
	}
	currentSemver, currentOK := parseSemver(current)
	latestSemver, latestOK := parseSemver(latest)
	if latestOK && !currentOK {
		return releaseNewer
	}
	if !latestOK || !currentOK {
		return releaseUnknown
	}
	switch compareSemver(latestSemver, currentSemver) {
	case 0:
		return releaseSame
	case 1:
		return releaseNewer
	default:
		return releaseOlder
	}
}

func parseSemver(version string) (semverVersion, bool) {
	version, _, _ = strings.Cut(version, "+")
	mainPart, prePart, hasPre := strings.Cut(version, "-")
	parts := strings.Split(mainPart, ".")
	if len(parts) != 3 {
		return semverVersion{}, false
	}
	major, ok := parseSemverNumber(parts[0])
	if !ok {
		return semverVersion{}, false
	}
	minor, ok := parseSemverNumber(parts[1])
	if !ok {
		return semverVersion{}, false
	}
	patch, ok := parseSemverNumber(parts[2])
	if !ok {
		return semverVersion{}, false
	}
	var pre []string
	if hasPre {
		if prePart == "" {
			return semverVersion{}, false
		}
		pre = strings.Split(prePart, ".")
		for _, id := range pre {
			if id == "" {
				return semverVersion{}, false
			}
		}
	}
	return semverVersion{major: major, minor: minor, patch: patch, pre: pre}, true
}

func parseSemverNumber(value string) (int, bool) {
	if value == "" {
		return 0, false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return 0, false
		}
	}
	n, err := strconv.Atoi(value)
	return n, err == nil
}

func compareSemver(a, b semverVersion) int {
	for _, pair := range [][2]int{{a.major, b.major}, {a.minor, b.minor}, {a.patch, b.patch}} {
		if pair[0] > pair[1] {
			return 1
		}
		if pair[0] < pair[1] {
			return -1
		}
	}
	return compareSemverPrerelease(a.pre, b.pre)
}

func compareSemverPrerelease(a, b []string) int {
	if len(a) == 0 && len(b) == 0 {
		return 0
	}
	if len(a) == 0 {
		return 1
	}
	if len(b) == 0 {
		return -1
	}
	limit := len(a)
	if len(b) < limit {
		limit = len(b)
	}
	for i := 0; i < limit; i++ {
		cmp := compareSemverIdentifier(a[i], b[i])
		if cmp != 0 {
			return cmp
		}
	}
	if len(a) > len(b) {
		return 1
	}
	if len(a) < len(b) {
		return -1
	}
	return 0
}

func compareSemverIdentifier(a, b string) int {
	aNum, aOK := parseSemverNumber(a)
	bNum, bOK := parseSemverNumber(b)
	switch {
	case aOK && bOK:
		if aNum > bNum {
			return 1
		}
		if aNum < bNum {
			return -1
		}
		return 0
	case aOK:
		return -1
	case bOK:
		return 1
	default:
		if a > b {
			return 1
		}
		if a < b {
			return -1
		}
		return 0
	}
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
		name, err := checksumArtifactName(fields[1])
		if err != nil {
			return nil, err
		}
		if _, exists := checksums[name]; exists {
			return nil, fmt.Errorf("duplicate checksum for %s", name)
		}
		checksums[name] = sum
	}
	return checksums, nil
}

func checksumArtifactName(raw string) (string, error) {
	name := strings.TrimPrefix(raw, "*")
	if name == "" {
		return "", errors.New("empty checksum artifact name")
	}
	clean := filepath.ToSlash(filepath.Clean(name))
	if clean == "." || strings.HasPrefix(clean, "/") || strings.HasPrefix(clean, "../") || strings.Contains(clean, "/") {
		return "", fmt.Errorf("checksum artifact %q must be a release artifact filename", raw)
	}
	return clean, nil
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

func validateUpdateBaseURL(baseURL string, allowInsecure bool) error {
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("invalid release base URL %q", baseURL)
	}
	if parsed.Scheme == "https" || allowInsecure {
		return nil
	}
	return fmt.Errorf("release base URL %q must use https", baseURL)
}

func releaseURL(baseURL, name string) string {
	return normalizeUpdateBaseURL(baseURL) + "/releases/" + strings.TrimLeft(name, "/")
}
