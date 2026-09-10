// Package telemetry collects privacy-preserving CLI command completion events.
//
// Telemetry is opt-in: it stays disabled unless the user enables it with
// DRIVE9_TELEMETRY=on or the "telemetry" section of the CLI config file
// (~/.drive9/config). Events are posted to the product-owned ingestion service
// shared with ti-cli and stored in the shared telemetry TiDB cluster. The
// package never captures flag values, credentials, paths, file contents,
// command output, or resource IDs.
package telemetry

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"
)

const (
	EnvironmentVariable  = "DRIVE9_TELEMETRY"
	dirName              = ".drive9"
	installationIDFile   = ".telemetry-installation-id"
	eventName            = "drive9.command.finished"
	schemaVersion        = 2
	installationIDPrefix = "drive9_"
	maxFlagNames         = 64
	maxCommandPathBytes  = 128
	// deliveryTimeout bounds the whole best-effort POST. Delivery happens on the
	// command's exit path, so this is also the worst-case latency added to an
	// opted-in command: dropping one event is preferable to a visibly slow CLI.
	deliveryTimeout = 1 * time.Second
)

// These patterns mirror the ingestion service contract. A payload that fails
// any of them is rejected as a whole, so the client drops such an event (or
// field) locally instead of spending a request on a guaranteed rejection.
var (
	installationIDPattern = regexp.MustCompile(`^drive9_[A-Za-z0-9_-]{22}$`)
	eventIDPattern        = regexp.MustCompile(`^[A-Za-z0-9_-]{16,64}$`)
	commandPathPattern    = regexp.MustCompile(`^drive9(?: [a-z][a-z0-9-]{0,63}){0,3}$`)
	flagNamePattern       = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)
	cliVersionPattern     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9.+_-]{0,63}$`)
)

var allowedRegions = map[string]struct{}{
	"":                        {},
	"unknown":                 {},
	"aws-us-east-1":           {},
	"aws-us-west-2":           {},
	"aws-eu-central-1":        {},
	"aws-ap-northeast-1":      {},
	"aws-ap-southeast-1":      {},
	"ali-ap-southeast-1":      {},
	"alicloud-ap-southeast-1": {},
}

// allowedOperatingSystems and allowedArchitectures mirror the ingestion
// service allowlists. A build outside them sends no events.
var allowedOperatingSystems = map[string]struct{}{
	"aix": {}, "android": {}, "darwin": {}, "dragonfly": {}, "freebsd": {},
	"illumos": {}, "ios": {}, "js": {}, "linux": {}, "netbsd": {},
	"openbsd": {}, "plan9": {}, "solaris": {}, "wasip1": {}, "windows": {},
}

var allowedArchitectures = map[string]struct{}{
	"386": {}, "amd64": {}, "arm": {}, "arm64": {}, "loong64": {},
	"mips": {}, "mips64": {}, "mips64le": {}, "mipsle": {}, "ppc64": {},
	"ppc64le": {}, "riscv64": {}, "s390x": {}, "wasm": {},
}

// Config controls whether a process starts a telemetry session.
type Config struct {
	Eligible      bool
	HomeDir       string
	Endpoint      string
	Version       string
	OS            string
	Arch          string
	InstallSource string
	Environment   map[string]string
	// Preference reports the persisted user opt-in (for the CLI: the
	// "telemetry" section of ~/.drive9/config). recorded is false when the user
	// never expressed a preference, and also when the preference cannot be read
	// or parsed: an unreadable preference must never enable telemetry.
	Preference  func() (enabled bool, recorded bool)
	Client      *http.Client
	Debug       bool
	DebugWriter io.Writer
	Now         func() time.Time
}

// EventInput is the allowlisted completion payload for one command invocation.
type EventInput struct {
	CommandPath   string
	FlagNames     []string
	ExitCode      int
	Duration      time.Duration
	CloudProvider string
	RegionCode    string
	ProfileSource string
}

// Session is a started telemetry identity used to send one completion event.
type Session struct {
	installationID string
	endpoint       string
	version        string
	os             string
	arch           string
	installSource  string
	client         *http.Client
	debug          bool
	debugWriter    io.Writer
	now            func() time.Time
}

type batchRequest struct {
	SchemaVersion int         `json:"schema_version"`
	SentAt        string      `json:"sent_at"`
	Events        []wireEvent `json:"events"`
}

type wireEvent struct {
	EventID                 string   `json:"event_id"`
	EventName               string   `json:"event_name"`
	OccurredAt              string   `json:"occurred_at"`
	AnonymousInstallationID string   `json:"anonymous_installation_id"`
	CommandPath             string   `json:"command_path"`
	FlagNames               []string `json:"flag_names"`
	ExitCode                int      `json:"exit_code"`
	DurationMS              int64    `json:"duration_ms"`
	CloudProvider           string   `json:"cloud_provider"`
	RegionCode              string   `json:"region_code"`
	CLIVersion              string   `json:"cli_version"`
	OS                      string   `json:"os"`
	Arch                    string   `json:"arch"`
	InstallSource           string   `json:"install_source"`
	ProfileSource           string   `json:"profile_source"`
}

// InstallationIDPath is the machine-generated pseudonymous identity file.
func InstallationIDPath(homeDir string) string {
	return filepath.Join(homeDir, dirName, installationIDFile)
}

// Start resolves enablement and loads or creates the installation ID.
// It returns nil when telemetry must stay disabled.
func Start(cfg Config) *Session {
	if !cfg.Eligible || strings.TrimSpace(cfg.Endpoint) == "" {
		return nil
	}
	if !resolveEnabled(cfg) {
		return nil
	}
	homeDir := strings.TrimSpace(cfg.HomeDir)
	if homeDir == "" {
		var err error
		homeDir, err = os.UserHomeDir()
		if err != nil {
			debug(cfg.Debug, cfg.DebugWriter, "telemetry disabled because its local identity is unavailable")
			return nil
		}
	}
	id, err := loadOrCreateInstallationID(homeDir)
	if err != nil {
		debug(cfg.Debug, cfg.DebugWriter, "telemetry disabled because its local identity is unavailable")
		return nil
	}
	client := cfg.Client
	if client == nil {
		client = &http.Client{
			Timeout: deliveryTimeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Session{
		installationID: id,
		endpoint:       cfg.Endpoint,
		version:        cfg.Version,
		os:             cfg.OS,
		arch:           cfg.Arch,
		installSource:  cfg.InstallSource,
		client:         client,
		debug:          cfg.Debug,
		debugWriter:    cfg.DebugWriter,
		now:            now,
	}
}

// Finish posts one allowlisted completion event. Delivery is best-effort and
// must not change command output or exit status.
func (s *Session) Finish(input EventInput) {
	if s == nil {
		return
	}
	commandPath := input.CommandPath
	if commandPath == "" || len(commandPath) > maxCommandPathBytes || !commandPathPattern.MatchString(commandPath) {
		debug(s.debug, s.debugWriter, "telemetry event was dropped because its command path is unsupported")
		return
	}
	osName := runtimeValue(s.os, runtime.GOOS)
	if _, ok := allowedOperatingSystems[osName]; !ok {
		debug(s.debug, s.debugWriter, "telemetry event was dropped because its operating system is unsupported")
		return
	}
	archName := runtimeValue(s.arch, runtime.GOARCH)
	if _, ok := allowedArchitectures[archName]; !ok {
		debug(s.debug, s.debugWriter, "telemetry event was dropped because its architecture is unsupported")
		return
	}
	now := s.now().UTC()
	eventID, err := randomIdentifier("")
	if err != nil || !eventIDPattern.MatchString(eventID) {
		debug(s.debug, s.debugWriter, "telemetry event was dropped before delivery")
		return
	}
	durationMS := input.Duration.Milliseconds()
	if durationMS < 0 {
		durationMS = 0
	}
	if durationMS > 86_400_000 {
		durationMS = 86_400_000
	}
	exitCode := input.ExitCode
	if exitCode < 0 {
		exitCode = 0
	}
	if exitCode > 255 {
		exitCode = 255
	}
	event := wireEvent{
		EventID:                 eventID,
		EventName:               eventName,
		OccurredAt:              now.Format(time.RFC3339Nano),
		AnonymousInstallationID: s.installationID,
		CommandPath:             commandPath,
		FlagNames:               normalizedFlagNames(input.FlagNames),
		ExitCode:                exitCode,
		DurationMS:              durationMS,
		CloudProvider:           normalizedProvider(input.CloudProvider),
		RegionCode:              normalizedRegion(input.CloudProvider, input.RegionCode),
		CLIVersion:              normalizedVersion(s.version),
		OS:                      osName,
		Arch:                    archName,
		InstallSource:           normalizedInstallSource(s.installSource),
		ProfileSource:           normalizedProfileSource(input.ProfileSource),
	}
	payload, err := json.Marshal(batchRequest{
		SchemaVersion: schemaVersion,
		SentAt:        now.Format(time.RFC3339Nano),
		Events:        []wireEvent{event},
	})
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), deliveryTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(payload))
	if err != nil {
		debug(s.debug, s.debugWriter, "telemetry event was dropped before delivery")
		return
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "drive9/"+normalizedVersion(s.version))
	response, err := s.client.Do(request)
	if err != nil {
		debug(s.debug, s.debugWriter, "telemetry delivery failed; the command result was not affected")
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	_ = response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		debug(s.debug, s.debugWriter, "telemetry delivery was rejected; the command result was not affected")
	}
}

// normalizedFlagNames keeps only names the ingestion service accepts, so one
// malformed entry cannot cost the whole event.
func normalizedFlagNames(names []string) []string {
	out := make([]string, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if !flagNamePattern.MatchString(name) {
			continue
		}
		if _, duplicate := seen[name]; duplicate {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
		if len(out) == maxFlagNames {
			break
		}
	}
	sort.Strings(out)
	return out
}

func resolveEnabled(cfg Config) bool {
	raw, exists := envValue(cfg.Environment, EnvironmentVariable)
	if exists && strings.TrimSpace(raw) != "" {
		enabled, valid := parseOverride(raw)
		if !valid {
			return false
		}
		return enabled
	}
	if cfg.Preference != nil {
		if enabled, recorded := cfg.Preference(); recorded {
			return enabled
		}
	}
	// Disabled by default: no build, install source, or environment heuristic
	// may enable telemetry on the user's behalf.
	return false
}

// loadOrCreateInstallationID publishes a fresh pseudonymous identity when none
// exists yet. Concurrent processes converge on the published value.
func loadOrCreateInstallationID(homeDir string) (string, error) {
	path := InstallationIDPath(homeDir)
	id, exists, err := readInstallationID(path)
	if err != nil || exists {
		return id, err
	}
	id, err = randomIdentifier(installationIDPrefix)
	if err != nil {
		return "", err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	temp, err := os.CreateTemp(dir, ".telemetry-installation-id.tmp-*")
	if err != nil {
		return "", err
	}
	tempPath := temp.Name()
	defer func() { _ = os.Remove(tempPath) }()
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return "", err
	}
	if _, err := temp.WriteString(id + "\n"); err != nil {
		_ = temp.Close()
		return "", err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return "", err
	}
	if err := temp.Close(); err != nil {
		return "", err
	}
	// Link publishes without clobbering an identity another process just wrote.
	if err := os.Link(tempPath, path); err != nil && !errors.Is(err, os.ErrExist) {
		// Fallback for filesystems without hard links (FAT/exFAT, some SMB/NFS
		// home directories). Claim the path exclusively first: renaming straight
		// over it would let two concurrent processes replace each other's
		// identity and then report events under different IDs. Replacing our own
		// placeholder afterwards keeps the content publish atomic.
		claimed, claimErr := claimInstallationIDPath(path)
		if claimErr != nil {
			return "", err
		}
		if claimed {
			if renameErr := os.Rename(tempPath, path); renameErr != nil {
				return "", renameErr
			}
		}
	}
	// Read back instead of trusting the local value: the loser of a race must
	// adopt the published identity, and a path that exists but yields nothing
	// must fail closed rather than send an empty installation ID.
	persisted, exists, err := readInstallationID(path)
	if err != nil {
		return "", err
	}
	if !exists {
		return "", fmt.Errorf("telemetry installation ID could not be persisted")
	}
	return persisted, nil
}

// claimInstallationIDPath creates path only when nothing is there yet. It is
// how an identity is published on filesystems without hard links: the loser of
// a race leaves the winner's file untouched instead of replacing it.
func claimInstallationIDPath(path string) (bool, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if errors.Is(err, os.ErrExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := file.Close(); err != nil {
		return false, err
	}
	return true, nil
}

func readInstallationID(path string) (string, bool, error) {
	// Lstat, not Stat: a symlink at this path is not an identity we created.
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", true, err
	}
	if !info.Mode().IsRegular() {
		return "", true, fmt.Errorf("invalid telemetry installation ID file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", true, err
	}
	id := strings.TrimSuffix(string(data), "\n")
	if strings.Contains(id, "\n") || !installationIDPattern.MatchString(id) {
		return "", true, fmt.Errorf("invalid telemetry installation ID")
	}
	return id, true, nil
}

func randomIdentifier(prefix string) (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return prefix + base64.RawURLEncoding.EncodeToString(value[:]), nil
}

func envValue(env map[string]string, key string) (string, bool) {
	if env != nil {
		value, ok := env[key]
		return value, ok
	}
	return os.LookupEnv(key)
}

func parseOverride(value string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "on", "true", "yes", "1":
		return true, true
	case "off", "false", "no", "0":
		return false, true
	default:
		return false, false
	}
}

func normalizedInstallSource(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "archive", "github-release":
		return "github-release"
	case "homebrew", "scoop", "source", "dev", "unknown":
		return strings.ToLower(strings.TrimSpace(value))
	case "local":
		return "dev"
	default:
		return "unknown"
	}
}

func normalizedProvider(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "aws":
		return "aws"
	case "alibaba_cloud", "alicloud", "alibaba", "ali":
		return "alibaba_cloud"
	case "":
		return ""
	default:
		return "unknown"
	}
}

func normalizedRegion(provider, value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if _, ok := allowedRegions[value]; ok {
		return value
	}
	composed := value
	switch normalizedProvider(provider) {
	case "aws":
		if !strings.HasPrefix(value, "aws-") {
			composed = "aws-" + value
		}
	case "alibaba_cloud":
		if !strings.HasPrefix(value, "ali-") && !strings.HasPrefix(value, "alicloud-") {
			composed = "ali-" + value
		}
	}
	if _, ok := allowedRegions[composed]; ok {
		return composed
	}
	return "unknown"
}

func normalizedVersion(value string) string {
	value = strings.TrimPrefix(strings.TrimSpace(value), "v")
	if !cliVersionPattern.MatchString(value) {
		return "unknown"
	}
	return value
}

func normalizedProfileSource(value string) string {
	switch value {
	case "default", "explicit", "env":
		return value
	default:
		return "unknown"
	}
}

func runtimeValue(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func debug(enabled bool, writer io.Writer, message string) {
	if enabled && writer != nil {
		_, _ = fmt.Fprintln(writer, "drive9 [DEBUG]: "+message)
	}
}
