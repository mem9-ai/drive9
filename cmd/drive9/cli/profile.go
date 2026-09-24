package cli

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	defaultMountProfile           = "coding-agent"
	codingAgentExtentMountProfile = "coding-agent-extent"
	noneMountProfile              = "none"
	extentMountProfile            = "extent"
	portableMountProfile          = "portable"
)

type profileConfig struct {
	Name               string
	Source             string
	LocalOnlyPatterns  []string
	RemoteOnlyPatterns []string
	AppendLogPatterns  []string
	PackPaths          []string
	ExtentPatterns     []string
	// LocalOnlyGitignoreAware gates whether a local-only pattern overlays a
	// path unconditionally (false) or only when the repository's Git ignore
	// rules also ignore it (true). nil means unset, which defaults to true.
	LocalOnlyGitignoreAware *bool
}

// localOnlyGitignoreAwareKey is the profile setting name, also the suffix of
// the --local-only-gitignore-aware CLI flag.
const localOnlyGitignoreAwareKey = "local-only-gitignore-aware"

func Profile(args []string) error {
	if len(args) == 0 {
		return profileUsage()
	}
	switch args[0] {
	case "show":
		return profileShow(args[1:])
	default:
		return profileUsage()
	}
}

func profileUsage() error {
	fmt.Fprintln(os.Stderr, "usage: drive9 profile show [profile]")
	return fmt.Errorf("unknown profile command")
}

func profileShow(args []string) error {
	fs := flag.NewFlagSet("profile show", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: drive9 profile show [profile]")
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 1 {
		fs.Usage()
		return fmt.Errorf("usage: drive9 profile show [profile]")
	}
	name := ""
	if fs.NArg() == 1 {
		name = fs.Arg(0)
	}
	cfg, err := loadProfileConfig(name)
	if err != nil {
		return err
	}
	fmt.Print(formatProfileConfig(cfg))
	return nil
}

func loadProfileConfig(name string) (profileConfig, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		name = defaultMountProfile
	}
	if err := validateProfileName(name); err != nil {
		return profileConfig{}, err
	}
	if name == noneMountProfile {
		return builtinNoneProfile(), nil
	}
	if name == extentMountProfile {
		return builtinExtentProfile(), nil
	}
	if name == "interactive" {
		return profileConfig{Name: "interactive", Source: "builtin:interactive"}, nil
	}
	if path := profileConfigPath(name); path != "" {
		if data, err := os.ReadFile(path); err == nil {
			cfg, err := parseProfileConfig(name, path, string(data))
			if err != nil {
				return profileConfig{}, err
			}
			return cfg, nil
		} else if !os.IsNotExist(err) {
			return profileConfig{}, fmt.Errorf("read profile %q: %w", name, err)
		}
	}
	if name == defaultMountProfile {
		return builtinCodingAgentProfile(), nil
	}
	if name == codingAgentExtentMountProfile {
		return builtinCodingAgentExtentProfile(), nil
	}
	if name == portableMountProfile {
		return builtinPortableProfile(), nil
	}
	return profileConfig{}, fmt.Errorf("drive9 profile %q not found in %s", name, profileConfigDir())
}

func profileConfigDir() string {
	dir := configDir()
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, "profiles")
}

func profileConfigPath(name string) string {
	dir := profileConfigDir()
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, name)
}

func validateProfileName(name string) error {
	if name == "" {
		return fmt.Errorf("drive9 profile: empty profile name")
	}
	if name == "." || name == ".." || strings.ContainsAny(name, `/\`) {
		return fmt.Errorf("drive9 profile: invalid profile name %q", name)
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '_' || r == '.':
		default:
			return fmt.Errorf("drive9 profile: invalid profile name %q", name)
		}
	}
	return nil
}

func builtinNoneProfile() profileConfig {
	return profileConfig{Name: noneMountProfile, Source: "builtin:none"}
}

func builtinExtentProfile() profileConfig {
	return profileConfig{
		Name:           extentMountProfile,
		Source:         "builtin:extent",
		ExtentPatterns: []string{"*"},
	}
}

func builtinCodingAgentProfile() profileConfig {
	return profileConfig{
		Name:               defaultMountProfile,
		Source:             "builtin:coding-agent",
		LocalOnlyPatterns:  builtinCodingAgentLocalOnlyPatterns(),
		RemoteOnlyPatterns: nil,
		AppendLogPatterns:  []string{"**/*-wal"},
		PackPaths:          nil,
	}
}

func builtinPortableProfile() profileConfig {
	return profileConfig{
		Name:               portableMountProfile,
		Source:             "builtin:portable",
		LocalOnlyPatterns:  builtinCodingAgentLocalOnlyPatterns(),
		RemoteOnlyPatterns: nil,
		PackPaths:          []string{"/"},
	}
}

func builtinCodingAgentExtentProfile() profileConfig {
	cfg := builtinCodingAgentProfile()
	cfg.Name = codingAgentExtentMountProfile
	cfg.Source = "builtin:coding-agent-extent"
	cfg.ExtentPatterns = []string{"*"}
	return cfg
}

func mergeProfileValues(groups ...[]string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, group := range groups {
		for _, value := range group {
			value = strings.TrimSpace(value)
			if value == "" {
				continue
			}
			if _, ok := seen[value]; ok {
				continue
			}
			seen[value] = struct{}{}
			out = append(out, value)
		}
	}
	return out
}

// builtinCodingAgentLocalOnlyPatterns is the shared local-only overlay policy
// for the coding-agent, coding-agent-extent, and portable profiles.
//
// By default the gitignore-aware gate (see the profile-level
// local-only-gitignore-aware setting) requires each matched path to also be
// ignored by the repository. Other build/cache output stays remote-persistent;
// VCS metadata is absent because `.git` is kept local only inside a Git
// workspace and is remote-persistent elsewhere.
func builtinCodingAgentLocalOnlyPatterns() []string {
	return []string{
		"**/node_modules/**",
		"**/target/**",
	}
}

func parseProfileConfig(name, source, body string) (profileConfig, error) {
	cfg := profileConfig{Name: name, Source: source}
	section := "local"
	for lineNo, raw := range strings.Split(body, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.ToLower(strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(line, "["), "]")))
			switch section {
			case "local", "remote", "pack", "append-log", "extent":
			default:
				return profileConfig{}, fmt.Errorf("profile %q line %d: unknown section [%s]", name, lineNo+1, section)
			}
			continue
		}
		// Top-of-file settings: `key = value`. Only known keys are accepted;
		// anything else is treated as a pattern for the current section.
		if key, value, ok := splitProfileSetting(line); ok && key == localOnlyGitignoreAwareKey {
			if section != "local" {
				return profileConfig{}, fmt.Errorf("profile %q line %d: %s must appear before the first section", name, lineNo+1, localOnlyGitignoreAwareKey)
			}
			parsed, err := parseProfileBool(value)
			if err != nil {
				return profileConfig{}, fmt.Errorf("profile %q line %d: %s: %w", name, lineNo+1, localOnlyGitignoreAwareKey, err)
			}
			cfg.LocalOnlyGitignoreAware = &parsed
			continue
		}
		switch section {
		case "local":
			cfg.LocalOnlyPatterns = append(cfg.LocalOnlyPatterns, line)
		case "remote":
			cfg.RemoteOnlyPatterns = append(cfg.RemoteOnlyPatterns, line)
		case "pack":
			cfg.PackPaths = append(cfg.PackPaths, line)
		case "append-log":
			cfg.AppendLogPatterns = append(cfg.AppendLogPatterns, line)
		case "extent":
			cfg.ExtentPatterns = append(cfg.ExtentPatterns, line)
		}
	}
	return cfg, nil
}

func formatProfileConfig(cfg profileConfig) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# drive9 profile: %s\n", cfg.Name)
	if cfg.Source != "" {
		fmt.Fprintf(&b, "# source: %s\n", cfg.Source)
	}
	// Emit the setting only when explicitly set so builtins round-trip and the
	// default (true) is not restated for every profile.
	if cfg.LocalOnlyGitignoreAware != nil {
		fmt.Fprintf(&b, "%s = %t\n", localOnlyGitignoreAwareKey, *cfg.LocalOnlyGitignoreAware)
	}
	writeProfileSection(&b, "local", cfg.LocalOnlyPatterns, "no local-only overlay paths")
	writeProfileSection(&b, "remote", cfg.RemoteOnlyPatterns, "no remote override paths")
	writeProfileSection(&b, "pack", cfg.PackPaths, "no automatic pack paths")
	writeProfileSection(&b, "append-log", cfg.AppendLogPatterns, "no append-log optimization paths")
	writeProfileSection(&b, "extent", cfg.ExtentPatterns, "no extent path patterns")
	return b.String()
}

// splitProfileSetting splits a `key = value` line. It reports ok only when the
// line has the exact shape (a non-empty key and a non-empty value around a
// single '='), so that a path pattern containing '=' is left to the pattern
// sections.
func splitProfileSetting(line string) (key, value string, ok bool) {
	eq := strings.IndexByte(line, '=')
	if eq < 0 {
		return "", "", false
	}
	key = strings.ToLower(strings.TrimSpace(line[:eq]))
	value = strings.TrimSpace(line[eq+1:])
	if key == "" || value == "" {
		return "", "", false
	}
	return key, value, true
}

func parseProfileBool(value string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "true", "1", "yes", "on":
		return true, nil
	case "false", "0", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean %q", value)
	}
}

func writeProfileSection(b *strings.Builder, name string, values []string, emptyComment string) {
	fmt.Fprintf(b, "\n[%s]\n", name)
	if len(values) == 0 {
		fmt.Fprintf(b, "# %s\n", emptyComment)
		return
	}
	values = append([]string(nil), values...)
	sort.Strings(values)
	for _, value := range values {
		fmt.Fprintln(b, value)
	}
}
