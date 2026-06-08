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
	defaultMountProfile = "coding-agent"
	noneMountProfile    = "none"
)

type profileConfig struct {
	Name               string
	Source             string
	LocalOnlyPatterns  []string
	RemoteOnlyPatterns []string
	PackPaths          []string
}

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

func builtinCodingAgentProfile() profileConfig {
	return profileConfig{
		Name:               defaultMountProfile,
		Source:             "builtin:coding-agent",
		LocalOnlyPatterns:  builtinCodingAgentLocalOnlyPatterns(),
		RemoteOnlyPatterns: nil,
		PackPaths:          nil,
	}
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

func builtinCodingAgentLocalOnlyPatterns() []string {
	return []string{
		"**/.git/**",
		"**/.hg/**",
		"**/.svn/**",
		"**/node_modules/**",
		"**/.pnpm-store/**",
		"**/target/**",
		"**/dist/**",
		"**/build/**",
		"**/coverage/**",
		"**/tmp/**",
		"**/.tmp/**",
		"**/.tmp-api-extractor/**",
		"**/.cache/**",
		"**/.turbo/**",
		"**/.next/cache/**",
		"**/.vitepress/cache/**",
		"**/.gradle/**",
		"**/.venv/**",
		"**/__pycache__/**",
		"**/.pytest_cache/**",
		"**/.mypy_cache/**",
		"**/.ruff_cache/**",
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
			case "local", "remote", "pack":
			default:
				return profileConfig{}, fmt.Errorf("profile %q line %d: unknown section [%s]", name, lineNo+1, section)
			}
			continue
		}
		switch section {
		case "local":
			cfg.LocalOnlyPatterns = append(cfg.LocalOnlyPatterns, line)
		case "remote":
			cfg.RemoteOnlyPatterns = append(cfg.RemoteOnlyPatterns, line)
		case "pack":
			cfg.PackPaths = append(cfg.PackPaths, line)
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
	writeProfileSection(&b, "local", cfg.LocalOnlyPatterns, "no local-only overlay paths")
	writeProfileSection(&b, "remote", cfg.RemoteOnlyPatterns, "no remote override paths")
	writeProfileSection(&b, "pack", cfg.PackPaths, "no automatic pack paths")
	return b.String()
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
