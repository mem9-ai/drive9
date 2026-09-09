package cli

import (
	"reflect"
	"strings"
	"testing"
)

func TestLoadProfileConfigDefaultCodingAgentHasNoPackPaths(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	cfg, err := loadProfileConfig("")
	if err != nil {
		t.Fatalf("loadProfileConfig: %v", err)
	}
	if cfg.Name != "coding-agent" {
		t.Fatalf("Name = %q, want coding-agent", cfg.Name)
	}
	if len(cfg.LocalOnlyPatterns) == 0 {
		t.Fatal("default coding-agent profile should include local-only overlay patterns")
	}
	if len(cfg.PackPaths) != 0 {
		t.Fatalf("PackPaths = %v, want no default pack paths", cfg.PackPaths)
	}
	if len(cfg.ExtentPatterns) != 0 {
		t.Fatalf("ExtentPatterns = %v, want empty (extent is opt-in via coding-agent-extent)", cfg.ExtentPatterns)
	}
}

func TestLoadProfileConfigNoneHasNoOverlayOrPackPaths(t *testing.T) {
	cfg, err := loadProfileConfig("none")
	if err != nil {
		t.Fatalf("loadProfileConfig: %v", err)
	}
	if cfg.Name != "none" {
		t.Fatalf("Name = %q, want none", cfg.Name)
	}
	if len(cfg.LocalOnlyPatterns) != 0 || len(cfg.RemoteOnlyPatterns) != 0 || len(cfg.PackPaths) != 0 || len(cfg.ExtentPatterns) != 0 {
		t.Fatalf("none profile = %#v, want empty policy, pack, and extent lists", cfg)
	}
}

func TestLoadProfileConfigCodingAgentExtentMatchesAllFiles(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	cfg, err := loadProfileConfig("coding-agent-extent")
	if err != nil {
		t.Fatalf("loadProfileConfig: %v", err)
	}
	if cfg.Name != "coding-agent-extent" {
		t.Fatalf("Name = %q, want coding-agent-extent", cfg.Name)
	}
	if cfg.Source != "builtin:coding-agent-extent" {
		t.Fatalf("Source = %q, want builtin:coding-agent-extent", cfg.Source)
	}
	base, err := loadProfileConfig("coding-agent")
	if err != nil {
		t.Fatalf("loadProfileConfig coding-agent: %v", err)
	}
	if !reflect.DeepEqual(cfg.LocalOnlyPatterns, base.LocalOnlyPatterns) {
		t.Fatalf("LocalOnlyPatterns = %v, want coding-agent overlay patterns", cfg.LocalOnlyPatterns)
	}
	if len(cfg.PackPaths) != 0 {
		t.Fatalf("PackPaths = %v, want none", cfg.PackPaths)
	}
	if !reflect.DeepEqual(cfg.ExtentPatterns, []string{"*"}) {
		t.Fatalf("ExtentPatterns = %v, want [*]", cfg.ExtentPatterns)
	}
}

func TestLoadProfileConfigPortablePacksLocalOverlay(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	cfg, err := loadProfileConfig("portable")
	if err != nil {
		t.Fatalf("loadProfileConfig: %v", err)
	}
	if cfg.Name != "portable" {
		t.Fatalf("Name = %q, want portable", cfg.Name)
	}
	if len(cfg.LocalOnlyPatterns) == 0 {
		t.Fatal("portable profile should include coding-agent local-only overlay patterns")
	}
	if !reflect.DeepEqual(cfg.PackPaths, []string{"/"}) {
		t.Fatalf("PackPaths = %v, want all local overlay marker", cfg.PackPaths)
	}
}

func TestLoadProfileConfigReadsHomeProfile(t *testing.T) {
	writeTestProfile(t, "custom", `
# defaults to [local] before the first explicit section
**/.scratch/**

[remote]
**/.scratch/keep/**

[pack]
.git
dist/
repo/.cache
`)

	cfg, err := loadProfileConfig("custom")
	if err != nil {
		t.Fatalf("loadProfileConfig: %v", err)
	}
	if cfg.Name != "custom" {
		t.Fatalf("Name = %q, want custom", cfg.Name)
	}
	if !reflect.DeepEqual(cfg.LocalOnlyPatterns, []string{"**/.scratch/**"}) {
		t.Fatalf("LocalOnlyPatterns = %v", cfg.LocalOnlyPatterns)
	}
	if !reflect.DeepEqual(cfg.RemoteOnlyPatterns, []string{"**/.scratch/keep/**"}) {
		t.Fatalf("RemoteOnlyPatterns = %v", cfg.RemoteOnlyPatterns)
	}
	if !reflect.DeepEqual(cfg.PackPaths, []string{".git", "dist/", "repo/.cache"}) {
		t.Fatalf("PackPaths = %v", cfg.PackPaths)
	}
}

func TestProfileShowPrintsDefaultConfig(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	out, err := captureStdoutE(t, func() error { return Profile([]string{"show"}) })
	if err != nil {
		t.Fatalf("Profile show: %v", err)
	}
	for _, want := range []string{
		"# drive9 profile: coding-agent",
		"# source: builtin:coding-agent",
		"[local]",
		"[pack]",
		"# no automatic pack paths",
		"[extent]",
		"# no extent path patterns",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("profile show output = %q, want %q", out, want)
		}
	}
	for _, dropped := range []string{"*.db", "*.sqlite", "*-wal", "*.jsonl"} {
		if strings.Contains(out, dropped) {
			t.Fatalf("profile show output = %q, must not include %q", out, dropped)
		}
	}
	if strings.Contains(out, "/coding-agent/") {
		t.Fatalf("profile show output = %q, should not contain profile-scoped pack path", out)
	}
}

func TestProfileShowPrintsCodingAgentExtentConfig(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	out, err := captureStdoutE(t, func() error { return Profile([]string{"show", "coding-agent-extent"}) })
	if err != nil {
		t.Fatalf("Profile show coding-agent-extent: %v", err)
	}
	for _, want := range []string{
		"# drive9 profile: coding-agent-extent",
		"# source: builtin:coding-agent-extent",
		"[local]",
		"[extent]",
		"*",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("profile show coding-agent-extent output = %q, want %q", out, want)
		}
	}
}

func TestProfileShowPrintsPortableConfig(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	out, err := captureStdoutE(t, func() error { return Profile([]string{"show", "portable"}) })
	if err != nil {
		t.Fatalf("Profile show portable: %v", err)
	}
	for _, want := range []string{
		"# drive9 profile: portable",
		"# source: builtin:portable",
		"[local]",
		"[pack]",
		"/",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("profile show portable output = %q, want %q", out, want)
		}
	}
}

func TestProfileShowPrintsCustomConfig(t *testing.T) {
	writeTestProfile(t, "with-pack", "[pack]\n.git\n")

	out, err := captureStdoutE(t, func() error { return Profile([]string{"show", "with-pack"}) })
	if err != nil {
		t.Fatalf("Profile show: %v", err)
	}
	for _, want := range []string{
		"# drive9 profile: with-pack",
		"[pack]",
		".git",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("profile show output = %q, want %q", out, want)
		}
	}
}
