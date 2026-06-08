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
}

func TestLoadProfileConfigNoneHasNoOverlayOrPackPaths(t *testing.T) {
	cfg, err := loadProfileConfig("none")
	if err != nil {
		t.Fatalf("loadProfileConfig: %v", err)
	}
	if cfg.Name != "none" {
		t.Fatalf("Name = %q, want none", cfg.Name)
	}
	if len(cfg.LocalOnlyPatterns) != 0 || len(cfg.RemoteOnlyPatterns) != 0 || len(cfg.PackPaths) != 0 {
		t.Fatalf("none profile = %#v, want empty policy and pack lists", cfg)
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
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("profile show output = %q, want %q", out, want)
		}
	}
	if strings.Contains(out, "/coding-agent/") {
		t.Fatalf("profile show output = %q, should not contain profile-scoped pack path", out)
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
