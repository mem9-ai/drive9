package cli

import (
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestProfileAppendLogRoundTrip(t *testing.T) {
	writeTestProfile(t, "verdent", "# custom\n[local]\n**/.cache/**\n[append-log]\n# WAL files\n **/*-wal \n\n**/events.log\n[remote]\n**/.cache/keep/**\n[pack]\n.git\n")
	cfg, err := loadProfileConfig("verdent")
	if err != nil {
		t.Fatal(err)
	}
	out := formatProfileConfig(cfg)
	for _, want := range []string{"[append-log]\n**/*-wal\n**/events.log\n", "[local]\n**/.cache/**", "[remote]\n**/.cache/keep/**", "[pack]\n.git"} {
		if !strings.Contains(out, want) {
			t.Fatalf("profile output = %q, want %q", out, want)
		}
	}
	roundTrip, err := parseProfileConfig(cfg.Name, cfg.Source, out)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(roundTrip, cfg) {
		t.Fatalf("round trip = %#v, want %#v", roundTrip, cfg)
	}
}

func TestProfileAppendLogDefaults(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	for _, name := range []string{"", "coding-agent", "portable", "none", "extent", "interactive"} {
		cfg, err := loadProfileConfig(name)
		if err != nil {
			t.Fatal(err)
		}
		want := "[append-log]\n# no append-log optimization paths\n"
		if name == "" || name == "coding-agent" {
			want = "[append-log]\n**/*-wal\n"
		}
		if out := formatProfileConfig(cfg); !strings.Contains(out, want) {
			t.Fatalf("profile %q output = %q, want %q", name, out, want)
		}
	}
}

func TestProfileAppendLogCustomReplacesBuiltin(t *testing.T) {
	for _, body := range []string{"[local]\n**/scratch/**\n", "[append-log]\n**/events.log\n"} {
		writeTestProfile(t, "coding-agent", body)
		cfg, err := loadProfileConfig("coding-agent")
		if err != nil {
			t.Fatal(err)
		}
		if out := formatProfileConfig(cfg); strings.Contains(out, "**/*-wal") {
			t.Fatalf("custom profile inherited builtin append-log rule: %q", out)
		}
	}
}

func TestProfileAppendLogFormattingDoesNotMutateConfig(t *testing.T) {
	cfg, err := parseProfileConfig("custom", "test", "[append-log]\n**/z.log\n**/a.log\n")
	if err != nil {
		t.Fatal(err)
	}
	before := formatProfileConfig(cfg)
	after, err := parseProfileConfig(cfg.Name, cfg.Source, before)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(cfg, after) {
		t.Fatal("formatter mutated source rules or failed to sort output")
	}
	sort.Strings(cfg.AppendLogPatterns)
	if !reflect.DeepEqual(cfg, after) {
		t.Fatalf("formatted config = %#v, want %#v", after, cfg)
	}
}

func TestLoadProfileConfigCodingAgentExtent(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	cfg, err := loadProfileConfig("coding-agent-extent")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cfg.ExtentPatterns, []string{"*"}) {
		t.Fatalf("ExtentPatterns = %v, want [*]", cfg.ExtentPatterns)
	}
}

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

func TestLoadProfileConfigExtentIsNonePlusAllFilesExtent(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	cfg, err := loadProfileConfig("extent")
	if err != nil {
		t.Fatalf("loadProfileConfig: %v", err)
	}
	if cfg.Name != "extent" || cfg.Source != "builtin:extent" {
		t.Fatalf("extent profile = %#v", cfg)
	}
	if len(cfg.LocalOnlyPatterns) != 0 || len(cfg.RemoteOnlyPatterns) != 0 || len(cfg.PackPaths) != 0 || len(cfg.AppendLogPatterns) != 0 {
		t.Fatalf("extent profile should match none except [extent]: %#v", cfg)
	}
	if !reflect.DeepEqual(cfg.ExtentPatterns, []string{"*"}) {
		t.Fatalf("ExtentPatterns = %v, want [*]", cfg.ExtentPatterns)
	}
}

func TestProfileAllowsOverlayExtentLikeNone(t *testing.T) {
	if profileAllowsOverlay("extent") {
		t.Fatal("extent must not require --local-root (same as none)")
	}
	if profileAllowsOverlay("none") {
		t.Fatal("none must not require --local-root")
	}
	if !profileAllowsOverlay("coding-agent-extent") {
		t.Fatal("coding-agent-extent keeps the coding-agent overlay")
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
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("profile show output = %q, want %q", out, want)
		}
	}
	if strings.Contains(out, "/coding-agent/") {
		t.Fatalf("profile show output = %q, should not contain profile-scoped pack path", out)
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
