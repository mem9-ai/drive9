package pathfilter

import (
	"testing"
)

func TestCompileThreeForms(t *testing.T) {
	cases := []struct {
		name    string
		pattern string
		path    string
		want    bool
	}{
		// **/x/** subpath form
		{"subpath-match", "**/node_modules/**", "proj/src/node_modules/react/index.js", true},
		{"subpath-match-root-level", "**/node_modules/**", "node_modules/react/index.js", true},
		{"subpath-no-match", "**/node_modules/**", "proj/src/app/main.go", false},
		{"subpath-nested", "**/.git/**", "proj/.git/refs/heads/main", true},
		{"subpath-bare-dir", "**/dist", "proj/dist", true},
		{"subpath-bare-dir-no-trailing", "**/dist", "proj/dist/index.js", true},
		// prefix/** form
		{"prefix-match", "dist/**", "dist/index.js", true},
		{"prefix-nested", "dist/**", "dist/a/b/c.js", true},
		{"prefix-no-match", "dist/**", "src/index.js", false},
		{"prefix-exact-dir", "dist/**", "dist", true},
		// exact/glob form
		{"exact-match", "build", "build", true},
		{"exact-no-match", "build", "build/dist", false},
		{"glob-match", "*.log", "app.log", true},
		{"glob-no-match", "*.log", "app.txt", false},
		{"glob-dir-level", "*.log", "logs/app.log", false}, // path.Match against full path
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := Compile(tc.pattern)
			if err != nil {
				t.Fatalf("Compile(%q) error: %v", tc.pattern, err)
			}
			if got := p.Match(tc.path); got != tc.want {
				t.Fatalf("Pattern(%q).Match(%q) = %v, want %v", tc.pattern, tc.path, got, tc.want)
			}
		})
	}
}

func TestValidateRejectsInvalid(t *testing.T) {
	// pathutil.Canonicalize rejects backslashes and .. segments.
	if err := Validate([]string{"**/../etc/**"}); err == nil {
		t.Fatal("Validate accepted a pattern containing ..")
	}
}

func TestMatcherIncludeExclude(t *testing.T) {
	// Include whitelist: everything under proj/** plus go.mod at root.
	// Exclude: vendor subtree and *_test.go exact files (glob, root-level only).
	m := NewMatcher(
		[]string{"proj/**", "go.mod"},
		[]string{"**/vendor/**", "*_test.go"},
		nil,
	)
	cases := []struct {
		path string
		want bool
	}{
		{"proj/src/main.go", true},            // matches include, not exclude
		{"proj/src/server/server.go", true},   // matches include, not exclude
		{"go.mod", true},                       // exact include match
		{"proj/vendor/foo.go", false},          // matches include but excluded (vendor)
		{"proj/main_test.go", true},           // *_test.go exclude is root-level glob only
		{"main_test.go", false},                // excluded by root-level *_test.go glob
		{"README.md", false},                   // not in include whitelist
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			if got := m.Match(tc.path); got != tc.want {
				t.Fatalf("Match(%q) = %v, want %v", tc.path, got, tc.want)
			}
		})
	}
}

func TestMatcherNoIncludeAcceptsAll(t *testing.T) {
	m := NewMatcher(nil, []string{"**/.git/**", "**/node_modules/**"}, nil)
	if !m.Match("proj/src/app.go") {
		t.Fatal("non-excluded path should match when include is empty")
	}
	if m.Match("proj/node_modules/react/index.js") {
		t.Fatal("excluded path should not match")
	}
	if m.Match("proj/.git/HEAD") {
		t.Fatal(".git should be excluded")
	}
}

func TestMatcherOverrideRestoresExcluded(t *testing.T) {
	// Simulate profile coding-agent: exclude node_modules, but override
	// restores a specific project's node_modules manifest.
	m := NewMatcher(
		nil,
		[]string{"**/node_modules/**"},
		[]string{"proj/node_modules/.package-lock.json"},
	)
	if m.Match("proj/node_modules/react/index.js") {
		t.Fatal("excluded node_modules should be dropped")
	}
	if !m.Match("proj/node_modules/.package-lock.json") {
		t.Fatal("override should restore the package-lock")
	}
	if !m.Match("proj/src/app.go") {
		t.Fatal("non-excluded path should pass")
	}
}

func TestMatcherExcludeOverridesInclude(t *testing.T) {
	// If a path matches both include and exclude, exclude wins (unless override).
	m := NewMatcher([]string{"proj/**"}, []string{"**/vendor/**"}, nil)
	if m.Match("proj/vendor/foo.go") {
		t.Fatal("exclude must win over include when no override present")
	}
	if !m.Match("proj/src/foo.go") {
		t.Fatal("included non-excluded path should match")
	}
}

func TestCodingAgentDefaultPatterns(t *testing.T) {
	patterns := []string{
		"**/.git/**", "**/.hg/**", "**/.svn/**", "**/node_modules/**",
		"**/.pnpm-store/**", "**/target/**", "**/dist/**", "**/build/**",
		"**/coverage/**", "**/tmp/**", "**/.tmp/**", "**/.tmp-api-extractor/**",
		"**/.cache/**", "**/.turbo/**", "**/.next/cache/**", "**/.vitepress/cache/**",
		"**/.gradle/**", "**/.venv/**", "**/__pycache__/**", "**/.pytest_cache/**",
		"**/.mypy_cache/**", "**/.ruff_cache/**",
	}
	m := NewMatcher(nil, patterns, nil)
	drop := []string{
		".git/HEAD", "proj/.git/config", "node_modules/react/index.js",
		"proj/dist/bundle.js", "proj/build/output.o", "proj/.venv/bin/python",
		"proj/__pycache__/foo.cpython-311.pyc", "proj/.next/cache/abc",
		"proj/.pytest_cache/v/cache/lastfailed",
	}
	for _, p := range drop {
		if m.Match(p) {
			t.Fatalf("coding-agent pattern should drop %q", p)
		}
	}
	keep := []string{"proj/src/main.go", "README.md", "proj/go.mod", "proj/.gitignore"}
	for _, p := range keep {
		if !m.Match(p) {
			t.Fatalf("coding-agent pattern should keep %q", p)
		}
	}
}

func TestEmptyMatcherMatchesAll(t *testing.T) {
	m := NewMatcher(nil, nil, nil)
	if !m.Match("anything/here.go") {
		t.Fatal("empty matcher should match all")
	}
	if !m.Match("") {
		t.Fatal("empty matcher should match empty path")
	}
}

func TestCompileAllSkipsBlankAndInvalid(t *testing.T) {
	got := CompileAll([]string{"", "  ", "dist/**", "**/../bad/**"})
	if len(got) != 1 {
		t.Fatalf("CompileAll kept %d patterns, want 1 (only dist/**)", len(got))
	}
	if got[0].Raw() != "dist/**" {
		t.Fatalf("CompileAll kept wrong pattern: %q", got[0].Raw())
	}
}

func TestMatchExcluded(t *testing.T) {
	m := NewMatcher(
		[]string{"src/app.go"}, // include whitelist (only this leaf)
		[]string{"**/node_modules/**", "**/.git/**"},
		[]string{"proj/node_modules/.package-lock.json"}, // override
	)
	// Excluded subtrees.
	if !m.MatchExcluded("proj/node_modules/react") {
		t.Fatal("node_modules/react should be MatchExcluded (prune subtree)")
	}
	if !m.MatchExcluded("proj/.git") {
		t.Fatal(".git should be MatchExcluded")
	}
	// Override restores — NOT excluded.
	if m.MatchExcluded("proj/node_modules/.package-lock.json") {
		t.Fatal("override should make package-lock.json not MatchExcluded")
	}
	// A directory that only fails the include whitelist is NOT MatchExcluded —
	// pruning it would drop its children (the B2 bug).
	if m.MatchExcluded("src") {
		t.Fatal("src (only failing include) must NOT be MatchExcluded — its child src/app.go is included")
	}
	// Leaf that fails include is not "excluded" in the prune sense either.
	if m.MatchExcluded("src/util.go") {
		t.Fatal("src/util.go is dropped by include, not by exclude — not MatchExcluded")
	}
}

func TestGlobQuestionAndCharClass(t *testing.T) {
	// Character class form: use direct pattern matching (compile + Match)
	// so the matcher's include/exclude semantics don't interfere.
	p, err := Compile("*.[Tt]xt")
	if err != nil {
		t.Fatalf("Compile *.[Tt]xt: %v", err)
	}
	if !p.Match("a.txt") {
		t.Fatal("*.[Tt]xt should match a.txt")
	}
	if !p.Match("a.Txt") {
		t.Fatal("*.[Tt]xt should match a.Txt")
	}
	if p.Match("a.go") {
		t.Fatal("*.[Tt]xt should not match a.go")
	}
	// ? matches a single non-separator char.
	pq, err := Compile("a?.go")
	if err != nil {
		t.Fatalf("Compile a?.go: %v", err)
	}
	if !pq.Match("ab.go") {
		t.Fatal("a?.go should match ab.go")
	}
	if pq.Match("abc.go") {
		t.Fatal("a?.go should not match abc.go (? is single char)")
	}
	if pq.Match("a/.go") {
		t.Fatal("a?.go should not match a/.go (? does not cross separators)")
	}
	// Negated class.
	pn, err := Compile("*.[^Tt]xt")
	if err != nil {
		t.Fatalf("Compile *.[^Tt]xt: %v", err)
	}
	if pn.Match("a.txt") {
		t.Fatal("*.[^Tt]xt should not match a.txt")
	}
	if !pn.Match("a.bxt") {
		t.Fatal("*.[^Tt]xt should match a.bxt")
	}
}