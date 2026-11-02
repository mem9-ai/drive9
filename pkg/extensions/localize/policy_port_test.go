package localize

// Rows ported from v1 pkg/fuse/local_policy_test.go: classification
// precedence (remote-only wins), nested-subpath matching, the intentionally
// short default lists, and unsafe-pattern validation.

import (
	"testing"

	"github.com/mem9-ai/drive9/pkg/pathfilter"
)

// v1 TestLocalPolicyRemoteOnlyOverridesLocalOnly.
func TestPortRemoteOnlyOverridesLocalOnly(t *testing.T) {
	p := NewPolicy("", []string{"**/node_modules/**"}, nil, []string{"**/node_modules/**"}, false)
	if got := p.Classify("/repo/node_modules/x.js", nil, false); got != LayerRemotePersistent {
		t.Fatalf("remote-only override = %v, want remote persistent", got)
	}
}

// v1 TestLocalPolicyMatchesNestedSubpath.
func TestPortNestedSubpathMatching(t *testing.T) {
	p := NewPolicy("", []string{"**/node_modules/**"}, nil, nil, false)
	if got := p.Classify("/repo/a/b/node_modules/c/util.js", nil, false); got != LayerLocalOnly {
		t.Fatalf("nested node_modules = %v, want local-only", got)
	}
	if got := p.Classify("/repo/web/node_modules_extra/util.js", nil, false); got != LayerRemotePersistent {
		t.Fatalf("node_modules_extra = %v, want remote persistent", got)
	}
}

// v1 TestLocalPolicyDefaultsDependencyAndTargetToLocalOnly + the default
// list shape (CLI builtinCodingAgent* pattern set).
func TestPortDefaultDependenciesClassifyLocalOnly(t *testing.T) {
	p := NewPolicy("", []string{"**/node_modules/**", "**/.venv/**"}, []string{"**/target/**"}, nil, false)
	confirm := GitIgnoreOracle(func(path string, dir bool) (bool, bool) { return true, true }) // gitignore confirms
	for _, path := range []string{
		"/repo/node_modules/pkg/index.js",
		"/repo/.venv/bin/python",
		"/repo/.venv/lib/python3.12/site-packages/numpy/core.py",
	} {
		if got := p.Classify(path, nil, false); got != LayerLocalOnly {
			t.Errorf("Classify(%q) = %v, want local-only default", path, got)
		}
	}
	if got := p.Classify("/repo/target/debug/app", confirm, false); got != LayerLocalOnly {
		t.Errorf("gitignore-confirmed target = %v, want local-only", got)
	}
	// v1: Classify does not apply the gitignore gate — a nil oracle leaves
	// gated paths local-only (the fs layer confirms them).
	if got := p.Classify("/repo/target/debug/app", nil, false); got != LayerLocalOnly {
		t.Errorf("target with nil oracle = %v, want local-only (gate at fs layer)", got)
	}
	deny := GitIgnoreOracle(func(path string, dir bool) (bool, bool) { return false, true })
	if got := p.Classify("/repo/target/debug/app", deny, false); got != LayerRemotePersistent {
		t.Errorf("gitignore-denied target = %v, want remote persistent", got)
	}
}

// v1 TestLocalPolicyDefaultsLeaveOtherBuildOutputRemote: the default lists
// are intentionally short; everything else stays remote.
func TestPortOtherBuildOutputStaysRemote(t *testing.T) {
	p := NewPolicy("", []string{"**/node_modules/**", "**/.venv/**"}, []string{"**/target/**"}, nil, false)
	for _, path := range []string{
		"/repo/.pnpm-store/v3/files/pkg",
		"/repo/.tox/py312/lib/python3.12/site-packages/x.py",
		"/repo/dist/app.js",
		"/repo/build/output.bin",
		"/repo/coverage/lcov.info",
		"/repo/.cache/tool/state.json",
		"/repo/.turbo/cache/state.json",
		"/repo/web/.next/cache/webpack.bin",
		"/repo/.pytest_cache/v/cache/nodeids",
		"/repo/.mypy_cache/3.12/module.meta.json",
		"/repo/pkg/__pycache__/mod.cpython-312.pyc",
		"/repo/.gradle/caches/modules-2/files-2.1",
	} {
		if got := p.Classify(path, nil, false); got != LayerRemotePersistent {
			t.Errorf("Classify(%q) = %v, want remote persistent default", path, got)
		}
	}
}

// v1 TestValidateLocalPolicyPatternsRejectsUnsafeNormalization: patterns
// that normalize away path separators must be rejected.
func TestPortValidateRejectsUnsafePatterns(t *testing.T) {
	for _, pat := range []string{"**/../**", "a/../b/**", `a\b\**`} {
		if err := pathfilter.Validate([]string{pat}); err == nil {
			t.Errorf("pattern %q accepted, want rejection", pat)
		}
	}
	if err := pathfilter.Validate([]string{"**/node_modules/**"}); err != nil {
		t.Fatalf("valid pattern rejected: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Remaining rows from v1 pkg/fuse/local_policy_test.go (d39a6d1b3), updated
// for the corrected builtin default set: node_modules/.venv are unconditionally
// local-only and target is gitignore-gated. The v1 append-log and SQLite
// sidecar rows (NewAppendLogMatcher, isSQLite*Path, usesTransientLocalOverlay)
// live in the appendlog/transient subsystems and were recorded as deferred
// decisions, not policy rows.

// v1 TestLocalPolicyCodingAgentDefaultsMatchGitSegmentExactly: the coding-agent
// builtin set is exactly node_modules, .venv (local-only) and target (gated) —
// VCS metadata is not a static pattern and stays remote.
func TestPortCodingAgentBuiltinDefaultsShape(t *testing.T) {
	p := NewPolicy("coding-agent", nil, nil, nil, true)
	if !p.Enabled() {
		t.Fatal("coding-agent builtin defaults must enable the policy")
	}
	confirm := GitIgnoreOracle(func(path string, dir bool) (bool, bool) { return true, true })
	rows := []struct {
		path string
		want Layer
	}{
		{path: "/repo/node_modules", want: LayerLocalOnly},
		{path: "/repo/node_modules/react/index.js", want: LayerLocalOnly},
		{path: "/repo/.venv", want: LayerLocalOnly},
		{path: "/repo/target/debug/app", want: LayerLocalOnly},
		// target is gated, but a nil oracle leaves gated paths local-only
		// (v1: the gate is confirmed at the fs layer, not in Classify).
		{path: "/repo/node_modules_extra/x.js", want: LayerRemotePersistent},
		{path: "/repo/.git", want: LayerRemotePersistent},
		{path: "/repo/.git/config", want: LayerRemotePersistent},
		{path: "/repo/.hg/store/data", want: LayerRemotePersistent},
		{path: "/repo/.svn/pristine/aa", want: LayerRemotePersistent},
		{path: "/repo/.gitignore", want: LayerRemotePersistent},
		{path: "/repo/.gitattributes", want: LayerRemotePersistent},
		{path: "/repo/notes.git.txt", want: LayerRemotePersistent},
	}
	for _, row := range rows {
		if got := p.Classify(row.path, nil, false); got != row.want {
			t.Errorf("Classify(%q) = %v, want %v", row.path, got, row.want)
		}
	}
	// A gitignore-denying oracle only demotes the gated target row.
	if got := p.Classify("/repo/target/debug/app", confirm, false); got != LayerLocalOnly {
		t.Errorf("confirmed target = %v, want local-only", got)
	}
	deny := GitIgnoreOracle(func(path string, dir bool) (bool, bool) { return false, true })
	if got := p.Classify("/repo/node_modules/react/index.js", deny, false); got != LayerLocalOnly {
		t.Errorf("denied node_modules = %v, want still local-only (not gated)", got)
	}
	if got := p.Classify("/repo/target/debug/app", deny, false); got != LayerRemotePersistent {
		t.Errorf("denied target = %v, want remote persistent", got)
	}
}

// v1 TestLocalPolicyRemoteOnlyOverridesLocalOnly, carve-out variant: a
// remote-only subpath carves a hole inside a local-only tree.
func TestPortRemoteOnlyCarvesOutLocalOnlySubtree(t *testing.T) {
	p := NewPolicy("coding-agent", []string{"**/node_modules/**"}, nil, []string{"**/node_modules/keep/**"}, true)
	if got := p.Classify("/repo/node_modules/pkg/index.js", nil, false); got != LayerLocalOnly {
		t.Fatalf("node_modules package = %v, want local-only", got)
	}
	if got := p.Classify("/repo/node_modules/keep/index.js", nil, false); got != LayerRemotePersistent {
		t.Fatalf("remote-only carve-out = %v, want remote persistent", got)
	}
}

// v1 TestLocalPolicyRemoteOnlyOverridesBuiltinTmpRule: a remote-only list
// coexists with the builtins — remote rows stay remote, default local rows
// stay local.
func TestPortRemoteOnlyListDoesNotDisturbBuiltinDefaults(t *testing.T) {
	p := NewPolicy("coding-agent", nil, nil, []string{"**/tmp/**"}, true)
	if got := p.Classify("/repo/tmp/object.blob", nil, false); got != LayerRemotePersistent {
		t.Fatalf("tmp object = %v, want remote persistent", got)
	}
	if got := p.Classify("/repo/blobs/aa/object.blob", nil, false); got != LayerRemotePersistent {
		t.Fatalf("blob object = %v, want remote persistent", got)
	}
	if got := p.Classify("/repo/node_modules/react/index.js", nil, false); got != LayerLocalOnly {
		t.Fatalf("builtin node_modules = %v, want local-only", got)
	}
}

// v1 TestLocalPolicyDisabledForExtentProfile + TestLocalPolicyDisabledFor-
// OrdinaryMount: extent / interactive / none / empty profiles never carry
// local routing.
func TestPortDisabledForExtentAndOrdinaryProfiles(t *testing.T) {
	for _, profile := range []string{"extent", "interactive", "none", ""} {
		p := NewPolicy(profile, nil, nil, nil, true)
		if p.Enabled() {
			t.Fatalf("profile %q must not enable the local overlay", profile)
		}
		if got := p.Classify("/repo/.git/config", nil, false); got != LayerRemotePersistent {
			t.Fatalf("profile %q .git = %v, want remote persistent", profile, got)
		}
	}
	// An ordinary mount with explicit patterns still routes them (v1:
	// explicit lists are honored regardless of profile).
	p := NewPolicy("", []string{"**/node_modules/**"}, nil, nil, true)
	if !p.Enabled() {
		t.Fatal("explicit local-only list must enable the policy")
	}
}

// The applyBuiltinDefaults switch: false means the caller already resolved
// an effective profile — a custom list is not silently merged with the
// builtins, and builtins alone do not enable the policy.
func TestPortBuiltinDefaultsToggle(t *testing.T) {
	p := NewPolicy("coding-agent", nil, nil, nil, false)
	if p.Enabled() {
		t.Fatal("coding-agent with no lists and no builtin merge must stay disabled")
	}
	q := NewPolicy("coding-agent", []string{"**/mine/**"}, nil, nil, false)
	if !q.Enabled() {
		t.Fatal("custom list must enable the policy without builtin merge")
	}
	if got := q.Classify("/repo/node_modules/react/index.js", nil, false); got != LayerRemotePersistent {
		t.Fatalf("builtin leaked into merged list: node_modules = %v, want remote", got)
	}
	if got := q.Classify("/repo/mine/x", nil, false); got != LayerLocalOnly {
		t.Fatalf("custom list = %v, want local-only", got)
	}
	// With the merge on, custom rows append after the builtins and both stay
	// active (v1 NewLocalPolicy semantics).
	r := NewPolicy("coding-agent", []string{"**/mine/**"}, nil, nil, true)
	for _, path := range []string{"/repo/node_modules/x", "/repo/mine/x"} {
		if got := r.Classify(path, nil, false); got != LayerLocalOnly {
			t.Fatalf("Classify(%q) = %v, want local-only (builtins + custom)", path, got)
		}
	}
}

// Pattern order is remote-only, then local-only, then gitignore-gated: a
// local-only match never consults the oracle; only a gated match does.
func TestPortClassifyOrderConsultsOracleOnlyForGatedRows(t *testing.T) {
	calls := 0
	oracle := GitIgnoreOracle(func(path string, dir bool) (bool, bool) {
		calls++
		return true, true
	})
	p := NewPolicy("coding-agent", nil, nil, nil, true)
	if got := p.Classify("/repo/node_modules/react/index.js", oracle, false); got != LayerLocalOnly {
		t.Fatalf("node_modules = %v, want local-only", got)
	}
	if calls != 0 {
		t.Fatalf("oracle consulted %d times for a local-only match, want 0", calls)
	}
	if got := p.Classify("/repo/target/debug/app", oracle, false); got != LayerLocalOnly {
		t.Fatalf("target = %v, want local-only", got)
	}
	if calls != 1 {
		t.Fatalf("oracle consulted %d times for a gated match, want 1", calls)
	}
	// A gated row with an unverified oracle (no repository to ask) stays
	// local-only: the gate exists to drop paths the repository keeps, not to
	// disable the overlay wherever no repository is available.
	unverified := GitIgnoreOracle(func(path string, dir bool) (bool, bool) { return false, false })
	if got := p.Classify("/repo/target/debug/app", unverified, false); got != LayerLocalOnly {
		t.Fatalf("unverified-oracle target = %v, want local-only", got)
	}
}

// The root path (and any bare single-segment path outside the patterns)
// never routes local.
func TestPortRootPathStaysRemote(t *testing.T) {
	p := NewPolicy("coding-agent", nil, nil, nil, true)
	for _, path := range []string{"/", "/node_modules2", "/node_modules_extra"} {
		if got := p.Classify(path, nil, true); got != LayerRemotePersistent {
			t.Errorf("Classify(%q) = %v, want remote persistent", path, got)
		}
	}
}

// v1 TestLocalPolicyInvalidRuntimePathDoesNotMatchLocalOnly: a runtime path
// that fails canonicalization (backslashes) must not match anything.
func TestPortInvalidRuntimePathDoesNotMatchLocalOnly(t *testing.T) {
	p := NewPolicy("coding-agent", nil, nil, nil, true)
	if got := p.Classify(`repo\node_modules\pkg\index.js`, nil, false); got != LayerRemotePersistent {
		t.Fatalf("invalid backslash runtime path = %v, want remote persistent", got)
	}
}

// v1 TestLocalPolicyRuntimePathWhitespaceIsNotTrimmedIntoMatch: surrounding
// whitespace is significant at the runtime boundary.
func TestPortRuntimePathWhitespaceIsNotTrimmedIntoMatch(t *testing.T) {
	p := NewPolicy("coding-agent", nil, nil, nil, true)
	if got := p.Classify("/repo/node_modules ", nil, false); got != LayerRemotePersistent {
		t.Fatalf("spaced node_modules segment = %v, want remote persistent", got)
	}
}
