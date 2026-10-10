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
