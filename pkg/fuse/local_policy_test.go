package fuse

import (
	"strings"
	"testing"
)

func TestLocalPolicyCodingAgentDefaultsMatchGitSegmentExactly(t *testing.T) {
	policy := NewLocalPolicy(MountProfileCodingAgent, nil, nil)

	tests := []struct {
		path string
		want PathLayer
	}{
		{path: "/repo/.git", want: PathLayerLocalOnly},
		{path: "/repo/.git/config", want: PathLayerLocalOnly},
		{path: "/repo/src/.git/objects/ab/cd", want: PathLayerLocalOnly},
		{path: "/repo/.gitignore", want: PathLayerRemotePersistent},
		{path: "/repo/.gitattributes", want: PathLayerRemotePersistent},
		{path: "/repo/notes.git.txt", want: PathLayerRemotePersistent},
	}

	for _, test := range tests {
		if got := policy.Classify(test.path); got != test.want {
			t.Errorf("Classify(%q) = %s, want %s", test.path, got, test.want)
		}
	}
}

func TestLocalPolicyRemoteOnlyOverridesLocalOnly(t *testing.T) {
	policy := NewLocalPolicy(
		MountProfileCodingAgent,
		[]string{"**/node_modules/**"},
		[]string{"**/node_modules/keep/**"},
	)

	if got := policy.Classify("/repo/node_modules/pkg/index.js"); got != PathLayerLocalOnly {
		t.Fatalf("node_modules package = %s, want local-only", got)
	}
	if got := policy.Classify("/repo/node_modules/keep/index.js"); got != PathLayerRemotePersistent {
		t.Fatalf("remote override = %s, want remote persistent", got)
	}
}

func TestLocalPolicyDisabledForOrdinaryMount(t *testing.T) {
	policy := NewLocalPolicy("", nil, nil)
	if policy.Enabled() {
		t.Fatal("ordinary mount policy should be disabled")
	}
	if got := policy.Classify("/repo/.git/config"); got != PathLayerRemotePersistent {
		t.Fatalf("ordinary mount .git = %s, want remote persistent", got)
	}
}

func TestLocalPolicyMatchesNestedSubpath(t *testing.T) {
	policy := NewLocalPolicy(MountProfileCodingAgent, []string{"**/.next/cache/**"}, nil)
	if got := policy.Classify("/repo/web/.next/cache/webpack.bin"); got != PathLayerLocalOnly {
		t.Fatalf(".next/cache = %s, want local-only", got)
	}
	if got := policy.Classify("/repo/web/.next/server/page.js"); got != PathLayerRemotePersistent {
		t.Fatalf(".next/server = %s, want remote persistent", got)
	}
}

func TestLocalPolicyDefaultsOnlyVCSDatabaseSegmentsToLocalOnly(t *testing.T) {
	policy := NewLocalPolicy(MountProfileCodingAgent, nil, nil)

	for _, path := range []string{
		"/repo/.hg/store/data",
		"/repo/.svn/wc.db",
	} {
		if got := policy.Classify(path); got != PathLayerLocalOnly {
			t.Errorf("Classify(%q) = %s, want local-only default", path, got)
		}
	}

	for _, path := range []string{
		"/repo/node_modules/pkg/index.js",
		"/repo/.pnpm-store/v3/files/pkg",
		"/repo/dist/app.js",
		"/repo/build/output.bin",
		"/repo/target/debug/app",
		"/repo/web/.next/cache/webpack.bin",
		"/repo/.pytest_cache/v/cache/nodeids",
		"/repo/.mypy_cache/3.12/module.meta.json",
		"/repo/.ruff_cache/content",
		"/repo/pkg/__pycache__/mod.cpython-312.pyc",
		"/repo/.gradle/caches/modules-2/files-2.1",
		"/repo/.venv/bin/python",
	} {
		if got := policy.Classify(path); got != PathLayerRemotePersistent {
			t.Errorf("Classify(%q) = %s, want remote-persistent default", path, got)
		}
	}
}

func TestValidateLocalPolicyPatternsRejectsUnsafeNormalization(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		want    string
	}{
		{name: "backslash", pattern: `**\\.git\\**`, want: "path contains backslash"},
		{name: "dotdot", pattern: "**/../.git/**", want: `path contains ".." segment`},
		{name: "dot", pattern: "**/./.git/**", want: `path contains "." segment`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateLocalPolicyPatterns([]string{test.pattern}, nil)
			if err == nil {
				t.Fatalf("validateLocalPolicyPatterns(%q) = nil, want error", test.pattern)
			}
			if got := err.Error(); !strings.Contains(got, test.want) {
				t.Errorf("error = %q, want %q", got, test.want)
			}
		})
	}
}

func TestLocalPolicyInvalidRuntimePathDoesNotMatchLocalOnly(t *testing.T) {
	policy := NewLocalPolicy(MountProfileCodingAgent, nil, nil)
	if got := policy.Classify(`repo\\.git\\config`); got != PathLayerRemotePersistent {
		t.Fatalf("invalid backslash runtime path = %s, want remote persistent", got)
	}
}

func TestLocalPolicyRuntimePathWhitespaceIsNotTrimmedIntoMatch(t *testing.T) {
	policy := NewLocalPolicy(MountProfileCodingAgent, nil, nil)
	if got := policy.Classify("/repo/.git "); got != PathLayerRemotePersistent {
		t.Fatalf("runtime path with spaced .git segment = %s, want remote persistent", got)
	}
}
