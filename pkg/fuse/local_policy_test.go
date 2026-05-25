package fuse

import "testing"

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
			t.Fatalf("Classify(%q) = %s, want %s", test.path, got, test.want)
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
	policy := NewLocalPolicy(MountProfileCodingAgent, nil, nil)
	if got := policy.Classify("/repo/web/.next/cache/webpack.bin"); got != PathLayerLocalOnly {
		t.Fatalf(".next/cache = %s, want local-only", got)
	}
	if got := policy.Classify("/repo/web/.next/server/page.js"); got != PathLayerRemotePersistent {
		t.Fatalf(".next/server = %s, want remote persistent", got)
	}
}

func TestLocalPolicyDoesNotDefaultProjectOutputsToLocalOnly(t *testing.T) {
	policy := NewLocalPolicy(MountProfileCodingAgent, nil, nil)

	for _, path := range []string{
		"/repo/dist/app.js",
		"/repo/build/output.bin",
		"/repo/.venv/bin/python",
		"/repo/target/debug/app",
		"/repo/node_modules/pkg/index.js",
	} {
		if got := policy.Classify(path); got != PathLayerRemotePersistent {
			t.Fatalf("Classify(%q) = %s, want remote persistent default", path, got)
		}
	}
}
