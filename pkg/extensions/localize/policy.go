package localize

import (
	"strings"

	"github.com/mem9-ai/drive9/pkg/pathfilter"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

// Layer names the storage plane a path routes to.
type Layer uint8

const (
	// LayerRemotePersistent syncs the path through the remote namespace.
	LayerRemotePersistent Layer = iota
	// LayerLocalOnly keeps the path in the mount-local overlay; a remote
	// copy at that path stays invisible while the overlay owns it.
	LayerLocalOnly
)

// GitIgnoreOracle reports whether the repository's ignore rules cover
// viewPath, and whether a repository could be consulted at all. verified
// is false when there is no repository to ask; the caller then keeps a
// gated pattern's overlay on its own (the gate exists to drop paths the
// repository keeps, not to disable the overlay wherever no repository is
// available). dirHint tells the oracle the path denotes a directory, which
// changes directory-anchored ignore rules.
type GitIgnoreOracle func(viewPath string, dirHint bool) (confirmed, verified bool)

// Policy routes paths between the local overlay and remote-persistent
// storage. There are two local lists with different confirmation rules:
// localOnly is overlaid unconditionally, while localGitignoreAware is
// overlaid only when the repository's ignore rules also ignore the path —
// this keeps a directory that merely shares a name with generated output
// out of the overlay. remoteOnly wins over both.
type Policy struct {
	enabled             bool
	localOnly           []pathfilter.Pattern
	localGitignoreAware []pathfilter.Pattern
	remoteOnly          []pathfilter.Pattern
}

// NewPolicy compiles a routing policy. A policy without any pattern stays
// disabled: every path then routes remote-persistent. applyBuiltinDefaults
// selects whether the profile's built-in local-only defaults are added
// ahead of the given lists (v1 NewLocalPolicy semantics); callers that have
// already resolved an effective profile (the CLI) pass false so a custom
// profile reusing a builtin name is not silently merged with the builtins.
func NewPolicy(profile string, localOnly, localGitignoreAware, remoteOnly []string, applyBuiltinDefaults bool) *Policy {
	p := &Policy{}
	if !profileAllowsLocalPolicy(profile) &&
		len(localOnly) == 0 && len(localGitignoreAware) == 0 && len(remoteOnly) == 0 {
		return p
	}
	localPatterns := localOnly
	gatedPatterns := localGitignoreAware
	if applyBuiltinDefaults {
		localPatterns = append(append([]string{}, builtinLocalOnlyPatterns(profile)...), localOnly...)
		gatedPatterns = append(append([]string{}, builtinGitignoreAwarePatterns(profile)...), localGitignoreAware...)
	}
	if len(localPatterns) == 0 && len(gatedPatterns) == 0 && len(remoteOnly) == 0 {
		return p
	}
	p.enabled = true
	p.localOnly = pathfilter.CompileAll(localPatterns)
	p.localGitignoreAware = pathfilter.CompileAll(gatedPatterns)
	p.remoteOnly = pathfilter.CompileAll(remoteOnly)
	return p
}

// profileAllowsLocalPolicy mirrors v1: these profiles never carry local
// routing defaults.
func profileAllowsLocalPolicy(profile string) bool {
	profile = strings.TrimSpace(profile)
	return profile != "" && profile != "interactive" && profile != "none" && profile != "extent"
}

// builtinLocalOnlyPatterns are overlaid unconditionally (v1
// defaultCodingAgentLocalOnlyPatterns). VCS metadata is deliberately absent:
// .git is routed structurally through git workspaces; .hg/.svn always sync.
func builtinLocalOnlyPatterns(profile string) []string {
	if profile != "coding-agent" {
		return nil
	}
	return []string{
		"**/node_modules/**",
		"**/.venv/**",
	}
}

// builtinGitignoreAwarePatterns are overlaid only when the repository's Git
// ignore rules also ignore them (v1 defaultCodingAgentGitignoreAwarePatterns):
// `target` is a common directory name that is not always build output.
func builtinGitignoreAwarePatterns(profile string) []string {
	if profile != "coding-agent" {
		return nil
	}
	return []string{
		"**/target/**",
	}
}

// Classify routes one path. The remote-only list is consulted first, then
// the unconditional local list, then the gitignore-gated local list (a
// gated match demotes to remote-persistent only when the oracle verified
// that the repository keeps the path).
func (p *Policy) Classify(viewPath string, gitIgnore GitIgnoreOracle, dirHint bool) Layer {
	if !p.Enabled() {
		return LayerRemotePersistent
	}
	cleaned, err := canonicalPolicyPath(viewPath)
	if err != nil {
		return LayerRemotePersistent
	}
	for _, pat := range p.remoteOnly {
		if pat.MatchCanonical(cleaned) {
			return LayerRemotePersistent
		}
	}
	for _, pat := range p.localOnly {
		if pat.MatchCanonical(cleaned) {
			return LayerLocalOnly
		}
	}
	for _, pat := range p.localGitignoreAware {
		if pat.MatchCanonical(cleaned) {
			if gitIgnore != nil {
				if confirmed, verified := gitIgnore(viewPath, dirHint); verified && !confirmed {
					return LayerRemotePersistent
				}
			}
			return LayerLocalOnly
		}
	}
	return LayerRemotePersistent
}

// Enabled reports whether any pattern list is active.
func (p *Policy) Enabled() bool { return p != nil && p.enabled }

// canonicalPolicyPath canonicalizes a runtime path for pattern matching.
// Surrounding whitespace is deliberately kept: a path like
// "/repo/node_modules " must not match "**/node_modules/**" — whitespace is
// significant at the runtime boundary. Pattern-side canonicalization trims;
// runtime-side canonicalization does not.
func canonicalPolicyPath(value string) (string, error) {
	cleaned, err := pathutil.Canonicalize(value)
	if err != nil {
		return "", err
	}
	return strings.TrimPrefix(cleaned, "/"), nil
}
