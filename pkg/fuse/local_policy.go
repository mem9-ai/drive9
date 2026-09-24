package fuse

import (
	"context"
	"strings"

	"github.com/mem9-ai/drive9/pkg/pathfilter"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

const (
	MountProfileInteractive = "interactive"
	MountProfileCodingAgent = "coding-agent"
	MountProfileNone        = "none"
	MountProfileExtent      = "extent"
)

type PathLayer string

const (
	PathLayerRemotePersistent PathLayer = "remote_persistent"
	PathLayerLocalOnly        PathLayer = "local_only"
	PathLayerGitWorkspace     PathLayer = "git_workspace"
)

type policyMatchSource string

const (
	policyMatchDisabled       policyMatchSource = "disabled"
	policyMatchRemoteDefault  policyMatchSource = "remote_default"
	policyMatchLocalOnly      policyMatchSource = "local_only"
	policyMatchLocalGated     policyMatchSource = "local_gitignore_aware"
	policyMatchRemoteOverride policyMatchSource = "remote_override"
)

// LocalPolicy routes paths to the local overlay or remote-persistent storage.
//
// There are two local lists with different confirmation rules:
//
//   - localOnly is overlaid unconditionally.
//   - localGitignoreAware is overlaid only when the repository's Git ignore
//     rules also ignore the path (see Dat9FS.observePathPolicyWithHint). This
//     keeps a directory that merely shares a name with generated output out of
//     the overlay.
//
// remoteOnly wins over both.
type LocalPolicy struct {
	enabled             bool
	localOnly           []pathfilter.Pattern
	localGitignoreAware []pathfilter.Pattern
	remoteOnly          []pathfilter.Pattern
}

// AppendLogMatcher recognizes operator-declared append-log paths without
// altering normal local/remote path routing.
type AppendLogMatcher struct {
	patterns []pathfilter.Pattern
}

func NewAppendLogMatcher(patterns []string) *AppendLogMatcher {
	return &AppendLogMatcher{patterns: pathfilter.CompileAll(patterns)}
}

func (matcher *AppendLogMatcher) Matches(localPath string) bool {
	if matcher == nil {
		return false
	}
	cleaned, err := canonicalRuntimePolicyPath(localPath)
	if err != nil {
		return false
	}
	for _, pattern := range matcher.patterns {
		if pattern.MatchCanonical(cleaned) {
			return true
		}
	}
	return false
}

func NewLocalPolicy(profile string, localOnlyPatterns, localGitignoreAwarePatterns, remoteOnlyPatterns []string) *LocalPolicy {
	policy := &LocalPolicy{}
	if !profileAllowsLocalPolicy(profile) &&
		len(localOnlyPatterns) == 0 && len(localGitignoreAwarePatterns) == 0 && len(remoteOnlyPatterns) == 0 {
		return policy
	}

	policy.enabled = true
	localPatterns := append([]string{}, defaultCodingAgentLocalOnlyPatterns(profile)...)
	localPatterns = append(localPatterns, localOnlyPatterns...)
	policy.localOnly = pathfilter.CompileAll(localPatterns)
	gatedPatterns := append([]string{}, defaultCodingAgentGitignoreAwarePatterns(profile)...)
	gatedPatterns = append(gatedPatterns, localGitignoreAwarePatterns...)
	policy.localGitignoreAware = pathfilter.CompileAll(gatedPatterns)
	policy.remoteOnly = pathfilter.CompileAll(remoteOnlyPatterns)
	return policy
}

func validMountProfile(profile string) bool {
	return validMountProfileName(profile)
}

func profileAllowsLocalPolicy(profile string) bool {
	profile = strings.TrimSpace(profile)
	return profile != "" && profile != MountProfileInteractive && profile != MountProfileNone && profile != MountProfileExtent
}

func validMountProfileName(profile string) bool {
	profile = strings.TrimSpace(profile)
	if profile == "" {
		return true
	}
	if profile == "." || profile == ".." || strings.ContainsAny(profile, `/\`) {
		return false
	}
	for _, r := range profile {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '_' || r == '.':
		default:
			return false
		}
	}
	return true
}

// defaultCodingAgentLocalOnlyPatterns are overlaid unconditionally.
//
// VCS metadata is deliberately absent. `.git` is routed structurally: it is
// local only when the path belongs to a registered Git workspace or one that is
// being created (see gitDirShouldBeLocalOverlay). An ordinary clone's `.git`
// therefore syncs to the remote, and `drive9 git clone --fast` overlays its
// `.git` locally from the first write via a pending marker. `.hg`/`.svn` have no
// workspace coupling and always sync to the remote.
//
// Dependency trees are listed here: their names unambiguously denote generated
// trees, so there is no need to consult the repository.
func defaultCodingAgentLocalOnlyPatterns(profile string) []string {
	if profile != MountProfileCodingAgent {
		return nil
	}
	return []string{
		"**/node_modules/**",
		"**/.venv/**",
	}
}

// defaultCodingAgentGitignoreAwarePatterns are overlaid only when the
// repository's Git ignore rules also ignore them. Rust build output is listed
// here because `target` is a common directory name that is not always build
// output.
func defaultCodingAgentGitignoreAwarePatterns(profile string) []string {
	if profile != MountProfileCodingAgent {
		return nil
	}
	return []string{
		"**/target/**",
	}
}

func (policy *LocalPolicy) Enabled() bool {
	return policy != nil && policy.enabled
}

func (policy *LocalPolicy) Classify(localPath string) PathLayer {
	layer, _ := policy.classifyWithSource(localPath)
	return layer
}

func (policy *LocalPolicy) classifyWithSource(localPath string) (PathLayer, policyMatchSource) {
	if !policy.Enabled() {
		return PathLayerRemotePersistent, policyMatchDisabled
	}
	cleaned, err := canonicalRuntimePolicyPath(localPath)
	if err != nil {
		return PathLayerRemotePersistent, policyMatchRemoteDefault
	}
	for _, pattern := range policy.remoteOnly {
		if pattern.MatchCanonical(cleaned) {
			return PathLayerRemotePersistent, policyMatchRemoteOverride
		}
	}
	for _, pattern := range policy.localOnly {
		if pattern.MatchCanonical(cleaned) {
			return PathLayerLocalOnly, policyMatchLocalOnly
		}
	}
	for _, pattern := range policy.localGitignoreAware {
		if pattern.MatchCanonical(cleaned) {
			return PathLayerLocalOnly, policyMatchLocalGated
		}
	}
	return PathLayerRemotePersistent, policyMatchRemoteDefault
}

func (fs *Dat9FS) observePathPolicy(localPath string) PathLayer {
	return fs.observePathPolicyWithHint(context.TODO(), localPath, false)
}

func (fs *Dat9FS) observePathPolicyWithContext(ctx context.Context, localPath string) PathLayer {
	return fs.observePathPolicyWithHint(ctx, localPath, false)
}

func (fs *Dat9FS) observeDirPathPolicyWithContext(ctx context.Context, localPath string) PathLayer {
	return fs.observePathPolicyWithHint(ctx, localPath, true)
}

func (fs *Dat9FS) observePathPolicyWithHint(ctx context.Context, localPath string, dirHint bool) PathLayer {
	if fs == nil || fs.localPolicy == nil {
		return PathLayerRemotePersistent
	}
	layer, source := fs.localPolicy.classifyWithSource(localPath)
	if layer == PathLayerLocalOnly && source == policyMatchLocalGated {
		// A path matched by a gitignore-aware pattern is overlaid only when the
		// repository also ignores it. When no Git ignore oracle is available (no
		// loaded workspace) the pattern stands on its own.
		if confirmed, verified := fs.gitIgnoreConfirmsLocalOnly(ctx, localPath, dirHint); verified && !confirmed {
			layer = PathLayerRemotePersistent
			source = policyMatchRemoteDefault
		}
	}
	// `.git` belongs to a workspace, not to a static pattern: overlay it locally
	// when the path is a registered workspace's `.git`, or a workspace is being
	// created there (pending marker). An ordinary clone's `.git` stays remote.
	if layer == PathLayerRemotePersistent && source == policyMatchRemoteDefault && fs.gitDirShouldBeLocalOverlay(ctx, localPath) {
		layer = PathLayerLocalOnly
		source = policyMatchLocalOnly
	}
	if fs.perfEnabled() {
		fs.perf.recordLocalPolicy(source)
	}
	return layer
}

func canonicalRuntimePolicyPath(value string) (string, error) {
	return canonicalPolicyPath(value)
}

func canonicalPolicyPath(value string) (string, error) {
	// NOTE: deliberately do NOT TrimSpace here. Runtime paths keep their
	// surrounding whitespace so that a path like "/repo/node_modules " does NOT
	// match the **/node_modules/** pattern (whitespace is significant at the
	// runtime boundary). Pattern-side canonicalization trims; runtime-side
	// canonicalization does not.
	cleaned, err := pathutil.Canonicalize(value)
	if err != nil {
		return "", err
	}
	return strings.TrimPrefix(cleaned, "/"), nil
}

func validateLocalPolicyPatterns(localOnlyPatterns, localGitignoreAwarePatterns, remoteOnlyPatterns []string) error {
	return pathfilter.Validate(localOnlyPatterns, localGitignoreAwarePatterns, remoteOnlyPatterns)
}

func validateAppendLogPatterns(patterns []string) error {
	return pathfilter.Validate(patterns)
}
