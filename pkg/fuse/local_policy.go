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
	policyMatchRemoteOverride policyMatchSource = "remote_override"
)

type LocalPolicy struct {
	enabled bool
	// gitignoreAware requires a path matched by a local-only pattern to also be
	// ignored by its repository's Git ignore rules before it is overlaid. It
	// defaults to true; see MountOptions.LocalOnlyGitignoreAware.
	gitignoreAware bool
	localOnly      []pathfilter.Pattern
	remoteOnly     []pathfilter.Pattern
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

func NewLocalPolicy(profile string, localOnlyPatterns []string, remoteOnlyPatterns []string, gitignoreAware bool) *LocalPolicy {
	policy := &LocalPolicy{gitignoreAware: gitignoreAware}
	if !profileAllowsLocalPolicy(profile) && len(localOnlyPatterns) == 0 && len(remoteOnlyPatterns) == 0 {
		return policy
	}

	policy.enabled = true
	localPatterns := append([]string{}, defaultCodingAgentLocalOnlyPatterns(profile)...)
	localPatterns = append(localPatterns, localOnlyPatterns...)
	policy.localOnly = pathfilter.CompileAll(localPatterns)
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

func defaultCodingAgentLocalOnlyPatterns(profile string) []string {
	if profile != MountProfileCodingAgent {
		return nil
	}
	// Only dependency and heavy build-output trees are listed: Node
	// dependencies, Python virtualenvs, and Rust build output. By default the
	// gitignore-aware gate (see LocalPolicy.gitignoreAware) further requires
	// each of these paths to be ignored by the repository, so a directory that
	// merely shares a name with generated output is not overlaid. VCS metadata
	// is deliberately absent: `.git` state is kept local only inside a Git
	// workspace, and is remote-persistent elsewhere.
	return []string{
		"**/node_modules/**",
		"**/.venv/**",
		"**/target/**",
	}
}

func (policy *LocalPolicy) Enabled() bool {
	return policy != nil && policy.enabled
}

// GitignoreAware reports whether local-only pattern matches require
// confirmation from the repository's Git ignore rules before being overlaid.
func (policy *LocalPolicy) GitignoreAware() bool {
	return policy != nil && policy.enabled && policy.gitignoreAware
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
	if layer == PathLayerLocalOnly && source == policyMatchLocalOnly && fs.localPolicy.GitignoreAware() {
		// A local-only pattern is honored only when the repository also ignores
		// the path. When no Git ignore oracle is available (no loaded
		// workspace) the pattern stands on its own.
		if confirmed, verified := fs.gitIgnoreConfirmsLocalOnly(ctx, localPath, dirHint); verified && !confirmed {
			layer = PathLayerRemotePersistent
			source = policyMatchRemoteDefault
		}
	}
	if layer == PathLayerRemotePersistent && source == policyMatchRemoteDefault && fs.gitWorkspaceGitDirLocalOnly(ctx, localPath) {
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

func validateLocalPolicyPatterns(localOnlyPatterns []string, remoteOnlyPatterns []string) error {
	return pathfilter.Validate(localOnlyPatterns, remoteOnlyPatterns)
}

func validateAppendLogPatterns(patterns []string) error {
	return pathfilter.Validate(patterns)
}
