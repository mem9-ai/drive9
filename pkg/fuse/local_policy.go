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
	enabled    bool
	localOnly  []pathfilter.Pattern
	remoteOnly []pathfilter.Pattern
}

func NewLocalPolicy(profile string, localOnlyPatterns []string, remoteOnlyPatterns []string) *LocalPolicy {
	policy := &LocalPolicy{}
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
	return profile != "" && profile != MountProfileInteractive && profile != MountProfileNone
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
	return []string{
		"**/.git/**",
		"**/.hg/**",
		"**/.svn/**",
		"**/node_modules/**",
		"**/.pnpm-store/**",
		"**/target/**",
		"**/dist/**",
		"**/build/**",
		"**/coverage/**",
		"**/tmp/**",
		"**/.tmp/**",
		"**/.tmp-api-extractor/**",
		"**/.cache/**",
		"**/.turbo/**",
		"**/.next/cache/**",
		"**/.vitepress/cache/**",
		"**/.gradle/**",
		"**/.venv/**",
		"**/__pycache__/**",
		"**/.pytest_cache/**",
		"**/.mypy_cache/**",
		"**/.ruff_cache/**",
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
	if layer == PathLayerRemotePersistent && source == policyMatchRemoteDefault && fs.gitIgnoredPathLocalOnly(ctx, localPath, dirHint) {
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
	// surrounding whitespace so that a path like "/repo/.git " does NOT match
	// the **/.git/** pattern (whitespace is significant at the runtime boundary).
	// Pattern-side canonicalization trims; runtime-side canonicalization does not.
	cleaned, err := pathutil.Canonicalize(value)
	if err != nil {
		return "", err
	}
	return strings.TrimPrefix(cleaned, "/"), nil
}

func validateLocalPolicyPatterns(localOnlyPatterns []string, remoteOnlyPatterns []string) error {
	return pathfilter.Validate(localOnlyPatterns, remoteOnlyPatterns)
}
