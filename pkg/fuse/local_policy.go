package fuse

import (
	"fmt"
	"path"
	"strings"

	"github.com/mem9-ai/dat9/pkg/pathutil"
)

const (
	MountProfileInteractive = "interactive"
	MountProfileCodingAgent = "coding-agent"
)

type PathLayer string

const (
	PathLayerRemotePersistent PathLayer = "remote_persistent"
	PathLayerLocalOnly        PathLayer = "local_only"
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
	localOnly  []localPolicyPattern
	remoteOnly []localPolicyPattern
}

func NewLocalPolicy(profile string, localOnlyPatterns []string, remoteOnlyPatterns []string) *LocalPolicy {
	policy := &LocalPolicy{}
	if profile != MountProfileCodingAgent && len(localOnlyPatterns) == 0 && len(remoteOnlyPatterns) == 0 {
		return policy
	}

	policy.enabled = true
	localPatterns := append([]string{}, defaultCodingAgentLocalOnlyPatterns(profile)...)
	localPatterns = append(localPatterns, localOnlyPatterns...)
	policy.localOnly = compileLocalPolicyPatterns(localPatterns)
	policy.remoteOnly = compileLocalPolicyPatterns(remoteOnlyPatterns)
	return policy
}

func validMountProfile(profile string) bool {
	switch profile {
	case "", MountProfileInteractive, MountProfileCodingAgent:
		return true
	default:
		return false
	}
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
		"**/.next/cache/**",
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
		if pattern.matchesCanonical(cleaned) {
			return PathLayerRemotePersistent, policyMatchRemoteOverride
		}
	}
	for _, pattern := range policy.localOnly {
		if pattern.matchesCanonical(cleaned) {
			return PathLayerLocalOnly, policyMatchLocalOnly
		}
	}
	return PathLayerRemotePersistent, policyMatchRemoteDefault
}

func (fs *Dat9FS) observePathPolicy(localPath string) PathLayer {
	if fs == nil || fs.localPolicy == nil {
		return PathLayerRemotePersistent
	}
	layer, source := fs.localPolicy.classifyWithSource(localPath)
	if fs.perfEnabled() {
		fs.perf.recordLocalPolicy(source)
	}
	return layer
}

type localPolicyPattern struct {
	raw     string
	subpath []string
	prefix  string
	exact   string
}

func compileLocalPolicyPatterns(patterns []string) []localPolicyPattern {
	compiled := make([]localPolicyPattern, 0, len(patterns))
	for _, pattern := range patterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		compiledPattern, err := newLocalPolicyPattern(pattern)
		if err != nil {
			continue
		}
		compiled = append(compiled, compiledPattern)
	}
	return compiled
}

func validateLocalPolicyPatterns(localOnlyPatterns []string, remoteOnlyPatterns []string) error {
	for _, pattern := range append(append([]string{}, localOnlyPatterns...), remoteOnlyPatterns...) {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		if _, err := newLocalPolicyPattern(pattern); err != nil {
			return fmt.Errorf("invalid local policy pattern %q: %w", pattern, err)
		}
	}
	return nil
}

func newLocalPolicyPattern(raw string) (localPolicyPattern, error) {
	cleaned, err := canonicalPolicyPattern(raw)
	if err != nil {
		return localPolicyPattern{}, err
	}
	pattern := localPolicyPattern{raw: raw}
	if strings.HasPrefix(cleaned, "**/") {
		rest := strings.TrimPrefix(cleaned, "**/")
		rest = strings.TrimSuffix(rest, "/**")
		rest = strings.TrimSuffix(rest, "/")
		if rest != "" {
			pattern.subpath, err = splitPolicyPath(rest)
			if err != nil {
				return localPolicyPattern{}, err
			}
			return pattern, nil
		}
	}
	if strings.HasSuffix(cleaned, "/**") {
		prefix := strings.TrimSuffix(cleaned, "/**")
		pattern.prefix = prefix
		return pattern, nil
	}
	pattern.exact = cleaned
	return pattern, nil
}

func (pattern localPolicyPattern) matches(localPath string) bool {
	cleaned, err := canonicalRuntimePolicyPath(localPath)
	if err != nil {
		return false
	}
	return pattern.matchesCanonical(cleaned)
}

func (pattern localPolicyPattern) matchesCanonical(cleaned string) bool {
	if len(pattern.subpath) > 0 {
		segments := splitCanonicalPolicyPath(cleaned)
		return containsSubpath(segments, pattern.subpath)
	}
	if pattern.prefix != "" {
		return cleaned == pattern.prefix || strings.HasPrefix(cleaned, pattern.prefix+"/")
	}
	if pattern.exact != "" {
		if ok, err := path.Match(pattern.exact, cleaned); err == nil && ok {
			return true
		}
		return cleaned == pattern.exact
	}
	return false
}

func canonicalPolicyPattern(value string) (string, error) {
	value = strings.TrimSpace(value)
	return canonicalPolicyPath(value)
}

func canonicalRuntimePolicyPath(value string) (string, error) {
	return canonicalPolicyPath(value)
}

func canonicalPolicyPath(value string) (string, error) {
	cleaned, err := pathutil.Canonicalize(value)
	if err != nil {
		return "", err
	}
	return strings.TrimPrefix(cleaned, "/"), nil
}

func splitPolicyPath(value string) ([]string, error) {
	value, err := canonicalPolicyPattern(value)
	if err != nil {
		return nil, err
	}
	return splitCanonicalPolicyPath(value), nil
}

func splitRuntimePolicyPath(value string) ([]string, error) {
	value, err := canonicalRuntimePolicyPath(value)
	if err != nil {
		return nil, err
	}
	return splitCanonicalPolicyPath(value), nil
}

func splitCanonicalPolicyPath(value string) []string {
	if value == "" || value == "." {
		return nil
	}
	return strings.Split(value, "/")
}

func containsSubpath(segments []string, subpath []string) bool {
	if len(subpath) == 0 || len(segments) < len(subpath) {
		return false
	}
	for start := 0; start <= len(segments)-len(subpath); start++ {
		matched := true
		for offset := range subpath {
			if segments[start+offset] != subpath[offset] {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}
