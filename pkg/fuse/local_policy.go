package fuse

import (
	"path"
	"strings"
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
		"**/__pycache__/**",
		"**/.pytest_cache/**",
		"**/.mypy_cache/**",
		"**/.ruff_cache/**",
		"**/.next/cache/**",
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
	for _, pattern := range policy.remoteOnly {
		if pattern.matches(localPath) {
			return PathLayerRemotePersistent, policyMatchRemoteOverride
		}
	}
	for _, pattern := range policy.localOnly {
		if pattern.matches(localPath) {
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
		compiled = append(compiled, newLocalPolicyPattern(pattern))
	}
	return compiled
}

func newLocalPolicyPattern(raw string) localPolicyPattern {
	cleaned := cleanPolicyPath(raw)
	pattern := localPolicyPattern{raw: raw}
	if strings.HasPrefix(cleaned, "**/") {
		rest := strings.TrimPrefix(cleaned, "**/")
		rest = strings.TrimSuffix(rest, "/**")
		rest = strings.TrimSuffix(rest, "/")
		if rest != "" {
			pattern.subpath = splitPolicyPath(rest)
			return pattern
		}
	}
	if strings.HasSuffix(cleaned, "/**") {
		prefix := strings.TrimSuffix(cleaned, "/**")
		pattern.prefix = prefix
		return pattern
	}
	pattern.exact = cleaned
	return pattern
}

func (pattern localPolicyPattern) matches(localPath string) bool {
	cleaned := cleanPolicyPath(localPath)
	if len(pattern.subpath) > 0 {
		return containsSubpath(splitPolicyPath(cleaned), pattern.subpath)
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

func cleanPolicyPath(value string) string {
	value = strings.TrimSpace(strings.ReplaceAll(value, "\\", "/"))
	value = strings.TrimPrefix(value, "/")
	cleaned := path.Clean("/" + value)
	return strings.TrimPrefix(cleaned, "/")
}

func splitPolicyPath(value string) []string {
	value = cleanPolicyPath(value)
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
