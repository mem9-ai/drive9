// Package pathfilter provides reusable path-pattern matching for archive and
// sync-style operations. It implements the same three pattern forms used by the
// FUSE local-policy layer (**/x/**, prefix/**, and exact/glob), exposed as a
// bidirectional include/exclude Matcher so non-FUSE callers (CLI archive, TS
// parity tests) can share one matching semantics without importing pkg/fuse.
package pathfilter

import (
	"fmt"
	"path"
	"strings"

	"github.com/mem9-ai/drive9/pkg/pathutil"
)

// Pattern is a compiled path-filter pattern. The zero value never matches.
type Pattern struct {
	raw     string
	subpath []string
	prefix  string
	exact   string
}

// Compile parses a single pattern. Empty/whitespace patterns are rejected.
func Compile(raw string) (Pattern, error) {
	cleaned, err := canonical(raw)
	if err != nil {
		return Pattern{}, err
	}
	p := Pattern{raw: raw}
	if strings.HasPrefix(cleaned, "**/") {
		rest := strings.TrimPrefix(cleaned, "**/")
		rest = strings.TrimSuffix(rest, "/**")
		rest = strings.TrimSuffix(rest, "/")
		if rest != "" {
			p.subpath, err = splitPath(rest)
			if err != nil {
				return Pattern{}, err
			}
			return p, nil
		}
	}
	if strings.HasSuffix(cleaned, "/**") {
		p.prefix = strings.TrimSuffix(cleaned, "/**")
		return p, nil
	}
	p.exact = cleaned
	return p, nil
}

// CompileAll compiles a list of patterns, skipping blank entries. Invalid
// patterns are silently dropped (mirrors the FUSE layer's tolerant compile
// path); use Validate to surface invalid patterns at a CLI boundary.
func CompileAll(patterns []string) []Pattern {
	compiled := make([]Pattern, 0, len(patterns))
	for _, raw := range patterns {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		p, err := Compile(raw)
		if err != nil {
			continue
		}
		compiled = append(compiled, p)
	}
	return compiled
}

// Validate reports the first invalid pattern among the given lists. Intended
// for CLI flag validation where the user should be told which pattern is bad
// rather than silently filtered out.
func Validate(patternLists ...[]string) error {
	for _, list := range patternLists {
		for _, raw := range list {
			raw = strings.TrimSpace(raw)
			if raw == "" {
				continue
			}
			if _, err := Compile(raw); err != nil {
				return fmt.Errorf("invalid pattern %q: %w", raw, err)
			}
		}
	}
	return nil
}

// Raw returns the original pattern string.
func (p Pattern) Raw() string { return p.raw }

// Match reports whether the given canonicalized path matches this pattern.
// The input is canonicalized internally so callers may pass slash-separated
// relative or absolute paths interchangeably. Whitespace in the input is
// trimmed before matching (matching the pattern-side canonicalization
// semantics used by the FUSE local-policy layer when compiling patterns).
func (p Pattern) Match(value string) bool {
	cleaned, err := canonical(value)
	if err != nil {
		return false
	}
	return p.matchCanonical(cleaned)
}

// MatchCanonical reports whether the already-canonicalized path matches this
// pattern. It does NOT re-canonicalize, so callers that have already
// canonicalized (and want to preserve whitespace significance, as the FUSE
// runtime path classifier does) can avoid the double-trim that Match would
// perform. The FUSE local-policy layer uses this entry point.
func (p Pattern) MatchCanonical(cleaned string) bool {
	return p.matchCanonical(cleaned)
}

func (p Pattern) matchCanonical(cleaned string) bool {
	if len(p.subpath) > 0 {
		segments := splitCanonicalPath(cleaned)
		return containsSubpath(segments, p.subpath)
	}
	if p.prefix != "" {
		return cleaned == p.prefix || strings.HasPrefix(cleaned, p.prefix+"/")
	}
	if p.exact != "" {
		if ok, err := path.Match(p.exact, cleaned); err == nil && ok {
			return true
		}
		return cleaned == p.exact
	}
	return false
}

// Matcher is the bidirectional include/exclude filter used by archive and
// similar bulk operations.
//
// Semantics:
//
//	Match(path) = (len(Include) == 0 || matchesAny(Include, path)) && !matchesAny(Exclude, path)
//
// Callers needing profile "remote-only" override semantics (where an override
// restores a path that an exclude would have dropped) should use Override
// explicitly: see Matcher.MatchWithOverride.
type Matcher struct {
	Include []Pattern
	Exclude []Pattern
	// Override patterns restore a path that Exclude would otherwise drop.
	// Only populated from profile [remote] rules; CLI flags do not set it.
	Override []Pattern
}

// NewMatcher compiles include and exclude pattern lists into a Matcher.
// includePatterns may be nil (meaning "match everything"); excludePatterns
// and overridePatterns are optional.
func NewMatcher(includePatterns, excludePatterns, overridePatterns []string) Matcher {
	return Matcher{
		Include:  CompileAll(includePatterns),
		Exclude:  CompileAll(excludePatterns),
		Override: CompileAll(overridePatterns),
	}
}

// Match reports whether a path should be included in the archive.
//
//  1. If Override contains a matching pattern → include (true).
//  2. Else if Exclude contains a matching pattern → drop (false).
//  3. Else if Include is non-empty and no include pattern matches → drop (false).
//  4. Otherwise → include (true).
func (m Matcher) Match(path string) bool {
	if len(m.Override) > 0 && matchesAny(m.Override, path) {
		return true
	}
	if matchesAny(m.Exclude, path) {
		return false
	}
	if len(m.Include) > 0 && !matchesAny(m.Include, path) {
		return false
	}
	return true
}

// HasInclude reports whether the matcher has a non-empty include whitelist.
// Used by callers to decide whether directory pruning is safe (an include
// whitelist that matches a parent does not imply all children match).
func (m Matcher) HasInclude() bool { return len(m.Include) > 0 }

// HasExclude reports whether the matcher has any exclude patterns.
func (m Matcher) HasExclude() bool { return len(m.Exclude) > 0 }

// MatchExcluded reports whether a path is dropped by an exclude pattern that
// no override restores. This is the "should this directory's subtree be
// pruned" predicate: a directory pruned by MatchExcluded means every
// descendant is guaranteed to be dropped too (exclude is subtree-inheritable
// unless override restores it). A directory that MatchExcluded returns false
// for may still fail the include whitelist at Match() — but its children
// must be walked because include matches leaf files, not necessarily their
// parent directories.
func (m Matcher) MatchExcluded(path string) bool {
	if len(m.Override) > 0 && matchesAny(m.Override, path) {
		return false
	}
	return matchesAny(m.Exclude, path)
}

func matchesAny(patterns []Pattern, path string) bool {
	for _, p := range patterns {
		if p.Match(path) {
			return true
		}
	}
	return false
}

func canonical(value string) (string, error) {
	value = strings.TrimSpace(value)
	cleaned, err := pathutil.Canonicalize(value)
	if err != nil {
		return "", err
	}
	return strings.TrimPrefix(cleaned, "/"), nil
}

func splitPath(value string) ([]string, error) {
	cleaned, err := canonical(value)
	if err != nil {
		return nil, err
	}
	return splitCanonicalPath(cleaned), nil
}

func splitCanonicalPath(value string) []string {
	if value == "" || value == "." {
		return nil
	}
	return strings.Split(value, "/")
}

func containsSubpath(segments, subpath []string) bool {
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
