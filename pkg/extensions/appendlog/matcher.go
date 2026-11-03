package appendlog

import (
	"strings"

	"github.com/mem9-ai/drive9/pkg/pathfilter"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

// AppendLogMatcher matches explicit append-log declarations: exact
// path-pattern match against the canonicalized path.
type AppendLogMatcher struct {
	patterns []pathfilter.Pattern
}

func NewAppendLogMatcher(patterns []string) *AppendLogMatcher {
	return &AppendLogMatcher{patterns: pathfilter.CompileAll(patterns)}
}

func (m *AppendLogMatcher) Matches(path string) bool {
	if m == nil {
		return false
	}
	cleaned, err := pathutil.Canonicalize(path)
	if err != nil {
		return false
	}
	// Runtime-side canonicalization keeps surrounding whitespace but drops
	// the leading slash, exactly like pkg/fuse's canonicalPolicyPath.
	cleaned = strings.TrimPrefix(cleaned, "/")
	for _, p := range m.patterns {
		if p.MatchCanonical(cleaned) {
			return true
		}
	}
	return false
}
