package datastore

import (
	"reflect"
	"strings"
	"testing"
)

func TestBuildVectorSearchQueryIncludesCurrentRevisionFilter(t *testing.T) {
	q, args, ok := buildVectorSearchQuery([]float32{0.1, 0.2, 0.3}, "/docs/", 7)
	if !ok {
		t.Fatal("expected non-empty query embedding to build vector search SQL")
	}
	if !strings.Contains(q, "s.embedding_revision = i.revision") {
		t.Fatalf("vector search SQL missing current-revision filter: %s", q)
	}
	if !strings.Contains(q, "VEC_EMBED_COSINE_DISTANCE(s.embedding, ?)") {
		t.Fatalf("vector search SQL missing vector-distance placeholder: %s", q)
	}
	wantArgs := []any{"[0.1,0.2,0.3]", "/docs", "/docs/%", "[0.1,0.2,0.3]", 7}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("vector search args=%#v, want %#v", args, wantArgs)
	}
}

func TestBuildVectorSearchQueryWithoutPathPrefix(t *testing.T) {
	q, args, ok := buildVectorSearchQuery([]float32{0.5, 0.4}, "/", 3)
	if !ok {
		t.Fatal("expected non-empty query embedding to build vector search SQL")
	}
	if strings.Contains(q, "fn.path = ? OR fn.path LIKE ?") {
		t.Fatalf("root path should not add subtree filter: %s", q)
	}
	wantArgs := []any{"[0.5,0.4]", "[0.5,0.4]", 3}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("vector search args=%#v, want %#v", args, wantArgs)
	}
}

func TestBuildVectorSearchQuerySkipsEmptyEmbedding(t *testing.T) {
	q, args, ok := buildVectorSearchQuery(nil, "/docs", 5)
	if ok {
		t.Fatalf("expected empty query embedding to short-circuit, got query=%q args=%#v", q, args)
	}
}

func TestBuildVectorSearchByTextQueryOmitsRevisionFilter(t *testing.T) {
	q, args, ok := buildVectorSearchByTextQuery("semantic probe", "/docs/", 7)
	if !ok {
		t.Fatal("expected non-empty query text to build vector search SQL")
	}
	if strings.Contains(q, "s.embedding_revision = i.revision") {
		t.Fatalf("text-query vector SQL should not depend on embedding_revision: %s", q)
	}
	if !strings.Contains(q, "VEC_EMBED_COSINE_DISTANCE(s.embedding, ?)") {
		t.Fatalf("text-query vector SQL missing distance expression: %s", q)
	}
	if !strings.Contains(q, "ORDER BY distance") {
		t.Fatalf("text-query vector SQL should order by projected alias: %s", q)
	}
	wantArgs := []any{"semantic probe", "/docs", "/docs/%", 7}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("text-query vector args=%#v, want %#v", args, wantArgs)
	}
}

func TestBuildVectorSearchByTextQuerySkipsEmptyQuery(t *testing.T) {
	q, args, ok := buildVectorSearchByTextQuery("   ", "/docs", 5)
	if ok {
		t.Fatalf("expected empty query text to short-circuit, got query=%q args=%#v", q, args)
	}
}

// The full-text search text is quoted into the statement rather than bound (the TiDB
// full-text planner reads it from the function argument while it plans), so the quoting
// itself is the security boundary. It must keep every input inside one string literal and
// must not silently rewrite what the user searched for.
func TestFTSQueryLiteralEscapesWithoutRewriting(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
		want  string
	}{
		{name: "plain", query: "needle", want: `'needle'`},
		{name: "apostrophe", query: "it's", want: `'it''s'`},
		{name: "quote then statement", query: "'; DROP TABLE file_nodes; --", want: `'''; DROP TABLE file_nodes; --'`},
		{name: "backslash", query: `a\`, want: `'a\\'`},
		{name: "backslash before quote", query: `a\'b`, want: `'a\\''b'`},
		{name: "comment markers stay searchable", query: "a--b/*c*/", want: `'a--b/*c*/'`},
		{name: "semicolon stays searchable", query: "a;b", want: `'a;b'`},
		{name: "unicode", query: "蓝牙耳机", want: `'蓝牙耳机'`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ftsQueryLiteral(tc.query, false)
			if err != nil {
				t.Fatalf("ftsQueryLiteral(%q): %v", tc.query, err)
			}
			if got != tc.want {
				t.Fatalf("ftsQueryLiteral(%q) = %s, want %s", tc.query, got, tc.want)
			}
			if !closesAsOneLiteral(got, false) {
				t.Fatalf("ftsQueryLiteral(%q) = %s does not render as a single closed literal", tc.query, got)
			}
		})
	}
	// The replay must reject the shapes a broken quoting implementation would produce, or it
	// would green-light the very literals it is meant to catch.
	for _, broken := range []string{`'a''`, `'a''b' OR 1=1 -- '`, `'\'' OR 1=1 -- '`, `'a\'`, `noquote`, `'`} {
		if closesAsOneLiteral(broken, false) {
			t.Fatalf("%q was accepted as one closed literal", broken)
		}
	}
	for _, good := range []string{`''`, `'a''b'`, `'a\\'`, `'a\\''b'`} {
		if !closesAsOneLiteral(good, false) {
			t.Fatalf("%q was rejected as a closed literal", good)
		}
	}
	// Mode matters for a trailing backslash: with escapes on it swallows the closing quote,
	// with NO_BACKSLASH_ESCAPES it is data and the literal closes.
	if closesAsOneLiteral(`'a\'`, false) {
		t.Fatal("a backslash-escaped quote was accepted as closing the literal")
	}
	if !closesAsOneLiteral(`'a\'`, true) {
		t.Fatal("under NO_BACKSLASH_ESCAPES a trailing backslash is data, so the literal closes")
	}
	if _, err := ftsQueryLiteral("a\x00b", false); err == nil || !strings.Contains(err.Error(), "NUL") {
		t.Fatalf("ftsQueryLiteral with a NUL byte error = %v, want a refusal", err)
	}

	// Under NO_BACKSLASH_ESCAPES a backslash is an ordinary character, so doubling it would
	// search for two. The literal has to denote exactly what the user typed in both modes.
	withBackslash := `C:\path\to`
	if got, want := mustFTSLiteral(t, withBackslash, true), `'C:\path\to'`; got != want {
		t.Fatalf("NO_BACKSLASH_ESCAPES literal = %s, want %s", got, want)
	}
	if got, want := mustFTSLiteral(t, withBackslash, false), `'C:\\path\\to'`; got != want {
		t.Fatalf("default-mode literal = %s, want %s", got, want)
	}
	// Quote escaping does not depend on the mode, which is why it is the delimiter rule.
	if got, want := mustFTSLiteral(t, "it's", true), "'it''s'"; got != want {
		t.Fatalf("NO_BACKSLASH_ESCAPES quote literal = %s, want %s", got, want)
	}
}

// closesAsOneLiteral replays the parser's view of a single-quoted MySQL literal. It must open
// with the delimiter and read to the end without being closed early: `''` and, in the default
// mode, `\` followed by any byte are escapes, so an interior quote that is neither doubled
// nor backslash-escaped ends the literal — the breakout this guards against.
func closesAsOneLiteral(literal string, noBackslashEscapes bool) bool {
	if literal == "" || literal[0] != '\'' {
		return false
	}
	closed := false
	for i := 1; i < len(literal); i++ {
		switch literal[i] {
		case '\\':
			if noBackslashEscapes {
				continue
			}
			// An escape consumes the next byte, so a quote after it is data.
			i++
		case '\'':
			if i+1 < len(literal) && literal[i+1] == '\'' {
				i++
				continue
			}
			closed = i == len(literal)-1
			i = len(literal)
		}
	}
	return closed
}

func TestBuildFTSSearchQueryInlinesTheQuotedQuery(t *testing.T) {
	literal, err := ftsQueryLiteral("it's 50% off -- really", false)
	if err != nil {
		t.Fatal(err)
	}
	q, args := buildFTSSearchQuery(StandaloneScope(), literal, "/docs/", 5)
	for _, want := range []string{
		"fts_match_word('it''s 50% off -- really', content_text)",
		"fts_match_word('it''s 50% off -- really', description)",
	} {
		if !strings.Contains(q, want) {
			t.Fatalf("full-text search SQL missing %q: %s", want, q)
		}
	}
	if got, want := strings.Count(q, "?"), len(args); got != want {
		t.Fatalf("placeholders = %d, args = %d, want one arg per placeholder", got, want)
	}
	for _, arg := range args {
		if text, ok := arg.(string); ok && strings.Contains(text, "it's") {
			t.Fatalf("search text %q is bound as an argument as well as quoted into the statement", text)
		}
	}
	wantArgs := []any{5, "/docs", "/docs/%"}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("args = %#v, want the limit and then the path subtree", args)
	}
}

// mustFTSLiteral renders a literal or fails the test.
func mustFTSLiteral(t *testing.T, query string, noBackslashEscapes bool) string {
	t.Helper()
	literal, err := ftsQueryLiteral(query, noBackslashEscapes)
	if err != nil {
		t.Fatalf("ftsQueryLiteral(%q, %v): %v", query, noBackslashEscapes, err)
	}
	return literal
}
