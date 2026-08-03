package fuse

import "testing"

func TestEscapeLogControlWhitespace(t *testing.T) {
	got := escapeLogControlWhitespace("/line\nbreak\tname\r.txt")
	if got != `/line\nbreak\tname\r.txt` {
		t.Fatalf("escaped path = %q, want %q", got, `/line\nbreak\tname\r.txt`)
	}
}
