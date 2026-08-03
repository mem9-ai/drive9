package fuse

import (
	"bytes"
	"errors"
	"log"
	"testing"
)

type testLogStringer string

func (s testLogStringer) String() string { return string(s) }

func TestEscapeLogControlWhitespace(t *testing.T) {
	got := escapeLogControlWhitespace("/line\nbreak\tname\r.txt")
	if got != `/line\nbreak\tname\r.txt` {
		t.Errorf("escaped path = %q, want %q", got, `/line\nbreak\tname\r.txt`)
	}
}

func TestSafeLogPrintfEscapesErrorAndStringer(t *testing.T) {
	var buf bytes.Buffer
	oldWriter := log.Writer()
	oldFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
	})

	safeLogPrintf("error=%v string=%s", errors.New("err\nline\r\t"), testLogStringer("value\nline\r\t"))
	if got, want := buf.String(), "error=err\\nline\\r\\t string=value\\nline\\r\\t\n"; got != want {
		t.Errorf("log output = %q, want %q", got, want)
	}
}
