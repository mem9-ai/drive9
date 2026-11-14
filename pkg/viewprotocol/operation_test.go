package viewprotocol

import (
	"net/http"
	"strings"
	"testing"
)

func TestOperationIDSingleBoundedIdentity(t *testing.T) {
	for _, id := range []string{"a", "mutation-1_2", strings.Repeat("x", 64)} {
		h := http.Header{}
		h.Set(OperationHeader, id)
		got, err := ParseOperationID(h)
		if err != nil || got != id {
			t.Fatal(got, err)
		}
	}
	for _, values := range [][]string{nil, {""}, {"a", "a"}, {"a", "b"}, {"a b"}, {"a/b"}, {" a"}, {strings.Repeat("x", 65)}} {
		h := http.Header{}
		for _, v := range values {
			h.Add(OperationHeader, v)
		}
		if _, err := ParseOperationID(h); err == nil {
			t.Fatal(values)
		}
	}
}
