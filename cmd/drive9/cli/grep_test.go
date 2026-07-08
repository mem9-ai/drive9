package cli

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
)

// newGrepTestServer returns an httptest.Server that responds to grep
// requests with the given JSON body. It also captures the layer query param
// via the provided pointer (left untouched if nil).
func newGrepTestServer(t *testing.T, responseBody string, layerPtr *string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("grep") {
			if layerPtr != nil {
				*layerPtr = r.URL.Query().Get("layer")
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, responseBody)
			return
		}
		http.NotFound(w, r)
	}))
}

func TestGrepJSONEmptyResultsOutputsArray(t *testing.T) {
	srv := newGrepTestServer(t, `[]`, nil)
	defer srv.Close()

	c := client.New(srv.URL, "")
	out, err := captureStdoutE(t, func() error {
		return Grep(c, []string{"--json", "needle", ":/repo/"})
	})
	if err != nil {
		t.Fatalf("Grep --json: %v", err)
	}

	if got := strings.TrimSpace(out); got != "[]" {
		t.Fatalf("got %q, want []", got)
	}
}

func TestGrepJSONOutputsResults(t *testing.T) {
	srv := newGrepTestServer(t, `[{"path":"/repo/a.txt","name":"a.txt","size_bytes":31,"score":0.03}]`, nil)
	defer srv.Close()

	c := client.New(srv.URL, "")
	out, err := captureStdoutE(t, func() error {
		return Grep(c, []string{"--json", "needle", ":/repo/"})
	})
	if err != nil {
		t.Fatalf("Grep --json: %v", err)
	}

	var results []client.SearchResult
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &results); err != nil {
		t.Fatalf("decode JSON output: %v\nraw: %s", err, out)
	}
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	if results[0].Path != "/repo/a.txt" {
		t.Fatalf("got path %q, want /repo/a.txt", results[0].Path)
	}
	if results[0].Score == nil || *results[0].Score != 0.03 {
		t.Fatalf("got score %v, want 0.03", results[0].Score)
	}
}

func TestGrepTextOutputWithoutJSON(t *testing.T) {
	srv := newGrepTestServer(t, `[{"path":"/repo/a.txt","name":"a.txt","size_bytes":31,"score":0.03}]`, nil)
	defer srv.Close()

	c := client.New(srv.URL, "")
	out, err := captureStdoutE(t, func() error {
		return Grep(c, []string{"needle", ":/repo/"})
	})
	if err != nil {
		t.Fatalf("Grep: %v", err)
	}

	if got := strings.TrimSpace(out); got != "/repo/a.txt\t0.03" {
		t.Fatalf("got %q, want /repo/a.txt\\t0.03", got)
	}
}

func TestGrepJSONWithLayer(t *testing.T) {
	var capturedLayer string
	srv := newGrepTestServer(t, `[]`, &capturedLayer)
	defer srv.Close()

	c := client.New(srv.URL, "")
	out, err := captureStdoutE(t, func() error {
		return Grep(c, []string{"--json", "--layer", "task=search", "needle", ":/repo/"})
	})
	if err != nil {
		t.Fatalf("Grep --json --layer: %v", err)
	}

	if capturedLayer != "task=search" {
		t.Fatalf("layer param = %q, want task=search", capturedLayer)
	}
	if got := strings.TrimSpace(out); got != "[]" {
		t.Fatalf("got %q, want []", got)
	}
}