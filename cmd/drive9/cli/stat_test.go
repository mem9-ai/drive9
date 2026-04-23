package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/dat9/pkg/client"
)

func TestStatDefaultOutputIncludesMetadataFields(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/fs/meta.txt" || !r.URL.Query().Has("stat") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"size":          12,
			"isdir":         false,
			"revision":      3,
			"mtime":         1700000000,
			"content_type":  "text/plain",
			"semantic_text": "hello world",
			"tags": map[string]string{
				"topic": "memo",
				"owner": "alice",
			},
		})
	}))
	defer srv.Close()

	c := client.New(srv.URL, "")
	out, err := captureStdoutE(t, func() error { return Stat(c, []string{":/meta.txt"}) })
	if err != nil {
		t.Fatalf("Stat(default): %v", err)
	}
	if !strings.Contains(out, "size: 12\n") {
		t.Fatalf("output missing size: %q", out)
	}
	if !strings.Contains(out, "semantic_text: hello world\n") {
		t.Fatalf("output missing semantic_text: %q", out)
	}
	if !strings.Contains(out, "mtime: 2023-11-14T22:13:20Z\n") {
		t.Fatalf("output missing RFC3339 mtime: %q", out)
	}
	ownerIdx := strings.Index(out, "tags.owner: alice\n")
	topicIdx := strings.Index(out, "tags.topic: memo\n")
	if ownerIdx < 0 || topicIdx < 0 {
		t.Fatalf("output missing tags: %q", out)
	}
	if ownerIdx > topicIdx {
		t.Fatalf("tags are not sorted by key: %q", out)
	}
}

func TestStatJSONOutput(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/fs/doc.txt" || !r.URL.Query().Has("stat") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"size":          5,
			"isdir":         false,
			"revision":      1,
			"content_type":  "text/plain",
			"semantic_text": "hello",
			"tags": map[string]string{
				"k": "v",
			},
		})
	}))
	defer srv.Close()

	c := client.New(srv.URL, "")
	out, err := captureStdoutE(t, func() error { return Stat(c, []string{"-o", "json", "/doc.txt"}) })
	if err != nil {
		t.Fatalf("Stat(-o json): %v", err)
	}
	var got client.StatMetadataResult
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("json.Unmarshal output: %v\noutput=%q", err, out)
	}
	if got.Size != 5 || got.Revision != 1 || got.Tags["k"] != "v" {
		t.Fatalf("unexpected json output: %+v", got)
	}
}

func TestStatJSONOutputLongFlag(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/fs/doc.txt" || !r.URL.Query().Has("stat") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"size":          5,
			"isdir":         false,
			"revision":      1,
			"content_type":  "text/plain",
			"semantic_text": "hello",
			"tags":          map[string]string{"k": "v"},
		})
	}))
	defer srv.Close()

	c := client.New(srv.URL, "")
	out, err := captureStdoutE(t, func() error { return Stat(c, []string{"--output", "json", "/doc.txt"}) })
	if err != nil {
		t.Fatalf("Stat(--output json): %v", err)
	}
	var got client.StatMetadataResult
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("json.Unmarshal output: %v\noutput=%q", err, out)
	}
	if got.Size != 5 || got.Revision != 1 || got.Tags["k"] != "v" {
		t.Fatalf("unexpected json output: %+v", got)
	}
}

func TestStatFallsBackToLegacyHead(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/legacy.txt" && r.URL.Query().Has("stat"):
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write([]byte("legacy stat body"))
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/legacy.txt":
			w.Header().Set("Content-Length", "9")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "11")
			w.Header().Set("X-Dat9-Mtime", "1700000123")
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := client.New(srv.URL, "")
	out, err := captureStdoutE(t, func() error { return Stat(c, []string{"/legacy.txt"}) })
	if err != nil {
		t.Fatalf("Stat(fallback): %v", err)
	}
	if !strings.Contains(out, "size: 9\n") {
		t.Fatalf("fallback output missing size: %q", out)
	}
	if !strings.Contains(out, "revision: 11\n") {
		t.Fatalf("fallback output missing revision: %q", out)
	}
	if !strings.Contains(out, "mtime: 2023-11-14T22:15:23Z\n") {
		t.Fatalf("fallback output missing RFC3339 mtime: %q", out)
	}
	if !strings.Contains(out, "degraded: true\n") {
		t.Fatalf("fallback output missing degraded marker: %q", out)
	}
}

func TestStatRejectsUnknownFlag(t *testing.T) {
	c := client.New("http://example.invalid", "")
	err := Stat(c, []string{"--verbose", "/x.txt"})
	if err == nil {
		t.Fatal("expected usage error")
	}
	if !strings.Contains(err.Error(), "usage: drive9 fs stat [-o text|json] <path>") {
		t.Fatalf("error = %q, want usage", err)
	}
}

func TestStatRejectsUnsupportedOutputFormat(t *testing.T) {
	c := client.New("http://example.invalid", "")
	err := Stat(c, []string{"--output", "yaml", "/x.txt"})
	if err == nil {
		t.Fatal("expected output format error")
	}
	if !strings.Contains(err.Error(), `unsupported output format "yaml"`) {
		t.Fatalf("error = %q, want output format rejection", err)
	}
}
