package cli

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mem9-ai/dat9/pkg/client"
)

func TestGrepTreatsEscapedDashPrefixedPatternAsData(t *testing.T) {
	var gotQuery, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query().Get("grep")
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	if err := Grep(client.New(srv.URL, ""), []string{"--", "--help", "/docs"}); err != nil {
		t.Fatalf("Grep(-- --help /docs): %v", err)
	}
	if gotQuery != "--help" || gotPath != "/v1/fs/docs" {
		t.Fatalf("query/path = %q %q, want %q %q", gotQuery, gotPath, "--help", "/v1/fs/docs")
	}
}

func TestFindTreatsEscapedDashPrefixedFilterValueAsData(t *testing.T) {
	var gotName, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotName = r.URL.Query().Get("name")
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	if err := Find(client.New(srv.URL, ""), []string{"-name", "--", "--help", "/docs"}); err != nil {
		t.Fatalf("Find(-name -- --help /docs): %v", err)
	}
	if gotName != "--help" || gotPath != "/v1/fs/docs" {
		t.Fatalf("name/path = %q %q, want %q %q", gotName, gotPath, "--help", "/v1/fs/docs")
	}
}

func TestFindFilterValuesCanBeHelpLike(t *testing.T) {
	var gotName, gotTag, gotNewer, gotOlder, gotMaxSize, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotName = r.URL.Query().Get("name")
		gotTag = r.URL.Query().Get("tag")
		gotNewer = r.URL.Query().Get("newer")
		gotOlder = r.URL.Query().Get("older")
		gotMaxSize = r.URL.Query().Get("maxsize")
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	err := Find(client.New(srv.URL, ""), []string{
		"/docs",
		"-name", "--help",
		"-tag", "-h",
		"-newer", "-help",
		"-older", "--help",
		"-size", "-h",
	})
	if err != nil {
		t.Fatalf("Find(help-like filter values): %v", err)
	}
	if gotName != "--help" {
		t.Fatalf("name = %q, want %q", gotName, "--help")
	}
	if gotTag != "-h" {
		t.Fatalf("tag = %q, want %q", gotTag, "-h")
	}
	if gotNewer != "-help" {
		t.Fatalf("newer = %q, want %q", gotNewer, "-help")
	}
	if gotOlder != "--help" {
		t.Fatalf("older = %q, want %q", gotOlder, "--help")
	}
	if gotMaxSize != "h" {
		t.Fatalf("maxsize = %q, want %q", gotMaxSize, "h")
	}
	if gotPath != "/v1/fs/docs" {
		t.Fatalf("path = %q, want %q", gotPath, "/v1/fs/docs")
	}
}
