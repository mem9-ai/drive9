package cli

import (
	"github.com/mem9-ai/drive9/pkg/client"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSearchCommandsPassLayerParam(t *testing.T) {
	var grepLayer, findLayer string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/repo/" && r.URL.Query().Has("grep"):
			grepLayer = r.URL.Query().Get("layer")
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `[]`)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/fs/repo/" && r.URL.Query().Has("find"):
			findLayer = r.URL.Query().Get("layer")
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `[]`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := client.New(srv.URL, "")
	if err := Grep(c, []string{"--layer", "task=search", "needle", ":/repo/"}); err != nil {
		t.Fatalf("Grep --layer: %v", err)
	}
	if err := Find(c, []string{"--layer", "task=search", ":/repo/"}); err != nil {
		t.Fatalf("Find --layer: %v", err)
	}
	if grepLayer != "task=search" || findLayer != "task=search" {
		t.Fatalf("grepLayer=%q findLayer=%q, want task=search", grepLayer, findLayer)
	}
}
