package cli

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestFindRejectsExplicitLayerBeforeSearch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected search request %s", r.URL.Path)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	for _, args := range [][]string{{"--layer="}, {"--layer", " "}, {"--layer", "work"}, {"--layer=work"}} {
		if err := Find(client.New(srv.URL, ""), args); !errors.Is(err, viewprotocol.ErrUnsupportedInLayerView) {
			t.Errorf("Find(%q) got %v", args, err)
		}
	}
}
