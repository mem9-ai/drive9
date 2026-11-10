package viewprotocol

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRoutePreservesAPIAndEscapedPaths(t *testing.T) {
	b := Binding{LayerID: "layer-a", WorkID: "w", Fence: 2}
	for _, path := range []string{"/v1/fs/a%2Fb%20c", "/v1/fs/%E6%96%87%E4%BB%B6", "/v1/fs:batch-stat", "/v2/uploads/u/complete"} {
		original := "http://example.invalid" + RoutePrefix + b.LayerID + path + "?q=a%2Fb"
		req := httptest.NewRequest(http.MethodPost, original, nil)
		_ = b.Apply(req.Header)
		out, got, err := ParseRoute(req)
		if err != nil || got != b {
			t.Fatalf("parse %+v %v", got, err)
		}
		if out.URL.EscapedPath() != path || out.URL.RawQuery != "q=a%2Fb" || out.RequestURI != path+"?q=a%2Fb" {
			t.Fatalf("route %s", out.URL.String())
		}
		if req.URL.String() != original {
			t.Fatal("mutated original request")
		}
		out.Header.Set(WorkHeader, "other")
		if req.Header.Get(WorkHeader) != "w" {
			t.Fatal("aliased headers")
		}
	}
}

func TestRouteRejectsMismatchMissingAndNestedBinding(t *testing.T) {
	for _, path := range []string{"/v1/fs/a", "/v1/views/b/v1/fs/a", "/v1/views/aa/v1/fs/a", "/v1/views/a/v3/fs/a", "/v1/views/%61/v1/fs/a", "/v1/views/a/v1/views/b/v1/fs/a"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		_ = (Binding{LayerID: "a", WorkID: "w", Fence: 1}).Apply(req.Header)
		if _, _, err := ParseRoute(req); err == nil {
			t.Fatalf("accepted %s", path)
		}
	}
	for _, h := range []http.Header{{}, {ViewHeader: []string{"main"}}, {ViewHeader: []string{"a"}}} {
		req := httptest.NewRequest(http.MethodGet, "/v1/views/a/v1/fs/file", nil)
		req.Header = h
		if _, _, err := ParseRoute(req); err == nil {
			t.Fatalf("accepted missing work %v", h)
		}
	}
}
