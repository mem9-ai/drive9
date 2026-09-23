package fuse

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestZZL6(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list") != "" {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	items, gen, err := fs.lookupListWithRetry(nil, "/only")
	t.Logf("returned items=%d gen=%d err=%v", len(items), gen, err)
	t.Logf("inflight after return: %v", fs.dirCache.inFlight)
}
