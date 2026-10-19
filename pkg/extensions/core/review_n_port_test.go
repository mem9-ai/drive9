package core

// codex twenty-third round: an HTTP 401 resets the mount view before the
// caller sees EACCES (the credential may now point at another tenant).

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func TestPortUnauthorizedResetsView(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello"))
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/f") {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	bus := fs.NewEventBus()
	var resets int32
	bus.On(fs.EventCheckout, func(fs.Event) { atomic.AddInt32(&resets, 1) })
	c, err := New(client.New(ts.URL, ""), Options{})
	if err != nil {
		t.Fatal(err)
	}
	c.AttachEvents(bus)
	_, derr := c.Do(context.Background(), &fs.Op{Kind: fs.OpGetAttr, Path: "/f"}, nil)
	if derr == nil {
		t.Fatal("401 lookup succeeded")
	}
	if st := fs.StatusOf(derr); st != gofuse.EACCES {
		t.Fatalf("401 status = %v, want EACCES", st)
	}
	if atomic.LoadInt32(&resets) == 0 {
		t.Fatal("401 did not emit a view reset")
	}
}
