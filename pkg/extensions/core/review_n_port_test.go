package core

// codex twenty-third round: an HTTP 401 resets the mount view before the
// caller sees EACCES (the credential may now point at another tenant).

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/dircache"
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
	reset := make(chan struct{}, 1)
	var kernelInvalidate bool
	bus.On(fs.EventCheckout, func(e fs.Event) {
		kernelInvalidate = e.KernelInvalidate
		select {
		case reset <- struct{}{}:
		default:
		}
	})
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
	// The reset is announced asynchronously (a synchronous broadcast
	// deadlocks a data plane holding its own handle lock).
	select {
	case <-reset:
	case <-time.After(2 * time.Second):
		t.Fatal("401 did not emit a view reset")
	}
	if !kernelInvalidate {
		t.Fatal("401 reset did not ask for kernel-cache invalidation")
	}
}

// The namespace-cache stat projection keeps the extent routing marker: an
// extent file's LOOKUP must not fall back to the ordinary HTTP data plane.
func TestPortNamespaceStatKeepsExtentIno(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	c, err := New(client.New(ts.URL, ""), Options{TrustLocalEvents: true})
	if err != nil {
		t.Fatal(err)
	}
	nc := dircache.NewNamespaceCache(time.Minute, time.Second, 0)
	nc.Put("/", []fs.DirEntryInfo{{Name: "f", Size: 5, Rev: 7, ExtentIno: 77, HasMode: true, Mode: 0o644}})
	c.Attach(nc)
	res, derr := c.Do(context.Background(), &fs.Op{Kind: fs.OpGetAttr, Path: "/f"}, nil)
	if derr != nil {
		t.Fatalf("getattr = %v", derr)
	}
	if res.Attr == nil || res.Attr.ExtentIno != 77 {
		t.Fatalf("attr = %+v, want ExtentIno 77 from the namespace row", res.Attr)
	}
}
