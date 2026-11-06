package fuse

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
)

// A directory this mount just created has a locally managed empty
// namespace: the first probes of fresh names inside it must not pay a HEAD
// each (#1026 dropped the v1 MarkSessionCreatedDir marker).
func TestPortMkdirMarksSessionCreated(t *testing.T) {
	b := testbackend.NewBackend()
	var heads atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == http.MethodHead {
			heads.Add(1)
		}
		b.ServeHTTP(w, r)
	}))
	defer ts.Close()
	f, _ := facadeOverCore(t, ts)

	var mout gofuse.EntryOut
	if st := f.Mkdir(nil, &gofuse.MkdirIn{InHeader: gofuse.InHeader{NodeId: 1}, Mode: 0o755}, "d", &mout); st != gofuse.OK {
		t.Fatalf("mkdir = %v", st)
	}
	for i := 0; i < 3; i++ {
		var out gofuse.EntryOut
		if st := f.Lookup(nil, &gofuse.InHeader{NodeId: mout.NodeId}, fmt.Sprintf("new-%d", i), &out); st != gofuse.ENOENT {
			t.Fatalf("lookup %d = %v, want ENOENT", i, st)
		}
	}
	if got := heads.Load(); got != 0 {
		t.Fatalf("HEADs under a session-created dir = %d, want 0", got)
	}
}

// A read-only OPEN follows a successful LOOKUP. The kernel resolved the
// dentry; the terminal must not stat it again on the open path.
func TestPortReadOnlyOpenDoesNotReHead(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("x"))
	var heads atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == http.MethodHead {
			heads.Add(1)
		}
		b.ServeHTTP(w, r)
	}))
	defer ts.Close()
	f, _ := facadeOverCore(t, ts)

	var out gofuse.EntryOut
	if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "f", &out); st != gofuse.OK {
		t.Fatalf("lookup = %v", st)
	}
	afterLookup := heads.Load()
	var opened gofuse.OpenOut
	if st := f.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: out.NodeId}, Flags: uint32(syscall.O_RDONLY)}, &opened); st != gofuse.OK {
		t.Fatalf("open = %v", st)
	}
	if got := heads.Load(); got != afterLookup {
		t.Fatalf("read-only open added HEADs: %d -> %d", afterLookup, got)
	}
}
