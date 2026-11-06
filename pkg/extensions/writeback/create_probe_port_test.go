package writeback

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// A kernel CREATE already resolved the name missing; the writeback tier's
// own existence probe added one HEAD per new file before the lazy create.
func TestPortKernelCreateSkipsExistenceProbe(t *testing.T) {
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
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	res, err := ch.Dispatch(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY),
		Mode: 0o644, HasMode: true, Kernel: &gofuse.CreateIn{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Handle == nil {
		t.Fatal("kernel create returned no handle")
	}
	if got := heads.Load(); got != 0 {
		t.Fatalf("kernel create HEADs = %d, want 0", got)
	}
	if err := res.Handle.Write(ctx, 0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if n, ok := b.Node("/f"); ok && string(n.Data) == "hello" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("created file never landed")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A writable re-open of a path whose newest image is still staged locally
// must serve that image instead of probing the remote and waiting for the
// previous upload.
func TestPortWritableReopenServesPendingImage(t *testing.T) {
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
	cl := client.New(ts.URL, "")
	wbcache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := wbcache.PutWithBaseRevAndMode("/f", []byte("abc"), 3, staging.PendingNew, 0, 0o644, true); err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: wbcache})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	res, err := ch.Dispatch(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/f",
		Flags:  uint32(syscall.O_WRONLY | syscall.O_APPEND),
		Kernel: &gofuse.OpenIn{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Handle == nil {
		t.Fatal("re-open returned no handle")
	}
	if got := heads.Load(); got != 0 {
		t.Fatalf("pending re-open HEADs = %d, want 0", got)
	}
	if err := res.Handle.Write(ctx, 3, []byte("d")); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if n, ok := b.Node("/f"); ok && string(n.Data) == "abcd" {
			return
		}
		if time.Now().After(deadline) {
			n, ok := b.Node("/f")
			t.Fatalf("pending re-open did not land: ok=%v data=%q", ok, n.Data)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
