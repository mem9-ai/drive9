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
		Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_CREAT | syscall.O_EXCL | syscall.O_WRONLY),
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

// A zero-byte pending create is a complete append base; capturePendingImage
// rejects size<=0, but the fast path must still serve it (touch; echo x >>
// file before the empty create lands).
func TestPortZeroBytePendingAppend(t *testing.T) {
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
	if err := wbcache.PutWithBaseRevAndMode("/f", nil, 0, staging.PendingNew, 0, 0o644, true); err != nil {
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
		t.Fatal("zero-byte pending append returned no handle")
	}
	if got := heads.Load(); got != 0 {
		t.Fatalf("zero-byte pending append HEADs = %d, want 0", got)
	}
	if err := res.Handle.Write(ctx, 0, []byte("x")); err != nil {
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
		if n, ok := b.Node("/f"); ok && string(n.Data) == "x" {
			return
		}
		if time.Now().After(deadline) {
			n, ok := b.Node("/f")
			t.Fatalf("zero-byte append did not land: ok=%v data=%q", ok, n.Data)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A pending-overwrite append opener must adopt the revision the in-flight
// upload publishes. Otherwise its CAS base stays at the pre-upload R and the
// append conflicts with this mount's own R+1.
func TestPortPendingOverwriteAppendAdoptsUploadRevision(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	defer ts.Close()
	cl := client.New(ts.URL, "")
	wbcache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gen, _, err := wbcache.PutWithBaseRevAndModeTimings("/f", []byte("base"), 4, staging.PendingOverwrite, 1, 0o644, true)
	if err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: wbcache})
	if err != nil {
		t.Fatal(err)
	}
	op := &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_WRONLY | syscall.O_APPEND)}
	res, ok := wb.openPendingWritable(op)
	if !ok || res.Handle == nil {
		t.Fatal("pending overwrite append did not take the fast path")
	}
	h, ok := res.Handle.(*bufferingHandle)
	if !ok {
		t.Fatalf("handle type = %T", res.Handle)
	}
	if h.baseRev != 1 || h.supersededPendingGen != gen {
		t.Fatalf("before ack: baseRev=%d gen=%d, want 1/%d", h.baseRev, h.supersededPendingGen, gen)
	}
	wb.acknowledgeUploadedCreate("/f", 2, gen, pendingGenCache, gen)
	h.mu.Lock()
	baseRev, supersedes := h.baseRev, h.supersededPendingGen
	h.mu.Unlock()
	if baseRev != 2 {
		t.Fatalf("after ack baseRev = %d, want 2", baseRev)
	}
	if supersedes != gen {
		t.Fatalf("after ack supersedes = %d, want %d", supersedes, gen)
	}
	if meta, ok := wbcache.GetMeta("/f"); !ok || meta.BaseRev != 2 {
		t.Fatalf("pending meta after ack = %+v ok=%v, want base 2", meta, ok)
	}
	// A handle opened AFTER the acknowledgement (the deferred-mode window)
	// must already start from the published revision.
	res, ok = wb.openPendingWritable(op)
	if !ok || res.Handle == nil {
		t.Fatal("post-ack pending overwrite append did not take the fast path")
	}
	h2 := res.Handle.(*bufferingHandle)
	if h2.baseRev != 2 {
		t.Fatalf("post-ack baseRev = %d, want 2", h2.baseRev)
	}
}

// If the append staged a newer pending image before the older upload
// acknowledged, that newer entry is the one that needs rebasing.
func TestPortPendingOverwriteAppendRebasesNewerStage(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	defer ts.Close()
	cl := client.New(ts.URL, "")
	wbcache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gen1, _, err := wbcache.PutWithBaseRevAndModeTimings("/f", []byte("base"), 4, staging.PendingOverwrite, 1, 0o644, true)
	if err != nil {
		t.Fatal(err)
	}
	gen2, _, err := wbcache.PutWithBaseRevAndModeTimings("/f", []byte("base+append"), 11, staging.PendingOverwrite, 1, 0o644, true)
	if err != nil {
		t.Fatal(err)
	}
	if !wbcache.MarkSupersedesPendingGen("/f", gen1, gen2) {
		t.Fatal("mark supersedes failed")
	}
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: wbcache})
	if err != nil {
		t.Fatal(err)
	}
	wb.acknowledgeUploadedCreate("/f", 2, gen1, pendingGenCache, gen1)
	if meta, ok := wbcache.GetMeta("/f"); !ok || meta.BaseRev != 2 {
		t.Fatalf("newer staged meta after ack = %+v ok=%v, want base 2", meta, ok)
	}
}

// Cache and pending-index generation counters are independent; a queue
// acknowledgement with a coincidentally equal number must not advance a
// cache-backed handle's base.
func TestPortAckIgnoresMatchingNumberFromOtherTrack(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	defer ts.Close()
	cl := client.New(ts.URL, "")
	wbcache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gen, _, err := wbcache.PutWithBaseRevAndModeTimings("/f", []byte("base"), 4, staging.PendingOverwrite, 1, 0o644, true)
	if err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: wbcache})
	if err != nil {
		t.Fatal(err)
	}
	res, ok := wb.openPendingWritable(&fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_WRONLY | syscall.O_APPEND)})
	if !ok {
		t.Fatal("pending overwrite append did not take the fast path")
	}
	h := res.Handle.(*bufferingHandle)
	wb.acknowledgeUploadedCreate("/f", 2, gen, pendingGenQueue, 0)
	h.mu.Lock()
	baseRev := h.baseRev
	h.mu.Unlock()
	if baseRev != 1 {
		t.Fatalf("queue ack changed cache handle baseRev to %d, want 1", baseRev)
	}
}

// A nonexclusive kernel CREATE can lose the negative-lookup race to another
// client. The writeback tier must reconcile against the existing remote row
// instead of returning a lazy PendingNew that the uploader can never land.
func TestPortKernelCreateReconcilesExistingNonexclusive(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("old"))
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
	res, err := ch.Dispatch(context.Background(), &fs.Op{
		Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR),
		Mode: 0o644, HasMode: true, Kernel: &gofuse.CreateIn{},
	})
	if err != nil {
		t.Fatal(err)
	}
	h, ok := res.Handle.(*bufferingHandle)
	if !ok {
		t.Fatalf("handle type = %T", res.Handle)
	}
	if h.isNew || h.baseRev != 1 {
		t.Fatalf("reconciled create isNew=%v baseRev=%d, want false/1", h.isNew, h.baseRev)
	}
	if got := heads.Load(); got != 1 {
		t.Fatalf("nonexclusive create probe HEADs = %d, want 1", got)
	}
}
