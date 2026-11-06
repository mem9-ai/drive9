package writeback

import (
	"context"
	"errors"
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
	"github.com/mem9-ai/drive9/pkg/fusecomp/writebuffer"
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
	if !wbcache.MarkCacheLineage("/f", gen1, gen2) {
		t.Fatal("mark cache lineage failed")
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

// A second append opener can materialize an intermediate generation G2
// before the G1 upload acknowledges. The acknowledgement must reach that
// descendant handle too, not just the direct G1 opener.
func TestPortPendingAppendAckReachesDescendantHandle(t *testing.T) {
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
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: wbcache})
	if err != nil {
		t.Fatal(err)
	}
	a, ok := wb.openPendingWritable(&fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_WRONLY | syscall.O_APPEND)})
	if !ok {
		t.Fatal("first opener did not take the fast path")
	}
	// A flush of the first opener would stage G2; model the same metadata
	// directly so the second opener captures G2 while G1 is still in flight.
	gen2, _, err := wbcache.PutWithBaseRevAndModeTimings("/f", []byte("base+one"), 8, staging.PendingOverwrite, 1, 0o644, true)
	if err != nil {
		t.Fatal(err)
	}
	if !wbcache.MarkCacheLineage("/f", gen1, gen2) {
		t.Fatal("mark descendant lineage")
	}
	bb, ok := wb.openPendingWritable(&fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_WRONLY | syscall.O_APPEND)})
	if !ok {
		t.Fatal("descendant opener did not take the fast path")
	}
	wb.acknowledgeUploadedCreate("/f", 2, gen1, pendingGenCache, gen1)
	for name, res := range map[string]fs.Result{"ancestor": a, "descendant": bb} {
		h := res.Handle.(*bufferingHandle)
		h.mu.Lock()
		baseRev := h.baseRev
		h.mu.Unlock()
		if baseRev != 2 {
			t.Fatalf("%s baseRev after G1 = %d, want 2", name, baseRev)
		}
	}
	// The descendant's own staged generation is the newer parent for its
	// next append; the ancestor handle did not contain G2 and must not move.
	wb.acknowledgeUploadedCreate("/f", 3, gen2, pendingGenCache, gen2)
	bh := bb.Handle.(*bufferingHandle)
	bh.mu.Lock()
	baseRev := bh.baseRev
	bh.mu.Unlock()
	if baseRev != 3 {
		t.Fatalf("descendant baseRev after G2 = %d, want 3", baseRev)
	}
	ah := a.Handle.(*bufferingHandle)
	ah.mu.Lock()
	baseRev = ah.baseRev
	ah.mu.Unlock()
	if baseRev != 2 {
		t.Fatalf("ancestor baseRev after G2 = %d, want 2", baseRev)
	}
}

// O_EXCL must get the server's create arbitration before open(2) returns,
// not a lazy PendingNew that only discovers the conflict at upload time.
func TestPortKernelCreateExclusiveChecksServer(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("old"))
	ts := httptest.NewServer(b)
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
	_, err = ch.Dispatch(context.Background(), &fs.Op{
		Kind: fs.OpCreate, Path: "/f",
		Flags: uint32(syscall.O_CREAT | syscall.O_EXCL | syscall.O_WRONLY),
		Mode:  0o644, HasMode: true, Kernel: &gofuse.CreateIn{},
	})
	if !errors.Is(err, syscall.EEXIST) {
		t.Fatalf("O_EXCL existing create err = %v, want EEXIST", err)
	}
}

// A cache-derived stage must persist its lineage in the cache field, not
// stamp a cache generation into SupersedesPendingGen where the uploader
// would treat it as a pending-index marker.
func TestPortCacheStageKeepsGenerationSpacesSeparate(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("base"))
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
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: wbcache})
	if err != nil {
		t.Fatal(err)
	}
	res, ok := wb.openPendingWritable(&fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_WRONLY | syscall.O_APPEND)})
	if !ok {
		t.Fatal("pending overwrite append did not take the fast path")
	}
	h := res.Handle.(*bufferingHandle)
	if err := h.Write(context.Background(), 4, []byte("+x")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	meta, ok := wbcache.GetMeta("/f")
	if !ok || meta == nil {
		t.Fatal("no staged meta")
	}
	if meta.SupersedesPendingGen != 0 {
		t.Fatalf("cache stage stamped SupersedesPendingGen=%d, want 0", meta.SupersedesPendingGen)
	}
	if meta.CacheLineageGen != gen1 {
		t.Fatalf("cache lineage = %d, want %d", meta.CacheLineageGen, gen1)
	}
	h.mu.Lock()
	ancestors := append([]uint64(nil), h.cacheAncestorGens...)
	h.mu.Unlock()
	if !cacheGenInList(ancestors, meta.Generation) {
		t.Fatalf("handle ancestors = %v, want staged generation %d", ancestors, meta.Generation)
	}
}

// A queue-sourced handle must not accept a cache acknowledgement that only
// shares the number; source is part of the CAS lineage identity.
func TestPortQueueHandleIgnoresCacheGenerationCollision(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	defer ts.Close()
	wb, err := New(Options{Policy: PolicyWriteBack, Client: client.New(ts.URL, "")})
	if err != nil {
		t.Fatal(err)
	}
	h := &bufferingHandle{w: wb, path: "/f", baseRev: 1,
		supersededPendingGen: 5, supersededPendingSource: pendingGenQueue}
	wb.track(h)
	wb.acknowledgeUploadedCreate("/f", 2, 5, pendingGenCache, 5)
	h.mu.Lock()
	baseRev := h.baseRev
	h.mu.Unlock()
	if baseRev != 1 {
		t.Fatalf("cache ack advanced queue handle to %d, want 1", baseRev)
	}
}

// A PendingOverwrite fast path must stand down while its upload is in flight:
// the PUT may already be published but not acknowledged.
func TestPortPendingOverwriteOpenWaitsForInflightUpload(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	defer ts.Close()
	cl := client.New(ts.URL, "")
	wbcache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := wbcache.PutWithBaseRevAndModeTimings("/f", []byte("base"), 4, staging.PendingOverwrite, 1, 0o644, true); err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: wbcache})
	if err != nil {
		t.Fatal(err)
	}
	release := wb.uploader.acquirePath("/f")
	defer release()
	if _, ok := wb.openPendingWritable(&fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_WRONLY | syscall.O_APPEND)}); ok {
		t.Fatal("pending overwrite opened while its upload was in flight")
	}
}

// A queue-derived handle that later stages its buffer into the cache must
// still acknowledge that cache image; the queue marker and cache lineage are
// different spaces.
func TestPortQueueHandleAcksItsOwnLaterCacheStage(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	defer ts.Close()
	wb, err := New(Options{Policy: PolicyWriteBack, Client: client.New(ts.URL, "")})
	if err != nil {
		t.Fatal(err)
	}
	h := &bufferingHandle{w: wb, path: "/f", baseRev: 1,
		supersededPendingGen: 5, supersededPendingSource: pendingGenQueue,
		cacheLineageGen: 9} // cache image staged by this handle
	wb.track(h)
	wb.acknowledgeUploadedCreate("/f", 2, 9, pendingGenCache, 9)
	h.mu.Lock()
	baseRev := h.baseRev
	h.mu.Unlock()
	if baseRev != 2 {
		t.Fatalf("queue handle did not adopt its own cache stage: baseRev=%d, want 2", baseRev)
	}
}

// Production staging (not a hand-built handle) must install the cache
// generation on a queue-derived handle while keeping the queue marker.
func TestPortQueueStageInstallsCacheLineage(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	defer ts.Close()
	wbcache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{Policy: PolicyWriteBack, Client: client.New(ts.URL, ""), WriteCache: wbcache})
	if err != nil {
		t.Fatal(err)
	}
	buf := writebuffer.NewWriteBuffer("/f", 1<<20, 0)
	if _, err := buf.Write(0, []byte("base")); err != nil {
		t.Fatal(err)
	}
	h := &bufferingHandle{w: wb, path: "/f", baseRev: 1, buf: buf,
		supersededPendingGen: 5, supersededPendingSource: pendingGenQueue}
	wb.track(h)
	if _, err := h.stageToWriteCache(context.Background(), stageFullDurable); err != nil {
		t.Fatal(err)
	}
	meta, ok := wbcache.GetMeta("/f")
	if !ok || meta == nil {
		t.Fatal("no staged metadata")
	}
	h.mu.Lock()
	lineage := h.cacheLineageGen
	h.mu.Unlock()
	if lineage != meta.Generation {
		t.Fatalf("handle cache lineage = %d, meta gen = %d", lineage, meta.Generation)
	}
	if meta.CacheLineageGen != meta.Generation {
		t.Fatalf("meta cache lineage = %d, gen = %d", meta.CacheLineageGen, meta.Generation)
	}
	if meta.SupersedesPendingGen != 5 {
		t.Fatalf("queue supersession marker = %d, want 5", meta.SupersedesPendingGen)
	}
}

// Even after the predecessor's cache entry is removed (uploadOne removes it
// before OnSuccess), the successor's persisted ancestor list must let that
// delayed acknowledgement rebase both the meta and the live handle.
func TestPortAncestorAckSurvivesCacheRemoval(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("base"))
	ts := httptest.NewServer(b)
	defer ts.Close()
	wbcache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gen1, _, err := wbcache.PutWithBaseRevAndModeTimings("/f", []byte("base"), 4, staging.PendingOverwrite, 1, 0o644, true)
	if err != nil {
		t.Fatal(err)
	}
	gen2, _, err := wbcache.PutWithBaseRevAndModeTimings("/f", []byte("base+2"), 6, staging.PendingOverwrite, 1, 0o644, true)
	if err != nil {
		t.Fatal(err)
	}
	if !wbcache.MarkCacheLineage("/f", gen1, gen2) {
		t.Fatal("mark lineage")
	}
	wb, err := New(Options{Policy: PolicyWriteBack, Client: client.New(ts.URL, ""), WriteCache: wbcache})
	if err != nil {
		t.Fatal(err)
	}
	res, ok := wb.openPendingWritable(&fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_WRONLY | syscall.O_APPEND)})
	if !ok {
		t.Fatal("fast path")
	}
	h := res.Handle.(*bufferingHandle)
	if err := h.Write(context.Background(), 6, []byte("+3")); err != nil {
		t.Fatal(err)
	}
	// Simulate uploadOne removing G2's cache entry just before OnSuccess.
	wbcache.Remove("/f")
	if _, err := h.stageToWriteCache(context.Background(), stageFullDurable); err != nil {
		t.Fatal(err)
	}
	wb.acknowledgeUploadedCreate("/f", 2, gen2, pendingGenCache, gen2)
	h.mu.Lock()
	baseRev := h.baseRev
	h.mu.Unlock()
	if baseRev != 2 {
		t.Fatalf("handle base after delayed G2 ack = %d, want 2", baseRev)
	}
	meta, ok := wbcache.GetMeta("/f")
	if !ok || meta.BaseRev != 2 || !cacheGenInList(meta.CacheAncestorGens, gen2) {
		t.Fatalf("successor meta after ack = %+v ok=%v", meta, ok)
	}
}

// Existing-file Write→Flush→Write→Release must not reacquire the upload gate
// that Release already owns.
func TestPortExistingFileReleaseAfterFlushDoesNotDeadlock(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("old"))
	chain, _, _ := newWriteCacheClientStack(t, PolicyWriteBack, b, Options{})
	h := dispatchOpen(t, chain, "/f", uint32(syscall.O_RDWR))
	if err := h.Write(context.Background(), 0, []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(context.Background(), 0, []byte("newer")); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- h.Release(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Release deadlocked after Flush")
	}
}
