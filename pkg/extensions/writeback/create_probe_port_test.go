package writeback

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/dircache"
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
// client, while a truncating kernel CREATE inside a session-created directory
// (unzip, tar, cp) still answers locally from dircache with zero remote HEADs.
func TestPortKernelCreateReconcilesExistingNonexclusive(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("old"))
	b.Seed("/ftrunc", []byte("old-trunc"))
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
	dc := dircache.NewNamespaceCache(10*time.Second, time.Second, 1000)
	terminal.Attach(dc)
	dc.MarkSessionCreatedDir("/uz")
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	// Truncating kernel CREATE inside a session-created directory claims
	// asynchronously with 0 HEADs.
	res, err := ch.Dispatch(context.Background(), &fs.Op{
		Kind: fs.OpCreate, Path: "/uz/fresh", Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_RDWR),
		Mode: 0o644, HasMode: true, Kernel: &gofuse.CreateIn{},
	})
	if err != nil {
		t.Fatal(err)
	}
	h, ok := res.Handle.(*bufferingHandle)
	if !ok {
		t.Fatalf("handle type = %T", res.Handle)
	}
	if !h.isNew || h.baseRev != 0 {
		t.Fatalf("async truncating create isNew=%v baseRev=%d, want true/0", h.isNew, h.baseRev)
	}
	if got := heads.Load(); got != 0 {
		t.Fatalf("truncating create probe HEADs = %d, want 0", got)
	}
	_ = h.Release(context.Background())

	// Non-truncating kernel CREATE (O_CREAT|O_RDWR without O_TRUNC) reconciles
	// an existing remote file so its contents and attributes are preserved.
	res2, err := ch.Dispatch(context.Background(), &fs.Op{
		Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR),
		Mode: 0o644, HasMode: true, Kernel: &gofuse.CreateIn{},
	})
	if err != nil {
		t.Fatal(err)
	}
	h2, ok := res2.Handle.(*bufferingHandle)
	if !ok {
		t.Fatalf("handle type = %T", res2.Handle)
	}
	if h2.isNew || h2.baseRev != 1 {
		t.Fatalf("non-truncating create isNew=%v baseRev=%d, want false/1", h2.isNew, h2.baseRev)
	}
	buf := make([]byte, 8)
	n, rerr := h2.Read(context.Background(), 0, buf)
	if rerr != nil || string(buf[:n]) != "old" {
		t.Fatalf("Read existing after non-truncating create = %q, %v; want %q", string(buf[:n]), rerr, "old")
	}
	_ = h2.Release(context.Background())

	// Truncating kernel CREATE against an uncached existing file reconciles
	// onto the existing revision and truncates/replaces it cleanly at fsync.
	res3, err := ch.Dispatch(context.Background(), &fs.Op{
		Kind: fs.OpCreate, Path: "/ftrunc", Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_RDWR),
		Mode: 0o644, HasMode: true, Kernel: &gofuse.CreateIn{},
	})
	if err != nil {
		t.Fatal(err)
	}
	h3, ok := res3.Handle.(*bufferingHandle)
	if !ok {
		t.Fatalf("handle type = %T", res3.Handle)
	}
	if h3.isNew || h3.baseRev != 1 || !h3.trunc {
		t.Fatalf("truncating existing create isNew=%v baseRev=%d trunc=%v, want false/1/true", h3.isNew, h3.baseRev, h3.trunc)
	}
	if err := h3.Sync(context.Background()); err != nil {
		t.Fatalf("truncate-only Sync: %v", err)
	}
	_ = h3.Release(context.Background())
	if node, ok := b.Node("/ftrunc"); !ok || len(node.Data) != 0 {
		t.Fatalf("remote /ftrunc after truncate-only Sync = %q ok=%v, want empty", node.Data, ok)
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

// An outstanding predecessor must survive more than 64 later stages; the
// ancestor list is bounded by upload completion, not by a fixed count.
func TestPortAncestorListRetainsOutstandingGeneration(t *testing.T) {
	gens := make([]uint64, 0, 100)
	for i := uint64(1); i <= 100; i++ {
		gens = append(gens, i)
	}
	list := mergeCacheAncestorGens(nil, gens...)
	if !cacheGenInList(list, 1) || !cacheGenInList(list, 100) {
		t.Fatalf("ancestor list dropped an outstanding generation: %v", list)
	}
}

// Regression for drive9/fuse/smoke: when a closed overwrite is still in flight
// in WriteBackUploader and a subsequent non-append truncate (`: > file`, sent
// either as OPEN(O_WRONLY)+SETATTR(size=0, fh) or OPEN(O_WRONLY|O_TRUNC))
// stages its 0-byte replacement before the first PUT finishes, the replacement
// must inherit cache lineage so OnSuccess rebases its BaseRev instead of
// leaving a 0-byte conflicted entry stuck in uploader_cached on Drain.
func TestNonAppendTruncateRacesInFlightOverwriteRebasesCleanly(t *testing.T) {
	for _, tc := range []struct {
		name   string
		oTrunc bool
	}{
		{name: "open-then-setattr-zero", oTrunc: false},
		{name: "open-o-trunc", oTrunc: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			b := testbackend.NewBackend()
			b.Seed("/alpha/hard.txt", []byte("initial-seed"))
			entered, allow := make(chan struct{}), make(chan struct{})
			var enteredOnce, allowOnce sync.Once
			unblock := func() { allowOnce.Do(func() { close(allow) }) }
			defer unblock()

			var putCount atomic.Int32
			srv := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/alpha/hard.txt") {
					if putCount.Add(1) == 1 {
						enteredOnce.Do(func() { close(entered) })
						select {
						case <-allow:
						case <-ctx.Done():
							return
						}
					}
				}
				b.ServeHTTP(w, r)
			})
			chain, wb := newReviewDrainStack(t, srv, nil)

			// First write ("hardlink-payload") closes and enters its async PUT.
			h1 := dispatchOpen(t, chain, "/alpha/hard.txt", uint32(syscall.O_WRONLY))
			if err := h1.Write(ctx, 0, []byte("hardlink-payload")); err != nil {
				t.Fatal(err)
			}
			if err := h1.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if err := h1.Release(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("first overwrite PUT did not start")
			}

			// Second open truncates to 0 bytes and closes while PUT #1 is still in flight.
			openFlags := uint32(syscall.O_WRONLY)
			if tc.oTrunc {
				openFlags |= uint32(syscall.O_TRUNC)
			}
			h2 := dispatchOpen(t, chain, "/alpha/hard.txt", openFlags)
			if !tc.oTrunc {
				if _, err := chain.Dispatch(ctx, &fs.Op{
					Kind: fs.OpSetAttr, Path: "/alpha/hard.txt", Size: 0, HasSize: true, Handle: h2,
				}); err != nil {
					t.Fatalf("ftruncate to 0: %v", err)
				}
			}
			if err := h2.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if err := h2.Release(ctx); err != nil {
				t.Fatal(err)
			}

			unblock()
			if err := wb.Drain(ctx); err != nil {
				t.Fatalf("Drain failed (conflicted entry stranded): %v", err)
			}
			if pending := wb.wbCache.ListPendingMetas(); len(pending) != 0 {
				t.Fatalf("uploader_cached entries stranded after Drain: %+v", pending)
			}
			node, ok := b.Node("/alpha/hard.txt")
			if !ok || len(node.Data) != 0 {
				t.Fatalf("remote /alpha/hard.txt = %q (ok=%v), want 0-byte truncated file", node.Data, ok)
			}
		})
	}
}

// Regression for drive9/fuse/nodefs: when close(2) on replace-old.txt has run
// FUSE_FLUSH (staging PendingNew into wbCache) while asynchronous FUSE_RELEASE
// is still pending in the kernel, a rename(replace-new.txt, replace-old.txt)
// marks the old handle unlinked and must retire its pre-rename wbCache entry
// rather than leaving an unsubmitted snapshot stranded in uploader_cached.
func TestRenameOverFlushedTargetBeforeReleaseRetiresWriteBackCache(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	b := testbackend.NewBackend()
	chain, wb := newReviewDrainStack(t, b, nil)

	resOld, err := chain.Dispatch(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/nodefs/replace-old.txt",
		Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY), Mode: 0o644, HasMode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	hOld := resOld.Handle
	if err := hOld.Write(ctx, 0, []byte("old\n")); err != nil {
		t.Fatal(err)
	}
	if err := hOld.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	resNew, err := chain.Dispatch(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/nodefs/replace-new.txt",
		Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY), Mode: 0o644, HasMode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	hNew := resNew.Handle
	if err := hNew.Write(ctx, 0, []byte("new\n")); err != nil {
		t.Fatal(err)
	}
	if err := hNew.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := hNew.Release(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := chain.Dispatch(ctx, &fs.Op{
		Kind: fs.OpRename, Path: "/nodefs/replace-new.txt", Path2: "/nodefs/replace-old.txt",
	}); err != nil {
		t.Fatalf("rename replace-new -> replace-old: %v", err)
	}

	// Asynchronous kernel FUSE_RELEASE arrives after rename completed.
	if err := hOld.Release(ctx); err != nil {
		t.Fatal(err)
	}

	rres, err := chain.Dispatch(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/nodefs/replace-old.txt", Flags: uint32(syscall.O_RDONLY),
	})
	if err != nil {
		t.Fatalf("reopen replace-old: %v", err)
	}
	buf := make([]byte, 16)
	n, _ := rres.Handle.Read(ctx, 0, buf)
	_ = rres.Handle.Release(ctx)
	if string(buf[:n]) != "new\n" {
		t.Fatalf("read after rename = %q, want %q", string(buf[:n]), "new\n")
	}

	if err := wb.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if reason, count := wb.DrainPending(); count != 0 {
		t.Fatalf("DrainPending = (%q, %d), want empty", reason, count)
	}
	node, ok := b.Node("/nodefs/replace-old.txt")
	if !ok || string(node.Data) != "new\n" {
		t.Fatalf("remote replace-old.txt = %q (ok=%v), want %q", node.Data, ok, "new\n")
	}
}

func TestRenameTargetChmodDuringInFlightRenameRetiresWriteBackCache(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	b := testbackend.NewBackend()
	enteredRename, allowRename := make(chan struct{}), make(chan struct{})
	var enteredOnce, allowOnce sync.Once
	unblock := func() { allowOnce.Do(func() { close(allowRename) }) }
	defer unblock()

	srv := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.RawQuery, "rename") {
			enteredOnce.Do(func() { close(enteredRename) })
			select {
			case <-allowRename:
			case <-ctx.Done():
				return
			}
		}
		b.ServeHTTP(w, r)
	})
	chain, wb := newReviewDrainStack(t, srv, nil)

	resOld, err := chain.Dispatch(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/dst",
		Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY), Mode: 0o644, HasMode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := resOld.Handle.Write(ctx, 0, []byte("old\n")); err != nil {
		t.Fatal(err)
	}
	if err := resOld.Handle.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	resNew, err := chain.Dispatch(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/src",
		Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY), Mode: 0o644, HasMode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := resNew.Handle.Write(ctx, 0, []byte("new\n")); err != nil {
		t.Fatal(err)
	}
	if err := resNew.Handle.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := resNew.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}

	renameDone := make(chan error, 1)
	go func() {
		_, rerr := chain.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/src", Path2: "/dst"})
		renameDone <- rerr
	}()

	select {
	case <-enteredRename:
	case <-ctx.Done():
		t.Fatal("rename RPC did not enter server")
	}

	// While remote rename is in flight, async FUSE_RELEASE untracks hOld and a
	// concurrent chmod bumps WriteBackMeta.Generation via UpdateMode.
	if err := resOld.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := chain.Dispatch(ctx, &fs.Op{
		Kind: fs.OpSetAttr, Path: "/dst", Mode: 0o600, HasMode: true,
	}); err != nil {
		t.Fatalf("chmod during in-flight rename: %v", err)
	}

	unblock()
	if err := <-renameDone; err != nil {
		t.Fatalf("rename failed: %v", err)
	}

	rres, err := chain.Dispatch(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/dst", Flags: uint32(syscall.O_RDONLY),
	})
	if err != nil {
		t.Fatalf("reopen /dst: %v", err)
	}
	buf := make([]byte, 16)
	n, _ := rres.Handle.Read(ctx, 0, buf)
	_ = rres.Handle.Release(ctx)
	if string(buf[:n]) != "new\n" {
		t.Fatalf("read /dst after rename = %q, want %q", string(buf[:n]), "new\n")
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if reason, count := wb.DrainPending(); count != 0 {
		t.Fatalf("DrainPending = (%q, %d), want empty", reason, count)
	}

	// Verify RemoveIfPutGeneration preserves a brand-new Put installed after the
	// pre-rename snapshot while retiring a descendant whose Generation was bumped
	// by UpdateMode.
	if err := wb.wbCache.PutWithBaseRevAndMode("/fresh", []byte("v1"), 2, staging.PendingNew, 0, 0o644, false); err != nil {
		t.Fatal(err)
	}
	m1, ok := wb.wbCache.GetMeta("/fresh")
	if !ok {
		t.Fatal("missing /fresh meta")
	}
	oldPutGen := m1.PutGen()
	if err := wb.wbCache.PutWithBaseRevAndMode("/fresh", []byte("v2"), 2, staging.PendingNew, 0, 0o644, false); err != nil {
		t.Fatal(err)
	}
	if wb.wbCache.RemoveIfPutGeneration("/fresh", oldPutGen) {
		t.Fatal("RemoveIfPutGeneration removed a newer Put incarnation")
	}
	if data, ok := wb.wbCache.Get("/fresh"); !ok || string(data) != "v2" {
		t.Fatalf("/fresh after stale RemoveIfPutGeneration = %q (ok=%v), want v2", data, ok)
	}
	if err := wb.wbCache.UpdateMode("/fresh", 0o600); err != nil {
		t.Fatal(err)
	}
	m2, ok := wb.wbCache.GetMeta("/fresh")
	if !ok || m2.Generation <= m1.Generation {
		t.Fatalf("expected UpdateMode to advance Generation, got %+v", m2)
	}
	if !wb.wbCache.RemoveIfPutGeneration("/fresh", m2.PutGen()) {
		t.Fatal("RemoveIfPutGeneration failed to retire current PutGen after UpdateMode")
	}
}

func TestFailedRenameAndDirRenameSubmitWithOpenReader(t *testing.T) {
	t.Run("failed-rename-rollback-with-open-reader", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		b := testbackend.NewBackend()
		enteredRename, allowRename := make(chan struct{}), make(chan struct{})
		var enteredOnce, allowOnce sync.Once
		unblock := func() { allowOnce.Do(func() { close(allowRename) }) }
		defer unblock()

		srv := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost && strings.Contains(r.URL.RawQuery, "rename") {
				enteredOnce.Do(func() { close(enteredRename) })
				select {
				case <-allowRename:
				case <-ctx.Done():
					return
				}
				http.Error(w, "rename unavailable", http.StatusServiceUnavailable)
				return
			}
			b.ServeHTTP(w, r)
		})
		chain, wb := newReviewDrainStack(t, srv, nil)

		resOld, err := chain.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/dst",
			Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY), Mode: 0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := resOld.Handle.Write(ctx, 0, []byte("old\n")); err != nil {
			t.Fatal(err)
		}
		if err := resOld.Handle.Flush(ctx); err != nil {
			t.Fatal(err)
		}

		// Open an O_RDONLY handle over /dst's pending overlay so w.live tracks a
		// non-staging reader on /dst across the failed rename rollback.
		resRead, err := chain.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/dst", Flags: uint32(syscall.O_RDONLY),
		})
		if err != nil {
			t.Fatal(err)
		}

		resSrc, err := chain.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/src",
			Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY), Mode: 0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := resSrc.Handle.Write(ctx, 0, []byte("new\n")); err != nil {
			t.Fatal(err)
		}
		if err := resSrc.Handle.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		if err := resSrc.Handle.Release(ctx); err != nil {
			t.Fatal(err)
		}

		renameDone := make(chan error, 1)
		go func() {
			_, rerr := chain.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/src", Path2: "/dst"})
			renameDone <- rerr
		}()

		select {
		case <-enteredRename:
		case <-ctx.Done():
			t.Fatal("rename RPC did not enter server")
		}

		// Writer closes while marked unlinked during the in-flight rename.
		if err := resOld.Handle.Release(ctx); err != nil {
			t.Fatal(err)
		}

		unblock()
		if err := <-renameDone; err == nil {
			t.Fatal("expected rename failure")
		}
		if err := resRead.Handle.Release(ctx); err != nil {
			t.Fatal(err)
		}

		if err := wb.Drain(ctx); err != nil {
			t.Fatalf("Drain: %v", err)
		}
		if reason, count := wb.DrainPending(); count != 0 {
			t.Fatalf("DrainPending = (%q, %d), want empty", reason, count)
		}
		node, ok := b.Node("/dst")
		if !ok || string(node.Data) != "old\n" {
			t.Fatalf("remote /dst after failed rename = %q (ok=%v), want %q", node.Data, ok, "old\n")
		}
	})

	t.Run("failed-rename-rollback-during-inner-release", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		b := testbackend.NewBackend()
		b.Seed("/dst", []byte("seed"))
		enteredRename, allowRename := make(chan struct{}), make(chan struct{})
		var enteredOnce, allowOnce sync.Once
		unblockRename := func() { allowOnce.Do(func() { close(allowRename) }) }
		defer unblockRename()

		srv := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost && strings.Contains(r.URL.RawQuery, "rename") {
				enteredOnce.Do(func() { close(enteredRename) })
				select {
				case <-allowRename:
				case <-ctx.Done():
					return
				}
				http.Error(w, "rename unavailable", http.StatusServiceUnavailable)
				return
			}
			b.ServeHTTP(w, r)
		})
		chain, wb := newReviewDrainStack(t, srv, nil)

		resOld := dispatchOpen(t, chain, "/dst", uint32(syscall.O_WRONLY|syscall.O_TRUNC))
		bhOld := writebackHandleOf(resOld)
		inner := bhOld.inner
		enteredInnerRelease, allowInnerRelease := make(chan struct{}), make(chan struct{})
		var innerOnce, allowInnerOnce sync.Once
		unblockInner := func() { allowInnerOnce.Do(func() { close(allowInnerRelease) }) }
		defer unblockInner()
		bhOld.inner = releaseBlockingHandle{
			Handle: inner,
			onRelease: func() {
				innerOnce.Do(func() { close(enteredInnerRelease) })
				<-allowInnerRelease
			},
		}

		if err := resOld.Write(ctx, 0, []byte("old-updated\n")); err != nil {
			t.Fatal(err)
		}
		if err := resOld.Flush(ctx); err != nil {
			t.Fatal(err)
		}

		resSrc, err := chain.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/src",
			Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY), Mode: 0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := resSrc.Handle.Write(ctx, 0, []byte("new\n")); err != nil {
			t.Fatal(err)
		}
		if err := resSrc.Handle.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		if err := resSrc.Handle.Release(ctx); err != nil {
			t.Fatal(err)
		}

		renameDone := make(chan error, 1)
		go func() {
			_, rerr := chain.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/src", Path2: "/dst"})
			renameDone <- rerr
		}()
		<-enteredRename

		// Pause hOld inside hOld.inner.Release(ctx), after skipping Submit (because
		// unlinked==true) and before defer untrackAndLast removes hOld from w.live.
		releaseDone := make(chan error, 1)
		go func() {
			releaseDone <- resOld.Release(ctx)
		}()
		<-enteredInnerRelease

		// Let the rename fail and roll back while hOld is still tracked in w.live.
		unblockRename()
		if err := <-renameDone; err == nil {
			t.Fatal("expected rename failure")
		}
		unblockInner()
		if err := <-releaseDone; err != nil {
			t.Fatal(err)
		}

		if err := wb.Drain(ctx); err != nil {
			t.Fatalf("Drain: %v", err)
		}
		if reason, count := wb.DrainPending(); count != 0 {
			t.Fatalf("DrainPending = (%q, %d), want empty", reason, count)
		}
		node, ok := b.Node("/dst")
		if !ok || string(node.Data) != "old-updated\n" {
			t.Fatalf("remote /dst = %q (ok=%v), want %q", node.Data, ok, "old-updated\n")
		}
	})

	t.Run("dir-rename-pending-with-open-reader", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		b := testbackend.NewBackend()
		chain, wb := newReviewDrainStack(t, b, nil)
		if wb.queue != nil {
			wb.queue.DrainAll()
			wb.queue = nil
		}

		if _, err := chain.Dispatch(ctx, &fs.Op{Kind: fs.OpMkdir, Path: "/d1", Mode: 0o755}); err != nil {
			t.Fatal(err)
		}
		if err := wb.wbCache.PutWithBaseRevAndMode("/d1/child.txt", []byte("child-v2\n"), 9, staging.PendingNew, 0, 0o644, false); err != nil {
			t.Fatal(err)
		}
		resRead, err := chain.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/d1/child.txt", Flags: uint32(syscall.O_RDONLY),
		})
		if err != nil {
			t.Fatal(err)
		}

		if _, err := chain.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/d1", Path2: "/d2"}); err != nil {
			t.Fatalf("dir rename /d1 -> /d2: %v", err)
		}
		if err := resRead.Handle.Release(ctx); err != nil {
			t.Fatal(err)
		}

		if err := wb.Drain(ctx); err != nil {
			t.Fatalf("Drain: %v", err)
		}
		if reason, count := wb.DrainPending(); count != 0 {
			t.Fatalf("DrainPending = (%q, %d), want empty", reason, count)
		}
		node, ok := b.Node("/d2/child.txt")
		if !ok || string(node.Data) != "child-v2\n" {
			t.Fatalf("remote /d2/child.txt = %q (ok=%v), want %q", node.Data, ok, "child-v2\n")
		}
	})
}

func TestOverlappingMarkUnlinkedSnapshotWaitsOnInFlightReleaseSubmit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	b := testbackend.NewBackend()
	chain, wb := newReviewDrainStack(t, b, nil)

	res, err := chain.Dispatch(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/overlap.txt",
		Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY), Mode: 0o644, HasMode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	bh := writebackHandleOf(res.Handle)
	if bh == nil {
		t.Fatal("expected bufferingHandle")
	}

	submitGate := make(chan struct{})
	bh.mu.Lock()
	bh.submitDone = submitGate
	bh.mu.Unlock()

	firstEntered := make(chan struct{})
	firstDone := make(chan struct{})
	go func() {
		close(firstEntered)
		_, _ = wb.markUnlinkedSnapshot("/overlap.txt", false)
		close(firstDone)
	}()
	<-firstEntered

	// Wait until the first caller has flipped bh.unlinked = true and is blocked
	// waiting on submitGate.
	deadline := time.Now().Add(2 * time.Second)
	for {
		bh.mu.Lock()
		unlinked := bh.unlinked
		bh.mu.Unlock()
		if unlinked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first markUnlinkedSnapshot did not flip unlinked")
		}
		time.Sleep(time.Millisecond)
	}

	secondDone := make(chan struct{})
	go func() {
		_, _ = wb.markUnlinkedSnapshot("/overlap.txt", false)
		close(secondDone)
	}()

	select {
	case <-secondDone:
		t.Fatal("second overlapping markUnlinkedSnapshot returned before submitDone closed")
	case <-time.After(30 * time.Millisecond):
	}

	bh.mu.Lock()
	bh.submitDone = nil
	bh.mu.Unlock()
	close(submitGate)

	select {
	case <-firstDone:
	case <-ctx.Done():
		t.Fatal("first markUnlinkedSnapshot did not complete")
	}
	select {
	case <-secondDone:
	case <-ctx.Done():
		t.Fatal("second markUnlinkedSnapshot did not complete")
	}
	_ = res.Handle.Release(ctx)
}

func TestWaitQueuedOrActiveDoesNotReturnEarlyDuringDrainAll(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	b := testbackend.NewBackend()
	enteredFirst, allowFirst := make(chan struct{}), make(chan struct{})
	var enteredOnce, allowOnce sync.Once
	unblock := func() { allowOnce.Do(func() { close(allowFirst) }) }
	defer unblock()

	var writeReqs atomic.Int32
	srv := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if (r.Method == http.MethodPut || r.Method == http.MethodPost) && writeReqs.Add(1) == 1 {
			enteredOnce.Do(func() { close(enteredFirst) })
			select {
			case <-allowFirst:
			case <-ctx.Done():
				return
			}
		}
		b.ServeHTTP(w, r)
	})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	cli := client.New(ts.URL, "test-key")
	cacheDir := t.TempDir()
	cache, err := staging.NewWriteBackCache(cacheDir)
	if err != nil {
		t.Fatal(err)
	}
	up := NewWriteBackUploader(cli, cache, 1)
	defer up.DrainAll()

	if err := cache.PutWithBaseRev("/first.txt", []byte("one"), 3, staging.PendingNew, 0); err != nil {
		t.Fatal(err)
	}
	if err := cache.PutWithBaseRev("/second.txt", []byte("two"), 3, staging.PendingNew, 0); err != nil {
		t.Fatal(err)
	}
	up.Submit("/first.txt")
	select {
	case <-enteredFirst:
	case <-ctx.Done():
		t.Fatal("first upload did not enter PUT")
	}
	up.Submit("/second.txt")

	drainStarted := make(chan struct{})
	drainDone := make(chan struct{})
	go func() {
		close(drainStarted)
		up.DrainAll()
		close(drainDone)
	}()
	<-drainStarted
	for !up.stopped.Load() {
		if time.Now().After(time.Now().Add(time.Second)) {
			break
		}
		time.Sleep(time.Millisecond)
	}

	waitDone := make(chan struct{})
	go func() {
		up.WaitQueuedOrActive("/second.txt")
		close(waitDone)
	}()

	select {
	case <-waitDone:
		t.Fatal("WaitQueuedOrActive(/second.txt) returned early while /second.txt was still queued during DrainAll")
	case <-time.After(30 * time.Millisecond):
	}

	unblock()
	select {
	case <-waitDone:
	case <-ctx.Done():
		t.Fatal("WaitQueuedOrActive(/second.txt) did not finish after unblock")
	}
	select {
	case <-drainDone:
	case <-ctx.Done():
		t.Fatal("DrainAll did not finish")
	}
	if node, ok := b.Node("/second.txt"); !ok || string(node.Data) != "two" {
		t.Fatalf("remote /second.txt = %q (ok=%v), want %q", node.Data, ok, "two")
	}
}

func TestRenameOverBatchQueuedTargetWaitsBeforeRemoteRename(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	b := testbackend.NewBackend()
	chain, wb := newReviewDrainStack(t, b, nil)
	wb.uploader.ConfigureBatchWrite(80*time.Millisecond, 8, 1<<20)

	resOld, err := chain.Dispatch(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/nodefs/replace-old.txt",
		Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY), Mode: 0o644, HasMode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := resOld.Handle.Write(ctx, 0, []byte("old\n")); err != nil {
		t.Fatal(err)
	}
	if err := resOld.Handle.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := resOld.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}

	resNew, err := chain.Dispatch(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/nodefs/replace-new.txt",
		Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY), Mode: 0o644, HasMode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := resNew.Handle.Write(ctx, 0, []byte("new\n")); err != nil {
		t.Fatal(err)
	}
	if err := resNew.Handle.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := chain.Dispatch(ctx, &fs.Op{
		Kind: fs.OpRename, Path: "/nodefs/replace-new.txt", Path2: "/nodefs/replace-old.txt",
	}); err != nil {
		t.Fatalf("rename replace-new -> replace-old: %v", err)
	}
	if err := resNew.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}

	if err := wb.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if reason, count := wb.DrainPending(); count != 0 {
		t.Fatalf("DrainPending = (%q, %d), want empty", reason, count)
	}
	node, ok := b.Node("/nodefs/replace-old.txt")
	if !ok || string(node.Data) != "new\n" {
		t.Fatalf("remote replace-old.txt = %q (ok=%v), want %q", node.Data, ok, "new\n")
	}
}

type releaseBlockingHandle struct {
	fs.Handle
	onRelease func()
}

func (r releaseBlockingHandle) Release(ctx context.Context) error {
	if r.onRelease != nil {
		r.onRelease()
	}
	if r.Handle != nil {
		return r.Handle.Release(ctx)
	}
	return nil
}
