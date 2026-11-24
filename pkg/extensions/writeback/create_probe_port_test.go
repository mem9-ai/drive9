package writeback

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
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

func TestReadOpenFlushRacesWriterCloseDoesNotStrandZeroByteCache(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wbcache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{
		Policy:      PolicyWriteBack,
		Client:      cl,
		Staging:     mustStaging(t),
		WriteCache:  wbcache,
		LazyStaging: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	wb.wbCacheMaxBytes = 32 * 1024
	if err := wb.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wb.Stop(context.Background()) })
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	chain, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}

	payload := bytes.Repeat([]byte("s"), 64*1024)

	// 1. Deterministic reproduction of a late internal Flush/stageToWriteCache
	// arriving on a writer handle after Flush bypassed wbCache to the commit
	// queue, Release discarded h.buf, and the queue finished landing.
	resW, err := chain.Dispatch(ctx, &fs.Op{
		Kind:  fs.OpCreate,
		Path:  "/nodefs/small-stream.bin",
		Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY),
		Mode:  0o644, HasMode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	writer := writebackHandleOf(resW.Handle)
	if err := resW.Handle.Write(ctx, 0, payload); err != nil {
		t.Fatal(err)
	}
	if err := resW.Handle.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := resW.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatalf("initial Drain: %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("late internal Flush on closed writer: %v", err)
	}
	if err := writer.DrainFlush(ctx); err != nil {
		t.Fatalf("late DrainFlush on closed writer: %v", err)
	}
	if snap, err := writer.stageToWriteCache(ctx, stageV1Ladder); err != nil || snap != nil {
		t.Fatalf("late stageToWriteCache on closed writer = (%v, %v), want (nil, nil)", snap, err)
	}
	if reason, count := wb.DrainPending(); count != 0 {
		t.Fatalf("DrainPending after late Flush = (%q, %d), want empty", reason, count)
	}

	// 2. Deterministic barrier-forced mid-Flush / mid-Release overlap: hold
	// pathLocks("/nodefs/barrier-stream.bin") while the writer enters Flush,
	// queue an O_RDONLY open (which calls writer.Flush) and writer.Release
	// concurrently behind the active Flush, then release the barrier so all
	// three overlap directly across the staging/close handoff.
	{
		const barrierPath = "/nodefs/barrier-stream.bin"
		openRes, err := chain.Dispatch(ctx, &fs.Op{
			Kind:  fs.OpCreate,
			Path:  barrierPath,
			Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY),
			Mode:  0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		bh := writebackHandleOf(openRes.Handle)
		if err := openRes.Handle.Write(ctx, 0, payload); err != nil {
			t.Fatal(err)
		}
		rawUnlock := wb.pathLocks.lock(barrierPath)
		var unlockOnce sync.Once
		releaseBarrier := func() { unlockOnce.Do(rawUnlock) }
		defer releaseBarrier()
		flushDone := make(chan error, 1)
		go func() {
			flushDone <- openRes.Handle.Flush(ctx)
		}()
		waitFlushMuHeld(t, bh)
		releaseEntered := make(chan struct{})
		releaseDone := make(chan error, 1)
		go func() {
			close(releaseEntered)
			releaseDone <- openRes.Handle.Release(ctx)
		}()
		readEntered := make(chan struct{})
		readDone := make(chan error, 1)
		go func() {
			close(readEntered)
			rdRes, rerr := chain.Dispatch(ctx, &fs.Op{
				Kind:  fs.OpCreate,
				Path:  barrierPath,
				Flags: uint32(syscall.O_RDONLY),
			})
			if rerr != nil {
				readDone <- rerr
				return
			}
			if rdRes.Handle != nil {
				if ferr := rdRes.Handle.Flush(ctx); ferr != nil {
					_ = rdRes.Handle.Release(ctx)
					readDone <- ferr
					return
				}
				if cerr := rdRes.Handle.Release(ctx); cerr != nil {
					readDone <- cerr
					return
				}
			}
			readDone <- nil
		}()
		select {
		case <-releaseEntered:
		case <-ctx.Done():
			t.Fatal("Release contender did not start")
		}
		select {
		case <-readEntered:
		case <-ctx.Done():
			t.Fatal("reader contender did not start")
		}
		runtime.Gosched()
		releaseBarrier()
		if err := <-flushDone; err != nil {
			t.Fatalf("barrier writer Flush: %v", err)
		}
		if err := <-releaseDone; err != nil {
			t.Fatalf("barrier writer Release: %v", err)
		}
		if err := <-readDone; err != nil {
			t.Fatalf("barrier reader workflow: %v", err)
		}
		if err := wb.Drain(ctx); err != nil {
			t.Fatalf("barrier Drain: %v", err)
		}
		if reason, count := wb.DrainPending(); count != 0 {
			t.Fatalf("barrier DrainPending = (%q, %d), want empty", reason, count)
		}
	}

	// 3. Concurrent race loop: Node.js WriteStream emits 'finish' before
	// closing its fd while ReadStream opens O_RDONLY (calling h.Flush on
	// w.handleFor(path)) concurrently with FUSE_FLUSH + FUSE_RELEASE.
	for i := 0; i < 25; i++ {
		p := fmt.Sprintf("/nodefs/stream-%d.bin", i)
		openRes, err := chain.Dispatch(ctx, &fs.Op{
			Kind:  fs.OpCreate,
			Path:  p,
			Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY),
			Mode:  0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := openRes.Handle.Write(ctx, 0, payload); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			if ferr := openRes.Handle.Flush(ctx); ferr != nil {
				t.Errorf("Flush(%s): %v", p, ferr)
			}
			if rerr := openRes.Handle.Release(ctx); rerr != nil {
				t.Errorf("Release(%s): %v", p, rerr)
			}
		}()
		go func() {
			defer wg.Done()
			rdRes, rerr := chain.Dispatch(ctx, &fs.Op{
				Kind:  fs.OpCreate,
				Path:  p,
				Flags: uint32(syscall.O_RDONLY),
			})
			if rerr != nil {
				t.Errorf("read Open(%s): %v", p, rerr)
				return
			}
			if rdRes.Handle != nil {
				if ferr := rdRes.Handle.Flush(ctx); ferr != nil {
					t.Errorf("read Flush(%s): %v", p, ferr)
				}
				if cerr := rdRes.Handle.Release(ctx); cerr != nil {
					t.Errorf("read Release(%s): %v", p, cerr)
				}
			}
		}()
		wg.Wait()
	}

	if err := wb.Drain(ctx); err != nil {
		t.Fatalf("final Drain: %v", err)
	}
	if reason, count := wb.DrainPending(); count != 0 {
		t.Fatalf("final DrainPending = (%q, %d), want empty", reason, count)
	}
	_, _, _, _, _, _, _, _, uCached, uCachedBytes := wb.DrainSnapshot()
	if uCached != 0 || uCachedBytes != 0 {
		t.Fatalf("DrainSnapshot uploaderCached=%d bytes=%d, want 0/0", uCached, uCachedBytes)
	}
}

func TestReadOnlyOpenDoesNotShadowWriterOrSwallowChmod(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	b := testbackend.NewBackend()
	chain, wb := newReviewDrainStack(t, b, nil)

	resW, err := chain.Dispatch(ctx, &fs.Op{
		Kind:  fs.OpCreate,
		Path:  "/nodefs/mode.txt",
		Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY),
		Mode:  0o644, HasMode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	writer := writebackHandleOf(resW.Handle)
	if err := resW.Handle.Write(ctx, 0, []byte("mode\n")); err != nil {
		t.Fatal(err)
	}

	resR, err := chain.Dispatch(ctx, &fs.Op{
		Kind:  fs.OpCreate,
		Path:  "/nodefs/mode.txt",
		Flags: uint32(syscall.O_RDONLY),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := wb.handleFor("/nodefs/mode.txt"); got != writer {
		t.Fatalf("handleFor(/nodefs/mode.txt) after O_RDONLY open = %p, want writer %p", got, writer)
	}

	if err := resW.Handle.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := resW.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}

	// While resR (O_RDONLY) is still open, a path chmod must hit the remote /
	// cache track rather than deferring onto the clean reader handle.
	if _, err := chain.Dispatch(ctx, &fs.Op{
		Kind:    fs.OpSetAttr,
		Path:    "/nodefs/mode.txt",
		Mode:    0o600,
		HasMode: true,
		Size:    -1,
	}); err != nil {
		t.Fatalf("chmod with open reader: %v", err)
	}
	if err := resR.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	node, ok := b.Node("/nodefs/mode.txt")
	if !ok || node.Mode&0o777 != 0o600 {
		t.Fatalf("remote mode = %o (ok=%v), want 0600", node.Mode, ok)
	}
	if reason, count := wb.DrainPending(); count != 0 {
		t.Fatalf("DrainPending = (%q, %d), want empty", reason, count)
	}
}

func TestStageAndEnqueueRetiresCacheSnapshotAfterChmodGenBump(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wbcache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{
		Policy:     PolicyWriteBack,
		Client:     cl,
		Staging:    mustStaging(t),
		WriteCache: wbcache,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := wb.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wb.Stop(context.Background()) })
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	chain, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}

	res, err := chain.Dispatch(ctx, &fs.Op{
		Kind:  fs.OpCreate,
		Path:  "/nodefs/large-copy.bin",
		Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY),
		Mode:  0o644, HasMode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Stage a 0-byte snapshot into wbCache (e.g. ftruncate(0) + internal Flush),
	// bump Generation via chmod (preserving PutGeneration), then grow and Sync
	// so commitLocked -> stageAndEnqueueOpt retires the superseded cache entry.
	if _, err := chain.Dispatch(ctx, &fs.Op{
		Kind:    fs.OpSetAttr,
		Path:    "/nodefs/large-copy.bin",
		Handle:  res.Handle,
		Size:    0,
		HasSize: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := chain.Dispatch(ctx, &fs.Op{
		Kind:    fs.OpSetAttr,
		Path:    "/nodefs/large-copy.bin",
		Handle:  res.Handle,
		Mode:    0o600,
		HasMode: true,
		Size:    -1,
	}); err != nil {
		t.Fatal(err)
	}
	large := bytes.Repeat([]byte("x"), 64*1024)
	if err := res.Handle.Write(ctx, 0, large); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if reason, count := wb.DrainPending(); count != 0 {
		t.Fatalf("DrainPending = (%q, %d), want empty", reason, count)
	}
	node, ok := b.Node("/nodefs/large-copy.bin")
	if !ok || len(node.Data) != len(large) {
		t.Fatalf("remote /nodefs/large-copy.bin len=%d (ok=%v), want %d", len(node.Data), ok, len(large))
	}
}

func TestReleaseCommitLockedBlocksConcurrentFlushAndDrainFlush(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	b := testbackend.NewBackend()
	enteredPut := make(chan struct{})
	allowPut := make(chan struct{})
	var closeAllow sync.Once
	unblockPut := func() { closeAllow.Do(func() { close(allowPut) }) }
	defer unblockPut()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/v1/fs/release-barrier.txt" {
			select {
			case <-enteredPut:
			default:
				close(enteredPut)
			}
			select {
			case <-allowPut:
			case <-ctx.Done():
				http.Error(w, "canceled", http.StatusRequestTimeout)
				return
			}
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyCloseSync, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	chain, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}

	res, err := chain.Dispatch(ctx, &fs.Op{
		Kind:  fs.OpCreate,
		Path:  "/release-barrier.txt",
		Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY),
		Mode:  0o644, HasMode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	h := writebackHandleOf(res.Handle)
	if err := h.Write(ctx, 0, []byte("must-land-before-barriers")); err != nil {
		t.Fatal(err)
	}

	releaseDone := make(chan error, 1)
	go func() {
		releaseDone <- h.Release(ctx)
	}()
	select {
	case <-enteredPut:
	case <-ctx.Done():
		t.Fatal("Release did not enter inline PUT")
	}

	flushEntered := make(chan struct{})
	flushDone := make(chan error, 1)
	go func() {
		close(flushEntered)
		flushDone <- h.Flush(ctx)
	}()
	drainEntered := make(chan struct{})
	drainDone := make(chan error, 1)
	go func() {
		close(drainEntered)
		drainDone <- h.DrainFlush(ctx)
	}()
	select {
	case <-flushEntered:
	case <-ctx.Done():
		t.Fatal("Flush contender did not start")
	}
	select {
	case <-drainEntered:
	case <-ctx.Done():
		t.Fatal("DrainFlush contender did not start")
	}
	runtime.Gosched()

	select {
	case err := <-flushDone:
		_, exists := b.Node("/release-barrier.txt")
		t.Fatalf("concurrent Flush returned (%v) before Release commitLocked finished (remoteExists=%v)", err, exists)
	case err := <-drainDone:
		_, exists := b.Node("/release-barrier.txt")
		t.Fatalf("concurrent DrainFlush returned (%v) before Release commitLocked finished (remoteExists=%v)", err, exists)
	default:
	}

	unblockPut()
	if err := <-releaseDone; err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := <-flushDone; err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := <-drainDone; err != nil {
		t.Fatalf("DrainFlush: %v", err)
	}
	node, ok := b.Node("/release-barrier.txt")
	if !ok || string(node.Data) != "must-land-before-barriers" {
		t.Fatalf("remote /release-barrier.txt = %q (ok=%v), want %q", node.Data, ok, "must-land-before-barriers")
	}
}

func TestDrainFlushSynchronizesWithConcurrentWritebackFlushAndDebounce(t *testing.T) {
	t.Run("drainflush-vs-writecache-flush", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		b := testbackend.NewBackend()
		chain, wb := newReviewDrainStack(t, b, nil)

		res, err := chain.Dispatch(ctx, &fs.Op{
			Kind:  fs.OpCreate,
			Path:  "/drain-race.txt",
			Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY),
			Mode:  0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		h := writebackHandleOf(res.Handle)
		if err := h.Write(ctx, 0, []byte("drain-flush-race")); err != nil {
			t.Fatal(err)
		}

		// Hold wbCache's per-path lock so h.Flush blocks mid-stageToWriteCache
		// while DrainFlush contends on flushMu right behind it.
		pl := wb.wbCache.AcquirePathLock("/drain-race.txt")
		var unlockOnce sync.Once
		releaseCacheLock := func() { unlockOnce.Do(func() { wb.wbCache.ReleasePathLock("/drain-race.txt", pl) }) }
		defer releaseCacheLock()
		flushDone := make(chan error, 1)
		go func() {
			flushDone <- h.Flush(ctx)
		}()
		waitFlushMuHeld(t, h)
		drainEntered := make(chan struct{})
		drainDone := make(chan error, 1)
		go func() {
			close(drainEntered)
			drainDone <- wb.Drain(ctx)
		}()
		select {
		case <-drainEntered:
		case <-ctx.Done():
			t.Fatal("Drain contender did not start")
		}
		runtime.Gosched()
		releaseCacheLock()

		if err := <-flushDone; err != nil {
			t.Fatalf("Flush: %v", err)
		}
		if err := <-drainDone; err != nil {
			t.Fatalf("Drain: %v", err)
		}
		if reason, count := wb.DrainPending(); count != 0 {
			t.Fatalf("DrainPending = (%q, %d), want empty", reason, count)
		}
		node, ok := b.Node("/drain-race.txt")
		if !ok || string(node.Data) != "drain-flush-race" {
			t.Fatalf("remote /drain-race.txt = %q (ok=%v), want %q", node.Data, ok, "drain-flush-race")
		}
		if err := h.Release(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("drainflush-vs-release-writecache-refresh", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		b := testbackend.NewBackend()
		chain, wb := newReviewDrainStack(t, b, nil)

		res, err := chain.Dispatch(ctx, &fs.Op{
			Kind:  fs.OpCreate,
			Path:  "/drain-release-race.txt",
			Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY),
			Mode:  0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		h := writebackHandleOf(res.Handle)
		if err := h.Write(ctx, 0, []byte("v1")); err != nil {
			t.Fatal(err)
		}
		if err := h.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		if err := h.Write(ctx, 0, []byte("v2-final")); err != nil {
			t.Fatal(err)
		}

		// Hold wbCache's path lock while Release enters stageToWriteCache
		// (h.releasing == true, h.stagedToCache == false, h.closed == false),
		// then invoke DrainFlush concurrently so it must wait on flushMu rather
		// than racing commitLocked against Release's cache refresh + Submit.
		pl := wb.wbCache.AcquirePathLock("/drain-release-race.txt")
		var unlockOnce sync.Once
		releaseCacheLock := func() { unlockOnce.Do(func() { wb.wbCache.ReleasePathLock("/drain-release-race.txt", pl) }) }
		defer releaseCacheLock()
		releaseDone := make(chan error, 1)
		go func() {
			releaseDone <- h.Release(ctx)
		}()
		waitFlushMuHeld(t, h)
		drainEntered := make(chan struct{})
		drainFlushDone := make(chan error, 1)
		go func() {
			close(drainEntered)
			drainFlushDone <- h.DrainFlush(ctx)
		}()
		select {
		case <-drainEntered:
		case <-ctx.Done():
			t.Fatal("DrainFlush contender did not start")
		}
		runtime.Gosched()
		select {
		case err := <-drainFlushDone:
			t.Fatalf("DrainFlush returned (%v) while Release held flushMu", err)
		default:
		}
		releaseCacheLock()

		if err := <-releaseDone; err != nil {
			t.Fatalf("Release: %v", err)
		}
		if err := <-drainFlushDone; err != nil {
			t.Fatalf("DrainFlush: %v", err)
		}
		if err := wb.Drain(ctx); err != nil {
			t.Fatalf("Drain: %v", err)
		}
		if reason, count := wb.DrainPending(); count != 0 {
			t.Fatalf("DrainPending = (%q, %d), want empty", reason, count)
		}
		node, ok := b.Node("/drain-release-race.txt")
		if !ok || string(node.Data) != "v2-final" {
			t.Fatalf("remote /drain-release-race.txt = %q (ok=%v), want %q", node.Data, ok, "v2-final")
		}
	})

	t.Run("debounced-flush-after-release-does-not-reschedule", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		b := testbackend.NewBackend()
		ts := httptest.NewServer(b)
		t.Cleanup(ts.Close)
		cl := client.New(ts.URL, "")
		wb, err := New(Options{
			Policy:        PolicyWriteBack,
			Client:        cl,
			Staging:       mustStaging(t),
			FlushDebounce: 80 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := wb.Start(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wb.Stop(context.Background()) })
		terminal, err := core.New(cl, core.Options{})
		if err != nil {
			t.Fatal(err)
		}
		chain, err := fs.NewChain(wb, terminal)
		if err != nil {
			t.Fatal(err)
		}

		res, err := chain.Dispatch(ctx, &fs.Op{
			Kind:  fs.OpCreate,
			Path:  "/debounced.txt",
			Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY),
			Mode:  0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		h := writebackHandleOf(res.Handle)
		if err := h.Write(ctx, 0, []byte("debounced-payload")); err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			if rerr := h.Release(ctx); rerr != nil {
				t.Errorf("Release: %v", rerr)
			}
		}()
		go func() {
			defer wg.Done()
			if ferr := h.Flush(ctx); ferr != nil {
				t.Errorf("Flush: %v", ferr)
			}
		}()
		wg.Wait()
		if err := wb.Drain(ctx); err != nil {
			t.Fatalf("Drain: %v", err)
		}
		if reason, count := wb.DrainPending(); count != 0 {
			t.Fatalf("DrainPending = (%q, %d), want empty", reason, count)
		}
	})
}

type recordingMigratedHandle struct {
	mu                sync.Mutex
	flushCalls        int
	releaseCalls      int
	flushAfterRelease int
	lastFlushCtxErr   error
	onFlush           func()
}

func (m *recordingMigratedHandle) Read(context.Context, int64, []byte) (int, error) { return 0, nil }
func (m *recordingMigratedHandle) Write(context.Context, int64, []byte) error       { return nil }
func (m *recordingMigratedHandle) Sync(context.Context) error                       { return nil }
func (m *recordingMigratedHandle) Flush(ctx context.Context) error {
	if m.onFlush != nil {
		m.onFlush()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushCalls++
	m.lastFlushCtxErr = ctx.Err()
	if m.releaseCalls > 0 {
		m.flushAfterRelease++
	}
	return ctx.Err()
}
func (m *recordingMigratedHandle) Release(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseCalls++
	return nil
}

func TestMigratedHandleNeverFlushesAfterReleaseAndPreservesCallerContext(t *testing.T) {
	t.Run("no-post-release-flush-and-active-flush-gates-release", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		b := testbackend.NewBackend()
		chain, _ := newReviewDrainStack(t, b, nil)
		res, err := chain.Dispatch(ctx, &fs.Op{
			Kind:  fs.OpCreate,
			Path:  "/migrated.bin",
			Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY),
			Mode:  0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		h := writebackHandleOf(res.Handle)

		enteredFlush := make(chan struct{})
		allowFlush := make(chan struct{})
		var once sync.Once
		var releaseOnce sync.Once
		releaseGate := func() { releaseOnce.Do(func() { close(allowFlush) }) }
		defer releaseGate()
		rec := &recordingMigratedHandle{
			onFlush: func() {
				once.Do(func() {
					close(enteredFlush)
					<-allowFlush
				})
			},
		}
		h.mu.Lock()
		h.landed = true
		h.isNew = false
		h.extent = rec
		h.mu.Unlock()

		flushDone := make(chan error, 1)
		go func() {
			flushDone <- h.Flush(ctx)
		}()
		select {
		case <-enteredFlush:
		case <-ctx.Done():
			t.Fatal("migrated Flush did not reach the extent handle")
		}

		releaseEntered := make(chan struct{})
		releaseDone := make(chan error, 1)
		go func() {
			close(releaseEntered)
			releaseDone <- h.Release(ctx)
		}()
		select {
		case <-releaseEntered:
		case <-ctx.Done():
			t.Fatal("migrated Release contender did not start")
		}
		runtime.Gosched()
		select {
		case err := <-releaseDone:
			t.Fatalf("Release completed (%v) while migrated Flush was still active", err)
		default:
		}

		releaseGate()
		if err := <-flushDone; err != nil {
			t.Fatalf("active migrated Flush: %v", err)
		}
		if err := <-releaseDone; err != nil {
			t.Fatalf("migrated Release: %v", err)
		}

		// Late Flush and DrainFlush after Release must never invoke rec.Flush.
		if err := h.Flush(ctx); err != nil {
			t.Fatalf("late Flush after migrated Release: %v", err)
		}
		if err := h.DrainFlush(ctx); err != nil {
			t.Fatalf("late DrainFlush after migrated Release: %v", err)
		}
		rec.mu.Lock()
		flushes, releases, after := rec.flushCalls, rec.releaseCalls, rec.flushAfterRelease
		rec.mu.Unlock()
		if releases != 1 || flushes != 1 || after != 0 {
			t.Fatalf("migrated calls: flush=%d release=%d flushAfterRelease=%d, want 1/1/0", flushes, releases, after)
		}
	})

	t.Run("mid-closesync-flush-adoption-uses-live-caller-ctx", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		b := testbackend.NewBackend()
		wbcache, err := staging.NewWriteBackCache(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		rec := &recordingMigratedHandle{}
		bus := fs.NewEventBus()
		var h *bufferingHandle
		var adopted atomic.Bool
		off := bus.On(fs.EventLanded, func(ev fs.Event) {
			if ev.Path == "/closesync-adopt.bin" && h != nil && adopted.CompareAndSwap(false, true) {
				// EventLanded fires synchronously inside uploadSyncWithMetadata's
				// OnSuccess callback during PolicyCloseSync Flush — before
				// detachCancel() runs in LIFO defer order — so adoptExtentHandle
				// deterministically installs rec before Flush's deferred recheck.
				if !h.w.adoptExtentHandle(h, rec) {
					t.Errorf("adoptExtentHandle failed inside EventLanded")
				}
			}
		})
		defer off()
		ts := httptest.NewServer(b)
		t.Cleanup(ts.Close)
		cl := client.New(ts.URL, "")
		wb, err := New(Options{
			Policy:               PolicyCloseSync,
			Client:               cl,
			Events:               bus,
			WriteCache:           wbcache,
			InterruptibleCommits: false,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wb.Stop(context.Background()) })
		terminal, err := core.New(cl, core.Options{})
		if err != nil {
			t.Fatal(err)
		}
		chain, err := fs.NewChain(wb, terminal)
		if err != nil {
			t.Fatal(err)
		}
		res, err := chain.Dispatch(ctx, &fs.Op{
			Kind:  fs.OpCreate,
			Path:  "/closesync-adopt.bin",
			Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY),
			Mode:  0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		h = writebackHandleOf(res.Handle)
		// Keep an empty O_CREAT (ContentVersion == stagedContentVersion == 0) so
		// adoptExtentHandle succeeds deterministically inside EventLanded during
		// PolicyCloseSync Flush before detachCancel() runs in LIFO defer order.
		if err := h.Flush(ctx); err != nil {
			t.Fatalf("PolicyCloseSync Flush: %v", err)
		}
		if !adopted.Load() {
			t.Fatal("EventLanded did not adopt extent handle during PolicyCloseSync Flush")
		}
		rec.mu.Lock()
		flushes, ctxErr := rec.flushCalls, rec.lastFlushCtxErr
		rec.mu.Unlock()
		if flushes != 1 {
			t.Fatalf("deferred migrated Flush calls = %d, want 1", flushes)
		}
		if ctxErr != nil {
			t.Fatalf("migrated Flush observed cancelled context: %v", ctxErr)
		}
		if err := h.Release(ctx); err != nil {
			t.Fatalf("Release: %v", err)
		}
		rec.mu.Lock()
		after := rec.flushAfterRelease
		rec.mu.Unlock()
		if after != 0 {
			t.Fatalf("flushAfterRelease = %d, want 0", after)
		}
	})
}

func TestHardlinkUnlinkSurvivorPropagatesUploadSyncFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	b := testbackend.NewBackend()
	b.Seed("/link1", []byte("orig"))

	var failPut atomic.Bool
	var deleteCalls atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/v1/fs/link1" && failPut.Load() {
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
			return
		}
		if r.Method == http.MethodDelete && r.URL.Path == "/v1/fs/link1" {
			deleteCalls.Add(1)
		}
		b.ServeHTTP(w, r)
	})
	chain, wb := newReviewDrainStack(t, handler, nil)
	if _, err := chain.Dispatch(ctx, &fs.Op{
		Kind:  fs.OpLink,
		Path:  "/link1",
		Path2: "/link2",
	}); err != nil {
		t.Fatalf("OpLink(/link1 -> /link2): %v", err)
	}
	wb.SetSurvivorLookup(func(path string) (string, bool) {
		if path == "/link1" {
			return "/link2", true
		}
		return "", false
	})

	res, err := chain.Dispatch(ctx, &fs.Op{
		Kind:  fs.OpCreate,
		Path:  "/link1",
		Flags: uint32(syscall.O_WRONLY),
	})
	if err != nil {
		t.Fatal(err)
	}
	h := writebackHandleOf(res.Handle)
	if err := h.Write(ctx, 0, []byte("updated-bytes")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	failPut.Store(true)

	// First unlink hits 503 during UploadSync: must fail without deleting
	// /link1 remotely or stranding wbCache["/link1"] under a deleted alias.
	if _, err := chain.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/link1"}); err == nil {
		t.Fatal("expected OpUnlink(/link1) to fail when UploadSync returns 503")
	}
	if got := deleteCalls.Load(); got != 0 {
		t.Fatalf("DELETE /link1 calls on failed UploadSync = %d, want 0", got)
	}
	if !wb.wbCache.HasMeta("/link1") {
		t.Fatal("wbCache[/link1] should remain staged for retry after failed unlink")
	}

	// Clear the 503 fault and retry: UploadSync lands updated-bytes, /link1 is
	// deleted remotely, h retargets onto /link2, and /link2 serves updated-bytes.
	failPut.Store(false)
	if _, err := chain.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/link1"}); err != nil {
		t.Fatalf("retry OpUnlink(/link1): %v", err)
	}
	if got := deleteCalls.Load(); got != 1 {
		t.Fatalf("DELETE /link1 calls after retry = %d, want 1", got)
	}
	if _, exists := b.Node("/link1"); exists {
		t.Fatal("/link1 still exists remotely after retry OpUnlink")
	}
	node2, ok := b.Node("/link2")
	if !ok || string(node2.Data) != "updated-bytes" {
		t.Fatalf("remote /link2 = %q (ok=%v), want %q", node2.Data, ok, "updated-bytes")
	}
	h.mu.Lock()
	retargetedPath := h.path
	h.mu.Unlock()
	if retargetedPath != "/link2" {
		t.Fatalf("writer path after survivor unlink = %q, want /link2", retargetedPath)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if reason, count := wb.DrainPending(); count != 0 {
		t.Fatalf("DrainPending = (%q, %d), want empty", reason, count)
	}
}

func waitFlushMuHeld(t *testing.T, h *bufferingHandle) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !h.flushMu.TryLock() {
			return
		}
		h.flushMu.Unlock()
		runtime.Gosched()
	}
	t.Fatal("timed out waiting for contender to acquire flushMu")
}

func TestConcurrentDrainFlushWaitsForInFlightCacheUploadOutcome(t *testing.T) {
	for _, failFirstPut := range []bool{false, true} {
		name := "shared-success"
		if failFirstPut {
			name = "shared-failure"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			b := testbackend.NewBackend()
			enteredPut := make(chan struct{})
			allowPut := make(chan struct{})
			var enterOnce, allowOnce sync.Once
			releaseGate := func() { allowOnce.Do(func() { close(allowPut) }) }
			defer releaseGate()

			var failPut atomic.Bool
			failPut.Store(failFirstPut)

			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPut && r.URL.Path == "/v1/fs/two-drains.txt" {
					enterOnce.Do(func() { close(enteredPut) })
					select {
					case <-allowPut:
					case <-r.Context().Done():
						return
					}
					if failPut.Load() {
						http.Error(w, "service unavailable", http.StatusServiceUnavailable)
						return
					}
				}
				b.ServeHTTP(w, r)
			})

			ts := httptest.NewServer(handler)
			t.Cleanup(ts.Close)
			cl := client.New(ts.URL, "")
			wbcache, err := staging.NewWriteBackCache(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			// Pure write-cache stack (no commit queue) plus clean staged handle:
			// a second DrainFlush must still block on the first DrainFlush's
			// synchronous wbCache upload and share its success/error outcome.
			wb, err := New(Options{
				Policy:     PolicyWriteBack,
				Client:     cl,
				WriteCache: wbcache,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = wb.Stop(context.Background()) })
			terminal, err := core.New(cl, core.Options{})
			if err != nil {
				t.Fatal(err)
			}
			chain, err := fs.NewChain(wb, terminal)
			if err != nil {
				t.Fatal(err)
			}

			res, err := chain.Dispatch(ctx, &fs.Op{
				Kind:  fs.OpCreate,
				Path:  "/two-drains.txt",
				Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY),
				Mode:  0o644, HasMode: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			h := writebackHandleOf(res.Handle)
			if err := h.Write(ctx, 0, []byte("two-drains-payload")); err != nil {
				t.Fatal(err)
			}
			if err := h.Flush(ctx); err != nil {
				t.Fatal(err)
			}

			drainADone := make(chan error, 1)
			go func() {
				drainADone <- h.DrainFlush(ctx)
			}()
			select {
			case <-enteredPut:
			case <-ctx.Done():
				t.Fatal("DrainFlush A did not enter synchronous PUT")
			}

			drainBEntered := make(chan struct{})
			drainBDone := make(chan error, 1)
			go func() {
				close(drainBEntered)
				drainBDone <- h.DrainFlush(ctx)
			}()
			select {
			case <-drainBEntered:
			case <-ctx.Done():
				t.Fatal("DrainFlush B did not start")
			}
			runtime.Gosched()

			select {
			case berr := <-drainBDone:
				t.Fatalf("concurrent DrainFlush B returned (%v) while DrainFlush A was still blocked in PUT", berr)
			default:
			}

			releaseGate()
			errA := <-drainADone
			errB := <-drainBDone

			if !failFirstPut {
				if errA != nil || errB != nil {
					t.Fatalf("DrainFlush A/B = (%v, %v), want (nil, nil)", errA, errB)
				}
				node, ok := b.Node("/two-drains.txt")
				if !ok || string(node.Data) != "two-drains-payload" {
					t.Fatalf("remote /two-drains.txt = %q (ok=%v), want %q", node.Data, ok, "two-drains-payload")
				}
			} else {
				if errA == nil || errB == nil {
					t.Fatalf("DrainFlush A/B on 503 = (%v, %v), want both non-nil", errA, errB)
				}
				h.mu.Lock()
				restored := h.stagedToCache
				h.mu.Unlock()
				if !restored {
					t.Fatal("failed DrainFlush did not restore stagedToCache for retry")
				}
				failPut.Store(false)
				if err := h.DrainFlush(ctx); err != nil {
					t.Fatalf("retry DrainFlush after clearing 503: %v", err)
				}
				node, ok := b.Node("/two-drains.txt")
				if !ok || string(node.Data) != "two-drains-payload" {
					t.Fatalf("remote /two-drains.txt after retry = %q (ok=%v), want %q", node.Data, ok, "two-drains-payload")
				}
			}

			if err := h.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if err := wb.Drain(ctx); err != nil {
				t.Fatal(err)
			}
			if reason, count := wb.DrainPending(); count != 0 {
				t.Fatalf("DrainPending = (%q, %d), want empty", reason, count)
			}
		})
	}
}

func TestHardlinkUnlinkSurvivorPreservesNewerFlushDuringUpload(t *testing.T) {
	t.Run("newer-write-flush-during-survivor-upload", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		b := testbackend.NewBackend()
		b.Seed("/link1", []byte("orig"))

		enteredPut := make(chan struct{})
		allowPut := make(chan struct{})
		var enterOnce, allowOnce sync.Once
		releaseGate := func() { allowOnce.Do(func() { close(allowPut) }) }
		defer releaseGate()

		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPut && r.URL.Path == "/v1/fs/link1" {
				enterOnce.Do(func() {
					close(enteredPut)
					<-allowPut
				})
			}
			b.ServeHTTP(w, r)
		})
		chain, wb := newReviewDrainStack(t, handler, nil)
		if _, err := chain.Dispatch(ctx, &fs.Op{
			Kind:  fs.OpLink,
			Path:  "/link1",
			Path2: "/link2",
		}); err != nil {
			t.Fatalf("OpLink(/link1 -> /link2): %v", err)
		}
		wb.SetSurvivorLookup(func(path string) (string, bool) {
			if path == "/link1" {
				return "/link2", true
			}
			return "", false
		})

		res, err := chain.Dispatch(ctx, &fs.Op{
			Kind:  fs.OpCreate,
			Path:  "/link1",
			Flags: uint32(syscall.O_WRONLY),
		})
		if err != nil {
			t.Fatal(err)
		}
		h := writebackHandleOf(res.Handle)
		if err := h.Write(ctx, 0, []byte("v1-older")); err != nil {
			t.Fatal(err)
		}
		if err := h.Flush(ctx); err != nil {
			t.Fatal(err)
		}

		unlinkDone := make(chan error, 1)
		go func() {
			_, uerr := chain.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/link1"})
			unlinkDone <- uerr
		}()
		select {
		case <-enteredPut:
		case <-ctx.Done():
			t.Fatal("survivor OpUnlink did not enter PUT /link1")
		}

		// While the older generation is paused in PUT /link1, the open writer
		// writes and flushes a newer generation on the same handle.
		if err := h.Write(ctx, 0, []byte("v2-newer-after-flush")); err != nil {
			t.Fatalf("Write during survivor PUT: %v", err)
		}
		if err := h.Flush(ctx); err != nil {
			t.Fatalf("Flush during survivor PUT: %v", err)
		}

		releaseGate()
		if err := <-unlinkDone; err != nil {
			t.Fatalf("OpUnlink(/link1): %v", err)
		}
		if err := h.Release(ctx); err != nil {
			t.Fatalf("Release retargeted writer: %v", err)
		}
		if err := wb.Drain(ctx); err != nil {
			t.Fatalf("Drain: %v", err)
		}
		if reason, count := wb.DrainPending(); count != 0 {
			t.Fatalf("DrainPending = (%q, %d), want empty", reason, count)
		}
		if _, exists := b.Node("/link1"); exists {
			t.Fatal("/link1 still exists remotely after OpUnlink + Release + Drain")
		}
		node2, ok := b.Node("/link2")
		if !ok || string(node2.Data) != "v2-newer-after-flush" {
			t.Fatalf("remote /link2 = %q (ok=%v), want %q", node2.Data, ok, "v2-newer-after-flush")
		}
	})

	t.Run("staged-replacement-retires-terminal-failure", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		b := testbackend.NewBackend()
		b.Seed("/link1", []byte("orig"))
		ts := httptest.NewServer(b)
		t.Cleanup(ts.Close)
		cl := client.New(ts.URL, "")
		wbcache, err := staging.NewWriteBackCache(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		wb, err := New(Options{
			Policy:     PolicyWriteBack,
			Client:     cl,
			Staging:    mustStaging(t),
			WriteCache: wbcache,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := wb.Start(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wb.Stop(context.Background()) })
		terminal, err := core.New(cl, core.Options{})
		if err != nil {
			t.Fatal(err)
		}
		chain, err := fs.NewChain(wb, terminal)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := chain.Dispatch(ctx, &fs.Op{
			Kind:  fs.OpLink,
			Path:  "/link1",
			Path2: "/link2",
		}); err != nil {
			t.Fatal(err)
		}
		wb.SetSurvivorLookup(func(path string) (string, bool) {
			if path == "/link1" {
				return "/link2", true
			}
			return "", false
		})

		res, err := chain.Dispatch(ctx, &fs.Op{
			Kind:  fs.OpCreate,
			Path:  "/link1",
			Flags: uint32(syscall.O_WRONLY),
		})
		if err != nil {
			t.Fatal(err)
		}
		h := writebackHandleOf(res.Handle)
		wb.queue.RecordTerminalFailureForPath("/link1", errors.New("prior failure"))
		if err := h.Write(ctx, 0, []byte("replacement-bytes")); err != nil {
			t.Fatal(err)
		}
		if err := h.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := chain.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/link1"}); err != nil {
			t.Fatalf("OpUnlink(/link1) with staged replacement: %v", err)
		}
		if err := h.Release(ctx); err != nil {
			t.Fatal(err)
		}
		if err := wb.Drain(ctx); err != nil {
			t.Fatal(err)
		}
		node2, ok := b.Node("/link2")
		if !ok || string(node2.Data) != "replacement-bytes" {
			t.Fatalf("remote /link2 = %q (ok=%v), want %q", node2.Data, ok, "replacement-bytes")
		}
	})
}

func TestDrainFlushRetargetDuringPausedCacheUploadSettlesUploadedPath(t *testing.T) {
	t.Run("success-retarget-settles-uploaded-path-only", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		b := testbackend.NewBackend()
		enteredPut := make(chan struct{})
		allowPut := make(chan struct{})
		var enterOnce, allowOnce sync.Once
		releaseGate := func() { allowOnce.Do(func() { close(allowPut) }) }
		defer releaseGate()

		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPut && r.URL.Path == "/v1/fs/old-drain.txt" {
				enterOnce.Do(func() { close(enteredPut) })
				select {
				case <-allowPut:
				case <-r.Context().Done():
					return
				}
			}
			b.ServeHTTP(w, r)
		})
		ts := httptest.NewServer(handler)
		t.Cleanup(ts.Close)
		cl := client.New(ts.URL, "")
		wbcache, err := staging.NewWriteBackCache(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		wb, err := New(Options{
			Policy:     PolicyWriteBack,
			Client:     cl,
			Staging:    mustStaging(t),
			WriteCache: wbcache,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := wb.Start(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wb.Stop(context.Background()) })
		terminal, err := core.New(cl, core.Options{})
		if err != nil {
			t.Fatal(err)
		}
		chain, err := fs.NewChain(wb, terminal)
		if err != nil {
			t.Fatal(err)
		}

		wb.queue.RecordTerminalFailureForPath("/old-drain.txt", errors.New("old failure"))
		wb.queue.RecordTerminalFailureForPath("/new-drain.txt", errors.New("new failure must stay"))

		res, err := chain.Dispatch(ctx, &fs.Op{
			Kind:  fs.OpCreate,
			Path:  "/old-drain.txt",
			Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY),
			Mode:  0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		h := writebackHandleOf(res.Handle)
		if err := h.Write(ctx, 0, []byte("old-drain-bytes")); err != nil {
			t.Fatal(err)
		}
		if err := h.Flush(ctx); err != nil {
			t.Fatal(err)
		}

		drainDone := make(chan error, 1)
		go func() {
			drainDone <- h.DrainFlush(ctx)
		}()
		select {
		case <-enteredPut:
		case <-ctx.Done():
			t.Fatal("DrainFlush did not enter PUT /old-drain.txt")
		}

		// Retarget h to /new-drain.txt while /old-drain.txt's PUT is paused.
		wb.retarget("/old-drain.txt", "/new-drain.txt")
		releaseGate()
		if err := <-drainDone; err != nil {
			t.Fatalf("DrainFlush: %v", err)
		}

		if ferr := wb.queue.PathTerminalFailure("/old-drain.txt"); ferr != nil {
			t.Fatalf("expected /old-drain.txt terminal failure retired, got %v", ferr)
		}
		if ferr := wb.queue.PathTerminalFailure("/new-drain.txt"); ferr == nil {
			t.Fatal("expected /new-drain.txt terminal failure preserved across /old-drain.txt settlement")
		}
		h.mu.Lock()
		landedOnNew := h.landed
		h.mu.Unlock()
		if landedOnNew {
			t.Fatal("old-path upload settlement must not mark retargeted handle landed on /new-drain.txt")
		}
		wb.markUnlinked("/new-drain.txt", false)
		if err := h.Release(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("failure-retarget-does-not-corrupt-new-path-staging", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		b := testbackend.NewBackend()
		enteredPut := make(chan struct{})
		allowPut := make(chan struct{})
		var enterOnce, allowOnce sync.Once
		releaseGate := func() { allowOnce.Do(func() { close(allowPut) }) }
		defer releaseGate()

		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPut && r.URL.Path == "/v1/fs/old-fail.txt" {
				enterOnce.Do(func() { close(enteredPut) })
				select {
				case <-allowPut:
				case <-r.Context().Done():
					return
				}
				http.Error(w, "service unavailable", http.StatusServiceUnavailable)
				return
			}
			b.ServeHTTP(w, r)
		})
		chain, wb := newReviewDrainStack(t, handler, nil)

		res, err := chain.Dispatch(ctx, &fs.Op{
			Kind:  fs.OpCreate,
			Path:  "/old-fail.txt",
			Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY),
			Mode:  0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		h := writebackHandleOf(res.Handle)
		if err := h.Write(ctx, 0, []byte("old-fail-bytes")); err != nil {
			t.Fatal(err)
		}
		if err := h.Flush(ctx); err != nil {
			t.Fatal(err)
		}

		drainDone := make(chan error, 1)
		go func() {
			drainDone <- h.DrainFlush(ctx)
		}()
		select {
		case <-enteredPut:
		case <-ctx.Done():
			t.Fatal("DrainFlush did not enter PUT /old-fail.txt")
		}

		wb.retarget("/old-fail.txt", "/new-fail.txt")
		releaseGate()
		if err := <-drainDone; err == nil {
			t.Fatal("expected DrainFlush on 503 to return error")
		}

		h.mu.Lock()
		stagedOnNew := h.stagedToCache
		h.mu.Unlock()
		if stagedOnNew {
			t.Fatal("failed /old-fail.txt upload must not set stagedToCache on retargeted /new-fail.txt handle")
		}
		wb.wbCache.Remove("/old-fail.txt")
		wb.markUnlinked("/new-fail.txt", false)
		if err := h.Release(ctx); err != nil {
			t.Fatal(err)
		}
	})
}
