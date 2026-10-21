package core

// codex twenty-third round: an HTTP 401 resets the mount view before the
// caller sees EACCES (the credential may now point at another tenant).

import (
	"context"
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

// A namespace row written by a single-field chown keeps the two presence
// flags apart: the unset field's preserve sentinel never becomes an owner.
func TestPortNamespaceStatKeepsSplitOwnerFlags(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	c, err := New(client.New(ts.URL, ""), Options{TrustLocalEvents: true})
	if err != nil {
		t.Fatal(err)
	}
	nc := dircache.NewNamespaceCache(time.Minute, time.Second, 0)
	nc.Put("/", []fs.DirEntryInfo{{
		Name: "f", Size: 5, Rev: 7, HasMode: true, Mode: 0o600,
		Uid: ^uint32(0), Gid: 2000, HasGID: true,
	}})
	c.Attach(nc)
	res, derr := c.Do(context.Background(), &fs.Op{Kind: fs.OpGetAttr, Path: "/f"}, nil)
	if derr != nil {
		t.Fatalf("getattr = %v", derr)
	}
	if res.Attr == nil || res.Attr.HasUID || !res.Attr.HasGID || res.Attr.Gid != 2000 {
		t.Fatalf("attr = %+v, want HasGID-only with gid 2000", res.Attr)
	}
}

// A core-only chain (no writeback) must retarget open terminal handles on a
// rename: the old fd's next write would otherwise recreate the old name and
// leave /moved stale.
func TestPortCoreRenameRetargetsOpenHandle(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/a", []byte("HELLO"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	c, err := New(client.New(ts.URL, ""), Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(c)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/a", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR)})
	if err != nil || res.Handle == nil {
		t.Fatalf("open = %v", err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/a", Path2: "/moved"}); err != nil {
		t.Fatalf("rename = %v", err)
	}
	if err := res.Handle.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatalf("write after rename = %v", err)
	}
	if err := res.Handle.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if node, ok := b.Node("/moved"); !ok || string(node.Data) != "XELLO" {
		t.Fatalf("/moved = %q ok=%v, want XELLO", node.Data, ok)
	}
	if _, ok := b.Node("/a"); ok {
		t.Fatal("old name was recreated by the open handle")
	}
}

// A 404 that belongs to a REPLACED view must not seed the namespace cache:
// the facade's reply fence comes too late, and the negative row would hide
// the new view's file for its TTL.
func TestPortStale404DoesNotSeedNegativeCache(t *testing.T) {
	var heads atomic.Int32
	gate := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && strings.HasSuffix(r.URL.Path, "/f") {
			if heads.Add(1) == 1 {
				once.Do(func() { close(gate) })
				<-release
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Length", "3")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "2")
			w.WriteHeader(http.StatusOK)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(ts.Close)
	bus := fs.NewEventBus()
	c, err := New(client.New(ts.URL, ""), Options{})
	if err != nil {
		t.Fatal(err)
	}
	c.AttachEvents(bus)
	c.Attach(dircache.NewNamespaceCache(time.Minute, time.Minute, 0))
	done := make(chan error, 1)
	go func() {
		_, derr := c.Do(context.Background(), &fs.Op{Kind: fs.OpGetAttr, Path: "/f"}, nil)
		done <- derr
	}()
	<-gate
	// Another request's 401 reset: the in-flight 404 is now stale.
	bus.Emit(fs.Event{Kind: fs.EventCheckout, Path: "/"})
	close(release)
	if err := <-done; !fs.IsNotFound(err) {
		t.Fatalf("stale 404 = %v, want not-found", err)
	}
	// The retry must reach the server again — a polluted negative row would
	// answer without a HEAD.
	res, err := c.Do(context.Background(), &fs.Op{Kind: fs.OpGetAttr, Path: "/f"}, nil)
	if err != nil || res.Attr == nil {
		t.Fatalf("second stat = %+v err=%v, want the new view's file", res.Attr, err)
	}
	if heads.Load() < 2 {
		t.Fatalf("HEAD count = %d, want the retried stat to revalidate", heads.Load())
	}
}

// blockingNegativeCache pauses inside RecordRemoteNotFound so a reset can be
// attempted mid-publication.
type blockingNegativeCache struct {
	inner   *dircache.DirCache
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingNegativeCache) LookupNamespace(parent, name string) (fs.DirEntryInfo, fs.LookupKind) {
	return b.inner.LookupNamespace(parent, name)
}

func (b *blockingNegativeCache) RecordRemoteNotFound(parent, name string) bool {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return b.inner.RecordRemoteNotFound(parent, name)
}

// The negative-row publication and the view reset share one boundary: a
// reset cannot sweep the cache between the epoch check and the insertion.
func TestPortNegativePublishAtomicWithReset(t *testing.T) {
	var heads atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && strings.HasSuffix(r.URL.Path, "/f") {
			if heads.Add(1) == 1 {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Length", "3")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "2")
			w.WriteHeader(http.StatusOK)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(ts.Close)
	bus := fs.NewEventBus()
	c, err := New(client.New(ts.URL, ""), Options{})
	if err != nil {
		t.Fatal(err)
	}
	c.AttachEvents(bus)
	bc := &blockingNegativeCache{
		inner:   dircache.NewNamespaceCache(time.Minute, time.Minute, 0),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	c.Attach(bc)
	done := make(chan error, 1)
	go func() {
		_, derr := c.Do(context.Background(), &fs.Op{Kind: fs.OpGetAttr, Path: "/f"}, nil)
		done <- derr
	}()
	<-bc.entered
	resetDone := make(chan struct{})
	go func() {
		// The facade's checkout handler: bump the epoch AND sweep the
		// namespace cache.
		bus.Emit(fs.Event{Kind: fs.EventCheckout, Path: "/"})
		bc.inner.InvalidateAll()
		close(resetDone)
	}()
	select {
	case <-resetDone:
		t.Fatal("reset completed while the negative row was being published")
	case <-time.After(100 * time.Millisecond):
	}
	close(bc.release)
	if err := <-done; !fs.IsNotFound(err) {
		t.Fatalf("stale 404 = %v, want not-found", err)
	}
	<-resetDone
	if item, kind := bc.LookupNamespace("/", "f"); kind == fs.LookupNegative {
		t.Fatalf("negative row survived the reset: %+v", item)
	}
	if _, err := c.Do(context.Background(), &fs.Op{Kind: fs.OpGetAttr, Path: "/f"}, nil); err != nil {
		t.Fatalf("second stat = %v, want the new view's file", err)
	}
	if heads.Load() < 2 {
		t.Fatalf("HEAD count = %d, want revalidation", heads.Load())
	}
}

// A core-only chain preserves the OLD object's bytes for descriptors whose
// name is replaced or removed: the terminal handle serves its anonymous
// snapshot.
func TestPortCorePreservesOverwrittenTarget(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/a", []byte("HELLO"))
	b.Seed("/b", []byte("OTHER"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	c, err := New(client.New(ts.URL, ""), Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(c)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/b", Flags: 0})
	if err != nil || res.Handle == nil {
		t.Fatalf("open target = %v", err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/a", Path2: "/b"}); err != nil {
		t.Fatalf("rename = %v", err)
	}
	buf := make([]byte, 16)
	n, rerr := res.Handle.Read(ctx, 0, buf)
	if rerr != nil {
		t.Fatal(rerr)
	}
	if got := string(buf[:n]); got != "OTHER" {
		t.Fatalf("overwritten target fd read %q, want OTHER", got)
	}
	_ = res.Handle.Release(ctx)
}

// The same for unlink + same-name recreate.
func TestPortCorePreservesUnlinkedObject(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/a", []byte("HELLO"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	c, err := New(client.New(ts.URL, ""), Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(c)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/a", Flags: 0})
	if err != nil || res.Handle == nil {
		t.Fatalf("open = %v", err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/a"}); err != nil {
		t.Fatalf("unlink = %v", err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/a", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR)}); err != nil {
		t.Fatalf("recreate = %v", err)
	}
	buf := make([]byte, 16)
	n, rerr := res.Handle.Read(ctx, 0, buf)
	if rerr != nil {
		t.Fatal(rerr)
	}
	if got := string(buf[:n]); got != "HELLO" {
		t.Fatalf("unlinked fd read %q after recreate, want HELLO", got)
	}
	_ = res.Handle.Release(ctx)
}

// A FAILED overwrite rename rolls the anonymous snapshots back: the target
// keeps its name, and its descriptors keep writing to the remote object.
func TestPortFailedRenameRestoresTargetHandle(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/a", []byte("HELLO"))
	b.Seed("/b", []byte("OTHER"))
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/b") && r.URL.Query().Has("rename") {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	c, err := New(client.New(ts.URL, ""), Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(c)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/b", Flags: uint32(syscall.O_RDWR)})
	if err != nil || res.Handle == nil {
		t.Fatalf("open = %v", err)
	}
	// The remote rename is rejected: the target keeps its name.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/a", Path2: "/b"}); err == nil {
		t.Fatal("rejected rename succeeded")
	}
	if err := res.Handle.Write(ctx, 0, []byte("AFTER")); err != nil {
		t.Fatalf("write after failed rename = %v", err)
	}
	if err := res.Handle.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if node, ok := b.Node("/b"); !ok || string(node.Data) != "AFTER" {
		t.Fatalf("/b = %q ok=%v, want AFTER (the failed rename must not anonymize it)", node.Data, ok)
	}
}

// A snapshot closed by a rollback while a read is in flight must not panic:
// readers keep the handle lock so Close waits, and the snapshot itself
// answers EOF once closed.
func TestPortAnonSnapshotCloseDuringReadIsSafe(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/x", []byte("HELLO-WORLD"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	c, err := New(client.New(ts.URL, ""), Options{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	snap, err := c.captureAnonSnapshot(ctx, "/x", 11)
	if err != nil {
		t.Fatal(err)
	}
	h := &SyncHandle{core: c, path: "/x", snap: snap}
	snap.Close()
	buf := make([]byte, 4)
	if n, rerr := h.Read(ctx, 6, buf); n != 0 || rerr != nil {
		t.Fatalf("read after close = %d, %v; want EOF", n, rerr)
	}
	// Concurrent close while reading repeatedly (no panic, no race).
	snap2, err := c.captureAnonSnapshot(ctx, "/x", 11)
	if err != nil {
		t.Fatal(err)
	}
	h2 := &SyncHandle{core: c, path: "/x", snap: snap2}
	done := make(chan struct{})
	go func() {
		defer close(done)
		small := make([]byte, 4)
		for i := 0; i < 200; i++ {
			_, _ = h2.Read(ctx, 2, small)
		}
	}()
	snap2.Close()
	<-done
}
