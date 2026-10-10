package cache

// Behavior rows for the directory-neighborhood prefetch trio (v1
// attribute_prefetch.go / directory_prefetch.go / readdir_prefetch.go):
// repeated lookup misses batch-stat the siblings into the namespace cache,
// a listing backgrounds sibling directory listings, and (gated) small file
// contents batch-read into the read cache — with local-write state
// (event-derived) excluded.

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/dircache"
)

type trioHarness struct {
	ch  *fs.Chain
	c   *Cache
	nc  *recordingNamespace
	b   *testbackend.Backend
	bus *fs.EventBus
}

// recordingNamespace wraps a real namespace cache so tests can observe
// upserts/puts.
type recordingNamespace struct {
	inner fs.DirListingCache
	puts  chan string
}

func (r *recordingNamespace) Get(dir string) ([]fs.DirEntryInfo, bool) {
	return r.inner.Get(dir)
}
func (r *recordingNamespace) Put(dir string, entries []fs.DirEntryInfo) {
	r.inner.Put(dir, entries)
	select {
	case r.puts <- dir:
	default:
	}
}
func (r *recordingNamespace) Upsert(parent string, entry fs.DirEntryInfo) {
	r.inner.Upsert(parent, entry)
	select {
	case r.puts <- parent + "/" + entry.Name:
	default:
	}
}
func (r *recordingNamespace) Invalidate(dir string)       { r.inner.Invalidate(dir) }
func (r *recordingNamespace) InvalidatePrefix(dir string) { r.inner.InvalidatePrefix(dir) }
func (r *recordingNamespace) InvalidateAll()              { r.inner.InvalidateAll() }

func newTrio(t *testing.T, content bool) *trioHarness {
	t.Helper()
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	bus := fs.NewEventBus()
	c, err := New(Options{
		MaxSize:            1 << 20,
		TTL:                time.Minute,
		PrefetchClient:     cl,
		RemoteRoot:         "/",
		DirContentPrefetch: content,
		Events:             bus,
	})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(c, terminal)
	if err != nil {
		t.Fatal(err)
	}
	nc := &recordingNamespace{inner: dircache.NewNamespaceCache(time.Minute, 0, 0), puts: make(chan string, 64)}
	c.SetNamespaceCache(nc)
	return &trioHarness{ch: ch, c: c, nc: nc, b: b, bus: bus}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}

func namespaceHas(nc *recordingNamespace, dir, name string, rev int64) bool {
	entries, ok := nc.Get(dir)
	if !ok {
		return false
	}
	for _, e := range entries {
		if e.Name == name && (rev == 0 || e.Rev == rev) {
			return true
		}
	}
	return false
}

// v1 attribute prefetch: two misses inside one directory activate a batch
// stat of the siblings, upserting them into the namespace cache (so the
// next lookup resolves without a remote round trip).
func TestTrioSiblingMetadataAfterMisses(t *testing.T) {
	h := newTrio(t, false)
	h.b.Seed("/d/sib", []byte("abc"))
	h.nc.Put("/d", []fs.DirEntryInfo{{Name: "sib", Size: 3}})

	ctx := context.Background()
	// Two distinct misses inside /d within the window trip the activation;
	// the backend answer (size 3, rev>0) upserts over the seed.
	for _, miss := range []string{"/d/x1", "/d/x2"} {
		_, _ = h.ch.Dispatch(ctx, &fs.Op{Kind: fs.OpLookup, Path: miss})
	}
	if !waitFor(t, 3*time.Second, func() bool { return namespaceHas(h.nc, "/d", "sib", 1) }) {
		t.Fatal("sibling metadata prefetch did not upsert /d/sib")
	}
}

// v1 directory prefetch: after /d/a lists, sibling /d/b's listing loads in
// the background and seeds the namespace cache.
func TestTrioSiblingDirectoryListing(t *testing.T) {
	h := newTrio(t, false)
	h.b.SeedDir("/d/a", 0o755)
	h.b.SeedDir("/d/b", 0o755)
	h.b.Seed("/d/b/inner", []byte("x"))
	h.nc.Put("/d", []fs.DirEntryInfo{{Name: "a", IsDir: true}, {Name: "b", IsDir: true}})
	h.nc.Put("/d/a", []fs.DirEntryInfo{})

	ctx := context.Background()
	if _, err := h.ch.Dispatch(ctx, &fs.Op{Kind: fs.OpList, Path: "/d/a"}); err != nil {
		t.Fatal(err)
	}
	if !waitFor(t, 3*time.Second, func() bool { return namespaceHas(h.nc, "/d/b", "inner", 0) }) {
		t.Fatal("sibling directory listing did not load /d/b")
	}
}

// v1 readdir content prefetch: after a listing, small files batch-read
// into the read cache and path-addressed reads serve from it; paths with
// un-landed local writes are excluded.
func TestTrioDirContentPrefetch(t *testing.T) {
	h := newTrio(t, true)
	h.b.SeedDir("/cd", 0o755)
	h.b.Seed("/cd/small.txt", []byte("tiny"))
	h.b.Seed("/cd/writing.txt", []byte("draft"))

	ctx := context.Background()
	// A path with un-landed local write state must not be prefetched.
	h.bus.Emit(fs.Event{Kind: fs.EventWrite, Path: "/cd/writing.txt"})

	if _, err := h.ch.Dispatch(ctx, &fs.Op{Kind: fs.OpList, Path: "/cd"}); err != nil {
		t.Fatal(err)
	}
	if !waitFor(t, 3*time.Second, func() bool {
		_, ok := h.c.mem.Get("/cd/small.txt", 0)
		return ok
	}) {
		t.Fatal("dir content prefetch did not cache /cd/small.txt")
	}
	if _, ok := h.c.mem.Get("/cd/writing.txt", 0); ok {
		t.Fatal("prefetch cached a path with un-landed local writes")
	}

	// Path-addressed reads now serve from the cache (no remote round trip
	// needed — the data equals the seeded bytes).
	res, err := h.ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/cd/small.txt"})
	if err != nil {
		t.Fatal(err)
	}
	if string(res.Data) != "tiny" {
		t.Fatalf("cached read = %q, want tiny", res.Data)
	}
}
