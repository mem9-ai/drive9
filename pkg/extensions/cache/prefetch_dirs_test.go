package cache

// Behavior rows for the directory-neighborhood prefetch trio (v1
// attribute_prefetch.go / directory_prefetch.go / readdir_prefetch.go):
// repeated lookup misses batch-stat the siblings into the namespace cache,
// a listing backgrounds sibling directory listings, and (gated) small file
// contents batch-read into the read cache — with local-write state
// (event-derived) excluded.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
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

// ---------------------------------------------------------------------------
// Pure-unit rows ported from v1 attribute_prefetch_test.go /
// directory_prefetch_test.go (d39a6d1b3): the miss window, the activation
// cooldown, the 256-path batch-stat cap, the tracked-directory bound, and
// sibling-directory scheduling caps. These drive the dirPrefetcher directly;
// the v1 rows that rode the mount view / singleflight joining (conditional
// listing installs, SSE-reconnect fencing, foreground joins) have no
// counterpart in this package and stay behind.

// batchStatRecorder serves POST /v1/fs:batch-stat and counts calls and the
// per-call path counts, so activation rows can be asserted offline.
type batchStatRecorder struct {
	mu     sync.Mutex
	calls  int
	paths  []int
	server *httptest.Server
}

func newBatchStatRecorder(t *testing.T) *batchStatRecorder {
	t.Helper()
	rec := &batchStatRecorder{}
	rec.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Paths []string `json:"paths"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		rec.mu.Lock()
		rec.calls++
		rec.paths = append(rec.paths, len(req.Paths))
		rec.mu.Unlock()
		results := make([]map[string]any, 0, len(req.Paths))
		for _, p := range req.Paths {
			results = append(results, map[string]any{"path": p, "status": 200, "size": int64(3), "isDir": false, "revision": int64(1)})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
	}))
	t.Cleanup(rec.server.Close)
	return rec
}

func (rec *batchStatRecorder) snapshot() (int, int) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	total := 0
	for _, n := range rec.paths {
		total += n
	}
	return rec.calls, total
}

// newUnitDirPrefetcher builds a dirPrefetcher over a real namespace cache and
// the counting batch-stat recorder. The cache is wrapped opaquely (no
// LookupNamespace) so misses count toward activation — a complete listing
// would otherwise answer every miss; the answered-miss row below covers that
// interplay against the raw cache.
func newUnitDirPrefetcher(t *testing.T) (*dirPrefetcher, *dircache.DirCache, *batchStatRecorder) {
	t.Helper()
	rec := newBatchStatRecorder(t)
	p := newDirPrefetcher(client.New(rec.server.URL, ""), "/")
	nc := dircache.NewNamespaceCache(time.Minute, time.Minute, 0)
	p.namespace = opaqueNamespace{inner: nc}
	return p, nc, rec
}

// opaqueNamespace forwards only fs.DirListingCache, hiding LookupNamespace so
// the type assertion in answeredFromNamespace fails and misses count.
type opaqueNamespace struct{ inner *dircache.DirCache }

func (o opaqueNamespace) Get(dir string) ([]fs.DirEntryInfo, bool) { return o.inner.Get(dir) }
func (o opaqueNamespace) Put(dir string, entries []fs.DirEntryInfo) {
	o.inner.Put(dir, entries)
}
func (o opaqueNamespace) Upsert(parent string, entry fs.DirEntryInfo) {
	o.inner.Upsert(parent, entry)
}
func (o opaqueNamespace) Invalidate(dir string)       { o.inner.Invalidate(dir) }
func (o opaqueNamespace) InvalidatePrefix(dir string) { o.inner.InvalidatePrefix(dir) }
func (o opaqueNamespace) InvalidateAll()              { o.inner.InvalidateAll() }

func waitForDirPrefetchIdle(t *testing.T, p *dirPrefetcher) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		p.mu.Lock()
		remaining := len(p.dirLoads) + p.busy
		p.mu.Unlock()
		if remaining == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("dir prefetch still busy: %d dirLoads", remaining)
		}
		time.Sleep(time.Millisecond)
	}
}

// v1 TestLookupSingleMissDoesNotPrefetchDirectory: one miss inside a
// directory never trips the batch-stat activation; two distinct misses inside
// the 1s window do (v1 TestLookupSecondSiblingMissPrefetchesDirectory).
func TestMetaPrefetchMissWindowActivatesOnSecondMiss(t *testing.T) {
	p, nc, rec := newUnitDirPrefetcher(t)
	nc.Put("/d", []fs.DirEntryInfo{{Name: "sib"}})
	ctx := context.Background()

	p.noteMiss(ctx, "/d/x1")
	time.Sleep(150 * time.Millisecond)
	if calls, _ := rec.snapshot(); calls != 0 {
		t.Fatalf("batch-stat calls after single miss = %d, want 0", calls)
	}

	p.noteMiss(ctx, "/d/x2")
	if !waitFor(t, 3*time.Second, func() bool {
		calls, _ := rec.snapshot()
		return calls == 1
	}) {
		t.Fatal("second miss in window did not activate sibling metadata prefetch")
	}
	// The activated refresh consulted the directory's siblings.
	if calls, paths := rec.snapshot(); calls != 1 || paths == 0 {
		t.Fatalf("activated batch-stat = (%d calls, %d paths), want 1 call with siblings", calls, paths)
	}
}

// v1 cooldown semantics (metadataPrefetchCooldown): misses arriving inside
// the 1s cooldown after an activation must not re-fire the batch-stat.
func TestMetaPrefetchCooldownSuppressesRefire(t *testing.T) {
	p, nc, rec := newUnitDirPrefetcher(t)
	nc.Put("/d", []fs.DirEntryInfo{{Name: "sib"}})
	ctx := context.Background()

	p.noteMiss(ctx, "/d/x1")
	p.noteMiss(ctx, "/d/x2")
	if !waitFor(t, 3*time.Second, func() bool {
		calls, _ := rec.snapshot()
		return calls == 1
	}) {
		t.Fatal("miss pair did not activate sibling metadata prefetch")
	}

	// A fresh miss pair inside the cooldown window stays suppressed.
	p.noteMiss(ctx, "/d/y1")
	p.noteMiss(ctx, "/d/y2")
	time.Sleep(250 * time.Millisecond)
	if calls, _ := rec.snapshot(); calls != 1 {
		t.Fatalf("batch-stat calls inside cooldown = %d, want 1 (no refire)", calls)
	}
}

// v1 TestLargeDirectorySiblingPrefetchUsesBoundedBatchStat: sibling refresh
// caps the hot-path set at client.MaxBatchStatPaths (256).
func TestMetaPrefetchCapsHotPathsAtBatchStatLimit(t *testing.T) {
	p, nc, rec := newUnitDirPrefetcher(t)
	siblings := make([]fs.DirEntryInfo, 0, 300)
	for i := range 300 {
		siblings = append(siblings, fs.DirEntryInfo{Name: fmt.Sprintf("f%03d", i)})
	}
	nc.Put("/big", siblings)

	p.refreshSiblingMetadata(context.Background(), "/big")

	if !waitFor(t, 3*time.Second, func() bool {
		calls, _ := rec.snapshot()
		return calls == 1
	}) {
		t.Fatal("sibling metadata refresh did not fire")
	}
	_, paths := rec.snapshot()
	if paths != 256 {
		t.Fatalf("batch-stat paths = %d, want capped at %d", paths, 256)
	}
}

// v1 metadataPrefetchMaxTrackedDirs: the tracked-directory set is bounded;
// the oldest window is evicted once the bound is reached.
func TestMetaPrefetchTrackedDirsAreBounded(t *testing.T) {
	p, _, _ := newUnitDirPrefetcher(t)
	ctx := context.Background()
	for i := range metaPrefetchMaxTrackedDirs + 1 {
		p.noteMiss(ctx, fmt.Sprintf("/d%03d/x", i))
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.metaDirs) > metaPrefetchMaxTrackedDirs {
		t.Fatalf("tracked dirs = %d, want <= %d", len(p.metaDirs), metaPrefetchMaxTrackedDirs)
	}
	if _, ok := p.metaDirs[fmt.Sprintf("/d%03d", metaPrefetchMaxTrackedDirs)]; !ok {
		t.Fatal("newest tracked directory was evicted")
	}
}

// Misses the namespace cache already answered (a fresh negative row) do not
// count toward activation — re-counting them would re-fire a redundant
// batch-stat on every cooldown (v1 noteMiss semantics).
func TestMetaPrefetchIgnoresNamespaceAnsweredMisses(t *testing.T) {
	rec := newBatchStatRecorder(t)
	p := newDirPrefetcher(client.New(rec.server.URL, ""), "/")
	// The raw cache exposes LookupNamespace: a fresh negative row answers the
	// lookup miss, so it must not count toward activation.
	nc := dircache.NewNamespaceCache(time.Minute, time.Minute, 0)
	p.namespace = nc
	nc.Put("/d", []fs.DirEntryInfo{{Name: "sib"}})
	nc.MarkNegative("/d", "x1")
	nc.MarkNegative("/d", "x2")

	p.noteMiss(context.Background(), "/d/x1")
	p.noteMiss(context.Background(), "/d/x2")
	time.Sleep(150 * time.Millisecond)

	if calls, _ := rec.snapshot(); calls != 0 {
		t.Fatalf("batch-stat calls for namespace-answered misses = %d, want 0", calls)
	}
	p.mu.Lock()
	tracked := len(p.metaDirs)
	p.mu.Unlock()
	if tracked != 0 {
		t.Fatalf("namespace-answered misses tracked %d directories, want 0", tracked)
	}
}

// v1 TestDirectoryPrefetchCapsConcurrencyAndReplenishesWithoutRecursion,
// driven without a mount: sibling-directory loads are capped at 4 in flight,
// complete cleanly, and replenish on the next listing without recursion.
func TestSiblingDirectoryPrefetchCapsConcurrencyAndReplenishes(t *testing.T) {
	rec := newBatchStatRecorder(t)
	p := newDirPrefetcher(client.New(rec.server.URL, ""), "/")
	nc := dircache.NewNamespaceCache(time.Minute, time.Minute, 0)
	p.namespace = nc
	names := []string{"a", "b", "c", "d", "e", "f", "g"}
	entries := make([]fs.DirEntryInfo, 0, len(names))
	for _, n := range names {
		entries = append(entries, fs.DirEntryInfo{Name: n, IsDir: true})
	}
	nc.Put("/repo", entries)
	nc.Put("/repo/a", []fs.DirEntryInfo{})

	var mu sync.Mutex
	active, maxActive := 0, 0
	started := make(chan string, 16)
	release := make(chan struct{})
	load := func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		mu.Lock()
		active++
		if active > maxActive {
			maxActive = active
		}
		mu.Unlock()
		started <- op.Path
		<-release
		mu.Lock()
		active--
		mu.Unlock()
		return fs.Result{Entries: []fs.Entry{{Name: "inner", Attr: fs.Attr{Kind: fs.KindFile, Size: 1}}}}, nil
	}

	p.maybePrefetchSiblingDirectories(load, "/repo/a")
	for range dirPrefetchConcurrency {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for initial prefetches")
		}
	}
	mu.Lock()
	gotMax := maxActive
	mu.Unlock()
	if gotMax != dirPrefetchConcurrency {
		t.Fatalf("max active loads = %d, want %d", gotMax, dirPrefetchConcurrency)
	}
	close(release)
	waitForDirPrefetchIdle(t, p)
	// The four loaded siblings seeded their listings.
	for _, n := range []string{"b", "c", "d", "e"} {
		if _, ok := nc.Get("/repo/" + n); !ok {
			t.Fatalf("sibling /repo/%s listing not seeded", n)
		}
	}

	// Replenish: listing b schedules the remaining uncached siblings only —
	// no recursion through the freshly cached listings.
	release = make(chan struct{})
	p.maybePrefetchSiblingDirectories(load, "/repo/b")
	for range 2 {
		select {
		case path := <-started:
			if path != "/repo/f" && path != "/repo/g" {
				t.Fatalf("replenished load = %s, want f or g", path)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for replenished prefetches")
		}
	}
	close(release)
	waitForDirPrefetchIdle(t, p)
}

// v1 dirPrefetchCandidateLimit: one scheduling pass enqueues at most 64
// sibling directories.
func TestSiblingDirectoryPrefetchCandidateLimit(t *testing.T) {
	rec := newBatchStatRecorder(t)
	p := newDirPrefetcher(client.New(rec.server.URL, ""), "/")
	nc := dircache.NewNamespaceCache(time.Minute, time.Minute, 0)
	p.namespace = nc
	entries := []fs.DirEntryInfo{{Name: "target", IsDir: true}}
	for i := range dirPrefetchCandidateLimit + 10 {
		entries = append(entries, fs.DirEntryInfo{Name: fmt.Sprintf("d%03d", i), IsDir: true})
	}
	nc.Put("/repo", entries)
	nc.Put("/repo/target", []fs.DirEntryInfo{})

	done := make(chan struct{})
	var mu sync.Mutex
	scheduled := 0
	load := func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		mu.Lock()
		scheduled++
		mu.Unlock()
		<-done
		return fs.Result{}, nil
	}
	p.maybePrefetchSiblingDirectories(load, "/repo/target")

	p.mu.Lock()
	inFlight := len(p.dirLoads)
	p.mu.Unlock()
	if inFlight > dirPrefetchCandidateLimit {
		t.Fatalf("scheduled siblings = %d, want <= %d", inFlight, dirPrefetchCandidateLimit)
	}
	close(done)
	waitForDirPrefetchIdle(t, p)
}
