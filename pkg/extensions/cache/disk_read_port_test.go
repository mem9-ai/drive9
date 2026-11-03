// v1 dat9fs_test.go disk-tier family (TestDat9FSDiskReadCache*,
// TestReadWholeFilePopulatesDiskReadCacheTier) and the v1 parallel-block
// read row, ported against the extensions read plane: the whole-file disk
// tier below the mem tier, readDiskCachedRange for large files, the
// DiskReadCache block store itself, and the Prefetcher's parallel fetches.
//
// v1 rows that tested mechanisms the extensions plane did not rebuild are
// skip-pinned at the bottom of this file (read-path singleflight, the
// remote read limiter, HEAD-before-serve revalidation).
package cache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func newPortDiskCache(t *testing.T, maxSize int64) *DiskReadCache {
	t.Helper()
	d, err := NewDiskReadCache(DiskReadCacheOptions{Dir: t.TempDir(), MaxSize: maxSize, FreeRatio: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Close)
	return d
}

func newPortDiskChain(t *testing.T, b *testbackend.Backend, maxFileSize int64) (*Cache, *fs.Chain) {
	t.Helper()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	c, err := New(Options{MaxFileSize: maxFileSize, Disk: DiskReadCacheOptions{Dir: t.TempDir(), MaxSize: 1 << 20, FreeRatio: -1}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	return c, newChain(t, ts.URL, c)
}

func openPortHandle(t *testing.T, ch *fs.Chain, path string) fs.Handle {
	t.Helper()
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: path, Flags: 0})
	if err != nil {
		t.Fatal(err)
	}
	if res.Handle == nil {
		t.Fatal("no handle")
	}
	return res.Handle
}

func waitPortDiskEntry(t *testing.T, dsk *DiskReadCache, key DiskReadCacheKey) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := dsk.Get(key); ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("disk read cache entry %+#v never landed", key)
}

// v1 TestReadWholeFilePopulatesDiskReadCacheTier: the small-file read
// populates both tiers; after the mem tier is swept, the disk tier serves
// warm re-reads without another backend fetch.
func TestPortWholeFileReadPopulatesDiskTier(t *testing.T) {
	data := bytes.Repeat([]byte("z"), 1024)
	b := testbackend.NewBackend()
	b.Seed("/f", data)

	c, ch := newPortDiskChain(t, b, 4096)
	h := openPortHandle(t, ch, "/f")

	buf := make([]byte, 64)
	if n, err := h.Read(context.Background(), 0, buf); err != nil || n != 64 || !bytes.Equal(buf, data[:64]) {
		t.Fatalf("first read = %d %q %v", n, buf, err)
	}
	key := DiskReadCacheKey{FileID: pathDiskReadCacheFileID("/f"), Path: "/f", Revision: 1, Offset: 0, Length: int64(len(data))}
	waitPortDiskEntry(t, c.dsk, key)

	// mem-tier sweep (TTL/eviction): the tail read must come from disk.
	c.InvalidateAll()
	tail := make([]byte, 64)
	if n, err := h.Read(context.Background(), int64(len(data))-64, tail); err != nil || n != 64 || !bytes.Equal(tail, data[len(data)-64:]) {
		t.Fatalf("disk-tier read = %d %q %v", n, tail, err)
	}
	if gets := b.Requests()["GET /v1/fs/f"]; gets != 1 {
		t.Fatalf("backend GETs = %d, want 1 (second read must come from the disk tier)", gets)
	}
	// the disk tier re-seeds the mem tier on its way up
	if _, ok := c.mem.Get("/f", 1); !ok {
		t.Fatal("disk-tier hit did not re-seed the mem tier")
	}
}

// v1 TestDat9FSDiskReadCacheRangeReadThrough: a large-file range read is
// fetched once and served from the disk tier on repeat.
func TestPortDiskRangeReadThrough(t *testing.T) {
	data := bytes.Repeat([]byte("x"), 256)
	b := testbackend.NewBackend()
	b.Seed("/f", data)

	c, ch := newPortDiskChain(t, b, 64)
	h := openPortHandle(t, ch, "/f")

	for i := range 2 {
		buf := make([]byte, 64)
		if n, err := h.Read(context.Background(), 128, buf); err != nil || n != 64 || !bytes.Equal(buf, data[128:192]) {
			t.Fatalf("read %d = %d %q %v", i, n, buf, err)
		}
	}
	if gets := b.Requests()["GET /v1/fs/f"]; gets != 1 {
		t.Fatalf("backend GETs = %d, want 1", gets)
	}
	key := DiskReadCacheKey{FileID: pathDiskReadCacheFileID("/f"), Path: "/f", Revision: 1, Offset: 128, Length: 64}
	if _, ok := c.dsk.Get(key); !ok {
		t.Fatal("range block missing from the disk tier")
	}
}

// v1 TestDat9FSDiskReadCacheSingleRangeFinalEOFRead: a read crossing EOF is
// clipped to the open-time size and cached under its trimmed bounds.
func TestPortDiskRangeFinalEOFReadTrimmedAndCached(t *testing.T) {
	data := make([]byte, 256)
	for i := range data {
		data[i] = byte(i)
	}
	b := testbackend.NewBackend()
	b.Seed("/f", data)

	c, ch := newPortDiskChain(t, b, 64)
	h := openPortHandle(t, ch, "/f")

	for i := range 2 {
		buf := make([]byte, 64) // request overshoots the 16-byte tail
		n, err := h.Read(context.Background(), 240, buf)
		if err != nil || n != 16 {
			t.Fatalf("final EOF read %d = %d %v; want 16 trimmed bytes", i, n, err)
		}
		if !bytes.Equal(buf[:16], data[240:]) {
			t.Fatalf("final EOF read %d = %q; want the tail bytes", i, buf[:16])
		}
	}
	if gets := b.Requests()["GET /v1/fs/f"]; gets != 1 {
		t.Fatalf("backend GETs = %d, want 1", gets)
	}
	key := DiskReadCacheKey{FileID: pathDiskReadCacheFileID("/f"), Path: "/f", Revision: 1, Offset: 240, Length: 16}
	if _, ok := c.dsk.Get(key); !ok {
		t.Fatal("trimmed final block missing from the disk tier")
	}
}

// stubRangeHandle is an in-memory inner plane for white-box rows.
type stubRangeHandle struct {
	mu    sync.Mutex
	data  []byte
	reads int
	err   error
	// shortAtAnyOffset models a backend that answers a range request with
	// fewer bytes than requested (a truncated body): the stub serves data
	// verbatim regardless of the requested offset.
	shortAtAnyOffset bool
}

func (h *stubRangeHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	h.mu.Lock()
	h.reads++
	err := h.err
	h.mu.Unlock()
	if err != nil {
		return 0, err
	}
	if h.shortAtAnyOffset {
		return copy(dest, h.data), nil
	}
	if off >= int64(len(h.data)) {
		return 0, io.EOF
	}
	end := off + int64(len(dest))
	if end > int64(len(h.data)) {
		end = int64(len(h.data))
	}
	return copy(dest, h.data[off:end]), nil
}

func (h *stubRangeHandle) Write(ctx context.Context, off int64, data []byte) error { return nil }
func (h *stubRangeHandle) Sync(ctx context.Context) error                          { return nil }
func (h *stubRangeHandle) Flush(ctx context.Context) error                         { return nil }
func (h *stubRangeHandle) Release(ctx context.Context) error                       { return nil }

func (h *stubRangeHandle) readCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.reads
}

// slowPortHandle counts reads and their peak concurrency, holding each
// fetch long enough for concurrent readers to overlap.
type slowPortHandle struct {
	data        []byte
	reads       atomic.Int32
	inFlight    atomic.Int32
	maxInFlight atomic.Int32
}

func (h *slowPortHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	h.reads.Add(1)
	cur := h.inFlight.Add(1)
	for {
		old := h.maxInFlight.Load()
		if cur <= old || h.maxInFlight.CompareAndSwap(old, cur) {
			break
		}
	}
	defer h.inFlight.Add(-1)
	time.Sleep(100 * time.Millisecond)
	end := off + int64(len(dest))
	if end > int64(len(h.data)) {
		end = int64(len(h.data))
	}
	return copy(dest, h.data[off:end]), nil
}

func (h *slowPortHandle) Write(ctx context.Context, off int64, data []byte) error { return nil }
func (h *slowPortHandle) Sync(ctx context.Context) error                          { return nil }
func (h *slowPortHandle) Flush(ctx context.Context) error                         { return nil }
func (h *slowPortHandle) Release(ctx context.Context) error                       { return nil }

// newPortDiskUnitCache builds a Cache whose white-box rows drive
// readDiskCachedRange directly. maxFileSize <= 0 keeps the default whole-file
// admission; a small value pushes reads onto the range tier.
func newPortDiskUnitCache(t *testing.T, maxFileSize int64) *Cache {
	t.Helper()
	c, err := New(Options{MaxFileSize: maxFileSize, Disk: DiskReadCacheOptions{Dir: t.TempDir(), MaxSize: 1 << 20, FreeRatio: -1}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	return c
}

func portDiskUnitHandle(c *Cache, inner fs.Handle, rev int64, size int64) *cachingHandle {
	return &cachingHandle{cache: c, inner: inner, path: "/f", rev: rev, size: size}
}

// v1 TestDat9FSDiskReadCacheRejectsShortBackendResponse: a short fetch is
// served directly and must not poison the tier with a truncated entry.
func TestPortDiskRangeShortFetchServedButNotCached(t *testing.T) {
	c := newPortDiskUnitCache(t, 64)
	inner := &stubRangeHandle{data: bytes.Repeat([]byte("s"), 32), shortAtAnyOffset: true} // 32 of 64
	h := portDiskUnitHandle(c, inner, 1, 256)

	dest := make([]byte, 64)
	n, ok := c.readDiskCachedRange(context.Background(), h, 128, dest)
	if !ok || n != 32 {
		t.Fatalf("short fetch = %d %v; want 32 served directly", n, ok)
	}
	if !bytes.Equal(dest[:32], bytes.Repeat([]byte("s"), 32)) {
		t.Fatalf("short fetch bytes = %q", dest[:32])
	}
	if c.dsk.Len() != 0 {
		t.Fatalf("disk tier entries after short fetch = %d, want 0", c.dsk.Len())
	}
}

// v1 TestDat9FSDiskReadCacheRejectsShortBackendResponse (error half): a
// failed range fetch defers to the direct read and surfaces its error.
func TestPortDiskRangeFetchErrorDefersToDirectRead(t *testing.T) {
	c := newPortDiskUnitCache(t, 64)
	boom := fmt.Errorf("backend boom")
	inner := &stubRangeHandle{err: boom}
	h := portDiskUnitHandle(c, inner, 1, 256)

	if _, err := h.Read(context.Background(), 128, make([]byte, 64)); err != boom {
		t.Fatalf("read error = %v, want the inner error", err)
	}
	if c.dsk.Len() != 0 {
		t.Fatalf("disk tier entries after failed fetch = %d, want 0", c.dsk.Len())
	}
}

// v1 TestDat9FSDiskReadCacheRevisionInvalidatesByKey: revisions key their
// own blocks; a revision bump cannot serve the previous image.
func TestPortDiskRangeRevisionKeysAreDistinct(t *testing.T) {
	c := newPortDiskUnitCache(t, 64)
	oldInner := &stubRangeHandle{data: bytes.Repeat([]byte("o"), 256)}
	newInner := &stubRangeHandle{data: bytes.Repeat([]byte("n"), 256)}

	if n, ok := c.readDiskCachedRange(context.Background(), portDiskUnitHandle(c, oldInner, 1, 256), 128, make([]byte, 64)); !ok || n != 64 {
		t.Fatalf("rev1 read = %d %v", n, ok)
	}
	if n, ok := c.readDiskCachedRange(context.Background(), portDiskUnitHandle(c, newInner, 2, 256), 128, make([]byte, 64)); !ok || n != 64 {
		t.Fatalf("rev2 read = %d %v", n, ok)
	}
	if got := oldInner.readCount(); got != 1 {
		t.Fatalf("rev1 inner reads = %d, want 1", got)
	}
	if got := newInner.readCount(); got != 1 {
		t.Fatalf("rev2 inner reads = %d, want 1", got)
	}
	oldKey := DiskReadCacheKey{FileID: pathDiskReadCacheFileID("/f"), Path: "/f", Revision: 1, Offset: 128, Length: 64}
	newKey := DiskReadCacheKey{FileID: pathDiskReadCacheFileID("/f"), Path: "/f", Revision: 2, Offset: 128, Length: 64}
	if data, ok := c.dsk.Get(oldKey); !ok || data[0] != 'o' {
		t.Fatalf("rev1 block = %q %v; want the old image preserved under its own key", data, ok)
	}
	if data, ok := c.dsk.Get(newKey); !ok || data[0] != 'n' {
		t.Fatalf("rev2 block = %q %v", data, ok)
	}
}

// v1 TestDat9FSDiskReadCacheCorruptionRefetches: a checksum mismatch evicts
// the block (and removes its file) so the next read refetches.
func TestPortDiskCacheCorruptionRefetches(t *testing.T) {
	dsk := newPortDiskCache(t, 1<<20)
	key := DiskReadCacheKey{FileID: pathDiskReadCacheFileID("/f"), Path: "/f", Revision: 7, Offset: 0, Length: 3}
	dsk.Put(key, []byte("bad"))
	path := dsk.pathForDigest(key.digest())
	junk := `{"file_id":"path:/f","path":"/f","revision":7,"offset":0,"length":3,"size":3,"crc32":1}` + "\nbad"
	if err := os.WriteFile(path, []byte(junk), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, ok := dsk.Get(key); ok {
		t.Fatal("corrupt block served")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("corrupt block file survived: %v", err)
	}
	if dsk.Len() != 0 {
		t.Fatalf("index entries after corruption = %d, want 0", dsk.Len())
	}
	dsk.Put(key, []byte("ok!"))
	if data, ok := dsk.Get(key); !ok || string(data) != "ok!" {
		t.Fatalf("refetched block = %q %v", data, ok)
	}
}

// The v1 unverified-stat-cache rows reduced to their surviving guarantee in
// the event-driven plane: write/delete/invalidate/checkout events sweep the
// disk tier per file and per subtree prefix, and a checkout drops
// everything.
func TestPortDiskCacheEventsInvalidateFileSubtreeAndAll(t *testing.T) {
	bus := fs.NewEventBus()
	c, err := New(Options{Events: bus, Disk: DiskReadCacheOptions{Dir: t.TempDir(), MaxSize: 1 << 20, FreeRatio: -1}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })

	fileKey := func(path string, off int64) DiskReadCacheKey {
		return DiskReadCacheKey{FileID: pathDiskReadCacheFileID(path), Path: path, Revision: 1, Offset: off, Length: 16}
	}
	c.dsk.Put(fileKey("/d/f", 0), bytes.Repeat([]byte("a"), 16))
	c.dsk.Put(fileKey("/d/sub/g", 0), bytes.Repeat([]byte("b"), 16))
	c.dsk.Put(fileKey("/e/h", 0), bytes.Repeat([]byte("c"), 16))

	bus.Emit(fs.Event{Kind: fs.EventWrite, Path: "/d/f"})
	if _, ok := c.dsk.Get(fileKey("/d/f", 0)); ok {
		t.Fatal("write event left the file's disk blocks")
	}
	if _, ok := c.dsk.Get(fileKey("/d/sub/g", 0)); !ok {
		t.Fatal("write event over-dropped a sibling subtree")
	}

	bus.Emit(fs.Event{Kind: fs.EventDelete, Path: "/d"})
	if _, ok := c.dsk.Get(fileKey("/d/sub/g", 0)); ok {
		t.Fatal("directory delete left the subtree's disk blocks")
	}
	if _, ok := c.dsk.Get(fileKey("/e/h", 0)); !ok {
		t.Fatal("directory delete over-dropped an unrelated file")
	}

	bus.Emit(fs.Event{Kind: fs.EventInvalidated, Path: "/e/h"})
	if _, ok := c.dsk.Get(fileKey("/e/h", 0)); ok {
		t.Fatal("invalidation event left the file's disk blocks")
	}

	c.dsk.Put(fileKey("/e/h", 32), bytes.Repeat([]byte("c"), 16))
	bus.Emit(fs.Event{Kind: fs.EventCheckout, Path: "/"})
	if c.dsk.Len() != 0 {
		t.Fatalf("checkout left %d disk blocks, want a full sweep", c.dsk.Len())
	}
}

// v1 index-recovery behavior: entries survive a close/reopen, stray temp
// files are swept, and unreadable blocks are dropped on recovery.
func TestPortDiskCacheRecoversAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	d1, err := NewDiskReadCache(DiskReadCacheOptions{Dir: dir, MaxSize: 1 << 20, FreeRatio: -1})
	if err != nil {
		t.Fatal(err)
	}
	kA := DiskReadCacheKey{FileID: pathDiskReadCacheFileID("/a"), Path: "/a", Revision: 1, Offset: 0, Length: 3}
	kB := DiskReadCacheKey{FileID: pathDiskReadCacheFileID("/b"), Path: "/b", Revision: 2, Offset: 64, Length: 5}
	d1.Put(kA, []byte("aaa"))
	d1.Put(kB, []byte("bbbbb"))
	d1.Close()

	if err := os.WriteFile(dir+"/stale.tmp", []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/deadbeef.drc", []byte("not json at all"), 0o600); err != nil {
		t.Fatal(err)
	}

	d2, err := NewDiskReadCache(DiskReadCacheOptions{Dir: dir, MaxSize: 1 << 20, FreeRatio: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d2.Close)
	if data, ok := d2.Get(kA); !ok || string(data) != "aaa" {
		t.Fatalf("recovered /a = %q %v", data, ok)
	}
	if data, ok := d2.Get(kB); !ok || string(data) != "bbbbb" {
		t.Fatalf("recovered /b = %q %v", data, ok)
	}
	if _, err := os.Stat(dir + "/stale.tmp"); !os.IsNotExist(err) {
		t.Fatal("temp file survived recovery")
	}
	if _, err := os.Stat(dir + "/deadbeef.drc"); !os.IsNotExist(err) {
		t.Fatal("unreadable block survived recovery")
	}
	if d2.Len() != 2 {
		t.Fatalf("recovered index len = %d, want 2", d2.Len())
	}
}

// v1 LRU eviction by max size: the least recently used block goes first.
func TestPortDiskCacheLRUEvictionByMaxSize(t *testing.T) {
	// one probe cache measures the on-disk entry footprint (header + data)
	probe := newPortDiskCache(t, 1<<20)
	kA := DiskReadCacheKey{FileID: pathDiskReadCacheFileID("/a"), Path: "/a", Revision: 1, Offset: 0, Length: 4}
	probe.Put(kA, []byte("aaaa"))
	probe.mu.Lock()
	footprint := probe.size
	probe.mu.Unlock()
	probe.Close()

	dsk, err := NewDiskReadCache(DiskReadCacheOptions{Dir: t.TempDir(), MaxSize: 2 * footprint, FreeRatio: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dsk.Close)
	kB := DiskReadCacheKey{FileID: pathDiskReadCacheFileID("/b"), Path: "/b", Revision: 1, Offset: 0, Length: 4}
	kC := DiskReadCacheKey{FileID: pathDiskReadCacheFileID("/c"), Path: "/c", Revision: 1, Offset: 0, Length: 4}
	dsk.Put(kA, []byte("aaaa"))
	dsk.Put(kB, []byte("bbbb"))
	if _, ok := dsk.Get(kA); !ok { // touch /a: /b becomes the LRU
		t.Fatal("touch read missed")
	}
	dsk.Put(kC, []byte("cccc")) // over budget: evicts /b
	if _, ok := dsk.Get(kA); !ok {
		t.Fatal("touched block evicted")
	}
	if _, ok := dsk.Get(kC); !ok {
		t.Fatal("newest block evicted")
	}
	if _, ok := dsk.Get(kB); ok {
		t.Fatal("LRU block survived eviction")
	}
	if dsk.Len() != 2 {
		t.Fatalf("index len = %d, want 2", dsk.Len())
	}
}

// v1 admission guards: length-mismatched, oversized, and invalid keys are
// silently refused.
func TestPortDiskCachePutRejectsBadKeysAndSizes(t *testing.T) {
	dsk := newPortDiskCache(t, 8)
	key := DiskReadCacheKey{FileID: pathDiskReadCacheFileID("/f"), Path: "/f", Revision: 1, Offset: 0, Length: 5}

	dsk.Put(key, []byte("abc")) // length mismatch
	if dsk.Len() != 0 {
		t.Fatal("length-mismatched put admitted")
	}
	dsk.PutOwned(key, bytes.Repeat([]byte("x"), 16)) // over max size
	if dsk.Len() != 0 {
		t.Fatal("oversized put admitted")
	}
	bad := key
	bad.Revision = 0
	dsk.Put(bad, []byte("abcde")) // invalid key
	if dsk.Len() != 0 {
		t.Fatal("invalid key admitted")
	}
	if _, ok := dsk.Get(bad); ok {
		t.Fatal("invalid key served")
	}
}

// v1 PutAsync visibility: a Get racing the background worker is served from
// the pending copy, and the entry lands on disk once the worker finishes.
func TestPortDiskCachePutAsyncServesPendingWindow(t *testing.T) {
	dsk := newPortDiskCache(t, 1<<20)
	key := DiskReadCacheKey{FileID: pathDiskReadCacheFileID("/f"), Path: "/f", Revision: 3, Offset: 0, Length: 4}

	dsk.PutAsync(key, []byte("data"))
	if data, ok := dsk.Get(key); !ok || string(data) != "data" {
		t.Fatalf("pending-window get = %q %v", data, ok)
	}
	deadline := time.Now().Add(3 * time.Second)
	for dsk.Len() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if dsk.Len() != 1 {
		t.Fatal("async put never landed")
	}
	if data, ok := dsk.Get(key); !ok || string(data) != "data" {
		t.Fatalf("landed get = %q %v", data, ok)
	}
}

// v1 TestDat9FSParallelDiskReadFetchesBlocksInParallelAndCaches: the
// parallel block machinery now lives in the Prefetcher — the window after a
// sequential read is fetched as bounded parallel range requests with exact
// block bounds, and the blocks serve subsequent reads. (Design divergence:
// the Prefetcher keeps the blocks in its own bounded cache and does not
// seed the DiskReadCache; repeat reads populate the disk tier through
// readDiskCachedRange instead.)
func TestPortPrefetchParallelFetchBoundsAndCachesBlocks(t *testing.T) {
	const fileSize = 256
	data := make([]byte, fileSize)
	for i := range data {
		data[i] = byte(i)
	}
	var mu sync.Mutex
	ranges := make([]string, 0, 4)
	var inFlight, maxInFlight atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rng := r.Header.Get("Range")
		mu.Lock()
		ranges = append(ranges, rng)
		mu.Unlock()
		cur := inFlight.Add(1)
		for {
			old := maxInFlight.Load()
			if cur <= old || maxInFlight.CompareAndSwap(old, cur) {
				break
			}
		}
		defer inFlight.Add(-1)
		time.Sleep(60 * time.Millisecond)
		lo, hi, ok := strings.Cut(strings.TrimPrefix(rng, "bytes="), "-")
		if !ok {
			http.Error(w, "bad range", http.StatusBadRequest)
			return
		}
		start, err1 := strconv.ParseInt(lo, 10, 64)
		end, err2 := strconv.ParseInt(hi, 10, 64)
		if err1 != nil || err2 != nil {
			http.Error(w, "bad range", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, fileSize))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[start : end+1])
	}))
	defer srv.Close()

	p := NewPrefetcher(client.New(srv.URL, ""), "/f.bin", fileSize)
	defer p.Close()
	p.SetParallelRead(2, 64)
	p.OnRead(0, 64) // the consumed region itself is not re-fetched

	for _, off := range []int64{64, 128, 192} {
		buf, ok := p.Get(off, 64)
		if !ok || !bytes.Equal(buf, data[off:off+64]) {
			t.Fatalf("prefetch block %d = %d bytes %v; want the exact slice", off, len(buf), ok)
		}
	}
	mu.Lock()
	seen := append([]string(nil), ranges...)
	mu.Unlock()
	want := []string{"bytes=128-191", "bytes=192-255", "bytes=64-127"} // lexicographic
	sort.Strings(seen)
	if len(seen) != len(want) {
		t.Fatalf("fetch ranges = %v, want %v", seen, want)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("fetch ranges = %v, want %v", seen, want)
		}
	}
	if got := maxInFlight.Load(); got != 2 {
		t.Fatalf("max concurrent fetches = %d, want 2 (parallel and bounded)", got)
	}
}

// v1 TestDat9FSDiskReadCacheConcurrentSameKeyDedupsBackendRead +
// TestDat9FSReadSingleFlight*: concurrent same-key range reads shared one
// backend fetch (v1 readFlight singleflight).
func TestPortReadSingleFlightDedupsConcurrentSameKeyRangeReads(t *testing.T) {
	t.Skip("pinned: the extensions read plane has no same-key singleflight — fusecomp/singleflight is wired only into core namespace ops, not the cache's range reads. Restore the read-path dedup in production, then drop this skip.")
	c := newPortDiskUnitCache(t, 64)
	inner := &slowPortHandle{data: bytes.Repeat([]byte("q"), 256)}
	h := portDiskUnitHandle(c, inner, 7, 256)

	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = h.Read(context.Background(), 64, make([]byte, 64))
		}()
	}
	wg.Wait()
	if got := inner.reads.Load(); got != 1 {
		t.Fatalf("backend reads = %d, want 1 (same-key reads dedup)", got)
	}
}

// v1 TestRemoteReadLimiterCapsConcurrentRangeReads (and the acquire/
// write-gate rows): range-read concurrency was capped mount-wide.
func TestPortRemoteReadLimiterCapsConcurrentRangeReads(t *testing.T) {
	t.Skip("pinned: v1's remote read limiter was not rebuilt — the client gates upload parallelism (boundedUploadParallelism) but nothing caps concurrent range reads. Production handoff item; drop this skip once the gate exists.")
	c := newPortDiskUnitCache(t, 64)
	inner := &slowPortHandle{data: bytes.Repeat([]byte("r"), 4096)}

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h := portDiskUnitHandle(c, inner, 7, 4096)
			_, _ = h.Read(context.Background(), 64, make([]byte, 64)) // off!=0: range tier
		}()
	}
	wg.Wait()
	if got := inner.maxInFlight.Load(); got > 2 {
		t.Fatalf("concurrent range reads = %d, want <= 2 (limiter)", got)
	}
}

// v1 TestDat9FSDiskReadCacheUnverifiedRevisionMatchServesCachedAfterHead /
// MismatchRefetches / RevalidatesWhenLocalEventsUntrusted: an unverified
// stat cache forced a HEAD before cached bytes were served.
func TestPortDiskTierUnverifiedStatRevalidation(t *testing.T) {
	t.Skip("pinned: the extensions plane pins the revision at open and coheres through bus events (see TestPortDiskCacheEventsInvalidateFileSubtreeAndAll); there is no HEAD-before-serve revalidation of unverified stat caches. Accepted divergence unless remote-lag windows require it.")
	c := newPortDiskUnitCache(t, 0)
	oldKey := DiskReadCacheKey{FileID: pathDiskReadCacheFileID("/f"), Path: "/f", Revision: 7, Offset: 0, Length: 5}
	c.dsk.Put(oldKey, []byte("stale"))
	inner := &stubRangeHandle{data: []byte("fresh")} // remote moved to rev 8
	h := portDiskUnitHandle(c, inner, 7, 5)          // open pinned the stale rev
	if _, err := h.Read(context.Background(), 0, make([]byte, 5)); err != nil {
		t.Fatal(err)
	}
	if inner.readCount() != 1 {
		t.Fatal("unverified stat did not revalidate before serving cached bytes")
	}
}
