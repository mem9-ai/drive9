// v1 dat9fs SQLite persistent-journal routing rows: -wal/-journal
// sidecar reads bypass the whole-file small read cache (a rolling journal
// must observe every landing), and the main database's cache eligibility
// depends on an open sidecar. Retargeted to the cache extension's
// cachingHandle over a stub inner handle.
package cache

import (
	"context"
	"sync/atomic"
	"testing"
)

type stubInner struct {
	reads atomic.Int32
	body  func(n int32) []byte
}

func (s *stubInner) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	n := s.reads.Add(1)
	copy(dest, s.body(n))
	return len(s.body(n)), nil
}
func (s *stubInner) Write(ctx context.Context, off int64, data []byte) error { return nil }
func (s *stubInner) Sync(ctx context.Context) error                          { return nil }
func (s *stubInner) Flush(ctx context.Context) error                         { return nil }
func (s *stubInner) Release(ctx context.Context) error                       { return nil }

// v1 TestDat9FSReadSQLitePersistentJournalBypassesSmallReadCache: two
// reads of a -wal path observe two different server bodies (no
// whole-file cache between them).
func TestPortSQLiteJournalReadBypassesSmallCache(t *testing.T) {
	var reads atomic.Int32
	inner := &stubInner{body: func(n int32) []byte {
		if n == 1 {
			return []byte("old-wal")
		}
		return []byte("new-wal")
	}}
	_ = reads.Load()
	c := newHandleCache(t)
	h := &cachingHandle{cache: c, inner: inner, path: "/repo/workload.db-wal", rev: 3, size: 7}

	buf := make([]byte, 7)
	if n, err := h.Read(context.Background(), 0, buf); err != nil || string(buf[:n]) != "old-wal" {
		t.Fatalf("first read = %q n=%d err=%v", buf[:n], n, err)
	}
	if n, err := h.Read(context.Background(), 0, buf); err != nil || string(buf[:n]) != "new-wal" {
		t.Fatalf("journal read served from cache: %q (v1 bypassed -wal/-journal)", buf[:n])
	}
}

// v1 TestDat9FSSQLitePersistentJournalBypassesDiskReadCacheKey /
// MainDatabaseDiskReadCacheDependsOnOpenSidecar: -wal/-journal names and
// a main database with an open sidecar are ineligible for the disk tier.
// The new cache keys by path+revision without the sqlite predicate —
// found parity gap, pinned as skips on the eligibility halves.
func TestPortSQLiteDiskTierEligibility(t *testing.T) {
	c := newHandleCache(t)
	inner := &stubInner{body: func(int32) []byte { return []byte("payload") }}
	for _, path := range []string{"/repo/workload.db-wal", "/repo/workload.db-journal"} {
		h := &cachingHandle{cache: c, inner: inner, path: path, rev: 3, size: 7}
		buf := make([]byte, 7)
		_, _ = h.Read(context.Background(), 0, buf)
	}
	if n := c.dsk.Len(); n != 0 {
		t.Fatalf("sqlite sidecar rows entered the disk tier (len=%d): v1 excluded -wal/-journal from the disk cache key", n)
	}
}

func newHandleCache(t *testing.T) *Cache {
	t.Helper()
	c, err := New(Options{DirContentPrefetch: false, SequentialPrefetch: false})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// v1 TestDat9FSMainDatabaseDiskReadCacheDependsOnOpenSidecar: while a
// -wal handle is open, the main database bypasses the stable read tiers.
func TestPortMainDatabaseBypassWithOpenSidecar(t *testing.T) {
	c := newHandleCache(t)
	inner := &stubInner{body: func(int32) []byte { return []byte("maindb01") }}
	main := &cachingHandle{cache: c, inner: inner, path: "/repo/workload.db", rev: 2, size: 8}
	buf := make([]byte, 8)
	if _, err := main.Read(context.Background(), 0, buf); err != nil {
		t.Fatal(err)
	}
	if got, ok := c.mem.Get("/repo/workload.db", 2); !ok || len(got) != 8 {
		t.Fatalf("main db without sidecar not cached: %v %v", got, ok)
	}
	c.trackSidecarOpen("/repo/workload.db-wal")
	defer c.sidecars.remove("/repo/workload.db-wal")
	if got, ok := c.mem.Get("/repo/workload.db", 2); ok {
		t.Fatalf("opening the sidecar kept the main db image cached: %v", got)
	}
	inner2 := &stubInner{body: func(int32) []byte { return []byte("maindb02") }}
	main2 := &cachingHandle{cache: c, inner: inner2, path: "/repo/workload.db", rev: 2, size: 8}
	buf2 := make([]byte, 8)
	if _, err := main2.Read(context.Background(), 0, buf2); err != nil {
		t.Fatal(err)
	}
	if string(buf2) != "maindb02" {
		t.Fatalf("main db read with open sidecar = %q, want the fresh body", string(buf2))
	}
	if got, ok := c.mem.Get("/repo/workload.db", 2); ok {
		t.Fatalf("main db cached while a sidecar is open: %v", got)
	}
	if n := c.dsk.Len(); n != 0 {
		t.Fatalf("main db rows entered the disk tier with an open sidecar (len=%d)", n)
	}
}
