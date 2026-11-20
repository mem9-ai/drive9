package cache

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// waitDiskFlushed blocks until the disk tier's asynchronous block writer has
// indexed at least one entry: the cache is allowed to drop in-flight writes on
// Close, so a remount test must not race them.
func waitDiskFlushed(t *testing.T, dsk *DiskReadCache) {
	t.Helper()
	if dsk == nil {
		t.Fatal("disk tier is disabled")
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if dsk.Len() > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("disk read cache never indexed the block")
}

// renameReader opens path through the chain and reads it whole, releasing the
// descriptor before returning: every call is a FRESH open, so the bytes it
// sees are what a new reader after the mutation would see.
func renameReader(t *testing.T, ch *fs.Chain, path string) string {
	t.Helper()
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: path, Flags: 0})
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	if res.Handle == nil {
		t.Fatalf("open %s returned no handle", path)
	}
	defer func() { _ = res.Handle.Release(context.Background()) }()
	buf := make([]byte, 64)
	n, err := res.Handle.Read(context.Background(), 0, buf)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(buf[:n])
}

// A rename REPLACES its destination: the destination's cached image describes
// the object the rename destroyed, so it must be dropped even when the source
// itself was never cached. A new handle on the name must serve the moved
// bytes, not the replaced image. Both tiers are checked: the whole-file memory
// tier would answer for its TTL, and the on-disk tier the memory tier falls
// through to.
func TestRenameOntoTargetDropsReplacedCachedImage(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/target", []byte("v1"))
	b.Seed("/tmp", []byte("v2"))
	ts := httptest.NewServer(b)
	defer ts.Close()

	c, err := New(Options{Disk: DiskReadCacheOptions{Dir: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if c.dsk != nil {
			c.dsk.Close()
		}
	})
	ch := newChain(t, ts.URL, c)
	if got := renameReader(t, ch, "/target"); got != "v1" {
		t.Fatalf("primed /target = %q, want v1", got)
	}
	if _, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpRename, Path: "/tmp", Path2: "/target"}); err != nil {
		t.Fatalf("rename /tmp -> /target: %v", err)
	}
	if got := renameReader(t, ch, "/target"); got != "v2" {
		t.Fatalf("post-rename /target = %q, want v2 (the replaced image was served)", got)
	}
}

// The disk tier outlives the mount: its entries must be keyed by OBJECT
// identity, not by (path, revision). A per-file revision counter restarts at
// 1, so a file replaced while this mount was down — here a rename onto the
// name, or any delete+recreate — presents the same path and revision as the
// image the previous mount cached. Keyed on the path alone the stale image is
// served again after the remount.
func TestDiskReadCacheKeysRemountByObjectIdentity(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/target", []byte("v1"))
	ts := httptest.NewServer(b)
	defer ts.Close()

	dir := t.TempDir()
	first, err := New(Options{Disk: DiskReadCacheOptions{Dir: dir}})
	if err != nil {
		t.Fatal(err)
	}
	ch := newChain(t, ts.URL, first)
	if got := renameReader(t, ch, "/target"); got != "v1" {
		t.Fatalf("primed /target = %q, want v1", got)
	}
	waitDiskFlushed(t, first.dsk)
	if first.dsk != nil {
		first.dsk.Close() // flush the async block write before the "remount"
	}

	// Another client replaces the object at the name while this mount is
	// down: a NEW object, same path, same revision (the counter restarts).
	b.Seed("/target", []byte("v2"))

	second, err := New(Options{Disk: DiskReadCacheOptions{Dir: dir}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if second.dsk != nil {
			second.dsk.Close()
		}
	})
	ch2 := newChain(t, ts.URL, second)
	if got := renameReader(t, ch2, "/target"); got != "v2" {
		t.Fatalf("post-remount /target = %q, want v2 (the previous mount's image was served)", got)
	}
}

// The in-memory tier is keyed by path and must drop the replaced target on a
// rename regardless of whether the source it moves had a cached image.
func TestReadCacheRenameDropsReplacedTarget(t *testing.T) {
	rc := NewReadCacheWithMaxFileSize(0, -1, 0)
	rc.Put("/target", []byte("v1"), 1)
	rc.Put("/tmp", []byte("v2"), 1)
	rc.Rename("/tmp", "/target")
	if data, ok := rc.Get("/target", 1); !ok || string(data) != "v2" {
		t.Fatalf("/target after rename = %q/%v, want v2/true", data, ok)
	}
	if got, want := rc.Size(), int64(len("v2")); got != want {
		t.Fatalf("cache size after rename = %d, want %d (replaced entry leak)", got, want)
	}

	// The source was never cached: the destination's image still describes
	// the object the rename replaced and must not survive.
	rc.Put("/target", []byte("stale"), 1)
	rc.Rename("/absent", "/target")
	if data, ok := rc.Get("/target", 1); ok {
		t.Fatalf("/target still serves %q after an uncached source replaced it", data)
	}
}

// A directory rename retargets the subtree: cached descendants move with the
// name (same objects, same revisions) and the replaced target's subtree is
// dropped.
func TestReadCacheRenameMovesSubtree(t *testing.T) {
	rc := NewReadCacheWithMaxFileSize(0, -1, 0)
	rc.Put("/src/child", []byte("keep"), 1)
	rc.Put("/dst/stale", []byte("old"), 1)
	rc.Put("/dst/child", []byte("older"), 1)
	rc.Rename("/src", "/dst")
	if data, ok := rc.Get("/dst/child", 1); !ok || string(data) != "keep" {
		t.Fatalf("/dst/child = %q/%v, want keep/true", data, ok)
	}
	if data, ok := rc.Get("/dst/stale", 1); ok {
		t.Fatalf("/dst/stale still serves %q from the replaced target", data)
	}
	if _, ok := rc.Get("/src/child", 1); ok {
		t.Fatal("/src/child survived the rename")
	}
}
