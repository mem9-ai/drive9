package cache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
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

// gatedReadHandle blocks its first Read until released, so a test can hold a
// read in flight across a namespace mutation.
type gatedReadHandle struct {
	started chan struct{}
	release chan struct{}
	data    []byte
	once    sync.Once
}

func (h *gatedReadHandle) Read(_ context.Context, off int64, dest []byte) (int, error) {
	h.once.Do(func() { close(h.started) })
	<-h.release
	if off >= int64(len(h.data)) {
		return 0, nil
	}
	return copy(dest, h.data[off:]), nil
}

func (h *gatedReadHandle) Write(context.Context, int64, []byte) error { return nil }
func (h *gatedReadHandle) Sync(context.Context) error                 { return nil }
func (h *gatedReadHandle) Flush(context.Context) error                { return nil }
func (h *gatedReadHandle) Release(context.Context) error              { return nil }

// A read that is already in flight when a rename replaces its target must not
// restore the replaced image when it finally publishes: the tier sweep and the
// publication fence have to be atomic, or a fresh handle would be handed the
// old bytes (both objects can carry revision 1).
func TestRenameFencesInFlightReadPublication(t *testing.T) {
	c, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	inner := &gatedReadHandle{
		started: make(chan struct{}),
		release: make(chan struct{}),
		data:    []byte("old"),
	}
	h := &cachingHandle{cache: c, inner: inner, path: "/target", rev: 1, size: 3}

	readErr := make(chan error, 1)
	go func() {
		_, err := h.Read(context.Background(), 0, make([]byte, 3))
		readErr <- err
	}()
	<-inner.started // the read has fetched nothing yet but will publish after the rename

	next := fs.Next(func(context.Context, *fs.Op) (fs.Result, error) {
		return fs.Result{}, nil
	})
	if _, err := c.Do(context.Background(), &fs.Op{Kind: fs.OpRename, Path: "/tmp", Path2: "/target"}, next); err != nil {
		t.Fatalf("rename: %v", err)
	}
	close(inner.release)
	if err := <-readErr; err != nil {
		t.Fatalf("in-flight read: %v", err)
	}
	if data, ok := c.mem.Get("/target", 1); ok {
		t.Fatalf("in-flight read repopulated the replaced target with %q", data)
	}
	if c.mem.Size() != 0 {
		t.Fatalf("cache retained %d bytes after the fenced sweep", c.mem.Size())
	}
}

// Event-driven sweeps must be fenced too: another client's rename-over or
// delete+recreate lands as an event, not a local OpRename, and the same
// interleaving (pre-sweep fetch published after the sweep) would otherwise
// restore the replaced image.
func TestEventSweepFencesInFlightReadPublication(t *testing.T) {
	bus := fs.NewEventBus()
	c, err := New(Options{Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	inner := &gatedReadHandle{
		started: make(chan struct{}),
		release: make(chan struct{}),
		data:    []byte("old"),
	}
	h := &cachingHandle{cache: c, inner: inner, path: "/target", rev: 1, size: 3}

	readErr := make(chan error, 1)
	go func() {
		_, err := h.Read(context.Background(), 0, make([]byte, 3))
		readErr <- err
	}()
	<-inner.started

	bus.Emit(fs.Event{Kind: fs.EventInvalidated, Path: "/target"})

	close(inner.release)
	if err := <-readErr; err != nil {
		t.Fatalf("in-flight read: %v", err)
	}
	if data, ok := c.mem.Get("/target", 1); ok {
		t.Fatalf("in-flight read repopulated the swept target with %q", data)
	}
}

// Reading the epoch must wait for an in-progress sweep: an unlocked load can
// observe the bumped epoch while the tiers and handles still describe the old
// image, and the publication fence would then accept a read fetched before the
// sweep finished.
func TestPublicationEpochWaitsForInProgressSweep(t *testing.T) {
	c, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	swept := make(chan struct{})
	go func() {
		c.fencedSweep(func() []string {
			close(entered)
			<-release
			return nil
		})
		close(swept)
	}()
	<-entered

	captured := make(chan uint64, 1)
	go func() { captured <- c.publicationEpoch() }()
	select {
	case got := <-captured:
		t.Fatalf("epoch %d captured while the sweep was still running", got)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	<-swept
	select {
	case got := <-captured:
		if want := c.epoch.Load(); got != want {
			t.Fatalf("captured epoch = %d, want %d after the sweep", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("epoch capture never returned after the sweep")
	}
}

// The facade's deferred-unlink survivor hook recycles a name too, so it must
// run the same fenced rename as the local rename handler: otherwise an
// in-flight read can republish the pre-retarget image under the recycled name.
func TestRetargetPathFencesInFlightReadPublication(t *testing.T) {
	c, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	inner := &gatedReadHandle{
		started: make(chan struct{}),
		release: make(chan struct{}),
		data:    []byte("old"),
	}
	h := &cachingHandle{cache: c, inner: inner, path: "/doomed", rev: 1, size: 3}

	readErr := make(chan error, 1)
	go func() {
		_, err := h.Read(context.Background(), 0, make([]byte, 3))
		readErr <- err
	}()
	<-inner.started

	c.RetargetPath("/doomed", "/survivor")

	close(inner.release)
	if err := <-readErr; err != nil {
		t.Fatalf("in-flight read: %v", err)
	}
	// The read captured the DOOMED name before the retarget, so an unfenced
	// retarget lets it repopulate the recycled name; a fenced one rejects it.
	for _, name := range []string{"/doomed", "/survivor"} {
		if data, ok := c.mem.Get(name, 1); ok {
			t.Fatalf("in-flight read repopulated %s with %q", name, data)
		}
	}
}

// A sweep must drop the INDEX inside the fence and unlink the backing files
// after it: the unlink batch is O(blocks) I/O and must not serialize every
// reader publication behind the fence.
func TestFencedSweepUnlinksDiskBlocksAfterReleasingFence(t *testing.T) {
	c, err := New(Options{Disk: DiskReadCacheOptions{Dir: t.TempDir(), MaxSize: 1 << 20, FreeRatio: -1}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.dsk.Close() })

	key := DiskReadCacheKey{FileID: pathDiskReadCacheFileID("/f"), Path: "/f", Revision: 1, Offset: 0, Length: 3}
	c.dsk.Put(key, []byte("abc"))
	backing := c.dsk.pathForDigest(key.digest())
	if _, err := os.Stat(backing); err != nil {
		t.Fatalf("block file missing before the sweep: %v", err)
	}

	var existedInsideFence bool
	c.fencedSweep(func() []string {
		detached := c.invalidateDiskPath("/f")
		if _, err := os.Stat(backing); err == nil {
			existedInsideFence = true
		}
		return detached
	})
	if !existedInsideFence {
		t.Fatal("block was unlinked while the publication fence was still held")
	}
	if _, err := os.Stat(backing); !os.IsNotExist(err) {
		t.Fatalf("block file survived the sweep: %v", err)
	}
	if c.dsk.Len() != 0 {
		t.Fatalf("index entries after the sweep = %d, want 0", c.dsk.Len())
	}
}

// remoteReadableHandle is an inner plane that claims the remote namespace so
// the read path runs its revalidation stat (the window between the path
// snapshot and the publication epoch).
type remoteReadableHandle struct {
	*gatedReadHandle
}

func (h remoteReadableHandle) IsRemoteNamespaceReadable() bool { return true }

// The publication epoch must be captured BEFORE the path/identity snapshot.
// A rename that lands while the read is revalidating (a remote stat between
// the two) would otherwise hand the read the post-sweep epoch, and its old
// bytes would be published under the pre-rename path key — exactly the stale
// image the fence exists to drop.
func TestEpochCapturedBeforePathSnapshot(t *testing.T) {
	statStarted := make(chan struct{})
	statRelease := make(chan struct{})
	var statOnce sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			http.NotFound(w, r)
			return
		}
		statOnce.Do(func() { close(statStarted) })
		<-statRelease
		w.Header().Set("Content-Length", "3")
		w.Header().Set("X-Dat9-IsDir", "false")
		w.Header().Set("X-Dat9-Revision", "1")
		w.Header().Set("X-Dat9-Resource-ID", "res-1")
		w.Header().Set("X-Dat9-Mtime", "1")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	stub := &gatedReadHandle{
		started: make(chan struct{}),
		release: make(chan struct{}),
		data:    []byte("old"),
	}
	c, err := New(Options{PrefetchClient: client.New(srv.URL, "")})
	if err != nil {
		t.Fatal(err)
	}
	releaseRead := make(chan struct{})
	h := &cachingHandle{
		cache: c, inner: remoteReadableHandle{stub}, path: "/target",
		rev: 1, size: 3, objectID: "res-1", revalidate: true,
	}
	readErr := make(chan error, 1)
	go func() {
		_, err := h.Read(context.Background(), 0, make([]byte, 3))
		readErr <- err
	}()
	<-statStarted

	// The rename completes (fenced sweep) while the read is between its path
	// snapshot and its fetch.
	c.renameTiers("/tmp", "/target")

	close(statRelease)
	<-stub.started
	close(stub.release)
	if err := <-readErr; err != nil {
		t.Fatalf("in-flight read: %v", err)
	}
	close(releaseRead)
	if data, ok := c.mem.Get("/target", 1); ok {
		t.Fatalf("read republished %q under the pre-rename path after the sweep", data)
	}
}
