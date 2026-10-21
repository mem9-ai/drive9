package writeback

// qiffang twenty-second round: a buffered overwrite keeps the CAS base its
// image was built from, the write cache is queried by LOCAL path, a clean
// same-path sibling sees an ftruncate, and a live writer's lazy base parts
// load before a sibling read.

import (
	"bytes"
	"context"
	"net/http/httptest"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

func newWriteCacheClientStack(t *testing.T, policy Policy, b *testbackend.Backend, opts Options) (*fs.Chain, *Writeback, *client.Client) {
	t.Helper()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	opts.Policy = policy
	opts.Client = cl
	if opts.WriteCache == nil {
		cache, err := staging.NewWriteBackCache(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		opts.WriteCache = cache
	}
	wb, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{RemoteRoot: opts.RemoteRoot})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	return ch, wb, cl
}

// A foreign write that lands between the buffered open and the flush must
// stay a CONFLICT: staging cannot adopt the foreign revision and let the
// stale image overwrite it (hello + local X + foreign WORLD must not become
// Xello).
func TestPortStageKeepsCapturedBaseOnForeignWrite(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello"))
	ch, wb, cl := newWriteCacheClientStack(t, PolicyCloseSync, b, Options{})
	ctx := context.Background()

	h := open(t, ch, "/f")
	if err := h.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatal(err)
	}
	if err := cl.WriteCtx(ctx, "/f", []byte("WORLD")); err != nil {
		t.Fatal(err)
	}
	// The conflict is the correct outcome (the base rejects the commit);
	// the foreign image must survive and the local image stay pending.
	flushErr := h.Flush(ctx)
	_ = h.Release(ctx)
	_ = wb.Drain(ctx)

	if node, _ := b.Node("/f"); string(node.Data) != "WORLD" {
		t.Fatalf("backend = %q (flush err %v), want the foreign WORLD preserved", node.Data, flushErr)
	}
}

// The pending write cache is keyed by the mount's LOCAL path: under a
// RemoteRoot subtree the preload used to query the prefixed name and miss
// the staged image, serving the stale remote base.
func TestPortWriteCacheLookupUsesLocalPath(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/team/f", []byte("stale"))
	cache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ch, _, _ := newWriteCacheClientStack(t, PolicyWriteBack, b, Options{RemoteRoot: "/team", WriteCache: cache})
	ctx := context.Background()

	if err := cache.PutWithBaseRev("/f", []byte("fresh"), 5, staging.PendingOverwrite, 1); err != nil {
		t.Fatal(err)
	}
	h := open(t, ch, "/f")
	buf := make([]byte, 5)
	n, rerr := h.Read(ctx, 0, buf)
	if rerr != nil {
		t.Fatalf("read = %v", rerr)
	}
	if got := string(buf[:n]); got != "fresh" {
		t.Fatalf("read = %q, want the staged local image", got)
	}
}

// A clean same-path sibling's buffer is a pre-truncate read cache: the
// ftruncate must retire it, or the sibling keeps serving the old tail.
func TestPortFtruncateClearsCleanSiblingBuffer(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello world"))
	ch, _ := newStagedStack(t, PolicyCloseSync, b)
	ctx := context.Background()

	w1 := open(t, ch, "/f")
	w2 := open(t, ch, "/f")
	// The facade stamps the kernel inode on open; the admission and the
	// visibility tiers resolve siblings by it.
	bw1, _ := w1.(*bufferingHandle)
	bw2, _ := w2.(*bufferingHandle)
	bw1.mu.Lock()
	bw1.ino = 1
	bw1.mu.Unlock()
	bw2.mu.Lock()
	bw2.ino = 1
	bw2.mu.Unlock()
	if err := w2.Write(ctx, 0, []byte("H")); err != nil {
		t.Fatal(err)
	}
	// Land h2's image: its buffer stays loaded but clean.
	if err := w2.Sync(ctx); err != nil {
		t.Fatalf("sync = %v", err)
	}
	if err := truncateViaHandle(t, ch, "/f", w1, 5); err != nil {
		t.Fatalf("ftruncate = %v", err)
	}
	_, got := readAt(t, w2, 0, 11)
	if string(got) != "Hello" {
		t.Fatalf("sibling read = %q, want the truncated Hello", got)
	}
}

// A live writer's buffer holds only the preload window; a sibling read of a
// later region must load those base parts instead of zero-filling them.
func TestPortLiveWriterReadLoadsLazyParts(t *testing.T) {
	const size = 12 << 20
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	b := testbackend.NewBackend()
	b.Seed("/big", payload)
	ch, _, _ := newWriteCacheClientStack(t, PolicyWriteBack, b, Options{PreloadLimit: 1 << 20})
	ctx := context.Background()

	writer := open(t, ch, "/big")
	if err := writer.Write(ctx, 0, []byte{'X'}); err != nil {
		t.Fatal(err)
	}
	reader := openRO(t, ch, "/big")
	buf := make([]byte, 4)
	n, err := reader.Read(ctx, 8<<20, buf)
	if err != nil {
		t.Fatalf("read = %v", err)
	}
	if n != 4 || !bytes.Equal(buf[:n], payload[8<<20:8<<20+4]) {
		t.Fatalf("read@8MiB = %v (n=%d), want %v", buf[:n], n, payload[8<<20:8<<20+4])
	}
}

// The flush after the pre-rename upload can stage a NEW generation under the
// OLD path; it must land before the remote rename, or the acknowledged bytes
// are stranded and the rename moves the stale image.
func TestPortRenameLandsFlushedImage(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/old", []byte("OLD!"))
	ch, wb, cache := newWriteCacheStack(t, PolicyWriteBack, b)
	ctx := context.Background()
	h := open(t, ch, "/old")
	if err := h.Write(ctx, 0, []byte("NEW!")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/old", Path2: "/new"}); err != nil {
		t.Fatalf("rename = %v", err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if node, ok := b.Node("/new"); !ok || string(node.Data) != "NEW!" {
		t.Fatalf("/new = %q ok=%v, want NEW!", node.Data, ok)
	}
	if _, ok := cache.GetMeta("/old"); ok {
		t.Fatal("staging survived under the old path after the rename")
	}
}

// A size-only SETATTR reply keeps the object's permissions.
func TestPortFtruncateReplyKeepsMode(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello"))
	ch, _, _ := newWriteCacheStack(t, PolicyWriteBack, b)
	ctx := context.Background()
	h := open(t, ch, "/f")
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/f", Size: 2, Handle: h})
	if err != nil {
		t.Fatalf("ftruncate = %v", err)
	}
	if res.Attr == nil || res.Attr.Mode&0o777 != 0o644 || res.Attr.Size != 2 {
		t.Fatalf("ftruncate reply = %+v, want size 2 and the permissions kept", res.Attr)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

// A pending chmod updates the write-back metadata under the LOCAL key even
// on a subtree mount (no live writer: the image is staged in the cache).
func TestPortPendingChmodUsesLocalKey(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/team/f", []byte("hello"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, RemoteRoot: "/team", WriteCache: cache})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{RemoteRoot: "/team"})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// Stage an image under the LOCAL key with the object's real permissions.
	if err := cache.PutWithBaseRevAndMode("/f", []byte("EDIT"), 5, staging.PendingOverwrite, 1, 0o644, true); err != nil {
		t.Fatal(err)
	}
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/f", HasMode: true, Mode: 0o600, Size: -1})
	if err != nil || res.Attr == nil || res.Attr.Mode&0o777 != 0o600 {
		t.Fatalf("chmod = %+v err=%v", res.Attr, err)
	}
	meta, ok := cache.GetMeta("/f")
	if !ok || !meta.HasMode || meta.Mode&0o777 != 0o600 {
		t.Fatalf("staged meta = %+v ok=%v, want the local key to carry mode 0600", meta, ok)
	}
	if _, ok := cache.GetMeta("/team/f"); ok {
		t.Fatal("metadata was written under the remote key")
	}
}

// A write continuing after the rename must commit on top of the LANDED
// revision: the landing returned by UploadSync was not fed back to the live
// handle, so its next flush CASed on the pre-rename revision and conflicted.
func TestPortRenameSettlesHandleRevision(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/a", []byte("HELLO"))
	ch, wb, _ := newWriteCacheStack(t, PolicyWriteBack, b)
	ctx := context.Background()
	h := open(t, ch, "/a")
	if err := h.Write(ctx, 0, []byte("NEW!!")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/a", Path2: "/moved"}); err != nil {
		t.Fatalf("rename = %v", err)
	}
	if err := h.Write(ctx, 5, []byte("AFTER")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if node, ok := b.Node("/moved"); !ok || string(node.Data) != "NEW!!AFTER" {
		t.Fatalf("/moved = %q ok=%v, want NEW!!AFTER", node.Data, ok)
	}
}

// A rename OVER an open target keeps the target's descriptors on the OLD
// object's bytes.
func TestPortRenameOverOpenTargetKeepsItsImage(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/a", []byte("HELLO"))
	b.Seed("/b", []byte("OTHER"))
	ch, _, _ := newWriteCacheStack(t, PolicyWriteBack, b)
	ctx := context.Background()
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/b", Flags: 0})
	if err != nil || res.Handle == nil {
		t.Fatalf("open target = %v", err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/a", Path2: "/b"}); err != nil {
		t.Fatalf("rename = %v", err)
	}
	buf := make([]byte, 16)
	n, err := res.Handle.Read(ctx, 0, buf)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(buf[:n]); got != "OTHER" {
		t.Fatalf("overwritten target fd read %q, want OTHER", got)
	}
	_ = res.Handle.Release(ctx)
}

// The deferred-unlink snapshot survives the queue cancel: the anonymous fd
// keeps reading the OLD object after a same-name recreate lands.
func TestPortDeferredUnlinkSnapshotSurvivesRecreate(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/a", []byte("HELLO"))
	ch, wb := newStagedStack(t, PolicyWriteBack, b)
	ctx := context.Background()
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/a", Flags: 0})
	if err != nil || res.Handle == nil {
		t.Fatalf("open = %v", err)
	}
	wb.MarkPathUnlinked("/a")
	// Recreate and commit a NEW file at the same name.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/a"}); err != nil && !fs.IsNotFound(err) {
		t.Fatalf("unlink = %v", err)
	}
	h2 := open(t, ch, "/a")
	if err := h2.Write(ctx, 0, []byte("FRESH")); err != nil {
		t.Fatal(err)
	}
	if err := h2.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 16)
	n, err := res.Handle.Read(ctx, 0, buf)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(buf[:n]); got != "HELLO" {
		t.Fatalf("anonymous fd read %q after recreate, want HELLO", got)
	}
	_ = res.Handle.Release(ctx)
}

// A single open→unlink→release cycle must retire the snapshot: the drop
// has to unpin with the LIFECYCLE token and remove by the CONTENT
// generation (they are different values).
func TestPortUnlinkSnapshotRetiresCleanly(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/a", []byte("HELLO"))
	ch, wb := newStagedStack(t, PolicyWriteBack, b)
	ctx := context.Background()
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/a", Flags: 0})
	if err != nil || res.Handle == nil {
		t.Fatalf("open = %v", err)
	}
	wb.MarkPathUnlinked("/a")
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/a"}); err != nil && !fs.IsNotFound(err) {
		t.Fatalf("unlink = %v", err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if wb.opts.Staging.Shadow.Has("/a") {
		t.Fatal("snapshot shadow survived the last close")
	}
	if wb.opts.Staging.Shadow.Has("/a") {
		t.Fatal("snapshot shadow still present")
	}
}

// Two clean fds of the same name share ONE snapshot pin: the last close
// retires it without leaking a reference.
func TestPortUnlinkSnapshotSharedPinRetires(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/a", []byte("HELLO"))
	ch, wb := newStagedStack(t, PolicyWriteBack, b)
	ctx := context.Background()
	open := func() fs.Handle {
		r, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/a", Flags: 0})
		if err != nil || r.Handle == nil {
			t.Fatalf("open = %v", err)
		}
		return r.Handle
	}
	h1, h2 := open(), open()
	wb.MarkPathUnlinked("/a")
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/a"}); err != nil && !fs.IsNotFound(err) {
		t.Fatalf("unlink = %v", err)
	}
	if err := h1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if wb.opts.Staging.Shadow.Has("/a") {
		t.Fatal("snapshot shadow survived the last close of two fds")
	}
}

// An EMPTY overwritten target is an authoritative empty image, not "no
// snapshot".
func TestPortRenameOverEmptyTargetServesEOF(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/a", []byte("HELLO"))
	b.Seed("/b", []byte{})
	ch, _, _ := newWriteCacheStack(t, PolicyWriteBack, b)
	ctx := context.Background()
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/b", Flags: 0})
	if err != nil || res.Handle == nil {
		t.Fatalf("open target = %v", err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/a", Path2: "/b"}); err != nil {
		t.Fatalf("rename = %v", err)
	}
	buf := make([]byte, 16)
	n, err := res.Handle.Read(ctx, 0, buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("empty overwritten target read %d bytes (%q), want EOF", n, buf[:n])
	}
	_ = res.Handle.Release(ctx)
}

// A writable anonymous fd (no buffer yet) keeps the OLD object's image for
// reads and for its first buffer preload.
func TestPortWritableAnonymousFdKeepsOldImage(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/a", []byte("HELLO"))
	ch, wb := newStagedStack(t, PolicyWriteBack, b)
	ctx := context.Background()
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/a", Flags: uint32(syscall.O_RDWR)})
	if err != nil || res.Handle == nil {
		t.Fatalf("open = %v", err)
	}
	wb.MarkPathUnlinked("/a")
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/a"}); err != nil && !fs.IsNotFound(err) {
		t.Fatalf("unlink = %v", err)
	}
	h2 := open(t, ch, "/a")
	if err := h2.Write(ctx, 0, []byte("FRESH")); err != nil {
		t.Fatal(err)
	}
	if err := h2.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 16)
	n, err := res.Handle.Read(ctx, 0, buf)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(buf[:n]); got != "HELLO" {
		t.Fatalf("writable anonymous fd read %q, want HELLO", got)
	}
	// Its first write composes on the old image (discarded anonymously).
	if err := res.Handle.Write(ctx, 5, []byte("!")); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

// The snapshot must capture the CURRENT object size: a sibling write that
// grew the target after the fd was opened is part of the old object.
func TestPortOverwrittenTargetUsesCurrentSize(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/a", []byte("HELLO"))
	b.Seed("/b", []byte{})
	ch, wb, _ := newWriteCacheStack(t, PolicyWriteBack, b)
	ctx := context.Background()
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/b", Flags: 0})
	if err != nil || res.Handle == nil {
		t.Fatalf("open target = %v", err)
	}
	h2 := open(t, ch, "/b")
	if err := h2.Write(ctx, 0, []byte("LONGER-OBJECT")); err != nil {
		t.Fatal(err)
	}
	if err := h2.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/a", Path2: "/b"}); err != nil {
		t.Fatalf("rename = %v", err)
	}
	buf := make([]byte, 32)
	n, rerr := res.Handle.Read(ctx, 0, buf)
	if rerr != nil {
		t.Fatal(rerr)
	}
	if got := string(buf[:n]); got != "LONGER-OBJECT" {
		t.Fatalf("old target fd read %q, want LONGER-OBJECT", got)
	}
	_ = res.Handle.Release(ctx)
}

// Concurrent last closes elect exactly one snapshot reclaimer.
func TestPortConcurrentLastCloseReclaimsSnapshot(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/a", []byte("HELLO"))
	ch, wb := newStagedStack(t, PolicyWriteBack, b)
	ctx := context.Background()
	openOne := func() fs.Handle {
		r, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/a", Flags: 0})
		if err != nil || r.Handle == nil {
			t.Fatalf("open = %v", err)
		}
		return r.Handle
	}
	h1, h2 := openOne(), openOne()
	wb.MarkPathUnlinked("/a")
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/a"}); err != nil && !fs.IsNotFound(err) {
		t.Fatalf("unlink = %v", err)
	}
	start := make(chan struct{})
	done := make(chan struct{}, 2)
	go func() { <-start; _ = h1.Release(ctx); done <- struct{}{} }()
	go func() { <-start; _ = h2.Release(ctx); done <- struct{}{} }()
	close(start)
	<-done
	<-done
	if wb.opts.Staging.Shadow.Has("/a") {
		t.Fatal("snapshot shadow survived concurrent last closes")
	}
}
