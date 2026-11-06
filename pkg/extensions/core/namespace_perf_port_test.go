package core

import (
	"context"
	"fmt"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/dircache"
)

func newPerfCore(t *testing.T, b *testbackend.Backend) (*Core, func()) {
	t.Helper()
	ts := httptest.NewServer(b)
	cl := client.New(ts.URL, "")
	c, err := New(cl, Options{})
	if err != nil {
		t.Fatal(err)
	}
	return c, ts.Close
}

func headCount(b *testbackend.Backend, path string) int {
	return b.Requests()["HEAD /v1/fs"+path]
}

// v1 Lookup served a fresh listing row without a remote stat. #1026 gated
// every positive row behind trust/LocalMutation, so a post-entryTTL path
// lookup paid a HEAD even though the parent listing already carried it.
func TestPortLookupServesFreshListingRow(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/d/f", []byte("x"))
	c, stop := newPerfCore(t, b)
	defer stop()
	dc := dircache.NewNamespaceCache(10*time.Second, time.Second, 1000)
	c.Attach(dc)
	req := dc.BeginRequest("/d")
	dc.PutListing("/d", []fs.DirEntryInfo{{Name: "f", Size: 1, Rev: 1, Mode: 0o644, HasMode: true}}, req)
	dc.EndRequest(req)

	ctx := context.Background()
	if _, err := c.statOp(ctx, "/d/f", true); err != nil {
		t.Fatal(err)
	}
	if got := headCount(b, "/d/f"); got != 0 {
		t.Fatalf("listing-backed lookup HEADs = %d, want 0", got)
	}
}

// A successful Lookup HEAD must seed the listing cache; otherwise every
// repeated lookup after the kernel entry TTL pays another HEAD.
func TestPortLookupStatObservesForNextLookup(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/d/f", []byte("x"))
	c, stop := newPerfCore(t, b)
	defer stop()
	c.Attach(dircache.NewNamespaceCache(10*time.Second, time.Second, 1000))

	ctx := context.Background()
	if _, err := c.statOp(ctx, "/d/f", true); err != nil {
		t.Fatal(err)
	}
	first := headCount(b, "/d/f")
	if first != 1 {
		t.Fatalf("first lookup HEADs = %d, want 1", first)
	}
	if _, err := c.statOp(ctx, "/d/f", true); err != nil {
		t.Fatal(err)
	}
	if got := headCount(b, "/d/f"); got != first {
		t.Fatalf("observed lookup re-HEADed: %d -> %d", first, got)
	}
}

// A GetAttr still revalidates listing-observed rows: the attr reply feeds
// size/revision decisions, where a foreign write must not hide behind a
// cached listing.
func TestPortGetAttrRevalidatesObservedRow(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/d/f", []byte("x"))
	c, stop := newPerfCore(t, b)
	defer stop()
	dc := dircache.NewNamespaceCache(10*time.Second, time.Second, 1000)
	c.Attach(dc)
	dc.ObserveRemote("/d", fs.DirEntryInfo{Name: "f", Size: 1, Rev: 1, Mode: 0o644, HasMode: true})

	if _, err := c.statOp(context.Background(), "/d/f", false); err != nil {
		t.Fatal(err)
	}
	if got := headCount(b, "/d/f"); got != 1 {
		t.Fatalf("GetAttr listing row HEADs = %d, want 1", got)
	}
}

// The negative-lookup storm LIST used to be discarded; install it so the
// next new name in the directory is a complete miss, not another HEAD.
func TestPortStormListingInstalls(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/d", 0o755)
	c, stop := newPerfCore(t, b)
	defer stop()
	c.Attach(dircache.NewNamespaceCache(10*time.Second, time.Second, 1000))

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_, _ = c.statOp(ctx, fmt.Sprintf("/d/new-%d", i), true)
	}
	if got := b.Requests()["GET /v1/fs/d"]; got != 1 {
		t.Fatalf("parent LISTs = %d, want 1", got)
	}
	heads := 0
	for k, v := range b.Requests() {
		if len(k) > 5 && k[:5] == "HEAD " {
			heads += v
		}
	}
	if heads != 3 {
		t.Fatalf("HEADs = %d, want 3 (the storm listing answers the rest)", heads)
	}
}

// A kernel CREATE already proved the name missing; probing it and then
// stat-ing the new file added two HEADs to every create.
func TestPortKernelCreateSkipsProbesAndSynthesizesAttrs(t *testing.T) {
	b := testbackend.NewBackend()
	c, stop := newPerfCore(t, b)
	defer stop()

	res, err := c.createOp(context.Background(), &fs.Op{
		Kind: fs.OpCreate, Path: "/new", Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY),
		Mode: 0o644, Kernel: &gofuse.CreateIn{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Attr == nil || res.Attr.Size != 0 || res.Attr.Rev <= 0 || res.Handle == nil {
		t.Fatalf("create result = %+v, want empty attr with revision and handle", res)
	}
	if got := headCount(b, "/new"); got != 0 {
		t.Fatalf("kernel create HEADs = %d, want 0", got)
	}
	if got := b.Requests()["POST /v1/fs/new"]; got != 1 {
		t.Fatalf("create POSTs = %d, want 1", got)
	}
}

// A read-only FUSE OPEN follows a successful lookup; the open reply carries
// no attributes, so the terminal must not stat again.
func TestPortReadOnlyOpenSkipsProbe(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("x"))
	c, stop := newPerfCore(t, b)
	defer stop()

	res, err := c.createOp(context.Background(), &fs.Op{
		Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDONLY),
		Kernel:   &gofuse.OpenIn{},
		OpenAttr: &fs.Attr{Kind: fs.KindFile, Mode: 0o644, Size: 1, Rev: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Handle == nil {
		t.Fatal("read-only open returned no handle")
	}
	if res.Attr == nil || res.Attr.Size != 0 {
		t.Fatalf("read-only open attr = %+v, want size 0 (unknown)", res.Attr)
	}
	if got := headCount(b, "/f"); got != 0 {
		t.Fatalf("read-only open HEADs = %d, want 0", got)
	}
}

// A just-created LocalMutation row can have Rev 0 until its create lands.
// A writable open must re-stat in that case; otherwise an overwrite/truncate
// stages PendingOverwrite at base 0 and the uploader rejects it as "missing
// base revision for overwrite".
func TestPortWritableOpenRevalidatesZeroRevisionRow(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("old"))
	c, stop := newPerfCore(t, b)
	defer stop()
	dc := dircache.NewNamespaceCache(10*time.Second, time.Second, 1000)
	c.Attach(dc)
	dc.Upsert("/", fs.DirEntryInfo{
		Name: "f", Size: 3, Rev: 0, Mode: 0o644, HasMode: true, LocalMutation: true,
	})

	res, err := c.createOp(context.Background(), &fs.Op{
		Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_WRONLY),
		Kernel: &gofuse.OpenIn{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Attr == nil || res.Attr.Rev <= 0 {
		t.Fatalf("writable open attr = %+v, want a remote revision", res.Attr)
	}
	if got := headCount(b, "/f"); got != 1 {
		t.Fatalf("zero-revision writable open HEADs = %d, want 1", got)
	}
}

// A kernel CREATE that loses to an existing file (stale negative) must
// still honor O_TRUNC during CREATE handling; deferring it to a later write
// leaves the old suffix visible if the writer crashes before writing.
func TestPortKernelCreateConflictPreservesTruncate(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("abcdef"))
	c, stop := newPerfCore(t, b)
	defer stop()
	res, err := c.createOp(context.Background(), &fs.Op{
		Kind: fs.OpCreate, Path: "/f",
		Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY),
		Mode:  0o644, HasMode: true, Kernel: &gofuse.CreateIn{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Attr == nil || res.Attr.Size != 0 {
		t.Fatalf("create-conflict attr = %+v, want size 0", res.Attr)
	}
	if n, ok := b.Node("/f"); !ok || len(n.Data) != 0 {
		t.Fatalf("remote after create-conflict truncate = %q ok=%v", n.Data, ok)
	}
}
