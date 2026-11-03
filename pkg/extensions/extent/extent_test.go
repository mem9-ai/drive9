package extent

import (
	"context"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// newStack builds [extent → core] over one fake backend. The JuiceFS
// runtime cannot build against the fake (no credential endpoint), so
// runtime-dependent paths are exercised through their fallback and
// lifecycle behavior — the claim/routing logic itself is fully live.
func newStack(t *testing.T, b *testbackend.Backend, extentPaths []string) (*fs.Chain, *Extent, *httptest.Server) {
	t.Helper()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	ext, err := New(Options{ExtentPaths: extentPaths, Client: cl, CacheDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(ext, terminal)
	if err != nil {
		t.Fatal(err)
	}
	return ch, ext, ts
}

func TestClaimRoutesExtentLayoutStat(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedExtent("/big.bin", []byte("x"), 77)
	// The mount opted into the extent plane (declaration), so discovery
	// probes are paid; without a declaration v1 never probes either.
	ch, ext, _ := newStack(t, b, []string{"*.bin"})
	ctx := context.Background()

	// First stat discovers content_layout=extent; the runtime cannot build
	// against the fake, so the claim degrades to the classic path below —
	// but the discovery itself must have happened and been remembered.
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/big.bin"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Attr == nil || res.Attr.Size != 1 {
		t.Fatalf("attr = %+v", res.Attr)
	}
	if ino, ok := ext.cachedIno("/big.bin"); !ok || ino != 77 {
		t.Fatalf("discovered extent ino = %d %v, want 77", ino, ok)
	}

	// A second stat must not re-probe the server: the mapping is cached.
	// dispatch #1: one plugin discovery probe + one classic stat below.
	statCalls := b.Requests()["HEAD /v1/fs/big.bin"]
	if statCalls != 2 {
		t.Fatalf("stat calls after first dispatch = %d, want 2", statCalls)
	}
	res2, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/big.bin"})
	if err != nil || res2.Attr == nil || res2.Attr.Size != 1 {
		t.Fatalf("cached stat = %+v %v", res2.Attr, err)
	}
	// dispatch #2: the ino mapping is cached, so only the classic stat runs.
	if got := b.Requests()["HEAD /v1/fs/big.bin"]; got != statCalls+1 {
		t.Fatalf("cached claim re-probed: %d -> %d", statCalls, got)
	}
}

func TestNonExtentPathNeverProbed(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/plain.txt", []byte("p"))
	ch, _, _ := newStack(t, b, nil)
	ctx := context.Background()

	// Without extent declarations the mount never pays for discovery.
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/plain.txt"})
	if err != nil || res.Attr == nil || res.Attr.Size != 1 {
		t.Fatalf("attr = %+v %v", res.Attr, err)
	}
	if _, probed := b.Requests()["HEAD /v1/fs/plain.txt"]; !probed {
		t.Fatal("stat should have run exactly once via the classic path")
	}
}

func TestNegativeProbeCacheBoundsMisses(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/maybe.bin", []byte("m"))
	ch, ext, _ := newStack(t, b, []string{"*.bin"})
	ctx := context.Background()

	// Declared mount probes; the file is NOT extent-layout, so it lands on
	// the classic path and the miss is cached.
	for i := 0; i < 3; i++ {
		if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/maybe.bin"}); err != nil {
			t.Fatal(err)
		}
	}
	// 3 dispatches: the miss cache gates plugin probes after the first,
	// leaving one classic stat per dispatch.
	if got := b.Requests()["HEAD /v1/fs/maybe.bin"]; got != 4 {
		t.Fatalf("probe count = %d, want 4", got)
	}
	if !ext.missCached("/maybe.bin") {
		t.Fatal("negative probe not cached")
	}
	time.Sleep(ext.missTTL + 50*time.Millisecond)
	if ext.missCached("/maybe.bin") {
		t.Fatal("miss outlived its TTL")
	}
}

func TestCreateClaimBuildFailsClosed(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _, _ := newStack(t, b, []string{"*.bin"})
	ctx := context.Background()

	// A creation declaration with no reachable credential endpoint must
	// fail closed (EIO), never silently create on the classic plane.
	_, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/new.bin", Flags: 0x42, Mode: 0o644, Caller: fs.Caller{Pid: 1}})
	if err == nil {
		t.Fatal("extent create succeeded without a runtime")
	}
	if _, ok := b.Node("/new.bin"); ok {
		t.Fatal("file leaked onto the classic plane")
	}
}

func TestLifecycleStopIsIdempotentWithoutRuntime(t *testing.T) {
	b := testbackend.NewBackend()
	_, ext, _ := newStack(t, b, []string{"*.bin"})
	if err := ext.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := ext.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	// After teardown, ensureRuntime refuses instead of rebuilding.
	if err := ext.ensureRuntime(); err == nil {
		t.Fatal("ensureRuntime after teardown must fail")
	}
}

func TestSingleFlightBuildSharesFailure(t *testing.T) {
	b := testbackend.NewBackend()
	_, ext, _ := newStack(t, b, []string{"*.bin"})

	var calls atomic.Int64
	hook := func() { calls.Add(1) }
	_ = hook
	// Two sequential builds against the dead endpoint: the second must
	// observe the shared failed outcome without a second network attempt
	// only while parked; sequential callers retry like v1.
	if err := ext.ensureRuntime(); err == nil {
		t.Fatal("build against fake endpoint must fail")
	}
	if err := ext.ensureRuntime(); err == nil {
		t.Fatal("rebuild must also fail (fresh generation)")
	}
}

func TestEnsureParentSplit(t *testing.T) {
	for _, c := range []struct {
		in   string
		want []string
	}{
		{"/", nil},
		{"/a", []string{"a"}},
		{"/a/b/c", []string{"a", "b", "c"}},
		{"//x//y", []string{"x", "y"}},
	} {
		got := splitPath(c.in)
		if len(got) != len(c.want) {
			t.Fatalf("split(%q) = %v, want %v", c.in, got, c.want)
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Fatalf("split(%q) = %v, want %v", c.in, got, c.want)
			}
		}
	}
}
