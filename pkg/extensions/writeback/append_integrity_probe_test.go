package writeback

// PR #1024 (#1023) contract probes against the new stack: independent
// O_APPEND handles must not lose acknowledged records when a sibling
// commits a newer image (A, B, A+B fsynced, then C must yield A+B+C, not
// A+C); hardlink aliases participate in the same file's append
// composition.

import (
	"context"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

func appendIntegrityStack(t *testing.T, ts *httptest.Server) (*Writeback, *fs.Chain) {
	t.Helper()
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyCloseSync, Client: cl, Staging: mustStaging(t)})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	return wb, ch
}

func openAppend(t *testing.T, ch *fs.Chain, path string) fs.Handle {
	t.Helper()
	res, err := ch.Dispatch(context.Background(), &fs.Op{
		Kind: fs.OpCreate, Path: path,
		Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY | syscall.O_APPEND), Mode: 0o644,
	})
	if err != nil {
		t.Fatal(err)
	}
	return res.Handle
}

// The #1023 core: H1 appends A; H2 appends B; H2 fsyncs (A+B lands); H1
// appends C and flushes — the final image must be A+B+C.
func TestProbeAppendSiblingCommitPreservesRecords(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	wb, ch := appendIntegrityStack(t, ts)
	ctx := context.Background()

	h1 := openAppend(t, ch, "/f")
	if err := h1.Write(ctx, 0, []byte("A")); err != nil {
		t.Fatal(err)
	}
	h2 := openAppend(t, ch, "/f")
	if err := h2.Write(ctx, 0, []byte("B")); err != nil {
		t.Fatal(err)
	}
	if err := h2.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h1.Write(ctx, 0, []byte("C")); err != nil {
		t.Fatal(err)
	}
	if err := h1.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := b.Node("/f")
	if string(n.Data) != "ABC" {
		t.Fatalf("final image = %q, want ABC (a sibling record was lost)", string(n.Data))
	}
}

// Both handles stay open and sync in interleaved order.
func TestProbeAppendInterleavedSyncs(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	wb, ch := appendIntegrityStack(t, ts)
	ctx := context.Background()

	h1 := openAppend(t, ch, "/f")
	h2 := openAppend(t, ch, "/f")
	for _, step := range []struct {
		h   fs.Handle
		rec byte
	}{
		{h1, 'A'}, {h2, 'B'}, {h1, 'C'}, {h2, 'D'},
	} {
		if err := step.h.Write(ctx, 0, []byte{step.rec}); err != nil {
			t.Fatal(err)
		}
		if err := step.h.Sync(ctx); err != nil {
			t.Fatalf("sync after %c: %v", step.rec, err)
		}
	}
	if err := h1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := b.Node("/f")
	if string(n.Data) != "ABCD" {
		t.Fatalf("final image = %q, want ABCD", string(n.Data))
	}
}

// Hardlink aliases: appends through both names compose one image (the
// inode table reports /b as /a's alias, as the facade does after Link).
func TestProbeAppendAcrossHardlinkAliases(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/a", []byte{})
	b.Seed("/b", []byte{})
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyCloseSync, Client: cl, Staging: mustStaging(t)})
	if err != nil {
		t.Fatal(err)
	}
	wb.SetInodeTable(&aliasInodeTable{pairs: map[string][]string{"/a": {"/a", "/b"}, "/b": {"/a", "/b"}}})
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	ha := openAppend(t, ch, "/a")
	if err := ha.Write(ctx, 0, []byte("A")); err != nil {
		t.Fatal(err)
	}
	hb := openAppend(t, ch, "/b")
	if err := hb.Write(ctx, 0, []byte("B")); err != nil {
		t.Fatal(err)
	}
	if err := hb.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ha.Write(ctx, 0, []byte("C")); err != nil {
		t.Fatal(err)
	}
	if err := ha.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ha.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := hb.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	na, _ := b.Node("/a")
	if string(na.Data) != "ABC" {
		t.Fatalf("/a final image = %q, want ABC", string(na.Data))
	}
}

// aliasInodeTable is the minimal InodeTable the append refresh consults:
// one inode with two hardlink paths.
type aliasInodeTable struct{ pairs map[string][]string }

func (t *aliasInodeTable) Ino(path string) (uint64, bool) { return 7, true }
func (t *aliasInodeTable) Path(ino uint64) (string, bool) { return "/a", true }
func (t *aliasInodeTable) Epoch(path string) uint64      { return 0 }
func (t *aliasInodeTable) NextEpoch(path string) uint64  { return 0 }
func (t *aliasInodeTable) Aliases(ino uint64) []string   { return t.pairs["/a"] }

// PR #1024's writeback-policy variant: under the async write-back policy
// (flush stages locally; the queue lands asynchronously), interleaved
// appends must still compose — the staging fence refreshes onto the
// sibling's landed image before publishing.
func TestProbeAppendWritebackPolicyComposition(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wbcache, _ := staging.NewWriteBackCache(t.TempDir())
	wb, _ := New(Options{Policy: PolicyWriteBack, Client: cl, Staging: mustStaging(t), WriteCache: wbcache})
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	h1 := openAppend(t, ch, "/f")
	if err := h1.Write(ctx, 0, []byte("A")); err != nil {
		t.Fatal(err)
	}
	h2 := openAppend(t, ch, "/f")
	if err := h2.Write(ctx, 0, []byte("B")); err != nil {
		t.Fatal(err)
	}
	// h2's flush stages locally; the queue lands asynchronously.
	if err := h2.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h1.Write(ctx, 0, []byte("C")); err != nil {
		t.Fatal(err)
	}
	if err := h1.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := b.Node("/f")
	if string(n.Data) != "ABC" {
		t.Fatalf("writeback-policy final image = %q, want ABC", string(n.Data))
	}
}

// A foreign commit (another mount's append) advancing the remote between
// this handle's write and its staging boundary is composed at the fence —
// the CAS conflict never publishes the older image over it.
func TestProbeAppendForeignCommitComposesAtStaging(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, _ := New(Options{Policy: PolicyCloseSync, Client: cl, Staging: mustStaging(t)})
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	// Seed the file so the handle opens over a real base.
	b.Seed("/f", []byte{})
	h1 := openAppend(t, ch, "/f")
	if err := h1.Write(ctx, 0, []byte("A")); err != nil {
		t.Fatal(err)
	}
	// A foreign writer appends and lands while h1 holds its buffer
	// (invisible to the local landed table — the client PUT bumps the
	// revision, so h1's staged CAS base is genuinely superseded).
	foreignTerm, _ := core.New(cl, core.Options{})
	foreignCh, _ := fs.NewChain(foreignTerm)
	if _, err := foreignCh.Dispatch(ctx, &fs.Op{Kind: fs.OpWrite, Path: "/f", Offset: 0, Data: []byte("X")}); err != nil {
		t.Fatal(err)
	}
	// A diverged foreign append (no shared ancestry with the local
	// records) is v1's retain-and-reject: writes/fsync may surface
	// zero-byte EAGAIN with the acknowledged data retained, and the
	// backend keeps the foreign record — the older local image is never
	// published over it.
	writeErr := h1.Write(ctx, 0, []byte("B"))
	syncErr := h1.Sync(ctx)
	_ = h1.Release(context.Background())
	_ = wb.Drain(context.Background())
	if writeErr != nil && writeErr != syscall.EAGAIN {
		t.Fatalf("write B: %v", writeErr)
	}
	if syncErr != nil && syncErr != syscall.EAGAIN {
		t.Fatalf("sync: %v", syncErr)
	}
	n, _ := b.Node("/f")
	got := string(n.Data)
	if !strings.Contains(got, "X") {
		t.Fatalf("foreign record X lost: final = %q (the older local image overwrote it)", got)
	}
}
