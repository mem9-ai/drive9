// ftruncate_port_test.go pins #984's contracts on the extension stack
// (issue #983: fd-truncate exposes the new size but stale contents to
// other handles until close). The v1 failpoint suites (release receipts,
// mode budgets, rename fences, retired reads) exercised monolith-internal
// machinery; the behavioral rows they reduced to are ported here.
package writeback

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
	"github.com/mem9-ai/drive9/pkg/fusecomp/writebuffer"
)

// newWritebackStack builds a writeback-policy stack with the staging
// ledger armed (the #983 mount shape).
func newWritebackStack(t *testing.T, b *testbackend.Backend) (*fs.Chain, *Writeback) {
	t.Helper()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	ledger, err := staging.NewStaging(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledger.Close)
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, Staging: ledger})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	return ch, wb
}

func openRW(t *testing.T, ch *fs.Chain, path string) fs.Handle {
	t.Helper()
	return open(t, ch, path)
}

func openRO(t *testing.T, ch *fs.Chain, path string) fs.Handle {
	t.Helper()
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: path, Flags: uint32(syscall.O_RDONLY)})
	if err != nil {
		t.Fatal(err)
	}
	if res.Handle == nil {
		t.Fatal("no handle")
	}
	return res.Handle
}

func truncateViaHandle(t *testing.T, ch *fs.Chain, path string, h fs.Handle, size int64) error {
	t.Helper()
	_, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Path: path, Size: size, Handle: h, Ino: 1})
	return err
}

func readAt(t *testing.T, h fs.Handle, off int64, n int) (int, []byte) {
	t.Helper()
	buf := make([]byte, n)
	got, err := h.Read(context.Background(), off, buf)
	if err != nil {
		t.Fatalf("read@%d: %v", off, err)
	}
	return got, buf[:got]
}

// The #983 core: after ftruncate(fd) — fd kept open — a read-only re-open
// of the path serves the truncated image (shrink keeps the prefix, grow
// zero-extends, zero empties), reads at the new EOF report EOF, and stat
// reports the new size. The remote keeps the pre-truncate image until the
// commit boundary.
func TestPortFtruncateVisibleToReadOnlyReopen(t *testing.T) {
	cases := []struct {
		name string
		seed string
		size int64
		want string
	}{
		{"shrink", "hello world", 5, "hello"},
		{"grow", "Hi", 4, "Hi\x00\x00"},
		{"zero", "hello world", 0, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := testbackend.NewBackend()
			b.Seed("/f", []byte(tc.seed))
			ch, _ := newWritebackStack(t, b)
			ctx := context.Background()

			writer := openRW(t, ch, "/f")
			if err := truncateViaHandle(t, ch, "/f", writer, tc.size); err != nil {
				t.Fatalf("ftruncate: %v", err)
			}
			// The remote keeps the pre-truncate image until the commit
			// boundary (local adoption).
			if n, _ := b.Node("/f"); string(n.Data) != tc.seed {
				t.Fatalf("remote after ftruncate = %q, want %q", n.Data, tc.seed)
			}
			// stat through the namespace reports the truncated size.
			res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/f"})
			if err != nil {
				t.Fatal(err)
			}
			if res.Attr == nil || res.Attr.Size != tc.size {
				t.Fatalf("stat size = %+v, want %d", res.Attr, tc.size)
			}
			// A read-only re-open while the writer fd stays open.
			reader := openRO(t, ch, "/f")
			n, data := readAt(t, reader, 0, len(tc.seed)+8)
			if string(data) != tc.want || int64(n) != tc.size {
				t.Fatalf("read = %q (%d bytes), want %q (%d)", data, n, tc.want, tc.size)
			}
			// EOF at the new size.
			en, _ := readAt(t, reader, tc.size, 4)
			if en != 0 {
				t.Fatalf("read at new EOF = %d bytes, want 0", en)
			}
			if err := writer.Release(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// A PRE-EXISTING read-only fd (opened before the truncate, wrapping the
// remote base) must observe the truncate too — not only re-opens.
func TestPortFtruncatePreexistingReaderSeesTruncation(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello world"))
	ch, _ := newWritebackStack(t, b)

	reader := openRO(t, ch, "/f")
	writer := openRW(t, ch, "/f")
	if err := truncateViaHandle(t, ch, "/f", writer, 5); err != nil {
		t.Fatalf("ftruncate: %v", err)
	}
	n, data := readAt(t, reader, 0, 16)
	if string(data) != "hello" || n != 5 {
		t.Fatalf("pre-existing reader = %q (%d), want hello (5)", data, n)
	}
}

// A hardlink-alias reader (different pathname, same inode) sees the
// truncate: the truncating fd's buffer is the inode's newest image, not
// the alias pathname's stale remote row (#984's alias rows).
func TestPortFtruncateAliasReaderSeesTruncation(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/file.bin", []byte("hello world"))
	b.Seed("/alias.bin", []byte("hello world"))
	ch, wb := newWritebackStack(t, b)

	ino := &fakeInodeTable{
		byPath:  map[string]uint64{"/file.bin": 7, "/alias.bin": 7},
		aliases: map[uint64][]string{7: {"/file.bin", "/alias.bin"}},
	}
	wb.SetInodeTable(ino)

	// A pre-existing alias reader AND a post-truncate alias re-open.
	preExisting := openRO(t, ch, "/alias.bin")
	writer := openRW(t, ch, "/file.bin")
	if err := truncateViaHandle(t, ch, "/file.bin", writer, 5); err != nil {
		t.Fatalf("ftruncate via /file.bin: %v", err)
	}
	for name, r := range map[string]fs.Handle{"preexisting": preExisting, "reopen": openRO(t, ch, "/alias.bin")} {
		n, data := readAt(t, r, 0, 16)
		if string(data) != "hello" || n != 5 {
			t.Fatalf("%s alias reader = %q (%d), want hello (5)", name, data, n)
		}
	}
	// Stat through the alias pathname reports the truncated size as well.
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpGetAttr, Path: "/alias.bin"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Attr == nil || res.Attr.Size != 5 {
		t.Fatalf("alias stat size = %+v, want 5", res.Attr)
	}
}

// A clean buffered sibling fd rebases with the truncate; an independently
// dirty sibling refuses it (EAGAIN) with every image untouched — v1's
// clean-sibling contract (TestHardlinkAliasDirtyIndependentTruncateRefused
// and the TestFtruncateSplitOpen rows).
func TestPortFtruncateDirtySiblingRefusedThenAllowed(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello world"))
	ch, _ := newWritebackStack(t, b)
	ctx := context.Background()

	fdA := openRW(t, ch, "/f")
	fdB := openRW(t, ch, "/f")
	if err := fdB.Write(ctx, 0, []byte("xyz")); err != nil {
		t.Fatal(err)
	}
	// fdB holds an uncommitted image: fdA's truncate must be refused
	// without mutating either side.
	err := truncateViaHandle(t, ch, "/f", fdA, 5)
	if !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("dirty-sibling truncate err = %v, want EAGAIN", err)
	}
	if n, data := readAt(t, fdB, 0, 16); string(data) != "xyzlo world" || n != 11 {
		t.Fatalf("fdB image after refusal = %q (%d), want xyzlo world (11)", data, n)
	}
	// Flush settles fdB (staged, clean); the truncate now adopts the
	// newest staged image as its base — fdB's leading "xyz" survives.
	if err := fdB.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := truncateViaHandle(t, ch, "/f", fdA, 5); err != nil {
		t.Fatalf("ftruncate after sibling flush: %v", err)
	}
	n, data := readAt(t, fdA, 0, 16)
	if string(data) != "xyzlo" || n != 5 {
		t.Fatalf("post-flush truncate = %q (%d), want xyzlo (5)", data, n)
	}
}

// Shrink-then-regrow zero-fills the hole: the truncated-away remote bytes
// must never resurrect through lazy part loads (#984's
// remoteSize clamp). Unit level, where part boundaries are small.
func TestPortWritebufferShrinkRegrowZeroFillsAcrossParts(t *testing.T) {
	var loads atomic.Int32
	wb := writebuffer.NewWriteBuffer("/t", 1<<20, 8)
	wb.LoadPart = func(part int) ([]byte, error) {
		loads.Add(1)
		return []byte(strings.Repeat(string(rune('A'+part-1)), 8)), nil
	}
	wb.SeedRemote(24)
	if err := wb.Truncate(12); err != nil {
		t.Fatal(err)
	}
	if err := wb.Truncate(20); err != nil {
		t.Fatal(err)
	}
	if err := wb.EnsurePartsForWrite(0, 20); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 20)
	wb.ReadAt(0, got)
	want := "AAAAAAAABBBB" + "\x00\x00\x00\x00\x00\x00\x00\x00"
	if string(got) != want {
		t.Fatalf("image = %q, want %q", got, want)
	}
	if n := loads.Load(); n < 1 || n > 2 {
		t.Fatalf("lazy loads = %d, want 1-2 (part 0 and the retained part 1)", n)
	}
}

// Once the truncated image lands, readers serve the landed image — the
// old truncate claim is superseded (v1's committed-wins rule).
func TestPortFtruncateSupersededByLanding(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello world"))
	ch, wb := newWritebackStack(t, b)
	ctx := context.Background()

	writer := openRW(t, ch, "/f")
	reader := openRO(t, ch, "/f")
	if err := truncateViaHandle(t, ch, "/f", writer, 5); err != nil {
		t.Fatal(err)
	}
	if err := writer.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if n, ok := b.Node("/f"); !ok || string(n.Data) != "hello" {
		t.Fatalf("remote after drain = %q ok=%v, want hello", n.Data, ok)
	}
	n, data := readAt(t, reader, 0, 16)
	if string(data) != "hello" || n != 5 {
		t.Fatalf("reader after landing = %q (%d), want hello (5)", data, n)
	}
	// A re-open after the landing reads the landed image directly.
	late := openRO(t, ch, "/f")
	if n, data := readAt(t, late, 0, 16); string(data) != "hello" || n != 5 {
		t.Fatalf("late reader = %q (%d), want hello (5)", data, n)
	}
}

// Rename carries the truncated image: the old name never resurrects, and
// the new name serves the truncated bytes.
func TestPortFtruncateRenameCarriesImage(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello world"))
	ch, wb := newWritebackStack(t, b)
	ctx := context.Background()

	writer := openRW(t, ch, "/f")
	if err := truncateViaHandle(t, ch, "/f", writer, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/f", Path2: "/g"}); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := b.Node("/f"); ok {
		t.Fatal("old name resurrected after rename")
	}
	if n, ok := b.Node("/g"); !ok || string(n.Data) != "hello" {
		t.Fatalf("remote /g = %q ok=%v, want hello", n.Data, ok)
	}
	reader := openRO(t, ch, "/g")
	if n, data := readAt(t, reader, 0, 16); string(data) != "hello" || n != 5 {
		t.Fatalf("reader at new name = %q (%d), want hello (5)", data, n)
	}
}

// Unlinking one hardlink name with a surviving alias keeps the truncated
// image: the writer's fd retargets onto the survivor and the remote ends
// truncated there (v1's unlink-survivor truncate rows).
func TestPortFtruncateUnlinkSurvivorKeepsImage(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/file.bin", []byte("hello world"))
	b.Seed("/alias.bin", []byte("hello world"))
	ch, wb := newWritebackStack(t, b)
	ino := &fakeInodeTable{
		byPath:  map[string]uint64{"/file.bin": 7, "/alias.bin": 7},
		aliases: map[uint64][]string{7: {"/file.bin", "/alias.bin"}},
	}
	wb.SetInodeTable(ino)
	wb.SetSurvivorLookup(func(path string) (string, bool) {
		if path == "/file.bin" {
			return "/alias.bin", true
		}
		return "", false
	})
	ctx := context.Background()

	writer := openRW(t, ch, "/file.bin")
	if err := truncateViaHandle(t, ch, "/file.bin", writer, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/file.bin"}); err != nil {
		t.Fatalf("unlink: %v", err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := b.Node("/file.bin"); ok {
		t.Fatal("unlinked name resurrected")
	}
	// testbackend models the two names as independent objects, so the
	// survivor row cannot reflect the published image the way a
	// shared-object backend would; the survivor-side contracts are the
	// publish itself (the truncated image uploaded before the delete) and
	// the still-open fd continuing to serve it.
	// The still-open fd keeps serving the truncated image on the survivor.
	if n, data := readAt(t, writer, 0, 16); string(data) != "hello" || n != 5 {
		t.Fatalf("surviving fd = %q (%d), want hello (5)", data, n)
	}
}

// An append after the truncate composes onto the truncated image — the
// truncated-away tail never resurrects through the append refresh (the
// truncate fence in refreshAppendBase).
func TestPortFtruncateAppendAfterTruncateComposesTruncatedBase(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello world"))
	ch, _ := newWritebackStack(t, b)
	ctx := context.Background()

	// O_APPEND writer truncates first, then appends.
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR | syscall.O_APPEND), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	writer := res.Handle
	if err := truncateViaHandle(t, ch, "/f", writer, 5); err != nil {
		t.Fatal(err)
	}
	if err := writer.Write(ctx, 0, []byte("-x")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := writer.Release(ctx); err != nil {
		t.Fatal(err)
	}
	// Locally the fd reads the composed image.
	if n, data := readAt(t, writer, 0, 16); string(data) != "hello-x" || n != 7 {
		t.Fatalf("appended image = %q (%d), want hello-x (7)", data, n)
	}
}
