package specials

import (
	"context"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// term answers every op with ENOENT: specials are metadata-only, so the
// chain below only needs to report "nothing there". existing optionally
// names paths the chain reports as taken (regular files).
type term struct{ existing map[string]bool }

func (t term) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	switch op.Kind {
	case fs.OpList:
		return fs.Result{Entries: nil}, nil
	case fs.OpRename:
		return fs.Result{}, nil
	}
	if t.existing[op.Path] {
		return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Mode: 0o644}}, nil
	}
	return fs.Result{}, fs.ErrNotFound
}

func dispatch(ch *fs.Chain, op *fs.Op) error {
	_, err := ch.Dispatch(context.Background(), op)
	return err
}

func mkfifo(ch *fs.Chain, path string) error {
	return dispatch(ch, &fs.Op{Kind: fs.OpMknod, Path: path, Mode: uint32(syscall.S_IFIFO) | 0o644})
}

func getMode(ch *fs.Chain, path string) (uint32, error) {
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpGetAttr, Path: path})
	if err != nil {
		return 0, err
	}
	return res.Attr.Mode, nil
}

func newStack(t *testing.T, existing ...string) (*Specials, *fs.Chain) {
	t.Helper()
	s, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	taken := make(map[string]bool, len(existing))
	for _, p := range existing {
		taken[p] = true
	}
	ch, err := fs.NewChain(s, term{existing: taken})
	if err != nil {
		t.Fatal(err)
	}
	return s, ch
}

func TestMknodRegistersAndLookupSynthesizes(t *testing.T) {
	_, ch := newStack(t)
	if err := mkfifo(ch, "/d/fifo"); err != nil {
		t.Fatal(err)
	}
	mode, err := getMode(ch, "/d/fifo")
	if err != nil {
		t.Fatal(err)
	}
	if mode&syscall.S_IFMT != syscall.S_IFIFO {
		t.Fatalf("mode = %o, want FIFO type bits", mode)
	}
	// Re-creation over the record is EEXIST.
	if err := mkfifo(ch, "/d/fifo"); err != syscall.EEXIST {
		t.Fatalf("re-create = %v, want EEXIST", err)
	}
}

func TestMknodOverExistingChainEntryRefused(t *testing.T) {
	_, ch := newStack(t, "/d/taken")
	if err := mkfifo(ch, "/d/taken"); err != syscall.EEXIST {
		t.Fatalf("mknod over chain entry = %v, want EEXIST", err)
	}
}

func TestUnlinkAndRmdirDropRecords(t *testing.T) {
	_, ch := newStack(t)
	if err := mkfifo(ch, "/d/fifo"); err != nil {
		t.Fatal(err)
	}
	if err := dispatch(ch, &fs.Op{Kind: fs.OpUnlink, Path: "/d/fifo"}); err != nil {
		t.Fatal(err)
	}
	if _, err := getMode(ch, "/d/fifo"); err != fs.ErrNotFound {
		t.Fatalf("lookup after unlink = %v, want ErrNotFound", err)
	}
}

func TestRenameMovesSubtreeRecords(t *testing.T) {
	s, ch := newStack(t, "/old")
	if err := mkfifo(ch, "/old/fifo"); err != nil {
		t.Fatal(err)
	}
	if err := mkfifo(ch, "/old/sub/deep"); err != nil {
		t.Fatal(err)
	}
	if err := dispatch(ch, &fs.Op{Kind: fs.OpRename, Path: "/old", Path2: "/new"}); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"/new/fifo", "/new/sub/deep"} {
		if _, err := getMode(ch, p); err != nil {
			t.Fatalf("record lost on subtree rename: %s: %v", p, err)
		}
	}
	for _, p := range []string{"/old/fifo", "/old/sub/deep"} {
		if _, err := getMode(ch, p); err == nil {
			t.Fatalf("stale record survived rename: %s", p)
		}
	}
	if s.Count() != 2 {
		t.Fatalf("record count = %d, want 2", s.Count())
	}
}

func TestListMergesRecordsIntoListing(t *testing.T) {
	_, ch := newStack(t, "/d")
	if err := mkfifo(ch, "/d/fifo"); err != nil {
		t.Fatal(err)
	}
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpList, Path: "/d"})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range res.Entries {
		if e.Name == "fifo" {
			found = true
			if e.Attr.Mode&syscall.S_IFMT != syscall.S_IFIFO {
				t.Fatalf("merged entry mode = %o", e.Attr.Mode)
			}
		}
	}
	if !found {
		t.Fatalf("special missing from listing: %v", res.Entries)
	}
}

func TestLinkAddsAliasRecord(t *testing.T) {
	_, ch := newStack(t)
	if err := mkfifo(ch, "/d/fifo"); err != nil {
		t.Fatal(err)
	}
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpLink, Path: "/d/fifo", Path2: "/d/fifo2"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Attr == nil || res.Attr.Mode&syscall.S_IFMT != syscall.S_IFIFO {
		t.Fatalf("link reply attr = %+v", res.Attr)
	}
	if _, err := getMode(ch, "/d/fifo2"); err != nil {
		t.Fatalf("alias record missing: %v", err)
	}
	// Linking over an existing record is EEXIST.
	if err := dispatch(ch, &fs.Op{Kind: fs.OpLink, Path: "/d/fifo", Path2: "/d/fifo2"}); err != syscall.EEXIST {
		t.Fatalf("link over record = %v, want EEXIST", err)
	}
}

func TestSetAttrMaintainsRecordLocally(t *testing.T) {
	_, ch := newStack(t)
	if err := mkfifo(ch, "/d/fifo"); err != nil {
		t.Fatal(err)
	}
	// A metadata-only node has no bytes: size changes are EINVAL.
	if err := dispatch(ch, &fs.Op{Kind: fs.OpSetAttr, Path: "/d/fifo", Size: 4}); err != syscall.EINVAL {
		t.Fatalf("truncate special = %v, want EINVAL", err)
	}
	// Chmod rewrites the permission bits, keeping the type bits (Size -1:
	// no size change requested).
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Path: "/d/fifo", Mode: 0o600, Size: -1})
	if err != nil {
		t.Fatal(err)
	}
	if got := res.Attr.Mode; got&syscall.S_IFMT != syscall.S_IFIFO || got&0o777 != 0o600 {
		t.Fatalf("chmod mode = %o, want FIFO|0600", got)
	}
	// Timestamp changes are accepted as no-ops.
	if err := dispatch(ch, &fs.Op{Kind: fs.OpSetAttr, Path: "/d/fifo", Size: -1}); err != nil {
		t.Fatalf("no-op setattr = %v", err)
	}
}

func TestRmdirWithSubtreeRecordsRefused(t *testing.T) {
	_, ch := newStack(t, "/d")
	if err := mkfifo(ch, "/d/inner/fifo"); err != nil {
		t.Fatal(err)
	}
	if err := dispatch(ch, &fs.Op{Kind: fs.OpRmdir, Path: "/d/inner"}); err != syscall.ENOTEMPTY {
		t.Fatalf("rmdir over special child = %v, want ENOTEMPTY", err)
	}
}

func TestDeleteEventDropsRecords(t *testing.T) {
	bus := fs.NewEventBus()
	s, err := New(Options{Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(s, term{})
	if err != nil {
		t.Fatal(err)
	}
	if err := mkfifo(ch, "/d/fifo"); err != nil {
		t.Fatal(err)
	}
	bus.Emit(fs.Event{Kind: fs.EventDelete, Path: "/d/fifo"})
	if _, err := getMode(ch, "/d/fifo"); err != fs.ErrNotFound {
		t.Fatalf("record survived delete event: %v", err)
	}
}

func TestReexecRefusalReportsCount(t *testing.T) {
	s, ch := newStack(t)
	if reason, n := s.ReexecRefusal(); n != 0 || reason != "" {
		t.Fatalf("empty refusal = %q %d", reason, n)
	}
	if err := mkfifo(ch, "/d/fifo"); err != nil {
		t.Fatal(err)
	}
	if reason, n := s.ReexecRefusal(); n != 1 || reason == "" {
		t.Fatalf("refusal = %q %d", reason, n)
	}
}
