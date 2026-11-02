// v1 rename-pending-new rows (dat9fs_test.go's vim-:w family), retargeted
// to the writeback extension's OpRename: a never-landed lazy create moves
// locally (no remote round trip, the buffer follows the new name and
// lands there), a landed writer flushes-then-renames (no resurrection of
// the old name), and staged descendants under a renamed directory
// migrate to the new prefix.
package writeback

import (
	"context"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// v1 TestRenamePendingNewMovesLocally (fast path): an unflushed create
// renamed before any commit never touches the remote for the OLD name;
// the image lands under the new name at the close boundary.
func TestPortRenamePendingNewMovesLocally(t *testing.T) {
	b := testbackend.NewBackend()
	ch, wb := newStagedStack(t, PolicyWriteBack, b)
	ctx := context.Background()

	h := open(t, ch, "/vim.txt").(*bufferingHandle)
	if err := h.Write(ctx, 0, []byte("vim swap")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/vim.txt", Path2: "/vim.txt~"}); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if _, ok := b.Node("/vim.txt"); ok {
		t.Fatal("old name must never exist remotely (local move only)")
	}
	h.mu.Lock()
	path := h.path
	h.mu.Unlock()
	if path != "/vim.txt~" {
		t.Fatalf("handle path = %q, want the retargeted name", path)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, ok := nodeString(b, "/vim.txt~"); !ok || got != "vim swap" {
		t.Fatalf("new name = %q ok=%v, want the moved image", got, ok)
	}
}

// v1 TestRenameLandedWriterFlushesThenRenames: a writer over an existing
// remote file flushes its buffer and waits the landing BEFORE the remote
// rename — the old name is gone afterwards and no queued commit
// resurrects it.
func TestPortRenameLandedWriterFlushesThenRenames(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/doc.md", []byte("old image"))
	ch, wb := newStagedStack(t, PolicyWriteBack, b)
	ctx := context.Background()

	h := open(t, ch, "/doc.md").(*bufferingHandle)
	if err := h.Write(ctx, 0, []byte("NEW IMAGE")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/doc.md", Path2: "/doc.new.md"}); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := b.Node("/doc.md"); ok {
		t.Fatal("old name resurrected after the rename")
	}
	if got, ok := nodeString(b, "/doc.new.md"); !ok || got != "NEW IMAGE" {
		t.Fatalf("new name = %q ok=%v, want the flushed image", got, ok)
	}
}

// v1 TestRenameDirectoryMigratesStagedDescendants: staged .dat/.meta
// entries under a renamed directory re-key to the new prefix, so their
// uploads land under the new tree.
func TestPortRenameDirectoryMigratesStagedDescendants(t *testing.T) {
	b := testbackend.NewBackend()
	ch, wb, _ := newWriteCacheStack(t, PolicyWriteBack, b)
	ctx := context.Background()

	// The directory must exist remotely for the rename: create it through
	// the chain's mkdir first (the new plane materializes dirs eagerly).
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpMkdir, Path: "/tree", Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	// Create and stage a file under /tree.
	h := open(t, ch, "/tree/inner.txt").(*bufferingHandle)
	if err := h.Write(ctx, 0, []byte("inner data")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// Rename the directory.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/tree", Path2: "/shrub"}); err != nil {
		t.Fatalf("rename dir: %v", err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, ok := nodeString(b, "/shrub/inner.txt"); !ok || got != "inner data" {
		t.Fatalf("migrated child = %q ok=%v, want inner data under the new prefix", got, ok)
	}
}

func nodeString(b *testbackend.Backend, path string) (string, bool) {
	n, ok := b.Node(path)
	if !ok {
		return "", false
	}
	return string(n.Data), true
}

// v1 TestRenameMissingSourceSurfacesNotFound: renaming a path with no
// remote entry and no live writer is ENOENT.
func TestPortRenameMissingSourceENOENT(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newStagedStack(t, PolicyWriteBack, b)
	_, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpRename, Path: "/ghost.txt", Path2: "/ghost2.txt"})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("err = %v, want ENOENT", err)
	}
}
