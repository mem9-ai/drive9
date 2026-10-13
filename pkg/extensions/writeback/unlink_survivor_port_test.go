// v1 unlink-survivor suites (unlink_survivor_test.go), retargeted to the
// new writeback plane's survivor machinery: markUnlinked's shadow snapshot
// for clean read-only fds, per-handle anonymity with the buffer serving
// the fd, the hardlink-alias rebinding (retarget + SurvivorAlias at the
// facade are covered by pkg/fuse tests), DELETE-failure rollback
// (markLinked), and the last-close deferred delete.
package writeback

import (
	"context"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// v1 TestIssue986SurvivorBasic (reader/writer × clean/dirty): the unlinked
// fd keeps serving its image through the last close; the deferred delete
// fires once every handle is gone; nothing resurrects the name.
func TestPortSurvivorBasic(t *testing.T) {
	for _, tc := range []struct {
		name  string
		dirty bool
	}{
		{"clean", false},
		{"dirty", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := testbackend.NewBackend()
			b.Seed("/surv.txt", []byte("hello"))
			ch, wb := newStagedStack(t, PolicyWriteBack, b)
			ctx := context.Background()

			h := open(t, ch, "/surv.txt").(*bufferingHandle)
			want := "hello"
			if tc.dirty {
				if err := h.Write(ctx, 5, []byte(" gopher")); err != nil {
					t.Fatal(err)
				}
				want = "hello gopher"
			}
			if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/surv.txt"}); err != nil {
				t.Fatalf("unlink: %v", err)
			}
			h.mu.Lock()
			unlinked := h.unlinked
			h.mu.Unlock()
			if !unlinked {
				t.Fatal("handle must be anonymous after unlink")
			}
			// The fd keeps reading its image (dirty buffer, or the shadow
			// snapshot staged by markUnlinked).
			buf := make([]byte, len(want))
			n, err := h.Read(ctx, 0, buf)
			if err != nil || string(buf[:n]) != want {
				t.Fatalf("post-unlink read = %q n=%d err=%v, want %q", buf[:n], n, err, want)
			}
			// Release drops the handle; the deferred delete lands.
			if err := h.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if err := wb.Drain(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, ok := b.Node("/surv.txt"); ok {
				t.Fatal("deferred delete must remove the remote entry at last close")
			}
		})
	}
}

// v1 TestIssue986SurvivorWarmReadOnly: a clean read-only fd of a larger
// base still serves post-unlink reads through the shadow snapshot (the
// snapshot covers bases up to unlinkedSnapshotMaxBytes).
func TestPortSurvivorWarmReadOnly(t *testing.T) {
	b := testbackend.NewBackend()
	seed := strings.Repeat("warm", 2048) // 8 KiB
	b.Seed("/warm.txt", []byte(seed))
	ch, wb := newStagedStack(t, PolicyWriteBack, b)
	ctx := context.Background()

	h := open(t, ch, "/warm.txt").(*bufferingHandle)
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/warm.txt"}); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len(seed))
	n, err := h.Read(ctx, 0, buf)
	if err != nil || n != len(seed) || string(buf) != seed {
		t.Fatalf("warm read = n=%d err=%v", n, err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	_ = wb.Drain(context.Background())
	if _, ok := b.Node("/warm.txt"); ok {
		t.Fatal("deferred delete missing")
	}
}

// v1 TestIssue986SurvivorWriteDuringSettle: a write on the anonymous fd
// keeps serving through Release; the write is discarded (the name is
// deleted — the buffer never publishes).
func TestPortSurvivorWriteDuringSettle(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/settle.txt", []byte("base"))
	ch, wb := newStagedStack(t, PolicyWriteBack, b)
	ctx := context.Background()

	h := open(t, ch, "/settle.txt").(*bufferingHandle)
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/settle.txt"}); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(ctx, 4, []byte("+late")); err != nil {
		t.Fatalf("post-unlink write: %v", err)
	}
	buf := make([]byte, 9)
	if n, err := h.Read(ctx, 0, buf); err != nil || string(buf[:n]) != "base+late" {
		t.Fatalf("read = %q err=%v", buf[:n], err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	_ = wb.Drain(context.Background())
	// The remote never sees the anonymous write.
	if _, ok := bNodeString(b, "/settle.txt"); ok {
		t.Fatal("anonymous fd must not publish after unlink")
	}
}

// v1 TestIssue986SurvivorPreflightNoMutation: a failed unlink (server
// rejects the deferred delete at the last close) leaves the remote entry
// intact — visible via the still-open fd's release path warning, and the
// node remains queryable.
func TestPortSurvivorDeleteFailureRollsBackHandles(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/fail.txt", []byte("keep"))
	ch, wb := newStagedStack(t, PolicyWriteBack, b)
	ctx := context.Background()

	h := open(t, ch, "/fail.txt").(*bufferingHandle)
	if err := h.Write(ctx, 0, []byte("KEEP")); err != nil {
		t.Fatal(err)
	}
	// Dirty unlink path: the facade defers the delete; simulate the
	// delete-failure rollback at the writeback level directly (v1
	// unmarkOpenHandlesAfterFailedUnlink → markLinked).
	wb.markUnlinked("/fail.txt", false)
	if err := h.Write(ctx, 4, []byte("!")); err != nil {
		t.Fatal(err)
	}
	// Rollback: the handles become linkable again and their buffers
	// commit normally.
	wb.markLinked("/fail.txt", false)
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !wb.waitPathCommit("/fail.txt") {
		t.Fatal("rolled-back commit did not land")
	}
	if got, _ := bNodeString(b, "/fail.txt"); got != "KEEP!" {
		t.Fatalf("backend = %q, want KEEP! (rolled-back handle publishes)", got)
	}
}

func bNodeString(b *testbackend.Backend, path string) (string, bool) {
	n, ok := b.Node(path)
	if !ok {
		return "", false
	}
	return string(n.Data), true
}

// v1 TestIssue986SurvivorLateQueuedPublication /
// SurvivorCommitAfterStat: with a hardlink survivor the surviving alias
// publishes the buffered image after the unlink (the retarget rebinds the
// writer to the surviving name). In the new plane the facade's
// SurvivorAlias + writeback's retarget cover the rebind; this row pins
// the writeback half: a renamed (retargeted) dirty handle publishes under
// the NEW path only.
func TestPortSurvivorRetargetedHandlePublishesUnderNewPath(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/old.txt", []byte("image"))
	ch, wb := newStagedStack(t, PolicyWriteBack, b)
	ctx := context.Background()

	h := open(t, ch, "/old.txt").(*bufferingHandle)
	// Simulate the rename/survivor retarget: the handle's path moves.
	h.mu.Lock()
	h.path = "/new.txt"
	h.mu.Unlock()
	if err := h.Write(ctx, 5, []byte("-moved")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	_ = wb.Drain(context.Background())
	if got, ok := bNodeString(b, "/new.txt"); !ok || got != "image-moved" {
		t.Fatalf("new path = %q ok=%v, want image-moved", got, ok)
	}
}
