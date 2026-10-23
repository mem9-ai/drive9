package layer

// qiffang fifteenth round: survivor promotion only after an own-slot CAS;
// landing identity is the server entry sequence; unlink anonymizes every
// descriptor and rolls back on whiteout failure; a commit waits out the
// mutation lock and re-verifies its path.

import (
	"context"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// Releasing the OLDEST of three same-path fds must not re-select an older
// handle over the newest owner: the compare-and-delete fails (the slot
// belongs to the newest claim) and no promotion runs. Releasing the OWNER
// promotes the surviving middle claim.
func TestPortDetachKeepsNewerClaimOwner(t *testing.T) {
	l, ch, _ := pStack(t)
	ctx := context.Background()

	open := func() *layerHandle {
		res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
		if err != nil {
			t.Fatal(err)
		}
		return res.Handle.(*layerHandle)
	}
	o1, o2, o3 := open(), open(), open()

	// Releasing the oldest leaves the newest owner untouched.
	l.untrackHandle(o1)
	if cur, ok := l.live.Load("/f"); !ok || cur != o3 {
		t.Fatalf("live[/f] = %v ok=%v after releasing the oldest, want the newest owner", cur, ok)
	}
	// Releasing the owner promotes the surviving middle claim.
	l.untrackHandle(o3)
	if cur, ok := l.live.Load("/f"); !ok || cur != o2 {
		t.Fatalf("live[/f] = %v ok=%v after releasing the owner, want the surviving claim", cur, ok)
	}
	// Releasing the last same-path claim empties the slot.
	l.untrackHandle(o2)
	if cur, ok := l.live.Load("/f"); ok {
		t.Fatalf("live[/f] = %v after releasing every claim, want empty", cur)
	}
}

// A foreign entry with byte-identical CONTENT still fences: the identity
// is the server entry sequence, so a chmod-only or same-content rewrite
// by another client carries a newer sequence and conflicts the dirty
// buffer (the content-hash comparison could not tell them apart).
func TestPortForeignSameContentStillFences(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("seed"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	l, ch := mStack(t, ts)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if err := h.Write(ctx, 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(ctx, 0, []byte("BBBB")); err != nil {
		t.Fatal(err)
	}
	// Foreign chmod-style upsert: identical bytes, different mode.
	postLayerEntry(t, ts, "L1", "/f", "upsert", "AAAA")
	l.refreshEventsForTest(ctx)
	if err := h.Write(ctx, 4, []byte("CC")); err == nil {
		t.Fatal("write over a foreign same-content entry succeeded, want EAGAIN fence")
	}
}

// Unlink anonymizes EVERY matching descriptor: an older sibling's sync
// after the whiteout must not upsert the deleted name back.
func TestPortUnlinkAnonymizesEveryDescriptor(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	_, ch := mStack(t, ts)
	ctx := context.Background()

	open := func() fs.Handle {
		res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
		if err != nil {
			t.Fatal(err)
		}
		return res.Handle
	}
	first, second := open(), open()
	if err := first.Write(ctx, 0, []byte("A")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/f"}); err != nil {
		t.Fatal(err)
	}
	// The older descriptor's sync must not resurrect /f.
	if err := first.Sync(ctx); err != nil {
		t.Fatalf("sync of anonymized descriptor: %v", err)
	}
	if err := first.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := second.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/f"}); err == nil {
		t.Fatal("unlinked name resurrected by the older descriptor's sync")
	}
}

// A FAILED whiteout rolls the anonymization back: the name still exists
// server-side, and every marked fd keeps its commits bound to it.
func TestPortFailedUnlinkRollsBackAnonymization(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello"))
	b.ForceLayerEntryFailure("/f", 1)
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	_, ch := mStack(t, ts)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/f"}); err == nil {
		t.Fatal("forced-failure whiteout reported success")
	}
	// The fd is NOT anonymous: its later write and sync still land.
	if err := h.Write(ctx, 0, []byte("J")); err != nil {
		t.Fatalf("write after failed unlink: %v", err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatalf("sync after failed unlink: %v", err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	rd, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/f", Offset: 0, Limit: 8})
	if err != nil || string(rd.Data) != "Jello" {
		t.Fatalf("/f after failed unlink + sync = %q err=%v, want Jello", rd.Data, err)
	}
}

// A commit that waits out the mutation lock must re-verify its path: a
// rename completing the move while the sync waited would otherwise
// publish the captured image under the renamed-away name.
func TestPortSyncRacingRenameDoesNotResurrectOldName(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/old", []byte("hello"))
	b.DelayLayerEntriesForTest(150 * time.Millisecond)
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	_, ch := mStack(t, ts)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/old", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if err := h.Write(ctx, 0, []byte("A")); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	renameErr := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/old", Path2: "/new"})
		renameErr <- err
	}()
	// Let the rename take the mutation locks and park in its (delayed)
	// entry POST before the sync captures its path.
	time.Sleep(50 * time.Millisecond)
	syncErr := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		syncErr <- h.Sync(ctx)
	}()
	if err := <-renameErr; err != nil {
		t.Fatalf("rename: %v", err)
	}
	<-syncErr // EAGAIN (moved) or a landed-at-new-path success — both safe
	wg.Wait()
	// The retry lands at the new name; the old name stays gone.
	if err := h.Sync(ctx); err != nil {
		t.Fatalf("retry sync: %v", err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	rd, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/new", Offset: 0, Limit: 8})
	if err != nil || string(rd.Data) != "Aello" {
		t.Fatalf("/new = %q err=%v, want Aello", rd.Data, err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/old"}); err == nil {
		t.Fatal("renamed-away name resurrected by the racing sync")
	}
}
