package appendlog

// qiffang twenty-first round: landed-generation dedup is scoped to one
// file incarnation; full rewrites keep their captured CAS base; sibling
// refresh never blocks on another handle.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// A whole-image rewrite must CAS against the handle's CAPTURED base: when a
// sibling append lands first, the stale composition fails instead of being
// republished at the sibling's revision (abcdef + local X + foreign TAIL
// must not become Xbcdef).
func TestPortRewriteConflictKeepsForeignAppend(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedWithLayout("/db", []byte("abcdef"), "append_log")
	ch := newStack(t, b, "**/db")
	ctx := context.Background()

	h := openLog(t, ch, "/db").(*Handle)
	sibling := openLog(t, ch, "/db").(*Handle)
	if err := h.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatal(err)
	}
	if err := sibling.Write(ctx, 6, []byte("TAIL")); err != nil {
		t.Fatal(err)
	}
	if err := sibling.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := sibling.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err == nil {
		t.Fatal("stale rewrite landed over the foreign append")
	}
	if node, _ := b.Node("/db"); string(node.Data) != "abcdefTAIL" {
		t.Fatalf("backend = %q, want the foreign append preserved", node.Data)
	}
	if !h.dirty {
		t.Fatal("failed sync must preserve the dirty generation")
	}
}

// unlink + recreate + one write: the new file's Sync commits (the
// recreation's local seq=1,size=3 must not match the deleted file's
// landed generation).
func TestPortRecreatedFileCommitsItsOwnGeneration(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newStack(t, b, "**/f.log")
	ctx := context.Background()

	open := func() *Handle {
		return openLog(t, ch, "/f.log").(*Handle)
	}
	h1 := open()
	if err := h1.Write(ctx, 0, []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := h1.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/f.log"}); err != nil {
		t.Fatal(err)
	}
	h2 := open()
	if err := h2.Write(ctx, 0, []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := h2.Sync(ctx); err != nil {
		t.Fatalf("recreated file sync: %v", err)
	}
	if err := h2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	node, ok := b.Node("/f.log")
	if !ok || len(node.Data) == 0 || strings.Contains(string(node.Data), "old") {
		t.Fatalf("remote after recreate = %q ok=%v, want new content", node.Data, ok)
	}
}

// Two O_APPEND handles refreshing concurrently never deadlock (the
// refresh path holds h.mu and must not block on the set mutex or another
// handle's mu).
func TestPortConcurrentAppendRefreshNoDeadlock(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newStack(t, b, "**/f.log")
	ctx := context.Background()

	open := func() *Handle {
		return openLog(t, ch, "/f.log").(*Handle)
	}
	h1, h2 := open(), open()
	for _, h := range []*Handle{h1, h2} {
		if err := h.Write(ctx, 0, []byte("seed")); err != nil {
			t.Fatal(err)
		}
	}
	// Hold each handle's mutex in turn while the other refreshes — with
	// the old lock order this formed the AB cycle; now the busy side
	// simply sits the refresh out.
	done := make(chan struct{})
	go func() {
		defer close(done)
		h1.mu.Lock()
		done2 := make(chan struct{})
		go func() { defer close(done2); h2.mu.Lock() }()
		<-done2
		_ = h1.refreshAppendBaselineLocked(ctx)
		h1.mu.Unlock()
		h2.mu.Unlock()
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh deadlocked on a sibling's held mutex")
	}
}
