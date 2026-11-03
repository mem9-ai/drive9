package extent

import (
	"context"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
)

// Round-14 P1: a resolution whose stat straddles the retention boundary
// must not drag the retained mapping back onto the replaceable name. The
// resolution selects B's inode, pauses in its stat while RetainOpenAlias
// installs the alias, and only then completes: remember() must keep the
// alias mapping, so the descriptor it returns still targets B's retained
// object after the source is renamed onto the name.
func TestResolutionAcrossRetentionKeepsMapping(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/b", "BBBBBBBB")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	orig, errno := b.ResolveLocal(context.Background(), "/b")
	if errno != 0 {
		t.Fatal(errno)
	}

	gate := make(chan struct{})
	f.gateHead("/b", gate)
	resolved := make(chan meta.Ino, 1)
	go func() {
		ino, errno := b.ResolveLocal(context.Background(), "/b")
		if errno != 0 {
			ino = 0
		}
		resolved <- ino
	}()
	deadline := time.Now().Add(10 * time.Second)
	for f.gateWaiting("/b") == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if f.gateWaiting("/b") == 0 {
		t.Fatal("the resolution never reached its stat")
	}

	// Retention runs while the resolution's stat is in flight; its own
	// stat passes (the gate is one-shot).
	sentinel, err := b.RetainOpenAlias("/b")
	if err != nil {
		t.Fatal(err)
	}
	if sentinel == "" {
		t.Fatal("retention declined a present file")
	}
	alias := b.fullPath(sentinel)
	close(gate)

	select {
	case ino := <-resolved:
		if ino != orig {
			t.Fatalf("straddling resolution returned inode %d, want B's %d", ino, orig)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the straddling resolution never completed")
	}
	b.mu.Lock()
	pathB, repB := b.paths[orig], b.replacements["/b"]
	b.mu.Unlock()
	if pathB != alias {
		t.Fatalf("the straddling resolution dragged the mapping back: paths[B]=%q, want the alias %q", pathB, alias)
	}
	if repB != 0 {
		t.Fatalf("the name still resolves to the retained inode: replacements[/b]=%d", repB)
	}
}
