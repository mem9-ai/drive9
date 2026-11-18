package extent

import (
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/client"
)

// A stat answered by a lagging replica must not become the floor that reads
// refuse to go below.
//
// The counter is the same one the freshness guard reads, so driving it through
// the real stat path (Bridge.GetAttr) is what proves the guard cannot be
// disarmed by an ordinary getattr — which every open and every kernel
// revalidation performs.
func TestGetAttrDoesNotLowerThePublishedRevision(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/d/a.db", "content") // the fake reports revision 3 here
	br := NewBridge(Config{Client: client.New(f.URL, ""), RemoteRoot: "/"})
	const ino = meta.Ino(61)

	// Pretend this mount published a newer revision than the server's answer.
	br.recordLandedRevision(ino, 12)
	br.remember(ino, "/d/a.db")

	var attr meta.Attr
	if errno := br.GetAttr(meta.NewContext(1, 0, []uint32{0}), ino, &attr); errno != 0 {
		t.Fatalf("GetAttr: %v", errno)
	}
	if got := br.publishedRevision(ino); got != 12 {
		t.Fatalf("GetAttr with a stale stat (3) lowered the floor to %d; it must stay 12", got)
	}
}

// The same through Resolve, the other stat-backed entry point the bridge
// records revisions from.
func TestResolveDoesNotLowerThePublishedRevision(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/d/a.db", "content")
	f.setDir("/d", true)
	br := NewBridge(Config{Client: client.New(f.URL, ""), RemoteRoot: "/"})
	ctx := meta.NewContext(1, 0, []uint32{0})

	// Resolve the parent, then pin a floor for the child's inode.
	var parentIno meta.Ino
	if errno := br.Resolve(ctx, rootIno, "d", &parentIno, nil, false); errno != 0 {
		t.Fatalf("resolve parent: %v", errno)
	}
	child := inoFor("/d/a.db")
	br.remember(child, "/d/a.db")
	br.recordLandedRevision(child, 12)

	var attr meta.Attr
	if errno := br.Resolve(ctx, parentIno, "a.db", nil, &attr, false); errno != 0 {
		t.Fatalf("resolve child: %v", errno)
	}
	if got := br.publishedRevision(child); got != 12 {
		t.Fatalf("Resolve with a stale stat lowered the floor to %d; it must stay 12", got)
	}
}
