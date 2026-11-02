package appendlog

// Review round Q ports: a chmod on a live append handle is effective
// locally at once (SETATTR reply, later stats, the open-permission gate)
// even though its remote publication rides the next commit; and a newer
// chmod on any fd of the object retires the older pending chmods of its
// siblings, so a later fsync/close of an old fd never republishes a mode
// the caller already replaced.

import (
	"context"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func chmodHandle(t *testing.T, ch *fs.Chain, path string, h fs.Handle, mode uint32) fs.Result {
	t.Helper()
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Path: path, Handle: h, Mode: mode, HasMode: true, Size: -1})
	if err != nil {
		t.Fatalf("fchmod(%#o) = %v", mode, err)
	}
	return res
}

func statMode(t *testing.T, ch *fs.Chain, kind fs.OpKind, path string) uint32 {
	t.Helper()
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: kind, Path: path})
	if err != nil || res.Attr == nil {
		t.Fatalf("%s %s = %+v err=%v", kind, path, res.Attr, err)
	}
	return res.Attr.Mode & 0o7777
}

// fchmod(fd, 0000) on a live handle with uncommitted writes: the SETATTR
// reply and every later stat report 0000 immediately (the facade must not
// fall back to the lower plane's 0644), and the commit publishes it.
func TestPortPendingChmodVisibleImmediately(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("abcdef"))
	ch := newStack(t, b, "**/*.log")
	ctx := context.Background()

	h := openLog(t, ch, "/app.log")
	if err := h.Write(ctx, 6, []byte("!")); err != nil {
		t.Fatal(err)
	}
	res := chmodHandle(t, ch, "/app.log", h, 0)
	if res.Attr == nil || res.Attr.Mode&0o7777 != 0 {
		t.Fatalf("fchmod reply attr = %+v, want mode 0000", res.Attr)
	}
	if res.Attr.Size != 7 {
		t.Fatalf("fchmod reply size = %d, want the pending 7", res.Attr.Size)
	}
	if got := statMode(t, ch, fs.OpGetAttr, "/app.log"); got != 0 {
		t.Fatalf("getattr mode after fchmod = %#o, want 0000", got)
	}
	if got := statMode(t, ch, fs.OpLookup, "/app.log"); got != 0 {
		t.Fatalf("lookup mode after fchmod = %#o, want 0000", got)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatalf("fsync = %v", err)
	}
	node, _ := b.Node("/app.log")
	if node.Mode&0o777 != 0 || string(node.Data) != "abcdef!" {
		t.Fatalf("backend after fsync = mode %#o data %q, want 0000 / abcdef!", node.Mode&0o777, node.Data)
	}
	if got := statMode(t, ch, fs.OpGetAttr, "/app.log"); got != 0 {
		t.Fatalf("getattr mode after landing = %#o, want 0000", got)
	}
}

// Two fds of one object: the newer chmod wins and retires the older fd's
// pending chmod — syncing the old fd afterwards must not restore 0644.
func TestPortNewerChmodRetiresSiblingPendingMode(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("abcdef"))
	ch := newStack(t, b, "**/*.log")
	ctx := context.Background()

	h1 := openLog(t, ch, "/app.log")
	h2 := openLog(t, ch, "/app.log")
	if err := h1.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatal(err)
	}
	if err := h1.Sync(ctx); err != nil {
		t.Fatalf("h1 fsync = %v", err)
	}
	chmodHandle(t, ch, "/app.log", h1, 0o644)
	chmodHandle(t, ch, "/app.log", h2, 0)
	if got := statMode(t, ch, fs.OpGetAttr, "/app.log"); got != 0 {
		t.Fatalf("getattr after the newer chmod = %#o, want 0000", got)
	}
	if err := h2.Sync(ctx); err != nil {
		t.Fatalf("h2 fsync = %v", err)
	}
	if node, _ := b.Node("/app.log"); node.Mode&0o777 != 0 {
		t.Fatalf("backend mode after h2 fsync = %#o, want 0000", node.Mode&0o777)
	}
	if err := h1.Sync(ctx); err != nil {
		t.Fatalf("h1 fsync = %v", err)
	}
	if node, _ := b.Node("/app.log"); node.Mode&0o777 != 0 {
		t.Fatalf("backend mode after the OLD fd's fsync = %#o, want 0000 (its stale pending 0644 was republished)", node.Mode&0o777)
	}
	if got := statMode(t, ch, fs.OpGetAttr, "/app.log"); got != 0 {
		t.Fatalf("getattr after both fsyncs = %#o, want 0000", got)
	}
	// The retirement is per object: the reverse order keeps the newest too.
	chmodHandle(t, ch, "/app.log", h2, 0)
	chmodHandle(t, ch, "/app.log", h1, 0o600)
	if err := h2.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h1.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if node, _ := b.Node("/app.log"); node.Mode&0o777 != 0o600 {
		t.Fatalf("backend mode after reversed chmods = %#o, want 0600", node.Mode&0o777)
	}
	if err := h1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if node, _ := b.Node("/app.log"); node.Mode&0o777 != 0o600 {
		t.Fatalf("backend mode after closes = %#o, want 0600", node.Mode&0o777)
	}
}
