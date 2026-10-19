package datastore

import (
	"bytes"
	"context"
	"syscall"
	"testing"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/extent"
)

// chmod 000 is a stored permission, not a missing one: the per-file stat
// overlay published it faithfully, but the listing overlay replaced a real 0
// with the type default (0644), so the same file answered 0000 to Stat and
// 0644 to ListDir. Both readers must see the stored bits, whatever they are;
// only a NULL mode may fall back to a default.
func TestExtentChmodZeroStatAndListDirAgree(t *testing.T) {
	s := newTestStore(t)
	rt := newExtentRuntime(t, s, 0)
	t.Cleanup(func() { _ = extent.CloseRuntime(rt) })

	ctx := jfsmeta.NewContext(1, 0, []uint32{0}).WithValue(jfsmeta.Drive9PathKey, "/locked.db")
	var ino jfsmeta.Ino
	var attr jfsmeta.Attr
	if st := rt.Meta.Create(ctx, jfsmeta.RootInode, "locked.db", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	w := rt.Writer.Open(ino, 0, 0)
	if st := w.Write(ctx, 0, bytes.Repeat([]byte("s"), 128)); st != 0 {
		t.Fatalf("write: %v", st)
	}
	if st := w.Flush(ctx); st != 0 {
		t.Fatalf("flush: %v", st)
	}
	_ = w.Close(ctx)

	attr.Mode = 0
	if st := rt.Meta.SetAttr(ctx, ino, jfsmeta.SetAttrMode, 0, &attr); st != 0 {
		t.Fatalf("chmod 000: %v", st)
	}

	bg := context.Background()
	lite, err := s.StatLite(bg, "/locked.db")
	if err != nil {
		t.Fatal(err)
	}
	if !lite.HasMode {
		t.Fatal("StatLite did not carry a mode")
	}
	if lite.Mode&0o7777 != 0 {
		t.Fatalf("StatLite mode = %o, want permission bits 0 (chmod 000 preserved)", lite.Mode)
	}

	entries, err := s.ListDir(bg, "/")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, nf := range entries {
		if nf.Node.Path != "/locked.db" {
			continue
		}
		found = true
		if !nf.HasMode {
			t.Fatal("ListDir entry did not carry a mode")
		}
		// The listing entry carries the file-type bits alongside the
		// permissions (readdirplus needs them); the permission bits must
		// agree with the per-file stat.
		if nf.Mode&0o7777 != 0 {
			t.Fatalf("ListDir mode = %o, want permission bits 0 (same as the per-file stat)", nf.Mode)
		}
		if nf.Mode&uint32(syscall.S_IFMT) != uint32(syscall.S_IFREG) {
			t.Fatalf("ListDir mode = %o, want S_IFREG type bits", nf.Mode)
		}
	}
	if !found {
		t.Fatal("ListDir did not return /locked.db")
	}

	// And a NULL mode still yields the documented default rather than 000.
	if got := jfsTypeToStatMode(jfsTypeFile, 0, false); got != 0o100644 {
		t.Fatalf("default mode for a missing permission = %o, want 100644", got)
	}
	if got := jfsTypeToStatMode(jfsTypeFile, 0, true); got != 0o100000 {
		t.Fatalf("stored chmod 000 mode = %o, want 100000", got)
	}
}
