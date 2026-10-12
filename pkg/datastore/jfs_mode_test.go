package datastore

import (
	"bytes"
	"context"
	"syscall"
	"testing"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/extent"
)

func TestExtentMetadataSurvivesRuntimeReopen(t *testing.T) {
	s := newTestStore(t)
	rt := newExtentRuntime(t, s, 0)

	ctx := jfsmeta.NewContext(1, 0, []uint32{0}).WithValue(jfsmeta.Drive9PathKey, "/metadata.db")
	var ino jfsmeta.Ino
	var attr jfsmeta.Attr
	if st := rt.Meta.Create(ctx, jfsmeta.RootInode, "metadata.db", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	w := rt.Writer.Open(ino, 0, 0)
	if st := w.Write(ctx, 0, []byte("metadata")); st != 0 {
		t.Fatalf("write: %v", st)
	}
	if st := w.Flush(ctx); st != 0 {
		t.Fatalf("flush: %v", st)
	}
	_ = w.Close(ctx)

	wantAtime := time.Date(2019, 2, 3, 4, 5, 6, 123456789, time.UTC)
	wantMtime := time.Date(2020, 1, 2, 3, 4, 5, 987654321, time.UTC)
	attr.Mode = 0o640
	attr.Uid = 1234
	attr.Gid = 2345
	attr.Atime = wantAtime.Unix()
	attr.Atimensec = uint32(wantAtime.Nanosecond())
	attr.Mtime = wantMtime.Unix()
	attr.Mtimensec = uint32(wantMtime.Nanosecond())
	set := uint16(jfsmeta.SetAttrMode | jfsmeta.SetAttrUID | jfsmeta.SetAttrGID |
		jfsmeta.SetAttrAtime | jfsmeta.SetAttrMtime)
	if st := rt.Meta.SetAttr(ctx, ino, set, 0, &attr); st != 0 {
		t.Fatalf("setattr: %v", st)
	}
	if err := extent.CloseRuntime(rt); err != nil {
		t.Fatalf("close first runtime: %v", err)
	}

	rt = newExtentRuntime(t, s, 0)
	t.Cleanup(func() { _ = extent.CloseRuntime(rt) })
	var got jfsmeta.Attr
	if st := rt.Meta.GetAttr(ctx, ino, &got); st != 0 {
		t.Fatalf("getattr after reopen: %v", st)
	}
	if got.Mode != 0o640 || got.Uid != 1234 || got.Gid != 2345 {
		t.Fatalf("metadata after reopen = mode=%#o owner=%d:%d, want 0640 1234:2345", got.Mode, got.Uid, got.Gid)
	}
	gotAtime := time.Unix(got.Atime, int64(got.Atimensec))
	gotMtime := time.Unix(got.Mtime, int64(got.Mtimensec))
	if !gotAtime.Equal(wantAtime) || !gotMtime.Equal(wantMtime) {
		t.Fatalf("times after reopen = atime %s mtime %s, want %s / %s", gotAtime, gotMtime, wantAtime, wantMtime)
	}
}

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

func TestListDirPublishesMirroredDirectoryExtentInode(t *testing.T) {
	s := newTestStore(t)
	// Dat9FS.Mkdir creates the Drive9 directory projection first, then mirrors
	// the same directory into the native extent namespace. Model that exact
	// ordering here: jfsMknodTx binds an existing directory projection; it must
	// not invent a second namespace row when called through the extent API.
	if err := s.EnsureParentDirs(context.Background(), "/root/child", genID); err != nil {
		t.Fatal(err)
	}
	before, err := s.GetExtentProjection(context.Background(), "/root/")
	if err != nil {
		t.Fatal(err)
	}
	if before.ExtentIno != 0 {
		t.Fatalf("unmirrored directory extent inode = %d, want 0", before.ExtentIno)
	}
	beforeEntries, err := s.ListDir(context.Background(), "/")
	if err != nil {
		t.Fatal(err)
	}
	if len(beforeEntries) != 1 || beforeEntries[0].Node.Path != "/root/" {
		t.Fatalf("unmirrored directory listing = %+v, want only /root/", beforeEntries)
	}
	if beforeEntries[0].ExtentIno != 0 {
		t.Fatalf("unmirrored listed directory extent inode = %d, want 0", beforeEntries[0].ExtentIno)
	}

	rt := newExtentRuntime(t, s, 0)
	t.Cleanup(func() { _ = extent.CloseRuntime(rt) })

	ctx := jfsmeta.NewContext(1, 0, []uint32{0}).WithValue(jfsmeta.Drive9PathKey, "/root/")
	var ino jfsmeta.Ino
	var attr jfsmeta.Attr
	if st := rt.Meta.Mkdir(ctx, jfsmeta.RootInode, "root", 0700, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("mkdir: %v", st)
	}

	entries, err := s.ListDir(context.Background(), "/")
	if err != nil {
		t.Fatal(err)
	}
	for _, nf := range entries {
		if nf.Node.Path != "/root/" {
			continue
		}
		if !nf.Node.IsDirectory {
			t.Fatalf("mirrored directory projection = %+v", nf)
		}
		if nf.ExtentIno != uint64(ino) {
			t.Fatalf("mirrored directory extent inode = %d, want %d", nf.ExtentIno, ino)
		}
		if nf.ContentLayout != "" {
			t.Fatalf("mirrored directory content layout = %q, want empty", nf.ContentLayout)
		}
		return
	}
	t.Fatal("ListDir did not return mirrored /root directory")
}

func TestBindDirectoryProjectionRepairsExistingNativeEdge(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.EnsureParentDirs(ctx, "/root/file", genID); err != nil {
		t.Fatal(err)
	}
	rt := newExtentRuntime(t, s, 0)
	t.Cleanup(func() { _ = extent.CloseRuntime(rt) })

	jctx := jfsmeta.NewContext(1, 0, []uint32{0}).WithValue(jfsmeta.Drive9PathKey, "/root/")
	var ino jfsmeta.Ino
	var attr jfsmeta.Attr
	if st := rt.Meta.Mkdir(jctx, jfsmeta.RootInode, "root", 0700, 0, 0, &ino, &attr); st != 0 && st != syscall.EEXIST {
		t.Fatalf("mkdir native root: %v", st)
	}
	if ino == 0 {
		st := rt.Meta.Lookup(jctx, jfsmeta.RootInode, "root", &ino, &attr, false)
		if st != 0 {
			t.Fatalf("lookup native root: %v", st)
		}
	}
	if _, err := s.DB().Exec(`UPDATE file_nodes SET extent_ino = NULL WHERE path = ?`, "/root/"); err != nil {
		t.Fatal(err)
	}

	body, errno, err := s.RunExtentMetaOp(ctx, "bind_dir_projection", mustJSON(map[string]any{
		"path": "/root/", "inode": uint64(ino),
	}), nil)
	if err != nil || errno != 0 {
		t.Fatalf("bind directory projection: body=%s errno=%d err=%v", body, errno, err)
	}
	proj, err := s.GetExtentProjection(ctx, "/root/")
	if err != nil {
		t.Fatal(err)
	}
	if proj.ExtentIno != uint64(ino) {
		t.Fatalf("bound extent inode = %d, want %d", proj.ExtentIno, ino)
	}

	// The same request is idempotent, while a different inode may not steal the
	// projection from the exact edge resolved above.
	if _, errno, err := s.RunExtentMetaOp(ctx, "bind_dir_projection", mustJSON(map[string]any{
		"path": "/root", "inode": uint64(ino),
	}), nil); err != nil || errno != 0 {
		t.Fatalf("repeat bind: errno=%d err=%v", errno, err)
	}
	if _, errno, err := s.RunExtentMetaOp(ctx, "bind_dir_projection", mustJSON(map[string]any{
		"path": "/root/", "inode": uint64(ino) + 1,
	}), nil); err != nil || errno != int(syscall.ESTALE) {
		t.Fatalf("wrong-inode bind: errno=%d err=%v, want ESTALE", errno, err)
	}
}
