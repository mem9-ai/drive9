package layer

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// The suite drives the shadow view through the path-addressed
// fs.Filesystem API over [layer → local-fs core] and asserts both the
// view and the on-disk truth (the tree/blackout/cas/attrs markers are
// plain files in the core's directory).

func mustRead(t *testing.T, s *stack, path string) string {
	t.Helper()
	data, err := s.fsys.Read(context.Background(), path, 0, 0)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func mustAttr(t *testing.T, s *stack, path string) fs.Attr {
	t.Helper()
	attr, err := s.fsys.GetAttr(context.Background(), path)
	if err != nil {
		t.Fatalf("getattr %s: %v", path, err)
	}
	return attr
}

// disk reads a file under the stack's root directly (the on-disk truth).
func (s *stack) disk(t *testing.T, path string) (string, bool) {
	t.Helper()
	data, err := os.ReadFile(s.core.sysPath(path))
	if err != nil {
		return "", false
	}
	return string(data), true
}

func (s *stack) diskExists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Lstat(s.core.sysPath(path))
	return err == nil
}

func TestShadowAddsOverClassicView(t *testing.T) {
	s := newLocalStack(t, "L1", "")
	ctx := context.Background()
	s.seed(t, "/base.txt", []byte("base"))

	if err := s.fsys.Mkdir(ctx, "/newdir", 0o755); err != nil {
		t.Fatal(err)
	}
	if attr := mustAttr(t, s, "/newdir"); !attr.IsDir() {
		t.Fatalf("shadow dir = %+v", attr)
	}
	if !s.diskExists(t, treePath("/newdir")) {
		t.Fatal("mkdir did not land in the layer tree")
	}
	if s.diskExists(t, "/newdir") {
		t.Fatal("mkdir leaked into the classic namespace")
	}
	if got := mustRead(t, s, "/base.txt"); got != "base" {
		t.Fatalf("classic read = %q", got)
	}
}

func TestCoWEditLeavesClassicUntouched(t *testing.T) {
	s := newLocalStack(t, "L1", "")
	ctx := context.Background()
	s.seed(t, "/classic.txt", []byte("classic body"))

	if err := s.fsys.Write(ctx, "/classic.txt", []byte("edited body"), fs.WriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if got := mustRead(t, s, "/classic.txt"); got != "edited body" {
		t.Fatalf("shadow read = %q", got)
	}
	if got, ok := s.disk(t, "/classic.txt"); !ok || got != "classic body" {
		t.Fatalf("classic mutated on disk: %q", got)
	}
	if got, ok := s.disk(t, treePath("/classic.txt")); !ok || got != "edited body" {
		t.Fatalf("tree copy = %q ok=%v", got, ok)
	}
	if got, ok := s.disk(t, "/.drive9/layers/L1/cas/classic.txt"); !ok || got == "0" {
		t.Fatalf("cas anchor = %q ok=%v (want a positive base rev)", got, ok)
	}
}

func TestOpenHandleCoWAndAppend(t *testing.T) {
	s := newLocalStack(t, "L1", "")
	ctx := context.Background()
	s.seed(t, "/doc.txt", []byte("doc body"))

	res, err := s.ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/doc.txt", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if h == nil {
		t.Fatal("no handle")
	}
	head := make([]byte, 3)
	if _, err := h.Read(ctx, 0, head); err != nil || string(head) != "doc" {
		t.Fatalf("handle read = %q %v", head, err)
	}
	if err := h.Write(ctx, 8, []byte("edited")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if got, ok := s.disk(t, treePath("/doc.txt")); !ok || got != "doc bodyedited" {
		t.Fatalf("tree copy after handle write = %q ok=%v", got, ok)
	}
	if got, _ := s.disk(t, "/doc.txt"); got != "doc body" {
		t.Fatalf("classic mutated: %q", got)
	}
}

func TestUnlinkBlackoutAndRecreate(t *testing.T) {
	s := newLocalStack(t, "L1", "")
	ctx := context.Background()
	s.seed(t, "/gone.txt", []byte("gone"))
	s.seed(t, "/keep.txt", []byte("keep"))

	if err := s.fsys.Unlink(ctx, "/gone.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.fsys.GetAttr(ctx, "/gone.txt"); err == nil {
		t.Fatal("blackout did not hide the classic file")
	}
	if got, ok := s.disk(t, "/gone.txt"); !ok || got != "gone" {
		t.Fatalf("classic file did not survive the unlink: %q ok=%v", got, ok)
	}
	if got, ok := s.disk(t, "/.drive9/layers/L1/blackouts/gone.txt"); !ok {
		t.Fatalf("blackout marker missing (content %q ok=%v)", got, ok)
	}

	// listing merges: hidden name gone, kept name present
	entries, err := s.fsys.List(ctx, "/")
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, e := range entries {
		names[e.Name] = true
	}
	if names["gone.txt"] || !names["keep.txt"] {
		t.Fatalf("merged listing = %v", names)
	}

	// recreate over the blackout (shell `>` = O_CREAT semantics): the
	// name returns with fresh content and the classic file stays
	// untouched underneath
	res, err := s.ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/gone.txt", Flags: uint32(syscall.O_WRONLY | syscall.O_CREAT | syscall.O_TRUNC), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if h := res.Handle; h != nil {
		if err := h.Write(ctx, 0, []byte("reborn")); err != nil {
			t.Fatal(err)
		}
		if err := h.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		_ = h.Release(ctx)
	}
	if got := mustRead(t, s, "/gone.txt"); got != "reborn" {
		t.Fatalf("recreated read = %q", got)
	}
	if got, _ := s.disk(t, "/gone.txt"); got != "gone" {
		t.Fatalf("classic mutated by recreate: %q", got)
	}
}

func TestDirBlackoutHidesSubtree(t *testing.T) {
	s := newLocalStack(t, "L1", "")
	ctx := context.Background()
	if err := s.fsys.Mkdir(ctx, "/d", 0o755); err != nil {
		t.Fatal(err)
	}
	s.seed(t, "/d/x.txt", []byte("x"))
	s.seed(t, "/d/y.txt", []byte("y"))

	if err := s.fsys.Rmdir(ctx, "/d"); err == nil {
		t.Fatal("rmdir of a non-empty merged dir should fail")
	}
	if err := s.fsys.Unlink(ctx, "/d/x.txt"); err != nil {
		t.Fatal(err)
	}
	if err := s.fsys.Unlink(ctx, "/d/y.txt"); err != nil {
		t.Fatal(err)
	}
	if err := s.fsys.Rmdir(ctx, "/d"); err != nil {
		t.Fatalf("rmdir of emptied dir: %v", err)
	}
	if _, err := s.fsys.GetAttr(ctx, "/d/x.txt"); err == nil {
		t.Fatal("dir blackout did not hide the subtree")
	}
	if got, ok := s.disk(t, "/.drive9/layers/L1/blackout-dirs/d"); !ok {
		t.Fatalf("dir blackout marker missing (content %q ok=%v)", got, ok)
	}
	if _, ok := s.disk(t, "/d/x.txt"); !ok {
		t.Fatal("dir blackout deleted classic content")
	}
}

func TestAttrsMarkerChmodsClassicFile(t *testing.T) {
	s := newLocalStack(t, "L1", "")
	ctx := context.Background()
	s.seed(t, "/script.sh", []byte("#!/bin/sh\n"))

	mode := uint32(0o755)
	if err := s.fsys.SetAttr(ctx, "/script.sh", fs.AttrPatch{Mode: &mode}); err != nil {
		t.Fatal(err)
	}
	if attr := mustAttr(t, s, "/script.sh"); attr.Mode&0o755 != 0o755 {
		t.Fatalf("patched attr = %+v", attr)
	}
	if info, err := os.Lstat(s.core.sysPath("/script.sh")); err != nil || info.Mode().Perm() != 0o644 {
		t.Fatalf("chmod leaked to the classic file: %v %v", info, err)
	}
	if got, ok := s.disk(t, "/.drive9/layers/L1/attrs/script.sh"); !ok || !strings.Contains(got, "493") {
		t.Fatalf("attrs marker = %q ok=%v", got, ok)
	}
}

func TestSetAttrTruncatePumpsClassicContent(t *testing.T) {
	s := newLocalStack(t, "L1", "")
	ctx := context.Background()
	s.seed(t, "/big.txt", []byte("0123456789"))

	size := int64(4)
	if err := s.fsys.SetAttr(ctx, "/big.txt", fs.AttrPatch{Size: &size}); err != nil {
		t.Fatal(err)
	}
	if got := mustRead(t, s, "/big.txt"); got != "0123" {
		t.Fatalf("truncated read = %q", got)
	}
	if got, ok := s.disk(t, treePath("/big.txt")); !ok || got != "0123" {
		t.Fatalf("tree copy = %q ok=%v", got, ok)
	}
	if got, _ := s.disk(t, "/big.txt"); got != "0123456789" {
		t.Fatalf("classic mutated by truncate: %q", got)
	}
}

func TestRenameClassicIntoTreeAndWithinTree(t *testing.T) {
	s := newLocalStack(t, "L1", "")
	ctx := context.Background()
	s.seed(t, "/src.txt", []byte("content"))

	if err := s.fsys.Rename(ctx, "/src.txt", "/dst.txt", fs.RenameOptions{}); err != nil {
		t.Fatal(err)
	}
	if got := mustRead(t, s, "/dst.txt"); got != "content" {
		t.Fatalf("renamed read = %q", got)
	}
	if got, ok := s.disk(t, treePath("/dst.txt")); !ok || got != "content" {
		t.Fatalf("tree copy = %q ok=%v", got, ok)
	}
	if got, _ := s.disk(t, "/src.txt"); got != "content" {
		t.Fatal("rename deleted the classic source")
	}
	if _, err := s.fsys.GetAttr(ctx, "/src.txt"); err == nil {
		t.Fatal("rename did not blackout the source name")
	}

	// second rename moves within the tree (server-side atomic)
	if err := s.fsys.Rename(ctx, "/dst.txt", "/final.txt", fs.RenameOptions{}); err != nil {
		t.Fatal(err)
	}
	if got := mustRead(t, s, "/final.txt"); got != "content" {
		t.Fatalf("tree rename read = %q", got)
	}
	if !s.diskExists(t, treePath("/final.txt")) || s.diskExists(t, treePath("/dst.txt")) {
		t.Fatal("tree rename did not move the tree file")
	}
}

func TestSymlinkShadowAndReadlink(t *testing.T) {
	s := newLocalStack(t, "L1", "")
	ctx := context.Background()

	if err := s.fsys.Symlink(ctx, "/classic.txt", "/link"); err != nil {
		t.Fatal(err)
	}
	target, err := s.fsys.Readlink(ctx, "/link")
	if err != nil || target != "/classic.txt" {
		t.Fatalf("readlink = %q %v", target, err)
	}
	if !s.diskExists(t, treePath("/link")) {
		t.Fatal("symlink did not land in the tree")
	}
	// the view classifies it as a symlink
	if attr := mustAttr(t, s, "/link"); !attr.IsSymlink() {
		t.Fatalf("link attr = %+v", attr)
	}
}

func TestCheckpointMountIsReadOnlyAndPinned(t *testing.T) {
	ctx := context.Background()
	// the checkpoint snapshot must exist before Start walks it into the
	// view index
	core := newLocalCore(t)
	reg := newFakeRegistry()
	reg.addLayer("L1")
	reg.addCheckpoint("cp1", "L1")
	snapTree := "/.drive9/layers/L1/checkpoints/cp1/tree/v.txt"
	if err := os.MkdirAll(filepath.Dir(core.sysPath(snapTree)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(core.sysPath(snapTree), []byte("v1"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := newLocalStackOver(t, core, reg, "L1", "cp1")

	if got := mustRead(t, s, "/v.txt"); got != "v1" {
		t.Fatalf("checkpoint read = %q", got)
	}
	if err := s.fsys.Write(ctx, "/w.txt", []byte("x"), fs.WriteOptions{}); !errors.Is(err, syscall.EROFS) {
		t.Fatalf("checkpoint write error = %v", err)
	}
	if err := s.fsys.Mkdir(ctx, "/d", 0o755); !errors.Is(err, syscall.EROFS) {
		t.Fatalf("checkpoint mkdir error = %v", err)
	}
	// plain read opens still work
	res, err := s.ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/v.txt", Flags: 0})
	if err != nil {
		t.Fatalf("checkpoint read open = %v", err)
	}
	if h := res.Handle; h != nil {
		_ = h.Release(ctx)
	}
}

func TestRollbackClearsShadowAndFailsWrites(t *testing.T) {
	s := newLocalStack(t, "L1", "")
	ctx := context.Background()
	s.seed(t, "/base.txt", []byte("base"))

	if err := s.fsys.Write(ctx, "/ov.txt", []byte("x"), fs.WriteOptions{}); err != nil {
		t.Fatal(err)
	}
	s.reg.rollback("L1")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		err := s.fsys.Write(ctx, "/probe.txt", []byte("x"), fs.WriteOptions{})
		if errors.Is(err, syscall.ESTALE) {
			break
		}
		if err != nil {
			t.Fatalf("post-rollback probe error = %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := s.fsys.Write(ctx, "/probe2.txt", []byte("x"), fs.WriteOptions{}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("post-rollback write error = %v", err)
	}
	// the classic view reappears; the overlay content is gone
	if got := mustRead(t, s, "/base.txt"); got != "base" {
		t.Fatalf("classic view lost after rollback: %q", got)
	}
	if _, err := s.fsys.GetAttr(ctx, "/ov.txt"); err == nil {
		t.Fatal("overlay content survived the rollback")
	}
}

func TestCheckoutHotSwitchesView(t *testing.T) {
	s := newLocalStack(t, "", "")
	ctx := context.Background()
	s.reg.addLayer("L2")
	s.seed(t, "/classic.txt", []byte("classic"))
	// another layer's tree already on disk
	if err := os.MkdirAll(filepath.Dir(s.core.sysPath("/.drive9/layers/L2/tree/layered.txt")), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.core.sysPath("/.drive9/layers/L2/tree/layered.txt"), []byte("from L2"), 0o644); err != nil {
		t.Fatal(err)
	}

	// live mode: writes land in the classic namespace
	if err := s.fsys.Write(ctx, "/live.txt", []byte("live"), fs.WriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if got, ok := s.disk(t, "/live.txt"); !ok || got != "live" {
		t.Fatalf("live write = %q ok=%v", got, ok)
	}

	// hot-switch to L2: its tree shadows the view without remount
	if err := s.l.Checkout(ctx, CheckoutTarget{LayerRef: "L2"}); err != nil {
		t.Fatal(err)
	}
	if got := mustRead(t, s, "/layered.txt"); got != "from L2" {
		t.Fatalf("layered read after checkout = %q", got)
	}
	if err := s.fsys.Write(ctx, "/w.txt", []byte("x"), fs.WriteOptions{}); err != nil {
		t.Fatal(err)
	}
	if got, ok := s.disk(t, "/.drive9/layers/L2/tree/w.txt"); !ok || got != "x" {
		t.Fatalf("post-checkout write missed the tree: %q ok=%v", got, ok)
	}
	if s.diskExists(t, "/w.txt") {
		t.Fatal("post-checkout write leaked to the classic namespace")
	}

	// back to live: the classic view returns
	if err := s.l.Checkout(ctx, CheckoutTarget{Live: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.fsys.GetAttr(ctx, "/layered.txt"); err == nil {
		t.Fatal("live view still shows the layer tree")
	}
	if got := mustRead(t, s, "/classic.txt"); got != "classic" {
		t.Fatalf("live read after checkout back = %q", got)
	}
}

func TestCheckoutRejectsTerminalLayers(t *testing.T) {
	s := newLocalStack(t, "", "")
	s.reg.addLayer("L9")
	s.reg.setState("L9", "committed")
	if err := s.l.Checkout(context.Background(), CheckoutTarget{LayerRef: "L9"}); err == nil {
		t.Fatal("checkout of a committed layer should fail")
	}
}
