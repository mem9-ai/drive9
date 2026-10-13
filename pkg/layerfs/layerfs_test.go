package layerfs

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
)

func newBackend(t *testing.T) (*testbackend.Backend, *client.Client) {
	t.Helper()
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	return b, client.New(ts.URL, "")
}

func TestLoadSnapshotFoldsMarkerTrees(t *testing.T) {
	b, c := newBackend(t)
	base := "/.drive9/layers/L1"
	b.Seed(base+"/tree/repo/a.txt", []byte("a"))
	b.Seed(base+"/tree/repo/sub/b.txt", []byte("b"))
	b.Seed(base+"/blackouts/repo/gone.txt", []byte("7"))
	b.Seed(base+"/blackout-dirs/repo/olddir", []byte("-"))
	b.Seed(base+"/cas/repo/a.txt", []byte("7"))
	b.Seed(base+"/attrs/repo/mode.txt", []byte(`{"mode":493}`))

	snap, err := LoadSnapshot(context.Background(), c, base)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := snap.Tree["/repo/sub/b.txt"]; !ok {
		t.Fatalf("tree = %v", snap.Tree)
	}
	if e := snap.Tree["/repo/a.txt"]; e.IsDir || e.Size != 1 {
		t.Fatalf("tree entry = %+v", e)
	}
	if bl := snap.Blackouts["/repo/gone.txt"]; !bl.HasBase || bl.BaseRev != 7 || bl.IsDir {
		t.Fatalf("blackout = %+v", bl)
	}
	if bl := snap.Blackouts["/repo/olddir"]; !bl.IsDir {
		t.Fatalf("dir blackout = %+v", bl)
	}
	if a := snap.Cas["/repo/a.txt"]; !a.HasBase || a.BaseRev != 7 {
		t.Fatalf("cas = %+v", a)
	}
	if p := snap.Attrs["/repo/mode.txt"]; p.Mode != 493 {
		t.Fatalf("attrs = %+v", p)
	}
}

func TestBlackoutCodec(t *testing.T) {
	// The marker body carries only the base revision; the dir-ness lives
	// in the root the marker sits under.
	cases := []Blackout{
		{},
		{BaseRev: 42, HasBase: true},
	}
	for _, want := range cases {
		got := DecodeBlackout(want.Encode())
		if got != want {
			t.Fatalf("codec round trip: %+v -> %+v", want, got)
		}
	}
	if got := DecodeBlackout([]byte("7")); !got.HasBase || got.BaseRev != 7 {
		t.Fatalf("rev marker = %+v", got)
	}
	if got := (Blackout{IsDir: true}).Encode(); string(got) != "-" {
		t.Fatalf("dir marker body = %q", got)
	}
}

func TestCasCodec(t *testing.T) {
	if rev, hasBase := DecodeCas(EncodeCas(0, false)); hasBase || rev != 0 {
		t.Fatalf("absent anchor = %d %v", rev, hasBase)
	}
	if rev, hasBase := DecodeCas(EncodeCas(12, true)); !hasBase || rev != 12 {
		t.Fatalf("rev anchor = %d %v", rev, hasBase)
	}
}

func TestCopySnapshotIsDeep(t *testing.T) {
	b, c := newBackend(t)
	src := "/.drive9/layers/L1"
	b.Seed(src+"/tree/repo/a.txt", []byte("content"))
	b.Seed(src+"/tree/repo/d", nil)
	b.Seed(src+"/blackouts/repo/g.txt", []byte("-"))
	b.Seed(src+"/cas/repo/a.txt", []byte("5"))

	if err := CopySnapshot(context.Background(), c, src, "/.drive9/layers/L2"); err != nil {
		t.Fatal(err)
	}
	if n, ok := b.Node("/.drive9/layers/L2/tree/repo/a.txt"); !ok || string(n.Data) != "content" {
		t.Fatalf("copied file = %+v", n)
	}
	// mutating the source after the copy leaves the child untouched
	if err := c.WriteCtx(context.Background(), src+"/tree/repo/a.txt", []byte("moved on")); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/.drive9/layers/L2/tree/repo/a.txt"); string(n.Data) != "content" {
		t.Fatalf("copy shares the source entity: %q", n.Data)
	}
	if _, ok := b.Node("/.drive9/layers/L2/blackouts/repo/g.txt"); !ok {
		t.Fatal("copy lost the blackout marker")
	}
}

func TestCommitEmptyLayerFlipsState(t *testing.T) {
	b, c := newBackend(t)
	layer := &client.FSLayer{LayerID: "L1", BaseRootPath: "/"}
	res, err := Commit(context.Background(), c, layer)
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied != 0 || len(res.Conflicts) != 0 {
		t.Fatalf("empty commit = %+v", res)
	}
	if st, _ := b.LayerStateForTest("L1"); st != "committed" {
		t.Fatalf("layer state = %q", st)
	}
}

func TestCommitRefusesTerminalStates(t *testing.T) {
	_, c := newBackend(t)
	layer := &client.FSLayer{LayerID: "L1", BaseRootPath: "/", State: "abandoned"}
	if _, err := Commit(context.Background(), c, layer); err == nil || !strings.Contains(err.Error(), "abandoned") {
		t.Fatalf("terminal-state commit err = %v", err)
	}
}

func TestCommitRefusesLegacyEntryLog(t *testing.T) {
	_, c := newBackend(t)
	// A legacy entry planted in the server-side log must block the state
	// flip (the server commit would replay it).
	if _, err := c.UpsertFSLayerEntry(context.Background(), "L1", client.FSLayerEntryRequest{Path: "/a", Op: "upsert", Kind: "file", Content: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	layer := &client.FSLayer{LayerID: "L1", BaseRootPath: "/", State: "active"}
	if _, err := Commit(context.Background(), c, layer); err == nil || !strings.Contains(err.Error(), "legacy") {
		t.Fatalf("legacy-log commit err = %v", err)
	}
}

func TestCommitWhiteoutRevisionConflict(t *testing.T) {
	b, c := newBackend(t)
	b.Seed("/repo/gone.txt", []byte("v1"))
	roots := "/.drive9/layers/layer-1"
	// The blackout anchored the deletion at revision 1; the base moved on.
	b.Seed(roots+"/blackouts/repo/gone.txt", []byte("1"))
	if err := c.WriteCtx(context.Background(), "/repo/gone.txt", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	res, err := Commit(context.Background(), c, &client.FSLayer{LayerID: "layer-1", BaseRootPath: "/", State: "active"})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(res.Conflicts) == 0 || res.Conflicts[0].Reason != "base revision changed" {
		t.Fatalf("conflicts = %+v", res.Conflicts)
	}
	if n, _ := b.Node("/repo/gone.txt"); string(n.Data) != "v2" {
		t.Fatalf("stale whiteout deleted the newer base: %q", n.Data)
	}
}

func TestCommitDirWhiteoutRemovesSubtree(t *testing.T) {
	b, c := newBackend(t)
	b.Seed("/repo/olddir/a.txt", []byte("a"))
	b.Seed("/repo/olddir/sub/b.txt", []byte("b"))
	roots := "/.drive9/layers/layer-1"
	b.Seed(roots+"/blackout-dirs/repo/olddir", []byte("-"))
	res, err := Commit(context.Background(), c, &client.FSLayer{LayerID: "layer-1", BaseRootPath: "/", State: "active"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Conflicts) != 0 {
		t.Fatalf("conflicts = %+v", res.Conflicts)
	}
	if _, ok := b.Node("/repo/olddir/a.txt"); ok {
		t.Fatal("dir whiteout left base children behind")
	}
}

func TestCommitResumeAfterConflictDoesNotStall(t *testing.T) {
	b, c := newBackend(t)
	b.Seed("/repo/keep.txt", []byte("keep"))
	roots := "/.drive9/layers/layer-1"
	b.Seed(roots+"/tree/repo/keep.txt", []byte("layered"))
	b.Seed(roots+"/cas/repo/keep.txt", []byte("1"))
	// Base moved past the anchor: the first commit conflicts and must
	// abort its journal; after aligning the anchor, a fresh commit
	// succeeds (the frozen plan does not block forever).
	if err := c.WriteCtx(context.Background(), "/repo/keep.txt", []byte("base moved")); err != nil {
		t.Fatal(err)
	}
	res, err := Commit(context.Background(), c, &client.FSLayer{LayerID: "layer-1", BaseRootPath: "/", State: "active"})
	if err != nil {
		t.Fatalf("first commit: %v", err)
	}
	if len(res.Conflicts) == 0 {
		t.Fatal("expected conflict")
	}
	st, err := c.StatCtx(context.Background(), "/repo/keep.txt")
	if err != nil {
		t.Fatal(err)
	}
	if werr := c.WriteCtx(context.Background(), roots+"/cas/repo/keep.txt", []byte(fmt.Sprintf("%d", st.Revision))); werr != nil {
		t.Fatal(werr)
	}
	res2, err := Commit(context.Background(), c, &client.FSLayer{LayerID: "layer-1", BaseRootPath: "/", State: "active"})
	if err != nil || len(res2.Conflicts) != 0 {
		t.Fatalf("second commit = %+v %v", res2, err)
	}
	if n, _ := b.Node("/repo/keep.txt"); string(n.Data) != "layered" {
		t.Fatalf("base = %q", n.Data)
	}
}
