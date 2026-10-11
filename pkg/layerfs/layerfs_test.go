package layerfs

import (
	"context"
	"net/http/httptest"
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
