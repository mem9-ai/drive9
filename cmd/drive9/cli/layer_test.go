package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
)

// newLayerBackend wires a CLI client over one fake backend.
func newLayerBackend(t *testing.T) (*testbackend.Backend, *client.Client) {
	t.Helper()
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	return b, client.New(ts.URL, "")
}

func TestUploadReaderToLayerLandsInTreeWithCasMarker(t *testing.T) {
	b, c := newLayerBackend(t)
	b.Seed("/repo/base.bin", []byte("base"))

	ctx := context.Background()
	// fresh path: absent base anchor (cas "0")
	if err := uploadReaderToLayer(ctx, c, "layer-1", "/repo/a.bin", strings.NewReader("data"), 4, 0o644, true); err != nil {
		t.Fatalf("uploadReaderToLayer: %v", err)
	}
	if n, ok := b.Node("/.drive9/layers/layer-1/tree/repo/a.bin"); !ok || string(n.Data) != "data" {
		t.Fatalf("tree content = %+v", n)
	}
	if n, ok := b.Node("/.drive9/layers/layer-1/cas/repo/a.bin"); !ok || string(n.Data) != "0" {
		t.Fatalf("cas marker = %+v", n)
	}

	// classic-backed path: the anchor pins the base revision
	if err := uploadReaderToLayer(ctx, c, "layer-1", "/repo/base.bin", strings.NewReader("over"), 4, 0o644, true); err != nil {
		t.Fatalf("uploadReaderToLayer (base): %v", err)
	}
	base, _ := b.Node("/repo/base.bin")
	if n, ok := b.Node("/.drive9/layers/layer-1/cas/repo/base.bin"); !ok || string(n.Data) == "0" {
		t.Fatalf("cas marker for classic base = %+v (base rev %d)", n, base.Rev)
	}
	if string(base.Data) != "base" {
		t.Fatalf("classic base mutated: %q", base.Data)
	}
}

func TestCpLayerLocalUploadUsesTreePath(t *testing.T) {
	b, c := newLayerBackend(t)
	localPath := filepath.Join(t.TempDir(), "layer.txt")
	if err := os.WriteFile(localPath, []byte("layer upload"), 0o640); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := uploadLocalFileToLayer(context.Background(), c, "layer-1", localPath, "/dst/layer.txt"); err != nil {
		t.Fatalf("uploadLocalFileToLayer: %v", err)
	}
	if n, ok := b.Node("/.drive9/layers/layer-1/tree/dst/layer.txt"); !ok || string(n.Data) != "layer upload" {
		t.Fatalf("tree upload = %+v", n)
	}
	if _, ok := b.Node("/dst/layer.txt"); ok {
		t.Fatal("layer upload leaked into the classic namespace")
	}
}

func TestSearchCommandsRejectLayer(t *testing.T) {
	_, c := newLayerBackend(t)
	if gerr := Grep(c, []string{"--layer", "layer-1", "pattern"}); gerr == nil || !strings.Contains(gerr.Error(), "mount the layer") {
		t.Fatalf("grep --layer error = %v", gerr)
	}
}

func TestLayerCommitAppliesTreeOntoBase(t *testing.T) {
	b, c := newLayerBackend(t)
	b.Seed("/repo/keep.txt", []byte("keep"))
	b.Seed("/repo/gone.txt", []byte("gone"))
	b.Seed("/repo/replaced.txt", []byte("classic v1"))
	roots := "/.drive9/layers/layer-1"
	// A replacement pins the live base revision as its CAS anchor.
	replacedStat, err := c.StatCtx(context.Background(), "/repo/replaced.txt")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.WriteCtx(context.Background(), roots+"/cas/repo/replaced.txt", []byte(fmt.Sprintf("%d", replacedStat.Revision))); err != nil {
		t.Fatal(err)
	}
	// tree additions + blackout + attrs, laid out directly on the backend
	b.Seed(roots+"/tree/repo/added.txt", []byte("added"))
	b.Seed(roots+"/tree/repo/replaced.txt", []byte("replaced"))
	b.Seed(roots+"/cas/repo/added.txt", []byte("0"))
	b.Seed(roots+"/blackouts/repo/gone.txt", []byte("-"))
	b.Seed(roots+"/attrs/repo/keep.txt", []byte(`{"mode":493}`))

	out, err := captureStdoutE(t, func() error { return Layer(c, []string{"commit", "layer-1"}) })
	if err != nil {
		t.Fatalf("Layer commit: %v (out %q)", err, out)
	}
	if !strings.Contains(out, "committed") || !strings.Contains(out, "layer=layer-1") {
		t.Fatalf("stdout = %q", out)
	}
	if n, ok := b.Node("/repo/added.txt"); !ok || string(n.Data) != "added" {
		t.Fatalf("committed addition = %+v", n)
	}
	if n, ok := b.Node("/repo/replaced.txt"); !ok || string(n.Data) != "replaced" {
		t.Fatalf("committed replacement = %+v", n)
	}
	if _, ok := b.Node("/repo/gone.txt"); ok {
		t.Fatal("committed whiteout left the base file behind")
	}
	if n, ok := b.Node("/repo/keep.txt"); !ok || n.Mode&0o755 != 0o755 {
		t.Fatalf("committed chmod = %+v", n)
	}
}

func TestLayerCommitReportsCasConflict(t *testing.T) {
	b, c := newLayerBackend(t)
	b.Seed("/repo/stale.txt", []byte("live-now"))
	roots := "/.drive9/layers/layer-1"
	b.Seed(roots+"/tree/repo/stale.txt", []byte("layered-old"))
	// the base moves past the anchor before the commit applies
	if err := c.WriteCtx(context.Background(), "/repo/stale.txt", []byte("live-now-2")); err != nil {
		t.Fatal(err)
	}
	if err := c.WriteCtx(context.Background(), roots+"/cas/repo/stale.txt", []byte("1")); err != nil {
		t.Fatal(err)
	}

	err := Layer(c, []string{"commit", "layer-1"})
	if err == nil {
		t.Fatal("stale CAS anchor committed")
	}
	if n, _ := b.Node("/repo/stale.txt"); string(n.Data) != "live-now-2" {
		t.Fatalf("conflicted commit mutated the base: %q", n.Data)
	}
}

func TestLayerDiffWalksTheTree(t *testing.T) {
	b, c := newLayerBackend(t)
	roots := "/.drive9/layers/layer-1"
	b.Seed(roots+"/tree/repo/a.txt", []byte("a"))
	b.Seed(roots+"/blackouts/repo/b.txt", []byte("-"))
	b.Seed(roots+"/attrs/repo/c.txt", []byte(`{"mode":493}`))

	out, err := captureStdoutE(t, func() error { return Layer(c, []string{"diff", "layer-1"}) })
	if err != nil {
		t.Fatalf("Layer diff: %v", err)
	}
	for _, want := range []string{"upsert", "/repo/a.txt", "whiteout", "/repo/b.txt", "chmod", "/repo/c.txt"} {
		if !strings.Contains(out, want) {
			t.Fatalf("diff output %q missing %q", out, want)
		}
	}
}

func TestLayerForkCopiesSnapshotBeforeRegistryRow(t *testing.T) {
	b, c := newLayerBackend(t)
	b.Seed("/.drive9/layers/parent/tree/repo/a.txt", []byte("from parent"))
	b.Seed("/.drive9/layers/parent/blackouts/repo/b.txt", []byte("-"))
	b.Seed("/.drive9/layers/parent/cas/repo/a.txt", []byte("0"))

	out, err := captureStdoutE(t, func() error { return Layer(c, []string{"fork", "--id", "child-1", "parent"}) })
	if err != nil {
		t.Fatalf("Layer fork: %v (out %q)", err, out)
	}
	if !strings.Contains(out, "child-1") {
		t.Fatalf("stdout = %q", out)
	}
	if n, ok := b.Node("/.drive9/layers/child-1/tree/repo/a.txt"); !ok || string(n.Data) != "from parent" {
		t.Fatalf("forked tree = %+v", n)
	}
	if _, ok := b.Node("/.drive9/layers/child-1/blackouts/repo/b.txt"); !ok {
		t.Fatal("fork lost the blackout marker")
	}
	if _, ok := b.Node("/.drive9/layers/child-1/cas/repo/a.txt"); !ok {
		t.Fatal("fork lost the cas marker")
	}
}

func TestLayerCheckpointSnapshotsTheTree(t *testing.T) {
	b, c := newLayerBackend(t)
	b.Seed("/.drive9/layers/layer-1/tree/repo/a.txt", []byte("v1"))

	out, err := captureStdoutE(t, func() error { return Layer(c, []string{"checkpoint", "--id", "cp9", "layer-1"}) })
	if err != nil {
		t.Fatalf("Layer checkpoint: %v (out %q)", err, out)
	}
	if !strings.Contains(out, "cp9") {
		t.Fatalf("stdout = %q", out)
	}
	if n, ok := b.Node("/.drive9/layers/layer-1/checkpoints/cp9/tree/repo/a.txt"); !ok || string(n.Data) != "v1" {
		t.Fatalf("checkpoint snapshot = %+v", n)
	}
	// post-checkpoint writes do not disturb the snapshot
	b.Seed("/.drive9/layers/layer-1/tree/repo/a.txt", []byte("v2"))
	if n, _ := b.Node("/.drive9/layers/layer-1/checkpoints/cp9/tree/repo/a.txt"); string(n.Data) != "v1" {
		t.Fatalf("snapshot mutated: %q", n.Data)
	}
}

func TestLayerDeleteSweepsTheTree(t *testing.T) {
	b, c := newLayerBackend(t)
	b.Seed("/.drive9/layers/layer-1/tree/repo/a.txt", []byte("a"))

	out, err := captureStdoutE(t, func() error { return Layer(c, []string{"delete", "layer-1"}) })
	if err != nil {
		t.Fatalf("Layer delete: %v (out %q)", err, out)
	}
	if n, ok := b.Node("/.drive9/layers/layer-1/tree/repo/a.txt"); ok {
		t.Fatalf("delete left the tree behind: %+v", n)
	}
}

func TestLayerCreatePrintsLayerID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/layers" {
			http.NotFound(w, r)
			return
		}
		var req client.FSLayerCreateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.BaseRootPath != "/repo" || req.Name != "task" || req.DurabilityMode != "restore-safe" {
			t.Fatalf("request = %+v", req)
		}
		if req.Tags["task"] != "auth" || req.Tags["env"] != "dev" {
			t.Fatalf("request tags = %+v", req.Tags)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(client.FSLayer{
			LayerID:        "layer-1",
			BaseRootPath:   "/repo/",
			Name:           "task",
			Tags:           map[string]string{"task": "auth", "env": "dev"},
			State:          "active",
			DurabilityMode: "restore-safe",
		})
	}))
	defer srv.Close()

	c := client.New(srv.URL, "")
	out, err := captureStdoutE(t, func() error {
		return Layer(c, []string{"create", "--name", "task", "--durability", "restore-safe", "--tag", "task=auth", "--tag", "env=dev", ":/repo"})
	})
	if err != nil {
		t.Fatalf("Layer create: %v", err)
	}
	if strings.TrimSpace(out) != "layer-1" {
		t.Fatalf("stdout = %q, want layer-1", out)
	}
}

func TestLayerCreateRejectsDuplicateTag(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(client.FSLayer{LayerID: "layer-1"})
	}))
	defer srv.Close()
	c := client.New(srv.URL, "")
	if err := Layer(c, []string{"create", "--tag", "a=1", "--tag", "a=2", ":/repo"}); err == nil || !strings.Contains(err.Error(), "duplicate layer tag") {
		t.Fatalf("duplicate tag error = %v", err)
	}
}
