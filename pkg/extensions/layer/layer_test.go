package layer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// newStack builds [layer → core] over one fake backend with the layer
// mounted on "L1".
func newStack(t *testing.T, b *testbackend.Backend, checkpoint string) (*fs.Chain, *Layer) {
	t.Helper()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	l, err := New(Options{LayerRef: "L1", CheckpointRef: checkpoint, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(l, terminal)
	if err != nil {
		t.Fatal(err)
	}
	return ch, l
}

func get(t *testing.T, ch *fs.Chain, path string) (fs.Attr, error) {
	t.Helper()
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpGetAttr, Path: path})
	if err != nil {
		return fs.Attr{}, err
	}
	if res.Attr == nil {
		t.Fatal("no attr")
	}
	return *res.Attr, nil
}

func TestOverlayMasksAndAddsOverBase(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/base.txt", []byte("base"))
	ch, _ := newStack(t, b, "")
	ctx := context.Background()

	// mkdir in the layer: visible through the overlay, base untouched
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpMkdir, Path: "/newdir", Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	if attr, err := get(t, ch, "/newdir"); err != nil || !attr.IsDir() {
		t.Fatalf("overlay dir = %+v %v", attr, err)
	}
	if _, err := get(t, ch, "/base.txt"); err != nil {
		t.Fatalf("base file hidden by layer: %v", err)
	}
	if n, _ := b.Node("/newdir"); n.IsDir {
		t.Fatal("mkdir leaked to the base namespace")
	}

	// whiteout hides the base file and records a layer entry
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/base.txt"}); err != nil {
		t.Fatal(err)
	}
	if _, err := get(t, ch, "/base.txt"); err == nil {
		t.Fatal("whiteout did not hide the base file")
	}
	if n, _ := b.Node("/base.txt"); n.Data == nil {
		t.Fatal("whiteout deleted the base file")
	}
	log := b.LayerLog("L1")
	if len(log) != 2 || log[0].Op != "mkdir" || log[1].Op != "whiteout" {
		t.Fatalf("layer log = %+v", log)
	}
}

func TestOverlayFileWriteReadAndChmod(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newStack(t, b, "")
	ctx := context.Background()

	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpWrite, Path: "/ov.txt", Offset: 0, Data: []byte("hello "), Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpWrite, Path: "/ov.txt", Offset: 6, Data: []byte("layer")}); err != nil {
		t.Fatal(err)
	}
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/ov.txt"})
	if err != nil || string(res.Data) != "hello layer" {
		t.Fatalf("overlay read = %q %v", res.Data, err)
	}
	if n, ok := b.Node("/ov.txt"); ok && n.Data != nil {
		t.Fatal("overlay write leaked into the base namespace")
	}

	// chmod publishes through the layer log and updates the overlay view
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/ov.txt", Mode: 0o600, Size: -1}); err != nil {
		t.Fatal(err)
	}
	if attr, err := get(t, ch, "/ov.txt"); err != nil || attr.Mode != 0o600 {
		t.Fatalf("chmod attr = %+v %v", attr, err)
	}
	found := false
	for _, e := range b.LayerLog("L1") {
		if e.Op == "chmod" && e.Path == "/ov.txt" && e.Mode == 0o600 {
			found = true
		}
	}
	if !found {
		t.Fatal("chmod not recorded in the layer log")
	}
}

func TestReplayRestoresOverlayAcrossRemount(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	defer ts.Close()
	cl := client.New(ts.URL, "")

	l1, err := New(Options{LayerRef: "L1", Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch1, _ := fs.NewChain(l1, terminal)
	ctx := context.Background()
	if _, err := ch1.Dispatch(ctx, &fs.Op{Kind: fs.OpMkdir, Path: "/d", Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	if _, err := ch1.Dispatch(ctx, &fs.Op{Kind: fs.OpWrite, Path: "/d/f", Offset: 0, Data: []byte("persisted"), Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	if _, err := ch1.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/gone"}); err != nil {
		t.Fatal(err)
	}

	// A fresh layer instance (remount) replays the same log.
	l2, err := New(Options{LayerRef: "L1", Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	if err := l2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	ch2, _ := fs.NewChain(l2, terminal)
	if attr, err := get(t, ch2, "/d"); err != nil || !attr.IsDir() {
		t.Fatalf("replayed dir = %+v %v", attr, err)
	}
	res, err := ch2.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/d/f"})
	if err != nil || string(res.Data) != "persisted" {
		t.Fatalf("replayed content = %q %v", res.Data, err)
	}
	b.Seed("/gone", []byte("back"))
	if _, err := get(t, ch2, "/gone"); err == nil {
		t.Fatal("replayed whiteout did not hide the base file")
	}
}

func TestRollbackClearsOverlayAndFailsWrites(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/base.txt", []byte("base"))
	ch, l := newStack(t, b, "")
	ctx := context.Background()

	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpWrite, Path: "/ov.txt", Offset: 0, Data: []byte("x"), Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	l.Rollback()

	// The overlay disappears: the base view returns, writes go ESTALE.
	if attr, err := get(t, ch, "/base.txt"); err != nil || attr.Size != 4 {
		t.Fatalf("base after rollback = %+v %v", attr, err)
	}
	if _, err := get(t, ch, "/ov.txt"); err == nil {
		t.Fatal("overlay survived rollback")
	}
	_, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpWrite, Path: "/ov.txt", Offset: 0, Data: []byte("y"), Mode: 0o644})
	if err == nil {
		t.Fatal("write on abandoned layer succeeded")
	}
}

func TestCheckpointMountIsReadOnly(t *testing.T) {
	b := testbackend.NewBackend()
	b.SetCheckpoint("cp-1", 0)
	ch, _ := newStack(t, b, "cp-1")
	_, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpMkdir, Path: "/d", Mode: 0o755})
	if err != syscall.EROFS {
		t.Fatalf("checkpoint write = %v, want EROFS", err)
	}
}

func TestRenameMovesOverlayContent(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newStack(t, b, "")
	ctx := context.Background()

	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpWrite, Path: "/a", Offset: 0, Data: []byte("moves"), Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/a", Path2: "/b"}); err != nil {
		t.Fatal(err)
	}
	if _, err := get(t, ch, "/a"); err == nil {
		t.Fatal("old overlay path still visible after rename")
	}
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/b"})
	if err != nil || string(res.Data) != "moves" {
		t.Fatalf("renamed content = %q %v", res.Data, err)
	}
	ops := map[string]int{}
	for _, e := range b.LayerLog("L1") {
		ops[e.Op]++
	}
	if ops["whiteout"] != 1 {
		t.Fatalf("rename whiteouts = %d, want 1", ops["whiteout"])
	}
}

// v1 StartLayerEventWatcher rows: another client's layer write propagates
// within a poll interval (overlay replays), and a rollback event clears the
// overlay and abandons the write path.
func TestLayerEventWatcherPropagatesWritesAndRollback(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	defer ts.Close()
	cl := client.New(ts.URL, "")
	l, err := New(Options{LayerRef: "ly", RemoteRoot: "/", Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Stop(context.Background()) })
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(l, terminal)
	if err != nil {
		t.Fatal(err)
	}

	// A remote client upserts an overlay entry through the layer log.
	entry := map[string]any{"path": "/remote.txt", "op": "upsert", "kind": "file", "mode": 0o644, "content_text": "from-remote"}
	body, _ := json.Marshal(entry)
	resp, err := http.Post(ts.URL+"/v1/layers/ly/entries", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := get(t, ch, "/remote.txt"); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := get(t, ch, "/remote.txt"); err != nil {
		t.Fatalf("remote layer write did not propagate: %v", err)
	}

	// A rollback event clears the overlay and fails later writes (ESTALE).
	b.AppendLayerEvent("ly", "rollback")
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := get(t, ch, "/remote.txt"); err != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := get(t, ch, "/remote.txt"); err == nil {
		t.Fatal("rollback did not clear the overlay")
	}
}

// v1 TestLayerRefreshReadsExternalMutation (update row), adapted: the event
// watcher's refresh replays the layer log so a remote client's newer upsert
// supersedes the locally served overlay content.
func TestLayerRefreshReadsExternalMutation(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	defer ts.Close()
	cl := client.New(ts.URL, "")
	l, err := New(Options{LayerRef: "L1", RemoteRoot: "/", Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Stop(context.Background()) })
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(l, terminal)
	if err != nil {
		t.Fatal(err)
	}

	// Remote client upserts v1, then supersedes it with v2.
	for _, content := range []string{"old", "new"} {
		entry := map[string]any{"path": "/a", "op": "upsert", "kind": "file", "mode": 0o644, "content_text": content}
		body, _ := json.Marshal(entry)
		resp, err := http.Post(ts.URL+"/v1/layers/L1/entries", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpRead, Path: "/a"})
		if err == nil && string(res.Data) == "new" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpRead, Path: "/a"})
	if err != nil {
		t.Fatal(err)
	}
	if string(res.Data) != "new" {
		t.Fatalf("refresh served %q, want new (superseded replay)", res.Data)
	}
}

// Writes on a claimed layer file buffer locally: nothing reaches the layer
// log until a durability boundary, and the boundary lands ONE entry with
// the merged image.
func TestLayerBufferedWriteCommitsOnceAtFlush(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newStack(t, b, "")
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/buf.txt", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	h, ok := res.Handle.(*layerHandle)
	if !ok {
		t.Fatalf("open not claimed by the layer write plane: %T", res.Handle)
	}
	for i, chunk := range []string{"one ", "two ", "three"} {
		if err := h.Write(ctx, int64(i*4), []byte(chunk)); err != nil {
			t.Fatal(err)
		}
	}
	if log := b.LayerLog("L1"); len(log) != 0 {
		t.Fatalf("writes reached the layer log before the boundary: %d entries", len(log))
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	log := b.LayerLog("L1")
	if len(log) != 1 || log[0].Op != "upsert" {
		t.Fatalf("layer log after flush = %+v, want one upsert", log)
	}
	if string(log[0].Content) != "one two three" {
		t.Fatalf("entry content = %q, want the merged image", log[0].Content)
	}
	if n, _ := b.Node("/buf.txt"); n.Data != nil {
		t.Fatal("layer write leaked into the base namespace")
	}
}

// A write-sync mount commits every write as its own layer entry.
func TestLayerWriteSyncCommitsPerWrite(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	l, err := New(Options{LayerRef: "L1", Client: cl, Policy: "write-sync"})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(l, terminal)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/s.txt", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle.(*layerHandle)
	if err := h.Write(ctx, 0, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(ctx, 1, []byte("b")); err != nil {
		t.Fatal(err)
	}
	log := b.LayerLog("L1")
	if len(log) != 2 {
		t.Fatalf("write-sync entries = %d, want 2 (one per write)", len(log))
	}
}

// An image beyond the inline threshold streams as an object upload; a
// fresh mount restores it through the object stream (the S3-stored entry
// path of the replay).
func TestLayerLargeImageStreamsAndRestores(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	l, err := New(Options{LayerRef: "L1", Client: cl, InlineMax: 16})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(l, terminal)
	ctx := context.Background()

	image := bytes.Repeat([]byte("z"), 64)
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/big.bin", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle.(*layerHandle)
	if err := h.Write(ctx, 0, image); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	log := b.LayerLog("L1")
	if len(log) != 1 || log[0].StorageType != "s3" {
		t.Fatalf("large commit did not stream as an object: %+v", log[0])
	}

	// A fresh mount replays the S3-stored entry through the object stream.
	l2, err := New(Options{LayerRef: "L1", Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	if err := l2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	terminal2, _ := core.New(cl, core.Options{})
	ch2, _ := fs.NewChain(l2, terminal2)
	rres, rerr := ch2.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/big.bin"})
	if rerr != nil {
		t.Fatal(rerr)
	}
	if !bytes.Equal(rres.Data, image) {
		t.Fatalf("restored image = %d bytes, want %d", len(rres.Data), len(image))
	}
}

// Unlink of an open claimed file whiteouts the name immediately; the open
// handle keeps serving its image and never publishes.
func TestLayerUnlinkWhileOpenKeepsHandleImage(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newStack(t, b, "")
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/u.txt", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle.(*layerHandle)
	if err := h.Write(ctx, 0, []byte("image")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/u.txt"}); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/u.txt"}); !errors.Is(err, fs.ErrNotFound) {
		t.Fatalf("unlinked layer file still visible: %v", err)
	}
	if log := b.LayerLog("L1"); len(log) != 1 || log[0].Op != "whiteout" {
		t.Fatalf("layer log = %+v, want the unlink whiteout only", log)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if log := b.LayerLog("L1"); len(log) != 1 {
		t.Fatalf("close published after unlink: %+v", log)
	}
}
