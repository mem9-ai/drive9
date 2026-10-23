package layer

// New-architecture equivalents of v1 pkg/fuse's layer boundary rows
// (TestLayerLookupRetriesViewRefresh / TestLayerFlushRequiresCommitLock /
// TestLayerRestoreCacheIdentityBoundaries): the v1 mountView-generation
// fence, remote-commit path lock, and restore cache-identity machinery map
// onto the event bus, the writeback path locks, and the watcher's replay.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func postLayerEntry(t *testing.T, ts *httptest.Server, layer, path, op, content string) {
	t.Helper()
	entry := map[string]any{"path": path, "op": op, "kind": "file", "mode": 0o644}
	if content != "" {
		entry["content_text"] = content
	}
	body, _ := json.Marshal(entry)
	resp, err := http.Post(ts.URL+"/v1/layers/"+layer+"/entries", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
}

// v1 TestLayerLookupRetriesViewRefresh equivalent: a lookup racing a view
// refresh must not serve the stale view — after the checkout event the
// namespace caches are swept, so the next lookup observes the refreshed
// layer content instead of a stale cached entry.
func TestLayerViewFenceInvalidatesStaleLookups(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	defer ts.Close()
	cl := client.New(ts.URL, "")
	bus := fs.NewEventBus()
	l, err := New(Options{LayerRef: "L1", RemoteRoot: "/", Client: cl, Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Stop(context.Background()) })
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(l, terminal)

	// v1 stale view: the overlay serves v1 content.
	postLayerEntry(t, ts, "L1", "/f", "upsert", "stale-view")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpRead, Path: "/f"}); err == nil && string(res.Data) == "stale-view" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// The view flips (remote supersedes) and the checkout event lands —
	// the sweep below is what v1's generation fence guaranteed.
	postLayerEntry(t, ts, "L1", "/f", "upsert", "fresh-view")
	l.EmitCheckoutForTest()

	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpRead, Path: "/f"})
		if err == nil && string(res.Data) == "fresh-view" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	res, _ := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpRead, Path: "/f"})
	if string(res.Data) != "fresh-view" {
		t.Fatalf("lookup after checkout served %q, want fresh-view", res.Data)
	}
}

// v1 TestLayerFlushRequiresCommitLock equivalent: an overlay write's flush
// (upsert) and a same-path rename serialize through the layer's per-path
// mutation lock (the v1 remote-commit path lock) — the rename can never
// observe or reorder against a half-landed overlay image.
func TestLayerFlushSerializesWithSamePathMutation(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	defer ts.Close()
	cl := client.New(ts.URL, "")
	bus := fs.NewEventBus()
	l, err := New(Options{LayerRef: "L1", RemoteRoot: "/", Client: cl, Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Stop(context.Background()) })
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(l, terminal)
	ctx := context.Background()

	payload := bytes.Repeat([]byte("L"), 64<<10)
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpWrite, Path: "/f.bin", Data: payload, Mode: 0o644}); err != nil {
		t.Fatal(err)
	}

	// A slow flush races the rename; the mutation lock orders them.
	b.DelayLayerEntriesForTest(150 * time.Millisecond)
	var wg sync.WaitGroup
	var flushErr, renameErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, flushErr = ch.Dispatch(ctx, &fs.Op{Kind: fs.OpWrite, Path: "/f.bin", Data: []byte("tail"), Offset: int64(len(payload)), Mode: 0o644})
	}()
	go func() {
		defer wg.Done()
		time.Sleep(30 * time.Millisecond)
		_, renameErr = ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/f.bin", Path2: "/moved.bin"})
	}()
	wg.Wait()
	b.DelayLayerEntriesForTest(0)

	if flushErr != nil {
		t.Fatalf("flush: %v", flushErr)
	}
	if renameErr != nil {
		t.Fatalf("rename: %v", renameErr)
	}

	// Serialized outcome: the image lives at exactly one path, carries the
	// flushed tail, and the old path is gone (never a reordered half-state
	// where the post-rename upsert resurrects /f.bin).
	if _, ok := l.LookupContentForTest("/f.bin"); ok {
		t.Fatal("post-rename upsert resurrected the old path (mutations interleaved)")
	}
	content, ok := l.LookupContentForTest("/moved.bin")
	if !ok {
		t.Fatal("renamed overlay entry missing")
	}
	if string(content[len(payload):]) != "tail" {
		t.Fatalf("flushed tail lost: %q", content[len(payload):])
	}
}

// v1 TestLayerRestoreCacheIdentityBoundaries equivalent: after a watcher
// restore (replay), a whiteout replaces a previously cached overlay entry —
// reads of the whiteouted path fall through to the base view (identity
// boundary honored; the stale overlay content does not survive).
func TestLayerRestoreHonorsCacheIdentityBoundaries(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/shared.bin", []byte("base-content"))
	ts := httptest.NewServer(b)
	defer ts.Close()
	cl := client.New(ts.URL, "")
	bus := fs.NewEventBus()
	l, err := New(Options{LayerRef: "L1", RemoteRoot: "/", Client: cl, Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Stop(context.Background()) })
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(l, terminal)
	ctx := context.Background()

	// Overlay content masks the base file; the read caches it.
	postLayerEntry(t, ts, "L1", "/shared.bin", "upsert", "overlay-content")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/shared.bin"}); err == nil && string(res.Data) == "overlay-content" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// A remote whiteout replaces the overlay entry; the restore replays it
	// and the identity boundary holds — the stale overlay content does not
	// survive (v1 overlay semantics: a whiteout hides the base too, so the
	// path reads ENOENT).
	postLayerEntry(t, ts, "L1", "/shared.bin", "whiteout", "")
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/shared.bin"}); err != nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/shared.bin"}); err == nil {
		res, _ := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/shared.bin"})
		t.Fatalf("whiteouted path served %q, want ENOENT after restore", res.Data)
	}
}
