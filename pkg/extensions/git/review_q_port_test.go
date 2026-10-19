package git

// qiffang fourteenth round: a stale overlay-upload completion must not
// retire writes acknowledged while the upload was in flight (the handle
// mutex is released across putOverlay).

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// Blocking the upload of AAAA, writing BBBB through the same handle, then
// completing the upload and syncing/releasing must persist BBBB — the
// completion's captured generation no longer matches, so the newer write
// keeps its dirty state and its own boundary lands it.
func TestPortStaleOverlayCompletionKeepsNewerWrite(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/repo/f.txt", []byte("seed"))
	b.SeedGitWorkspace("ws-1", "/repo")

	gate := make(chan struct{})
	var hits atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "overlay") &&
			(r.Method == http.MethodPut || r.Method == http.MethodPost) &&
			hits.Add(1) == 1 {
			// Hold the FIRST overlay upsert (the AAAA snapshot) in flight.
			<-gate
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	g, err := New(Options{Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	g.armed = true
	if err := g.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Stop(context.Background()) })
	terminal, _ := core.New(cl, core.Options{})
	ch, err := fs.NewChain(g, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/repo/f.txt", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if err := h.Write(ctx, 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	syncErr := make(chan error, 1)
	go func() { syncErr <- h.Sync(ctx) }()
	// Wait until the AAAA upload is parked inside putOverlay.
	deadline := time.Now().Add(5 * time.Second)
	for hits.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if hits.Load() == 0 {
		t.Fatal("overlay upload never started")
	}
	// The newer write is acknowledged while the upload holds no mutex.
	if err := h.Write(ctx, 0, []byte("BBBB")); err != nil {
		t.Fatal(err)
	}
	close(gate)
	if err := <-syncErr; err != nil {
		t.Fatalf("first sync: %v", err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatalf("second sync: %v", err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}

	rd, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/repo/f.txt", Offset: 0, Limit: 8})
	if err != nil {
		t.Fatal(err)
	}
	if string(rd.Data) != "BBBB" {
		t.Fatalf("persisted overlay = %q, want BBBB", rd.Data)
	}
}

// The Drain-flush ordering: an older snapshot's PUT must never overtake
// a newer commit's — publication is serialized per workspace, so a Fsync
// racing a parked Drain flush lands AFTER it and the persisted overlay
// keeps the newer bytes.
func TestPortOutOfOrderUploadsKeepNewerImage(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/repo/f.txt", []byte("seed"))
	b.SeedGitWorkspace("ws-1", "/repo")

	gate := make(chan struct{})
	var hits atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "overlay") &&
			(r.Method == http.MethodPut || r.Method == http.MethodPost) &&
			hits.Add(1) == 1 {
			<-gate
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	g, err := New(Options{Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	g.armed = true
	if err := g.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Stop(context.Background()) })
	terminal, _ := core.New(cl, core.Options{})
	ch, err := fs.NewChain(g, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/repo/f.txt", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if err := h.Write(ctx, 0, []byte("AAAAA")); err != nil {
		t.Fatal(err)
	}
	firstErr := make(chan error, 1)
	go func() { firstErr <- h.Sync(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for hits.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if hits.Load() == 0 {
		t.Fatal("first overlay upload never started")
	}
	// The newer write AND its own sync start while the older PUT parks.
	if err := h.Write(ctx, 0, []byte("BBBBB")); err != nil {
		t.Fatal(err)
	}
	secondErr := make(chan error, 1)
	go func() { secondErr <- h.Sync(ctx) }()
	time.Sleep(50 * time.Millisecond)
	close(gate)
	if err := <-firstErr; err != nil {
		t.Fatalf("first sync: %v", err)
	}
	if err := <-secondErr; err != nil {
		t.Fatalf("second sync: %v", err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	rd, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/repo/f.txt", Offset: 0, Limit: 8})
	if err != nil {
		t.Fatal(err)
	}
	if string(rd.Data) != "BBBBB" {
		t.Fatalf("persisted overlay = %q, want BBBBB (newer upload overtook)", rd.Data)
	}
}
