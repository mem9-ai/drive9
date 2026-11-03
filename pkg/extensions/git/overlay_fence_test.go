package git

// #1029 review round: the overlay refresh fence must protect per-entry
// mutations — an OVERWRITE of an existing key (or its deletion) that lands
// while ListGitOverlayEntries is in flight wins over the fetched response,
// which predates it. Deterministic: the fetch's response is captured on
// request receipt and delivered only after the local overwrite.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func TestOverlayRefreshPreservesInFlightOverwrite(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/repo/f", []byte("base"))
	b.Seed("/repo/gone.txt", []byte("gone"))
	b.SeedGitWorkspace("ws-1", "/repo")

	release := make(chan struct{})
	var once sync.Once
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/overlay") {
			// Capture the pre-write log the moment the request arrives;
			// deliver it only after the local overwrite has landed.
			rec := httptest.NewRecorder()
			b.ServeHTTP(rec, r)
			<-release
			for k, vs := range rec.Header() {
				for _, v := range vs {
					w.Header().Add(k, v)
				}
			}
			w.WriteHeader(rec.Code)
			_, _ = w.Write(rec.Body.Bytes())
			return
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(func() { once.Do(func() { close(release) }); ts.Close() })

	g, cl, _ := newGitExt(t, b, "")
	g.armed = true
	if err := g.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(g, terminal)
	ctx := context.Background()

	// Prime the workspace and the mirror with the first write.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpWrite, Path: "/repo/f", Offset: 0, Data: []byte("AAAA")}); err != nil {
		t.Fatal(err)
	}

	reloadDone := make(chan struct{}, 1)
	go func() { g.Reload(ctx); close(reloadDone) }()

	// The reload's overlay fetch is now parked on the gate holding the
	// AAAA-era response. Overwrite the same key and delete another.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpWrite, Path: "/repo/f", Offset: 0, Data: []byte("BBBB")}); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/repo/gone.txt"}); err != nil {
		t.Fatal(err)
	}
	once.Do(func() { close(release) })
	select {
	case <-reloadDone:
	case <-time.After(10 * time.Second):
		t.Fatal("reload did not finish after gate release")
	}

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/repo/f", Offset: 0, Limit: 4})
	if err != nil {
		t.Fatal(err)
	}
	if string(res.Data) != "BBBB" {
		t.Fatalf("post-reload overlay read = %q, want the acknowledged BBBB overwrite", string(res.Data))
	}
	list, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpList, Path: "/repo"})
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range list.Entries {
		if e.Name == "gone.txt" {
			t.Fatal("in-flight deletion resurrected by the fetched overlay response")
		}
	}
}
