package layer

// Review follow-ups (third round): (1) a large base-backed truncate must
// publish its logical size and stay readable through the handler — an
// uploaded-but-unmaterialized image is not an empty overlay file; (2) an
// acknowledged truncate bumps the write generation so a drain upload that
// started earlier cannot retire it; (3) a base-only chmod stays visible in
// stat and survives a fresh replay with the base as the content source.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func layerStack(t *testing.T, ts *httptest.Server, inlineMax int64) (*Layer, *fs.Chain) {
	t.Helper()
	cl := client.New(ts.URL, "")
	bus := fs.NewEventBus()
	l, err := New(Options{LayerRef: "L1", RemoteRoot: "/", Client: cl, Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	if inlineMax > 0 {
		l.inlineMaxOverride = inlineMax
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Stop(context.Background()) })
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(l, terminal)
	return l, ch
}

// An 8 MiB → 7 MiB truncate of a base-backed file uploads the retained
// prefix; afterwards GetAttr reports 7 MiB and reads serve the uploaded
// image (the mirror records the logical size and streams the stored
// object, instead of claiming an empty complete overlay file).
func TestPortLargeBaseTruncatePublishesSizeAndReads(t *testing.T) {
	const oldSize = 8 << 20
	const newSize = 7 << 20
	b := testbackend.NewBackend()
	b.Seed("/f", []byte(strings.Repeat("A", oldSize)))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	_, ch := layerStack(t, ts, 64)

	if _, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Path: "/f", Size: newSize}); err != nil {
		t.Fatal(err)
	}
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpGetAttr, Path: "/f"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Attr == nil || res.Attr.Size != newSize {
		t.Fatalf("post-truncate attr = %+v, want size %d", res.Attr, newSize)
	}
	rd, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpRead, Path: "/f", Offset: 0, Limit: 16})
	if err != nil {
		t.Fatal(err)
	}
	if string(rd.Data) != strings.Repeat("A", 16) {
		t.Fatalf("post-truncate read head = %q, want retained A prefix", string(rd.Data))
	}
	tail, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpRead, Path: "/f", Offset: newSize - 4, Limit: 16})
	if err != nil {
		t.Fatal(err)
	}
	if string(tail.Data) != strings.Repeat("A", 4) {
		t.Fatalf("tail read = %q, want the final retained bytes", string(tail.Data))
	}
}

// A truncate acknowledged while an earlier flush's upload is still in
// flight must survive: the flush cannot retire the dirty flag afterwards,
// and a second flush persists the truncated image.
func TestPortTruncateRacesDrainUpload(t *testing.T) {
	b := testbackend.NewBackend()
	release := make(chan struct{})
	var once sync.Once
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/v1/layers/") {
			once.Do(func() { <-release })
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(func() { once.Do(func() { close(release) }); ts.Close() })
	_, ch := layerStack(t, ts, 0)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_WRONLY | syscall.O_CREAT), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle.(interface {
		Write(context.Context, int64, []byte) error
		Truncate(int64) error
		Flush(context.Context) error
	})
	if err := h.Write(ctx, 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	flushed := make(chan error, 1)
	go func() { flushed <- h.Flush(ctx) }()
	// Let the flush reach its (parked) upload, then truncate the same
	// descriptor — the acknowledged mutation must fence the completion.
	time.Sleep(100 * time.Millisecond)
	if err := h.Truncate(2); err != nil {
		t.Fatal(err)
	}
	once.Do(func() { close(release) })
	if err := <-flushed; err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/f"})
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Data) != "AA" {
		t.Fatalf("persisted image = %q, want the acknowledged 2-byte truncation", string(got.Data))
	}
}

// A base-only chmod(0600) stays visible in GetAttr, and a fresh stack
// replaying the log keeps the base as the content source (reads return
// the base content, not an empty overlay image).
func TestPortBaseOnlyChmodVisibleAndReplayable(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("important content"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	_, ch := layerStack(t, ts, 0)
	ctx := context.Background()

	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/f", Mode: 0o600}); err != nil {
		t.Fatal(err)
	}
	st, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/f"})
	if err != nil {
		t.Fatal(err)
	}
	if st.Attr == nil || st.Attr.Mode&0o777 != 0o600 {
		t.Fatalf("post-chmod attr = %+v, want mode 0600", st.Attr)
	}

	// A fresh stack over the same log replays the chmod record with the
	// base as the content source.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/f"}); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	_, ch2 := layerStack(t, ts, 0)
	sawContent := false
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rd, rerr := ch2.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/f"})
		if rerr == nil && string(rd.Data) == "important content" {
			sawContent = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !sawContent {
		rd, _ := ch2.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/f"})
		t.Fatalf("replayed read = %q, want the base's important content", string(rd.Data))
	}
	st2, err := ch2.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/f"})
	if err != nil {
		t.Fatal(err)
	}
	if st2.Attr == nil || st2.Attr.Mode&0o777 != 0o600 {
		t.Fatalf("replayed attr = %+v, want the chmod'd 0600", st2.Attr)
	}
}
