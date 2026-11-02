package appendlog

// Ported from v1 pkg/fuse/append_log_finalize_test.go: finalization and
// retry semantics across the three transports — content lands before the
// deferred chmod, Release retries only the pending mode, content failures
// never retry the mode, and the kernel-notify assertions are retargeted to
// the events bus (fs.EventLanded fires only after a fully finalized commit).

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// newStackWithEvents is newStack with an EventBus wired into the extension.
func newStackWithEvents(t *testing.T, b *testbackend.Backend, patterns ...string) (*fs.Chain, *fs.EventBus) {
	t.Helper()
	return newEventStack(t, b, patterns...)
}

// newEventStack builds a chain over an arbitrary root handler with an
// EventBus wired into the extension.
func newEventStack(t *testing.T, root http.Handler, patterns ...string) (*fs.Chain, *fs.EventBus) {
	t.Helper()
	ts := httptest.NewServer(root)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	events := fs.NewEventBus()
	ext, err := New(Options{Patterns: patterns, Client: cl, Events: events})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(ext, terminal)
	if err != nil {
		t.Fatal(err)
	}
	return ch, events
}

// landedCollector records every EventLanded delivery.
type landedCollector struct {
	mu   sync.Mutex
	evts []fs.Event
}

func (c *landedCollector) attach(bus *fs.EventBus) func() {
	return bus.On(fs.EventLanded, func(e fs.Event) {
		c.mu.Lock()
		c.evts = append(c.evts, e)
		c.mu.Unlock()
	})
}

func (c *landedCollector) snapshot() []fs.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]fs.Event(nil), c.evts...)
}

// finalizeRoute stages the dirty generation that routes each transport:
// "append" (tail-safe write), "rewrite" (non-tail overwrite), "reset"
// (salt-proved WAL rotation over an append_log object).
func finalizeRoute(t *testing.T, b *testbackend.Backend, ch *fs.Chain, route string) *Handle {
	t.Helper()
	ctx := context.Background()
	switch route {
	case "reset":
		seed := append(makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2), bytes.Repeat([]byte("o"), 32)...)
		b.SeedWithLayout("/configured.log", seed, "append_log")
		h := openLog(t, ch, "/configured.log").(*Handle)
		if err := h.Write(ctx, 0, makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)); err != nil {
			t.Fatal(err)
		}
		return h
	case "rewrite":
		b.Seed("/configured.log", []byte("pre"))
		h := openLog(t, ch, "/configured.log").(*Handle)
		if err := h.Write(ctx, 0, []byte("rewrite")); err != nil {
			t.Fatal(err)
		}
		return h
	default: // append
		b.Seed("/configured.log", []byte("pre"))
		h := openLog(t, ch, "/configured.log").(*Handle)
		if err := h.Write(ctx, 3, []byte("tail")); err != nil {
			t.Fatal(err)
		}
		return h
	}
}

// chmodGate counts chmod calls and fails the first failFirst of them (or all
// of them when persistent).
type chmodGate struct {
	mu         sync.Mutex
	calls      int
	failFirst  int
	persistent bool
}

func (g *chmodGate) intercept(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodPost || !r.URL.Query().Has("chmod") {
		return false
	}
	g.mu.Lock()
	g.calls++
	fail := g.persistent || g.calls <= g.failFirst
	g.mu.Unlock()
	if fail {
		w.WriteHeader(http.StatusInternalServerError)
		return true
	}
	return false
}

func (g *chmodGate) count() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls
}

// v1 TestAppendLogReleaseRetriesOnlyPendingMode: the deferred mode rides the
// commit — a transient chmod failure retries at Release without re-uploading
// content; a persistent one leaves the mode pending and the content landed.
func TestPortAppendLogReleaseRetriesOnlyPendingMode(t *testing.T) {
	for _, route := range []string{"append", "rewrite", "reset"} {
		for _, persistent := range []bool{false, true} {
			name := route + "/transient"
			if persistent {
				name = route + "/persistent"
			}
			t.Run(name, func(t *testing.T) {
				b := testbackend.NewBackend()
				var mu sync.Mutex
				contentCalls := 0
				gate := &chmodGate{failFirst: 1, persistent: persistent}
				ch := newWrappedStack(t, wrapBackend(b, func(w http.ResponseWriter, r *http.Request) bool {
					if gate.intercept(w, r) {
						return true
					}
					if (r.Method == http.MethodPost && r.URL.Query().Has("append-log")) ||
						(r.Method == http.MethodPut && fsPath(r.URL.Path)) {
						mu.Lock()
						contentCalls++
						mu.Unlock()
					}
					return false
				}), "**/configured.log")
				ctx := context.Background()

				h := finalizeRoute(t, b, ch, route)
				setattrMode(t, ch, "/configured.log", 0o600)
				if err := h.Flush(ctx); err == nil {
					t.Fatal("first flush must fail on the chmod")
				}
				err := h.Release(ctx)
				if persistent && err == nil {
					t.Fatal("persistent mode failure must fail the release")
				} else if !persistent && err != nil {
					t.Fatalf("transient mode retry: %v", err)
				}
				mu.Lock()
				defer mu.Unlock()
				if contentCalls != 1 {
					t.Fatalf("content calls = %d, want 1 (never re-uploaded for a mode retry)", contentCalls)
				}
				if got := gate.count(); got != 2 {
					t.Fatalf("chmod calls = %d, want 2 (flush attempt plus release retry)", got)
				}
				if h.hasPendingMode != persistent {
					t.Fatalf("pending mode = %t, want %t", h.hasPendingMode, persistent)
				}
				if h.dirty {
					t.Fatal("content finalization must clear dirty state")
				}
			})
		}
	}
}

// v1 TestAppendLogReleaseDoesNotRetryModeAfterContentFailure: when the
// content itself fails, Release surfaces the error without ever touching the
// deferred mode, and the dirty generation survives for the next attempt.
func TestPortAppendLogReleaseDoesNotRetryModeAfterContentFailure(t *testing.T) {
	for _, route := range []string{"append", "rewrite", "reset"} {
		t.Run(route, func(t *testing.T) {
			b := testbackend.NewBackend()
			gate := &chmodGate{}
			ch := newWrappedStack(t, wrapBackend(b, func(w http.ResponseWriter, r *http.Request) bool {
				if (r.Method == http.MethodPost && (r.URL.Query().Has("append-log") || r.URL.Query().Has("chmod"))) ||
					(r.Method == http.MethodPut && fsPath(r.URL.Path)) {
					_ = gate.intercept(w, r) // count, but everything fails below anyway
					w.WriteHeader(http.StatusInternalServerError)
					return true
				}
				return false
			}), "**/configured.log")
			ctx := context.Background()

			h := finalizeRoute(t, b, ch, route)
			setattrMode(t, ch, "/configured.log", 0o600)
			if err := h.Release(ctx); err == nil {
				t.Fatal("release with failing content must error")
			}
			if got := gate.count(); got != 0 {
				t.Fatalf("chmod calls = %d, want 0 (mode only rides a landed commit)", got)
			}
			if !h.dirty || !h.buf.HasDirtyParts() {
				t.Fatal("failed content must preserve the dirty generation")
			}
			if !h.hasPendingMode {
				t.Fatal("pending mode must survive a content failure")
			}
		})
	}
}

// v1 publication kernel-notify rows, retargeted to the events bus: the
// append and rewrite transports each publish exactly one EventLanded
// carrying the committed revision and size.
func TestPortAppendLogCommitEmitsLandedEventPerTransport(t *testing.T) {
	for _, route := range []string{"append", "rewrite"} {
		t.Run(route, func(t *testing.T) {
			b := testbackend.NewBackend()
			ch, events := newStackWithEvents(t, b, "**/configured.log")
			collector := &landedCollector{}
			detach := collector.attach(events)
			defer detach()

			h := finalizeRoute(t, b, ch, route)
			if err := h.Flush(context.Background()); err != nil {
				t.Fatal(err)
			}
			node, _ := b.Node("/configured.log")
			wantData, wantSize := "pretail", int64(len("pretail"))
			if route == "rewrite" {
				wantData, wantSize = "rewrite", int64(len("rewrite"))
			}
			if string(node.Data) != wantData {
				t.Fatalf("backend = %q, want %q", node.Data, wantData)
			}
			evts := collector.snapshot()
			if len(evts) != 1 {
				t.Fatalf("landed events = %d, want 1", len(evts))
			}
			if evts[0].Path != "/configured.log" || evts[0].Rev != node.Rev || evts[0].Size != wantSize {
				t.Fatalf("event = %+v, want path/rev/size %s/%d/%d", evts[0], "/configured.log", node.Rev, wantSize)
			}
		})
	}
}

// v1 TestAppendLogGenerationResetNotifiesKernelAfterCommit, retargeted to
// the events bus: a generation reset announces its landing only when the
// whole finalization (reset PUT plus deferred mode) succeeded.
func TestPortAppendLogGenerationResetLandedEventDependsOnFinalize(t *testing.T) {
	for _, test := range []struct {
		name     string
		failPut  bool
		failMode bool
		wantEvt  int
	}{
		{name: "success", wantEvt: 1},
		{name: "put-failure", failPut: true, wantEvt: 0},
		{name: "post-commit-mode-failure", failMode: true, wantEvt: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := testbackend.NewBackend()
			gate := &chmodGate{persistent: true}
			faultyChain, events := newEventStack(t, wrapBackend(b, func(w http.ResponseWriter, r *http.Request) bool {
				if test.failPut && r.Method == http.MethodPut && fsPath(r.URL.Path) {
					w.WriteHeader(http.StatusInternalServerError)
					return true
				}
				return gate.intercept(w, r)
			}), "**/configured.log")
			collector := &landedCollector{}
			detach := collector.attach(events)
			defer detach()

			h := finalizeRoute(t, b, faultyChain, "reset")
			if test.failMode {
				setattrMode(t, faultyChain, "/configured.log", 0o600)
			}
			err := h.Flush(context.Background())
			if (err == nil) != (test.wantEvt == 1) {
				t.Fatalf("flush error = %v, want ok=%t", err, test.wantEvt == 1)
			}
			if got := len(collector.snapshot()); got != test.wantEvt {
				t.Fatalf("landed events = %d, want %d", got, test.wantEvt)
			}
			if test.failPut && (!h.dirty || h.baseRev != 1) {
				t.Fatalf("failed reset lost dirty state: dirty=%t rev=%d", h.dirty, h.baseRev)
			}
		})
	}
}

// Supplemental row: the whole-image truncate of a handle-less path also
// announces its landing on the events bus (v1 path-truncate notification).
func TestPortAppendLogPathTruncateEmitsLandedEvent(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedWithLayout("/configured.log", []byte("abc"), "append_log")
	ch, events := newStackWithEvents(t, b, "**/configured.log")
	collector := &landedCollector{}
	detach := collector.attach(events)
	defer detach()

	if _, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Path: "/configured.log", Size: 0}); err != nil {
		t.Fatal(err)
	}
	evts := collector.snapshot()
	if len(evts) != 1 {
		t.Fatalf("landed events = %d, want 1", len(evts))
	}
	if evts[0].Size != 0 || evts[0].Rev == 0 {
		t.Fatalf("event = %+v, want size 0 and a positive revision", evts[0])
	}
}
