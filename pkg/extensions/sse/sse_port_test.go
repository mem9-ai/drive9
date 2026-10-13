package sse

// Rows ported from v1 pkg/fuse/sse_test.go: change events invalidate the
// directory namespace of child directories, foreign structural resets sweep
// caches, and heartbeat resets do nothing. The second block drives the same
// mappings through a fake /v1/events stream + client.WatchEventsWithLifecycle.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/dircache"
)

// v1 TestSSEWatcherHandleChangeInvalidatesChildDirNamespace: a change under
// a child directory drops that directory's cached listing.
func TestPortSSEChangeInvalidatesChildDirNamespace(t *testing.T) {
	w, bus, _ := newSSE(t, "my-actor", "/")
	ns := dircache.NewNamespaceCache(0, 0, 0)
	ns.Put("/docs/subdir", []fs.DirEntryInfo{{Name: "stale.txt", Size: 50}})
	invalidate := func(e fs.Event) {
		if ns != nil {
			ns.Invalidate(e.Path)
		}
	}
	bus.On(fs.EventInvalidated, invalidate)
	bus.On(fs.EventDelete, invalidate)

	w.handleEvent(&client.ChangeEvent{Seq: 1, Path: "/docs/subdir", Op: "delete", Actor: "other"}, nil)
	if _, ok := ns.Get("/docs/subdir"); ok {
		t.Error("child dir namespace should be invalidated after directory change")
	}
}

// v1 TestSSEWatcherHandleEventReset + TestSSEWatcherForeignStructuralReset-
// ClearsCache: a foreign reset sweeps every cache and notifies the kernel
// wholesale.
func TestPortSSEForeignResetClearsCaches(t *testing.T) {
	w, _, notify := newSSE(t, "me", "/")

	w.handleEvent(nil, &client.ResetEvent{Reason: "structural_change", Actor: "someone-else"})
	if notify.all == 0 {
		t.Error("foreign structural reset must InvalidateAll")
	}
	if !w.current {
		t.Error("reset must mark the stream current")
	}
}

// v1 TestSSEWatcherSelfFilterSkipsOwnStructuralReset: the mount's own
// structural reset is filtered like its own writes.
func TestPortSSESkipOwnStructuralReset(t *testing.T) {
	w, _, notify := newSSE(t, "me", "/")

	w.handleEvent(nil, &client.ResetEvent{Reason: "structural_change", Actor: "me"})
	if notify.all != 0 {
		t.Error("own structural reset must be self-filtered")
	}
}

// v1 TestSSEWatcherHeartbeatDoesNotReset: empty-reason resets (heartbeats)
// are ignored.
func TestPortSSEHeartbeatDoesNotReset(t *testing.T) {
	w, _, notify := newSSE(t, "me", "/")

	w.handleEvent(nil, &client.ResetEvent{Reason: "", Actor: "other"})
	if notify.all != 0 {
		t.Error("heartbeat reset must not sweep caches")
	}
}

// ---------------------------------------------------------------------------
// Stream-driven rows from v1 pkg/fuse/sse_test.go (d39a6d1b3): watcher wire
// events mapped onto bus events through a fake /v1/events stream and
// client.WatchEventsWithLifecycle — the op-string delete classification, the
// path.Clean of server trailing slashes, reset sweeps, disconnect handling,
// and self/remote-root filtering. The v1 stat-cache verification rows
// (markStatCacheUnverified, initial-sync tagging) and the stderr-escaping row
// ride the facade's mount state / debug logging and have no counterpart here.

// eventLog records bus events across kinds for the stream-driven rows.
type eventLog struct {
	mu     sync.Mutex
	events []fs.Event
}

func (l *eventLog) record(e fs.Event) {
	l.mu.Lock()
	l.events = append(l.events, e)
	l.mu.Unlock()
}

func (l *eventLog) of(kind fs.EventKind) []fs.Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []fs.Event
	for _, e := range l.events {
		if e.Kind == kind {
			out = append(out, e)
		}
	}
	return out
}

func (l *eventLog) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.events)
}

// startStreamSSE spins a fake /v1/events server whose handler streams the
// scripted SSE frames, plus a watcher over a real client. The watcher stops
// with the test.
func startStreamSSE(t *testing.T, actor, root string, serve func(w http.ResponseWriter, r *http.Request, flush func())) (*SSE, *eventLog, *recordingNotifier) {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flush := func() {
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		w.Header().Set("Content-Type", "text/event-stream")
		serve(w, r, flush)
	}))
	t.Cleanup(ts.Close)

	bus := fs.NewEventBus()
	log := &eventLog{}
	notify := &recordingNotifier{}
	w, err := New(Options{
		Client:     client.New(ts.URL, ""),
		Actor:      actor,
		RemoteRoot: root,
		Events:     bus,
		Inodes:     fakeInodes{},
		Notify:     notify,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []fs.EventKind{fs.EventInvalidated, fs.EventDelete, fs.EventCheckout} {
		bus.On(kind, log.record)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Stop(context.Background()) })
	return w, log, notify
}

func writeSSE(t *testing.T, w http.ResponseWriter, flush func(), event string, payload any) {
	t.Helper()
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b)
	flush()
}

func waitForLog(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

// v1 change-event mapping: the op string classifies the bus event — anything
// containing delete/remove (case-insensitive) is EventDelete, everything else
// EventInvalidated — and known inodes get kernel content/dentry
// invalidations.
func TestPortStreamChangeOpClassification(t *testing.T) {
	_, log, notify := startStreamSSE(t, "me", "/", func(hw http.ResponseWriter, hr *http.Request, flush func()) {
		rows := []struct {
			seq uint64
			op  string
		}{
			{1, "write"},
			{2, "DELETE"},
			{3, "FileRemoved"},
			{4, "unlink"},
		}
		for _, r := range rows {
			writeSSE(t, hw, flush, "file_changed", map[string]any{"seq": r.seq, "path": "/sub/f", "op": r.op, "actor": "other"})
		}
		time.Sleep(200 * time.Millisecond)
	})
	if !waitForLog(t, 3*time.Second, func() bool { return log.count() >= 4 }) {
		t.Fatalf("stream changes did not reach the bus: %+v", log.events)
	}
	invalidated := log.of(fs.EventInvalidated)
	if len(invalidated) != 2 || invalidated[0].Path != "/sub/f" || invalidated[1].Path != "/sub/f" {
		t.Fatalf("invalidated events = %+v, want write+unlink rows", invalidated)
	}
	deleted := log.of(fs.EventDelete)
	if len(deleted) != 2 {
		t.Fatalf("delete events = %+v, want DELETE+FileRemoved rows", deleted)
	}
	if content, entries, _ := notify.snapshot(); len(content) != 4 || entries != 4 {
		t.Fatalf("kernel invalidations = content %v entries %d, want one pair per change", content, entries)
	}
}

// The path.Clean fix: server directory paths carry a trailing slash and may
// double up separators; downstream consumers see the cleaned form only.
func TestPortStreamDirectoryTrailingSlashIsCleaned(t *testing.T) {
	_, log, _ := startStreamSSE(t, "me", "/", func(hw http.ResponseWriter, hr *http.Request, flush func()) {
		writeSSE(t, hw, flush, "file_changed", map[string]any{"seq": 1, "path": "/docs/subdir/", "op": "delete", "actor": "other"})
		writeSSE(t, hw, flush, "file_changed", map[string]any{"seq": 2, "path": "//docs//nested//", "op": "write", "actor": "other"})
		time.Sleep(200 * time.Millisecond)
	})
	if !waitForLog(t, 3*time.Second, func() bool { return log.count() >= 2 }) {
		t.Fatalf("cleaned-path changes did not reach the bus: %+v", log.events)
	}
	deleted := log.of(fs.EventDelete)
	if len(deleted) != 1 || deleted[0].Path != "/docs/subdir" {
		t.Fatalf("delete events = %+v, want cleaned /docs/subdir", deleted)
	}
	invalidated := log.of(fs.EventInvalidated)
	if len(invalidated) != 1 || invalidated[0].Path != "/docs/nested" {
		t.Fatalf("invalidated events = %+v, want cleaned /docs/nested", invalidated)
	}
}

// v1 TestSSEWatcherHandleEventReset / ForeignStructuralResetClearsCache,
// streamed: a reset emits EventCheckout for "/" and notifies InvalidateAll.
func TestPortStreamResetEmitsCheckoutAndInvalidatesAll(t *testing.T) {
	_, log, notify := startStreamSSE(t, "me", "/", func(hw http.ResponseWriter, hr *http.Request, flush func()) {
		writeSSE(t, hw, flush, "reset", map[string]any{"seq": 5, "reason": "structural_change", "actor": "someone-else"})
		time.Sleep(200 * time.Millisecond)
	})
	if !waitForLog(t, 3*time.Second, func() bool { return len(log.of(fs.EventCheckout)) >= 1 }) {
		t.Fatal("reset did not emit checkout")
	}
	if got := log.of(fs.EventCheckout)[0]; got.Path != "/" {
		t.Fatalf("checkout path = %q, want /", got.Path)
	}
	if _, _, all := notify.snapshot(); all != 1 {
		t.Fatalf("InvalidateAll calls = %d, want 1", all)
	}
}

// v1 TestSSEWatcherOwnActorNonStructuralResetClearsCache: only structural
// resets are self-filtered; an own-actor reset for another reason still
// sweeps.
func TestPortStreamOwnActorNonStructuralResetSweeps(t *testing.T) {
	_, log, notify := startStreamSSE(t, "me", "/", func(hw http.ResponseWriter, hr *http.Request, flush func()) {
		writeSSE(t, hw, flush, "reset", map[string]any{"seq": 7, "reason": "seq_too_old", "actor": "me"})
		time.Sleep(200 * time.Millisecond)
	})
	if !waitForLog(t, 3*time.Second, func() bool { return len(log.of(fs.EventCheckout)) >= 1 }) {
		t.Fatal("own-actor non-structural reset was wrongly filtered")
	}
	if _, _, all := notify.snapshot(); all != 1 {
		t.Fatalf("InvalidateAll calls = %d, want 1 (own-actor non-structural reset sweeps)", all)
	}
}

// v1 disconnect semantics (OnDisconnected -> markStatCacheUnverified): after
// the stream drops the watcher emits EventCheckout — local caches that would
// mask unobserved remote changes drop — but never kernel-invalidates
// wholesale, and the stream is no longer current.
func TestPortStreamDisconnectEmitsCheckoutWithoutKernelSweep(t *testing.T) {
	var connections atomic.Int32
	_, log, notify := startStreamSSE(t, "me", "/", func(hw http.ResponseWriter, hr *http.Request, flush func()) {
		if connections.Add(1) > 1 {
			// Reconnect attempt: park until the test cancels.
			<-hr.Context().Done()
			return
		}
		writeSSE(t, hw, flush, "heartbeat", map[string]any{"seq": 9})
		// Hold the stream open briefly so the heartbeat lands, then return:
		// the response ends, the watcher sees a clean EOF and reports the
		// disconnect.
		time.Sleep(300 * time.Millisecond)
	})
	if !waitForLog(t, 3*time.Second, func() bool { return len(log.of(fs.EventCheckout)) >= 1 }) {
		t.Fatal("disconnect did not emit checkout")
	}
	if _, _, all := notify.snapshot(); all != 0 {
		t.Fatalf("InvalidateAll calls after disconnect = %d, want 0 (kernel caches stay)", all)
	}
}

// v1 TestSSEWatcherSelfFilterSkipsOwnEvents, streamed end-to-end.
func TestPortStreamSelfFiltersOwnActor(t *testing.T) {
	_, log, _ := startStreamSSE(t, "me", "/", func(hw http.ResponseWriter, hr *http.Request, flush func()) {
		writeSSE(t, hw, flush, "file_changed", map[string]any{"seq": 1, "path": "/sub/f", "op": "write", "actor": "me"})
		writeSSE(t, hw, flush, "file_changed", map[string]any{"seq": 2, "path": "/sub/f", "op": "delete", "actor": "me"})
		time.Sleep(250 * time.Millisecond)
	})
	time.Sleep(250 * time.Millisecond)
	if got := log.count(); got != 0 {
		t.Fatalf("own-actor events reached the bus: %+v", log.events)
	}
}

// v1 TestSSEWatcherHandleChangeFiltersAndRebasesRemoteRoot, streamed: events
// outside the mount's remote root (including prefix siblings) are dropped;
// in-root paths rebase into the mount view.
func TestPortStreamRemoteRootFilteringAndRebase(t *testing.T) {
	_, log, _ := startStreamSSE(t, "", "/tenant-a", func(hw http.ResponseWriter, hr *http.Request, flush func()) {
		writeSSE(t, hw, flush, "file_changed", map[string]any{"seq": 1, "path": "/outside.txt", "op": "write", "actor": "x"})
		writeSSE(t, hw, flush, "file_changed", map[string]any{"seq": 2, "path": "/tenant-a2/docs/x", "op": "write", "actor": "x"})
		writeSSE(t, hw, flush, "file_changed", map[string]any{"seq": 3, "path": "/tenant-a/docs/readme.md", "op": "write", "actor": "x"})
		writeSSE(t, hw, flush, "file_changed", map[string]any{"seq": 4, "path": "/tenant-a", "op": "write", "actor": "x"})
		time.Sleep(250 * time.Millisecond)
	})
	if !waitForLog(t, 3*time.Second, func() bool { return log.count() >= 2 }) {
		t.Fatalf("in-root changes did not reach the bus: %+v", log.events)
	}
	invalidated := log.of(fs.EventInvalidated)
	if len(invalidated) != 2 || invalidated[0].Path != "/docs/readme.md" || invalidated[1].Path != "/" {
		t.Fatalf("invalidated events = %+v, want rebased /docs/readme.md and root", invalidated)
	}
}

// v1 TestSSEWatcherStreamCurrentVerifiesStatCache / heartbeat rows: the
// stream-current heartbeat marker sweeps nothing. (The current flag itself is
// written on the watcher goroutine; asserting it from the test goroutine
// would race, so the ordered assertions stay with the synchronous tests.)
func TestPortStreamHeartbeatDoesNotSweep(t *testing.T) {
	_, log, notify := startStreamSSE(t, "me", "/", func(hw http.ResponseWriter, hr *http.Request, flush func()) {
		writeSSE(t, hw, flush, "heartbeat", map[string]any{"seq": 3})
		time.Sleep(300 * time.Millisecond)
	})
	time.Sleep(200 * time.Millisecond)
	if got := log.count(); got != 0 {
		t.Fatalf("heartbeat emitted %d bus events, want none", got)
	}
	if _, _, all := notify.snapshot(); all != 0 {
		t.Fatalf("InvalidateAll calls = %d, want none", all)
	}
}
