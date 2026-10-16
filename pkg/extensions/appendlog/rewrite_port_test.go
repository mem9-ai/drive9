package appendlog

// Ported from v1 pkg/fuse/append_log_rewrite_test.go: the fullRewrite
// transport's public paths — layout learning, the single-layout generic
// commit (new behavior: B1), EEXIST rejection of baseline drift, failure
// rows preserving dirty state, deferred chmod ordering, the truncate-pinned
// conditional baseline, handle-less path truncates, and concurrent
// mutations racing the in-flight PUT.

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// putRecorder captures PUT bodies and expected revisions to /v1/fs paths.
type putRecorder struct {
	mu   sync.Mutex
	puts []struct {
		path string
		rev  string
		body []byte
	}
}

func (p *putRecorder) intercept(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodPut || !fsPath(r.URL.Path) {
		return false
	}
	body := drainBody(r)
	p.mu.Lock()
	p.puts = append(p.puts, struct {
		path string
		rev  string
		body []byte
	}{r.URL.Path, r.Header.Get("X-Dat9-Expected-Revision"), body})
	p.mu.Unlock()
	return false
}

func (p *putRecorder) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.puts)
}

func (p *putRecorder) snapshot() []struct {
	path string
	rev  string
	body []byte
} {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]struct {
		path string
		rev  string
		body []byte
	}(nil), p.puts...)
}

func fsPath(p string) bool {
	return len(p) > len("/v1/fs") && p[:len("/v1/fs")] == "/v1/fs"
}

// v1 TestAppendLogFullRewriteUsesOneConditionalPUT: one conditional PUT of
// the whole image against the committed revision; the handle adopts the new
// baseline and learns the append_log layout.
func TestPortAppendLogFullRewriteUsesOneConditionalPUT(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/db-wal", []byte("pre"))
	rec := &putRecorder{}
	ch := newWrappedStack(t, wrapBackend(b, rec.intercept), "**/db-wal")
	ctx := context.Background()

	h := openLog(t, ch, "/db-wal").(*Handle)
	if err := h.Write(ctx, 0, []byte("rewrite")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	puts := rec.snapshot()
	if len(puts) != 1 || puts[0].rev != "1" || string(puts[0].body) != "rewrite" {
		t.Fatalf("PUTs = %+v, want one rev=1 body=rewrite", puts)
	}
	if n, _ := b.Node("/db-wal"); string(n.Data) != "rewrite" {
		t.Fatalf("backend = %q", n.Data)
	}
	if h.dirty || h.baseRev != n(b, "/db-wal").Rev {
		t.Fatalf("handle after rewrite = dirty=%t rev=%d", h.dirty, h.baseRev)
	}
	if got := h.layoutAtLocked(h.baseRev, int64(len("rewrite"))); got != client.ContentLayoutAppendLog {
		t.Fatalf("layout after rewrite = %q, want append_log", got)
	}
}

func n(b *testbackend.Backend, path string) testbackend.Node {
	node, _ := b.Node(path)
	return node
}

// v1 TestAppendLogFullRewriteResolvesUnknownLayoutOnce: an unknown layout
// is resolved with exactly one stat. New behavior (B1): an append_log
// layout streams one server-side whole-image PUT, while a single layout
// commits through the generic size-aware path (also one PUT).
func TestPortAppendLogFullRewriteResolvesUnknownLayoutOnce(t *testing.T) {
	for _, test := range []struct {
		name   string
		layout string
	}{
		{name: "append log", layout: "append_log"},
		{name: "single generic commit", layout: "single"},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := testbackend.NewBackend()
			b.SeedWithLayout("/db-wal", []byte("pre"), test.layout)
			rec := &putRecorder{}
			var mu sync.Mutex
			heads := 0
			ch := newWrappedStack(t, wrapBackend(b, func(w http.ResponseWriter, r *http.Request) bool {
				if r.Method == http.MethodHead && fsPath(r.URL.Path) {
					mu.Lock()
					heads++
					mu.Unlock()
				}
				return rec.intercept(w, r)
			}), "**/db-wal")
			ctx := context.Background()

			h := openLog(t, ch, "/db-wal").(*Handle)
			if err := h.Write(ctx, 0, []byte("rewrite")); err != nil {
				t.Fatal(err)
			}
			if err := h.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			flushHeads := heads - 2 // two stats at open
			mu.Unlock()
			if rec.count() != 1 {
				t.Fatalf("PUTs = %d, want 1", rec.count())
			}
			if flushHeads > 2 {
				t.Fatalf("flush HEADs = %d, want at most the layout resolve plus the commit stat", flushHeads)
			}
			if node, _ := b.Node("/db-wal"); string(node.Data) != "rewrite" {
				t.Fatalf("backend = %q", node.Data)
			}
			if h.dirty {
				t.Fatal("rewrite must clear dirty state")
			}
		})
	}
}

// v1 TestAppendLogFullRewriteRejectsMissingLayoutWithoutWrite, retargeted to
// the NEW rejection contract: with the layout still unknown, a baseline that
// drifted since open (here: a sibling commit) is refused with EEXIST before
// any PUT, preserving the dirty generation. (A KNOWN append_log layout skips
// the verification stat and CASes against the fresh revision instead —
// last-writer-wins for a whole-image rewrite.)
func TestPortAppendLogFullRewriteRejectsBaselineDriftWithEEXIST(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/db-wal", []byte("pre"))
	rec := &putRecorder{}
	ch := newWrappedStack(t, wrapBackend(b, rec.intercept), "**/db-wal")
	ctx := context.Background()

	h := openLog(t, ch, "/db-wal").(*Handle)
	sibling := openLog(t, ch, "/db-wal").(*Handle)
	if err := sibling.Write(ctx, 0, []byte("XYZ")); err != nil {
		t.Fatal(err)
	}
	if err := sibling.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(ctx, 0, []byte("rewrite")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); !errors.Is(err, syscall.EEXIST) {
		t.Fatalf("flush error = %v, want EEXIST", err)
	}
	if rec.count() != 1 { // only the sibling's rewrite PUT
		t.Fatalf("PUTs after drift rejection = %d, want 1 (no rewrite of the stale handle)", rec.count())
	}
	if !h.dirty || !h.buf.HasDirtyParts() {
		t.Fatal("EEXIST must preserve dirty state")
	}
}

// v1 TestAppendLogFullRewriteTooLargePreservesLayoutAndDirtyState: a
// too-large PUT maps to EFBIG and preserves the dirty image and layout.
func TestPortAppendLogFullRewriteTooLargePreservesDirtyState(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedWithLayout("/db-wal", []byte("pre"), "append_log")
	ch := newWrappedStack(t, wrapBackend(b, func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && fsPath(r.URL.Path) {
			setStatusError(w, http.StatusRequestEntityTooLarge, client.AppendLogCodeTooLarge)
			return true
		}
		return false
	}), "**/db-wal")
	ctx := context.Background()

	h := openLog(t, ch, "/db-wal").(*Handle)
	if err := h.Write(ctx, 0, []byte("rewrite")); err != nil {
		t.Fatal(err)
	}
	err := h.Flush(ctx)
	if fs.StatusOf(err) != gofuse.Status(syscall.EFBIG) {
		t.Fatalf("flush error = %v, want EFBIG", err)
	}
	if !h.dirty || !h.buf.HasDirtyParts() {
		t.Fatal("failed full rewrite must preserve dirty state")
	}
	if got := h.layoutAtLocked(1, 3); got != client.ContentLayoutAppendLog {
		t.Fatalf("layout after failure = %q, want append_log", got)
	}
}

// v1 TestAppendLogFullRewriteFailuresPreserveLayoutAndDirtyState: server
// errors, exhausted revision conflicts, and canceled contexts all fail
// terminally without mutating the handle.
func TestPortAppendLogFullRewriteFailuresPreserveDirtyState(t *testing.T) {
	tests := []struct {
		name   string
		setUp  func(b *testbackend.Backend) interceptFunc
		ctxErr bool
	}{
		{
			name: "server error",
			setUp: func(b *testbackend.Backend) interceptFunc {
				return func(w http.ResponseWriter, r *http.Request) bool {
					if r.Method == http.MethodPut && fsPath(r.URL.Path) {
						w.WriteHeader(http.StatusInternalServerError)
						return true
					}
					return false
				}
			},
		},
		{
			name: "conflict exhaustion",
			setUp: func(b *testbackend.Backend) interceptFunc {
				b.ForceConflict("/db-wal", 10)
				return nil
			},
		},
		{name: "canceled context", ctxErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b := testbackend.NewBackend()
			b.SeedWithLayout("/db-wal", []byte("pre"), "append_log")
			var f interceptFunc
			if test.setUp != nil {
				f = test.setUp(b)
			}
			ch := newWrappedStack(t, wrapBackend(b, f), "**/db-wal")
			ctx := context.Background()

			h := openLog(t, ch, "/db-wal").(*Handle)
			if err := h.Write(context.Background(), 0, []byte("rewrite")); err != nil {
				t.Fatal(err)
			}
			if test.ctxErr {
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = canceled
			}
			err := h.Flush(ctx)
			if err == nil || fs.StatusOf(err) == gofuse.OK {
				t.Fatalf("flush error = %v, want terminal failure", err)
			}
			if !h.dirty || !h.buf.HasDirtyParts() {
				t.Fatal("failed full rewrite must preserve dirty state")
			}
			if got := h.layoutAtLocked(1, 3); got != client.ContentLayoutAppendLog {
				t.Fatalf("layout after failure = %q, want append_log", got)
			}
		})
	}
}

// v1 TestAppendLogFullRewriteFinalizesBeforePendingModeFailure: the whole
// image lands before the deferred chmod is applied; a failing first chmod
// leaves only the mode pending, and the next flush retries the mode alone.
func TestPortAppendLogFullRewriteFinalizesBeforePendingModeFailure(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/db-wal", []byte("pre"))
	var mu sync.Mutex
	puts, chmods := 0, 0
	ch := newWrappedStack(t, wrapBackend(b, func(w http.ResponseWriter, r *http.Request) bool {
		switch {
		case r.Method == http.MethodPut && fsPath(r.URL.Path):
			mu.Lock()
			puts++
			mu.Unlock()
			return false
		case r.Method == http.MethodPost && r.URL.Query().Has("chmod"):
			mu.Lock()
			chmods++
			first := chmods == 1
			mu.Unlock()
			if first {
				w.WriteHeader(http.StatusInternalServerError)
				return true
			}
			return false
		}
		return false
	}), "**/db-wal")
	ctx := context.Background()

	h := openLog(t, ch, "/db-wal").(*Handle)
	setattrMode(t, ch, "/db-wal", 0o600)
	if err := h.Write(ctx, 0, []byte("rewrite")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err == nil {
		t.Fatal("first flush must fail on the chmod")
	}
	if node, _ := b.Node("/db-wal"); string(node.Data) != "rewrite" {
		t.Fatalf("content after chmod failure = %q, want rewrite (finalized first)", node.Data)
	}
	if h.dirty || !h.hasPendingMode {
		t.Fatalf("handle after chmod failure = dirty=%t pending=%t, want false/true", h.dirty, h.hasPendingMode)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatalf("mode retry: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if puts != 1 || chmods != 2 {
		t.Fatalf("put/chmod calls = %d/%d, want 1/2 (no duplicate rewrite)", puts, chmods)
	}
}

// v1 TestAppendLogFullRewriteAppliesPendingModeAndCachesDirEntry (mode row).
func TestPortAppendLogFullRewriteAppliesPendingMode(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/db-wal", []byte("pre"))
	var mu sync.Mutex
	chmods := 0
	ch := newWrappedStack(t, wrapBackend(b, func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && r.URL.Query().Has("chmod") {
			mu.Lock()
			chmods++
			mu.Unlock()
		}
		return false
	}), "**/db-wal")
	ctx := context.Background()

	h := openLog(t, ch, "/db-wal").(*Handle)
	setattrMode(t, ch, "/db-wal", 0o600)
	if err := h.Write(ctx, 0, []byte("rewrite")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if chmods != 1 {
		t.Fatalf("chmod calls = %d, want 1", chmods)
	}
	if node, _ := b.Node("/db-wal"); node.Mode != 0o600 || string(node.Data) != "rewrite" {
		t.Fatalf("backend = %q mode=%o, want rewrite mode 600", node.Data, node.Mode)
	}
}

// v1 TestAppendLogFtruncateZeroRetainsCommittedBaselineForFullRewrite: a
// local truncate is visible immediately but lands remotely only at the next
// flush, and the conditional PUT still CASes against the ORIGINAL committed
// revision, not zero.
func TestPortAppendLogFtruncateZeroRetainsCommittedBaseline(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedWithLayout("/db-wal", []byte("pre"), "append_log")
	rec := &putRecorder{}
	ch := newWrappedStack(t, wrapBackend(b, rec.intercept), "**/db-wal")
	ctx := context.Background()

	h := openLog(t, ch, "/db-wal").(*Handle)
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/db-wal", Size: 0}); err != nil {
		t.Fatal(err)
	}
	if node, _ := b.Node("/db-wal"); string(node.Data) != "pre" {
		t.Fatalf("backend after local truncate = %q, want pre (remote landing deferred to flush)", node.Data)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	puts := rec.snapshot()
	if len(puts) != 1 || puts[0].rev != "1" || len(puts[0].body) != 0 {
		t.Fatalf("PUTs = %+v, want one empty body against revision 1", puts)
	}
	if node, _ := b.Node("/db-wal"); len(node.Data) != 0 {
		t.Fatalf("backend after flush = %q, want empty", node.Data)
	}
}

// v1 TestAppendLogCheckpointReuseFsyncUsesConditionalFullPUT: after a
// truncate the checkpoint reuse pattern (whole-buffer rewrite) commits via
// one conditional PUT, never the append endpoint.
func TestPortAppendLogTruncateThenWriteSyncUsesConditionalFullPUT(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedWithLayout("/db-wal", []byte("pre"), "append_log")
	rec := &putRecorder{}
	ch := newWrappedStack(t, wrapBackend(b, rec.intercept), "**/db-wal")
	ctx := context.Background()

	h := openLog(t, ch, "/db-wal").(*Handle)
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/db-wal", Size: 0}); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(ctx, 0, []byte("checkpoint")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, hit := b.Requests()["POST /v1/fs/db-wal"]; hit {
		t.Fatal("checkpoint reuse must not use the append endpoint")
	}
	puts := rec.snapshot()
	if len(puts) != 1 || string(puts[0].body) != "checkpoint" {
		t.Fatalf("PUTs = %+v, want one checkpoint body", puts)
	}
	if node, _ := b.Node("/db-wal"); string(node.Data) != "checkpoint" {
		t.Fatalf("backend = %q", node.Data)
	}
}

// v1 TestAppendLogPathTruncateWithoutRulesFallsBackToConditionalPUT
// (append-log rows): a handle-less truncate of an append_log object
// rewrites the remote image with one conditional PUT; shrink drops the
// tail, growth zero-fills, and a PUT conflict surfaces EAGAIN.
func TestPortAppendLogPathTruncateRewritesRemote(t *testing.T) {
	t.Run("shrink-to-zero", func(t *testing.T) {
		b := testbackend.NewBackend()
		b.SeedWithLayout("/db-wal", []byte("abc"), "append_log")
		rec := &putRecorder{}
		ch := newWrappedStack(t, wrapBackend(b, rec.intercept), "**/db-wal")
		if _, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Path: "/db-wal", Size: 0}); err != nil {
			t.Fatal(err)
		}
		puts := rec.snapshot()
		if len(puts) != 1 || puts[0].rev != "1" || len(puts[0].body) != 0 {
			t.Fatalf("PUTs = %+v, want one empty body against revision 1", puts)
		}
		if node, _ := b.Node("/db-wal"); len(node.Data) != 0 {
			t.Fatalf("backend = %q, want empty", node.Data)
		}
	})

	t.Run("growth-freezes-prefix-and-zero-fills", func(t *testing.T) {
		b := testbackend.NewBackend()
		b.SeedWithLayout("/db-wal", []byte("abc"), "append_log")
		rec := &putRecorder{}
		ch := newWrappedStack(t, wrapBackend(b, rec.intercept), "**/db-wal")
		if _, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Path: "/db-wal", Size: 5}); err != nil {
			t.Fatal(err)
		}
		puts := rec.snapshot()
		if len(puts) != 1 || puts[0].rev != "1" {
			t.Fatalf("PUTs = %+v, want one against revision 1", puts)
		}
		if want := []byte{'a', 'b', 'c', 0, 0}; !bytes.Equal(puts[0].body, want) {
			t.Fatalf("truncate body = %q, want %q", puts[0].body, want)
		} else if node, _ := b.Node("/db-wal"); !bytes.Equal(node.Data, want) {
			t.Fatalf("backend = %q", node.Data)
		}
	})

	t.Run("put-conflict-surfaces-error", func(t *testing.T) {
		b := testbackend.NewBackend()
		b.SeedWithLayout("/db-wal", []byte("abc"), "append_log")
		b.ForceConflict("/db-wal", 5)
		ch := newWrappedStack(t, b, "**/db-wal")
		_, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Path: "/db-wal", Size: 0})
		if err == nil || fs.StatusOf(err) == gofuse.OK {
			t.Fatalf("truncate error = %v, want failure", err)
		}
		if node, _ := b.Node("/db-wal"); string(node.Data) != "abc" {
			t.Fatalf("backend after failed truncate = %q, want abc", node.Data)
		}
	})
}

// New-behavior row: a single-layout object falls through to the generic
// setattr path (handled=false), which truncates it remotely.
func TestPortAppendLogPathTruncateSingleLayoutFallsThrough(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedWithLayout("/db-wal", []byte("abc"), "single")
	ch := newWrappedStack(t, b, "**/db-wal")

	if _, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Path: "/db-wal", Size: 0}); err != nil {
		t.Fatal(err)
	}
	if node, _ := b.Node("/db-wal"); len(node.Data) != 0 {
		t.Fatalf("backend = %q, want the generic path to truncate remotely", node.Data)
	}
}

// v1 TestAppendLogReleaseNonTailUsesConditionalFullPUT: closing a handle
// with a non-tail dirty generation commits exactly one whole-image PUT.
func TestPortAppendLogReleaseNonTailUsesConditionalFullPUT(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/db-wal", []byte("pre"))
	rec := &putRecorder{}
	ch := newWrappedStack(t, wrapBackend(b, rec.intercept), "**/db-wal")
	ctx := context.Background()

	h := openLog(t, ch, "/db-wal").(*Handle)
	if err := h.Write(ctx, 0, []byte("rewrite")); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, hit := b.Requests()["POST /v1/fs/db-wal"]; hit {
		t.Fatal("release of a non-tail generation must not use the append endpoint")
	}
	if rec.count() != 1 {
		t.Fatalf("release PUTs = %d, want 1", rec.count())
	}
	if node, _ := b.Node("/db-wal"); string(node.Data) != "rewrite" {
		t.Fatalf("backend = %q", node.Data)
	}
}

// v1 TestAppendLogCapabilityChangeStillFullRewritesKnownAppendLayout /
// TestAppendLogCapabilityDisabledPureTailFsyncUsesConditionalFullPUT: with
// the capability absent, even a tail-safe write on a known append_log
// layout commits through one conditional whole-image PUT.
func TestPortAppendLogCapabilityDisabledRewritesKnownAppendLayout(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedWithLayout("/db-wal", []byte("pre"), "append_log")
	rec := &putRecorder{}
	ch := newWrappedStack(t, wrapBackend(b, func(w http.ResponseWriter, r *http.Request) bool {
		if stripCapabilityIntercept(w, r) {
			return true
		}
		return rec.intercept(w, r)
	}), "**/db-wal")
	ctx := context.Background()

	h := openLog(t, ch, "/db-wal").(*Handle)
	if got := h.layoutAtLocked(1, 3); got != client.ContentLayoutAppendLog {
		t.Fatalf("open layout = %q, want append_log observed from stat", got)
	}
	if err := h.Write(ctx, 3, []byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, hit := b.Requests()["POST /v1/fs/db-wal"]; hit {
		t.Fatal("append endpoint probed without the capability")
	}
	puts := rec.snapshot()
	if len(puts) != 1 || puts[0].rev != "1" || string(puts[0].body) != "pretail" {
		t.Fatalf("PUTs = %+v, want one whole image against revision 1", puts)
	}
}

// New-behavior row (documented decision): an existing handle whose path
// vanished remotely does NOT resurrect the name — the flush surfaces
// ENOENT and keeps the dirty image.
func TestPortAppendLogFullRewriteVanishedPathReturnsENOENT(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/db-wal", []byte("pre"))
	armed := false
	ch := newWrappedStack(t, wrapBackend(b, func(w http.ResponseWriter, r *http.Request) bool {
		// Vanish the name at the first post-write stat.
		if armed && r.Method == http.MethodHead && r.URL.Path == "/v1/fs/db-wal" {
			armed = false
			b.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodDelete, "/v1/fs/db-wal", nil))
			w.WriteHeader(http.StatusNotFound)
			return true
		}
		return false
	}), "**/db-wal")
	ctx := context.Background()

	h := openLog(t, ch, "/db-wal").(*Handle)
	if err := h.Write(ctx, 0, []byte("rewrite")); err != nil {
		t.Fatal(err)
	}
	armed = true
	if err := h.Flush(ctx); !errors.Is(err, fs.ErrNotFound) {
		t.Fatalf("flush error = %v, want ENOENT (no resurrect)", err)
	}
	if _, ok := b.Node("/db-wal"); ok {
		t.Fatal("vanished name must not be recreated")
	}
	if !h.dirty || !h.buf.HasDirtyParts() {
		t.Fatal("ENOENT must preserve the dirty image")
	}
}

// New-behavior row (B1 fix): a never-landed create whose image vanished
// remotely commits through the generic creation path (expected revision 0)
// instead of failing the flush with ENOENT. The capability is stripped so
// the append transport cannot rebase-adopt a rewrite base first.
func TestPortAppendLogFullRewriteNewFileCreatesGenerically(t *testing.T) {
	b := testbackend.NewBackend()
	armed := false
	rec := &putRecorder{}
	ch := newWrappedStack(t, wrapBackend(b, func(w http.ResponseWriter, r *http.Request) bool {
		if stripCapabilityIntercept(w, r) {
			return true
		}
		if armed && r.Method == http.MethodHead && r.URL.Path == "/v1/fs/new-wal" {
			armed = false
			b.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodDelete, "/v1/fs/new-wal", nil))
			w.WriteHeader(http.StatusNotFound)
			return true
		}
		return rec.intercept(w, r)
	}), "**/new-wal")
	ctx := context.Background()

	h := openLog(t, ch, "/new-wal").(*Handle)
	if err := h.Write(ctx, 0, []byte("image")); err != nil {
		t.Fatal(err)
	}
	armed = true
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	puts := rec.snapshot()
	if len(puts) != 1 || puts[0].rev != "0" || string(puts[0].body) != "image" {
		t.Fatalf("PUTs = %+v, want one generic create against revision 0", puts)
	}
	if node, _ := b.Node("/new-wal"); string(node.Data) != "image" {
		t.Fatalf("backend = %q", node.Data)
	}
	if h.dirty {
		t.Fatal("generic create must clear dirty state")
	}
}

// v1 TestAppendLogFullRewriteConcurrentMutationPreservesNewerDirtyGeneration:
// a write landing while the rewrite PUT is in flight keeps its newer dirty
// generation; the handle adopts the landed revision as its next baseline and
// the following flush publishes the complete image.
func TestPortAppendLogFullRewriteConcurrentMutationPreservesNewerDirtyGeneration(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/db-wal", []byte("pre"))
	started := make(chan struct{})
	rel := newReleaseOnce()
	t.Cleanup(rel.release)
	ch := newWrappedStack(t, wrapBackend(b, withheldCommit(b, func(r *http.Request) bool {
		return r.Method == http.MethodPut && r.URL.Path == "/v1/fs/db-wal"
	}, started, rel.ch)), "**/db-wal")
	ctx := context.Background()

	h := openLog(t, ch, "/db-wal").(*Handle)
	if err := h.Write(ctx, 0, []byte("rewrite")); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- h.Flush(ctx) }()
	<-started

	if err := h.Write(ctx, 7, []byte("next")); err != nil {
		t.Fatal(err)
	}
	rel.release()
	if err := <-done; err != nil {
		t.Fatalf("concurrent rewrite flush: %v", err)
	}

	if !h.dirty || !h.buf.HasDirtyParts() {
		t.Fatal("newer dirty generation must survive the in-flight rewrite")
	}
	if got := string(h.buf.Bytes()); got != "rewritenext" {
		t.Fatalf("buffer after concurrent write = %q, want rewritenext", got)
	}
	if node, _ := b.Node("/db-wal"); string(node.Data) != "rewrite" || h.baseRev != node.Rev {
		t.Fatalf("landed baseline = %q rev=%d handle rev=%d", node.Data, node.Rev, h.baseRev)
	}
	// The next flush publishes the complete newer generation.
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if node, _ := b.Node("/db-wal"); string(node.Data) != "rewritenext" || h.dirty {
		t.Fatalf("after second flush = %q dirty=%t", node.Data, h.dirty)
	}
}

// v1 TestAppendLogFullRewriteConcurrentWALKeepsLiveHeaderFence: a WAL frame
// write landing while the whole-image PUT is in flight keeps its
// beyond-header fence — the next generation cannot ride a header-only reset.
func TestPortAppendLogFullRewriteConcurrentWALKeepsLiveHeaderFence(t *testing.T) {
	oldHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 1, 2)
	newHeader := makeSQLiteWALHeaderForTest(t, sqliteWALMagicBig, 4096, 3, 4)
	seed := append(append([]byte(nil), oldHeader...), bytes.Repeat([]byte("o"), 32)...)
	b := testbackend.NewBackend()
	b.SeedWithLayout("/wal.db", seed, "append_log")
	started := make(chan struct{})
	rel := newReleaseOnce()
	t.Cleanup(rel.release)
	ch := newWrappedStack(t, wrapBackend(b, withheldCommit(b, func(r *http.Request) bool {
		return r.Method == http.MethodPut && r.URL.Path == "/v1/fs/wal.db"
	}, started, rel.ch)), "**/wal.db")
	ctx := context.Background()

	h := openLog(t, ch, "/wal.db").(*Handle)
	// Header rewrite plus a frame write fence the reset to a full image.
	if err := h.Write(ctx, 0, newHeader); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(ctx, 40, []byte("x")); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- h.Flush(ctx) }()
	<-started

	if err := h.Write(ctx, 40, []byte("frame")); err != nil {
		t.Fatal(err)
	}
	rel.release()
	if err := <-done; err != nil {
		t.Fatalf("concurrent WAL rewrite flush: %v", err)
	}

	if !h.st.sqliteWALWriteBeyondHeader {
		t.Fatal("concurrent WAL frame write lost its header-only reset fence")
	}
	if !h.dirty || !h.buf.HasDirtyParts() {
		t.Fatal("concurrent dirty generation must survive the in-flight rewrite")
	}
}
