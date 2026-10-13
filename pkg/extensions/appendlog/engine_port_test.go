package appendlog

// Ported from v1 pkg/fuse/append_log_engine_test.go: HTTP interaction
// sequences of the tail-append transport — append endpoint call order and
// baseline headers, rebase retry and rejection, unsupported/TooLarge
// fallbacks, deferred chmod finalization, mid-request retarget/unlink, and
// the capability gate. This file also carries the shared wrapper helpers
// the other port files use.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// interceptFunc sees every request before the backend; returning true means
// the request is fully handled.
type interceptFunc func(w http.ResponseWriter, r *http.Request) bool

// wrapBackend gives f the first look at each request, delegating to b when
// f declines.
func wrapBackend(b *testbackend.Backend, f interceptFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if f != nil && f(w, r) {
			return
		}
		b.ServeHTTP(w, r)
	})
}

// newWrappedStack is newStack over an arbitrary root handler (used to
// intercept or rewrite backend responses).
func newWrappedStack(t *testing.T, root http.Handler, patterns ...string) *fs.Chain {
	t.Helper()
	ts := httptest.NewServer(root)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	ext, err := New(Options{Patterns: patterns, Client: cl})
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
	return ch
}

// stripCapabilityIntercept answers /v1/status without the append_log_v1
// capability, modeling a server (or capability change) where the append
// endpoint must never be probed.
func stripCapabilityIntercept(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Path != "/v1/status" || r.Method != http.MethodGet {
		return false
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":               "ok",
		"max_upload_bytes":     1 << 30,
		"inline_threshold":     1 << 20,
		"storage_capabilities": map[string]any{},
	})
	return true
}

// stripAppendLogCapability wraps b with stripCapabilityIntercept.
func stripAppendLogCapability(b *testbackend.Backend) http.Handler {
	return wrapBackend(b, stripCapabilityIntercept)
}

// drainBody reads (and rewires) the request body so an interceptor can both
// inspect it and delegate the request.
func drainBody(r *http.Request) []byte {
	body, _ := io.ReadAll(r.Body)
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))
	return body
}

// setStatusError writes one coded error response.
func setStatusError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": code, "code": code})
}

// setattrMode applies a mode-only SetAttr on a live protocol path.
func setattrMode(t *testing.T, ch *fs.Chain, path string, mode uint32) {
	t.Helper()
	_, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Path: path, Mode: mode, Size: -1})
	if err != nil {
		t.Fatalf("setattr mode: %v", err)
	}
}

// withheldCommit models a commit that already landed on the server while the
// client is still waiting for the response: the backend fully processes the
// matching request, then the response is withheld until release.
func withheldCommit(b *testbackend.Backend, match func(*http.Request) bool, started chan<- struct{}, release <-chan struct{}) interceptFunc {
	var once sync.Once
	return func(w http.ResponseWriter, r *http.Request) bool {
		if !match(r) {
			return false
		}
		rec := httptest.NewRecorder()
		b.ServeHTTP(rec, r)
		once.Do(func() { close(started) })
		<-release
		for k, vs := range rec.Header() {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
		return true
	}
}

// v1 TestAppendLogTailCommit: one POST per flush with the exact committed
// baseline headers, the tail body frozen at snapshot time, and the adopted
// baseline carrying into the next append.
func TestPortAppendLogTailCommitSendsBaselineHeaders(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/db-wal", []byte("pre")) // rev 1, size 3
	var mu sync.Mutex
	var posts []struct{ rev, size, body string }
	ch := newWrappedStack(t, wrapBackend(b, func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && r.URL.Query().Has("append-log") {
			body := drainBody(r)
			mu.Lock()
			posts = append(posts, struct{ rev, size, body string }{
				r.Header.Get("X-Dat9-Expected-Revision"),
				r.Header.Get("X-Dat9-Expected-Size"),
				string(body),
			})
			mu.Unlock()
		}
		return false
	}), "**/db-wal")
	ctx := context.Background()

	h := openLog(t, ch, "/db-wal").(*Handle)
	if err := h.Write(ctx, 3, []byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if len(posts) != 1 || posts[0].rev != "1" || posts[0].size != "3" || posts[0].body != "tail" {
		t.Fatalf("append posts = %+v, want one post rev=1 size=3 body=tail", posts)
	}
	mu.Unlock()
	if n, _ := b.Node("/db-wal"); string(n.Data) != "pretail" || n.Rev != 11 {
		t.Fatalf("backend = %q rev=%d, want pretail rev=11", n.Data, n.Rev)
	}

	// A second append continues from the atomically adopted baseline.
	if err := h.Write(ctx, 7, []byte("!")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if len(posts) != 2 || posts[1].rev != "11" || posts[1].size != "7" || posts[1].body != "!" {
		t.Fatalf("second append post = %+v, want rev=11 size=7 body=!", posts[1])
	}
	mu.Unlock()
	if n, _ := b.Node("/db-wal"); string(n.Data) != "pretail!" {
		t.Fatalf("backend after second append = %q", n.Data)
	}
	if h.dirty || h.baseRev != 12 || h.origSize != 8 {
		t.Fatalf("adopted handle = dirty=%t rev=%d size=%d", h.dirty, h.baseRev, h.origSize)
	}
}

// v1 TestAppendLogNewRandomAssemblyCreatesCompleteImage: a brand-new file
// assembled out of order is published as one complete zero-filled image
// through the append endpoint's create form (expected 0/0). Because the
// chain terminal materializes the empty file at open time, the 0/0 create
// form rebases once against that landing and retries at the fresh revision.
func TestPortAppendLogNewRandomAssemblyCreatesCompleteImage(t *testing.T) {
	b := testbackend.NewBackend()
	var mu sync.Mutex
	var posts []struct{ rev, size string }
	var createBody []byte
	ch := newWrappedStack(t, wrapBackend(b, func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && r.URL.Query().Has("append-log") {
			body := drainBody(r)
			mu.Lock()
			posts = append(posts, struct{ rev, size string }{
				r.Header.Get("X-Dat9-Expected-Revision"),
				r.Header.Get("X-Dat9-Expected-Size"),
			})
			createBody = body
			mu.Unlock()
		}
		return false
	}), "**/new-wal")
	ctx := context.Background()

	h := openLog(t, ch, "/new-wal").(*Handle)
	if err := h.Write(ctx, 4, []byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(ctx, 0, []byte("hi")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	want := []byte{'h', 'i', 0, 0, 't', 'a', 'i', 'l'}
	if len(posts) != 2 || posts[0].rev != "0" || posts[0].size != "0" {
		t.Fatalf("create posts = %+v, want the first at expected 0/0", posts)
	}
	if !bytes.Equal(createBody, want) {
		t.Fatalf("create body = %q, want %q", createBody, want)
	}
	if n, _ := b.Node("/new-wal"); !bytes.Equal(n.Data, want) {
		t.Fatalf("backend = %q", n.Data)
	}
	if h.dirty || h.isNew {
		t.Fatalf("created handle = dirty=%t isNew=%t", h.dirty, h.isNew)
	}
}

// v1 TestAppendLogRebaseRetriesOnceWithFrozenTail: a revision bump at the
// same size (here: a sibling's same-length rewrite) rebases the first POST;
// one stat and one retry with the new revision commits the frozen tail.
func TestPortAppendLogRebaseRetriesOnceWithFrozenTail(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/db-wal", []byte("pre"))
	var mu sync.Mutex
	appends, heads := 0, 0
	ch := newWrappedStack(t, wrapBackend(b, func(w http.ResponseWriter, r *http.Request) bool {
		switch {
		case r.Method == http.MethodPost && r.URL.Query().Has("append-log"):
			mu.Lock()
			appends++
			mu.Unlock()
		case r.Method == http.MethodHead:
			mu.Lock()
			heads++
			mu.Unlock()
		}
		return false
	}), "**/db-wal")
	ctx := context.Background()

	// Both handles adopt the rev-1 baseline BEFORE the sibling commits: the
	// sibling's overwrite forces a full rewrite (revision 1 -> 10 at the
	// same size 3), which the writer's append must rebase onto.
	h := openLog(t, ch, "/db-wal").(*Handle)
	sibling := openLog(t, ch, "/db-wal").(*Handle)
	if err := sibling.Write(ctx, 0, []byte("XYZ")); err != nil {
		t.Fatal(err)
	}
	if err := sibling.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	if err := h.Write(ctx, 3, []byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if appends != 2 || heads < 3 { // 2 opens + the rebase stat
		t.Fatalf("append/head calls = %d/%d, want 2/>=3", appends, heads)
	}
	if n, _ := b.Node("/db-wal"); string(n.Data) != "XYZtail" || h.dirty || h.baseRev != n.Rev {
		t.Fatalf("backend = %q rev=%d, handle = dirty=%t rev=%d", n.Data, n.Rev, h.dirty, h.baseRev)
	}
}

// v1 TestAppendLogRebaseRejectsChangedSizeWithoutRetry: a rebase whose stat
// shows a different size is a terminal failure — the tail is not retried and
// the dirty generation is preserved.
func TestPortAppendLogRebaseRejectsChangedSizeWithoutRetry(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/db-wal", []byte("pre"))
	var mu sync.Mutex
	appends := 0
	ch := newWrappedStack(t, wrapBackend(b, func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && r.URL.Query().Has("append-log") {
			mu.Lock()
			appends++
			mu.Unlock()
		}
		return false
	}), "**/db-wal")
	ctx := context.Background()

	// Both handles adopt the rev-1 baseline before the sibling's append
	// grows the remote (revision AND size move).
	h := openLog(t, ch, "/db-wal").(*Handle)
	sibling := openLog(t, ch, "/db-wal").(*Handle)
	if err := sibling.Write(ctx, 3, []byte("grown")); err != nil {
		t.Fatal(err)
	}
	if err := sibling.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	if err := h.Write(ctx, 3, []byte("tail")); err != nil {
		t.Fatal(err)
	}
	err := h.Flush(ctx)
	if err == nil {
		t.Fatal("rebase with changed size must fail")
	}
	mu.Lock()
	defer mu.Unlock()
	if appends != 2 { // sibling commit + this handle's single attempt
		t.Fatalf("append calls = %d, want 2 (no retry after changed size)", appends)
	}
	if n, _ := b.Node("/db-wal"); string(n.Data) != "pregrown" {
		t.Fatalf("backend = %q, want pregrown (rejected tail must not land)", n.Data)
	}
	if !h.dirty || !h.buf.HasDirtyParts() {
		t.Fatal("failed rebase must preserve dirty state")
	}
}

// v1 TestAppendLogRebasedUnsupportedFallsBackWithRebasedRevision: after a
// rebase-learned baseline, an unsupported retry falls back to the full
// rewrite, which PUTs against the rebased revision.
func TestPortAppendLogRebasedUnsupportedFallsBackWithRebasedRevision(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/db-wal", []byte("pre"))
	var mu sync.Mutex
	appends, puts := 0, 0
	var putRevs []string
	ch := newWrappedStack(t, wrapBackend(b, func(w http.ResponseWriter, r *http.Request) bool {
		switch {
		case r.Method == http.MethodPost && r.URL.Query().Has("append-log"):
			mu.Lock()
			appends++
			mu.Unlock()
		case r.Method == http.MethodPut && r.URL.Path == "/v1/fs/db-wal":
			mu.Lock()
			puts++
			putRevs = append(putRevs, r.Header.Get("X-Dat9-Expected-Revision"))
			mu.Unlock()
		}
		return false
	}), "**/db-wal")
	ctx := context.Background()

	// Both handles adopt the rev-1 baseline before the sibling's same-size
	// rewrite (revision 1 -> 10); the writer's append rebases onto it.
	h := openLog(t, ch, "/db-wal").(*Handle)
	sibling := openLog(t, ch, "/db-wal").(*Handle)
	if err := sibling.Write(ctx, 0, []byte("XYZ")); err != nil {
		t.Fatal(err)
	}
	if err := sibling.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	b.AppendUnsupported = true

	if err := h.Write(ctx, 3, []byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if appends != 2 || puts != 2 { // sibling's PUT + fallback PUT
		t.Fatalf("append/put calls = %d/%d, want 2/2", appends, puts)
	}
	if len(putRevs) != 2 || putRevs[1] != "10" {
		t.Fatalf("fallback PUT revisions = %v, want the second against the rebased revision 10", putRevs)
	}
	if n, _ := b.Node("/db-wal"); string(n.Data) != "XYZtail" {
		t.Fatalf("backend = %q, want XYZtail", n.Data)
	}
	if !h.st.unsupported {
		t.Fatal("unsupported response must suppress this handle")
	}
}

// v1 TestAppendLogUnsupportedAndTooLargePreserveDirtyState (TooLarge row):
// an append_log_too_large reply maps to EFBIG and preserves the dirty image.
func TestPortAppendLogAppendTooLargeReturnsEFBIG(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/db-wal", []byte("pre"))
	ch := newWrappedStack(t, wrapBackend(b, func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && r.URL.Query().Has("append-log") {
			setStatusError(w, http.StatusRequestEntityTooLarge, client.AppendLogCodeTooLarge)
			return true
		}
		return false
	}), "**/db-wal")
	ctx := context.Background()

	h := openLog(t, ch, "/db-wal").(*Handle)
	if err := h.Write(ctx, 3, []byte("tail")); err != nil {
		t.Fatal(err)
	}
	err := h.Flush(ctx)
	if !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("flush error = %v, want EFBIG", err)
	}
	if !h.dirty || !h.buf.HasDirtyParts() {
		t.Fatal("TooLarge must preserve dirty state")
	}
	if got := b.Requests()["PUT /v1/fs/db-wal"]; got != 0 {
		t.Fatalf("TooLarge must not fall through to a rewrite: PUTs = %d", got)
	}
}

// v1 TestAppendLogConflictAndTimeoutPreserveDirtyState: a coded conflict
// maps to EAGAIN and a canceled context fails before the wire; both keep the
// dirty generation.
func TestPortAppendLogAppendConflictAndCancelPreserveDirtyState(t *testing.T) {
	t.Run("conflict", func(t *testing.T) {
		b := testbackend.NewBackend()
		b.Seed("/db-wal", []byte("pre"))
		ch := newWrappedStack(t, wrapBackend(b, func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method == http.MethodPost && r.URL.Query().Has("append-log") {
				setStatusError(w, http.StatusConflict, client.AppendLogCodeConflict)
				return true
			}
			return false
		}), "**/db-wal")
		ctx := context.Background()

		h := openLog(t, ch, "/db-wal").(*Handle)
		if err := h.Write(ctx, 3, []byte("tail")); err != nil {
			t.Fatal(err)
		}
		if err := h.Flush(ctx); !errors.Is(err, syscall.EAGAIN) {
			t.Fatalf("flush error = %v, want EAGAIN", err)
		}
		if !h.dirty || !h.buf.HasDirtyParts() {
			t.Fatal("conflict must preserve dirty state")
		}
	})

	t.Run("canceled-context", func(t *testing.T) {
		b := testbackend.NewBackend()
		b.Seed("/db-wal", []byte("pre"))
		ch := newWrappedStack(t, wrapBackend(b, func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method != http.MethodGet && r.Method != http.MethodHead {
				t.Errorf("canceled flush reached the server: %s %s", r.Method, r.URL)
				w.WriteHeader(http.StatusInternalServerError)
				return true
			}
			return false
		}), "**/db-wal")

		h := openLog(t, ch, "/db-wal").(*Handle)
		if err := h.Write(context.Background(), 3, []byte("tail")); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := h.Flush(ctx); err == nil {
			t.Fatal("canceled flush must fail")
		}
		if !h.dirty || !h.buf.HasDirtyParts() {
			t.Fatal("canceled flush must preserve dirty state")
		}
	})
}

// v1 TestAppendLogTailCommitFinalizesBeforePendingModeFailure: content
// finalization happens before the deferred chmod — a failing first chmod
// leaves the tail committed and only the mode pending; the next flush
// retries the mode alone.
func TestPortAppendLogTailCommitFinalizesBeforePendingModeFailure(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/db-wal", []byte("pre"))
	var mu sync.Mutex
	appends, chmods := 0, 0
	ch := newWrappedStack(t, wrapBackend(b, func(w http.ResponseWriter, r *http.Request) bool {
		switch {
		case r.Method == http.MethodPost && r.URL.Query().Has("append-log"):
			mu.Lock()
			appends++
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
	if err := h.Write(ctx, 3, []byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err == nil {
		t.Fatal("first flush must fail on the chmod")
	}
	if n, _ := b.Node("/db-wal"); string(n.Data) != "pretail" {
		t.Fatalf("content after chmod failure = %q, want pretail (finalized first)", n.Data)
	}
	if h.dirty || !h.hasPendingMode {
		t.Fatalf("handle after chmod failure = dirty=%t pendingMode=%t, want false/true", h.dirty, h.hasPendingMode)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatalf("mode retry: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if appends != 1 || chmods != 2 {
		t.Fatalf("append/chmod calls = %d/%d, want 1/2 (no duplicate tail)", appends, chmods)
	}
	if h.hasPendingMode {
		t.Fatal("mode retry must clear the pending flag")
	}
}

// v1 TestAppendLogTailCommitAppliesPendingModeAndCachesDirEntry (mode row):
// a successful append applies the deferred chmod exactly once.
func TestPortAppendLogTailCommitAppliesPendingMode(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/db-wal", []byte("pre"))
	var mu sync.Mutex
	chmods := 0
	var chmodModes []uint32
	ch := newWrappedStack(t, wrapBackend(b, func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && r.URL.Query().Has("chmod") {
			body := drainBody(r)
			var req struct {
				Mode uint32 `json:"mode"`
			}
			_ = json.Unmarshal(body, &req)
			mu.Lock()
			chmods++
			chmodModes = append(chmodModes, req.Mode)
			mu.Unlock()
		}
		return false
	}), "**/db-wal")
	ctx := context.Background()

	h := openLog(t, ch, "/db-wal").(*Handle)
	setattrMode(t, ch, "/db-wal", 0o600)
	if err := h.Write(ctx, 3, []byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if chmods != 1 || len(chmodModes) != 1 || chmodModes[0] != 0o600 {
		t.Fatalf("chmod calls/modes = %d/%o, want one mode 600", chmods, chmodModes)
	}
	if n, _ := b.Node("/db-wal"); n.Mode != 0o600 {
		t.Fatalf("backend mode = %o, want 600", n.Mode)
	}
	if h.hasPendingMode || h.dirty {
		t.Fatal("successful append must clear the pending mode and dirty state")
	}
}

// releaseOnce guards a withhold channel against double close (explicit
// release plus test cleanup after an early failure).
type releaseOnce struct {
	once sync.Once
	ch   chan struct{}
}

func newReleaseOnce() *releaseOnce { return &releaseOnce{ch: make(chan struct{})} }

func (r *releaseOnce) release() { r.once.Do(func() { close(r.ch) }) }

// v1 TestAppendLogTailRetargetDuringRequestPreservesDirtyGeneration: the
// remote landing of a tail whose handle was renamed mid-request is recorded
// for the OLD path; the renamed handle keeps its dirty generation instead of
// publishing a stale baseline under the new name.
func TestPortAppendLogTailRetargetDuringRequestPreservesDirtyGeneration(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/db-wal", []byte("pre"))
	started := make(chan struct{})
	rel := newReleaseOnce()
	t.Cleanup(rel.release)
	ch := newWrappedStack(t, wrapBackend(b, withheldCommit(b, func(r *http.Request) bool {
		return r.Method == http.MethodPost && r.URL.Query().Has("append-log")
	}, started, rel.ch)), "**/db-wal")
	ctx := context.Background()

	h := openLog(t, ch, "/db-wal").(*Handle)
	if err := h.Write(ctx, 3, []byte("tail")); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- h.Flush(ctx) }()
	<-started

	// Rename the live handle while its append is in flight.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/db-wal", Path2: "/renamed.db-wal"}); err != nil {
		t.Fatal(err)
	}
	rel.release()
	if err := <-done; !errors.Is(err, syscall.EEXIST) {
		t.Fatalf("retargeted flush error = %v, want EEXIST (the stale baseline no longer matches the renamed node)", err)
	}

	if h.path != "/renamed.db-wal" {
		t.Fatalf("handle path = %q, want the renamed path", h.path)
	}
	if !h.dirty || !h.buf.HasDirtyParts() {
		t.Fatal("retargeted handle must retain its dirty generation")
	}
	if h.baseRev != 1 {
		t.Fatalf("retargeted handle adopted the stale baseline: rev=%d", h.baseRev)
	}
	if n, ok := b.Node("/renamed.db-wal"); !ok || string(n.Data) != "pretail" {
		t.Fatalf("renamed backend node = %q ok=%t, want pretail", n.Data, ok)
	}
	if _, ok := b.Node("/db-wal"); ok {
		t.Fatal("old name must be gone after the rename")
	}
	if g, ok := h.a.committed.Load("/db-wal"); !ok || g.(landedGeneration).rev == 0 {
		t.Fatal("remote landing must be recorded under the snapshot path")
	}
}

// v1 TestAppendLogTailUnlinkDuringRequestPreservesDirtyGeneration: when the
// path is unlinked while its append is in flight, the landing is recorded
// but never published locally — the deferred delete wins and the dirty
// generation stays with the anonymous handle.
func TestPortAppendLogTailUnlinkDuringRequestPreservesDirtyGeneration(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/db-wal", []byte("pre"))
	started := make(chan struct{})
	rel := newReleaseOnce()
	t.Cleanup(rel.release)
	ch := newWrappedStack(t, wrapBackend(b, withheldCommit(b, func(r *http.Request) bool {
		return r.Method == http.MethodPost && r.URL.Query().Has("append-log")
	}, started, rel.ch)), "**/db-wal")
	ctx := context.Background()

	h := openLog(t, ch, "/db-wal").(*Handle)
	if err := h.Write(ctx, 3, []byte("tail")); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- h.Flush(ctx) }()
	<-started

	// POSIX unlink-while-open: the name disappears now, the delete is
	// deferred to the last close.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/db-wal"}); err != nil {
		t.Fatal(err)
	}
	rel.release()
	if err := <-done; err != nil {
		t.Fatalf("unlinked flush: %v", err)
	}

	if !h.unlinked || !h.dirty || !h.buf.HasDirtyParts() {
		t.Fatalf("unlinked handle = unlinked=%t dirty=%t, want true/true", h.unlinked, h.dirty)
	}
	// The in-flight tail landed just before the unlink; the name stays alive
	// only until the last close publishes the deferred delete.
	if n, ok := b.Node("/db-wal"); !ok || string(n.Data) != "pretail" {
		t.Fatalf("pre-release backend = %q ok=%t, want pretail (delete deferred)", n.Data, ok)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/db-wal"}); !errors.Is(err, fs.ErrNotFound) {
		t.Fatalf("unlinked path still visible: %v", err)
	}
	if g, ok := h.a.committed.Load("/db-wal"); !ok || g.(landedGeneration).rev == 0 {
		t.Fatal("remote landing must still be recorded for the unlinked path")
	}
	// Last close publishes the deferred delete (idempotent against the
	// landed tail) and never re-uploads the dirty generation.
	if err := h.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, ok := b.Node("/db-wal"); ok {
		t.Fatal("release must publish the deferred delete")
	}
}

// v1 TestAppendLogCapabilityDisabledDoesNotProbeEndpoint: without the
// negotiated append_log_v1 capability the tail-append transport is skipped
// entirely and the flush commits through one conditional full-image PUT.
func TestPortAppendLogCapabilityDisabledSkipsAppendEndpoint(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/db-wal", []byte("pre"))
	ch := newWrappedStack(t, stripAppendLogCapability(b), "**/db-wal")
	ctx := context.Background()

	h := openLog(t, ch, "/db-wal").(*Handle)
	if err := h.Write(ctx, 3, []byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if _, hit := b.Requests()["POST /v1/fs/db-wal"]; hit {
		t.Fatal("append endpoint probed without the capability")
	}
	if got := b.Requests()["PUT /v1/fs/db-wal"]; got != 1 {
		t.Fatalf("conditional PUTs = %d, want 1", got)
	}
	if n, _ := b.Node("/db-wal"); string(n.Data) != "pretail" {
		t.Fatalf("backend = %q, want pretail", n.Data)
	}
}
