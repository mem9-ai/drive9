package writeback

// Ports of v1 pkg/fuse/close_sync_mode_test.go: the combined-mode contract
// for close-sync commits, re-expressed against the new plane's semantics.
// v1 negotiated a combined create-content+mode batch item
// (uploadCloseSyncCreateWithMode) with a cooldown-driven probe; the new
// package implements that negotiation at the commit queue's batch layer
// (commit_queue_mode_port_test.go) while the fs-level close-sync boundary
// owns three restored contracts asserted here:
//
//   - an empty creation lands through the CreateFileCtx endpoint first
//     (B10.4: the create endpoint wins over the conditional write, with the
//     write as fallback when create fails);
//   - large files commit through the shadow-spill staging path and the flush
//     waits for the path's own landing (B4);
//   - a deferred mode (pendingMode) applies only after the content it
//     describes has landed, as a separate remote chmod when the server has
//     not advertised the combined contract (testbackend does not).
//
// Not ported from v1:
//   - TestCloseSyncCombinedModeLogsFallbackOncePerCooldown and
//     TestCloseSyncBatchCooldownDoesNotExtendForInflightFailures: the new
//     package has no batch probe cooldown — an unsupported batch endpoint
//     disables batching permanently (pinned below);
//   - TestCloseSyncCombinedModeDoesNotFinalizeChangedHandle: the new plane
//     serializes rename/unlink behind the synchronous flush plus the queue's
//     waitPathCommit, so a handle cannot mutate under an in-flight close-sync
//     upload through the public path (observable semantics live in
//     writeback_align_test.go's rename/unlink rows);
//   - TestCloseSyncCombinedModeLeavesOtherUploadsUnchanged: the v1
//     write-policy scope rows are covered by the per-policy suites
//     (write-sync per-write commits, writeback local staging).

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// methodRecorder tags the wire-level operations a flush produced, in order:
// CREATE (POST ?create), CHMOD (POST ?chmod), PUT, INITIATE/PART/COMPLETE
// (multipart), everything else by method.
type methodRecorder struct {
	mu     sync.Mutex
	events []string
}

func (m *methodRecorder) tag(r *http.Request) string {
	switch {
	case r.URL.Query().Has("create"):
		return "CREATE"
	case r.URL.Query().Has("chmod"):
		return "CHMOD"
	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/fs/"):
		return "PUT"
	case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
		return "INITIATE"
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/complete"):
		return "COMPLETE"
	case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/part/"):
		return "PART"
	default:
		return r.Method
	}
}

func (m *methodRecorder) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.events = append(m.events, m.tag(r))
		m.mu.Unlock()
		next.ServeHTTP(w, r)
	})
}

func (m *methodRecorder) snapshot() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.events...)
}

func (m *methodRecorder) count(tag string) int {
	n := 0
	for _, e := range m.snapshot() {
		if e == tag {
			n++
		}
	}
	return n
}

// newModeStack wires a close-sync stack over testbackend with the recorder
// in front, so tests can assert endpoint order.
func newModeStack(t *testing.T, rec *methodRecorder) (*fs.Chain, *testbackend.Backend) {
	t.Helper()
	b := testbackend.NewBackend()
	ts := httptest.NewServer(rec.wrap(b))
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyCloseSync, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	return ch, b
}

func createModeFile(t *testing.T, ch *fs.Chain, path string, mode uint32) fs.Handle {
	t.Helper()
	res, err := ch.Dispatch(context.Background(), &fs.Op{
		Kind: fs.OpCreate, Path: path,
		Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: mode,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Handle == nil {
		t.Fatal("no handle")
	}
	return res.Handle
}

// v1 TestCloseSyncCombinedModeCommitsContentAndMode: create modes ride the
// commit boundary — content and mode both land at close, the remote mode is
// the permission slice (mode&0o777), and a repeated flush does not re-upload.
// A create mode of exactly 0 is legitimate but the remote create keeps the
// server default and no chmod is issued (applyPendingMode skips mode 0).
func TestPortCloseSyncCreateModeCommitsContentAndMode(t *testing.T) {
	for _, mode := range []uint32{0o755, 0, 0o4755} {
		t.Run(fmt.Sprintf("%o", mode), func(t *testing.T) {
			rec := &methodRecorder{}
			ch, b := newModeStack(t, rec)
			ctx := context.Background()

			h := createModeFile(t, ch, "/mode.txt", mode)
			if err := h.Write(ctx, 0, []byte("close-sync content")); err != nil {
				t.Fatal(err)
			}
			if err := h.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			n, ok := b.Node("/mode.txt")
			if !ok || string(n.Data) != "close-sync content" {
				t.Fatalf("remote = %q ok=%v, want close-sync content", n.Data, ok)
			}
			wantMode := mode & 0o777
			if mode == 0 {
				wantMode = 0o644 // no chmod for a zero create mode
			}
			if n.Mode&0o777 != wantMode {
				t.Fatalf("remote mode = %o, want %o", n.Mode&0o777, wantMode)
			}
			puts, chmods := rec.count("PUT"), rec.count("CHMOD")
			if puts != 1 {
				t.Fatalf("PUT count = %d, want 1", puts)
			}
			wantChmods := 1
			if mode == 0 {
				wantChmods = 0
			}
			if chmods != wantChmods {
				t.Fatalf("CHMOD count = %d, want %d", chmods, wantChmods)
			}
			// Release of the already-committed handle must not re-upload.
			if err := h.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if got := rec.count("PUT"); got != 1 {
				t.Fatalf("release re-uploaded committed content: PUTs=%d", got)
			}
		})
	}
}

// v1 TestCloseSyncCombinedModeRequiresAdvertisedContract (fs-level half):
// without the advertised combined contract the commit is content-only and
// the mode follows as a separate chmod — ordered strictly after the content
// upload so a mode can never describe bytes that are not durable yet. The
// queue-level capability matrix is commit_queue_mode_port_test.go's
// TestCommitQueueBatchModeRequiresCapability.
func TestPortCloseSyncUnadvertisedModeAppliesChmodAfterContent(t *testing.T) {
	rec := &methodRecorder{}
	ch, b := newModeStack(t, rec)
	ctx := context.Background()

	h := createModeFile(t, ch, "/unadvertised.txt", 0o755)
	if err := h.Write(ctx, 0, []byte("close-sync content")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := b.Node("/unadvertised.txt")
	if string(n.Data) != "close-sync content" || n.Mode&0o777 != 0o755 {
		t.Fatalf("remote = %q mode=%o", n.Data, n.Mode&0o777)
	}
	events := rec.snapshot()
	putAt, chmodAt := -1, -1
	for i, e := range events {
		switch e {
		case "PUT":
			if putAt < 0 {
				putAt = i
			}
		case "CHMOD":
			if chmodAt < 0 {
				chmodAt = i
			}
		}
	}
	if putAt < 0 || chmodAt < 0 || chmodAt < putAt {
		t.Fatalf("event order = %v, want content PUT before mode CHMOD", events)
	}
}

// B10.4 (v1 TestCloseSyncCombinedModeFallsBackForUnsupportedEndpoint's new
// shape): an empty creation prefers the CreateFileCtx endpoint; when that
// endpoint fails, the conditional write lands the file instead. A create
// mode other than the default applies as a chmod after either route.
func TestPortCloseSyncEmptyCreatePrefersCreateEndpoint(t *testing.T) {
	for _, tc := range []struct {
		name        string
		createFails bool
		mode        uint32
		wantEvents  []string
	}{
		{"create-endpoint", false, 0o644, []string{"CREATE"}},
		{"create-fallback", true, 0o644, []string{"CREATE", "PUT"}},
		{"create-mode", false, 0o755, []string{"CREATE", "CHMOD"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &methodRecorder{}
			b := testbackend.NewBackend()
			ts := httptest.NewServer(rec.wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Has("create") && tc.createFails {
					http.Error(w, `{"error":"create unavailable"}`, http.StatusInternalServerError)
					return
				}
				b.ServeHTTP(w, r)
			})))
			t.Cleanup(ts.Close)
			cl := client.New(ts.URL, "")
			wb, err := New(Options{Policy: PolicyCloseSync, Client: cl})
			if err != nil {
				t.Fatal(err)
			}
			terminal, err := core.New(cl, core.Options{})
			if err != nil {
				t.Fatal(err)
			}
			ch, err := fs.NewChain(wb, terminal)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			h := createModeFile(t, ch, "/probe.txt", tc.mode)
			if err := h.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			n, ok := b.Node("/probe.txt")
			if !ok || len(n.Data) != 0 {
				t.Fatalf("empty create vanished: ok=%v data=%q", ok, n.Data)
			}
			events := rec.snapshot()
			trimmed := events[:0]
			for _, e := range events {
				if e == "HEAD" || e == "GET" {
					continue // stat preflights are not part of the route contract
				}
				trimmed = append(trimmed, e)
			}
			if fmt.Sprint(trimmed) != fmt.Sprint(tc.wantEvents) {
				t.Fatalf("route = %v, want %v", trimmed, tc.wantEvents)
			}
			if tc.mode == 0o755 && n.Mode&0o777 != 0o755 {
				t.Fatalf("create mode = %o, want 755", n.Mode&0o777)
			}
			if err := h.Release(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// v1 TestCloseSyncCombinedModeForbiddenFallsBackWithoutBypassingPermissions,
// adapted to the new plane: a chmod denied after the content commit fails the
// flush while the content stays durably committed; the new inline path
// consumes the deferred mode at that boundary, so a retry neither re-uploads
// the content nor re-applies the denied mode.
func TestPortCloseSyncModeFailureKeepsCommittedContent(t *testing.T) {
	rec := &methodRecorder{}
	b := testbackend.NewBackend()
	ts := httptest.NewServer(rec.wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("chmod") {
			http.Error(w, `{"error":"chmod is owner-only"}`, http.StatusForbidden)
			return
		}
		b.ServeHTTP(w, r)
	})))
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyCloseSync, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	h := createModeFile(t, ch, "/scoped.txt", 0o644)
	if err := h.Write(ctx, 0, []byte("close-sync content")); err != nil {
		t.Fatal(err)
	}
	// A chmod on the dirty path defers to the commit boundary.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/scoped.txt", Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err == nil {
		t.Fatal("denied mode returned success")
	}
	n, _ := b.Node("/scoped.txt")
	if string(n.Data) != "close-sync content" {
		t.Fatalf("denied mode lost committed content: %q", n.Data)
	}
	if n.Mode&0o777 != 0o644 {
		t.Fatalf("denied mode was applied: %o", n.Mode&0o777)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatalf("retry flush = %v", err)
	}
	if got := rec.count("PUT"); got != 1 {
		t.Fatalf("retry re-uploaded content: PUTs=%d", got)
	}
	if got := rec.count("CHMOD"); got != 1 {
		t.Fatalf("retry re-applied denied mode: CHMODs=%d", got)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

// v1 TestCloseSyncCombinedModePreservesConcurrentChmod and
// TestCloseSyncCombinedModeConcurrentChmodFailureKeepsCommittedCAS: a chmod
// racing an in-flight upload defers onto the handle and, because the new
// inline path applies the pending mode only after the content lands, the
// newer mode wins — or its failure fails the flush with the content kept.
func TestPortCloseSyncConcurrentChmodDuringUpload(t *testing.T) {
	for _, tc := range []struct {
		name       string
		chmodFails bool
		wantMode   uint32
		wantErr    bool
	}{
		{"newer-mode-wins", false, 0o600, false},
		{"newer-mode-failure-keeps-content", true, 0o644, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			rec := &methodRecorder{}
			b := testbackend.NewBackend()
			ts := httptest.NewServer(rec.wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/race.txt") {
					once.Do(func() { close(entered) })
					<-release
				}
				if r.URL.Query().Has("chmod") && tc.chmodFails {
					http.Error(w, `{"error":"chmod is owner-only"}`, http.StatusForbidden)
					return
				}
				b.ServeHTTP(w, r)
			})))
			t.Cleanup(ts.Close)
			cl := client.New(ts.URL, "")
			wb, err := New(Options{Policy: PolicyCloseSync, Client: cl, RemoteCommitWaitTimeout: 10 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			terminal, err := core.New(cl, core.Options{})
			if err != nil {
				t.Fatal(err)
			}
			ch, err := fs.NewChain(wb, terminal)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				select {
				case <-release:
				default:
					close(release)
				}
			})
			ctx := context.Background()

			h := createModeFile(t, ch, "/race.txt", 0o644)
			if err := h.Write(ctx, 0, []byte("close-sync content")); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- h.Flush(ctx) }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("upload did not start")
			}
			if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/race.txt", Mode: 0o600}); err != nil {
				t.Fatalf("concurrent chmod: %v", err)
			}
			close(release)
			select {
			case err := <-done:
				if tc.wantErr && err == nil {
					t.Fatal("failed newer chmod returned success")
				}
				if !tc.wantErr && err != nil {
					t.Fatalf("flush: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("flush did not finish")
			}
			n, _ := b.Node("/race.txt")
			if string(n.Data) != "close-sync content" {
				t.Fatalf("remote content = %q", n.Data)
			}
			if n.Mode&0o777 != tc.wantMode {
				t.Fatalf("remote mode = %o, want %o", n.Mode&0o777, tc.wantMode)
			}
		})
	}
}

// v1 TestCloseSyncCombinedModeErrorsKeepDirtyWithoutFallback (new shape): a
// failed content upload keeps the buffered image dirty so the next boundary
// retries — one PUT per flush attempt, no batch or alternate route involved.
func TestPortCloseSyncCommitFailureKeepsDirtyForRetry(t *testing.T) {
	var mu sync.Mutex
	var puts int
	var committed []byte
	fail := true
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/status":
			_, _ = w.Write([]byte(`{"inline_threshold":50000}`))
		case r.Method == http.MethodHead:
			if committed != nil {
				w.Header().Set("Content-Length", fmt.Sprint(len(committed)))
				w.Header().Set("X-Dat9-IsDir", "false")
				w.Header().Set("X-Dat9-Revision", "1")
				w.WriteHeader(http.StatusOK)
				return
			}
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPut:
			puts++
			if fail {
				http.Error(w, `{"error":"temporary"}`, http.StatusInternalServerError)
				return
			}
			body, _ := io.ReadAll(r.Body)
			committed = append([]byte(nil), body...)
			_, _ = w.Write([]byte(`{"revision":1}`))
		default:
			_, _ = io.Copy(io.Discard, r.Body)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		}
	}))
	defer ts.Close()
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyCloseSync, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	h := createModeFile(t, ch, "/error.txt", 0o644)
	if err := h.Write(ctx, 0, []byte("close-sync content")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err == nil {
		t.Fatal("failed upload returned success")
	}
	mu.Lock()
	if puts != 1 {
		t.Fatalf("PUT attempts = %d, want 1 (no fallback route)", puts)
	}
	fail = false
	mu.Unlock()
	if err := h.Flush(ctx); err != nil {
		t.Fatalf("explicit retry: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if puts != 2 {
		t.Fatalf("retry PUT attempts = %d, want 2", puts)
	}
	if string(committed) != "close-sync content" {
		t.Fatalf("retry landed %q", committed)
	}
}

// v1 TestCloseSyncCombinedModeLostAckDoesNotOverwriteCommittedFile, adapted
// to the new inline semantics: when the create PUT's acknowledgement is lost
// (the server committed the bytes but the connection died), the retried
// flush re-stats the path and lands the SAME image against the committed
// revision instead of failing forever — the new inline path refreshes the
// CAS base per attempt, where v1 pinned the create-only base at zero.
func TestPortCloseSyncLostAckRetryLandsSameImage(t *testing.T) {
	var mu sync.Mutex
	remote := map[string][]byte{}
	revisions := map[string]int64{}
	hijacked := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		path := strings.TrimPrefix(r.URL.Path, "/v1/fs")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/status":
			_, _ = w.Write([]byte(`{"inline_threshold":50000}`))
		case r.Method == http.MethodHead:
			if data, ok := remote[path]; ok {
				w.Header().Set("Content-Length", fmt.Sprint(len(data)))
				w.Header().Set("X-Dat9-IsDir", "false")
				w.Header().Set("X-Dat9-Revision", fmt.Sprint(revisions[path]))
				w.WriteHeader(http.StatusOK)
				return
			}
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			if !hijacked {
				// The server commits the transaction, then the response is lost.
				hijacked = true
				remote[path] = append([]byte(nil), body...)
				revisions[path]++
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				_ = conn.Close()
				return
			}
			remote[path] = append([]byte(nil), body...)
			revisions[path]++
			_, _ = w.Write([]byte(`{"revision":2}`))
		default:
			_, _ = io.Copy(io.Discard, r.Body)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		}
	}))
	defer ts.Close()
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyCloseSync, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	h := createModeFile(t, ch, "/lost-ack.txt", 0o644)
	if err := h.Write(ctx, 0, []byte("close-sync content")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err == nil {
		t.Fatal("lost acknowledgement returned success")
	}
	mu.Lock()
	if got := remote["/lost-ack.txt"]; string(got) != "close-sync content" {
		t.Fatalf("unacknowledged remote image = %q", got)
	}
	mu.Unlock()
	// The retry re-stats the committed image and re-lands the same bytes.
	if err := h.Flush(ctx); err != nil {
		t.Fatalf("retry after lost ack: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if got := remote["/lost-ack.txt"]; string(got) != "close-sync content" {
		t.Fatalf("retry changed committed bytes: %q", got)
	}
	if revisions["/lost-ack.txt"] < 1 {
		t.Fatalf("no revision ever committed: %v", revisions)
	}
}

// v1 TestCloseSyncCombinedModeKeepsMultipartBoundary +
// TestCloseSyncCombinedModeAcceptsLastInlineSize: routing follows the
// negotiated inline threshold — a file under it lands as one direct PUT,
// a file at or above it goes through the multipart protocol, and a multipart
// failure keeps the image dirty for the retry boundary.
func TestPortCloseSyncThresholdRoutesInlineVsMultipart(t *testing.T) {
	const payload = "close-sync content"
	for _, tc := range []struct {
		name        string
		threshold   int64
		multipartOK bool
		wantFlushOK bool
	}{
		{"last-inline-size", int64(len(payload)) + 1, true, true},
		{"first-multipart-size", int64(len(payload)), true, true},
		{"multipart-failure", int64(len(payload)), false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &methodRecorder{}
			b := testbackend.NewBackend()
			ts := httptest.NewServer(rec.wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate" && !tc.multipartOK {
					http.Error(w, `{"error":"unavailable"}`, http.StatusServiceUnavailable)
					return
				}
				b.ServeHTTP(w, r)
			})))
			defer ts.Close()
			cl := newTestClient(ts.URL)
			cl.SetSmallFileThresholdForTests(tc.threshold)
			wb, err := New(Options{Policy: PolicyCloseSync, Client: cl})
			if err != nil {
				t.Fatal(err)
			}
			terminal, err := core.New(cl, core.Options{})
			if err != nil {
				t.Fatal(err)
			}
			ch, err := fs.NewChain(wb, terminal)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()

			h := createModeFile(t, ch, "/boundary.bin", 0o644)
			if err := h.Write(ctx, 0, []byte(payload)); err != nil {
				t.Fatal(err)
			}
			err = h.Flush(ctx)
			if tc.wantFlushOK && err != nil {
				t.Fatalf("flush: %v", err)
			}
			if !tc.wantFlushOK && err == nil {
				t.Fatal("multipart failure returned success")
			}
			if tc.threshold > int64(len(payload)) {
				if got := rec.count("INITIATE"); got != 0 {
					t.Fatalf("inline file initiated multipart: %d", got)
				}
				if got := rec.count("PUT"); got != 1 {
					t.Fatalf("direct PUTs = %d, want 1", got)
				}
			} else {
				if got := rec.count("INITIATE"); got < 1 {
					t.Fatal("threshold-size file did not use multipart")
				}
			}
			if tc.wantFlushOK {
				if n, ok := b.Node("/boundary.bin"); !ok || string(n.Data) != payload {
					t.Fatalf("remote = %q ok=%v, want %q", n.Data, ok, payload)
				}
			}
		})
	}
}

// v1 TestCloseSyncCombinedModeCannotBypassLocalBarrier (spill variant): the
// large-file close path stages into the shadow ledger first; when that local
// durability step fails, the flush fails before any remote traffic.
func TestPortCloseSyncLargeFileLocalBarrierBeforeRemote(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	dir := t.TempDir()
	ledger, err := staging.NewStaging(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledger.Close)
	wb, err := New(Options{Policy: PolicyCloseSync, Client: cl, Staging: ledger})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	h := createModeFile(t, ch, "/local-barrier.bin", 0o644)
	payload := make([]byte, commitInlineStreamThreshold+64)
	for i := range payload {
		payload[i] = byte(i)
	}
	if err := h.Write(ctx, 0, payload); err != nil {
		t.Fatal(err)
	}
	// Break local durability: the shadow plane cannot create its spill file
	// once the shadow directory is unwritable.
	if err := os.Chmod(filepath.Join(dir, "shadow"), 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(filepath.Join(dir, "shadow"), 0o700) })
	before := len(b.Requests())
	if err := h.Flush(ctx); err == nil {
		t.Fatal("flush bypassed the local staging barrier")
	}
	if after := len(b.Requests()); after != before {
		t.Fatalf("failed local barrier reached the remote: %d -> %d requests", before, after)
	}
}

// v1 TestCloseSyncCombinedModeKeepsMultipartBoundary's large-file row (B4):
// a close-sync image at or above the inline-stream threshold commits through
// the shadow-spill staging path and the flush waits for the path's OWN
// landing — it must not return while the upload is held, and it must not
// materialize the whole image on the inline path.
func TestPortCloseSyncLargeFileSpillsAndWaitsOwnLanding(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	b := testbackend.NewBackend()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/complete") {
			once.Do(func() { close(entered) })
			<-release
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	cl := client.New(ts.URL, "")
	ledger, err := staging.NewStaging(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledger.Close)
	wb, err := New(Options{Policy: PolicyCloseSync, Client: cl, Staging: ledger})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	h := createModeFile(t, ch, "/big.bin", 0o644)
	payload := make([]byte, commitInlineStreamThreshold+64)
	for i := range payload {
		payload[i] = byte(i)
	}
	if err := h.Write(ctx, 0, payload); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- h.Flush(ctx) }()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("flush returned before the own landing: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start")
	}
	select {
	case err := <-done:
		t.Fatalf("flush returned while its own commit was blocked: %v", err)
	default:
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("flush: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("flush did not finish after release")
	}
	n, ok := b.Node("/big.bin")
	if !ok || !bytes.Equal(n.Data, payload) {
		t.Fatalf("remote = ok=%t len=%d, want the full %d-byte image", ok, len(n.Data), len(payload))
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

// v1 TestCloseSyncCombinedModeReprobesAfterCooldown (new shape): the new
// package has no probe cooldown — after the batch endpoint answers
// unsupported, batching is disabled permanently and later commits go straight
// to the single-entry path.
func TestPortCloseSyncBatchUnsupportedDisablesBatchingPermanently(t *testing.T) {
	var batchCalls atomic.Int32
	var putCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/status":
			_, _ = w.Write([]byte(`{"inline_threshold":50000}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-write":
			batchCalls.Add(1)
			http.NotFound(w, r)
		case r.Method == http.MethodPut:
			putCalls.Add(1)
			_, _ = w.Write([]byte(`{"status":"ok","revision":1}`))
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	shadow, err := staging.NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := newTestClient(ts.URL)
	c.Warm(context.Background())
	cq := NewCommitQueue(c, shadow, pending, nil, 1, 8)
	defer cq.DrainAll()
	// A wide window keeps the two entries in one batch even on loaded
	// CI runners; the contract under test is the endpoint probe, not the
	// coalescing latency.
	cq.ConfigureBatchWrite(250*time.Millisecond, 64, client.MaxBatchWriteBytes)

	// Round 1: two same-window entries try the batch, get 404, fall back.
	for _, name := range []string{"/a.txt", "/b.txt"} {
		if err := shadow.WriteFull(name, []byte("x"), 0); err != nil {
			t.Fatal(err)
		}
		gen, err := pending.PutWithBaseRev(name, 1, staging.PendingNew, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := cq.Enqueue(attachTestStagingGens(shadow, pending, &CommitEntry{Path: name, Size: 1, Kind: staging.PendingNew, ShadowGen: shadow.ActiveGeneration(name), PendingIndexGen: gen})); err != nil {
			t.Fatal(err)
		}
	}
	waitCommitQueueIdle(t, cq)
	if batchCalls.Load() != 1 || putCalls.Load() != 2 {
		t.Fatalf("round 1: batch=%d put=%d, want 1/2", batchCalls.Load(), putCalls.Load())
	}

	// Round 2: batching is off — no further batch probes, direct PUTs only.
	for _, name := range []string{"/c.txt", "/d.txt"} {
		if err := shadow.WriteFull(name, []byte("y"), 0); err != nil {
			t.Fatal(err)
		}
		gen, err := pending.PutWithBaseRev(name, 1, staging.PendingNew, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := cq.Enqueue(attachTestStagingGens(shadow, pending, &CommitEntry{Path: name, Size: 1, Kind: staging.PendingNew, ShadowGen: shadow.ActiveGeneration(name), PendingIndexGen: gen})); err != nil {
			t.Fatal(err)
		}
	}
	waitCommitQueueIdle(t, cq)
	if batchCalls.Load() != 1 || putCalls.Load() != 4 {
		t.Fatalf("round 2: batch=%d put=%d, want batch stays 1 and 4 total PUTs", batchCalls.Load(), putCalls.Load())
	}
}
