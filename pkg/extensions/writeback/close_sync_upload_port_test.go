package writeback

// Ports of v1 pkg/fuse/close_sync_upload_test.go (the rows not already in
// shadow_durability_port_test.go, which carries the generation-fenced
// shadow-remote-durability pair): the flush-tier durability contract —
// which policy/sync-mode combination owes the remote acknowledgement at
// which boundary, upload-failure retry data retention, fsync local
// barriers, crash recovery of an acknowledged close-sync commit, and
// empty-vs-multipart upload routing.
//
// Not ported from v1:
//   - TestCloseSyncAppendLogFsyncFallbackKeepsLocalBarrier: the append-log
//     matcher and its fsync fallback are not part of this package;
//   - TestCloseSyncShadowDurabilityGuard's per-field rows (foregroundShadow-
//     UploadDurabilityLocked internals): the new plane encodes the guard
//     structurally — close-sync always lands remotely at flush, writeback
//     stages locally, interactive only changes fsync under writeback — which
//     the policy matrix below pins through the public path.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// newUploadStack wires a full stack (optionally with the staging ledger)
// over a custom http.Handler.
func newUploadStack(t *testing.T, handler http.Handler, opts Options) (*fs.Chain, *Writeback) {
	t.Helper()
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	opts.Client = cl
	if opts.Staging == nil {
		ledger, err := staging.NewStaging(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(ledger.Close)
		opts.Staging = ledger
	}
	wb, err := New(opts)
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
	return ch, wb
}

// v1 TestCloseSyncShadowUploadSkipsLocalSyncOnlyForCloseSync, re-expressed
// against the new plane's flush tiers: close-sync (strict or interactive)
// owes the remote acknowledgement at the flush boundary; writeback's flush
// returns after the local staging boundary and the remote lands with the
// drain. (v1 let interactive close-sync skip the remote wait; the new plane
// restricts SyncMode=interactive to fsync under writeback.)
func TestPortCloseSyncFlushPolicyMatrix(t *testing.T) {
	for _, tc := range []struct {
		name       string
		policy     Policy
		syncMode   string
		withQueue  bool
		wantAtOnce bool
	}{
		{"close-sync-strict", PolicyCloseSync, "strict", true, true},
		{"close-sync-interactive", PolicyCloseSync, "interactive", true, true},
		{"writeback", PolicyWriteBack, "strict", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := testbackend.NewBackend()
			ledger, err := staging.NewStaging(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(ledger.Close)
			ch, wb := newUploadStack(t, b, Options{Policy: tc.policy, SyncMode: tc.syncMode, Staging: ledger})
			ctx := context.Background()

			h := open(t, ch, "/matrix.txt")
			if err := h.Write(ctx, 0, []byte("close-sync content")); err != nil {
				t.Fatal(err)
			}
			if err := h.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			n, ok := b.Node("/matrix.txt")
			if tc.wantAtOnce {
				if !ok || string(n.Data) != "close-sync content" {
					t.Fatalf("remote at flush = %q ok=%v, want the committed image", n.Data, ok)
				}
			} else if ok {
				t.Fatal("writeback flush published remotely before the drain")
			}
			if err := wb.Drain(ctx); err != nil {
				t.Fatal(err)
			}
			if n, ok := b.Node("/matrix.txt"); !ok || string(n.Data) != "close-sync content" {
				t.Fatalf("remote after drain = %q ok=%v", n.Data, ok)
			}
		})
	}
}

// v1 TestCloseSyncShadowUploadWaitsForRemoteAck: the close-sync flush must
// not return before the remote acknowledgement, and the acknowledged
// generation must not be re-uploaded at release.
func TestPortCloseSyncFlushWaitsForRemoteAck(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	rec := &methodRecorder{}
	b := testbackend.NewBackend()
	ts := httptest.NewServer(rec.wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/pinned.txt") {
			once.Do(func() { close(entered) })
			<-release
		}
		b.ServeHTTP(w, r)
	})))
	defer ts.Close()
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
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

	h := open(t, ch, "/pinned.txt")
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
	select {
	case err := <-done:
		t.Fatalf("flush returned before remote acknowledgement: %v", err)
	default:
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("flush: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("flush did not finish")
	}
	if n, _ := b.Node("/pinned.txt"); string(n.Data) != "close-sync content" {
		t.Fatalf("remote = %q", n.Data)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if got := rec.count("PUT"); got != 1 {
		t.Fatalf("release re-uploaded the acknowledged generation: PUTs=%d", got)
	}
}

// v1 TestCloseSyncShadowUploadFailureRetainsInProcessRetryableData: a failed
// spill upload leaves the staged image recoverable from the shadow, and the
// retry lands exactly those bytes.
func TestPortCloseSyncShadowUploadFailureRetainsRetryableData(t *testing.T) {
	var mu sync.Mutex
	fail := true
	puts := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/status":
			_, _ = w.Write([]byte(`{"inline_threshold":50000}`))
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/retry.txt"):
			mu.Lock()
			puts++
			failNow := fail
			mu.Unlock()
			if failNow {
				http.Error(w, `{"error":"temporary"}`, http.StatusInternalServerError)
				return
			}
			_, _ = io.Copy(io.Discard, r.Body)
			_, _ = w.Write([]byte(`{"revision":1}`))
		default:
			_, _ = io.Copy(io.Discard, r.Body)
			_, _ = w.Write([]byte(`{}`))
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
	const path = "/retry.txt"
	if err := shadow.WriteFull(path, []byte("close-sync content"), 0); err != nil {
		t.Fatal(err)
	}
	gen, err := pending.PutShadowSpillWithMode(path, int64(len("close-sync content")), staging.PendingNew, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	defer cq.DrainAll()
	entry := &CommitEntry{
		Path: path, Size: int64(len("close-sync content")), Kind: staging.PendingNew,
		ShadowSpill: true, ShadowGen: shadow.ActiveGeneration(path), PendingIndexGen: gen,
	}
	if _, err := cq.uploadEntry(context.Background(), entry); err == nil {
		t.Fatal("failed upload returned success")
	}
	data, rerr := shadow.ReadAll(path)
	if rerr != nil || string(data) != "close-sync content" {
		t.Fatalf("retry data = %q, %v", data, rerr)
	}
	mu.Lock()
	fail = false
	mu.Unlock()
	if _, err := cq.uploadEntry(context.Background(), entry); err != nil {
		t.Fatalf("retry: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if puts != 2 {
		t.Fatalf("PUT attempts = %d, want 2", puts)
	}
}

// v1 TestCloseSyncInteractiveFsyncKeepsLocalBarrier: under writeback +
// interactive, fsync is served by the local staging ledger — when staging
// itself fails, fsync fails before any remote traffic.
func TestPortCloseSyncInteractiveFsyncKeepsLocalBarrier(t *testing.T) {
	b := testbackend.NewBackend()
	dir := t.TempDir()
	ledger, err := staging.NewStaging(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledger.Close)
	ch, _ := newUploadStack(t, b, Options{Policy: PolicyWriteBack, SyncMode: "interactive", Staging: ledger})
	ctx := context.Background()

	h := open(t, ch, "/staged.txt")
	if err := h.Write(ctx, 0, []byte("close-sync content")); err != nil {
		t.Fatal(err)
	}
	// Break the local ledger: new staging cannot create its shadow file.
	if err := chmodShadowDir(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = chmodShadowDir(dir, 0o700) })
	before := len(b.Requests())
	if err := h.Sync(ctx); err == nil {
		t.Fatal("interactive fsync skipped the local staging barrier")
	}
	if after := len(b.Requests()); after != before {
		t.Fatalf("failed local barrier reached the remote: %d -> %d requests", before, after)
	}
}

// v1 TestCloseSyncStrictFsyncKeepsLocalBarrier, adapted to the new plane:
// strict fsync's durability barrier IS the remote landing — it fails when
// the remote fails and succeeds once the image is durable.
func TestPortCloseSyncStrictFsyncBarrierIsRemote(t *testing.T) {
	var mu sync.Mutex
	fail := true
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/status":
			_, _ = w.Write([]byte(`{"inline_threshold":50000}`))
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/fsync.txt"):
			mu.Lock()
			failNow := fail
			mu.Unlock()
			if failNow {
				http.Error(w, `{"error":"temporary"}`, http.StatusInternalServerError)
				return
			}
			_, _ = io.Copy(io.Discard, r.Body)
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

	h := open(t, ch, "/fsync.txt")
	if err := h.Write(ctx, 0, []byte("close-sync content")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err == nil {
		t.Fatal("explicit fsync reported durability without the remote barrier")
	}
	mu.Lock()
	fail = false
	mu.Unlock()
	if err := h.Sync(ctx); err != nil {
		t.Fatalf("retry fsync: %v", err)
	}
}

// v1 TestCloseSyncShadowCommitHasNoRecoveryUpload: an acknowledged
// close-sync commit must not be replayed as an upload after a crash — a
// shadow file orphaned by the crash (whole, or torn by lost page-cache
// bytes) must not turn into recovery traffic; the pending metadata the
// success cleanup removed is the only recovery source.
func TestPortCloseSyncCommitHasNoRecoveryUpload(t *testing.T) {
	for _, tc := range []struct {
		name string
		torn bool
	}{
		{"surviving-shadow", false},
		{"torn-shadow", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := testbackend.NewBackend()
			ts := httptest.NewServer(b)
			t.Cleanup(ts.Close)
			cl := client.New(ts.URL, "")
			dir := t.TempDir()
			ledger, err := staging.NewStaging(dir)
			if err != nil {
				t.Fatal(err)
			}
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

			h := open(t, ch, "/recover.txt")
			if err := h.Write(ctx, 0, []byte("close-sync content")); err != nil {
				t.Fatal(err)
			}
			if err := h.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if err := h.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if err := wb.Drain(ctx); err != nil {
				t.Fatal(err)
			}
			puts := b.Requests()["PUT /v1/fs/recover.txt"]

			// Model the crash: the success cleanup removed the pending entry,
			// but the shadow file survived (optionally torn by lost page-cache
			// bytes). The mount wiring replays the journal and Start drives
			// RecoverFromDisk + RecoverPending — orphan bytes alone must never
			// become recovery traffic.
			ledger.Close()
			reopened, err := staging.NewStaging(dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(reopened.Close)
			orphan := []byte("close-sync content")
			if tc.torn {
				orphan = orphan[:3]
			}
			if err := reopened.Shadow.WriteFull("/recover.txt", orphan, 1); err != nil {
				t.Fatal(err)
			}
			if err := reopened.Recover(); err != nil { // mount wiring: journal replay
				t.Fatal(err)
			}
			wb2, err := New(Options{Policy: PolicyCloseSync, Client: client.New(ts.URL, ""), Staging: reopened})
			if err != nil {
				t.Fatal(err)
			}
			if err := wb2.Start(ctx); err != nil {
				t.Fatal(err)
			}
			if err := wb2.Drain(ctx); err != nil {
				t.Fatal(err)
			}
			if got := b.Requests()["PUT /v1/fs/recover.txt"]; got != puts {
				t.Fatalf("acknowledged path replayed: PUTs %d -> %d", puts, got)
			}
			if n, ok := b.Node("/recover.txt"); !ok || string(n.Data) != "close-sync content" {
				t.Fatalf("recovery disturbed the committed image: %q ok=%v", n.Data, ok)
			}
		})
	}
}

// v1 TestCloseSyncShadowUploadEmptyAndMultipart, adapted to the new routing:
// an empty truncation lands through the create endpoint (B10.4), an image
// above the negotiated inline threshold lands through multipart.
func TestPortCloseSyncEmptyAndMultipartRouting(t *testing.T) {
	for _, tc := range []struct {
		size int64
	}{
		{0},
		{256 * 1024},
	} {
		t.Run(fmt.Sprint(tc.size), func(t *testing.T) {
			rec := &methodRecorder{}
			b := testbackend.NewBackend()
			ts := httptest.NewServer(rec.wrap(b))
			defer ts.Close()
			cl := newTestClient(ts.URL)
			cl.SetSmallFileThresholdForTests(128 * 1024)
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

			h := open(t, ch, "/payload.bin")
			payload := make([]byte, 256*1024)
			for i := range payload {
				payload[i] = byte(i)
			}
			if err := h.Write(ctx, 0, payload); err != nil {
				t.Fatal(err)
			}
			if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/payload.bin", Size: tc.size}); err != nil {
				t.Fatal(err)
			}
			if err := h.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			n, ok := b.Node("/payload.bin")
			if !ok || int64(len(n.Data)) != tc.size {
				t.Fatalf("remote size = %d ok=%v, want %d", len(n.Data), ok, tc.size)
			}
			if tc.size == 0 {
				if got := rec.count("CREATE"); got != 1 {
					t.Fatalf("empty truncation route: CREATE=%d, want 1", got)
				}
				if got := rec.count("INITIATE"); got != 0 {
					t.Fatalf("empty truncation initiated multipart: %d", got)
				}
			} else {
				if got := rec.count("INITIATE"); got < 1 {
					t.Fatal("256KiB image did not use multipart")
				}
				for i := 0; i < len(n.Data); i += 4093 {
					if n.Data[i] != payload[i] {
						t.Fatalf("byte %d = %d, want %d", i, n.Data[i], payload[i])
					}
				}
			}
		})
	}
}

// chmodShadowDir changes the staging root's shadow directory permissions.
func chmodShadowDir(dir string, mode uint32) error {
	return os.Chmod(filepath.Join(dir, "shadow"), os.FileMode(mode))
}
