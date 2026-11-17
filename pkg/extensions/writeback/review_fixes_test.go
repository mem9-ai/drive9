package writeback

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

// Finding #13: retarget updates liveByPath so closing one renamed handle
// promotes the surviving sibling, untrackAndLast reports false, and
// acknowledgeUploadedCreate reaches renamed handles.
func TestRetargetUpdatesLiveByPathAndPromotesSurvivingSibling(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/a", []byte("seed"))
	ts := httptest.NewServer(b)
	defer ts.Close()

	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl})
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

	res1, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/a", Flags: syscall.O_RDWR})
	if err != nil {
		t.Fatal(err)
	}
	res2, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/a", Flags: syscall.O_RDWR})
	if err != nil {
		t.Fatal(err)
	}
	h1 := res1.Handle.(*bufferingHandle)
	h2 := res2.Handle.(*bufferingHandle)

	wb.retarget("/a", "/b")

	wb.regMu.Lock()
	oldCount := len(wb.liveByPath["/a"])
	newCount := len(wb.liveByPath["/b"])
	wb.regMu.Unlock()
	if oldCount != 0 || newCount != 2 {
		t.Fatalf("liveByPath counts after retarget: /a=%d /b=%d, want 0/2", oldCount, newCount)
	}

	// Closing h2 (the current live["/b"] slot) must NOT report last close and must promote h1.
	if err := h2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if got := wb.handleFor("/b"); got != h1 {
		t.Fatalf("handleFor(/b) after h2 close = %v, want promoted h1 (%v)", got, h1)
	}

	// Closing h1 cleans up /b completely.
	if err := h1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	wb.regMu.Lock()
	remB := len(wb.liveByPath["/b"])
	wb.regMu.Unlock()
	if remB != 0 {
		t.Fatalf("liveByPath[/b] after all closes = %d, want 0", remB)
	}
}

// Finding #6: Failed post-adoption chmod retains pendingMode and retries on
// migrated Sync/Flush/Release until it succeeds.
func TestPostAdoptionChmodFailureRetainedUntilMigratedSync(t *testing.T) {
	var failChmod atomic.Bool
	failChmod.Store(true)
	var appliedMode atomic.Uint32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := r.URL.Query()["chmod"]; ok {
			if failChmod.Load() {
				http.Error(w, "chmod unavailable", http.StatusServiceUnavailable)
				return
			}
			appliedMode.Store(0o755)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cl := client.New(srv.URL, "")
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	h := &bufferingHandle{
		w:              wb,
		path:           "/migrated.bin",
		landed:         true,
		pendingMode:    0o755,
		hasPendingMode: true,
	}
	wb.track(h)
	defer wb.untrack(h)

	stubExt := &noopExtentHandle{}
	// Make retryPostUploadMode fail fast by canceling after initial attempts?
	// Instead, arm pendingMode directly on an already-adopted handle or let
	// flushMigratedMode run against failChmod=true then false.
	h.mu.Lock()
	h.extent = stubExt
	h.mu.Unlock()

	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	if serr := h.Sync(cctx); serr == nil {
		t.Fatal("migrated Sync with failing deferred chmod must return error")
	}
	h.mu.Lock()
	stillPending := h.hasPendingMode
	h.mu.Unlock()
	if !stillPending {
		t.Fatal("failed deferred chmod was dropped instead of retained")
	}

	failChmod.Store(false)
	if serr := h.Sync(context.Background()); serr != nil {
		t.Fatalf("migrated Sync after server recovery: %v", serr)
	}
	if appliedMode.Load() != 0o755 {
		t.Fatalf("appliedMode = %o, want 0755", appliedMode.Load())
	}
	h.mu.Lock()
	stillPending = h.hasPendingMode
	h.mu.Unlock()
	if stillPending {
		t.Fatal("pendingMode remained set after successful Sync")
	}
}

type noopExtentHandle struct{}

func (*noopExtentHandle) Read(context.Context, int64, []byte) (int, error) { return 0, nil }
func (*noopExtentHandle) Write(context.Context, int64, []byte) error       { return nil }
func (*noopExtentHandle) Flush(context.Context) error                      { return nil }
func (*noopExtentHandle) Sync(context.Context) error                       { return nil }
func (*noopExtentHandle) Release(context.Context) error                    { return nil }

// Finding #2: reclaimEmptyOccupant refuses directories, CASes without DELETE on
// classic overwrites when revision > 0, and sends conditional file DELETE when
// promoting to extent (refusing if a competing writer advanced revision).
func TestReclaimEmptyOccupantFencesAgainstCompetingWriterAndDirs(t *testing.T) {
	var deleteSeen atomic.Int32
	var curRev atomic.Int64
	curRev.Store(1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			if r.URL.Path == "/v1/fs/dir" {
				w.Header().Set("X-Dat9-IsDir", "true")
				w.WriteHeader(http.StatusOK)
				return
			}
			w.Header().Set("Content-Length", "0")
			w.Header().Set("X-Dat9-Revision", "1")
			w.Header().Set("X-Dat9-Resource-ID", "orig-id")
			w.WriteHeader(http.StatusOK)
			// Simulate competing writer advancing revision between HEAD and DELETE!
			curRev.Store(2)
		case http.MethodDelete:
			deleteSeen.Add(1)
			if r.URL.Query().Get("kind") != "file" {
				http.Error(w, "missing kind=file", http.StatusBadRequest)
				return
			}
			if r.Header.Get("X-Dat9-Expected-Revision") != "2" && curRev.Load() == 2 {
				http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
				return
			}
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	cl := client.New(srv.URL, "")
	cq := &CommitQueue{client: cl}

	// Directory is refused with zero DELETEs.
	if _, ok, err := cq.reclaimEmptyOccupant(context.Background(), &CommitEntry{Path: "/dir", Size: 16, Kind: staging.PendingOverwrite}, "/dir"); err != nil || ok {
		t.Fatalf("directory reclaim = (ok=%v, err=%v), want (false, nil)", ok, err)
	}
	if deleteSeen.Load() != 0 {
		t.Fatalf("DELETEs on directory = %d, want 0", deleteSeen.Load())
	}

	// Classic overwrite resolves revision=1 directly with ZERO DELETEs!
	rev, ok, err := cq.reclaimEmptyOccupant(context.Background(), &CommitEntry{Path: "/f", Size: 16, Kind: staging.PendingOverwrite}, "/f")
	if err != nil || !ok || rev != 1 {
		t.Fatalf("classic empty reclaim = (rev=%d, ok=%v, err=%v), want (1, true, nil)", rev, ok, err)
	}
	if deleteSeen.Load() != 0 {
		t.Fatalf("classic empty reclaim issued %d DELETEs, want 0", deleteSeen.Load())
	}

	// Extent promotion sends conditional DELETE (?kind=file + X-Dat9-Expected-Revision),
	// which fails closed when a competing writer advanced the revision to 2.
	cq.extentTakeover = true
	cq.SetExtentRouting(nil, 0, 1024)
	if _, ok, err := cq.reclaimEmptyOccupant(context.Background(), &CommitEntry{Path: "/f", Size: 4096, Kind: staging.PendingOverwrite}, "/f"); err == nil || ok {
		t.Fatalf("raced extent empty reclaim = (ok=%v, err=%v), want conflict error", ok, err)
	}
}

// Finding #6 (ordering): Adoption-time deferred chmod (0644) must never
// overwrite a newer acknowledged chmod (0600), whether adoption pauses before
// entering flushMigratedMode or mid-flight inside ChmodCtx.
func TestAdoptExtentHandleDoesNotOverwriteNewerAcknowledgedChmod(t *testing.T) {
	t.Run("adoption-paused-before-flush", func(t *testing.T) {
		var remoteMode atomic.Uint32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, ok := r.URL.Query()["chmod"]; ok {
				var body struct {
					Mode uint32 `json:"mode"`
				}
				_ = json.NewDecoder(r.Body).Decode(&body)
				remoteMode.Store(body.Mode)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"status":"ok"}`))
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		cl := client.New(srv.URL, "")
		wb, err := New(Options{Policy: PolicyWriteBack, Client: cl})
		if err != nil {
			t.Fatal(err)
		}
		paused := make(chan struct{})
		resume := make(chan struct{})
		wb.testBeforeAdoptModeFlush = func() {
			close(paused)
			<-resume
		}

		h := &bufferingHandle{
			w:              wb,
			path:           "/order1.bin",
			landed:         true,
			pendingMode:    0o644,
			hasPendingMode: true,
		}
		wb.track(h)
		defer wb.untrack(h)

		adoptDone := make(chan bool, 1)
		go func() {
			adoptDone <- wb.adoptExtentHandle(h, &noopExtentHandle{})
		}()
		<-paused

		// Concurrent chmod(0600) arrives after h.extent is installed and succeeds.
		if _, err := wb.Do(context.Background(), &fs.Op{
			Kind: fs.OpSetAttr, Path: "/order1.bin", Handle: h,
			HasMode: true, Mode: 0o600,
		}, nil); err != nil {
			t.Fatalf("migrated chmod 0600: %v", err)
		}
		if got := remoteMode.Load(); got != 0o600 {
			t.Fatalf("remoteMode after chmod 0600 = %o, want 0600", got)
		}

		close(resume)
		if !<-adoptDone {
			t.Fatal("adoptExtentHandle returned false")
		}
		if got := remoteMode.Load(); got != 0o600 {
			t.Fatalf("adoption overwrote acknowledged chmod: remoteMode = %o, want 0600", got)
		}
	})

	t.Run("adoption-paused-in-flight", func(t *testing.T) {
		var remoteMode atomic.Uint32
		firstEntered := make(chan struct{})
		releaseFirst := make(chan struct{})
		var once sync.Once

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, ok := r.URL.Query()["chmod"]; ok {
				var body struct {
					Mode uint32 `json:"mode"`
				}
				_ = json.NewDecoder(r.Body).Decode(&body)
				if body.Mode == 0o644 {
					once.Do(func() {
						close(firstEntered)
						<-releaseFirst
					})
				}
				remoteMode.Store(body.Mode)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"status":"ok"}`))
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		cl := client.New(srv.URL, "")
		wb, err := New(Options{Policy: PolicyWriteBack, Client: cl})
		if err != nil {
			t.Fatal(err)
		}
		h := &bufferingHandle{
			w:              wb,
			path:           "/order2.bin",
			landed:         true,
			pendingMode:    0o644,
			hasPendingMode: true,
		}
		wb.track(h)
		defer wb.untrack(h)

		adoptDone := make(chan bool, 1)
		go func() {
			adoptDone <- wb.adoptExtentHandle(h, &noopExtentHandle{})
		}()
		<-firstEntered

		chmodDone := make(chan error, 1)
		go func() {
			_, cerr := wb.Do(context.Background(), &fs.Op{
				Kind: fs.OpSetAttr, Path: "/order2.bin", Handle: h,
				HasMode: true, Mode: 0o600,
			}, nil)
			chmodDone <- cerr
		}()
		// Give OpSetattr a moment to record modeSeq=2 and queue on modeMu.
		time.Sleep(20 * time.Millisecond)
		close(releaseFirst)

		if err := <-chmodDone; err != nil {
			t.Fatalf("migrated chmod 0600: %v", err)
		}
		<-adoptDone
		if got := remoteMode.Load(); got != 0o600 {
			t.Fatalf("final remoteMode = %o, want 0600", got)
		}
	})
}

// Finding #6 (error propagation): An explicit chmod on a migrated handle that
// is rejected with HTTP 403 Forbidden must fail and propagate the denial rather
// than claiming 0600 succeeded, while still clearing any impossible deferred
// obligation so subsequent Sync/Release is not stranded.
func TestMigratedExplicitChmodForbiddenPropagatesError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := r.URL.Query()["chmod"]; ok {
			http.Error(w, `{"error":"chmod is owner-only"}`, http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cl := client.New(srv.URL, "")
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	h := &bufferingHandle{
		w:              wb,
		path:           "/forbidden-migrated.bin",
		landed:         true,
		pendingMode:    0o644,
		hasPendingMode: true,
	}
	wb.track(h)
	defer wb.untrack(h)

	// Adoption drops the unapplyable pre-migration deferred mode without failing adoption.
	if !wb.adoptExtentHandle(h, &noopExtentHandle{}) {
		t.Fatal("adoptExtentHandle returned false")
	}

	// Explicit chmod(0600) on the migrated handle MUST propagate the 403 error!
	res, cerr := wb.Do(context.Background(), &fs.Op{
		Kind: fs.OpSetAttr, Path: "/forbidden-migrated.bin", Handle: h,
		HasMode: true, Mode: 0o600,
	}, nil)
	if cerr == nil || !client.IsForbidden(cerr) {
		t.Fatalf("explicit forbidden chmod returned (res=%+v, err=%v), want 403 Forbidden error", res, cerr)
	}
	if res.Attr != nil {
		t.Fatalf("denied explicit chmod returned non-nil Attr: %+v", res.Attr)
	}

	// Subsequent Sync must not be stranded by the rejected explicit chmod.
	if serr := h.Sync(context.Background()); serr != nil {
		t.Fatalf("Sync after denied explicit chmod: %v", serr)
	}
}
