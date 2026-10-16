// v1 setattr_time_override rows (writeback half): a time-only SetAttr
// skips the pending-commit wait (utimensat is mount-local and never waits
// behind remote work), while a mode change on a queued path waits for its
// own landing (the queue snapshot's mode would be clobbered otherwise).
package writeback

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// controlledServer answers stats immediately and holds the first content
// PUT until release() runs — one deterministic in-flight commit.
type controlledServer struct {
	putStarted chan struct{}
	release    chan struct{}
	heads      atomic.Int32
	puts       atomic.Int32
}

func newControlledServer() *controlledServer {
	return &controlledServer{putStarted: make(chan struct{}), release: make(chan struct{})}
}

func (s *controlledServer) handler() http.HandlerFunc {
	var once sync.Once
	return func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/status":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodHead:
			s.heads.Add(1)
			w.Header().Set("Content-Length", "5")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "2")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet:
			// The open-existing preload reads the base through the
			// terminal.
			_, _ = w.Write([]byte("base"))
		case r.Method == http.MethodPut || r.Method == http.MethodPost:
			s.puts.Add(1)
			once.Do(func() { close(s.putStarted) })
			<-s.release
			w.Header().Set("X-Dat9-Revision", "3")
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}
}

// newStagedHandlerStack mounts a staged writeback over a raw handler
// (not testbackend).
func newStagedHandlerStack(t *testing.T, s *controlledServer) (*fs.Chain, *Writeback) {
	t.Helper()
	ts := httptest.NewServer(s.handler())
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	ledger, err := staging.NewStaging(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledger.Close)
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, Staging: ledger})
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

// v1 TestSetAttrTimeOnlySkipsPendingCommitWait: with the path's commit
// held in flight, a time-only SetAttr returns immediately (mount-local
// override; no queue wait).
func TestPortSetAttrTimeOnlySkipsPendingCommitWait(t *testing.T) {
	s := newControlledServer()
	ch, wb := newStagedHandlerStack(t, s)
	ctx := context.Background()

	h := open(t, ch, "/time.txt").(*bufferingHandle)
	if err := h.Write(ctx, 0, []byte("image")); err != nil {
		t.Fatal(err)
	}
	// Hold the close commit in flight.
	done := make(chan error, 1)
	go func() { done <- h.commitLocked(ctx) }()
	select {
	case <-s.putStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("commit never started")
	}

	// Time-only setattr returns without waiting for the landing.
	settle := make(chan error, 1)
	go func() {
		_, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/time.txt", Mtime: time.Unix(1234567890, 0), Size: -1})
		settle <- err
	}()
	select {
	case err := <-settle:
		if err != nil {
			t.Fatalf("time-only setattr: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("time-only setattr waited behind the in-flight commit")
	}

	close(s.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	_ = wb.Drain(context.Background())
}

// v1 TestSetAttrModeStillWaitsForPendingCommit: a chmod on a queued path
// waits for the commit to land (the entry's enqueue-time mode snapshot
// would otherwise clobber the newer chmod at post-landing application).
func TestPortSetAttrModeWaitsForPendingCommit(t *testing.T) {
	s := newControlledServer()
	ch, wb := newStagedHandlerStack(t, s)
	ctx := context.Background()

	h := open(t, ch, "/mode.txt").(*bufferingHandle)
	if err := h.Write(ctx, 0, []byte("image")); err != nil {
		t.Fatal(err)
	}
	// Stage + enqueue asynchronously (the write-back Flush contract), then
	// drop the handle: the chmod now takes the queued (non-live) path whose
	// wait v1 pinned.
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.putStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("commit never started")
	}

	// The chmod must NOT complete while the commit is held.
	settle := make(chan error, 1)
	go func() {
		_, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/mode.txt", Mode: 0o600, Size: -1})
		settle <- err
	}()
	select {
	case err := <-settle:
		t.Fatalf("chmod completed while the commit was in flight: %v", err)
	case <-time.After(300 * time.Millisecond):
		// held, as v1 requires.
	}

	close(s.release)
	select {
	case err := <-settle:
		if err != nil {
			t.Fatalf("chmod after landing: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("chmod did not complete after the commit landed")
	}
	_ = wb.Drain(context.Background())
}
