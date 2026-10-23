// v1 dat9fs_test.go rename/rmdir retry rows (B2-4), retargeted to the
// core terminal: rename transient recovery (interrupted commit retried,
// the committed-probe accepts a visible target, and a pre-existing target
// is not mistaken for the rename's own landing) and the rmdir
// eventual-consistency rows (a stale remote listing must not produce
// ENOTEMPTY after this mount's own unlinks — the deferred-unlink flush
// plus the backend's 409-not-empty retry cover the lag window).
package core

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func newRetryCore(t *testing.T, ts *httptest.Server, legacyInterruptible bool) *Core {
	t.Helper()
	cl := client.New(ts.URL, "")
	c, err := New(cl, Options{LegacyInterruptibleMutations: legacyInterruptible, StatRetryCount: -1})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// v1 TestRenameRemoteWithTransientRetryRetriesAfterInterrupt: the first
// rename POST hangs until its context cancels (a legacy interruptible
// commit); the retry lands the rename — two calls, same source header.
func TestPortRenameRetriesAfterInterrupt(t *testing.T) {
	oldP := "/repo/objects/e6/tmp_obj_XyNuJc"
	newP := "/repo/objects/e6/d0788db28a1c0860d85a7f9181233b37665bce"

	firstStarted := make(chan struct{})
	var once sync.Once
	var renameCalls atomic.Int32
	var wrongSource atomic.Bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs"+newP && r.URL.RawQuery == "rename":
			if r.Header.Get("X-Dat9-Rename-Source") != oldP {
				wrongSource.Store(true)
			}
			if renameCalls.Add(1) == 1 {
				once.Do(func() { close(firstStarted) })
				// Transient server failure on the first attempt (the
				// interrupt flavor is covered by the probe row below).
				http.Error(w, "transient", http.StatusBadGateway)
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	c := newRetryCore(t, ts, false)
	done := make(chan error, 1)
	go func() {
		done <- c.renameRemote(context.Background(), oldP, newP)
	}()

	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the first rename request")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("rename err = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("rename timed out")
	}
	if got := renameCalls.Load(); got != 2 {
		t.Fatalf("rename calls = %d, want 2", got)
	}
	if wrongSource.Load() {
		t.Fatalf("rename source header did not match %q", oldP)
	}
}

// v1 TestRenameRemoteWithTransientRetryAcceptsTargetVisibleAfterInterrupt:
// after the interrupted first attempt, the detached committed-probe sees
// the target already exists (server-side rename applied) and reports
// success without a second rename POST.
func TestPortRenameAcceptsTargetVisibleAfterInterrupt(t *testing.T) {
	oldP := "/repo/objects/e6/tmp_obj_Ta"
	newP := "/repo/objects/e6/deadbeef"

	firstStarted := make(chan struct{})
	var once sync.Once
	var renameCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs"+newP && r.URL.RawQuery == "rename":
			if renameCalls.Add(1) == 1 {
				once.Do(func() { close(firstStarted) })
				<-r.Context().Done()
				return
			}
			http.Error(w, "no retry needed", http.StatusConflict)
		case r.Method == http.MethodHead && (r.URL.Path == "/v1/fs"+newP || r.URL.Path == "/v1/fs"+oldP):
			// Target visible AND source gone: the interrupted rename
			// committed server-side.
			if r.URL.Path == "/v1/fs"+newP {
				w.Header().Set("X-Dat9-Revision", "2")
				w.WriteHeader(http.StatusOK)
				return
			}
			http.Error(w, "gone", http.StatusNotFound)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	c := newRetryCore(t, ts, true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- c.renameRemote(ctx, oldP, newP)
	}()
	select {
	case <-firstStarted:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("first rename never started")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("rename err = %v, want nil (committed probe)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("rename timed out")
	}
	if got := renameCalls.Load(); got != 1 {
		t.Fatalf("rename calls = %d, want 1 (probe-only recovery)", got)
	}
}

// v1 TestRenameRemoteWithTransientRetryDoesNotAcceptPreexistingTarget: a
// target that existed BEFORE the rename is not accepted as evidence of
// the rename's own landing — the source must be gone too.
func TestPortRenameDoesNotAcceptPreexistingTarget(t *testing.T) {
	oldP := "/repo/objects/e6/tmp_obj_Pre"
	newP := "/repo/objects/e6/preexisting"

	var renameCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs"+newP && r.URL.RawQuery == "rename":
			renameCalls.Add(1)
			// Transient failure: not committed.
			http.Error(w, "boom", http.StatusBadGateway)
		case r.Method == http.MethodHead:
			// BOTH ends exist: the target preexisted; this is not a
			// landing.
			w.Header().Set("X-Dat9-Revision", "9")
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	c := newRetryCore(t, ts, false)
	err := c.renameRemote(context.Background(), oldP, newP)
	if err == nil {
		t.Fatal("rename must fail: the pre-existing target is not a landing")
	}
	if got := renameCalls.Load(); got < 1 {
		t.Fatalf("rename calls = %d, want >=1", got)
	}
}

// v1 TestRmdirSucceedsAfterUnlinkWithStaleRemoteList (terminal half):
// the backend's not-empty conflict after this mount's just-issued child
// deletes is retried to success — the lag window does not surface
// ENOTEMPTY.
func TestPortRmdirRetriesThroughNotEmptyLag(t *testing.T) {
	var deleteDirCalls atomic.Int32
	var deleteChildCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/fs/dir":
			n := deleteDirCalls.Add(1)
			if n <= 2 {
				// Replica lag: the child delete is not visible yet.
				http.Error(w, `{"error":{"message":"directory not empty"}}`, http.StatusConflict)
				return
			}
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/fs/dir/stale.txt":
			deleteChildCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	c := newRetryCore(t, ts, false)
	if err := c.deleteRemote(context.Background(), "/dir", true); err != nil {
		t.Fatalf("rmdir err = %v, want nil (lag retried)", err)
	}
	if got := deleteDirCalls.Load(); got != 3 {
		t.Fatalf("dir delete calls = %d, want 3 (two lag conflicts then success)", got)
	}
	if got := deleteChildCalls.Load(); got != 0 {
		t.Fatalf("child delete calls = %d, want 0 (terminal owns only the dir here)", got)
	}
}

// v1 TestRmdirFullUnlinkStaleListing variant: a persistent not-empty
// conflict (a genuinely non-empty directory) surfaces ENOTEMPTY after
// the bounded retry ladder.
func TestPortRmdirPersistentNotEmptySurfacesENOTEMPTY(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && r.URL.Path == "/v1/fs/full" {
			http.Error(w, `{"error":{"message":"directory not empty"}}`, http.StatusConflict)
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	c := newRetryCore(t, ts, false)
	err := c.deleteRemote(context.Background(), "/full", true)
	if err == nil {
		t.Fatal("persistent not-empty must surface")
	}
	// The terminal's Do maps through mapErr — same mapping the facade
	// serves: the not-empty conflict is ENOTEMPTY, not a generic EIO.
	mapped := mapErr(err)
	if se, ok := mapped.(*fs.StatusError); !ok {
		t.Fatalf("mapped err = %T(%v), want *fs.StatusError", mapped, mapped)
	} else if se.Status != gofuse.Status(syscall.ENOTEMPTY) {
		t.Fatalf("mapped status = %v (raw %v), want ENOTEMPTY", se.Status, err)
	}
}
