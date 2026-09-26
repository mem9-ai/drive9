package fuse

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// TestSyncDataCommitContextPolicy pins the context policy for synchronous
// data commits (close-sync/write-sync Flush, Fsync, and Release): under
// interrupt-safe mutations (the default) the commit detaches from the FUSE
// cancel channel and keeps its full budget; with
// --legacy-interruptible-mutations it stays cancelable through the channel.
func TestSyncDataCommitContextPolicy(t *testing.T) {
	cancel := make(chan struct{})
	close(cancel)

	interruptSafe := &MountOptions{}
	interruptSafe.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1:1"), interruptSafe)
	commitCtx, commitCancel := fs.syncDataCommitContext(cancel, 5*time.Second)
	defer commitCancel()
	if err := commitCtx.Err(); err != nil {
		t.Fatalf("interrupt-safe sync commit ctx err = %v, want detached from the closed cancel channel", err)
	}
	deadline, ok := commitCtx.Deadline()
	if !ok {
		t.Fatal("interrupt-safe sync commit ctx has no deadline")
	}
	if budget := time.Until(deadline); budget > 5*time.Second {
		t.Fatalf("interrupt-safe sync commit budget = %v, want <= the requested 5s", budget)
	}

	legacy := &MountOptions{LegacyInterruptibleMutations: true}
	legacy.setDefaults()
	legacyFS := NewDat9FS(newTestClient("http://127.0.0.1:1"), legacy)
	legacyCtx, legacyCancel := legacyFS.syncDataCommitContext(cancel, 5*time.Second)
	defer legacyCancel()
	if _, ok := legacyCtx.Deadline(); !ok {
		t.Fatal("legacy sync commit ctx has no deadline")
	}
	// The cancel watcher cancels the context asynchronously; give it a moment.
	deadlineExhausted := time.After(2 * 5 * time.Second)
	for legacyCtx.Err() == nil {
		select {
		case <-deadlineExhausted:
			t.Fatal("legacy sync commit ctx was never canceled by the closed cancel channel")
		case <-time.After(time.Millisecond):
		}
	}
}

// TestCloseSyncFlushCompletesThroughFuseInterrupt drives the observed EAGAIN:
// a close-sync close(2) whose remote upload is still in flight when the
// kernel interrupts the FUSE request. The interrupt lands while the commit
// PUT is being answered. With interrupt-safe mutations the commit completes
// and Flush returns OK; with --legacy-interruptible-mutations the in-flight
// PUT is canceled and close surfaces the historical EAGAIN.
func TestCloseSyncFlushCompletesThroughFuseInterrupt(t *testing.T) {
	for _, tc := range []struct {
		name   string
		legacy bool
		want   gofuse.Status
	}{
		{name: "interrupt-safe", legacy: false, want: gofuse.OK},
		{name: "legacy-interruptible", legacy: true, want: gofuse.EAGAIN},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const filePath = "/close-sync-interrupt.txt"
			var puts atomic.Int32
			var interruptOnce sync.Once
			cancel := make(chan struct{})
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodHead:
					w.Header().Set("Content-Length", "0")
					w.Header().Set("X-Dat9-IsDir", "false")
					w.Header().Set("X-Dat9-Revision", "1")
					w.WriteHeader(http.StatusOK)
				case r.Method == http.MethodPut && r.URL.Path == "/v1/fs"+filePath:
					puts.Add(1)
					// The FUSE interrupt arrives while the commit is being
					// answered; hold the response briefly so the canceled
					// client context always wins the race in legacy mode.
					interruptOnce.Do(func() {
						close(cancel)
						time.Sleep(50 * time.Millisecond)
					})
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"revision":2}`))
				default:
					http.Error(w, "unexpected request", http.StatusInternalServerError)
				}
			}))
			defer ts.Close()

			opts := &MountOptions{LegacyInterruptibleMutations: tc.legacy}
			opts.setDefaults()
			opts.WritePolicy = WritePolicyCloseSync
			fs := NewDat9FS(newTestClient(ts.URL), opts)
			cleanupKernelCacheBypassAfterTest(t, fs)

			ino := fs.inodes.Lookup(filePath, false, 0, time.Now())
			fs.inodes.UpdateRevision(ino, 1)

			var out gofuse.OpenOut
			if st := fs.Open(nil, &gofuse.OpenIn{
				InHeader: gofuse.InHeader{NodeId: ino},
				Flags:    uint32(syscall.O_RDWR),
			}, &out); st != gofuse.OK {
				t.Fatalf("Open status = %v, want OK", st)
			}
			if _, st := fs.Write(nil, &gofuse.WriteIn{
				InHeader: gofuse.InHeader{NodeId: ino},
				Fh:       out.Fh,
				Offset:   0,
			}, []byte("payload")); st != gofuse.OK {
				t.Fatalf("Write status = %v, want OK", st)
			}

			st := fs.Flush(cancel, &gofuse.FlushIn{
				InHeader: gofuse.InHeader{NodeId: ino},
				Fh:       out.Fh,
			})
			if st != tc.want {
				t.Fatalf("Flush status = %v, want %v", st, tc.want)
			}
			if got := puts.Load(); got != 1 {
				t.Fatalf("commit PUT calls = %d, want exactly 1", got)
			}
		})
	}
}

// TestFlushJournalSyncFallbackCompletesThroughFuseInterrupt pins the
// production path behind review finding #995 (qiffang): with the default
// durability policy, a SQLite persistent journal reaches the synchronous
// flushHandle fallback through flushHandleDebounced (journal paths never
// take the async write-back staging branch). The interrupt landing mid-upload
// must not turn that upload into EAGAIN under interrupt-safe mode, and
// --gvisor-compat takes precedence over --legacy-interruptible-mutations:
// a gVisor mount keeps data commits detached even with the legacy option set
// (the documented "no effect with --gvisor-compat" contract).
func TestFlushJournalSyncFallbackCompletesThroughFuseInterrupt(t *testing.T) {
	for _, tc := range []struct {
		name   string
		legacy bool
		gvisor bool
		want   gofuse.Status
	}{
		{name: "interrupt-safe", legacy: false, gvisor: false, want: gofuse.OK},
		{name: "legacy-interruptible", legacy: true, gvisor: false, want: gofuse.EAGAIN},
		{name: "gvisor-takes-precedence-over-legacy", legacy: true, gvisor: true, want: gofuse.OK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const filePath = "/journal-interrupt.db-journal"
			var puts atomic.Int32
			var interruptOnce sync.Once
			cancel := make(chan struct{})
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodHead:
					w.Header().Set("Content-Length", "0")
					w.Header().Set("X-Dat9-IsDir", "false")
					w.Header().Set("X-Dat9-Revision", "1")
					w.WriteHeader(http.StatusOK)
				case r.Method == http.MethodPut && r.URL.Path == "/v1/fs"+filePath:
					puts.Add(1)
					interruptOnce.Do(func() {
						close(cancel)
						time.Sleep(50 * time.Millisecond)
					})
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"revision":2}`))
				default:
					http.Error(w, "unexpected request", http.StatusInternalServerError)
				}
			}))
			defer ts.Close()

			opts := &MountOptions{LegacyInterruptibleMutations: tc.legacy, GVisorCompat: tc.gvisor}
			opts.setDefaults()
			fs := NewDat9FS(newTestClient(ts.URL), opts)
			cleanupKernelCacheBypassAfterTest(t, fs)

			ino := fs.inodes.Lookup(filePath, false, 0, time.Now())
			fs.inodes.UpdateRevision(ino, 1)

			var out gofuse.OpenOut
			if st := fs.Open(nil, &gofuse.OpenIn{
				InHeader: gofuse.InHeader{NodeId: ino},
				Flags:    uint32(syscall.O_RDWR),
			}, &out); st != gofuse.OK {
				t.Fatalf("Open status = %v, want OK", st)
			}
			if _, st := fs.Write(nil, &gofuse.WriteIn{
				InHeader: gofuse.InHeader{NodeId: ino},
				Fh:       out.Fh,
				Offset:   0,
			}, []byte("journal payload")); st != gofuse.OK {
				t.Fatalf("Write status = %v, want OK", st)
			}

			st := fs.Flush(cancel, &gofuse.FlushIn{
				InHeader: gofuse.InHeader{NodeId: ino},
				Fh:       out.Fh,
			})
			if st != tc.want {
				t.Fatalf("Flush status = %v, want %v", st, tc.want)
			}
			if got := puts.Load(); got != 1 {
				t.Fatalf("journal PUT calls = %d, want exactly 1", got)
			}
		})
	}
}

// TestReleaseLayerPendingChmodCompletesThroughFuseInterrupt pins review
// finding 2: on a layer mount, the Release pending-chmod finalizer completes
// the content commit's mode, so it must run detached from the FUSE cancel
// channel. The interrupt lands during the detached content commit; the layer
// chmod that follows must still reach the layer endpoint and clear the
// pending mode, instead of being dropped with an already-canceled context.
func TestReleaseLayerPendingChmodCompletesThroughFuseInterrupt(t *testing.T) {
	for _, tc := range []struct {
		name            string
		legacy          bool
		wantModePending bool
	}{
		{name: "interrupt-safe", legacy: false, wantModePending: false},
		{name: "legacy-interruptible", legacy: true, wantModePending: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const filePath = "/layer-mode.txt"
			var layerPOSTs atomic.Int32
			var interruptOnce sync.Once
			cancel := make(chan struct{})
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body := make([]byte, 512)
				n, _ := r.Body.Read(body)
				t.Logf("req %s %s body=%s", r.Method, r.URL.Path, string(body[:n]))
				switch {
				case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/layers/"):
					if layerPOSTs.Add(1) == 1 {
						// First layer POST is the content commit: land the
						// FUSE interrupt while it is being answered.
						interruptOnce.Do(func() {
							close(cancel)
							time.Sleep(50 * time.Millisecond)
						})
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{}`))
				case r.Method == http.MethodHead:
					w.Header().Set("Content-Length", "0")
					w.Header().Set("X-Dat9-IsDir", "false")
					w.Header().Set("X-Dat9-Revision", "1")
					w.WriteHeader(http.StatusOK)
				default:
					http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusInternalServerError)
				}
			}))
			defer ts.Close()

			opts := &MountOptions{LegacyInterruptibleMutations: tc.legacy}
			opts.setDefaults()
			opts.WritePolicy = WritePolicyCloseSync
			opts.LayerRef = "layer-1"
			fs := NewDat9FS(newTestClient(ts.URL), opts)
			shadow, err := NewShadowStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer shadow.Close()
			pending, err := NewPendingIndex(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			fs.shadowStore = shadow
			fs.pendingIndex = pending
			if err := shadow.WriteFull(filePath, []byte("payload"), 0); err != nil {
				t.Fatal(err)
			}
			// Pending metadata WITHOUT a mode: the Release pending-chmod
			// finalizer's UpdateMode is what records the mode, so the pending
			// index doubles as the observable for whether the chmod landed.
			if _, err := pending.PutWithBaseRev(filePath, int64(len("payload")), PendingNew, 0); err != nil {
				t.Fatal(err)
			}

			ino := fs.inodes.Lookup(filePath, false, 0, time.Now())
			fs.inodes.UpdateRevision(ino, 1)

			var out gofuse.OpenOut
			if st := fs.Open(nil, &gofuse.OpenIn{
				InHeader: gofuse.InHeader{NodeId: ino},
				Flags:    uint32(syscall.O_RDWR),
			}, &out); st != gofuse.OK {
				t.Fatalf("Open status = %v, want OK", st)
			}
			fh, ok := fs.fileHandles.Get(out.Fh)
			if !ok {
				t.Fatal("file handle not found")
			}
			// White-box: the handle is clean (content already durable); it
			// only carries a pending chmod whose earlier attempt failed —
			// exactly the state whose completion Release owns.
			fh.Lock()
			fs.setPendingModeLocked(fh, 0o600, 0)
			gotPending := fh.HasPendingMode
			fh.Unlock()
			if !gotPending {
				t.Fatal("pending mode not set before Release")
			}

			fs.Release(cancel, &gofuse.ReleaseIn{
				InHeader: gofuse.InHeader{NodeId: ino},
				Fh:       out.Fh,
			})
			// The layer chmod POST re-upserts the shadow content with the
			// mode; on success it records the mode in the pending index.
			// Under legacy mode the canceled context drops the chmod and the
			// mode never lands.
			meta, metaOK := pending.GetMeta(filePath)
			if !metaOK {
				t.Fatal("pending metadata missing after Release")
			}
			if meta.HasMode != !tc.wantModePending {
				t.Fatalf("pending meta mode after Release = (hasMode=%v, mode=%#o), want hasMode=%v", meta.HasMode, meta.Mode, !tc.wantModePending)
			}
		})
	}
}
