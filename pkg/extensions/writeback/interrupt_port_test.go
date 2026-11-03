// v1 sync_commit_interrupt rows (writeback half): a close-sync flush
// whose commit PUT is answered while the kernel interrupts the FUSE
// request. Interrupt-safe (the default): the commit detaches, the PUT
// completes, Flush returns nil. Legacy: the PUT is canceled and Flush
// surfaces EAGAIN.
package writeback

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// v1 TestSyncDataCommitContextPolicy (policy half): the default detaches
// commits from the caller's cancel channel; the legacy flag keeps them
// bound.
func TestPortCommitContextPolicy(t *testing.T) {
	wbSafe, err := New(Options{Policy: PolicyCloseSync, Client: client.New("http://127.0.0.1:1", "")})
	if err != nil {
		t.Fatal(err)
	}
	if wbSafe.opts.InterruptibleCommits {
		t.Fatal("interrupt-safe default flipped")
	}
	wbLegacy, err := New(Options{Policy: PolicyCloseSync, Client: client.New("http://127.0.0.1:1", ""), InterruptibleCommits: true})
	if err != nil {
		t.Fatal(err)
	}
	if !wbLegacy.opts.InterruptibleCommits {
		t.Fatal("legacy flag did not arm")
	}
}

// v1 TestCloseSyncFlushCompletesThroughFuseInterrupt.
func TestPortCloseSyncFlushThroughInterrupt(t *testing.T) {
	for _, tc := range []struct {
		name   string
		legacy bool
		wantOK bool
	}{
		{name: "interrupt-safe", legacy: false, wantOK: true},
		{name: "legacy-interruptible", legacy: true, wantOK: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var puts atomic.Int32
			var once sync.Once
			cancel := make(chan struct{})
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/v1/status":
					w.WriteHeader(http.StatusOK)
				case r.Method == http.MethodHead:
					w.Header().Set("Content-Length", "0")
					w.Header().Set("X-Dat9-IsDir", "false")
					w.Header().Set("X-Dat9-Revision", "1")
					w.WriteHeader(http.StatusOK)
				case r.Method == http.MethodPut || r.Method == http.MethodPost:
					puts.Add(1)
					once.Do(func() { close(cancel) })
					time.Sleep(25 * time.Millisecond)
					w.Header().Set("X-Dat9-Revision", "2")
					w.WriteHeader(http.StatusOK)
				default:
					http.NotFound(w, r)
				}
			}))
			defer ts.Close()

			cl := client.New(ts.URL, "")
			cl.SetSmallFileThresholdForTests(1 << 20)
			wb, err := New(Options{Policy: PolicyCloseSync, Client: cl,
				WriteCacheDir: t.TempDir(), InterruptibleCommits: tc.legacy})
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

			// The kernel request context is canceled mid-PUT.
			ctx, ctxCancel := context.WithCancel(context.Background())
			go func() {
				<-cancel
				ctxCancel()
			}()

			h := open(t, ch, "/cs-interrupt.txt")
			if err := h.Write(ctx, 0, []byte("payload")); err != nil && tc.wantOK {
				t.Fatalf("write: %v", err)
			}
			// The flush verdict runs on the (already canceled) request
			// context — v1 delivered Flush under the same cancel channel.
			err = h.Flush(ctx)
			if tc.wantOK {
				if err != nil {
					t.Fatalf("flush err = %v, want nil (commit completes through the interrupt)", err)
				}
			} else {
				if err == nil {
					t.Fatal("flush completed under legacy interrupt, want EAGAIN")
				}
				// The raw cancel error reaches the facade, whose StatusOf
				// maps it to EAGAIN for the kernel (v1 httpToFuseStatus).
				if st := fs.StatusOf(err); st != gofuse.EAGAIN {
					t.Fatalf("flush err = %v (status %v), want EAGAIN", err, st)
				}
			}
		})
	}
}
