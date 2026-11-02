//go:build unix

package writeback

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// Port of v1's TestODSyncOpenPromotesCloseSyncToWriteSync: an O_SYNC/O_DSYNC
// open forces per-Write commits regardless of the mount policy — the handle
// carries forceSync and every Write commits inline.
func TestSyncOpenFlagsForceInlineCommits(t *testing.T) {
	for _, flag := range []string{"O_SYNC", "O_DSYNC"} {
		t.Run(flag, func(t *testing.T) {
			var f uint32
			if flag == "O_SYNC" {
				f = uint32(syscall.O_SYNC)
			} else {
				f = uint32(syscall.O_DSYNC)
			}
			if !hasSyncOpenFlag(f) {
				t.Errorf("hasSyncOpenFlag(%s) = false, want true", flag)
			}
			if hasSyncOpenFlag(0) {
				t.Error("hasSyncOpenFlag(0) = true, want false")
			}
			if hasSyncOpenFlag(uint32(syscall.O_WRONLY)) {
				t.Error("plain O_WRONLY flagged as sync")
			}
		})
	}
}

// v1 writeback_syncflags_unix_test.go's TestODSyncOpenPromotesCloseSyncTo-
// WriteSync plus writeback_test.go's TestOSyncOpenPromotesWriteBackTo-
// WriteSync, driven through the public path: an O_SYNC/O_DSYNC open commits
// every Write inline under both the close-sync and the writeback policies —
// each Write returns only after its PUT, so two writes produce two remote
// images at return.
func TestSyncOpenFlagsPromotePoliciesToPerWriteCommits(t *testing.T) {
	for _, tc := range []struct {
		flag   uint32
		name   string
		policy Policy
	}{
		{uint32(syscall.O_SYNC), "O_SYNC-close-sync", PolicyCloseSync},
		{uint32(syscall.O_DSYNC), "O_DSYNC-close-sync", PolicyCloseSync},
		{uint32(syscall.O_SYNC), "O_SYNC-writeback", PolicyWriteBack},
		{uint32(syscall.O_DSYNC), "O_DSYNC-writeback", PolicyWriteBack},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var puts atomic.Int32
			b := testbackend.NewBackend()
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPut {
					puts.Add(1)
				}
				b.ServeHTTP(w, r)
			}))
			defer ts.Close()
			cl := client.New(ts.URL, "")
			wb, err := New(Options{Policy: tc.policy, Client: cl})
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

			res, err := ch.Dispatch(ctx, &fs.Op{
				Kind: fs.OpCreate, Path: "/sync.txt",
				Flags: tc.flag | uint32(syscall.O_CREAT|syscall.O_RDWR), Mode: 0o644,
			})
			if err != nil {
				t.Fatal(err)
			}
			h := res.Handle
			if err := h.Write(ctx, 0, []byte("one ")); err != nil {
				t.Fatal(err)
			}
			// Per-write durability: the first write's image is remote before
			// the second write starts, without any flush.
			if n, ok := b.Node("/sync.txt"); !ok || string(n.Data) != "one " {
				t.Fatalf("remote after first write = %q ok=%v, want one", n.Data, ok)
			}
			if err := h.Write(ctx, 4, []byte("two")); err != nil {
				t.Fatal(err)
			}
			if n, _ := b.Node("/sync.txt"); string(n.Data) != "one two" {
				t.Fatalf("remote after second write = %q, want one two", n.Data)
			}
			if err := h.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if got := puts.Load(); got < 2 {
				t.Fatalf("PUT count = %d, want >= 2 (one per write)", got)
			}
		})
	}
}
