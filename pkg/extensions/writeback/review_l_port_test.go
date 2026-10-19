package writeback

// qiffang twenty-first round: the irreversible staging purge runs only
// after the remote DELETE succeeds.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// A staged overwrite is the only retry source once the writer closed: a
// DELETE that fails must leave it in place, and only a successful DELETE
// may purge it. The entry is staged directly (no live uploader) so the
// assertion cannot race a background upload draining it.
func TestPortUnlinkFailureKeepsStagedOverwrite(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("old"))
	var failDeletes atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/f") && failDeletes.Load() > 0 {
			failDeletes.Add(-1)
			http.Error(w, "forced delete failure", http.StatusServiceUnavailable)
			return
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wbcache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: wbcache})
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

	if err := wbcache.PutWithBaseRev("/f", []byte("new"), 3, staging.PendingOverwrite, 1); err != nil {
		t.Fatal(err)
	}
	if _, ok := wbcache.GetMeta("/f"); !ok {
		t.Fatal("staged overwrite missing before unlink")
	}
	failDeletes.Store(1)
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/f"}); err == nil {
		t.Fatal("forced-failure delete reported success")
	}
	if _, ok := wbcache.GetMeta("/f"); !ok {
		t.Fatal("staged overwrite purged on a FAILED delete — the retry source is gone")
	}
	// After the failure clears, a retry unlink succeeds and the purge runs.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/f"}); err != nil {
		t.Fatalf("retry unlink: %v", err)
	}
	if _, ok := wbcache.GetMeta("/f"); ok {
		t.Fatal("staged overwrite survived a successful delete")
	}
}

// With stat revalidation (no trusted namespace cache), a staged overwrite
// must still serve its OWN size while the remote keeps the mode: the
// write-time size is the mount's newest truth until the commit lands.
func TestPortPendingOverwriteServesSizeWithRemoteMode(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("0123456789"))
	b.SetNodeMode("/f", 0o640)
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wbcache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: wbcache})
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

	if err := wbcache.PutWithBaseRev("/f", []byte("0123456789012345678901"), 22, staging.PendingOverwrite, 1); err != nil {
		t.Fatal(err)
	}
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/f"})
	if err != nil {
		t.Fatalf("getattr = %v", err)
	}
	if res.Attr == nil || res.Attr.Size != 22 {
		t.Fatalf("attr = %+v, want the staged size 22", res.Attr)
	}
	if res.Attr.Mode&0o777 != 0o640 {
		t.Fatalf("mode = %#o, want the remote 0640 (pending overwrite must not clobber the mode)", res.Attr.Mode)
	}
}
