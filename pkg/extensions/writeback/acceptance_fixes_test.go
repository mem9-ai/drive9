package writeback

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// TestReopenActiveUnflushedCreateAllModesAndPolicies verifies Issue 1:
// opening a newly created, still-open, unflushed file without O_CREAT across
// O_WRONLY, O_RDWR, O_RDONLY, O_TRUNC, and O_APPEND under both PolicyWriteBack
// and PolicyCloseSync never falls through to a remote 404 ENOENT and never
// blocks open(2) on a synchronous remote upload.
func TestReopenActiveUnflushedCreateAllModesAndPolicies(t *testing.T) {
	for _, policy := range []Policy{PolicyWriteBack, PolicyCloseSync} {
		policyName := "writeback"
		if policy == PolicyCloseSync {
			policyName = "close-sync"
		}
		t.Run(policyName, func(t *testing.T) {
			ctx := context.Background()
			b := testbackend.NewBackend()
			var heads, puts atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/status" {
					w.WriteHeader(http.StatusOK)
					return
				}
				switch r.Method {
				case http.MethodHead:
					heads.Add(1)
				case http.MethodPut:
					puts.Add(1)
				}
				b.ServeHTTP(w, r)
			}))
			t.Cleanup(ts.Close)

			cl := client.New(ts.URL, "")
			wbcache, err := staging.NewWriteBackCache(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			ledger, err := staging.NewStaging(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(ledger.Close)
			wb, err := New(Options{
				Policy:     policy,
				Client:     cl,
				WriteCache: wbcache,
				Staging:    ledger,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = wb.Stop(context.Background()) })
			terminal, err := core.New(cl, core.Options{})
			if err != nil {
				t.Fatal(err)
			}
			ch, err := fs.NewChain(wb, terminal)
			if err != nil {
				t.Fatal(err)
			}

			// 1. Rust file_lock_* pattern: File::create (0 bytes, open) -> Open(O_WRONLY) without O_CREAT.
			c1, err := ch.Dispatch(ctx, &fs.Op{
				Kind: fs.OpCreate, Path: "/lockfile",
				Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY),
				Mode:  0o600, HasMode: true, Kernel: &gofuse.CreateIn{},
			})
			if err != nil {
				t.Fatalf("create /lockfile: %v", err)
			}
			heads.Store(0)
			puts.Store(0)
			o2, err := ch.Dispatch(ctx, &fs.Op{
				Kind: fs.OpCreate, Path: "/lockfile",
				Flags: uint32(syscall.O_WRONLY), Kernel: &gofuse.OpenIn{},
			})
			if err != nil {
				t.Fatalf("reopen O_WRONLY without O_CREAT: %v", err)
			}
			// Exclusive O_CREAT|O_EXCL against the live unlanded create must reject with EEXIST.
			if _, exclErr := ch.Dispatch(ctx, &fs.Op{
				Kind: fs.OpCreate, Path: "/lockfile",
				Flags: uint32(syscall.O_CREAT | syscall.O_EXCL | syscall.O_WRONLY),
				Mode:  0o644, HasMode: true, Kernel: &gofuse.CreateIn{},
			}); !errors.Is(exclErr, syscall.EEXIST) {
				t.Fatalf("O_EXCL over live unlanded create = %v, want EEXIST", exclErr)
			}
			if gotH, gotP := heads.Load(), puts.Load(); gotH != 0 || gotP != 0 {
				t.Fatalf("open(2) remote calls: HEAD=%d PUT=%d, want 0/0", gotH, gotP)
			}
			// Write through f1, close unwritten f2 FIRST (must not clobber f1's data with 0 bytes!).
			if err := c1.Handle.Write(ctx, 0, []byte("from-f1")); err != nil {
				t.Fatal(err)
			}
			if err := o2.Handle.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if err := o2.Handle.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if err := c1.Handle.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if err := c1.Handle.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if err := wb.Drain(ctx); err != nil {
				t.Fatal(err)
			}
			if n, ok := b.Node("/lockfile"); !ok || string(n.Data) != "from-f1" || n.Mode&0o777 != 0o600 {
				t.Fatalf("/lockfile remote = (%v, %q, %o), want (true, from-f1, 0600)", ok, n.Data, n.Mode&0o777)
			}

			// 2. Node file-handle pattern: open('w') -> open('r') & open('r+') BEFORE write -> write -> read across fds -> f1 closes unwritten after f2 writes.
			w1, err := ch.Dispatch(ctx, &fs.Op{
				Kind: fs.OpCreate, Path: "/shared",
				Flags: uint32(syscall.O_CREAT | syscall.O_TRUNC | syscall.O_WRONLY),
				Mode:  0o644, HasMode: true, Kernel: &gofuse.CreateIn{},
			})
			if err != nil {
				t.Fatal(err)
			}
			rdo, err := ch.Dispatch(ctx, &fs.Op{
				Kind: fs.OpCreate, Path: "/shared",
				Flags: uint32(syscall.O_RDONLY), Kernel: &gofuse.OpenIn{},
			})
			if err != nil {
				t.Fatalf("reopen O_RDONLY on unwritten create: %v", err)
			}
			rdwr, err := ch.Dispatch(ctx, &fs.Op{
				Kind: fs.OpCreate, Path: "/shared",
				Flags: uint32(syscall.O_RDWR), Kernel: &gofuse.OpenIn{},
			})
			if err != nil {
				t.Fatalf("reopen O_RDWR on unwritten create: %v", err)
			}
			appH, err := ch.Dispatch(ctx, &fs.Op{
				Kind: fs.OpCreate, Path: "/shared",
				Flags: uint32(syscall.O_WRONLY | syscall.O_APPEND), Kernel: &gofuse.OpenIn{},
			})
			if err != nil {
				t.Fatalf("reopen O_APPEND on unwritten create: %v", err)
			}

			// Read on empty unlanded file returns 0, nil (EOF).
			buf := make([]byte, 16)
			if n, rerr := rdo.Handle.Read(ctx, 0, buf); rerr != nil || n != 0 {
				t.Fatalf("empty read = (%d, %v), want (0, nil)", n, rerr)
			}
			// Write via O_RDWR handle, append via O_APPEND handle, read via O_RDONLY handle!
			if err := rdwr.Handle.Write(ctx, 0, []byte("hello")); err != nil {
				t.Fatal(err)
			}
			if n, rerr := rdo.Handle.Read(ctx, 0, buf); rerr != nil || string(buf[:n]) != "hello" {
				t.Fatalf("cross-fd read after rdwr write = (%q, %v), want hello", string(buf[:n]), rerr)
			}
			if err := appH.Handle.Write(ctx, 0, []byte("-world")); err != nil {
				t.Fatal(err)
			}
			if err := rdwr.Handle.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if err := rdwr.Handle.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if err := appH.Handle.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if err := appH.Handle.Release(ctx); err != nil {
				t.Fatal(err)
			}
			// Close unwritten original creator w1 LAST — must NOT clobber "hello-world"!
			if err := w1.Handle.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if err := w1.Handle.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if err := wb.Drain(ctx); err != nil {
				t.Fatal(err)
			}
			// rdo opened while file was local-only must still read committed remote data after Drain!
			if n, rerr := rdo.Handle.Read(ctx, 0, buf); rerr != nil || string(buf[:n]) != "hello-world" {
				t.Fatalf("rdo read after remote landing = (%q, %v), want hello-world", string(buf[:n]), rerr)
			}
			_ = rdo.Handle.Release(ctx)
			if n, ok := b.Node("/shared"); !ok || string(n.Data) != "hello-world" {
				t.Fatalf("/shared remote = (%v, %q), want hello-world", ok, n.Data)
			}

			// 3. Reopen with O_WRONLY|O_TRUNC truncates buffered bytes on live create.
			t1, err := ch.Dispatch(ctx, &fs.Op{
				Kind: fs.OpCreate, Path: "/truncme",
				Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY),
				Mode:  0o644, HasMode: true, Kernel: &gofuse.CreateIn{},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := t1.Handle.Write(ctx, 0, []byte("discard-me")); err != nil {
				t.Fatal(err)
			}
			t2, err := ch.Dispatch(ctx, &fs.Op{
				Kind: fs.OpCreate, Path: "/truncme",
				Flags: uint32(syscall.O_WRONLY | syscall.O_TRUNC), Kernel: &gofuse.OpenIn{},
			})
			if err != nil {
				t.Fatalf("reopen O_WRONLY|O_TRUNC: %v", err)
			}
			if err := t2.Handle.Write(ctx, 0, []byte("kept")); err != nil {
				t.Fatal(err)
			}
			_ = t1.Handle.Flush(ctx)
			_ = t1.Handle.Release(ctx)
			_ = t2.Handle.Flush(ctx)
			_ = t2.Handle.Release(ctx)
			if err := wb.Drain(ctx); err != nil {
				t.Fatal(err)
			}
			if n, ok := b.Node("/truncme"); !ok || string(n.Data) != "kept" {
				t.Fatalf("/truncme remote = (%v, %q), want kept", ok, n.Data)
			}
		})
	}
}

// TestShadowQuotaZeroLeakAcrossOverwriteUnlinkAndOpenUnlink verifies Issue 2a:
// ShadowStore.quotaBytes returns to exact 0 across multi-round shadow commits,
// closed-file unlinks of queued overwrites, sparse 1 GiB open-file unlinks
// (MarkPathUnlinked), and RetainShadowForLanding transitions.
func TestShadowQuotaZeroLeakAcrossOverwriteUnlinkAndOpenUnlink(t *testing.T) {
	ctx := context.Background()
	b := testbackend.NewBackend()

	var holdUploads atomic.Bool
	releaseUploads := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if holdUploads.Load() && (r.Method == http.MethodPut || r.Method == http.MethodPost) {
			select {
			case <-releaseUploads:
			case <-r.Context().Done():
				http.Error(w, "canceled", http.StatusRequestTimeout)
				return
			}
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)

	cl := client.New(ts.URL, "")
	wbcache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// Configure a 64 MiB byte quota so a 1 GiB sparse unlink inflation would
	// immediately blow the quota if MarkPathUnlinked densified holes.
	ledger, err := staging.NewStagingWithQuota(t.TempDir(), 0, 64)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledger.Close)

	wb, err := New(Options{
		Policy:     PolicyWriteBack,
		SyncMode:   "interactive",
		Client:     cl,
		WriteCache: wbcache,
		Staging:    ledger,
	})
	if err != nil {
		t.Fatal(err)
	}
	wb.wbCacheMaxBytes = 32 << 10
	t.Cleanup(func() { _ = wb.Stop(context.Background()) })
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}

	// Phase 1: Land a 256 KiB file, stage an overwrite while holding uploads,
	// close fd, then Unlink the closed file -> quotaBytes must return to 0!
	payload := bytes.Repeat([]byte("Q"), 256<<10)
	res, err := ch.Dispatch(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/overwrite-unlink.bin",
		Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644, HasMode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Write(ctx, 0, payload); err != nil {
		t.Fatal(err)
	}
	_ = res.Handle.Flush(ctx)
	_ = res.Handle.Release(ctx)
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if got := ledger.Shadow.QuotaBytes(); got != 0 {
		t.Fatalf("after initial drain QuotaBytes = %d, want 0", got)
	}

	holdUploads.Store(true)
	res2, err := ch.Dispatch(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/overwrite-unlink.bin", Flags: uint32(syscall.O_RDWR),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := res2.Handle.Write(ctx, 0, bytes.Repeat([]byte("W"), 512<<10)); err != nil {
		t.Fatal(err)
	}
	if err := res2.Handle.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := res2.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if ledger.Shadow.QuotaBytes() == 0 {
		t.Fatal("expected staged overwrite to charge QuotaBytes before unlink")
	}
	holdUploads.Store(false)
	close(releaseUploads)
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/overwrite-unlink.bin"}); err != nil {
		t.Fatalf("unlink closed file: %v", err)
	}
	if got := ledger.Shadow.QuotaBytes(); got != 0 {
		t.Fatalf("after closed-file unlink QuotaBytes = %d, want 0", got)
	}

	// Phase 2: Open-file unlink (MarkPathUnlinked) on a 1 GiB sparse file +
	// anonymous reader -> must NOT densify 1 GiB into QuotaBytes, and must
	// end at exact 0 after Release + deferred OpUnlink!
	sparseRes, err := ch.Dispatch(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/sparse-1g.bin",
		Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644, HasMode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := sparseRes.Handle.Write(ctx, 0, []byte("header")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{
		Kind: fs.OpSetAttr, Path: "/sparse-1g.bin", Size: 1 << 30, Handle: sparseRes.Handle, Ino: 99,
	}); err != nil {
		t.Fatalf("ftruncate 1GiB: %v", err)
	}
	if err := sparseRes.Handle.Sync(ctx); err != nil {
		t.Fatalf("interactive sync 1GiB sparse: %v", err)
	}
	readerRes, err := ch.Dispatch(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/sparse-1g.bin", Flags: uint32(syscall.O_RDONLY),
	})
	if err != nil {
		t.Fatalf("open reader on sparse 1GiB: %v", err)
	}
	if err := wb.MarkPathUnlinked("/sparse-1g.bin"); err != nil {
		t.Fatalf("MarkPathUnlinked sparse 1GiB: %v", err)
	}
	if got := ledger.Shadow.QuotaBytes(); got > 1<<20 {
		t.Fatalf("MarkPathUnlinked inflated sparse 1GiB QuotaBytes to %d", got)
	}
	rbuf := make([]byte, 6)
	if n, rerr := readerRes.Handle.Read(ctx, 0, rbuf); rerr != nil || string(rbuf[:n]) != "header" {
		t.Fatalf("anonymous reader after MarkPathUnlinked = (%q, %v), want header", string(rbuf[:n]), rerr)
	}
	_ = sparseRes.Handle.Release(ctx)
	_ = readerRes.Handle.Release(ctx)
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/sparse-1g.bin"}); err != nil && !fs.IsNotFound(err) {
		t.Fatalf("deferred OpUnlink: %v", err)
	}
	if got := ledger.Shadow.QuotaBytes(); got != 0 {
		t.Fatalf("after open-unlink cleanup QuotaBytes = %d, want 0", got)
	}

	// Phase 3: RetainShadowForLanding followed by small wbCache commit + close -> QuotaBytes == 0.
	retRes, err := ch.Dispatch(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/retained.bin",
		Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644, HasMode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := retRes.Handle.Write(ctx, 0, bytes.Repeat([]byte("R"), 128<<10)); err != nil {
		t.Fatal(err)
	}
	if err := retRes.Handle.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	// Mutate handle to a small 8-byte image before the 128 KiB commit lands so
	// completeStagedLanding retains the shadow, then close so wbCache lands it.
	if _, err := ch.Dispatch(ctx, &fs.Op{
		Kind: fs.OpSetAttr, Path: "/retained.bin", Size: 8, Handle: retRes.Handle, Ino: 100,
	}); err != nil {
		t.Fatal(err)
	}
	if err := retRes.Handle.Write(ctx, 0, []byte("tiny-end")); err != nil {
		t.Fatal(err)
	}
	_ = retRes.Handle.Flush(ctx)
	_ = retRes.Handle.Release(ctx)
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if got := ledger.Shadow.QuotaBytes(); got != 0 {
		t.Fatalf("after retained-shadow close & drain QuotaBytes = %d, want 0", got)
	}
}

// TestShadowQuotaBackpressureWaitsForDrainAndRejectsTrueExcess verifies Issue 2b:
// when ShadowStore.quotaBytes reaches writeCacheMaxBytes while CommitQueue work
// is queued/in-flight, foreground staging waits for background drain instead of
// failing immediately with ENOSPC, whereas genuinely un-drainable excess still
// returns ENOSPC promptly.
func TestShadowQuotaBackpressureWaitsForDrainAndRejectsTrueExcess(t *testing.T) {
	ctx := context.Background()
	b := testbackend.NewBackend()

	putEntered := make(chan struct{}, 2)
	allowPut := make(chan struct{})
	var blockPut atomic.Bool
	blockPut.Store(true)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if blockPut.Load() && (r.Method == http.MethodPut || r.Method == http.MethodPost) {
			select {
			case putEntered <- struct{}{}:
			default:
			}
			select {
			case <-allowPut:
			case <-r.Context().Done():
				http.Error(w, "canceled", http.StatusRequestTimeout)
				return
			}
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)

	cl := client.New(ts.URL, "")
	// 1 MiB writeback shadow quota.
	ledger, err := staging.NewStagingWithQuota(t.TempDir(), 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledger.Close)
	wbcache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{
		Policy:     PolicyWriteBack,
		Client:     cl,
		WriteCache: wbcache,
		Staging:    ledger,
	})
	if err != nil {
		t.Fatal(err)
	}
	wb.wbCacheMaxBytes = 32 << 10
	t.Cleanup(func() { _ = wb.Stop(context.Background()) })
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}

	// Stage 800 KiB into the 1 MiB quota while the upload worker is parked.
	r1, err := ch.Dispatch(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/first.bin",
		Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY), Mode: 0o644, HasMode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r1.Handle.Write(ctx, 0, bytes.Repeat([]byte("A"), 800<<10)); err != nil {
		t.Fatal(err)
	}
	if err := r1.Handle.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r1.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-putEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("background commit did not start")
	}

	// Now write another 600 KiB (800 + 600 > 1024 KiB): Flush must block on
	// backpressure rather than immediately failing with ENOSPC, then succeed
	// once the first upload completes!
	r2, err := ch.Dispatch(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/second.bin",
		Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY), Mode: 0o644, HasMode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r2.Handle.Write(ctx, 0, bytes.Repeat([]byte("B"), 600<<10)); err != nil {
		t.Fatal(err)
	}
	flushDone := make(chan error, 1)
	go func() {
		flushDone <- r2.Handle.Flush(ctx)
	}()

	select {
	case ferr := <-flushDone:
		t.Fatalf("second Flush returned before quota drained: err=%v", ferr)
	case <-time.After(80 * time.Millisecond):
	}

	// Unblock background commits so /first.bin lands and frees 800 KiB.
	blockPut.Store(false)
	close(allowPut)

	select {
	case ferr := <-flushDone:
		if ferr != nil {
			t.Fatalf("second Flush after backpressure drain failed: %v", ferr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second Flush did not unblock after quota drained")
	}
	_ = r2.Handle.Release(ctx)
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if got := ledger.Shadow.QuotaBytes(); got != 0 {
		t.Fatalf("QuotaBytes after drain = %d, want 0", got)
	}

	// A single image larger than the entire 1 MiB quota (1.5 MiB) with an idle
	// queue must return ENOSPC promptly without stalling.
	r3, err := ch.Dispatch(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/oversize.bin",
		Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY), Mode: 0o644, HasMode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r3.Handle.Write(ctx, 0, bytes.Repeat([]byte("C"), 1536<<10)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	ferr := r3.Handle.Flush(ctx)
	if !errors.Is(ferr, syscall.ENOSPC) {
		t.Fatalf("oversize Flush err = %v, want ENOSPC", ferr)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("oversize ENOSPC took %v, expected immediate fail", elapsed)
	}
	// Mark unlinked so Release discards the oversize dirty buffer cleanly.
	_ = wb.MarkPathUnlinked("/oversize.bin")
	_ = r3.Handle.Release(ctx)
}

// TestInvalidPathsRejectedUpfrontAndPurgedOnUnlinkOrRmdir verifies Issue 3:
// 1. Control characters (0x04), backslashes (D:\abspath), and invalid UTF-8
//    (\xff\xfe) are synchronously rejected with EINVAL at writeback entrypoints
//    without polluting wbCache, ShadowStore, or DrainPending().
// 2. Defense-in-depth: pre-existing invalid local entries in uploader_cached or
//    commit_queue_conflicts are completely cleared on Unlink or parent Rmdir.
func TestInvalidPathsRejectedUpfrontAndPurgedOnUnlinkOrRmdir(t *testing.T) {
	ctx := context.Background()
	b := testbackend.NewBackend()
	b.Seed("/src.txt", []byte("ok"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)

	cl := client.New(ts.URL, "")
	wbcache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := staging.NewStaging(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledger.Close)
	wb, err := New(Options{
		Policy:     PolicyWriteBack,
		Client:     cl,
		WriteCache: wbcache,
		Staging:    ledger,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wb.Stop(context.Background()) })
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}

	badPaths := []string{
		"/dir/=\xd8\x04\xdc",
		`/dst/D:\abspath`,
		"/bad/\xff\xfe",
	}
	for _, bad := range badPaths {
		for _, op := range []*fs.Op{
			{Kind: fs.OpCreate, Path: bad, Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY), Mode: 0o644, HasMode: true},
			{Kind: fs.OpMkdir, Path: bad, Mode: 0o755},
			{Kind: fs.OpMknod, Path: bad, Mode: syscall.S_IFREG | 0o644},
			{Kind: fs.OpSymlink, Path: bad, Target: "../valid"},
			{Kind: fs.OpLink, Path: "/src.txt", Path2: bad},
			{Kind: fs.OpRename, Path: "/src.txt", Path2: bad},
		} {
			_, derr := ch.Dispatch(ctx, op)
			if !errors.Is(derr, syscall.EINVAL) {
				t.Fatalf("op=%v bad=%q err=%v, want EINVAL", op.Kind, bad, derr)
			}
		}
	}
	if reason, count := wb.DrainPending(); reason != "" || count != 0 {
		t.Fatalf("DrainPending after rejected bad paths = (%q, %d), want empty", reason, count)
	}

	// Defense-in-depth: pre-seed uploader_cached and commit_queue_conflicts
	// with invalid paths, verify DrainPending reports them, then Unlink / Rmdir
	// and verify DrainPending is ("", 0) and QuotaBytes is 0.
	badCached := `/work/dst/D:\abspath`
	if err := wbcache.PutWithBaseRev(badCached, []byte("badzip!"), 7, staging.PendingNew, 0); err != nil {
		t.Fatal(err)
	}
	badConflict := "/work/ucs2/=\xd8\x04\xdc"
	if err := ledger.Shadow.WriteFull(badConflict, []byte("ucs2"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Pending.PutWithBaseRev(badConflict, 4, staging.PendingNew, 0); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Pending.MarkConflict(badConflict); err != nil {
		t.Fatal(err)
	}

	if reason, count := wb.DrainPending(); count == 0 {
		t.Fatalf("expected DrainPending to report pre-seeded bad entries, got (%q, %d)", reason, count)
	}

	// Direct Unlink on badCached must not fail validateFSPath and must clear wbCache.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: badCached}); err != nil {
		t.Fatalf("Unlink pre-seeded invalid path: %v", err)
	}
	// Parent Rmdir on /work/ucs2 (never created remotely) must purge PendingConflict + ShadowStore!
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRmdir, Path: "/work/ucs2"}); err != nil {
			t.Errorf("Rmdir parent of conflicted invalid path: %v", err)
		}
	}()
	wg.Wait()

	if reason, count := wb.DrainPending(); reason != "" || count != 0 {
		t.Fatalf("DrainPending after Unlink+Rmdir cleanup = (%q, %d), want (\"\", 0)", reason, count)
	}
	if got := ledger.Shadow.QuotaBytes(); got != 0 {
		t.Fatalf("ShadowStore.QuotaBytes after Rmdir cleanup = %d, want 0", got)
	}
}
