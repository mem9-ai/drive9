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
	"github.com/mem9-ai/drive9/pkg/fusecomp/membudget"
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
	uploadEntered := make(chan struct{}, 4)
	releaseUploads := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if holdUploads.Load() && (r.Method == http.MethodPut || r.Method == http.MethodPost) {
			select {
			case uploadEntered <- struct{}{}:
			default:
			}
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

	// Phase 3: Deterministic RetainShadowForLanding followed by small wbCache
	// commit + close -> verify shadow was retained across landing, then freed to 0.
	releaseUploads = make(chan struct{})
	for len(uploadEntered) > 0 {
		<-uploadEntered
	}
	holdUploads.Store(true)
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
	select {
	case <-uploadEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("128 KiB upload for /retained.bin did not enter server gate")
	}
	// Mutate handle to a small 8-byte image WHILE the 128 KiB commit is held in
	// flight so completeStagedLanding deterministically calls RetainShadowForLanding!
	if _, err := ch.Dispatch(ctx, &fs.Op{
		Kind: fs.OpSetAttr, Path: "/retained.bin", Size: 8, Handle: retRes.Handle, Ino: 100,
	}); err != nil {
		t.Fatal(err)
	}
	if err := retRes.Handle.Write(ctx, 0, []byte("tiny-end")); err != nil {
		t.Fatal(err)
	}
	// Release the in-flight 128 KiB upload and wait for CommitQueue to settle it.
	holdUploads.Store(false)
	close(releaseUploads)
	if !wb.queue.WaitPathDeadline("/retained.bin", 5*time.Second) {
		t.Fatal("timed out waiting for /retained.bin 128 KiB commit to finish")
	}
	// While retRes.Handle is still open, RetainShadowForLanding must have kept
	// the shadow alive.
	if got := ledger.Shadow.QuotaBytes(); got == 0 {
		t.Fatal("expected shadow to remain retained while open handle holds post-stage edits")
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
	quotaWaitEntered := make(chan struct{})
	var quotaGateOnce sync.Once
	gateFn := func(gate string) {
		if gate == "shadow:quotaWait" {
			quotaGateOnce.Do(func() { close(quotaWaitEntered) })
		}
	}
	wb.testGateHook.Store(&gateFn)
	defer wb.testGateHook.Store(nil)

	flushDone := make(chan error, 1)
	go func() {
		flushDone <- r2.Handle.Flush(ctx)
	}()

	select {
	case ferr := <-flushDone:
		t.Fatalf("second Flush returned before reaching quota wait: err=%v", ferr)
	case <-quotaWaitEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("second Flush did not enter shadow:quotaWait gate")
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
//  1. Control characters (0x04), backslashes (D:\abspath), and invalid UTF-8
//     (\xff\xfe) are synchronously rejected with EINVAL at writeback entrypoints
//     without polluting wbCache, ShadowStore, or DrainPending().
//  2. Defense-in-depth: pre-existing invalid local entries in uploader_cached or
//     commit_queue_conflicts are completely cleared on Unlink or parent Rmdir.
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

// TestReviewEdgeCasesAndUploadCutConflictExcl covers the CI webdev/upload-cut-and-5xx
// regression (O_CREAT|O_EXCL over a PendingConflict whose remote row is absent)
// and PR review edge cases (interleaved multi-fd writes on unlanded creates,
// reopened partial write after remote landing, O_RDONLY live-writer visibility
// over wbCache, >=10MiB reopen without ledger not blocking on PUT, retried
// spillToShadow preserving evicted part bytes, and CancelPrefix generation fencing).
func TestReviewEdgeCasesAndUploadCutConflictExcl(t *testing.T) {
	ctx := context.Background()

	t.Run("excl-create-over-absent-pending-conflict-heals", func(t *testing.T) {
		b := testbackend.NewBackend()
		ts := httptest.NewServer(b)
		t.Cleanup(ts.Close)
		cl := client.New(ts.URL, "")
		ledger, err := staging.NewStagingWithQuota(t.TempDir(), 0, 16)
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
			SyncMode:   "interactive",
			Client:     cl,
			WriteCache: wbcache,
			Staging:    ledger,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wb.Stop(context.Background()) })
		terminal, _ := core.New(cl, core.Options{})
		ch, _ := fs.NewChain(wb, terminal)

		// Simulate upload-cut-and-5xx: /mid/f-2.bin.part failed upload (PendingConflict)
		// while remote file is absent.
		if err := ledger.Shadow.WriteFull("/mid/f-2.bin.part", []byte("stale-cut"), 1); err != nil {
			t.Fatal(err)
		}
		if _, err := ledger.Pending.PutWithBaseRev("/mid/f-2.bin.part", 9, staging.PendingOverwrite, 1); err != nil {
			t.Fatal(err)
		}
		if err := ledger.Pending.MarkConflict("/mid/f-2.bin.part"); err != nil {
			t.Fatal(err)
		}
		// GNU cp retry opens with O_CREAT|O_EXCL|O_WRONLY after Lookup 404s.
		res, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/mid/f-2.bin.part",
			Flags: uint32(syscall.O_CREAT | syscall.O_EXCL | syscall.O_WRONLY),
			Mode:  0o644, HasMode: true, Kernel: &gofuse.CreateIn{},
		})
		if err != nil {
			t.Fatalf("O_CREAT|O_EXCL over absent PendingConflict failed: %v", err)
		}
		if err := res.Handle.Write(ctx, 0, bytes.Repeat([]byte("M"), 64<<10)); err != nil {
			t.Fatal(err)
		}
		if err := res.Handle.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		_ = res.Handle.Release(ctx)
		if err := wb.Drain(ctx); err != nil {
			t.Fatal(err)
		}
		if reason, count := wb.DrainPending(); reason != "" || count != 0 {
			t.Fatalf("DrainPending = (%q, %d), want empty", reason, count)
		}
	})

	t.Run("interleaved-reopened-writes-reconcile", func(t *testing.T) {
		b := testbackend.NewBackend()
		ts := httptest.NewServer(b)
		t.Cleanup(ts.Close)
		cl := client.New(ts.URL, "")
		wbcache, _ := staging.NewWriteBackCache(t.TempDir())
		wb, _ := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: wbcache})
		t.Cleanup(func() { _ = wb.Stop(context.Background()) })
		terminal, _ := core.New(cl, core.Options{})
		ch, _ := fs.NewChain(wb, terminal)

		h1, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/interleave",
			Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		h2, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/interleave", Flags: uint32(syscall.O_RDWR),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := h1.Handle.Write(ctx, 0, []byte("A")); err != nil {
			t.Fatal(err)
		}
		if err := h2.Handle.Write(ctx, 1, []byte("B")); err != nil {
			t.Fatal(err)
		}
		if err := h1.Handle.Write(ctx, 2, []byte("C")); err != nil {
			t.Fatal(err)
		}
		_ = h2.Handle.Flush(ctx)
		_ = h2.Handle.Release(ctx)
		_ = h1.Handle.Flush(ctx)
		_ = h1.Handle.Release(ctx)
		if err := wb.Drain(ctx); err != nil {
			t.Fatal(err)
		}
		if n, ok := b.Node("/interleave"); !ok || string(n.Data) != "ABC" {
			t.Fatalf("/interleave = (%v, %q), want ABC", ok, n.Data)
		}
	})

	t.Run("reopened-partial-write-after-landing-loads-base", func(t *testing.T) {
		b := testbackend.NewBackend()
		ts := httptest.NewServer(b)
		t.Cleanup(ts.Close)
		cl := client.New(ts.URL, "")
		wbcache, _ := staging.NewWriteBackCache(t.TempDir())
		wb, _ := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: wbcache})
		t.Cleanup(func() { _ = wb.Stop(context.Background()) })
		terminal, _ := core.New(cl, core.Options{})
		ch, _ := fs.NewChain(wb, terminal)

		h1, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/landed-base",
			Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY), Mode: 0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := h1.Handle.Write(ctx, 0, []byte("abc")); err != nil {
			t.Fatal(err)
		}
		h2, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/landed-base", Flags: uint32(syscall.O_RDWR),
		})
		if err != nil {
			t.Fatal(err)
		}
		// Land h1 and retire wbCache before h2 ever touches its buffer!
		_ = h1.Handle.Flush(ctx)
		_ = h1.Handle.Release(ctx)
		if err := wb.Drain(ctx); err != nil {
			t.Fatal(err)
		}
		if _, hasMeta := wbcache.GetMeta("/landed-base"); hasMeta {
			t.Fatal("expected wbCache entry to be retired after Drain")
		}
		// Partial write at offset 1 on h2 must compose onto "abc" -> "aXc"!
		if err := h2.Handle.Write(ctx, 1, []byte("X")); err != nil {
			t.Fatal(err)
		}
		_ = h2.Handle.Flush(ctx)
		_ = h2.Handle.Release(ctx)
		if err := wb.Drain(ctx); err != nil {
			t.Fatal(err)
		}
		if n, ok := b.Node("/landed-base"); !ok || string(n.Data) != "aXc" {
			t.Fatalf("/landed-base = (%v, %q), want aXc", ok, n.Data)
		}
	})

	t.Run("rdonly-reopen-sees-subsequent-sibling-write", func(t *testing.T) {
		b := testbackend.NewBackend()
		b.Seed("/existing", []byte("v0"))
		ts := httptest.NewServer(b)
		t.Cleanup(ts.Close)
		cl := client.New(ts.URL, "")
		wbcache, _ := staging.NewWriteBackCache(t.TempDir())
		wb, _ := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: wbcache})
		stopSSETestWriteback(t, wb)
		t.Cleanup(func() { _ = wb.Stop(context.Background()) })
		terminal, _ := core.New(cl, core.Options{})
		ch, _ := fs.NewChain(wb, terminal)

		w1, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/existing", Flags: uint32(syscall.O_WRONLY)})
		if err != nil {
			t.Fatal(err)
		}
		if err := w1.Handle.Write(ctx, 0, []byte("v1")); err != nil {
			t.Fatal(err)
		}
		if err := w1.Handle.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		r1, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/existing", Flags: uint32(syscall.O_RDONLY)})
		if err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 16)
		n, err := r1.Handle.Read(ctx, 0, buf)
		if err != nil || string(buf[:n]) != "v1" {
			t.Fatalf("first read = (%q, %v), want v1", string(buf[:n]), err)
		}
		// Now w1 writes newer unflushed bytes; r1 must see them immediately!
		if err := w1.Handle.Write(ctx, 0, []byte("v2-fresh")); err != nil {
			t.Fatal(err)
		}
		n, err = r1.Handle.Read(ctx, 0, buf)
		if err != nil || string(buf[:n]) != "v2-fresh" {
			t.Fatalf("second read after sibling write = (%q, %v), want v2-fresh", string(buf[:n]), err)
		}
		// Flush + close w1 and Drain so "v2-fresh" lands remotely and wbCache
		// retires: linked r1 must STILL read "v2-fresh" (not stale "v1")!
		if err := w1.Handle.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		if err := w1.Handle.Release(ctx); err != nil {
			t.Fatal(err)
		}
		if err := wb.Drain(ctx); err != nil {
			t.Fatal(err)
		}
		n, err = r1.Handle.Read(ctx, 0, buf)
		if err != nil || string(buf[:n]) != "v2-fresh" {
			t.Fatalf("read after sibling write landed & drained = (%q, %v), want v2-fresh", string(buf[:n]), err)
		}
		_ = r1.Handle.Release(ctx)
	})

	t.Run("concurrent-sibling-writes-and-open-trunc-interleaving", func(t *testing.T) {
		b := testbackend.NewBackend()
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
		t.Cleanup(func() { _ = wb.Stop(context.Background()) })
		terminal, err := core.New(cl, core.Options{})
		if err != nil {
			t.Fatal(err)
		}
		ch, err := fs.NewChain(wb, terminal)
		if err != nil {
			t.Fatal(err)
		}

		waitGate := func(gate <-chan struct{}, done <-chan struct{}, errPtr *error, desc string) {
			t.Helper()
			timer := time.NewTimer(5 * time.Second)
			defer timer.Stop()
			select {
			case <-gate:
				return
			case <-done:
				select {
				case <-gate:
					return
				default:
				}
				var werr error
				if errPtr != nil {
					werr = *errPtr
				}
				t.Fatalf("%s completed before reaching gate (err=%v)", desc, werr)
			case <-timer.C:
				t.Fatalf("timed out waiting for %s", desc)
			}
		}

		// 1. Concurrent non-overlapping writes across co-open handles: pause A
		// at write:beforePropagate while B concurrently enters Write and contends
		// on coOpenLocks, then verify BOTH descriptors hold "AB" before flush.
		a, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/race-ab",
			Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		bH, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/race-ab", Flags: uint32(syscall.O_RDWR),
		})
		if err != nil {
			t.Fatal(err)
		}
		// Pre-initialize both buffers so both have buf != nil before the race.
		if err := a.Handle.Write(ctx, 0, []byte("__")); err != nil {
			t.Fatal(err)
		}
		if err := bH.Handle.Write(ctx, 0, []byte("__")); err != nil {
			t.Fatal(err)
		}

		aEntered := make(chan struct{})
		bContended := make(chan struct{})
		bAcquired := make(chan struct{})
		allowA := make(chan struct{})
		var aOnce, bContendOnce, bAcqOnce, allowAOnce sync.Once
		releaseAllowA := func() { allowAOnce.Do(func() { close(allowA) }) }
		t.Cleanup(func() {
			wb.testGateHook.Store(nil)
			releaseAllowA()
		})
		var watchB atomic.Bool
		hookAB := func(gate string) {
			switch gate {
			case "write:beforePropagate":
				aOnce.Do(func() {
					close(aEntered)
					<-allowA
				})
			case "write:coOpenContended":
				if watchB.Load() {
					bContendOnce.Do(func() { close(bContended) })
				}
			case "write:coOpenAcquired":
				if watchB.Load() {
					bAcqOnce.Do(func() { close(bAcquired) })
				}
			}
		}
		wb.testGateHook.Store(&hookAB)

		var aErr, bErr error
		aDone := make(chan struct{})
		bDone := make(chan struct{})
		go func() {
			aErr = a.Handle.Write(ctx, 0, []byte("A"))
			close(aDone)
		}()
		waitGate(aEntered, aDone, &aErr, "A write:beforePropagate")

		watchB.Store(true)
		go func() {
			bErr = bH.Handle.Write(ctx, 1, []byte("B"))
			close(bDone)
		}()
		waitGate(bContended, bDone, &bErr, "B write:coOpenContended")
		select {
		case <-bAcquired:
			t.Fatal("B acquired coOpenLocks before A released it")
		default:
		}
		releaseAllowA()
		waitGate(bAcquired, bDone, &bErr, "B write:coOpenAcquired")
		<-aDone
		if aErr != nil {
			t.Fatal(aErr)
		}
		<-bDone
		if bErr != nil {
			t.Fatal(bErr)
		}
		wb.testGateHook.Store(nil)

		// Verify BOTH descriptors hold "AB" BEFORE flushing either handle.
		bufA := make([]byte, 4)
		nA, err := a.Handle.Read(ctx, 0, bufA)
		if err != nil || string(bufA[:nA]) != "AB" {
			t.Fatalf("descriptor A pre-flush Read = (%q, %v), want AB", string(bufA[:nA]), err)
		}
		bufB := make([]byte, 4)
		nB, err := bH.Handle.Read(ctx, 0, bufB)
		if err != nil || string(bufB[:nB]) != "AB" {
			t.Fatalf("descriptor B pre-flush Read = (%q, %v), want AB", string(bufB[:nB]), err)
		}

		// Close A first, then B last, and verify backend persistence.
		_ = a.Handle.Flush(ctx)
		_ = a.Handle.Release(ctx)
		_ = bH.Handle.Flush(ctx)
		_ = bH.Handle.Release(ctx)
		if err := wb.Drain(ctx); err != nil {
			t.Fatal(err)
		}
		if n, ok := b.Node("/race-ab"); !ok || string(n.Data) != "AB" {
			t.Fatalf("/race-ab = (%v, %q), want AB", ok, n.Data)
		}

		// 2. Deterministic O_TRUNC + concurrent sibling Write interleaving:
		// s1 holds "abcdef"; t2 opens with O_WRONLY|O_TRUNC and pauses at
		// openTrunc:coOpenLocked while s1 concurrently contends on coOpenLocks;
		// releasing the gate must drop "abcdef" AND preserve "Y" -> "\x00\x00Y"!
		s1, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/trunc-interleave",
			Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := s1.Handle.Write(ctx, 0, []byte("abcdef")); err != nil {
			t.Fatal(err)
		}

		truncEntered := make(chan struct{})
		writeContended := make(chan struct{})
		allowTrunc := make(chan struct{})
		var truncOnce, writeOnce, allowTruncOnce sync.Once
		releaseAllowTrunc := func() { allowTruncOnce.Do(func() { close(allowTrunc) }) }
		t.Cleanup(func() {
			wb.testGateHook.Store(nil)
			releaseAllowTrunc()
		})
		hookTrunc := func(gate string) {
			switch gate {
			case "openTrunc:coOpenLocked":
				truncOnce.Do(func() {
					close(truncEntered)
					<-allowTrunc
				})
			case "write:coOpenContended":
				writeOnce.Do(func() {
					close(writeContended)
				})
			}
		}
		wb.testGateHook.Store(&hookTrunc)

		var t2Res fs.Result
		var t2Err error
		t2Done := make(chan struct{})
		go func() {
			t2Res, t2Err = ch.Dispatch(ctx, &fs.Op{
				Kind: fs.OpCreate, Path: "/trunc-interleave",
				Flags: uint32(syscall.O_WRONLY | syscall.O_TRUNC),
			})
			close(t2Done)
		}()
		waitGate(truncEntered, t2Done, &t2Err, "openTrunc:coOpenLocked")

		var s1WriteErr error
		s1WriteDone := make(chan struct{})
		go func() {
			s1WriteErr = s1.Handle.Write(ctx, 2, []byte("Y"))
			close(s1WriteDone)
		}()
		waitGate(writeContended, s1WriteDone, &s1WriteErr, "s1 write:coOpenContended")
		releaseAllowTrunc()

		<-t2Done
		if t2Err != nil {
			t.Fatalf("open O_TRUNC: %v", t2Err)
		}
		<-s1WriteDone
		if s1WriteErr != nil {
			t.Fatalf("post-trunc sibling write: %v", s1WriteErr)
		}
		wb.testGateHook.Store(nil)

		// Flush s1 LAST so if s1 had retained pre-truncate "abcdef" tail it
		// would stage "abYdef" instead of "\x00\x00Y"!
		_ = t2Res.Handle.Flush(ctx)
		_ = t2Res.Handle.Release(ctx)
		_ = s1.Handle.Flush(ctx)
		_ = s1.Handle.Release(ctx)
		if err := wb.Drain(ctx); err != nil {
			t.Fatal(err)
		}
		if n, ok := b.Node("/trunc-interleave"); !ok || !bytes.Equal(n.Data, []byte{0, 0, 'Y'}) {
			t.Fatalf("/trunc-interleave = (%v, %q), want \"\\x00\\x00Y\"", ok, n.Data)
		}
	})

	t.Run("cancel-prefix-coherent-pair-preserves-newer-stage", func(t *testing.T) {
		ledger, err := staging.NewStagingWithQuota(t.TempDir(), 0, 16)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(ledger.Close)
		b := testbackend.NewBackend()
		ts := httptest.NewServer(b)
		t.Cleanup(ts.Close)
		cl := client.New(ts.URL, "")
		wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, Staging: ledger})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wb.Stop(context.Background()) })

		// Stage an old conflicted entry under /sub/old.bin and an older version
		// on /sub/replace.bin before CancelPrefix captures its watermarks.
		if err := ledger.Shadow.WriteFull("/sub/old.bin", []byte("old-shadow"), 0); err != nil {
			t.Fatal(err)
		}
		if _, err := ledger.Pending.PutWithBaseRev("/sub/old.bin", 10, staging.PendingNew, 0); err != nil {
			t.Fatal(err)
		}
		if err := ledger.Pending.MarkConflict("/sub/old.bin"); err != nil {
			t.Fatal(err)
		}
		if err := ledger.Shadow.WriteFull("/sub/replace.bin", []byte("stale"), 0); err != nil {
			t.Fatal(err)
		}
		if _, err := ledger.Pending.PutWithBaseRev("/sub/replace.bin", 5, staging.PendingNew, 0); err != nil {
			t.Fatal(err)
		}

		snapCaptured := make(chan struct{})
		allowCancel := make(chan struct{})
		var snapOnce, allowOnce sync.Once
		releaseCancel := func() { allowOnce.Do(func() { close(allowCancel) }) }
		t.Cleanup(func() {
			wb.testGateHook.Store(nil)
			releaseCancel()
		})
		hookSnap := func(gate string) {
			if gate == "cancelPrefix:afterSnapshot" {
				snapOnce.Do(func() {
					close(snapCaptured)
					<-allowCancel
				})
			}
		}
		wb.testGateHook.Store(&hookSnap)

		cancelDone := make(chan struct{})
		go func() {
			defer close(cancelDone)
			wb.queue.CancelPrefix("/sub/")
		}()
		select {
		case <-snapCaptured:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for CancelPrefix snapshot gate")
		}

		// Publish a replacement pair on /sub/replace.bin AFTER CancelPrefix
		// captured maxPendingAtCancel/maxShadowAtCancel; both items must survive!
		unlockRep := wb.pathLocks.lock("/sub/replace.bin")
		if err := ledger.Shadow.WriteFull("/sub/replace.bin", []byte("coherent-new"), 0); err != nil {
			unlockRep()
			t.Fatal(err)
		}
		if _, err := ledger.Pending.PutWithBaseRev("/sub/replace.bin", 12, staging.PendingNew, 0); err != nil {
			unlockRep()
			t.Fatal(err)
		}
		unlockRep()

		releaseCancel()
		select {
		case <-cancelDone:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for CancelPrefix completion")
		}
		wb.testGateHook.Store(nil)

		if !ledger.Pending.HasPending("/sub/replace.bin") || !ledger.Shadow.Has("/sub/replace.bin") {
			t.Fatalf("expected post-snapshot /sub/replace.bin pair to survive: hasPending=%v hasShadow=%v",
				ledger.Pending.HasPending("/sub/replace.bin"), ledger.Shadow.Has("/sub/replace.bin"))
		}
		gotPayload, err := ledger.Shadow.ReadAll("/sub/replace.bin")
		if err != nil || string(gotPayload) != "coherent-new" {
			t.Fatalf("/sub/replace.bin shadow = (%q, %v), want coherent-new", string(gotPayload), err)
		}
		if ledger.Pending.HasPending("/sub/old.bin") || ledger.Shadow.Has("/sub/old.bin") {
			t.Fatal("expected /sub/old.bin to be purged by CancelPrefix")
		}
		ledger.Pending.Remove("/sub/replace.bin")
		ledger.Shadow.Remove("/sub/replace.bin")
	})

	t.Run("reopen-bypass-size-without-ledger-does-not-put-on-open", func(t *testing.T) {
		b := testbackend.NewBackend()
		var puts atomic.Int64
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPut || r.Method == http.MethodPost {
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
		wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: wbcache})
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

		h1, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/oversize-no-ledger",
			Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY), Mode: 0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		want := bytes.Repeat([]byte("Z"), 64<<10)
		if err := h1.Handle.Write(ctx, 0, want); err != nil {
			t.Fatal(err)
		}
		puts.Store(0)
		h2, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/oversize-no-ledger", Flags: uint32(syscall.O_RDONLY),
		})
		if err != nil {
			t.Fatalf("reopen without ledger: %v", err)
		}
		if got := puts.Load(); got != 0 {
			t.Fatalf("reopen without ledger issued %d remote PUTs during open(2), want 0", got)
		}
		gotBuf := make([]byte, len(want))
		if n, rerr := h2.Handle.Read(ctx, 0, gotBuf); rerr != nil || !bytes.Equal(gotBuf[:n], want) {
			t.Fatalf("read from unlanded oversize sibling = (%d, %v)", n, rerr)
		}
		_ = h2.Handle.Release(ctx)
		_ = h1.Handle.Release(ctx)
	})

	t.Run("retried-spill-preserves-part0-bytes", func(t *testing.T) {
		// 18 MiB shadow quota: 8 MiB occupied by /other.bin leaves 10 MiB free.
		// A 12 MiB (8 MiB Part 0 + 4 MiB Part 1) zero-base spill with
		// retainBuffer=false writes Part 0 (8+8=16 <= 18 MiB) and fails Part 1
		// with ENOSPC (16+4=20 > 18 MiB). Freeing /other.bin and retrying
		// spillToShadow must preserve Part 0's 'P' bytes instead of truncating
		// and skipping them as zeros.
		ledger, err := staging.NewStagingWithQuota(t.TempDir(), 0, 18)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(ledger.Close)
		if err := ledger.Shadow.WriteFull("/other.bin", bytes.Repeat([]byte("O"), 8<<20), 0); err != nil {
			t.Fatal(err)
		}
		b := testbackend.NewBackend()
		ts := httptest.NewServer(b)
		t.Cleanup(ts.Close)
		cl := client.New(ts.URL, "")
		wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, Staging: ledger})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wb.Stop(context.Background()) })
		terminal, _ := core.New(cl, core.Options{})
		ch, _ := fs.NewChain(wb, terminal)

		multi, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/multi12m.bin",
			Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY), Mode: 0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		part0 := bytes.Repeat([]byte("P"), 8<<20)
		part1 := bytes.Repeat([]byte("Q"), 4<<20)
		want := append(append([]byte(nil), part0...), part1...)
		if err := multi.Handle.Write(ctx, 0, want); err != nil {
			t.Fatal(err)
		}
		bh := writebackHandleOf(multi.Handle)
		bh.mu.Lock()
		firstErr := bh.spillToShadow(ledger, "/multi12m.bin", bh.buf, 0, true, false)
		bh.mu.Unlock()
		if !errors.Is(firstErr, syscall.ENOSPC) {
			t.Fatalf("first spillToShadow err = %v, want ENOSPC on Part 1", firstErr)
		}

		// Free /other.bin so quota becomes available, then retry spillToShadow.
		ledger.Shadow.Remove("/other.bin")
		bh.mu.Lock()
		retryErr := bh.spillToShadow(ledger, "/multi12m.bin", bh.buf, 0, true, false)
		bh.mu.Unlock()
		if retryErr != nil {
			t.Fatalf("retry spillToShadow err = %v, want nil", retryErr)
		}
		spilled, err := ledger.Shadow.ReadAll("/multi12m.bin")
		if err != nil {
			t.Fatal(err)
		}
		if len(spilled) != len(want) || !bytes.Equal(spilled[:64], part0[:64]) || !bytes.Equal(spilled[8<<20:8<<20+64], part1[:64]) {
			t.Fatalf("spilled image corrupted after retry: len=%d head=%q part1=%q", len(spilled), spilled[:8], spilled[8<<20:8<<20+8])
		}
		ledger.Shadow.Remove("/multi12m.bin")
		bh.mu.Lock()
		bh.dirty = false
		bh.landed = true
		bh.mu.Unlock()
		_ = multi.Handle.Release(ctx)
	})

	t.Run("post-landing-coopen-writes-preserve-both-descriptors", func(t *testing.T) {
		for _, closeOrder := range []string{"creator-last", "reopener-last"} {
			t.Run(closeOrder, func(t *testing.T) {
				b := testbackend.NewBackend()
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
				t.Cleanup(func() { _ = wb.Stop(context.Background()) })
				terminal, err := core.New(cl, core.Options{})
				if err != nil {
					t.Fatal(err)
				}
				ch, err := fs.NewChain(wb, terminal)
				if err != nil {
					t.Fatal(err)
				}

				h1, err := ch.Dispatch(ctx, &fs.Op{
					Kind: fs.OpCreate, Path: "/post-land",
					Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644, HasMode: true,
				})
				if err != nil {
					t.Fatal(err)
				}
				h2, err := ch.Dispatch(ctx, &fs.Op{
					Kind: fs.OpCreate, Path: "/post-land", Flags: uint32(syscall.O_RDWR),
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := h1.Handle.Write(ctx, 0, []byte("abc")); err != nil {
					t.Fatal(err)
				}
				if err := h1.Handle.Flush(ctx); err != nil {
					t.Fatal(err)
				}
				if err := wb.Drain(ctx); err != nil {
					t.Fatal(err)
				}
				// Now create has landed (isNew=false on both handles). Write "X"
				// on creator h1 and "Y" on reopener h2, then commit in both orders!
				if err := h1.Handle.Write(ctx, 0, []byte("X")); err != nil {
					t.Fatal(err)
				}
				if err := h2.Handle.Write(ctx, 2, []byte("Y")); err != nil {
					t.Fatal(err)
				}
				if closeOrder == "creator-last" {
					if err := h2.Handle.Flush(ctx); err != nil {
						t.Fatalf("h2.Flush: %v", err)
					}
					if err := h2.Handle.Release(ctx); err != nil {
						t.Fatalf("h2.Release: %v", err)
					}
					if err := wb.Drain(ctx); err != nil {
						t.Fatal(err)
					}
					if err := h1.Handle.Flush(ctx); err != nil {
						t.Fatalf("h1.Flush: %v", err)
					}
					if err := h1.Handle.Release(ctx); err != nil {
						t.Fatalf("h1.Release: %v", err)
					}
				} else {
					if err := h1.Handle.Flush(ctx); err != nil {
						t.Fatalf("h1.Flush: %v", err)
					}
					if err := h1.Handle.Release(ctx); err != nil {
						t.Fatalf("h1.Release: %v", err)
					}
					if err := wb.Drain(ctx); err != nil {
						t.Fatal(err)
					}
					if err := h2.Handle.Flush(ctx); err != nil {
						t.Fatalf("h2.Flush: %v", err)
					}
					if err := h2.Handle.Release(ctx); err != nil {
						t.Fatalf("h2.Release: %v", err)
					}
				}
				if err := wb.Drain(ctx); err != nil {
					t.Fatal(err)
				}
				if n, ok := b.Node("/post-land"); !ok || string(n.Data) != "XbY" {
					t.Fatalf("/post-land (%s) = (%v, %q), want XbY", closeOrder, ok, n.Data)
				}
			})
		}
	})

	t.Run("unlinked-local-reader-serves-original-bytes-across-recreate", func(t *testing.T) {
		b := testbackend.NewBackend()
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
		stopSSETestWriteback(t, wb)
		t.Cleanup(func() { _ = wb.Stop(context.Background()) })
		terminal, err := core.New(cl, core.Options{})
		if err != nil {
			t.Fatal(err)
		}
		ch, err := fs.NewChain(wb, terminal)
		if err != nil {
			t.Fatal(err)
		}

		w1, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/unlink-recreate",
			Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY), Mode: 0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := w1.Handle.Write(ctx, 0, []byte("orig-v1")); err != nil {
			t.Fatal(err)
		}
		r1, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/unlink-recreate", Flags: uint32(syscall.O_RDONLY),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := w1.Handle.Write(ctx, 0, []byte("orig-v2")); err != nil {
			t.Fatal(err)
		}
		_ = w1.Handle.Flush(ctx)
		_ = w1.Handle.Release(ctx)
		if err := wb.Drain(ctx); err != nil {
			t.Fatal(err)
		}

		// Pause r1 inside readRemoteRangeFallback while concurrent Unlink +
		// Recreate tries to replace /unlink-recreate with "REPLACEMENT".
		inFallback := make(chan struct{})
		allowFallback := make(chan struct{})
		unlinkWaiting := make(chan struct{})
		var fbOnce, allowOnce, waitOnce sync.Once
		releaseFB := func() { allowOnce.Do(func() { close(allowFallback) }) }
		t.Cleanup(func() {
			wb.testGateHook.Store(nil)
			releaseFB()
		})
		hookFB := func(gate string) {
			switch gate {
			case "read:remoteFallback":
				fbOnce.Do(func() {
					close(inFallback)
					<-allowFallback
				})
			case "unlink:waitingLinkedReads":
				waitOnce.Do(func() { close(unlinkWaiting) })
			}
		}
		wb.testGateHook.Store(&hookFB)

		type readRes struct {
			data string
			err  error
		}
		readDone := make(chan readRes, 1)
		go func() {
			buf := make([]byte, 32)
			n, rerr := r1.Handle.Read(ctx, 0, buf)
			readDone <- readRes{data: string(buf[:n]), err: rerr}
		}()
		select {
		case <-inFallback:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for read:remoteFallback")
		}

		unlinkDone := make(chan error, 1)
		go func() {
			_, uerr := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/unlink-recreate"})
			unlinkDone <- uerr
		}()
		// Unlink must reach the read-drain boundary and remain blocked until
		// readRemoteRangeFallback finishes.
		select {
		case <-unlinkWaiting:
		case uerr := <-unlinkDone:
			t.Fatalf("Unlink completed before reaching linked-read drain boundary: %v", uerr)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for unlink:waitingLinkedReads")
		}
		select {
		case uerr := <-unlinkDone:
			t.Fatalf("Unlink completed while readRemoteRangeFallback was still paused: %v", uerr)
		default:
		}
		releaseFB()

		rr := <-readDone
		if rr.err != nil || rr.data != "orig-v2" {
			t.Fatalf("in-flight Read = (%q, %v), want orig-v2", rr.data, rr.err)
		}
		if uerr := <-unlinkDone; uerr != nil {
			t.Fatalf("Unlink: %v", uerr)
		}
		wb.testGateHook.Store(nil)

		// Recreate /unlink-recreate with completely different bytes and land it.
		w2, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/unlink-recreate",
			Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY), Mode: 0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := w2.Handle.Write(ctx, 0, []byte("REPLACEMENT")); err != nil {
			t.Fatal(err)
		}
		_ = w2.Handle.Flush(ctx)
		_ = w2.Handle.Release(ctx)
		if err := wb.Drain(ctx); err != nil {
			t.Fatal(err)
		}

		// Anonymous r1 must STILL serve "orig-v2", never "REPLACEMENT"!
		buf := make([]byte, 32)
		n, err := r1.Handle.Read(ctx, 0, buf)
		if err != nil || string(buf[:n]) != "orig-v2" {
			t.Fatalf("post-unlink Read after recreate = (%q, %v), want orig-v2", string(buf[:n]), err)
		}
		_ = r1.Handle.Release(ctx)
	})

	t.Run("open-trunc-sweep-ignores-renamed-path-replacement", func(t *testing.T) {
		b := testbackend.NewBackend()
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
		t.Cleanup(func() { _ = wb.Stop(context.Background()) })
		terminal, err := core.New(cl, core.Options{})
		if err != nil {
			t.Fatal(err)
		}
		ch, err := fs.NewChain(wb, terminal)
		if err != nil {
			t.Fatal(err)
		}

		orig, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/swap-f",
			Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := orig.Handle.Write(ctx, 0, []byte("old-bytes")); err != nil {
			t.Fatal(err)
		}

		truncPaused := make(chan struct{})
		allowTrunc := make(chan struct{})
		var pOnce, aOnce sync.Once
		releaseTrunc := func() { aOnce.Do(func() { close(allowTrunc) }) }
		t.Cleanup(func() {
			wb.testGateHook.Store(nil)
			releaseTrunc()
		})
		hook := func(gate string) {
			if gate == "openTrunc:coOpenLocked" {
				pOnce.Do(func() {
					close(truncPaused)
					<-allowTrunc
				})
			}
		}
		wb.testGateHook.Store(&hook)

		tDone := make(chan fs.Result, 1)
		tErr := make(chan error, 1)
		go func() {
			r, oerr := ch.Dispatch(ctx, &fs.Op{
				Kind: fs.OpCreate, Path: "/swap-f",
				Flags: uint32(syscall.O_WRONLY | syscall.O_TRUNC),
			})
			if oerr != nil {
				tErr <- oerr
				return
			}
			tDone <- r
		}()
		select {
		case <-truncPaused:
		case oerr := <-tErr:
			t.Fatalf("open O_TRUNC failed early: %v", oerr)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for openTrunc:coOpenLocked")
		}

		// Simulate rename retargeting the original incarnation from /swap-f -> /swap-g,
		// then creating a brand-new replacement handle at /swap-f holding "keep-me"!
		wb.retarget("/swap-f", "/swap-g")
		rep, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/swap-f",
			Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := rep.Handle.Write(ctx, 0, []byte("keep-me")); err != nil {
			t.Fatal(err)
		}

		releaseTrunc()
		var tRes fs.Result
		select {
		case tRes = <-tDone:
		case oerr := <-tErr:
			t.Fatalf("open O_TRUNC failed: %v", oerr)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for open O_TRUNC completion")
		}
		wb.testGateHook.Store(nil)

		// Replacement handle at /swap-f MUST retain "keep-me", while retargeted
		// orig at /swap-g was truncated to 0 bytes!
		gotRep := make([]byte, 16)
		n, err := rep.Handle.Read(ctx, 0, gotRep)
		if err != nil || string(gotRep[:n]) != "keep-me" {
			t.Fatalf("replacement /swap-f Read = (%q, %v), want keep-me", string(gotRep[:n]), err)
		}
		gotOrig := make([]byte, 16)
		nOrig, err := orig.Handle.Read(ctx, 0, gotOrig)
		if err != nil || nOrig != 0 {
			t.Fatalf("truncated /swap-g Read = (%q, %d, %v), want empty", string(gotOrig[:nOrig]), nOrig, err)
		}
		_ = rep.Handle.Release(ctx)
		_ = tRes.Handle.Release(ctx)
		_ = orig.Handle.Release(ctx)
	})

	t.Run("read-before-live-writer-fenced-against-unlink-recreate", func(t *testing.T) {
		b := testbackend.NewBackend()
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
		stopSSETestWriteback(t, wb)
		t.Cleanup(func() { _ = wb.Stop(context.Background()) })
		terminal, err := core.New(cl, core.Options{})
		if err != nil {
			t.Fatal(err)
		}
		ch, err := fs.NewChain(wb, terminal)
		if err != nil {
			t.Fatal(err)
		}

		w1, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/live-writer-fence",
			Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY), Mode: 0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := w1.Handle.Write(ctx, 0, []byte("orig-landed")); err != nil {
			t.Fatal(err)
		}
		r1, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/live-writer-fence", Flags: uint32(syscall.O_RDONLY),
		})
		if err != nil {
			t.Fatal(err)
		}
		_ = w1.Handle.Flush(ctx)
		_ = w1.Handle.Release(ctx)
		if err := wb.Drain(ctx); err != nil {
			t.Fatal(err)
		}

		pausedBeforeLive := make(chan struct{})
		allowLive := make(chan struct{})
		unlinkWaiting := make(chan struct{})
		var pOnce, aOnce, wOnce sync.Once
		releaseLive := func() { aOnce.Do(func() { close(allowLive) }) }
		t.Cleanup(func() {
			wb.testGateHook.Store(nil)
			releaseLive()
		})
		hook := func(gate string) {
			switch gate {
			case "read:beforeLiveWriter":
				pOnce.Do(func() {
					close(pausedBeforeLive)
					<-allowLive
				})
			case "unlink:waitingLinkedReads":
				wOnce.Do(func() { close(unlinkWaiting) })
			}
		}
		wb.testGateHook.Store(&hook)

		type readOut struct {
			data string
			err  error
		}
		rDone := make(chan readOut, 1)
		go func() {
			buf := make([]byte, 32)
			n, rerr := r1.Handle.Read(ctx, 0, buf)
			rDone <- readOut{data: string(buf[:n]), err: rerr}
		}()
		select {
		case <-pausedBeforeLive:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for read:beforeLiveWriter")
		}

		unlinkDone := make(chan error, 1)
		go func() {
			_, uerr := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/live-writer-fence"})
			unlinkDone <- uerr
		}()
		select {
		case <-unlinkWaiting:
		case uerr := <-unlinkDone:
			t.Fatalf("Unlink finished before reaching linked-read drain boundary: %v", uerr)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for unlink:waitingLinkedReads")
		}
		select {
		case uerr := <-unlinkDone:
			t.Fatalf("Unlink finished before linked Read drained: %v", uerr)
		default:
		}
		releaseLive()
		ro := <-rDone
		if ro.err != nil || ro.data != "orig-landed" {
			t.Fatalf("in-flight Read = (%q, %v), want orig-landed", ro.data, ro.err)
		}
		if uerr := <-unlinkDone; uerr != nil {
			t.Fatalf("Unlink: %v", uerr)
		}
		wb.testGateHook.Store(nil)

		// Recreate with a dirty live writer at the same path; r1 must NOT read it!
		w2, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/live-writer-fence",
			Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := w2.Handle.Write(ctx, 0, []byte("NEW-DIRTY-WRITER")); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 32)
		n, err := r1.Handle.Read(ctx, 0, buf)
		if err != nil || string(buf[:n]) != "orig-landed" {
			t.Fatalf("post-unlink Read with replacement dirty writer = (%q, %v), want orig-landed", string(buf[:n]), err)
		}
		_ = w2.Handle.Release(ctx)
		_ = r1.Handle.Release(ctx)
	})

	t.Run("otrunc-large-file-multi-flush-preserves-committed-prefix", func(t *testing.T) {
		ledger, err := staging.NewStagingWithQuota(t.TempDir(), 0, 64)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(ledger.Close)
		b := testbackend.NewBackend()
		b.Seed("/big.bin", []byte("old-seed"))
		ts := httptest.NewServer(b)
		t.Cleanup(ts.Close)
		cl := client.New(ts.URL, "")
		wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, Staging: ledger})
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

		res, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/big.bin",
			Flags: uint32(syscall.O_RDWR | syscall.O_TRUNC),
		})
		if err != nil {
			t.Fatal(err)
		}
		prefix := bytes.Repeat([]byte("A"), 16<<20)
		if err := res.Handle.Write(ctx, 0, prefix); err != nil {
			t.Fatal(err)
		}
		if err := res.Handle.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		if err := wb.Drain(ctx); err != nil {
			t.Fatal(err)
		}
		if err := res.Handle.Write(ctx, 16<<20, []byte("TAIL")); err != nil {
			t.Fatal(err)
		}
		if err := res.Handle.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		if err := res.Handle.Release(ctx); err != nil {
			t.Fatal(err)
		}
		if err := wb.Drain(ctx); err != nil {
			t.Fatal(err)
		}
		n, ok := b.Node("/big.bin")
		if !ok || len(n.Data) != (16<<20)+4 {
			t.Fatalf("/big.bin len = %d (ok=%v), want %d", len(n.Data), ok, (16<<20)+4)
		}
		if !bytes.Equal(n.Data[:64], prefix[:64]) || !bytes.Equal(n.Data[8<<20:(8<<20)+64], prefix[:64]) || string(n.Data[16<<20:]) != "TAIL" {
			t.Fatalf("/big.bin prefix/tail corrupted: head=%x mid=%x tail=%q",
				n.Data[:8], n.Data[8<<20:(8<<20)+8], n.Data[16<<20:])
		}
	})

	t.Run("ftruncate-after-spill-clears-stale-shadow-on-sparse-regrow", func(t *testing.T) {
		ledger, err := staging.NewStagingWithQuota(t.TempDir(), 0, 128)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(ledger.Close)
		b := testbackend.NewBackend()
		blockUpload := make(chan struct{})
		uploadEntered := make(chan struct{})
		var enterOnce, relOnce sync.Once
		releaseUpload := func() { relOnce.Do(func() { close(blockUpload) }) }
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost || r.Method == http.MethodPut {
				enterOnce.Do(func() {
					close(uploadEntered)
					select {
					case <-blockUpload:
					case <-r.Context().Done():
						return
					}
				})
			}
			b.ServeHTTP(w, r)
		}))
		t.Cleanup(ts.Close)
		cl := client.New(ts.URL, "")
		wbcache, err := staging.NewWriteBackCache(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: wbcache, Staging: ledger})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wb.Stop(context.Background()) })
		// Register gate release AFTER wb.Stop and ts.Close so LIFO cleanup
		// unblocks in-flight HTTP handlers before teardown waits on them.
		t.Cleanup(releaseUpload)
		terminal, err := core.New(cl, core.Options{})
		if err != nil {
			t.Fatal(err)
		}
		ch, err := fs.NewChain(wb, terminal)
		if err != nil {
			t.Fatal(err)
		}

		res, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/ftrunc-big",
			Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		stale := bytes.Repeat([]byte("old!"), 3<<20) // 12 MiB
		if err := res.Handle.Write(ctx, 0, stale); err != nil {
			t.Fatal(err)
		}
		if err := res.Handle.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		select {
		case <-uploadEntered:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for first upload to enter")
		}

		// ftruncate(0) via OpSetAttr while the first 12 MiB staging is still in flight!
		if _, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpSetAttr, Path: "/ftrunc-big", Handle: res.Handle, Size: 0,
		}); err != nil {
			t.Fatalf("ftruncate(0): %v", err)
		}
		releaseUpload()
		if err := res.Handle.Write(ctx, 30<<20, []byte("NEW!")); err != nil {
			t.Fatal(err)
		}
		if err := res.Handle.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		if err := res.Handle.Release(ctx); err != nil {
			t.Fatal(err)
		}
		if err := wb.Drain(ctx); err != nil {
			t.Fatal(err)
		}
		n, ok := b.Node("/ftrunc-big")
		if !ok || len(n.Data) != (30<<20)+4 {
			t.Fatalf("/ftrunc-big len = %d (ok=%v), want %d", len(n.Data), ok, (30<<20)+4)
		}
		for i, c := range n.Data[:30<<20] {
			if c != 0 {
				t.Fatalf("post-truncate hole has stale non-zero byte 0x%02x at offset %d", c, i)
			}
		}
		if string(n.Data[30<<20:]) != "NEW!" {
			t.Fatalf("tail = %q, want NEW!", string(n.Data[30<<20:]))
		}
	})

	t.Run("sparse-enospc-retry-backpressure-and-partial-spill-rollback", func(t *testing.T) {
		// 10 MiB quota: first verify partial mid-spill ENOSPC on a 12 MiB write
		// rolls back Part 0's 8 MiB from Shadow.QuotaBytes() instead of leaking.
		ledger, err := staging.NewStagingWithQuota(t.TempDir(), 0, 10)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(ledger.Close)
		b := testbackend.NewBackend()
		var blockActive atomic.Bool
		blockPut := make(chan struct{})
		putEntered := make(chan struct{})
		var putOnce, relOnce sync.Once
		releasePut := func() { relOnce.Do(func() { close(blockPut) }) }
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if blockActive.Load() && (r.Method == http.MethodPost || r.Method == http.MethodPut) {
				putOnce.Do(func() {
					close(putEntered)
					select {
					case <-blockPut:
					case <-r.Context().Done():
						return
					}
				})
			}
			b.ServeHTTP(w, r)
		}))
		t.Cleanup(ts.Close)
		cl := client.New(ts.URL, "")
		wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, Staging: ledger})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wb.Stop(context.Background()) })
		// Register gate release AFTER wb.Stop and ts.Close so LIFO cleanup
		// unblocks in-flight HTTP handlers before teardown waits on them.
		t.Cleanup(releasePut)
		terminal, err := core.New(cl, core.Options{})
		if err != nil {
			t.Fatal(err)
		}
		ch, err := fs.NewChain(wb, terminal)
		if err != nil {
			t.Fatal(err)
		}

		fail12, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/fail12.bin",
			Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY), Mode: 0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := fail12.Handle.Write(ctx, 0, bytes.Repeat([]byte("F"), 12<<20)); err != nil {
			t.Fatal(err)
		}
		if ferr := fail12.Handle.Flush(ctx); !errors.Is(ferr, syscall.ENOSPC) {
			t.Fatalf("Flush 12MiB under 10MiB quota = %v, want ENOSPC", ferr)
		}
		if got := ledger.Shadow.QuotaBytes(); got != 0 {
			t.Fatalf("partial spill leaked Shadow.QuotaBytes() = %d, want 0", got)
		}
		bhFail := writebackHandleOf(fail12.Handle)
		bhFail.mu.Lock()
		bhFail.dirty = false
		bhFail.landed = true
		bhFail.mu.Unlock()
		_ = fail12.Handle.Release(ctx)

		// Now verify a 64 MiB logical (> 10 MiB quota) sparse file with 4 KiB
		// written hits ENOSPC inside spillToShadow while /hog.bin holds 9 MiB,
		// waits for /hog.bin to drain (using clamped shadowWaitQuotaBytesLocked),
		// and succeeds!
		blockActive.Store(true)
		hog, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/hog.bin",
			Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY), Mode: 0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := hog.Handle.Write(ctx, 0, bytes.Repeat([]byte("H"), 9<<20)); err != nil {
			t.Fatal(err)
		}
		if err := hog.Handle.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		_ = hog.Handle.Release(ctx)
		select {
		case <-putEntered:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for /hog.bin upload")
		}

		sparse, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/sparse64m.bin",
			Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY), Mode: 0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := sparse.Handle.Write(ctx, 64<<20, bytes.Repeat([]byte("S"), 4096)); err != nil {
			t.Fatal(err)
		}
		bhSparse := writebackHandleOf(sparse.Handle)
		// Call stageAndEnqueueOpt when pre-wait already ran or simulate retry:
		// waitGate shadow:quotaWait fires during stageAndEnqueueOpt and unblocks
		// once /hog.bin finishes!
		quotaWaiting := make(chan struct{})
		var qwOnce sync.Once
		hookQW := func(gate string) {
			if gate == "shadow:quotaWait" {
				qwOnce.Do(func() { close(quotaWaiting) })
			}
		}
		wb.testGateHook.Store(&hookQW)
		t.Cleanup(func() { wb.testGateHook.Store(nil) })

		// Test the retry path directly by holding 9 MiB in /hog.bin while only
		// 1 MiB is free and /sparse64m.bin needs 4 KiB? Wait: 9 MiB + 4 KiB <= 10 MiB,
		// so write 2 MiB at 64 MiB (9 MiB + 2 MiB = 11 MiB > 10 MiB, logical size 66 MiB > 10 MiB)!
		if err := sparse.Handle.Write(ctx, 64<<20, bytes.Repeat([]byte("S"), 2<<20)); err != nil {
			t.Fatal(err)
		}
		_ = bhSparse
		flushDone := make(chan error, 1)
		go func() {
			flushDone <- sparse.Handle.Flush(ctx)
		}()
		select {
		case <-quotaWaiting:
		case ferr := <-flushDone:
			t.Fatalf("sparse Flush exited before waiting on quota: %v", ferr)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for shadow:quotaWait")
		}
		releasePut()
		if ferr := <-flushDone; ferr != nil {
			t.Fatalf("sparse 66MiB logical Flush after backpressure = %v, want nil", ferr)
		}
		_ = sparse.Handle.Release(ctx)
		if err := wb.Drain(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("coopen-propagation-budget-failure-preserves-acknowledged-bytes", func(t *testing.T) {
		for _, closeOrder := range []string{"writer-first", "sibling-first"} {
			t.Run(closeOrder, func(t *testing.T) {
				b := testbackend.NewBackend()
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
				// 96 KiB budget fits ONE 64 KiB buffer, forcing ENOMEM when
				// mirroring a 60 KiB write into the second co-open buffer!
				wb.SetMemoryBudget(membudget.New(96<<10, membudget.ModeEnforce, nil))
				t.Cleanup(func() { _ = wb.Stop(context.Background()) })
				terminal, err := core.New(cl, core.Options{})
				if err != nil {
					t.Fatal(err)
				}
				ch, err := fs.NewChain(wb, terminal)
				if err != nil {
					t.Fatal(err)
				}

				h1, err := ch.Dispatch(ctx, &fs.Op{
					Kind: fs.OpCreate, Path: "/budget-coopen",
					Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644, HasMode: true,
				})
				if err != nil {
					t.Fatal(err)
				}
				h2, err := ch.Dispatch(ctx, &fs.Op{
					Kind: fs.OpCreate, Path: "/budget-coopen", Flags: uint32(syscall.O_RDWR),
				})
				if err != nil {
					t.Fatal(err)
				}
				header := []byte("HDR:")
				if err := h2.Handle.Write(ctx, 0, header); err != nil {
					t.Fatal(err)
				}
				body := bytes.Repeat([]byte("M"), 60<<10)
				if err := h1.Handle.Write(ctx, int64(len(header)), body); err != nil {
					t.Fatal(err)
				}
				want := append(append([]byte(nil), header...), body...)

				if closeOrder == "writer-first" {
					if err := h1.Handle.Flush(ctx); err != nil {
						t.Fatalf("h1.Flush: %v", err)
					}
					if err := h1.Handle.Release(ctx); err != nil {
						t.Fatalf("h1.Release: %v", err)
					}
					if err := wb.Drain(ctx); err != nil {
						t.Fatal(err)
					}
					if err := h2.Handle.Flush(ctx); err != nil {
						t.Fatalf("h2.Flush: %v", err)
					}
					if err := h2.Handle.Release(ctx); err != nil {
						t.Fatalf("h2.Release: %v", err)
					}
				} else {
					if err := h2.Handle.Flush(ctx); err != nil {
						t.Fatalf("h2.Flush: %v", err)
					}
					if err := h2.Handle.Release(ctx); err != nil {
						t.Fatalf("h2.Release: %v", err)
					}
					if err := h1.Handle.Flush(ctx); err != nil {
						t.Fatalf("h1.Flush: %v", err)
					}
					if err := h1.Handle.Release(ctx); err != nil {
						t.Fatalf("h1.Release: %v", err)
					}
				}
				if err := wb.Drain(ctx); err != nil {
					t.Fatal(err)
				}
				if n, ok := b.Node("/budget-coopen"); !ok || !bytes.Equal(n.Data, want) {
					t.Fatalf("/budget-coopen (%s) len=%d ok=%v, want %d bytes with HDR: prefix", closeOrder, len(n.Data), ok, len(want))
				}
			})
		}
	})

	t.Run("reclaim-retained-shadow-fences-by-generation", func(t *testing.T) {
		ledger, err := staging.NewStagingWithQuota(t.TempDir(), 0, 16)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(ledger.Close)
		b := testbackend.NewBackend()
		ts := httptest.NewServer(b)
		t.Cleanup(ts.Close)
		cl := client.New(ts.URL, "")
		wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, Staging: ledger})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wb.Stop(context.Background()) })

		if err := ledger.Shadow.WriteFull("/fenced-ret", []byte("gen1"), 0); err != nil {
			t.Fatal(err)
		}
		gen1 := ledger.Shadow.ActiveGeneration("/fenced-ret")
		if err := ledger.Shadow.WriteFull("/fenced-ret", []byte("gen2"), 0); err != nil {
			t.Fatal(err)
		}
		gen2 := ledger.Shadow.ActiveGeneration("/fenced-ret")
		wb.queue.RetainShadowForLanding("/fenced-ret", gen2)

		// Reclaiming stale gen1 must NOT delete gen2's retention marker or shadow!
		wb.queue.ReclaimRetainedShadow("/fenced-ret", gen1)
		if got, ok := wb.queue.retainedShadows.Load("/fenced-ret"); !ok || got != gen2 {
			t.Fatalf("retainedShadows after stale ReclaimRetainedShadow = (%v, %v), want (%v, true)", got, ok, gen2)
		}
		if !ledger.Shadow.Has("/fenced-ret") {
			t.Fatal("expected gen2 shadow to survive stale ReclaimRetainedShadow")
		}
		wb.queue.ReclaimRetainedShadow("/fenced-ret", gen2)
		if ledger.Shadow.Has("/fenced-ret") {
			t.Fatal("expected gen2 shadow to be reclaimed when matching gen2 is passed")
		}
	})

	t.Run("concurrent-mark-unlinked-snapshot-no-deadlock", func(t *testing.T) {
		b := testbackend.NewBackend()
		b.Seed("/dir/a", []byte("alpha"))
		b.Seed("/dir/sub/a", []byte("alpha"))
		b.Seed("/dir/sub/b", []byte("bravo"))
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
		t.Cleanup(func() { _ = wb.Stop(context.Background()) })
		terminal, err := core.New(cl, core.Options{})
		if err != nil {
			t.Fatal(err)
		}
		ch, err := fs.NewChain(wb, terminal)
		if err != nil {
			t.Fatal(err)
		}

		for round := 0; round < 30; round++ {
			var openHandles []fs.Handle
			for i := 0; i < 3; i++ {
				ha, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/dir/sub/a", Flags: uint32(syscall.O_RDONLY)})
				if err != nil {
					t.Fatal(err)
				}
				hb, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/dir/sub/b", Flags: uint32(syscall.O_RDONLY)})
				if err != nil {
					t.Fatal(err)
				}
				openHandles = append(openHandles, ha.Handle, hb.Handle)
			}
			var wg sync.WaitGroup
			wg.Add(4)
			go func() {
				defer wg.Done()
				_, _ = wb.markUnlinkedSnapshot("/dir", true)
			}()
			go func() {
				defer wg.Done()
				_, _ = wb.markUnlinkedSnapshot("/dir/sub", true)
			}()
			go func() {
				defer wg.Done()
				_, _ = wb.markUnlinkedSnapshot("/dir/sub/a", false)
			}()
			go func() {
				defer wg.Done()
				_, _ = wb.markUnlinkedSnapshot("/dir/sub/b", false)
			}()
			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("deadlock in concurrent markUnlinkedSnapshot")
			}
			for _, oh := range openHandles {
				_ = oh.Release(ctx)
			}
		}
	})

	t.Run("close-sync-spilled-create-reopen-preserves-evicted-parts", func(t *testing.T) {
		ledger, err := staging.NewStagingWithQuota(t.TempDir(), 0, 256)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(ledger.Close)
		b := testbackend.NewBackend()
		ts := httptest.NewServer(b)
		t.Cleanup(ts.Close)
		cl := client.New(ts.URL, "")
		wb, err := New(Options{Policy: PolicyCloseSync, Client: cl, Staging: ledger})
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

		// Create /spilled-cs and write 68 MiB (> 64 MiB liveSpillThreshold) so
		// Part 0 spills to liveSpillFile while remoteSize remains 0.
		h1, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/spilled-cs",
			Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		chunk := bytes.Repeat([]byte("SpilData"), 1<<20) // 8 MiB
		for off := int64(0); off < 64<<20; off += 8 << 20 {
			if err := h1.Handle.Write(ctx, off, chunk); err != nil {
				t.Fatalf("h1.Write(%d): %v", off, err)
			}
		}
		if err := h1.Handle.Write(ctx, 64<<20, bytes.Repeat([]byte("Tail"), 1<<20)); err != nil {
			t.Fatalf("h1 tail write triggering liveSpillFile: %v", err)
		}
		bh1 := writebackHandleOf(h1.Handle)
		bh1.mu.Lock()
		spilled := bh1.liveSpillFile != nil
		canMat := bh1.buf.CanMaterializeFull()
		bh1.mu.Unlock()
		if !spilled {
			t.Fatal("expected 68 MiB close-sync write to spill parts into liveSpillFile")
		}
		if canMat {
			t.Fatal("expected CanMaterializeFull()=false while liveSpillFile holds evicted parts")
		}

		// Case 1 (> 64 MiB): reopen exercises lazy SeedRemote + LoadPart backed
		// by bh1 while Part 0 is evicted to liveSpillFile.
		h2, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/spilled-cs", Flags: uint32(syscall.O_RDWR),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := h2.Handle.Write(ctx, 16, []byte("PATCH1")); err != nil {
			t.Fatalf("h2.Write(16): %v", err)
		}
		bh1.mu.Lock()
		bh1MemAfterSnapshot := bh1.buf.MemoryUsage()
		bh1.mu.Unlock()
		if bh1MemAfterSnapshot > 16<<20 {
			t.Fatalf("bh1.buf.MemoryUsage() after sibling snapshot + patch = %d, want <= %d (evicted parts must be re-evicted)", bh1MemAfterSnapshot, 16<<20)
		}
		gotHead := make([]byte, 32)
		if n, rerr := h2.Handle.Read(ctx, 0, gotHead); rerr != nil || n != 32 {
			t.Fatalf("h2.Read(0..32) = (%d, %v)", n, rerr)
		}
		wantHead := append(append([]byte(nil), []byte("SpilDataSpilData")...), []byte("PATCH1taSpilData")...)
		if !bytes.Equal(gotHead, wantHead) {
			t.Fatalf("h2 head after patch on >64MiB spilled sibling = %q, want %q", gotHead, wantHead)
		}
		if err := h1.Handle.Release(ctx); err != nil {
			t.Fatalf("h1.Release: %v", err)
		}
		if err := h2.Handle.Flush(ctx); err != nil {
			t.Fatalf("h2.Flush: %v", err)
		}
		if err := h2.Handle.Release(ctx); err != nil {
			t.Fatalf("h2.Release: %v", err)
		}
		n1, ok := b.Node("/spilled-cs")
		if !ok || len(n1.Data) != 68<<20 {
			t.Fatalf("/spilled-cs len = %d (ok=%v), want %d", len(n1.Data), ok, 68<<20)
		}
		wantPart0 := append([]byte(nil), chunk...)
		copy(wantPart0[16:], []byte("PATCH1"))
		if !bytes.Equal(n1.Data[:8<<20], wantPart0) {
			t.Fatal("persisted >64MiB Part 0 mismatch against full expected 8MiB chunk")
		}
		for partIdx := 1; partIdx < 8; partIdx++ {
			start := partIdx * (8 << 20)
			if !bytes.Equal(n1.Data[start:start+(8<<20)], chunk) {
				t.Fatalf("persisted >64MiB evicted Part %d (@%d) corrupted", partIdx, start)
			}
		}
		wantTail := bytes.Repeat([]byte("Tail"), 1<<20)
		if !bytes.Equal(n1.Data[64<<20:], wantTail) {
			t.Fatal("persisted >64MiB tail (64..68MiB) mismatch")
		}

		// Case 2 (<= 64 MiB): spill 68 MiB on c1 so Part 0 & Part 1 evict to
		// liveSpillFile, ftruncate c1 down to 16 MiB while c1 is the sole open
		// handle, then reopen c2 (O_RDWR) to exercise ForceEnsurePartsForWrite.
		c1, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/spilled-cs-small",
			Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		for off := int64(0); off < 64<<20; off += 8 << 20 {
			if err := c1.Handle.Write(ctx, off, chunk); err != nil {
				t.Fatalf("c1.Write(%d): %v", off, err)
			}
		}
		if err := c1.Handle.Write(ctx, 64<<20, bytes.Repeat([]byte("Tail"), 1<<20)); err != nil {
			t.Fatalf("c1 tail write: %v", err)
		}
		if _, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpSetAttr, Path: "/spilled-cs-small", Handle: c1.Handle, Size: 16 << 20,
		}); err != nil {
			t.Fatalf("ftruncate(16MiB): %v", err)
		}
		c2, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/spilled-cs-small", Flags: uint32(syscall.O_RDWR),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := c2.Handle.Write(ctx, (8<<20)+8, []byte("PATCH2")); err != nil {
			t.Fatalf("c2.Write(8MiB+8): %v", err)
		}
		if err := c1.Handle.Release(ctx); err != nil {
			t.Fatalf("c1.Release: %v", err)
		}
		if err := c2.Handle.Flush(ctx); err != nil {
			t.Fatalf("c2.Flush: %v", err)
		}
		if err := c2.Handle.Release(ctx); err != nil {
			t.Fatalf("c2.Release: %v", err)
		}
		n2, ok := b.Node("/spilled-cs-small")
		if !ok || len(n2.Data) != 16<<20 {
			t.Fatalf("/spilled-cs-small len = %d (ok=%v), want %d", len(n2.Data), ok, 16<<20)
		}
		if !bytes.Equal(n2.Data[:8<<20], chunk) {
			t.Fatal("persisted <=64MiB Part 0 (0..8MiB) mismatch against full expected chunk")
		}
		wantPart1 := append([]byte(nil), chunk...)
		copy(wantPart1[8:], []byte("PATCH2"))
		if !bytes.Equal(n2.Data[8<<20:16<<20], wantPart1) {
			t.Fatal("persisted <=64MiB Part 1 (8..16MiB) mismatch against full expected chunk")
		}

		// Case 3: create 68 MiB on a1, reopen b1 (O_RDWR), patch Part 0 on b1,
		// Flush a1 while leaving a1 open, then ftruncate b1 to non-aligned
		// 36 MiB (admitFtruncate holds a1.mu while b1.buf.Truncate loads the
		// boundary part at 32..40 MiB — must never re-lock a1.mu or deadlock!).
		a1, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/spilled-cs-ftrunc",
			Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		for off := int64(0); off < 64<<20; off += 8 << 20 {
			if err := a1.Handle.Write(ctx, off, chunk); err != nil {
				t.Fatalf("a1.Write(%d): %v", off, err)
			}
		}
		if err := a1.Handle.Write(ctx, 64<<20, bytes.Repeat([]byte("Tail"), 1<<20)); err != nil {
			t.Fatalf("a1 tail write: %v", err)
		}
		b1, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/spilled-cs-ftrunc", Flags: uint32(syscall.O_RDWR),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := b1.Handle.Write(ctx, 16, []byte("PATCH3")); err != nil {
			t.Fatalf("b1.Write(16): %v", err)
		}
		if err := a1.Handle.Flush(ctx); err != nil {
			t.Fatalf("a1.Flush: %v", err)
		}
		truncDone := make(chan error, 1)
		go func() {
			_, terr := ch.Dispatch(ctx, &fs.Op{
				Kind: fs.OpSetAttr, Path: "/spilled-cs-ftrunc", Handle: b1.Handle, Size: 36 << 20,
			})
			truncDone <- terr
		}()
		select {
		case terr := <-truncDone:
			if terr != nil {
				t.Fatalf("ftruncate(36MiB) on reopened spilled handle with clean sibling open: %v", terr)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("deadlock in ftruncate(36MiB) while clean sibling handle remained open")
		}
		if err := b1.Handle.Flush(ctx); err != nil {
			t.Fatalf("b1.Flush: %v", err)
		}
		if err := b1.Handle.Release(ctx); err != nil {
			t.Fatalf("b1.Release: %v", err)
		}
		if err := a1.Handle.Release(ctx); err != nil {
			t.Fatalf("a1.Release: %v", err)
		}
		n3, ok := b.Node("/spilled-cs-ftrunc")
		if !ok || len(n3.Data) != 36<<20 {
			t.Fatalf("/spilled-cs-ftrunc len = %d (ok=%v), want %d", len(n3.Data), ok, 36<<20)
		}
		wantP3 := append([]byte(nil), chunk...)
		copy(wantP3[16:], []byte("PATCH3"))
		if !bytes.Equal(n3.Data[:8<<20], wantP3) {
			t.Fatal("persisted 36MiB Part 0 mismatch")
		}
		if !bytes.Equal(n3.Data[32<<20:36<<20], chunk[:4<<20]) {
			t.Fatal("persisted 36MiB boundary tail (32..36MiB) mismatch")
		}
	})

	t.Run("close-sync-unlanded-reader-survives-unlink-before-flush", func(t *testing.T) {
		for _, withStaging := range []bool{false, true} {
			name := "no-staging"
			if withStaging {
				name = "with-staging"
			}
			t.Run(name, func(t *testing.T) {
				b := testbackend.NewBackend()
				ts := httptest.NewServer(b)
				t.Cleanup(ts.Close)
				cl := client.New(ts.URL, "")
				opts := Options{Policy: PolicyCloseSync, Client: cl}
				if withStaging {
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
				t.Cleanup(func() { _ = wb.Stop(context.Background()) })
				terminal, err := core.New(cl, core.Options{})
				if err != nil {
					t.Fatal(err)
				}
				ch, err := fs.NewChain(wb, terminal)
				if err != nil {
					t.Fatal(err)
				}

				w1, err := ch.Dispatch(ctx, &fs.Op{
					Kind: fs.OpCreate, Path: "/cs-unlink",
					Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY), Mode: 0o644, HasMode: true,
				})
				if err != nil {
					t.Fatal(err)
				}
				const payload = "unlanded-close-sync-payload"
				if err := w1.Handle.Write(ctx, 0, []byte(payload)); err != nil {
					t.Fatal(err)
				}
				r1, err := ch.Dispatch(ctx, &fs.Op{
					Kind: fs.OpCreate, Path: "/cs-unlink", Flags: uint32(syscall.O_RDONLY),
				})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/cs-unlink"}); err != nil {
					t.Fatalf("OpUnlink: %v", err)
				}

				buf := make([]byte, 64)
				n, err := r1.Handle.Read(ctx, 0, buf)
				if err != nil || string(buf[:n]) != payload {
					t.Fatalf("post-unlink Read while writer open = (%q, %v), want %q", string(buf[:n]), err, payload)
				}
				if err := w1.Handle.Release(ctx); err != nil {
					t.Fatalf("w1.Release: %v", err)
				}
				n, err = r1.Handle.Read(ctx, 0, buf)
				if err != nil || string(buf[:n]) != payload {
					t.Fatalf("post-unlink Read after writer closed = (%q, %v), want %q", string(buf[:n]), err, payload)
				}
				if err := r1.Handle.Release(ctx); err != nil {
					t.Fatalf("r1.Release: %v", err)
				}
				if _, ok := b.Node("/cs-unlink"); ok {
					t.Fatal("unlinked close-sync file unexpectedly landed on remote")
				}
			})
		}
	})

	t.Run("delegated-dirty-survives-non-sync-flush-for-later-fsync", func(t *testing.T) {
		var rejectPut atomic.Bool
		b := testbackend.NewBackend()
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if rejectPut.Load() && r.Method == http.MethodPut {
				w.WriteHeader(http.StatusServiceUnavailable)
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
		wb.SetMemoryBudget(membudget.New(96<<10, membudget.ModeEnforce, nil))
		t.Cleanup(func() { _ = wb.Stop(context.Background()) })
		terminal, err := core.New(cl, core.Options{})
		if err != nil {
			t.Fatal(err)
		}
		ch, err := fs.NewChain(wb, terminal)
		if err != nil {
			t.Fatal(err)
		}

		h1, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/delegated-sync",
			Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		h2, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/delegated-sync", Flags: uint32(syscall.O_RDWR),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := h2.Handle.Write(ctx, 0, []byte("HDR:")); err != nil {
			t.Fatal(err)
		}
		if err := h1.Handle.Write(ctx, 4, bytes.Repeat([]byte("D"), 60<<10)); err != nil {
			t.Fatal(err)
		}
		// Non-sync Flush on h2 stages h1's image into wbCache without waiting
		// for remote upload; while PUTs are rejected, h2.Sync MUST still observe
		// delegatedDirty and fail instead of returning nil prematurely!
		rejectPut.Store(true)
		if err := h2.Handle.Flush(ctx); err != nil {
			t.Fatalf("h2.Flush: %v", err)
		}
		if serr := h2.Handle.Sync(ctx); serr == nil {
			t.Fatal("expected h2.Sync to wait/fail while delegated sibling staging has not landed")
		}
		rejectPut.Store(false)
		if serr := h2.Handle.Sync(ctx); serr != nil {
			t.Fatalf("h2.Sync after remote recovered: %v", serr)
		}
		wantDelegated := append(append([]byte(nil), []byte("HDR:")...), bytes.Repeat([]byte("D"), 60<<10)...)
		if n, ok := b.Node("/delegated-sync"); !ok || !bytes.Equal(n.Data, wantDelegated) {
			t.Fatalf("/delegated-sync before Release = (ok=%v, len=%d), want %d bytes published by Sync", ok, len(n.Data), len(wantDelegated))
		}
		// Also verify two mutually delegated handles do not recurse on Sync.
		if err := h1.Handle.Write(ctx, 0, []byte("NEW:")); err != nil {
			t.Fatal(err)
		}
		if err := h1.Handle.Flush(ctx); err != nil {
			t.Fatalf("h1.Flush: %v", err)
		}
		bh1 := writebackHandleOf(h1.Handle)
		bh2 := writebackHandleOf(h2.Handle)
		bh1.mu.Lock()
		bh1.delegatedDirty = true
		bh1.mu.Unlock()
		bh2.mu.Lock()
		bh2.delegatedDirty = true
		bh2.mu.Unlock()
		if serr := h2.Handle.Sync(ctx); serr != nil {
			t.Fatalf("mutual delegatedDirty Sync: %v", serr)
		}
		_ = h2.Handle.Release(ctx)
		_ = h1.Handle.Release(ctx)
	})

	t.Run("append-evicted-part-never-overwrites-foreign-append", func(t *testing.T) {
		b := testbackend.NewBackend()
		ts := httptest.NewServer(b)
		t.Cleanup(ts.Close)
		cl := client.New(ts.URL, "")
		wbcache, err := staging.NewWriteBackCache(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		wb, err := New(Options{Policy: PolicyCloseSync, Client: cl, WriteCache: wbcache})
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

		// Case A (<= 1 MiB with evicted uploadedParts[0]): CanMaterializeFull()
		// is false until fenceRefreshAppendBase restores Part 0; a diverged
		// foreign write must fail with EAGAIN instead of being overwritten.
		if err := cl.WriteCtx(ctx, "/app-evicted.log", []byte("orig-base")); err != nil {
			t.Fatal(err)
		}
		apSmall, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/app-evicted.log",
			Flags: uint32(syscall.O_WRONLY | syscall.O_APPEND),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := apSmall.Handle.Write(ctx, 9, []byte(":local")); err != nil {
			t.Fatal(err)
		}
		bhSmall := writebackHandleOf(apSmall.Handle)
		bhSmall.mu.Lock()
		savedSmall := append([]byte(nil), bhSmall.buf.Bytes()...)
		bhSmall.buf.RestorePart = func(partNum int) ([]byte, error) {
			return append([]byte(nil), savedSmall...), nil
		}
		bhSmall.buf.EvictPart(0)
		if bhSmall.buf.CanMaterializeFull() {
			bhSmall.mu.Unlock()
			t.Fatal("expected CanMaterializeFull()=false after EvictPart(0)")
		}
		bhSmall.mu.Unlock()
		if err := cl.WriteCtx(ctx, "/app-evicted.log", []byte("diverged-foreign-append")); err != nil {
			t.Fatal(err)
		}
		if ferr := apSmall.Handle.Flush(ctx); !errors.Is(ferr, syscall.EAGAIN) {
			t.Fatalf("Flush over diverged foreign write with evicted part = %v, want EAGAIN", ferr)
		}
		if n, ok := b.Node("/app-evicted.log"); !ok || string(n.Data) != "diverged-foreign-append" {
			t.Fatalf("/app-evicted.log = (%v, %q), want diverged-foreign-append", ok, n.Data)
		}
		bhSmall.mu.Lock()
		bhSmall.dirty = false
		bhSmall.mu.Unlock()
		_ = apSmall.Handle.Release(ctx)

		// Case B (> 1 MiB with evicted uploadedParts[0]): Size() exceeds
		// appendRefreshMaxBytes so fenceCheckUnmaterializableAppend runs,
		// detects the advanced remote revision, and returns EAGAIN.
		base2m := bytes.Repeat([]byte("A"), 2<<20)
		if err := cl.WriteCtx(ctx, "/app-foreign.log", base2m); err != nil {
			t.Fatal(err)
		}
		ap, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/app-foreign.log",
			Flags: uint32(syscall.O_WRONLY | syscall.O_APPEND),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := ap.Handle.Write(ctx, 2<<20, []byte(":local-tail")); err != nil {
			t.Fatal(err)
		}
		bh := writebackHandleOf(ap.Handle)
		bh.mu.Lock()
		bh.buf.EvictPart(0)
		if bh.buf.CanMaterializeFull() {
			bh.mu.Unlock()
			t.Fatal("expected >1MiB CanMaterializeFull()=false after EvictPart(0)")
		}
		bh.mu.Unlock()
		foreign := append(append([]byte(nil), base2m...), []byte(":FOREIGN-APPEND")...)
		if err := cl.WriteCtx(ctx, "/app-foreign.log", foreign); err != nil {
			t.Fatal(err)
		}

		ferr := ap.Handle.Flush(ctx)
		if !errors.Is(ferr, syscall.EAGAIN) {
			t.Fatalf("Flush over foreign append with unmaterializable >1MiB buffer = %v, want EAGAIN", ferr)
		}
		if n, ok := b.Node("/app-foreign.log"); !ok || !bytes.Equal(n.Data, foreign) {
			t.Fatalf("foreign append was overwritten: len=%d", len(n.Data))
		}
		bh.mu.Lock()
		bh.dirty = false
		bh.mu.Unlock()
		_ = ap.Handle.Release(ctx)
	})

	t.Run("landed-reopen-exceeding-budget-seeds-logical-size", func(t *testing.T) {
		b := testbackend.NewBackend()
		ts := httptest.NewServer(b)
		t.Cleanup(ts.Close)
		cl := client.New(ts.URL, "")
		wb, err := New(Options{Policy: PolicyCloseSync, Client: cl})
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

		// Create /lazy-landed, write 128 KiB, reopen h2 untouched while unlanded,
		// close creator h1 so 128 KiB lands remotely, then tighten memory budget
		// to 96 KiB so h2's lazy buffer initialization cannot preload 128 KiB in
		// full and takes armRemoteRestoreLocked + SeedRemote(128 KiB).
		h1, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/lazy-landed",
			Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644, HasMode: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		orig128k := bytes.Repeat([]byte("Landed16Byte Blk"), 8<<10) // 128 KiB
		if err := h1.Handle.Write(ctx, 0, orig128k); err != nil {
			t.Fatal(err)
		}
		h2, err := ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpCreate, Path: "/lazy-landed", Flags: uint32(syscall.O_RDWR),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := h1.Handle.Flush(ctx); err != nil {
			t.Fatalf("h1.Flush: %v", err)
		}
		if err := h1.Handle.Release(ctx); err != nil {
			t.Fatalf("h1.Release: %v", err)
		}

		// Force h2 onto the lazy remote-restore branch by capping preload to 64 KiB.
		wb.preload = 64 << 10
		if err := h2.Handle.Write(ctx, 0, []byte("HEAD")); err != nil {
			t.Fatalf("h2.Write(0, HEAD): %v", err)
		}
		if err := h2.Handle.Flush(ctx); err != nil {
			t.Fatalf("h2.Flush: %v", err)
		}
		if err := h2.Handle.Release(ctx); err != nil {
			t.Fatalf("h2.Release: %v", err)
		}
		want := append([]byte("HEAD"), orig128k[4:]...)
		if n, ok := b.Node("/lazy-landed"); !ok || !bytes.Equal(n.Data, want) {
			t.Fatalf("/lazy-landed after lazy-seeded 4B patch: ok=%v len=%d, want %d bytes", ok, len(n.Data), len(want))
		}
	})
}
