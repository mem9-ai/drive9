package extent

import (
	"context"
	"path"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// TestSentinelHiddenFromOpListButUserFilePreserved verifies that internal
// .d9open-* silly-rename sentinels registered by KeepDeletedOpen are filtered
// out of OpList (Readdir), while ordinary user files whose names happen to
// contain ".d9open-" remain visible.
func TestSentinelHiddenFromOpListButUserFilePreserved(t *testing.T) {
	ctx := context.Background()
	f := newFakeV2(t)
	f.seedExtent(t, "/dir/open.bin", "payload")
	f.seedExtent(t, "/dir/.user.d9open-keep", "user-data")
	h := newTestHandler(t, f, 1<<20)

	eh := claimOpen(t, h, "/dir/open.bin")
	defer func() { _ = eh.Release(ctx) }()

	if _, err := h.Do(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/dir/open.bin"}, func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		t.Fatal("open-file unlink should be silly-renamed locally without forwarding DELETE")
		return fs.Result{}, nil
	}); err != nil {
		t.Fatalf("OpUnlink open extent file: %v", err)
	}

	sentinels := hiddenNames(f)
	var realSentinelBase string
	for _, s := range sentinels {
		base := path.Base(s)
		if base != ".user.d9open-keep" {
			realSentinelBase = base
		}
	}
	if realSentinelBase == "" {
		t.Fatalf("expected KeepDeletedOpen sentinel on fake server, got %v", sentinels)
	}

	res, err := h.Do(ctx, &fs.Op{Kind: fs.OpList, Path: "/dir"}, func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		return fs.Result{
			Entries: []fs.Entry{
				{Name: realSentinelBase, Attr: fs.Attr{Kind: fs.KindFile, Size: 7}},
				{Name: ".user.d9open-keep", Attr: fs.Attr{Kind: fs.KindFile, Size: 9}},
				{Name: "normal.txt", Attr: fs.Attr{Kind: fs.KindFile, Size: 4}},
			},
		}, nil
	})
	if err != nil {
		t.Fatalf("OpList: %v", err)
	}
	var names []string
	for _, e := range res.Entries {
		names = append(names, e.Name)
	}
	if len(names) != 2 || names[0] != ".user.d9open-keep" || names[1] != "normal.txt" {
		t.Fatalf("OpList entries = %v, want [.user.d9open-keep normal.txt] (sentinel %q hidden)", names, realSentinelBase)
	}
}

// TestRmdirWaitsForFlushedClosingSentinel verifies the close-sync / FUSE_FLUSH
// -> async FUSE_RELEASE race (Node test-fs-readfile.js tmpdir cleanup): when
// an open extent file has already completed Flush() (close(2) returned) and
// is unlinked just before async Release() arrives, OpRmdir waits for
// DropDeletedOpen to delete the .d9open-* sentinel from the server instead of
// failing with ENOTEMPTY.
func TestRmdirWaitsForFlushedClosingSentinel(t *testing.T) {
	ctx := context.Background()
	f := newFakeV2(t)
	f.seedExtent(t, "/tmpdir/1M.txt", strings.Repeat("x", 1024))
	h := newTestHandler(t, f, 1<<20)

	eh := claimOpen(t, h, "/tmpdir/1M.txt")
	if err := eh.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Unlink arrives right after close(2) returned, before kernel FUSE_RELEASE.
	if _, err := h.Do(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/tmpdir/1M.txt"}, func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		return fs.Result{}, nil
	}); err != nil {
		t.Fatalf("OpUnlink: %v", err)
	}
	if got := hiddenNames(f); len(got) != 1 {
		t.Fatalf("expected 1 active sentinel before async Release, got %v", got)
	}

	// Use a generous deterministic grace window while testing the Rmdir ->
	// async Release handoff so CI scheduler delays cannot expire eligibility
	// before waitClosingSentinelsInDir runs; default 25ms aging is verified
	// separately below.
	h.flushGrace = 5 * time.Second
	waitEntered := make(chan struct{})
	var waitOnce sync.Once
	h.onSentinelWait = func() {
		waitOnce.Do(func() { close(waitEntered) })
	}

	releaseDone := make(chan struct{})
	go func() {
		defer close(releaseDone)
		<-waitEntered
		_ = eh.Release(ctx)
	}()
	t.Cleanup(func() {
		waitOnce.Do(func() { close(waitEntered) })
		<-releaseDone
	})

	if _, err := h.Do(ctx, &fs.Op{Kind: fs.OpRmdir, Path: "/tmpdir"}, func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		if rem := hiddenNames(f); len(rem) > 0 {
			return fs.Result{}, syscall.ENOTEMPTY
		}
		return fs.Result{}, nil
	}); err != nil {
		t.Fatalf("OpRmdir raced closing sentinel: %v (want nil)", err)
	}
	<-releaseDone
	if rem := hiddenNames(f); len(rem) != 0 {
		t.Fatalf("leftover sentinels after Release+Rmdir = %v, want empty", rem)
	}

	// Also verify a duplicated descriptor that flushed >25ms ago without
	// starting Release does NOT stall OpRmdir for 2 seconds (restore default
	// 25ms dispatch grace first).
	h.flushGrace = 0
	h.onSentinelWait = nil
	f.seedExtent(t, "/dupdir/keep.txt", "still-open")
	ehDup := claimOpen(t, h, "/dupdir/keep.txt")
	defer func() { _ = ehDup.Release(ctx) }()
	if err := ehDup.Flush(ctx); err != nil {
		t.Fatalf("Flush dup: %v", err)
	}
	if _, err := h.Do(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/dupdir/keep.txt"}, func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		return fs.Result{}, nil
	}); err != nil {
		t.Fatalf("OpUnlink dup: %v", err)
	}
	// Backdate flush past the 25ms kernel-dispatch grace window.
	ehDup.flushedAtNano.Store(time.Now().Add(-100 * time.Millisecond).UnixNano())
	start := time.Now()
	_, err := h.Do(ctx, &fs.Op{Kind: fs.OpRmdir, Path: "/dupdir"}, func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		return fs.Result{}, syscall.ENOTEMPTY
	})
	if err != syscall.ENOTEMPTY {
		t.Fatalf("OpRmdir with still-open dup fd = %v, want ENOTEMPTY", err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("OpRmdir with non-releasing flushed handle stalled for %v", elapsed)
	}
}
