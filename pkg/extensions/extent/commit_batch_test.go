package extent

import (
	"context"
	"crypto/rand"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
)

// pendingBytesFor sums a file's pending slice volume (test helper).
func pendingBytesFor(b *Bridge, path string) int64 {
	ino, ok := b.LocalResolve(path)
	if !ok {
		return -1
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	var n int64
	for _, w := range b.pending[ino] {
		n += w.size
	}
	return n
}

// waitUntil polls cond until it holds or the deadline passes.
func waitUntil(t *testing.T, d time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestWriteBatchesCommitsUntilThreshold pins Bridge.Write's batched
// publication: slices below the batch threshold stay pending (nothing
// reaches the server), crossing it publishes the accumulated batch, and
// flush publishes whatever remains. Close-to-open is unchanged — mid-write
// publication was always best-effort.
func TestWriteBatchesCommitsUntilThreshold(t *testing.T) {
	f := newFakeV2(t)
	h := newTestHandler(t, f, 0) // full mode
	restore := tTempSet(&commitBatchBytes, 8<<20)
	defer restore()
	ctx := context.Background()
	br := h.opts.Engine.Bridge

	res, err, passed := dispatchCreate(ctx, h, "/batch.bin", syscall.O_RDWR|syscall.O_CREAT)
	if err != nil || passed {
		t.Fatalf("create: err=%v passed=%v", err, passed)
	}
	eh, ok := res.Handle.(*engineHandle)
	if !ok {
		t.Fatalf("handle = %T", res.Handle)
	}

	first := mkBatch(t, 6<<20) // under the 8 MiB threshold
	if err := eh.Write(ctx, 0, first); err != nil {
		t.Fatalf("write 1: %v", err)
	}
	waitUntil(t, 10*time.Second, func() bool { return pendingBytesFor(br, "/batch.bin") >= 6<<20 }, "engine to hand the first slices to the bridge")
	f.mu.Lock()
	creates := len(f.creates)
	f.mu.Unlock()
	if creates != 0 {
		t.Fatalf("under-threshold writes published: creates=%d, want 0", creates)
	}

	second := mkBatch(t, 4<<20) // crosses the threshold
	if err := eh.Write(ctx, int64(len(first)), second); err != nil {
		t.Fatalf("write 2: %v", err)
	}
	waitUntil(t, 10*time.Second, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.creates) == 1
	}, "the threshold-crossing batch to publish")

	if err := eh.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	f.mu.Lock()
	got := f.extent["/batch.bin"]
	f.mu.Unlock()
	want := append(append([]byte(nil), first...), second...)
	if string(got) != string(want) {
		t.Fatalf("final image = %d bytes, want %d (flush must publish the remainder)", len(got), len(want))
	}
}

// TestWriteCommitErrorBypassesBatching verifies a remembered commit failure
// keeps every write driving recovery instead of waiting out the batch
// threshold.
func TestWriteCommitErrorBypassesBatching(t *testing.T) {
	f := newFakeV2(t)
	h := newTestHandler(t, f, 0)
	restore := tTempSet(&commitBatchBytes, 8<<20)
	defer restore()
	ctx := context.Background()
	br := h.opts.Engine.Bridge

	res, _, _ := dispatchCreate(ctx, h, "/batch.bin", syscall.O_RDWR|syscall.O_CREAT)
	eh := res.Handle.(*engineHandle)
	// Claimed; now make the threshold-crossing publication's stat fail.
	f.mu.Lock()
	f.statFail["/batch.bin"] = true
	f.mu.Unlock()

	big := mkBatch(t, 9<<20) // crosses the threshold; the commit's stat fails
	if err := eh.Write(ctx, 0, big); err != nil {
		t.Fatalf("write 1: %v", err)
	}
	waitUntil(t, 10*time.Second, func() bool {
		ino, ok := br.LocalResolve("/batch.bin")
		if !ok {
			return false
		}
		br.mu.Lock()
		defer br.mu.Unlock()
		return br.commitErr[ino] != nil
	}, "the failed commit to be remembered")

	// A small second write — far below the threshold — must retry
	// publication immediately.
	f.mu.Lock()
	delete(f.statFail, "/batch.bin")
	f.mu.Unlock()
	small := mkBatch(t, 1<<20)
	if err := eh.Write(ctx, int64(len(big)), small); err != nil {
		t.Fatalf("write 2: %v", err)
	}
	waitUntil(t, 10*time.Second, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.creates) == 1
	}, "the bypassing retry to publish")
	if err := eh.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	f.mu.Lock()
	got := f.extent["/batch.bin"]
	f.mu.Unlock()
	if int64(len(got)) != int64(len(big))+int64(len(small)) {
		t.Fatalf("recovered image = %d bytes, want %d", len(got), len(big)+len(small))
	}
}

func mkBatch(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

// tTempSet swaps a package int64 for the test's duration.
func tTempSet(p *int64, v int64) func() {
	old := *p
	*p = v
	return func() { *p = old }
}

var _ = meta.Ino(0) // keep the import shape stable for helper evolution
