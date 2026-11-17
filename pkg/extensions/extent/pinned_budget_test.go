package extent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"
)

func shaHex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func TestPinnedBudgetAccounting(t *testing.T) {
	r := newBlockRegistry()
	a := shaHex([]byte("block-a"))
	b := shaHex([]byte("block-b"))

	r.stagePinned(a, make([]byte, 600), true)
	if got := r.pinnedBudget(); got != 600 {
		t.Fatalf("pinned budget after first stage = %d, want 600", got)
	}
	// A second reference to the same checksum must not double-account.
	r.stagePinned(a, make([]byte, 600), true)
	if got := r.pinnedBudget(); got != 600 {
		t.Fatalf("pinned budget after re-stage = %d, want 600", got)
	}
	r.stagePinned(b, make([]byte, 400), true)
	if got := r.pinnedBudget(); got != 1000 {
		t.Fatalf("pinned budget = %d, want 1000", got)
	}

	// Drop one of the two references on a: budget unchanged.
	r.unpin(a)
	if got := r.pinnedBudget(); got != 1000 {
		t.Fatalf("pinned budget after first unpin = %d, want 1000", got)
	}
	// Last reference: budget retires exactly the staged share.
	r.unpin(a)
	if got := r.pinnedBudget(); got != 400 {
		t.Fatalf("pinned budget after last unpin = %d, want 400", got)
	}
	// Extra unpin must not drive the budget negative.
	r.unpin(a)
	if got := r.pinnedBudget(); got != 400 {
		t.Fatalf("pinned budget after extra unpin = %d, want 400", got)
	}
}

func TestWaitPinnedBudgetBlocksUntilRelease(t *testing.T) {
	r := newBlockRegistry()
	a := shaHex([]byte("budget"))
	r.stagePinned(a, make([]byte, 1024), true)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.waitPinnedBudget(ctx, 1024) }()

	select {
	case err := <-done:
		t.Fatalf("waitPinnedBudget returned %v while budget full", err)
	case <-time.After(100 * time.Millisecond):
	}

	r.unpin(a) // last reference: budget drains and the waiter wakes
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waitPinnedBudget error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waitPinnedBudget did not wake after unpin")
	}
}

func TestWaitPinnedBudgetRespectsContext(t *testing.T) {
	r := newBlockRegistry()
	a := shaHex([]byte("cancel"))
	r.stagePinned(a, make([]byte, 1024), true)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := r.waitPinnedBudget(ctx, 1024); err == nil {
		t.Fatal("waitPinnedBudget with full budget and expiring context must error")
	}
}

func TestWaitTotalStagingBudgetTracksStagedMemAndWakes(t *testing.T) {
	r := newBlockRegistry()
	a := shaHex([]byte("pinned-part"))
	r.stagePinned(a, make([]byte, 600), true)
	r.addStagedMem(500)
	if got := r.totalStagingBudget(); got != 1100 {
		t.Fatalf("totalStagingBudget = %d, want 1100", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.waitTotalStagingBudget(ctx, 1024) }()

	select {
	case err := <-done:
		t.Fatalf("waitTotalStagingBudget returned early while combined bytes 1100 >= 1024: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	r.addStagedMem(-500)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waitTotalStagingBudget error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waitTotalStagingBudget did not wake after stagedMem release")
	}
}

func TestWaitSliceAdmissionOrderedAndBounded(t *testing.T) {
	r := newBlockRegistry()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Admit slice 1 reserving 600 of a 1000-byte limit.
	if err := r.waitSliceAdmission(ctx, 1, 600, 1000); err != nil {
		t.Fatalf("admit slice 1: %v", err)
	}
	// Admit slice 2 reserving 400 -> total reaches 1000 (limit).
	if err := r.waitSliceAdmission(ctx, 2, 400, 1000); err != nil {
		t.Fatalf("admit slice 2: %v", err)
	}
	if got := r.totalStagingBudget(); got != 1000 {
		t.Fatalf("totalStagingBudget = %d, want 1000", got)
	}
	// Another block of already-admitted slice 2 bypasses admission wait.
	if err := r.waitSliceAdmission(ctx, 2, 400, 1000); err != nil {
		t.Fatalf("re-admit slice 2: %v", err)
	}

	// Enqueue slice 10 then slice 5; slice 5 must be admitted before slice 10.
	admitted := make(chan uint64, 2)
	go func() {
		if err := r.waitSliceAdmission(ctx, 10, 400, 1000); err == nil {
			admitted <- 10
		}
	}()
	// Wait until slice 10 is parked in waitingSlices before queueing slice 5.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		n := r.waitingSlices[10]
		r.mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	go func() {
		if err := r.waitSliceAdmission(ctx, 5, 400, 1000); err == nil {
			admitted <- 5
		}
	}()
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		n := r.waitingSlices[5]
		r.mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Retire slice 1 (600) -> only one waiter fits cleanly under 1000 after slice 5 takes 400.
	// Note 400 (slice 2) + 400 (slice 5) = 800 < 1000, so wait: release slice 2 (400 -> 600)
	// after retiring slice 2 first -> total = 600, wait for 5 first!
	r.retireSliceMem(2) // 1000 -> 600 < 1000, enough for ONE 400 slice (5, NOT 10)!
	select {
	case id := <-admitted:
		if id != 5 {
			t.Fatalf("admitted slice %d first, want smaller ID 5", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for slice 5 admission")
	}

	// Now 600 + 400 = 1000; slice 10 must still be waiting until slice 1 retires.
	select {
	case id := <-admitted:
		t.Fatalf("unexpectedly admitted %d while budget full", id)
	case <-time.After(80 * time.Millisecond):
	}

	r.retireSliceMem(1)
	select {
	case id := <-admitted:
		if id != 10 {
			t.Fatalf("admitted slice %d, want 10", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for slice 10 admission")
	}
}
