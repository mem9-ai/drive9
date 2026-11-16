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
