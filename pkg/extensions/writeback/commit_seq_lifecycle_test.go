package writeback

// The unconditional commit-stamp table's lifecycle (#1083): every successful
// commit stamps its pathname, so create→upload→delete churn on uniquely
// named files must not retain every name for the mount's lifetime. The table
// trims once no rename window holds the stamps; pins are per-window (an
// ended window never keeps another one's obligations), and ending the last
// window restores the bound without waiting for future commits.

import (
	"fmt"
	"testing"

	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// TestCommitSeqLifecycleBounded asserts boundedness after obligations end,
// pin survival while a window is open, idle restoration of the bound at
// window close, and progress under overlapping windows.
func TestCommitSeqLifecycleBounded(t *testing.T) {
	pending, err := staging.NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cq := NewCommitQueue(nil, nil, pending, nil, 1, 8)
	t.Cleanup(cq.DrainAll)

	limit := cq.commitSeqLimitLocked()
	churn := func(round, n int) {
		for i := 0; i < n; i++ {
			cq.recordCommitSeq(fmt.Sprintf("/tmp/churn-%d-%d.txt", round, i))
		}
	}
	stampCount := func() int {
		cq.mu.Lock()
		defer cq.mu.Unlock()
		return len(cq.commitSeq)
	}

	// Obligation-free churn: the table stays at the bound, and the oldest
	// stamps are reclaimed first.
	churn(0, 3*limit)
	if n := stampCount(); n > limit {
		t.Fatalf("commitSeq holds %d stamps after obligation-free churn, want <= %d", n, limit)
	}
	if got := cq.CommitSeq("/tmp/churn-0-0.txt"); got != 0 {
		t.Fatalf("the oldest stamp survived the trim: %d", got)
	}

	// An open window pins what its flow may still compare: the renamed
	// endpoints and the paths under its prefixes.
	cq.recordCommitSeq("/keep/old.txt")
	cq.recordCommitSeq("/keep/dst.txt")
	wantOld := cq.CommitSeq("/keep/old.txt")
	preLanding := cq.CommitSeq("/keep/dst.txt")
	win := cq.BeginCommitSeqWindow("/keep/old.txt", "/keep/dst.txt")
	// A landing DURING the window inside its universe — the delta case the
	// fences exist for.
	cq.recordCommitSeq("/keep/dst.txt")
	wantLanding := cq.CommitSeq("/keep/dst.txt")
	if wantLanding <= preLanding {
		t.Fatalf("the in-window landing did not advance the stamp: %d <= %d", wantLanding, preLanding)
	}
	// Churn OUTSIDE the window's universe is not this window's obligation:
	// it stays reclaimable while the window is open.
	churn(1, 3*limit)
	if n := stampCount(); n > limit {
		t.Fatalf("commitSeq holds %d stamps while a narrow window is open, want <= %d", n, limit)
	}
	if got := cq.CommitSeq("/keep/old.txt"); got != wantOld {
		t.Fatalf("pinned stamp /keep/old.txt = %d, want %d", got, wantOld)
	}
	if got := cq.CommitSeq("/keep/dst.txt"); got != wantLanding {
		t.Fatalf("pinned in-window landing stamp = %d, want %d", got, wantLanding)
	}

	// Ending the window restores the bound IMMEDIATELY — an idle mount must
	// not retain the window's growth until some unrelated future commit.
	win.End()
	if n := stampCount(); n > limit {
		t.Fatalf("commitSeq holds %d stamps right after the window ended, want <= %d without further churn", n, limit)
	}

	// Overlapping windows keep progress: each window's close drops exactly
	// ITS pins, so a finished rename never waits for the last sibling.
	a := cq.BeginCommitSeqWindow("/ov/a", "/ov/a-dst")
	b := cq.BeginCommitSeqWindow("/ov/b", "/ov/b-dst")
	cq.recordCommitSeq("/ov/a/x.txt")
	cq.recordCommitSeq("/ov/b/x.txt")
	wantB := cq.CommitSeq("/ov/b/x.txt")
	a.End()
	// A's obligations are gone; B's survive A's close and any churn.
	churn(2, 3*limit)
	if got := cq.CommitSeq("/ov/b/x.txt"); got != wantB {
		t.Fatalf("window B's pin did not survive window A's close: %d, want %d", got, wantB)
	}
	if got := cq.CommitSeq("/ov/a/x.txt"); got != 0 {
		t.Fatalf("window A's stamp survived its own end: %d", got)
	}
	b.End()
	churn(3, 3*limit)
	if got := cq.CommitSeq("/ov/b/x.txt"); got != 0 {
		t.Fatalf("window B's stamp survived its own end: %d", got)
	}
	if n := stampCount(); n > limit {
		t.Fatalf("commitSeq holds %d stamps after every window ended, want <= %d", n, limit)
	}
}
