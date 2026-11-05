package writeback

// The unconditional commit-stamp table's lifecycle (#1083): every successful
// commit stamps its pathname, so create→upload→delete churn on uniquely
// named files must not retain every name for the mount's lifetime. The table
// trims once no rename obligation holds the stamps, while an open rename
// window keeps every stamp it may still compare pinned — its snapshot paths,
// its in-window landings, and the renamed names themselves.

import (
	"fmt"
	"testing"

	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// TestCommitSeqLifecycleBounded asserts the boundedness after obligations
// end and the pinning while a rename window is open.
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

	// An open rename window pins what its flow may still compare: the
	// explicitly pinned endpoints and an in-window landing. Anything stamped
	// while the window is open is a potential obligation, so the table may
	// grow past the bound TRANSIENTLY — a window spans one rename flow, and
	// the pins die with it.
	cq.recordCommitSeq("/keep/old.txt")
	cq.recordCommitSeq("/keep/dst.txt")
	wantOld := cq.CommitSeq("/keep/old.txt")
	preLanding := cq.CommitSeq("/keep/dst.txt")
	cq.BeginCommitSeqWindow()
	defer cq.EndCommitSeqWindow()
	cq.PinCommitSeq("/keep/old.txt", "/keep/dst.txt")
	_ = cq.LandedSnapshotsForRename("/keep/dst.txt", "/keep/dst/")
	// A landing DURING the window — the delta case the fences exist for.
	cq.recordCommitSeq("/keep/dst.txt")
	wantLanding := cq.CommitSeq("/keep/dst.txt")
	if wantLanding <= preLanding {
		t.Fatalf("the in-window landing did not advance the stamp: %d <= %d", wantLanding, preLanding)
	}
	churn(1, 3*limit)
	if got := cq.CommitSeq("/keep/old.txt"); got != wantOld {
		t.Fatalf("pinned stamp /keep/old.txt = %d, want %d", got, wantOld)
	}
	if got := cq.CommitSeq("/keep/dst.txt"); got != wantLanding {
		t.Fatalf("pinned in-window landing stamp = %d, want %d", got, wantLanding)
	}

	// The window closes: the pins die with it, and later churn reclaims the
	// former witnesses like any other expired stamp.
	cq.EndCommitSeqWindow()
	churn(2, 3*limit)
	if n := stampCount(); n > limit {
		t.Fatalf("commitSeq holds %d stamps after the window closed, want <= %d", n, limit)
	}
	if got := cq.CommitSeq("/keep/old.txt"); got != 0 {
		t.Fatalf("expired stamp /keep/old.txt survived the window close: %d", got)
	}
	if got := cq.CommitSeq("/keep/dst.txt"); got != 0 {
		t.Fatalf("expired stamp /keep/dst.txt survived the window close: %d", got)
	}
}
