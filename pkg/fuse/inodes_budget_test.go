package fuse

import (
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/fusecomp/membudget"
)

func TestInodeTableChargesAndReleasesBudget(t *testing.T) {
	m := NewInodeToPath()
	budget := membudget.New(1<<20, membudget.ModeEnforce, nil)
	m.SetMemoryBudget(budget)

	const paths = 10
	for i := 0; i < paths; i++ {
		m.Lookup("/dir/file"+string(rune('a'+i)), false, 1, time.Now())
	}
	// Attaching the budget charges the pre-existing root as well.
	want := int64(paths+1) * inodeTableEntryBytes
	if got := budget.UsedBy(membudget.ClassInodeTable); got != want {
		t.Fatalf("inode charge = %d, want %d", got, want)
	}
	if got := m.Entries(); got != paths+1 {
		t.Fatalf("entries = %d, want %d (root + %d)", got, paths+1, paths)
	}

	// Forgetting an inode returns its estimate.
	ino, ok := m.GetInode("/dir/filea")
	if !ok {
		t.Fatal("filea not found")
	}
	m.Forget(ino, 1)
	if _, ok := m.GetInode("/dir/filea"); ok {
		t.Fatal("filea still mapped after Forget")
	}
	if got := budget.UsedBy(membudget.ClassInodeTable); got != want-inodeTableEntryBytes {
		t.Fatalf("inode charge after Forget = %d, want %d", got, want-inodeTableEntryBytes)
	}
}

func TestInodeTableBudgetIsForced(t *testing.T) {
	m := NewInodeToPath()
	// A budget far too small for even one entry: lookups must still work
	// (the namespace cannot fail on memory accounting) and the overrun must
	// be visible in the snapshot.
	budget := membudget.New(1, membudget.ModeEnforce, nil)
	m.SetMemoryBudget(budget)
	for i := 0; i < 5; i++ {
		if ino := m.Lookup("/f"+string(rune('a'+i)), false, 0, time.Now()); ino == 0 {
			t.Fatal("lookup returned inode 0 under pressure")
		}
	}
	snap := budget.Snapshot()
	if !snap.Over {
		t.Fatalf("snapshot not over limit: %+v", snap)
	}
	if snap.Overruns == 0 {
		t.Fatal("overruns not counted")
	}
	if got := snap.Classes[membudget.ClassInodeTable]; got != 6*inodeTableEntryBytes {
		t.Fatalf("inode class charge = %d", got)
	}
}

func TestInodeTableWithoutBudget(t *testing.T) {
	m := NewInodeToPath()
	if m.MemoryBudget() != nil {
		t.Fatal("unexpected budget")
	}
	m.Lookup("/plain", false, 1, time.Now())
	if got := m.Entries(); got != 2 {
		t.Fatalf("entries = %d", got)
	}
}
