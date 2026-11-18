package cache

import (
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/fusecomp/membudget"
)

func TestReadCacheBudgetEvictsOwnLRUToAdmit(t *testing.T) {
	rc := NewReadCache(1<<20, time.Minute)
	budget := membudget.New(400, membudget.ModeEnforce, nil)
	rc.SetMemoryBudget(budget)

	rc.Put("/small", make([]byte, 300), 1)
	if got := budget.UsedBy(membudget.ClassReadCache); got != 300 {
		t.Fatalf("budget charge = %d, want 300", got)
	}

	// The second entry does not fit next to the first, so the cache evicts
	// its own LRU tail and admits it: a cache miss next time, never a
	// failed read.
	rc.Put("/second", make([]byte, 300), 1)
	if got := rc.Size(); got != 300 {
		t.Fatalf("Size after admitting second entry = %d, want 300", got)
	}
	if _, ok := rc.Get("/second", 1); !ok {
		t.Fatal("second entry was not cached")
	}
	if _, ok := rc.Get("/small", 1); ok {
		t.Fatal("LRU tail was not evicted to make room")
	}

	// InvalidateAll returns every charged byte.
	rc.InvalidateAll()
	if got := budget.UsedBy(membudget.ClassReadCache); got != 0 {
		t.Fatalf("budget charge after InvalidateAll = %d, want 0", got)
	}
}

func TestReadCacheBudgetSkipsRetentionWhenFull(t *testing.T) {
	rc := NewReadCache(1<<20, time.Minute)
	budget := membudget.New(400, membudget.ModeEnforce, nil)
	rc.SetMemoryBudget(budget)

	// A non-reclaimable class (the write buffer) holds the whole budget, so
	// evicting the read cache's own entries cannot make room.
	if err := budget.Charge(membudget.ClassWriteBuffer, 400); err != nil {
		t.Fatal(err)
	}
	rc.Put("/does-not-fit", make([]byte, 300), 1)
	if got := rc.Size(); got != 0 {
		t.Fatalf("Size = %d, want 0 (entry must not be retained)", got)
	}
	if _, ok := rc.Get("/does-not-fit", 1); ok {
		t.Fatal("entry was cached without budget")
	}
}

func TestReadCacheBudgetReclaimerEvictsLRU(t *testing.T) {
	rc := NewReadCache(1<<20, time.Minute)
	budget := membudget.New(1000, membudget.ModeEnforce, nil)
	rc.SetMemoryBudget(budget)
	rc.Put("/a", make([]byte, 400), 1)
	rc.Put("/b", make([]byte, 400), 1)

	// A write buffer acquisition asks for 900 bytes: only 200 are free, so
	// the reclaimer must free at least 700 (it evicts both entries).
	if err := budget.Acquire(membudget.ClassWriteBuffer, 900); err != nil {
		t.Fatalf("Acquire after reclaim = %v", err)
	}
	if got := rc.Size(); got != 0 {
		t.Fatalf("cache Size after reclaim = %d, want 0", got)
	}
	if got := budget.UsedBy(membudget.ClassReadCache); got != 0 {
		t.Fatalf("read cache charge after reclaim = %d", got)
	}
}

func TestReadCacheBudgetReplaceKeepsAccountingExact(t *testing.T) {
	rc := NewReadCache(1<<20, time.Minute)
	budget := membudget.New(2000, membudget.ModeEnforce, nil)
	rc.SetMemoryBudget(budget)

	rc.Put("/a", make([]byte, 300), 1)
	if got := budget.UsedBy(membudget.ClassReadCache); got != 300 {
		t.Fatalf("charge after insert = %d, want 300", got)
	}
	// Replacing with a larger payload must transition the charge, not add to
	// it: the old entry's bytes are no longer retained.
	rc.Put("/a", make([]byte, 500), 2)
	if got := budget.UsedBy(membudget.ClassReadCache); got != 500 {
		t.Fatalf("charge after grow-replace = %d, want 500", got)
	}
	// Replacing with a smaller payload returns the difference.
	rc.Put("/a", make([]byte, 100), 3)
	if got := budget.UsedBy(membudget.ClassReadCache); got != 100 {
		t.Fatalf("charge after shrink-replace = %d, want 100", got)
	}
	if got := rc.Size(); got != 100 {
		t.Fatalf("cache size = %d, want 100", got)
	}
}
