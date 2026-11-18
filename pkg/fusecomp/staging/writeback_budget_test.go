package staging

import (
	"testing"

	"github.com/mem9-ai/drive9/pkg/fusecomp/membudget"
)

func TestWriteBackCacheChargesBudget(t *testing.T) {
	c, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	budget := membudget.New(1<<20, membudget.ModeEnforce, nil)
	c.SetMemoryBudget(budget)

	payload := make([]byte, 1024)
	if err := c.Put("/a.txt", payload, int64(len(payload)), PendingNew); err != nil {
		t.Fatal(err)
	}
	if got := c.InMemoryUsage(); got != int64(len(payload)) {
		t.Fatalf("InMemoryUsage = %d, want %d", got, len(payload))
	}
	if got := budget.UsedBy(membudget.ClassWriteCache); got != int64(len(payload)) {
		t.Fatalf("budget charge = %d, want %d", got, len(payload))
	}

	// Removing the entry releases the charge.
	c.Remove("/a.txt")
	if got := budget.UsedBy(membudget.ClassWriteCache); got != 0 {
		t.Fatalf("budget charge after Remove = %d, want 0", got)
	}
}

func TestWriteBackCacheDegradesToDiskWhenBudgetIsFull(t *testing.T) {
	c, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	budget := membudget.New(1<<20, membudget.ModeEnforce, nil)
	c.SetMemoryBudget(budget)
	// Another class owns the whole budget: the payload cache can neither
	// charge nor free room for itself.
	if err := budget.Charge(membudget.ClassWriteBuffer, 1<<20); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 1024)
	if err := c.Put("/b.txt", payload, int64(len(payload)), PendingNew); err != nil {
		t.Fatalf("Put must still succeed (disk-only): %v", err)
	}
	if got := c.InMemoryUsage(); got != 0 {
		t.Fatalf("InMemoryUsage = %d, want 0 (disk-only)", got)
	}
	if c.HasInMemoryPayload("/b.txt") {
		t.Fatal("payload retained without budget")
	}
	// The .dat file is authoritative: the entry is still readable.
	meta, ok := c.GetMeta("/b.txt")
	if !ok || meta.Size != int64(len(payload)) {
		t.Fatalf("meta = %+v ok=%v", meta, ok)
	}
	if _, ok := c.GetView("/b.txt"); !ok {
		t.Fatal("GetView after degrade failed")
	}
}

func TestWriteBackCacheReclaimsUnderPressure(t *testing.T) {
	c, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	budget := membudget.New(4096, membudget.ModeEnforce, nil)
	c.SetMemoryBudget(budget)
	payload := make([]byte, 2048)
	if err := c.Put("/c.txt", payload, int64(len(payload)), PendingNew); err != nil {
		t.Fatal(err)
	}
	// A write-buffer acquisition needs the cache's bytes.
	if err := budget.Acquire(membudget.ClassWriteBuffer, 4000); err != nil {
		t.Fatalf("Acquire = %v", err)
	}
	if got := c.InMemoryUsage(); got != 0 {
		t.Fatalf("InMemoryUsage after reclaim = %d, want 0", got)
	}
	if _, ok := c.GetMeta("/c.txt"); !ok {
		t.Fatal("reclaim dropped the pending entry (metadata must survive)")
	}
}
