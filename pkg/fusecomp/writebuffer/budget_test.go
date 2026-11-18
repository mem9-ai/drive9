package writebuffer

import (
	"errors"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/pkg/fusecomp/membudget"
)

// newBudgetedBuffer returns a part-mode buffer wired to a budget of limit
// bytes. The small-file fast path is skipped so every write goes through the
// part map's allocation path.
func newBudgetedBuffer(t *testing.T, limit int64) (*WriteBuffer, *membudget.Budget) {
	t.Helper()
	budget := membudget.New(limit, membudget.ModeEnforce, nil)
	wb := NewWriteBuffer("/budgeted.txt", 0, 64<<10)
	wb.SetSmallFileMax(1024)
	wb.SetMemoryBudget(budget)
	t.Cleanup(func() {
		wb.Reset()
	})
	return wb, budget
}

func TestWriteBufferBudgetRefusesGrowth(t *testing.T) {
	wb, budget := newBudgetedBuffer(t, 100<<10)
	if _, err := wb.Write(0, make([]byte, 2048)); err != nil {
		t.Fatalf("first part write: %v", err)
	}
	used := budget.UsedBy(membudget.ClassWriteBuffer)
	if used == 0 {
		t.Fatal("write buffer charged nothing to the budget")
	}
	if used != wb.MemoryUsage() {
		t.Fatalf("budget charge %d != MemoryUsage %d", used, wb.MemoryUsage())
	}
	// A second part needs another 64 KiB allocation; the budget refuses it.
	_, err := wb.Write(64<<10, make([]byte, 2048))
	if !errors.Is(err, syscall.ENOMEM) {
		t.Fatalf("second part write = %v, want ENOMEM", err)
	}
	if got := budget.Refusals(); got != 1 {
		t.Fatalf("refusals = %d, want 1", got)
	}
	// Reset releases the charge.
	wb.Reset()
	if got := budget.UsedBy(membudget.ClassWriteBuffer); got != 0 {
		t.Fatalf("charge after Reset = %d, want 0", got)
	}
}

func TestWriteBufferBudgetReleaseOnEvict(t *testing.T) {
	wb, budget := newBudgetedBuffer(t, 100<<10)
	if _, err := wb.Write(0, make([]byte, 2048)); err != nil {
		t.Fatal(err)
	}
	before := budget.UsedBy(membudget.ClassWriteBuffer)
	if before == 0 {
		t.Fatal("no charge")
	}
	wb.EvictPart(0)
	if got := budget.UsedBy(membudget.ClassWriteBuffer); got != 0 {
		t.Fatalf("charge after EvictPart = %d, want 0", got)
	}
	if wb.MemoryUsage() != 0 {
		t.Fatalf("MemoryUsage after EvictPart = %d", wb.MemoryUsage())
	}
}

func TestWriteBufferBudgetReleaseOnTruncate(t *testing.T) {
	wb, budget := newBudgetedBuffer(t, 1<<20)
	if _, err := wb.Write(0, make([]byte, 64<<10)); err != nil {
		t.Fatal(err)
	}
	if _, err := wb.Write(64<<10, make([]byte, 64<<10)); err != nil {
		t.Fatal(err)
	}
	before := budget.UsedBy(membudget.ClassWriteBuffer)
	if before < 2*(64<<10) {
		t.Fatalf("charge = %d, want at least two parts", before)
	}
	if err := wb.Truncate(0); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if got := budget.UsedBy(membudget.ClassWriteBuffer); got != 0 {
		t.Fatalf("charge after Truncate(0) = %d, want 0", got)
	}
}

func TestWriteBufferChargesSmallFileCapacity(t *testing.T) {
	budget := membudget.New(4096, membudget.ModeEnforce, nil)
	wb := NewWriteBuffer("/small.txt", 0, 8<<20)
	wb.SetSmallFileMax(4096)
	wb.SetMemoryBudget(budget)
	t.Cleanup(func() { wb.Reset() })

	// The fast path allocates at least 1 KiB for a 100-byte image, and that
	// allocation is charged: many open small files must not exceed the
	// process budget between them.
	if _, err := wb.Write(0, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	want := int64(1024)
	if got := budget.UsedBy(membudget.ClassWriteBuffer); got != want {
		t.Fatalf("small-file charge = %d, want %d", got, want)
	}
	if got := wb.MemoryUsage(); got != want {
		t.Fatalf("MemoryUsage = %d, want %d", got, want)
	}
	// The migration to part mode transfers the charge to the part allocation
	// (length 100) instead of charging a second time.
	if _, err := wb.Write(0, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	wb.Reset()
	if got := budget.UsedBy(membudget.ClassWriteBuffer); got != 0 {
		t.Fatalf("charge after Reset = %d, want 0", got)
	}
}

func TestWriteBufferBudgetRefusesPartGrowthAndKeepsSize(t *testing.T) {
	// The budget holds the small-file allocation but not the first 64 KiB
	// part allocation it would migrate into.
	budget := membudget.New(2048, membudget.ModeEnforce, nil)
	wb := NewWriteBuffer("/small.txt", 0, 64<<10)
	wb.SetSmallFileMax(4096)
	wb.SetMemoryBudget(budget)
	t.Cleanup(func() { wb.Reset() })
	if _, err := wb.Write(0, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	// Crossing the cutoff migrates (charge transferred), then the part
	// allocation is refused.
	_, err := wb.Write(4096, make([]byte, 100))
	if !errors.Is(err, syscall.ENOMEM) {
		t.Fatalf("part growth write = %v, want ENOMEM", err)
	}
	// A failed write must not extend the logical image over the range it
	// never staged, and the staged bytes survive.
	if got := wb.Size(); got != 100 {
		t.Fatalf("size after refused write = %d, want 100", got)
	}
	if part := wb.PartData(1); len(part) == 0 {
		t.Fatal("staged bytes lost")
	}
}

func TestWriteBufferWithoutBudgetIsUnlimited(t *testing.T) {
	wb := NewWriteBuffer("/plain.txt", 0, 64<<10)
	wb.SetSmallFileMax(1024)
	for i := 0; i < 4; i++ {
		if _, err := wb.Write(int64(i)*(64<<10), make([]byte, 2048)); err != nil {
			t.Fatalf("write %d without budget: %v", i, err)
		}
	}
	if wb.MemoryBudget() != nil {
		t.Fatal("unexpected budget")
	}
}
