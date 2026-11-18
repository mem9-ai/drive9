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

func TestWriteBufferBudgetRefusesSmallFileMigration(t *testing.T) {
	// Smaller than the staged small-file image: the migration's new part
	// allocation cannot be charged.
	budget := membudget.New(50, membudget.ModeEnforce, nil)
	wb := NewWriteBuffer("/small.txt", 0, 8<<20)
	wb.SetSmallFileMax(4096)
	wb.SetMemoryBudget(budget)
	t.Cleanup(func() { wb.Reset() })
	// Land in the small-file fast path first (uncharged).
	if _, err := wb.Write(0, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	if got := budget.Used(); got != 0 {
		t.Fatalf("small-file mode charged %d bytes, want 0", got)
	}
	// Crossing the cutoff migrates to a part allocation that cannot fit.
	_, err := wb.Write(4096, make([]byte, 100))
	if !errors.Is(err, syscall.ENOMEM) {
		t.Fatalf("migration write = %v, want ENOMEM", err)
	}
	// The small-file image must survive the refused migration.
	if got := wb.Size(); got != 100 {
		t.Fatalf("size after refused migration = %d, want 100", got)
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
