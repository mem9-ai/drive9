package membudget

import (
	"errors"
	"sync"
	"testing"

	"go.uber.org/zap"
)

func TestNilBudgetIsNoop(t *testing.T) {
	b := New(0, ModeEnforce, nil)
	if b != nil {
		t.Fatalf("New(0) = %v, want nil", b)
	}
	if err := b.Charge(ClassReadCache, 1<<30); err != nil {
		t.Fatalf("nil Charge: %v", err)
	}
	if err := b.Acquire(ClassWriteBuffer, 1<<30); err != nil {
		t.Fatalf("nil Acquire: %v", err)
	}
	b.Release(ClassWriteBuffer, 1<<30)
	if got := b.Used(); got != 0 {
		t.Fatalf("nil Used = %d", got)
	}
	if got := b.Snapshot(); got.Over || got.Limit != 0 {
		t.Fatalf("nil Snapshot = %+v", got)
	}
	b.RegisterReclaimer(ClassReadCache, func(int64) int64 { return 0 })
}

func TestChargeTracksClassesAndLimits(t *testing.T) {
	b := New(100, ModeEnforce, nil)
	if err := b.Charge(ClassReadCache, 40); err != nil {
		t.Fatalf("Charge read: %v", err)
	}
	if err := b.Charge(ClassWriteBuffer, 50); err != nil {
		t.Fatalf("Charge buffer: %v", err)
	}
	if got := b.Used(); got != 90 {
		t.Fatalf("Used = %d, want 90", got)
	}
	if got := b.UsedBy(ClassReadCache); got != 40 {
		t.Fatalf("UsedBy read = %d", got)
	}
	if got := b.UsedBy(ClassWriteBuffer); got != 50 {
		t.Fatalf("UsedBy buffer = %d", got)
	}
	err := b.Charge(ClassWriteBuffer, 11)
	if !errors.Is(err, ErrExceeded) {
		t.Fatalf("over-budget Charge = %v, want ErrExceeded", err)
	}
	if got := b.Used(); got != 90 {
		t.Fatalf("refused charge changed Used to %d", got)
	}
	if got := b.Refusals(); got != 1 {
		t.Fatalf("Refusals = %d, want 1", got)
	}
	// An exact fit is allowed.
	if err := b.Charge(ClassWriteBuffer, 10); err != nil {
		t.Fatalf("exact fit Charge: %v", err)
	}
	if got := b.Peak(); got != 100 {
		t.Fatalf("Peak = %d, want 100", got)
	}
	if b.OverLimit() {
		t.Fatal("OverLimit at the limit")
	}
	// Releasing returns capacity.
	b.Release(ClassReadCache, 40)
	if got := b.Used(); got != 60 {
		t.Fatalf("Used after release = %d", got)
	}
	if err := b.Charge(ClassReadCache, 40); err != nil {
		t.Fatalf("Charge after release: %v", err)
	}
}

func TestReleaseClampsAtZero(t *testing.T) {
	b := New(100, ModeEnforce, nil)
	if err := b.Charge(ClassReadCache, 10); err != nil {
		t.Fatal(err)
	}
	b.Release(ClassReadCache, 4096)
	if got := b.Used(); got != 0 {
		t.Fatalf("Used = %d, want clamped 0", got)
	}
	if got := b.UsedBy(ClassReadCache); got != 0 {
		t.Fatalf("UsedBy = %d, want clamped 0", got)
	}
	if err := b.Charge(ClassReadCache, 100); err != nil {
		t.Fatalf("Charge after clamp: %v", err)
	}
}

func TestWarnModeAdmitsOverLimit(t *testing.T) {
	b := New(100, ModeWarn, nil)
	if err := b.Charge(ClassWriteBuffer, 150); err != nil {
		t.Fatalf("warn Charge = %v, want nil", err)
	}
	if got := b.Used(); got != 150 {
		t.Fatalf("Used = %d, want 150", got)
	}
	if !b.OverLimit() {
		t.Fatal("OverLimit = false")
	}
	if got := b.Overruns(); got != 1 {
		t.Fatalf("Overruns = %d, want 1", got)
	}
	// Acquire behaves the same in warn mode.
	if err := b.Acquire(ClassWriteBuffer, 100); err != nil {
		t.Fatalf("warn Acquire = %v", err)
	}
	if got := b.Overruns(); got != 2 {
		t.Fatalf("Overruns = %d, want 2", got)
	}
}

func TestAcquireReclaimsBeforeRefusing(t *testing.T) {
	b := New(100, ModeEnforce, nil)
	var calls int
	var targets []int64
	b.RegisterReclaimer(ClassReadCache, func(target int64) int64 {
		calls++
		targets = append(targets, target)
		// Free 40 bytes, reporting them through the budget like a real
		// cache eviction does.
		freed := int64(40)
		b.Release(ClassReadCache, freed)
		return freed
	})
	if err := b.Charge(ClassReadCache, 60); err != nil {
		t.Fatal(err)
	}
	if err := b.Charge(ClassWriteBuffer, 40); err != nil {
		t.Fatal(err)
	}
	// Need 30 more: 40 is reclaimable, so the acquisition succeeds.
	if err := b.Acquire(ClassWriteBuffer, 30); err != nil {
		t.Fatalf("Acquire = %v, want nil after reclaim", err)
	}
	if calls != 1 {
		t.Fatalf("reclaimer calls = %d, want 1", calls)
	}
	if len(targets) != 1 || targets[0] != 30 {
		t.Fatalf("reclaim target = %v, want [30]", targets)
	}
	// 60 read + 40 buffer - 40 reclaimed + 30 acquired.
	if got := b.Used(); got != 90 {
		t.Fatalf("Used = %d, want 90", got)
	}
}

func TestAcquireRefusesWhenReclaimIsInsufficient(t *testing.T) {
	b := New(100, ModeEnforce, nil)
	b.RegisterReclaimer(ClassReadCache, func(target int64) int64 {
		freed := int64(5)
		b.Release(ClassReadCache, freed)
		return freed
	})
	if err := b.Charge(ClassReadCache, 100); err != nil {
		t.Fatal(err)
	}
	err := b.Acquire(ClassWriteBuffer, 50)
	if !errors.Is(err, ErrExceeded) {
		t.Fatalf("Acquire = %v, want ErrExceeded", err)
	}
	if got := b.Used(); got != 95 {
		t.Fatalf("Used = %d, want 95 (only the reclaimed bytes freed)", got)
	}
	if got := b.Refusals(); got != 1 {
		t.Fatalf("Refusals = %d, want 1", got)
	}
}

func TestAcquireDoesNotReclaimForCharge(t *testing.T) {
	b := New(10, ModeEnforce, nil)
	b.RegisterReclaimer(ClassReadCache, func(int64) int64 {
		t.Fatal("Charge must not run a reclaimer")
		return 0
	})
	if err := b.Charge(ClassReadCache, 11); !errors.Is(err, ErrExceeded) {
		t.Fatalf("Charge = %v, want ErrExceeded", err)
	}
}

func TestReclaimerOrderIsDeterministic(t *testing.T) {
	b := New(100, ModeEnforce, nil)
	var order []Class
	b.RegisterReclaimer(ClassWriteCache, func(target int64) int64 {
		order = append(order, ClassWriteCache)
		return 0
	})
	b.RegisterReclaimer(ClassReadCache, func(target int64) int64 {
		order = append(order, ClassReadCache)
		return 0
	})
	if err := b.Charge(ClassWriteBuffer, 100); err != nil {
		t.Fatal(err)
	}
	_ = b.Acquire(ClassWriteBuffer, 10)
	want := []Class{ClassReadCache, ClassWriteCache}
	if len(order) != len(want) {
		t.Fatalf("reclaimer order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("reclaimer order = %v, want %v", order, want)
		}
	}
}

func TestConcurrentChargeNeverExceedsLimit(t *testing.T) {
	b := New(1_000_000, ModeEnforce, nil)
	var wg sync.WaitGroup
	var mu sync.Mutex
	admitted := 0
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if b.Charge(ClassWriteBuffer, 1024) == nil {
					mu.Lock()
					admitted++
					mu.Unlock()
					continue
				}
				return
			}
		}()
	}
	wg.Wait()
	if got := b.Used(); got > b.Limit() {
		t.Fatalf("Used = %d exceeds limit %d", got, b.Limit())
	}
	if int64(admitted)*1024 != b.Used() {
		t.Fatalf("admitted*1024 = %d, Used = %d", admitted*1024, b.Used())
	}
}

func TestSnapshotString(t *testing.T) {
	b := New(1000, ModeWarn, zap.NewNop())
	if err := b.Charge(ClassInodeTable, 12); err != nil {
		t.Fatal(err)
	}
	got := b.Snapshot().String()
	want := "used=12 limit=1000 mode=warn read_cache=0 write_cache=0 write_buffer=0 inode_table=12"
	if got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

func TestParseMode(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want Mode
		ok   bool
	}{
		{"", ModeEnforce, true},
		{"enforce", ModeEnforce, true},
		{"warn", ModeWarn, true},
		{"degrade", "", false},
	} {
		got, err := ParseMode(tc.in)
		if tc.ok && err != nil {
			t.Fatalf("ParseMode(%q): %v", tc.in, err)
		}
		if !tc.ok {
			if err == nil {
				t.Fatalf("ParseMode(%q) = %v, want error", tc.in, got)
			}
			continue
		}
		if got != tc.want {
			t.Fatalf("ParseMode(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}
