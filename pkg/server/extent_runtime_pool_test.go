package server

import (
	"context"
	"errors"
	"testing"

	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/extent"
	"github.com/mem9-ai/drive9/pkg/s3client"
)

func newPoolTestS3(t *testing.T) s3client.S3Client {
	t.Helper()
	local, err := s3client.NewLocal(t.TempDir(), "http://127.0.0.1:9")
	if err != nil {
		t.Fatal(err)
	}
	return local
}

// newRecordingPool builds a pool whose runtime constructor and closer are
// fakes, so reuse/close behavior is observable without JuiceFS or TiDB.
func newRecordingPool(t *testing.T) (pool *extentRuntimePool, built *int, closed *map[*extent.Runtime]int) {
	t.Helper()
	builtCount := 0
	closedCount := map[*extent.Runtime]int{}
	pool = newExtentRuntimePool()
	pool.newRuntime = func(extent.RuntimeConfig) (*extent.Runtime, error) {
		builtCount++
		return &extent.Runtime{}, nil
	}
	pool.closeRuntime = func(rt *extent.Runtime) error {
		closedCount[rt]++
		return nil
	}
	return pool, &builtCount, &closedCount
}

func TestExtentRuntimePoolReusesRuntimePerTenantAndStore(t *testing.T) {
	pool, built, closed := newRecordingPool(t)
	store := &datastore.Store{}
	s3 := newPoolTestS3(t)
	var seen []*extent.Runtime
	run := func() {
		t.Helper()
		if err := pool.withRuntime("tenant-a", store, s3, func(rt *extent.Runtime) error {
			seen = append(seen, rt)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	run()
	run()
	if *built != 1 {
		t.Fatalf("runtime constructions = %d, want 1 (reused for the same tenant + backend)", *built)
	}
	if len(seen) != 2 || seen[0] != seen[1] {
		t.Fatalf("the same runtime must be handed to both passes: %v", seen)
	}
	if len(*closed) != 0 {
		t.Fatalf("nothing may be closed while the runtime is still current: %v", *closed)
	}

	// A different tenant gets its own runtime.
	if err := pool.withRuntime("tenant-b", store, s3, func(*extent.Runtime) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if *built != 2 {
		t.Fatalf("runtime constructions = %d, want 2 (one per tenant)", *built)
	}
	if seen[0] == nil {
		t.Fatal("first runtime is nil")
	}
}

func TestExtentRuntimePoolRebuildsWhenBackendIsReplaced(t *testing.T) {
	pool, built, closed := newRecordingPool(t)
	oldStore := &datastore.Store{}
	newStore := &datastore.Store{}
	s3 := newPoolTestS3(t)
	if err := pool.withRuntime("tenant-a", oldStore, s3, func(*extent.Runtime) error { return nil }); err != nil {
		t.Fatal(err)
	}
	var reused *extent.Runtime
	if err := pool.withRuntime("tenant-a", newStore, s3, func(rt *extent.Runtime) error {
		reused = rt
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if *built != 2 {
		t.Fatalf("runtime constructions = %d, want 2 after the store changed", *built)
	}
	if len(*closed) != 1 {
		t.Fatalf("stale runtime not closed exactly once: %v", *closed)
	}
	for rt, n := range *closed {
		if n != 1 {
			t.Fatalf("runtime %p closed %d times", rt, n)
		}
		if rt == reused {
			t.Fatal("the pool handed back the runtime it just closed")
		}
	}
	// The cached runtime always pairs with the store it was built against: the
	// entry holds that store alive until the runtime is dropped, which is what
	// makes the pointer comparison above an exact backend-generation check.
	e := pool.entries["tenant-a"]
	if e == nil || e.rt != reused || e.store != newStore {
		t.Fatalf("entry = %+v, want the new runtime paired with the new store", e)
	}
}

func TestExtentRuntimePoolCloseAllClosesEveryRuntimeOnce(t *testing.T) {
	pool, built, closed := newRecordingPool(t)
	store := &datastore.Store{}
	s3 := newPoolTestS3(t)
	for _, tenantID := range []string{"tenant-a", "tenant-b"} {
		if err := pool.withRuntime(tenantID, store, s3, func(*extent.Runtime) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	if *built != 2 {
		t.Fatalf("runtime constructions = %d, want 2", *built)
	}
	pool.closeAll()
	if len(*closed) != 2 {
		t.Fatalf("closeAll closed %d runtimes, want 2", len(*closed))
	}
	for rt, n := range *closed {
		if n != 1 {
			t.Fatalf("runtime %p closed %d times", rt, n)
		}
	}
	// Closing twice must not close anything again.
	pool.closeAll()
	for rt, n := range *closed {
		if n != 1 {
			t.Fatalf("runtime %p closed %d times after a second closeAll", rt, n)
		}
	}
}

func TestExtentRuntimePoolForgetClosesOnlyThatTenant(t *testing.T) {
	pool, _, closed := newRecordingPool(t)
	store := &datastore.Store{}
	s3 := newPoolTestS3(t)
	if err := pool.withRuntime("tenant-a", store, s3, func(*extent.Runtime) error { return nil }); err != nil {
		t.Fatal(err)
	}
	var kept *extent.Runtime
	if err := pool.withRuntime("tenant-b", store, s3, func(rt *extent.Runtime) error {
		kept = rt
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	pool.forget("tenant-a")
	if len(*closed) != 1 {
		t.Fatalf("forget closed %d runtimes, want 1", len(*closed))
	}
	if _, ok := pool.entries["tenant-a"]; ok {
		t.Fatal("forget left the deleted tenant's entry behind")
	}
	if err := pool.withRuntime("tenant-b", store, s3, func(rt *extent.Runtime) error {
		if rt != kept {
			t.Fatal("forget evicted an unrelated tenant's runtime")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	pool.forget("")
	pool.forget("tenant-c")
}

func TestExtentRuntimePoolRejectsMissingInputs(t *testing.T) {
	pool, built, _ := newRecordingPool(t)
	called := false
	fn := func(*extent.Runtime) error {
		called = true
		return nil
	}
	if err := pool.withRuntime("", &datastore.Store{}, newPoolTestS3(t), fn); err == nil {
		t.Fatal("empty tenant must fail")
	}
	if err := pool.withRuntime("tenant-a", nil, newPoolTestS3(t), fn); err == nil {
		t.Fatal("nil store must fail")
	}
	if err := pool.withRuntime("tenant-a", &datastore.Store{}, nil, fn); err == nil {
		t.Fatal("nil s3 client must fail")
	}
	if called {
		t.Fatal("no runtime callback may run when inputs are missing")
	}
	if *built != 0 {
		t.Fatalf("runtime constructions = %d, want 0", *built)
	}
	var nilPool *extentRuntimePool
	if err := nilPool.withRuntime("tenant-a", &datastore.Store{}, newPoolTestS3(t), fn); err == nil {
		t.Fatal("nil pool must fail")
	}
	nilPool.closeAll()
	nilPool.forget("tenant-a")
}

func TestExtentRuntimePoolReportsConstructorFailure(t *testing.T) {
	pool, _, closed := newRecordingPool(t)
	s3 := newPoolTestS3(t)
	wantErr := errors.New("meta unavailable")
	pool.newRuntime = func(extent.RuntimeConfig) (*extent.Runtime, error) { return nil, wantErr }
	called := false
	err := pool.withRuntime("tenant-a", &datastore.Store{}, s3, func(*extent.Runtime) error {
		called = true
		return nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
	if called {
		t.Fatal("callback must not run without a runtime")
	}
	if len(*closed) != 0 {
		t.Fatalf("nothing to close on a failed construction: %v", *closed)
	}
	// A later attempt must retry construction (no poisoned entry).
	pool.newRuntime = func(extent.RuntimeConfig) (*extent.Runtime, error) { return &extent.Runtime{}, nil }
	if err := pool.withRuntime("tenant-a", &datastore.Store{}, s3, func(*extent.Runtime) error { return nil }); err != nil {
		t.Fatalf("retry after a failed construction: %v", err)
	}
}

func TestExtentRuntimePoolPassesThroughCallbackError(t *testing.T) {
	pool, _, _ := newRecordingPool(t)
	wantErr := errors.New("compact failed")
	err := pool.withRuntime("tenant-a", &datastore.Store{}, newPoolTestS3(t), func(*extent.Runtime) error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
}

func TestExtentRuntimePoolSerializesOneTenant(t *testing.T) {
	pool, built, _ := newRecordingPool(t)
	store := &datastore.Store{}
	s3 := newPoolTestS3(t)
	release := make(chan struct{})
	entered := make(chan struct{})
	firstErr := make(chan error, 1)
	go func() {
		firstErr <- pool.withRuntime("tenant-a", store, s3, func(*extent.Runtime) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	second := make(chan struct{})
	go func() {
		_ = pool.withRuntime("tenant-a", store, s3, func(*extent.Runtime) error {
			close(second)
			return nil
		})
	}()
	select {
	case <-second:
		t.Fatal("a second task entered the same tenant's runtime concurrently")
	default:
	}
	close(release)
	if err := <-firstErr; err != nil {
		t.Fatal(err)
	}
	<-second
	if *built != 1 {
		t.Fatalf("runtime constructions = %d, want 1", *built)
	}
}

func TestDrainExtentMaintenanceSkipsWithoutInputs(t *testing.T) {
	ctx := context.Background()
	// A manager built outside newTenantWorkerManager has no runtime pool.
	bare := &tenantWorkerManager{}
	if bare.drainExtentMaintenance(ctx, &tenantTarget{tenantID: "tenant-a", store: &datastore.Store{}}) {
		t.Fatal("manager without a runtime pool must not report extent work")
	}
	if bare.drainExtentMaintenance(ctx, nil) {
		t.Fatal("nil target must not report extent work")
	}
	pooled := &tenantWorkerManager{extentRuntimes: newExtentRuntimePool()}
	if pooled.drainExtentMaintenance(ctx, &tenantTarget{tenantID: "tenant-a"}) {
		t.Fatal("target without store/backend must not report extent work")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if pooled.drainExtentMaintenance(cancelled, &tenantTarget{tenantID: "tenant-a", store: &datastore.Store{}}) {
		t.Fatal("cancelled context must not report extent work")
	}
}
