package backend

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// TestMutationDispatcherAppliesAcrossTenantsPreservingOrder drives the full
// enqueue path with the dispatcher running: items from multiple tenants must
// all be applied and batch-marked, and each tenant's applies must execute in
// durable log order (one shard per tenant guarantees FIFO).
func TestMutationDispatcherAppliesAcrossTenantsPreservingOrder(t *testing.T) {
	StartMutationDispatcher()
	t.Cleanup(StopMutationDispatcher)

	fake := newFakeMetaQuotaStore()
	const tenantCount = 3
	const perTenant = 5

	var orderMu sync.Mutex
	appliedOrder := make(map[string][]int)

	var backends []*Dat9Backend
	for i := 0; i < tenantCount; i++ {
		b := &Dat9Backend{}
		b.SetMetaQuotaStore(context.Background(), fmt.Sprintf("disp-tenant-%d", i), fake)
		backends = append(backends, b)
	}

	for n := 0; n < perTenant; n++ {
		for _, b := range backends {
			tenantID := b.TenantID()
			err := b.logAndEnqueueMutation(context.Background(), "file_create",
				fileCreateMutationData{FileID: fmt.Sprintf("file-%d", n), SizeBytes: 1},
				quotaPendingDeltas{}, func(context.Context, *sql.Tx) error {
					orderMu.Lock()
					appliedOrder[tenantID] = append(appliedOrder[tenantID], n)
					orderMu.Unlock()
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	// stopMutationWorker waits until the dispatcher has fully processed every
	// outstanding item of that backend (batch or fallback, plus bookkeeping).
	for _, b := range backends {
		b.stopMutationWorker()
	}

	if len(appliedOrder) != tenantCount {
		t.Fatalf("applied tenants = %d, want %d", len(appliedOrder), tenantCount)
	}
	for tenantID, got := range appliedOrder {
		if len(got) != perTenant {
			t.Fatalf("tenant %s applied %d mutations, want %d", tenantID, len(got), perTenant)
		}
		for i, v := range got {
			if v != i {
				t.Fatalf("tenant %s apply order %v, want 0..%d (per-tenant FIFO violated)", tenantID, got, perTenant-1)
			}
		}
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.mutations) != tenantCount*perTenant {
		t.Fatalf("logged mutations = %d, want %d", len(fake.mutations), tenantCount*perTenant)
	}
	for _, m := range fake.mutations {
		if m.status != "applied" {
			t.Fatalf("mutation %d tenant %s status = %q, want applied", m.id, m.tenantID, m.status)
		}
	}
	if fake.markAppliedCalls+fake.markBatchAppliedCalls == 0 {
		t.Fatal("no mark-applied calls recorded, want every mutation marked")
	}
}

// TestMutationBatchSingleTxForSameStore verifies the deterministic batch
// path: items sharing one store are applied in received order inside one
// transaction and marked with a single batch mark (no per-item marks).
func TestMutationBatchSingleTxForSameStore(t *testing.T) {
	fake := newFakeMetaQuotaStore()

	var orderMu sync.Mutex
	var received []string
	items := make([]quotaMutationItem, 0, 3)
	for i := 0; i < 3; i++ {
		tenantID := fmt.Sprintf("batch-tenant-%d", i)
		b := &Dat9Backend{}
		b.SetMetaQuotaStore(context.Background(), tenantID, fake)
		items = append(items, newDispatcherTestItem(t, b, fake, func(context.Context, *sql.Tx) error {
			orderMu.Lock()
			received = append(received, tenantID)
			orderMu.Unlock()
			return nil
		}))
	}

	processMutationBatch(context.Background(), items)

	if len(received) != len(items) {
		t.Fatalf("applied %d items, want %d", len(received), len(items))
	}
	for i := range items {
		if got := received[i]; got != items[i].tenantID {
			t.Fatalf("apply order %v, want received order", received)
		}
	}
	if fake.markBatchAppliedCalls != 1 {
		t.Fatalf("batch mark calls = %d, want 1 (single batch transaction)", fake.markBatchAppliedCalls)
	}
	if fake.markAppliedCalls != 0 {
		t.Fatalf("per-item mark calls = %d, want 0 on the batch path", fake.markAppliedCalls)
	}
	for _, item := range items {
		if got := fake.mutationStatus(item.logID); got != "applied" {
			t.Fatalf("mutation %d status = %q, want applied", item.logID, got)
		}
	}
}

// TestMutationBatchGroupsByStore verifies that items for different store
// identities are never mixed into one transaction: each store gets its own
// batch.
func TestMutationBatchGroupsByStore(t *testing.T) {
	fakeA := newFakeMetaQuotaStore()
	fakeB := newFakeMetaQuotaStore()

	bA := &Dat9Backend{}
	bA.SetMetaQuotaStore(context.Background(), "store-a-tenant", fakeA)
	bB := &Dat9Backend{}
	bB.SetMetaQuotaStore(context.Background(), "store-b-tenant", fakeB)

	items := []quotaMutationItem{
		newDispatcherTestItem(t, bA, fakeA, func(context.Context, *sql.Tx) error { return nil }),
		newDispatcherTestItem(t, bB, fakeB, func(context.Context, *sql.Tx) error { return nil }),
		newDispatcherTestItem(t, bA, fakeA, func(context.Context, *sql.Tx) error { return nil }),
		newDispatcherTestItem(t, bB, fakeB, func(context.Context, *sql.Tx) error { return nil }),
	}

	processMutationBatch(context.Background(), items)

	if fakeA.markBatchAppliedCalls != 1 || fakeB.markBatchAppliedCalls != 1 {
		t.Fatalf("batch marks = %d/%d, want 1 per store", fakeA.markBatchAppliedCalls, fakeB.markBatchAppliedCalls)
	}
	for i, item := range items {
		fake := fakeA
		if i%2 == 1 {
			fake = fakeB
		}
		if got := fake.mutationStatus(item.logID); got != "applied" {
			t.Fatalf("mutation %d status = %q, want applied", item.logID, got)
		}
	}
}

// adapterFakeStore wraps a shared *fakeMetaQuotaStore as a distinct
// MetaQuotaStore value — the way tenant.metaQuotaAdapter wraps one
// *meta.Store per backend — and reports the wrapped store as its identity.
type adapterFakeStore struct {
	*fakeMetaQuotaStore
}

func (a *adapterFakeStore) QuotaStoreIdentity() any { return a.fakeMetaQuotaStore }

// TestMutationBatchGroupsByStoreIdentity proves the production grouping key:
// two DIFFERENT MetaQuotaStore values (per-tenant adapters) over the SAME
// underlying store must batch into ONE transaction. Grouping by interface
// value instead would produce singleton groups and take the legacy per-item
// path for each item — which is exactly what happened in production before
// QuotaStoreIdentity existed.
func TestMutationBatchGroupsByStoreIdentity(t *testing.T) {
	shared := newFakeMetaQuotaStore()
	storeA := &adapterFakeStore{shared}
	storeB := &adapterFakeStore{shared}

	bA := &Dat9Backend{}
	bA.SetMetaQuotaStore(context.Background(), "identity-tenant-a", storeA)
	bB := &Dat9Backend{}
	bB.SetMetaQuotaStore(context.Background(), "identity-tenant-b", storeB)

	items := []quotaMutationItem{
		newDispatcherTestItem(t, bA, storeA, func(context.Context, *sql.Tx) error { return nil }),
		newDispatcherTestItem(t, bB, storeB, func(context.Context, *sql.Tx) error { return nil }),
	}

	processMutationBatch(context.Background(), items)

	if shared.markBatchAppliedCalls != 1 {
		t.Fatalf("batch mark calls = %d, want 1 (one transaction across adapters of the same store)", shared.markBatchAppliedCalls)
	}
	if shared.markAppliedCalls != 0 {
		t.Fatalf("per-item mark calls = %d, want 0 on the batch path", shared.markAppliedCalls)
	}
	for _, item := range items {
		if got := shared.mutationStatus(item.logID); got != "applied" {
			t.Fatalf("mutation %d status = %q, want applied", item.logID, got)
		}
	}
}

// TestMutationBatchAlreadyAppliedMarkFallsBackToPerItem drives the batch
// rollback path where the batch mark detects an already-applied row (e.g. a
// sibling pod applied it first): the batch transaction fails and every item
// must fall back to the per-item path — each item is per-item marked and
// sibling items still get applied.
func TestMutationBatchAlreadyAppliedMarkFallsBackToPerItem(t *testing.T) {
	fake := newFakeMetaQuotaStore()

	b1 := &Dat9Backend{}
	b1.SetMetaQuotaStore(context.Background(), "aa-tenant-1", fake)
	b2 := &Dat9Backend{}
	b2.SetMetaQuotaStore(context.Background(), "aa-tenant-2", fake)

	items := []quotaMutationItem{
		newDispatcherTestItem(t, b1, fake, func(context.Context, *sql.Tx) error { return nil }),
		newDispatcherTestItem(t, b2, fake, func(context.Context, *sql.Tx) error { return nil }),
	}
	// Pre-apply the first item outside the dispatcher so the batch mark fails
	// with an already-applied-wrapping error on the whole batch.
	if err := fake.MarkMutationAppliedTx(nil, items[0].logID); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	perItemMarksBefore := fake.markAppliedCalls
	fake.mu.Unlock()

	processMutationBatch(context.Background(), items)

	if fake.markBatchAppliedCalls != 1 {
		t.Fatalf("batch mark calls = %d, want 1 (attempted before the fallback)", fake.markBatchAppliedCalls)
	}
	if got := fake.markAppliedCalls - perItemMarksBefore; got != len(items) {
		t.Fatalf("per-item mark calls during fallback = %d, want %d (one per item)", got, len(items))
	}
	for _, item := range items {
		if got := fake.mutationStatus(item.logID); got != "applied" {
			t.Fatalf("mutation %d status = %q, want applied (sibling items applied via per-item path)", item.logID, got)
		}
	}
}

// TestMutationBatchPoisonItemFallsBackToPerItem verifies that one item whose
// apply always fails does not take down the rest of the batch: the batch
// transaction fails at the poison apply, every item falls back to the legacy
// per-item path, the good items are applied and marked, and the poison item
// stays pending for MutationReplayWorker.
func TestMutationBatchPoisonItemFallsBackToPerItem(t *testing.T) {
	fake := newFakeMetaQuotaStore()
	poisonErr := errors.New("poison apply")

	good1 := &Dat9Backend{}
	good1.SetMetaQuotaStore(context.Background(), "good-tenant-1", fake)
	poison := &Dat9Backend{}
	poison.SetMetaQuotaStore(context.Background(), "poison-tenant", fake)
	good2 := &Dat9Backend{}
	good2.SetMetaQuotaStore(context.Background(), "good-tenant-2", fake)

	goodApply := func(context.Context, *sql.Tx) error { return nil }
	items := []quotaMutationItem{
		newDispatcherTestItem(t, good1, fake, goodApply),
		newDispatcherTestItem(t, poison, fake, func(context.Context, *sql.Tx) error { return poisonErr }),
		newDispatcherTestItem(t, good2, fake, goodApply),
	}

	processMutationBatch(context.Background(), items)

	// The batch transaction aborted at the poison apply, so the batch mark
	// was never attempted and every item went through the per-item path.
	if fake.markBatchAppliedCalls != 0 {
		t.Fatalf("batch mark calls = %d, want 0 (batch aborted at poison apply)", fake.markBatchAppliedCalls)
	}
	if fake.markAppliedCalls != 2 {
		t.Fatalf("per-item mark calls = %d, want 2 (both good items)", fake.markAppliedCalls)
	}
	if got := fake.mutationStatus(items[0].logID); got != "applied" {
		t.Fatalf("good mutation before poison status = %q, want applied", got)
	}
	if got := fake.mutationStatus(items[2].logID); got != "applied" {
		t.Fatalf("good mutation after poison status = %q, want applied", got)
	}
	if got := fake.mutationStatus(items[1].logID); got != "pending" {
		t.Fatalf("poison mutation status = %q, want pending (left for MutationReplayWorker)", got)
	}
}

// TestEnqueueMutationItemInlineWhenDispatcherNotStarted verifies the inline
// fallback: without a running dispatcher the apply runs synchronously, as the
// pre-dispatcher queue fallback did.
func TestEnqueueMutationItemInlineWhenDispatcherNotStarted(t *testing.T) {
	if currentMutationDispatcher() != nil {
		t.Skip("mutation dispatcher is running (leaked by another test)")
	}
	fake := newFakeMetaQuotaStore()
	b := &Dat9Backend{}
	b.SetMetaQuotaStore(context.Background(), "inline-tenant", fake)

	var applied atomic.Int64
	err := b.logAndEnqueueMutation(context.Background(), "file_create",
		fileCreateMutationData{FileID: "inline-file", SizeBytes: 1},
		quotaPendingDeltas{}, func(context.Context, *sql.Tx) error {
			applied.Add(1)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if got := applied.Load(); got != 1 {
		t.Fatalf("apply count after enqueue = %d, want 1 (inline, synchronous)", got)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.mutations) != 1 {
		t.Fatalf("logged mutations = %d, want 1", len(fake.mutations))
	}
	if got := fake.mutations[0].status; got != "applied" {
		t.Fatalf("mutation status = %q, want applied", got)
	}
}

// newDispatcherTestItem durably logs a mutation through the backend and
// builds the dispatcher item exactly the way logAndEnqueueMutation does,
// including the mutationWG slot the worker releases after processing.
func newDispatcherTestItem(t *testing.T, b *Dat9Backend, store MetaQuotaStore, apply func(context.Context, *sql.Tx) error) quotaMutationItem {
	t.Helper()
	logID, err := b.logQuotaMutation(context.Background(), "file_create", fileCreateMutationData{FileID: "f", SizeBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	b.mutationWG.Add(1)
	return quotaMutationItem{
		backend:      b,
		store:        store,
		tenantID:     b.TenantID(),
		mutationType: "file_create",
		logID:        logID,
		apply:        apply,
	}
}
