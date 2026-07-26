package backend

import (
	"context"
	"database/sql"
	"testing"
)

// TestMutationBatchAggregatesCounterDeltasSortedByTenant drives a cross-tenant
// batch (3 mutations for one tenant, 1 for another) and asserts the "hot row
// last" contract: the runner flushes exactly one IncrQuotaUsageCountersTx per
// tenant, in sorted tenant_id order (deterministic cross-pod lock order),
// with the per-item deltas correctly summed.
func TestMutationBatchAggregatesCounterDeltasSortedByTenant(t *testing.T) {
	fake := newFakeMetaQuotaStore()

	bA := &Dat9Backend{}
	bA.SetMetaQuotaStore(context.Background(), "agg-tenant-a", fake)
	bB := &Dat9Backend{}
	bB.SetMetaQuotaStore(context.Background(), "agg-tenant-b", fake)

	fixedDeltas := func(d quotaCounterDeltas) func(context.Context, *sql.Tx) (quotaCounterDeltas, error) {
		return func(context.Context, *sql.Tx) (quotaCounterDeltas, error) { return d, nil }
	}
	items := []quotaMutationItem{
		newDispatcherTestItem(t, bA, fake, fixedDeltas(quotaCounterDeltas{storageBytes: 10, fileCount: 1})),
		newDispatcherTestItem(t, bB, fake, fixedDeltas(quotaCounterDeltas{storageBytes: 7, fileCount: 1})),
		newDispatcherTestItem(t, bA, fake, fixedDeltas(quotaCounterDeltas{storageBytes: 20})),
		newDispatcherTestItem(t, bA, fake, fixedDeltas(quotaCounterDeltas{storageBytes: 5, mediaFileCount: 1, reservedBytes: -3})),
	}

	processMutationBatch(context.Background(), items)

	// processMutationBatch is synchronous, so the fake state is stable here.
	want := []incrQuotaUsageCountersCall{
		{tenantID: "agg-tenant-a", storageDelta: 35, fileDelta: 1, mediaDelta: 1, reservedDelta: -3},
		{tenantID: "agg-tenant-b", storageDelta: 7, fileDelta: 1},
	}
	if len(fake.incrQuotaUsageCountersCalls) != len(want) {
		t.Fatalf("counter flush calls = %v, want %v", fake.incrQuotaUsageCountersCalls, want)
	}
	for i, w := range want {
		if got := fake.incrQuotaUsageCountersCalls[i]; got != w {
			t.Fatalf("counter flush call %d = %+v, want %+v (sorted tenant order, summed deltas)", i, got, w)
		}
	}
	for _, item := range items {
		if got := fake.mutationStatus(item.logID); got != "applied" {
			t.Fatalf("mutation %d status = %q, want applied", item.logID, got)
		}
	}
}

// TestMutationBatchCreateThenOverwriteSumsDeltas proves the in-tx read-your-
// writes property of delta aggregation: create(file,100) followed by
// overwrite(file,100→150) in ONE batch must sum to storage_bytes +150 and
// file_count +1, because the overwrite's GetFileMetaForUpdateTx sees the
// create's in-tx upsert.
func TestMutationBatchCreateThenOverwriteSumsDeltas(t *testing.T) {
	fake := newFakeMetaQuotaStore()
	b := &Dat9Backend{}
	b.SetMetaQuotaStore(context.Background(), "hot-tenant", fake)

	items := []quotaMutationItem{
		newDispatcherTestItem(t, b, fake, func(ctx context.Context, tx *sql.Tx) (quotaCounterDeltas, error) {
			return applyCentralFileCreateTx(fake, tx, "hot-tenant", fileCreateMutationData{FileID: "f1", SizeBytes: 100})
		}),
		newDispatcherTestItem(t, b, fake, func(ctx context.Context, tx *sql.Tx) (quotaCounterDeltas, error) {
			return applyCentralFileOverwriteTx(fake, tx, "hot-tenant", fileOverwriteMutationData{
				FileID: "f1", OldSizeBytes: 100, NewSizeBytes: 150,
			})
		}),
	}

	processMutationBatch(context.Background(), items)

	if len(fake.incrQuotaUsageCountersCalls) != 1 {
		t.Fatalf("counter flush calls = %v, want exactly 1 for a single-tenant batch", fake.incrQuotaUsageCountersCalls)
	}
	got := fake.incrQuotaUsageCountersCalls[0]
	want := incrQuotaUsageCountersCall{tenantID: "hot-tenant", storageDelta: 150, fileDelta: 1}
	if got != want {
		t.Fatalf("counter flush = %+v, want %+v (create +100 then overwrite +50)", got, want)
	}
	fm, err := fake.GetFileMeta(context.Background(), "hot-tenant", "f1")
	if err != nil {
		t.Fatalf("get file meta: %v", err)
	}
	if fm.SizeBytes != 150 {
		t.Fatalf("file meta size = %d, want 150 after in-tx create+overwrite", fm.SizeBytes)
	}
	for _, item := range items {
		if got := fake.mutationStatus(item.logID); got != "applied" {
			t.Fatalf("mutation %d status = %q, want applied", item.logID, got)
		}
	}
}

// TestMutationBatchNoOpOverwriteSkipsUpsertAndCounters covers the no-op skip:
// an overwrite whose new size/isMedia already match the shadow row must not
// upsert tenant_file_meta and must produce zero counter deltas — while the
// mutation is still marked applied.
func TestMutationBatchNoOpOverwriteSkipsUpsertAndCounters(t *testing.T) {
	fake := newFakeMetaQuotaStore()
	b := &Dat9Backend{}
	b.SetMetaQuotaStore(context.Background(), "noop-tenant", fake)
	if err := fake.UpsertFileMeta(context.Background(), &FileMetaView{
		TenantID: "noop-tenant", FileID: "f1", SizeBytes: 100, IsMedia: true,
	}); err != nil {
		t.Fatal(err)
	}

	noopOverwrite := func(context.Context, *sql.Tx) (quotaCounterDeltas, error) {
		return applyCentralFileOverwriteTx(fake, nil, "noop-tenant", fileOverwriteMutationData{
			FileID: "f1", OldSizeBytes: 100, OldIsMedia: true, NewSizeBytes: 100, NewIsMedia: true,
		})
	}
	items := []quotaMutationItem{
		newDispatcherTestItem(t, b, fake, noopOverwrite),
		newDispatcherTestItem(t, b, fake, noopOverwrite),
	}

	processMutationBatch(context.Background(), items)

	if fake.upsertFileMetaCalls != 0 {
		t.Fatalf("UpsertFileMetaTx calls = %d, want 0 (identical size/isMedia must skip the upsert)", fake.upsertFileMetaCalls)
	}
	if len(fake.incrQuotaUsageCountersCalls) != 0 {
		t.Fatalf("counter flush calls = %v, want none (no-op mutations produce zero deltas)", fake.incrQuotaUsageCountersCalls)
	}
	for _, item := range items {
		if got := fake.mutationStatus(item.logID); got != "applied" {
			t.Fatalf("mutation %d status = %q, want applied (no-op mutations are still marked)", item.logID, got)
		}
	}
	// The shadow row is untouched.
	fm, err := fake.GetFileMeta(context.Background(), "noop-tenant", "f1")
	if err != nil {
		t.Fatal(err)
	}
	if fm.SizeBytes != 100 || !fm.IsMedia {
		t.Fatalf("file meta = %+v, want unchanged {100 true}", fm)
	}
}

// TestInlineApplyFlushesCountersBeforeMark covers the per-item path
// (dispatcher not running): the apply's counter deltas must be flushed in
// one statement BEFORE MarkMutationAppliedTx, inside the same transaction.
func TestInlineApplyFlushesCountersBeforeMark(t *testing.T) {
	if currentMutationDispatcher() != nil {
		t.Skip("mutation dispatcher is running (leaked by another test)")
	}
	fake := newFakeMetaQuotaStore()
	b := &Dat9Backend{}
	b.SetMetaQuotaStore(context.Background(), "inline-deltas-tenant", fake)

	err := b.logAndEnqueueMutation(context.Background(), "file_create",
		fileCreateMutationData{FileID: "f1", SizeBytes: 64, IsMedia: true},
		quotaPendingDeltas{storageDelta: 64, fileDelta: 1, mediaDelta: 1},
		func(ctx context.Context, tx *sql.Tx) (quotaCounterDeltas, error) {
			return applyCentralFileCreateTx(fake, tx, "inline-deltas-tenant",
				fileCreateMutationData{FileID: "f1", SizeBytes: 64, IsMedia: true})
		})
	if err != nil {
		t.Fatal(err)
	}

	// Inline apply is synchronous: the counter flush must precede the mark.
	if got := fake.counterMarkEvents; len(got) != 2 || got[0] != "counters" || got[1] != "mark" {
		t.Fatalf("counter/mark events = %v, want [counters mark]", got)
	}
	usage, err := fake.GetQuotaUsage(context.Background(), "inline-deltas-tenant")
	if err != nil {
		t.Fatal(err)
	}
	if usage.StorageBytes != 64 || usage.FileCount != 1 || usage.MediaFileCount != 1 {
		t.Fatalf("usage = %+v, want storage=64 file=1 media=1", usage)
	}
	if got := fake.mutations[len(fake.mutations)-1].status; got != "applied" {
		t.Fatalf("mutation status = %q, want applied", got)
	}
}
