package backend

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// Regression tests for Round A review of PR #251. Every test is named
// TestRoundA_<fix>_<case> so ownership / gating is obvious in CI output.
// Each fix gates on an invariant (not just a happy path).

// --- Fix 1: atomic reserve + insert in a single server-DB transaction ---

// Happy path: quota ok + no existing row → reserved_bytes bumped and
// reservation row is visible. Matches test case (a) in the @adversary-1
// contract.
func TestRoundA_Fix1_ReserveAndInsert_HappyPath(t *testing.T) {
	fake := newFakeMetaQuotaStore()
	fake.config["t1"] = &QuotaConfigView{TenantID: "t1", MaxStorageBytes: 1 << 30}

	err := fake.AtomicReserveAndInsertUpload(context.Background(), &UploadReservationView{
		TenantID:      "t1",
		UploadID:      "u1",
		ReservedBytes: 100,
		TargetPath:    "/a",
		Status:        "active",
		ExpiresAt:     time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("AtomicReserveAndInsertUpload happy path: %v", err)
	}
	u, _ := fake.GetQuotaUsage(context.Background(), "t1")
	if u.ReservedBytes != 100 {
		t.Fatalf("reserved_bytes after reserve+insert = %d, want 100", u.ReservedBytes)
	}
	r, err := fake.GetUploadReservation(context.Background(), "t1", "u1")
	if err != nil {
		t.Fatalf("reservation row missing: %v", err)
	}
	if r.Status != "active" || r.ReservedBytes != 100 {
		t.Fatalf("reservation row = %+v", r)
	}
}

// Invariant: INSERT failure inside the tx must leave reserved_bytes untouched.
// Matches test case (b): "Step2 失败 → reserved_bytes 不动 + 无 reservation 行".
func TestRoundA_Fix1_ReserveAndInsert_InsertFailsLeavesNoLeak(t *testing.T) {
	fake := newFakeMetaQuotaStore()
	fake.config["t1"] = &QuotaConfigView{TenantID: "t1", MaxStorageBytes: 1 << 30}
	fake.insertReservationErr = errors.New("sim: INSERT failed mid-tx")

	err := fake.AtomicReserveAndInsertUpload(context.Background(), &UploadReservationView{
		TenantID:      "t1",
		UploadID:      "u1",
		ReservedBytes: 100,
		TargetPath:    "/a",
		Status:        "active",
		ExpiresAt:     time.Now().Add(time.Hour),
	})
	if err == nil {
		t.Fatal("expected INSERT failure to propagate")
	}
	// CRITICAL: reserved_bytes stays at 0 — no leak.
	u, _ := fake.GetQuotaUsage(context.Background(), "t1")
	if u.ReservedBytes != 0 {
		t.Fatalf("leak: reserved_bytes = %d after failed tx, want 0", u.ReservedBytes)
	}
	if _, err := fake.GetUploadReservation(context.Background(), "t1", "u1"); !errors.Is(err, ErrReservationNotFound) {
		t.Fatalf("reservation row should not exist after rolled-back tx, got err=%v", err)
	}
}

// Invariant: quota exceeded → tx rolls back, reserved_bytes untouched, no row.
// Matches test case (c).
func TestRoundA_Fix1_ReserveAndInsert_QuotaExceededSentinel(t *testing.T) {
	fake := newFakeMetaQuotaStore()
	fake.config["t1"] = &QuotaConfigView{TenantID: "t1", MaxStorageBytes: 50}

	err := fake.AtomicReserveAndInsertUpload(context.Background(), &UploadReservationView{
		TenantID:      "t1",
		UploadID:      "u1",
		ReservedBytes: 100,
		TargetPath:    "/a",
		Status:        "active",
		ExpiresAt:     time.Now().Add(time.Hour),
	})
	if !errors.Is(err, ErrStorageQuotaExceeded) {
		t.Fatalf("expected ErrStorageQuotaExceeded, got %v", err)
	}
	u, _ := fake.GetQuotaUsage(context.Background(), "t1")
	if u.ReservedBytes != 0 {
		t.Fatalf("quota-exceeded path bumped reserved_bytes to %d", u.ReservedBytes)
	}
	if _, err := fake.GetUploadReservation(context.Background(), "t1", "u1"); !errors.Is(err, ErrReservationNotFound) {
		t.Fatalf("quota-exceeded path should not create reservation row, got %v", err)
	}
}

// Invariant: duplicate (tenant_id, upload_id) returns the sentinel and the
// caller MUST NOT bump reserved_bytes a second time. Matches test case (d)
// added by @adversary-1.
func TestRoundA_Fix1_ReserveAndInsert_DuplicateIsSentinelIdempotent(t *testing.T) {
	fake := newFakeMetaQuotaStore()
	fake.config["t1"] = &QuotaConfigView{TenantID: "t1", MaxStorageBytes: 1 << 30}

	first := &UploadReservationView{
		TenantID: "t1", UploadID: "u1", ReservedBytes: 100,
		TargetPath: "/a", Status: "active", ExpiresAt: time.Now().Add(time.Hour),
	}
	if err := fake.AtomicReserveAndInsertUpload(context.Background(), first); err != nil {
		t.Fatalf("first reserve: %v", err)
	}
	before, _ := fake.GetQuotaUsage(context.Background(), "t1")

	// Retry with the same upload_id.
	err := fake.AtomicReserveAndInsertUpload(context.Background(), first)
	if !errors.Is(err, ErrReservationAlreadyExists) {
		t.Fatalf("duplicate insert: expected ErrReservationAlreadyExists, got %v", err)
	}
	after, _ := fake.GetQuotaUsage(context.Background(), "t1")
	if after.ReservedBytes != before.ReservedBytes {
		t.Fatalf("duplicate retry bumped reserved_bytes %d → %d (must be idempotent)",
			before.ReservedBytes, after.ReservedBytes)
	}
}

// End-to-end invariant: the backend-level reserveUploadOnServer maps each case
// correctly. Verifies Round A contract at the caller layer.
func TestRoundA_Fix1_ReserveUploadOnServer_NoLeakOnInsertFailure(t *testing.T) {
	b, fake := newCentralQuotaBackend(t)
	fake.insertReservationErr = errors.New("sim: INSERT failed mid-tx")

	reserved, err := b.reserveUploadOnServer(context.Background(), "u1", "/a", 100)
	// Non-quota errors are fail-open: (false, nil).
	if reserved || err != nil {
		t.Fatalf("fail-open path: reserved=%v err=%v, want (false, nil)", reserved, err)
	}
	u, _ := fake.GetQuotaUsage(context.Background(), "tenant-a")
	if u.ReservedBytes != 0 {
		t.Fatalf("leak: reserved_bytes = %d after failed tx, want 0", u.ReservedBytes)
	}
}

func TestRoundA_Fix1_ReserveUploadOnServer_DuplicateIsIdempotent(t *testing.T) {
	b, fake := newCentralQuotaBackend(t)

	if _, err := b.reserveUploadOnServer(context.Background(), "u1", "/a", 100); err != nil {
		t.Fatalf("first reserve: %v", err)
	}
	fake.mu.Lock()
	mutationsBefore := len(fake.mutations)
	reservationsBefore := len(fake.reservations)
	fake.mu.Unlock()
	before, _ := fake.GetQuotaUsage(context.Background(), "tenant-a")

	reserved, err := b.reserveUploadOnServer(context.Background(), "u1", "/a", 100)
	if !reserved || err != nil {
		t.Fatalf("duplicate retry: reserved=%v err=%v, want (true, nil)", reserved, err)
	}
	after, _ := fake.GetQuotaUsage(context.Background(), "tenant-a")
	if after.ReservedBytes != before.ReservedBytes {
		t.Fatalf("duplicate retry double-bumped reserved_bytes: %d → %d",
			before.ReservedBytes, after.ReservedBytes)
	}
	// @adversary pre-push self-check #1: the idempotent (true, nil) path must
	// NOT produce a second reservation row and must NOT emit a mutation log
	// entry. It is a pure "already have it" return, not a hidden error-swallow.
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.mutations) != mutationsBefore {
		t.Fatalf("duplicate retry wrote mutation log entries: %d → %d",
			mutationsBefore, len(fake.mutations))
	}
	if len(fake.reservations) != reservationsBefore {
		t.Fatalf("duplicate retry produced a second reservation side-effect: %d → %d",
			reservationsBefore, len(fake.reservations))
	}
}

// --- Fix 2: IncrMutationRetry has WHERE status='pending' guard ---

// Invariant: calling IncrMutationRetry on an 'applied' row must NOT bump
// retry_count and must NOT flip status back to 'pending'.
func TestRoundA_Fix2_IncrMutationRetry_RefusesToBumpAppliedRow(t *testing.T) {
	fake := newFakeMetaQuotaStore()
	id, err := fake.InsertMutationLog(context.Background(), &MutationLogView{
		TenantID:     "t1",
		MutationType: "file_create",
		MutationData: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	// Settle the row.
	if err := fake.MarkMutationAppliedTx(nil, id); err != nil {
		t.Fatalf("mark applied: %v", err)
	}

	// Simulate a misbehaving call (test fake, backfill CLI, future refactor).
	if err := fake.IncrMutationRetry(context.Background(), id, 5); err != nil {
		t.Fatalf("incr retry on applied: %v", err)
	}
	var got fakeMutationRecord
	fake.mu.Lock()
	got = fake.mutations[0]
	fake.mu.Unlock()
	if got.status != "applied" {
		t.Fatalf("applied row flipped to %q", got.status)
	}
	if got.retryCount != 0 {
		t.Fatalf("retry_count bumped on applied row: %d", got.retryCount)
	}
}

// Invariant: calling IncrMutationRetry on a 'failed' row must NOT bump
// retry_count and must NOT revive the DLQ row to 'pending'.
func TestRoundA_Fix2_IncrMutationRetry_RefusesToBumpFailedRow(t *testing.T) {
	fake := newFakeMetaQuotaStore()
	id, _ := fake.InsertMutationLog(context.Background(), &MutationLogView{
		TenantID:     "t1",
		MutationType: "file_create",
		MutationData: json.RawMessage(`{}`),
	})
	// Bump to failed.
	fake.mu.Lock()
	fake.mutations[0].status = "failed"
	fake.mutations[0].retryCount = 5
	fake.mu.Unlock()

	if err := fake.IncrMutationRetry(context.Background(), id, 5); err != nil {
		t.Fatalf("incr retry on failed: %v", err)
	}
	fake.mu.Lock()
	got := fake.mutations[0]
	fake.mu.Unlock()
	if got.status != "failed" {
		t.Fatalf("failed DLQ row flipped to %q", got.status)
	}
	if got.retryCount != 5 {
		t.Fatalf("retry_count bumped on failed row: %d", got.retryCount)
	}
}

// Happy path: pending row still bumps normally.
func TestRoundA_Fix2_IncrMutationRetry_PendingRowStillBumps(t *testing.T) {
	fake := newFakeMetaQuotaStore()
	id, _ := fake.InsertMutationLog(context.Background(), &MutationLogView{
		TenantID:     "t1",
		MutationType: "file_create",
		MutationData: json.RawMessage(`{}`),
	})
	if err := fake.IncrMutationRetry(context.Background(), id, 5); err != nil {
		t.Fatalf("incr retry: %v", err)
	}
	fake.mu.Lock()
	got := fake.mutations[0]
	fake.mu.Unlock()
	if got.status != "pending" || got.retryCount != 1 {
		t.Fatalf("pending row after retry = %+v, want status=pending retry=1", got)
	}
}

// --- Fix 3: replay worker per-tenant failure barrier ---

// Invariant: once any entry for tenant T fails in the batch, all subsequent
// entries for T are skipped in this batch. Other tenants proceed.
func TestRoundA_Fix3_ReplayWorker_PerTenantFailureBarrier(t *testing.T) {
	fake := newFakeMetaQuotaStore()

	// Tenant A: entry 1 will fail (invalid JSON → applyMutation errors),
	// entry 2 must be SKIPPED due to the barrier.
	// Tenant B: a single good entry must still apply.
	//
	// Construction: insert in ListPendingMutations order (tenant_id, id).
	// Tenant IDs are compared lexicographically so "tenant-a" sorts before
	// "tenant-b".
	fake.mutations = []fakeMutationRecord{
		{tenantID: "tenant-a", id: 1, typ: "file_create", status: "pending", data: []byte("{not-json")},
		{tenantID: "tenant-a", id: 2, typ: "file_create", status: "pending", data: []byte(`{"file_id":"f2","size_bytes":0,"is_media":false}`)},
		{tenantID: "tenant-b", id: 3, typ: "file_create", status: "pending", data: []byte(`{"file_id":"f3","size_bytes":0,"is_media":false}`)},
	}
	fake.nextID = 4

	w := &MutationReplayWorker{store: fake}
	w.replayBatch(context.Background())

	fake.mu.Lock()
	defer fake.mu.Unlock()

	// Entry 1 (tenant-a, bad JSON): retry bumped, status still pending.
	if fake.mutations[0].retryCount != 1 {
		t.Fatalf("entry 1 retry_count = %d, want 1", fake.mutations[0].retryCount)
	}
	// Entry 2 (tenant-a): MUST be skipped — no apply, no retry bump.
	if fake.mutations[1].status != "pending" || fake.mutations[1].retryCount != 0 {
		t.Fatalf("entry 2 was NOT skipped by per-tenant barrier: status=%q retry=%d",
			fake.mutations[1].status, fake.mutations[1].retryCount)
	}
	// Entry 3 (tenant-b): independent tenant, must have applied.
	if fake.mutations[2].status != "applied" {
		t.Fatalf("entry 3 (other tenant) did not apply: status=%q", fake.mutations[2].status)
	}
}

// Invariant sanity check: without the barrier contract, a per-tenant
// create-then-delete sequence where create fails would run delete on a
// nonexistent row. The barrier prevents that.
func TestRoundA_Fix3_ReplayWorker_CreateFailedDoesNotRunDelete(t *testing.T) {
	fake := newFakeMetaQuotaStore()
	fake.mutations = []fakeMutationRecord{
		{tenantID: "tenant-a", id: 1, typ: "file_create", status: "pending", data: []byte("{bad")},
		{tenantID: "tenant-a", id: 2, typ: "file_delete", status: "pending", data: []byte(`{"file_id":"f","size_bytes":0,"is_media":false}`)},
	}
	fake.nextID = 3

	w := &MutationReplayWorker{store: fake}
	w.replayBatch(context.Background())

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.mutations[1].status != "pending" {
		t.Fatalf("delete ran out-of-order after failed create: status=%q", fake.mutations[1].status)
	}
}

// Invariant: the per-tenant failure barrier is batch-LOCAL. It must NOT
// persist across replayBatch invocations — otherwise a single transient
// failure would permanently wedge a tenant's replay queue. Requested by
// @adversary-1 to lock down the semantics of the `blockedTenants` map.
//
// Scenario:
//  1. Batch #1: tenant-a entry 1 fails (bad JSON) → entry 2 skipped.
//  2. Between batches, repair entry 1's payload (simulating a fix / manual
//     intervention / transient DB recovery).
//  3. Batch #2: entry 1 now applies cleanly; entry 2 must ALSO apply in the
//     same batch — proving the barrier did not carry over from batch #1.
func TestRoundA_Fix3_ReplayWorker_PerTenantBarrierClearsNextBatch(t *testing.T) {
	fake := newFakeMetaQuotaStore()
	fake.mutations = []fakeMutationRecord{
		{tenantID: "tenant-a", id: 1, typ: "file_create", status: "pending", data: []byte("{not-json")},
		{tenantID: "tenant-a", id: 2, typ: "file_create", status: "pending", data: []byte(`{"file_id":"f2","size_bytes":0,"is_media":false}`)},
	}
	fake.nextID = 3

	w := &MutationReplayWorker{store: fake}
	w.replayBatch(context.Background())

	// Batch #1 post-conditions: entry 1 failed once, entry 2 untouched.
	fake.mu.Lock()
	if fake.mutations[0].retryCount != 1 || fake.mutations[0].status != "pending" {
		fake.mu.Unlock()
		t.Fatalf("batch #1 entry 1 = {status=%q retry=%d}, want {pending, 1}",
			fake.mutations[0].status, fake.mutations[0].retryCount)
	}
	if fake.mutations[1].status != "pending" || fake.mutations[1].retryCount != 0 {
		fake.mu.Unlock()
		t.Fatalf("batch #1 entry 2 was NOT skipped: status=%q retry=%d",
			fake.mutations[1].status, fake.mutations[1].retryCount)
	}
	// Repair entry 1's payload so it will apply cleanly on the next batch.
	fake.mutations[0].data = []byte(`{"file_id":"f1","size_bytes":0,"is_media":false}`)
	fake.mu.Unlock()

	// Batch #2: run a fresh batch. If the barrier were worker-persistent,
	// tenant-a would still be blocked and entry 1 could never apply.
	w.replayBatch(context.Background())

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.mutations[0].status != "applied" {
		t.Fatalf("batch #2 entry 1 did not apply after repair: status=%q", fake.mutations[0].status)
	}
	// The critical assertion: entry 2 must ALSO apply in the same batch,
	// proving the barrier cleared between batches (not worker-persistent).
	if fake.mutations[1].status != "applied" {
		t.Fatalf("batch #2 entry 2 was still skipped — barrier leaked across batches: status=%q",
			fake.mutations[1].status)
	}
}

// --- Fix 4: completeUploadReservation mutation-first on transient lookup ---

// Invariant: even when the reservation lookup would transient-fail, the
// upload_complete mutation is still written to the log (durable outbox).
// storage_bytes / file_meta / media_count deltas are not silently dropped.
func TestRoundA_Fix4_CompleteUpload_WritesMutationEvenOnTransientPath(t *testing.T) {
	b, fake := newCentralQuotaBackend(t)
	// Seed a prior reservation claim for realism.
	_, _ = b.reserveUploadOnServer(context.Background(), "u1", "/clip.wav", 50)
	// Also inject a transient error on any future GetUploadReservation call
	// to prove the apply path does NOT depend on pre-lookup anymore.
	fake.getReservationErr = errors.New("sim: transient DB error")

	b.completeUploadReservation(context.Background(),
		"u1", /*reservedBytes*/ 50, "file-1",
		/*oldSize*/ 0, /*oldMedia*/ false,
		/*newSize*/ 50, /*newMedia*/ true)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.mutations) == 0 {
		t.Fatal("no mutation log entry written under transient lookup path")
	}
	last := fake.mutations[len(fake.mutations)-1]
	if last.typ != "upload_complete" {
		t.Fatalf("last mutation type = %q, want upload_complete", last.typ)
	}
	// The apply itself may land as 'pending' (because fake's GetUploadReservation
	// is not used on the apply path any more). The key invariant is: the log
	// entry EXISTS — replay worker can finish it.
	if last.status != "applied" && last.status != "pending" {
		t.Fatalf("unexpected mutation status after transient path: %q", last.status)
	}
}

// Invariant: when there is NO reservation row at apply time, the apply tx
// still advances storage_bytes and file_meta for the confirmed file. This
// covers two sub-cases of the settled=false branch in applyUploadCompleteTx:
//   - fail-open initiate (no reservation row was ever written)
//   - already-settled terminal row (row exists but status != "active")
//
// The prior code silently wrote 0 here.
func TestRoundA_Fix4_CompleteUpload_FailOpenInitiateStillAppliesStorage(t *testing.T) {
	b, fake := newCentralQuotaBackend(t)
	// No prior reserve → no reservation row. Simulate fail-open initiate.
	b.completeUploadReservation(context.Background(),
		"u1", /*reservedBytes*/ 0, "file-1",
		/*oldSize*/ 0, /*oldMedia*/ false,
		/*newSize*/ 40, /*newMedia*/ false)

	u, _ := fake.GetQuotaUsage(context.Background(), "tenant-a")
	if u.StorageBytes != 40 {
		t.Fatalf("storage_bytes after fail-open complete = %d, want 40", u.StorageBytes)
	}
	fm, err := fake.GetFileMeta(context.Background(), "tenant-a", "file-1")
	if err != nil {
		t.Fatalf("file meta missing after fail-open complete: %v", err)
	}
	if fm.SizeBytes != 40 {
		t.Fatalf("file meta size_bytes = %d, want 40", fm.SizeBytes)
	}
}

// Invariant: apply-time race between expiry sweep and complete. An upload
// successfully reserves on the server; the expiry sweep (or any concurrent
// actor) removes the reservation row BEFORE the apply/replay tx runs. The
// apply path must fall through settled=false and still advance
// storage_bytes / file_meta for the confirmed bytes, never panicking or
// dropping the mutation. Requested by @adversary-1 to cover the apply-side
// sweep race distinct from the initiate-side fail-open.
func TestRoundA_Fix4_CompleteUpload_ApplyAfterReservationSwept(t *testing.T) {
	b, fake := newCentralQuotaBackend(t)
	ctx := context.Background()

	// 1. Successfully reserve on the server (normal initiate).
	reserved, err := b.reserveUploadOnServer(ctx, "u1", "/clip.wav", 50)
	if err != nil || !reserved {
		t.Fatalf("reserve setup: reserved=%v err=%v", reserved, err)
	}
	before, _ := fake.GetQuotaUsage(ctx, "tenant-a")
	if before.ReservedBytes != 50 {
		t.Fatalf("precondition: reserved_bytes = %d, want 50", before.ReservedBytes)
	}

	// 2. Simulate expiry sweep / external cleanup removing the reservation
	// row between initiate and complete. We bypass the normal sweep to
	// isolate the apply-path branch: we want a scenario where at apply time
	// SettleActiveReservationTx returns settled=false because no active row
	// exists. Also roll back the reserved_bytes the sweep would release so
	// the fake state reflects a real post-sweep world.
	fake.mu.Lock()
	delete(fake.reservations, metaKey("tenant-a", "u1"))
	fake.usage["tenant-a"].ReservedBytes -= 50
	fake.mu.Unlock()

	// 3. Drive completeUploadReservation with the same reservedBytes the
	// initiate claimed. The apply tx should see settled=false and therefore
	// skip the reserved→storage transfer (reserved_bytes already released by
	// the sweep) but still advance storage_bytes + file_meta for the
	// confirmed bytes.
	b.completeUploadReservation(ctx,
		"u1", /*reservedBytes*/ 50, "file-1",
		/*oldSize*/ 0, /*oldMedia*/ false,
		/*newSize*/ 50, /*newMedia*/ true)

	after, _ := fake.GetQuotaUsage(ctx, "tenant-a")
	if after.ReservedBytes != 0 {
		t.Fatalf("apply-after-sweep leaked reserved_bytes: %d", after.ReservedBytes)
	}
	if after.StorageBytes != 50 {
		t.Fatalf("apply-after-sweep storage_bytes = %d, want 50", after.StorageBytes)
	}
	if after.MediaFileCount != 1 {
		t.Fatalf("apply-after-sweep media_file_count = %d, want 1", after.MediaFileCount)
	}
	fm, err := fake.GetFileMeta(ctx, "tenant-a", "file-1")
	if err != nil {
		t.Fatalf("file meta missing after apply-after-sweep: %v", err)
	}
	if fm.SizeBytes != 50 || !fm.IsMedia {
		t.Fatalf("file meta after apply-after-sweep = %+v", fm)
	}

	// Invariant: the upload_complete mutation log entry is durable.
	fake.mu.Lock()
	defer fake.mu.Unlock()
	last := fake.mutations[len(fake.mutations)-1]
	if last.typ != "upload_complete" {
		t.Fatalf("last mutation type = %q, want upload_complete", last.typ)
	}
	if last.status != "applied" {
		t.Fatalf("apply-after-sweep mutation status = %q, want applied", last.status)
	}
}
