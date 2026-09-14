package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testtidb"
	"github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
)

// newExtentReportTarget builds a tenantTarget wired to a real tenant store and
// a real central meta store, the way the worker runs in production.
func newExtentReportTarget(t *testing.T) (*tenantTarget, *datastore.Store, *meta.Store) {
	t.Helper()
	store, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	testtidb.ResetDB(t, store.DB())
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	testtidb.ResetMetaDB(t, metaStore.DB())
	b, err := backend.New(store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)
	b.SetMetaQuotaStore(context.Background(), "extent-report-tenant", tenant.NewMetaQuotaAdapter(metaStore))
	// ResetDB/ResetMetaDB do not clean the central extent counters or the
	// mutation log, and both tests here reuse the same tenant: pin the
	// starting state explicitly instead of inheriting whatever an earlier
	// test (or a previous run of this one) left behind.
	if _, err := metaStore.DB().Exec(
		`DELETE FROM tenant_quota_usage WHERE tenant_id = ?`, "extent-report-tenant",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := metaStore.DB().Exec(
		`DELETE FROM quota_mutation_log WHERE tenant_id = ?`, "extent-report-tenant",
	); err != nil {
		t.Fatal(err)
	}
	return &tenantTarget{tenantID: "extent-report-tenant", store: store, backend: b}, store, metaStore
}

// waitExtentApplied polls the central counters to a deadline. In a full-suite
// run other tests start the process-global mutation dispatcher, which turns
// the extent apply asynchronous; asserting immediately after the report would
// race that lag and flake the default gate.
func waitExtentApplied(t *testing.T, metaStore *meta.Store, tenantID string, wantStorage, wantWatermark int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		usage, err := metaStore.GetQuotaUsage(context.Background(), tenantID)
		if err != nil {
			t.Fatal(err)
		}
		watermark, err := metaStore.GetExtentReportedBytes(context.Background(), tenantID)
		if err != nil {
			t.Fatal(err)
		}
		if usage.StorageBytes == wantStorage && watermark == wantWatermark {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("extent usage did not settle: storage=%d watermark=%d, want %d/%d",
				usage.StorageBytes, watermark, wantStorage, wantWatermark)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func seedExtentBytes(t *testing.T, store *datastore.Store, length int64) {
	t.Helper()
	ctx := context.Background()
	if _, errno, err := store.RunExtentMetaOp(ctx, "mknod", mustJSONWorker(map[string]any{
		"parent": 1, "name": "report.db", "type": 1, "mode": 0644,
		"inode": 601, "proj_path": "/report.db", "uid": 0, "gid": 0,
		"indx": 0,
		"parts": []map[string]any{
			{"off": 0, "slice": map[string]any{"Id": 601, "Size": length, "Off": 0, "Len": length}},
		},
	}), nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
}

func mustJSONWorker(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// One report through the real worker must move the central storage counters by
// exactly the reported range — once, not twice. The apply used to adjust
// storage_bytes inside the watermark CAS and hand the same delta to the apply
// owner for a second flush, double-counting every applied report on the real
// store while the test fakes (watermark-only) hid it.
func TestReportExtentQuotaUsageMovesStorageExactlyOnce(t *testing.T) {
	target, store, metaStore := newExtentReportTarget(t)
	ctx := context.Background()
	seedExtentBytes(t, store, 10)

	total, delta, err := store.PeekExtentUsageDelta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if total != 10 || delta != 10 {
		t.Fatalf("peek total=%d delta=%d, want 10/10", total, delta)
	}
	reportExtentQuotaUsage(ctx, target)
	waitExtentApplied(t, metaStore, target.tenantID, 10, 10)
	if _, delta, err := store.PeekExtentUsageDelta(ctx); err != nil || delta != 0 {
		t.Fatalf("tenant marker delta after the report = %d, %v; want 0, nil", delta, err)
	}

	// A settled pass is a no-op: no new mutations, no counter movement.
	reportExtentQuotaUsage(ctx, target)
	waitExtentApplied(t, metaStore, target.tenantID, 10, 10)
}

// The tenant marker advances when a report is enqueued, while the central
// watermark moves only when the apply lands. When an apply is pending or
// superseded, the marker can read exactly right while the watermark is off the
// tenant total — and the cheap marker filter would then opt out forever for an
// idle tenant. The worker must enter on watermark drift and heal it in either
// direction.
func TestReportExtentQuotaUsageHealsWatermarkDrift(t *testing.T) {
	target, store, metaStore := newExtentReportTarget(t)
	ctx := context.Background()
	seedExtentBytes(t, store, 10)
	reportExtentQuotaUsage(ctx, target)
	waitExtentApplied(t, metaStore, target.tenantID, 10, 10)

	// Shrink the tenant total, and simulate the marker commit of that
	// report's range landing while its apply is still pending: marker = 6,
	// watermark = 10 (stale), total = 6.
	if _, err := store.DB().Exec(`UPDATE jfs_node SET length = 6 WHERE inode = 601`); err != nil {
		t.Fatal(err)
	}
	if err := store.SetExtentUsageReported(ctx, 6); err != nil {
		t.Fatal(err)
	}
	if _, delta, err := store.PeekExtentUsageDelta(ctx); err != nil || delta != 0 {
		t.Fatalf("marker delta before the drift pass = %d, %v; want 0 (the marker is settled)", delta, err)
	}

	// The marker filter alone would return here; the watermark check must not.
	reportExtentQuotaUsage(ctx, target)
	waitExtentApplied(t, metaStore, target.tenantID, 6, 6)
}
