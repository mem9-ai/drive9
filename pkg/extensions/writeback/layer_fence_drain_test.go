package writeback

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

func TestSelfFencedDrainReportsClosedPendingWrites(t *testing.T) {
	ledger, err := staging.NewStaging(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledger.Close)
	q := &CommitQueue{index: ledger.Pending, maxPending: 4, workCh: make(chan *CommitEntry, 4), zeroTruncateDelay: time.Hour}
	w := &Writeback{opts: Options{Staging: ledger}, queue: q}
	for _, path := range []string{"/queued", "/delayed"} {
		gen, err := ledger.Pending.Put(path, 0, staging.PendingOverwrite)
		if err != nil {
			t.Fatal(err)
		}
		entry := &CommitEntry{Path: path, Kind: staging.PendingOverwrite, BaseRev: 2, PendingIndexGen: gen, CoalesceZeroTruncate: path == "/delayed"}
		if err := q.Enqueue(entry); err != nil {
			t.Fatal(err)
		}
	}
	if len(q.delayed) != 1 {
		t.Fatalf("delayed entries = %d", len(q.delayed))
	}
	w.AbandonLayerForRollback()
	w.AbandonLayerForRollback()
	if err := w.Drain(context.Background()); !errors.Is(err, errLayerRolledBack) {
		t.Fatalf("drain = %v, want a fenced write error", err)
	}
	if _, n := w.DrainPending(); n != 2 {
		t.Fatalf("undrained writes = %d, want 2", n)
	}
	for _, path := range []string{"/queued", "/delayed"} {
		meta, ok := ledger.Pending.GetMeta(path)
		if !ok || meta.Kind != staging.PendingConflict {
			t.Fatalf("%s missing durable conflict: %+v", path, meta)
		}
		ledger.Pending.Remove(path)
	}
	if err := w.Drain(context.Background()); err != nil {
		t.Fatalf("clean fenced mount: %v", err)
	}
}

func TestFencedUploaderRetainsItsCachedImage(t *testing.T) {
	cache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	u := NewWriteBackUploader(nil, cache, 1)
	w := &Writeback{uploader: u, wbCache: cache}
	t.Cleanup(u.DrainAll)
	if err := cache.Put("/closed", []byte("save"), 4, staging.PendingNew); err != nil {
		t.Fatal(err)
	}
	w.AbandonLayerForRollback()
	u.Submit("/closed")
	if _, err := u.UploadSyncWithRevision(context.Background(), "/closed"); !errors.Is(err, errLayerRolledBack) {
		t.Fatalf("fenced sync = %v", err)
	}
	if err := w.Drain(context.Background()); !errors.Is(err, errLayerRolledBack) {
		t.Fatalf("fenced drain = %v", err)
	}
	if data, ok := cache.Get("/closed"); !ok || string(data) != "save" {
		t.Fatalf("recovery image = %q", data)
	}
}
