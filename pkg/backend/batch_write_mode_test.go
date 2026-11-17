package backend

import (
	"context"
	"errors"
	"testing"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/drive9/pkg/datastore"
)

// A batch item that carries permission bits is a chmod by another name, and chmod is
// owner-only. The backend refuses such an item unless the caller marked the context,
// so a handler that forgets the authorization check cannot reopen the escalation.
func TestBatchWriteModeChangeRequiresContextMark(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()

	if _, _, err := b.WriteCtxIfRevisionWithTagsResult(ctx, "/f.txt", []byte("old"), 0,
		filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, -1, nil, ""); err != nil {
		t.Fatal(err)
	}
	stat, err := b.StatNodeCtx(ctx, "/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	before := stat.Mode & 0o777

	items := []BatchWriteItem{{Path: "/f.txt", Data: []byte("new"), ExpectedRevision: 1, Mode: 0o777, HasMode: true}}
	results, err := b.BatchWriteCtx(ctx, items)
	if err != nil {
		t.Fatalf("batch write: %v", err)
	}
	if len(results) != 1 || !errors.Is(results[0].Err, ErrBatchWriteModeDenied) {
		t.Fatalf("unmarked mode item result = %+v, want ErrBatchWriteModeDenied", results)
	}
	// Nothing was applied: neither the bits nor the content of a denied item.
	stat, err = b.StatNodeCtx(ctx, "/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	if stat.Mode&0o777 != before {
		t.Fatalf("mode = %o, want unchanged %o", stat.Mode&0o777, before)
	}

	// A create carrying bits is refused the same way, and no file appears.
	created := []BatchWriteItem{{Path: "/new.txt", Data: []byte("x"), Mode: 0o600, HasMode: true}}
	results, err = b.BatchWriteCtx(ctx, created)
	if err != nil {
		t.Fatalf("batch write create: %v", err)
	}
	if !errors.Is(results[0].Err, ErrBatchWriteModeDenied) {
		t.Fatalf("unmarked create result = %+v, want ErrBatchWriteModeDenied", results[0])
	}
	if _, err := b.StatNodeCtx(ctx, "/new.txt"); !errors.Is(err, datastore.ErrNotFound) {
		t.Fatalf("denied create left a file behind: err=%v", err)
	}

	// With the owner mark the same item is applied.
	ownerCtx := WithBatchWriteModeChanges(ctx)
	results, err = b.BatchWriteCtx(ownerCtx, items)
	if err != nil {
		t.Fatalf("owner batch write: %v", err)
	}
	if results[0].Err != nil {
		t.Fatalf("owner batch write result = %+v", results[0])
	}
	stat, err = b.StatNodeCtx(ctx, "/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	if stat.Mode&0o777 != 0o777 {
		t.Fatalf("owner mode = %o, want 777", stat.Mode&0o777)
	}
}
