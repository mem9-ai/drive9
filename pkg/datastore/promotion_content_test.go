package datastore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/promotion"
)

func createPromotionImportForContent(t *testing.T, entries []promotion.ManifestEntry, idempotencyKey string) (*Store, *PromotionStore, *PromotionImport, []promotion.ManifestEntry) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Millisecond)
	store, promotionStore, _, _ := newTestPromotionStore(t, &now)
	manifest, err := promotion.CanonicalizeManifest(entries, testPromotionLimits())
	if err != nil {
		t.Fatal(err)
	}
	plan, err := promotionStore.PlanImport(context.Background(), promotion.PlanImportRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest.Entries,
	})
	if err != nil {
		t.Fatal(err)
	}
	allocation, err := promotionStore.AllocateImportID(context.Background(), PromotionAllocationRequest{
		TenantID: "tenant-a", Target: "/published", ExpectedTargetAbsent: true,
		IdempotencyKey: idempotencyKey, AllocationLease: "allocation-lease",
	})
	if err != nil {
		t.Fatal(err)
	}
	created, err := promotionStore.CreateImport(context.Background(), PromotionCreateRequest{
		TenantID: "tenant-a", MigrationID: allocation.MigrationID, AllocationProof: allocation.AllocationProof,
		Target: "/published", ExpectedTargetAbsent: true, Manifest: manifest.Entries, Plan: *plan,
		OwnerToken: "owner-token", RecoveryToken: "recovery-token", WriterLease: "writer-lease",
	})
	if err != nil {
		t.Fatal(err)
	}
	return store, promotionStore, created, manifest.Entries
}

func inlineContentRequest(created *PromotionImport, entry promotion.ManifestEntry, key string, body io.Reader) PromotionInlineContentRequest {
	return PromotionInlineContentRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID, OwnerEpoch: created.OwnerEpoch,
		OwnerToken: "owner-token", WriterLease: "writer-lease", RelativePath: entry.RelativePath,
		EntryHash: entry.EntryHash, SizeBytes: entry.ExpectedSizeBytes,
		ChecksumSHA256: entry.ExpectedChecksumSHA256, IdempotencyKey: key, Body: body,
	}
}

func TestPromotionInlineContentAndVerifyRoundTrip(t *testing.T) {
	store, promotionStore, created, manifest := createPromotionImportForContent(t, []promotion.ManifestEntry{
		{RelativePath: "dir", Type: promotion.EntryTypeDirectory, Mode: 0o755, MtimeNS: 1},
		{RelativePath: "dir/empty", Type: promotion.EntryTypeFile, Mode: 0o644, MtimeNS: 2, ExpectedChecksumSHA256: promotionTestHash("")},
		{RelativePath: "dir/file", Type: promotion.EntryTypeFile, Mode: 0o640, MtimeNS: 3, ExpectedSizeBytes: 5, ExpectedChecksumSHA256: promotionTestHash("hello")},
		{RelativePath: "link", Type: promotion.EntryTypeSymlink, Mode: 0o777, MtimeNS: 4, SymlinkTarget: "dir/file"},
	}, "allocate-content-round-trip")

	empty, err := promotionStore.PutInlineImportContent(context.Background(), inlineContentRequest(created, manifest[1], "put-empty", bytes.NewReader(nil)))
	if err != nil {
		t.Fatalf("PutInlineImportContent(empty): %v", err)
	}
	if empty.State != "STAGING" || empty.StateVersion != 2 || empty.SizeBytes != 0 {
		t.Fatalf("empty content result = %+v", empty)
	}
	nonemptyReq := inlineContentRequest(created, manifest[2], "put-file", bytes.NewBufferString("hello"))
	nonempty, err := promotionStore.PutInlineImportContent(context.Background(), nonemptyReq)
	if err != nil {
		t.Fatalf("PutInlineImportContent(file): %v", err)
	}
	if nonempty.State != "STAGING" || nonempty.StateVersion != 2 || nonempty.ContentID == empty.ContentID {
		t.Fatalf("nonempty content result = %+v, empty = %+v", nonempty, empty)
	}

	// Lost-response retry returns the same immutable identity without another
	// row, binding, or state-version increment.
	nonemptyReq.Body = bytes.NewBufferString("hello")
	retried, err := promotionStore.PutInlineImportContent(context.Background(), nonemptyReq)
	if err != nil {
		t.Fatalf("PutInlineImportContent(retry): %v", err)
	}
	if retried.ContentID != nonempty.ContentID || retried.StateVersion != 2 {
		t.Fatalf("retried content = %+v, want content %q at version 2", retried, nonempty.ContentID)
	}
	var contentRows, boundRows int
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM promotion_import_contents
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&contentRows); err != nil {
		t.Fatal(err)
	}
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM promotion_import_entries
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?
		  AND import_content_id IS NOT NULL`, created.AllocationEpoch, created.AllocationSequence).Scan(&boundRows); err != nil {
		t.Fatal(err)
	}
	if contentRows != 2 || boundRows != 2 {
		t.Fatalf("content/bound rows = %d/%d, want 2/2", contentRows, boundRows)
	}

	verified, err := promotionStore.VerifyImport(context.Background(), PromotionVerifyRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID, OwnerEpoch: created.OwnerEpoch,
		OwnerToken: "owner-token", WriterLease: "writer-lease", ManifestHash: created.ManifestHash,
	})
	if err != nil {
		t.Fatalf("VerifyImport: %v", err)
	}
	if verified.State != "VERIFIED" || verified.StateVersion != 3 {
		t.Fatalf("verified import = %+v", verified)
	}
	nonemptyReq.Body = bytes.NewBufferString("hello")
	recoveredContent, err := promotionStore.PutInlineImportContent(context.Background(), nonemptyReq)
	if err != nil {
		t.Fatalf("PutInlineImportContent retry after verify: %v", err)
	}
	if recoveredContent.ContentID != nonempty.ContentID || recoveredContent.State != "VERIFIED" || recoveredContent.StateVersion != 3 {
		t.Fatalf("content retry after verify = %+v", recoveredContent)
	}
	retriedVerify, err := promotionStore.VerifyImport(context.Background(), PromotionVerifyRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID, OwnerEpoch: created.OwnerEpoch,
		OwnerToken: "owner-token", WriterLease: "writer-lease", ManifestHash: created.ManifestHash,
	})
	if err != nil || retriedVerify.StateVersion != verified.StateVersion {
		t.Fatalf("VerifyImport retry = %+v, %v", retriedVerify, err)
	}
	late := inlineContentRequest(created, manifest[2], "late-put", bytes.NewBufferString("hello"))
	if _, err := promotionStore.PutInlineImportContent(context.Background(), late); !errors.Is(err, ErrPromotionConflict) {
		t.Fatalf("late PutInlineImportContent error = %v, want ErrPromotionConflict", err)
	}
}

func TestPromotionMetadataOnlyVerifyDirectlyFromCreated(t *testing.T) {
	tests := []struct {
		name    string
		entries []promotion.ManifestEntry
	}{
		{name: "empty"},
		{name: "directory", entries: []promotion.ManifestEntry{{RelativePath: "empty", Type: promotion.EntryTypeDirectory, Mode: 0o755}}},
		{name: "symlink", entries: []promotion.ManifestEntry{{RelativePath: "link", Type: promotion.EntryTypeSymlink, Mode: 0o777, SymlinkTarget: "target"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, promotionStore, created, _ := createPromotionImportForContent(t, test.entries, "allocate-verify-"+test.name)
			verified, err := promotionStore.VerifyImport(context.Background(), PromotionVerifyRequest{
				TenantID: "tenant-a", MigrationID: created.MigrationID, OwnerEpoch: created.OwnerEpoch,
				OwnerToken: "owner-token", WriterLease: "writer-lease", ManifestHash: created.ManifestHash,
			})
			if err != nil {
				t.Fatalf("VerifyImport metadata-only: %v", err)
			}
			if verified.State != "VERIFIED" || verified.StateVersion != 2 {
				t.Fatalf("verified import = %+v, want VERIFIED version 2", verified)
			}
		})
	}
}

type promotionCountingReader struct {
	reads int
	body  io.Reader
}

type promotionBlockingReader struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	body    io.Reader
}

func (r *promotionBlockingReader) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.started) })
	<-r.release
	return r.body.Read(p)
}

func (r *promotionCountingReader) Read(p []byte) (int, error) {
	r.reads++
	return r.body.Read(p)
}

func TestPromotionInlineContentRejectsTupleBeforeReadingOrMutation(t *testing.T) {
	store, promotionStore, created, manifest := createPromotionImportForContent(t, []promotion.ManifestEntry{
		{RelativePath: "file", Type: promotion.EntryTypeFile, Mode: 0o644, ExpectedSizeBytes: 5, ExpectedChecksumSHA256: promotionTestHash("hello")},
	}, "allocate-content-tuple")

	reader := &promotionCountingReader{body: bytes.NewBufferString("hello")}
	req := inlineContentRequest(created, manifest[0], "put-mismatch", reader)
	req.EntryHash = promotionTestHash("different-entry")
	if _, err := promotionStore.PutInlineImportContent(context.Background(), req); !errors.Is(err, ErrPromotionConflict) {
		t.Fatalf("tuple mismatch error = %v, want ErrPromotionConflict", err)
	}
	if reader.reads != 0 {
		t.Fatalf("body reads before tuple rejection = %d, want 0", reader.reads)
	}
	var contents int
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM promotion_import_contents`).Scan(&contents); err != nil {
		t.Fatal(err)
	}
	if contents != 0 {
		t.Fatalf("content rows after rejected tuple = %d, want 0", contents)
	}

	badBody := inlineContentRequest(created, manifest[0], "put-bad-body", bytes.NewBufferString("helloo"))
	if _, err := promotionStore.PutInlineImportContent(context.Background(), badBody); !errors.Is(err, ErrPromotionConflict) {
		t.Fatalf("oversized body error = %v, want ErrPromotionConflict", err)
	}
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM promotion_import_contents`).Scan(&contents); err != nil {
		t.Fatal(err)
	}
	if contents != 0 {
		t.Fatalf("content rows after rejected body = %d, want 0", contents)
	}
}

func TestPromotionInlineContentIdempotencyAndEntryBindingAreSingleWinner(t *testing.T) {
	store, promotionStore, created, manifest := createPromotionImportForContent(t, []promotion.ManifestEntry{
		{RelativePath: "file", Type: promotion.EntryTypeFile, Mode: 0o644, ExpectedSizeBytes: 5, ExpectedChecksumSHA256: promotionTestHash("hello")},
	}, "allocate-content-race")
	req := inlineContentRequest(created, manifest[0], "put-race", bytes.NewBufferString("hello"))
	type result struct {
		content *PromotionInlineContent
		err     error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			local := req
			local.Body = bytes.NewBufferString("hello")
			content, err := promotionStore.PutInlineImportContent(context.Background(), local)
			results <- result{content: content, err: err}
		}()
	}
	close(start)
	first, second := <-results, <-results
	if first.err != nil || second.err != nil {
		t.Fatalf("concurrent identical content errors = %v, %v", first.err, second.err)
	}
	if first.content.ContentID != second.content.ContentID || first.content.StateVersion != 2 || second.content.StateVersion != 2 {
		t.Fatalf("concurrent identical results = %+v, %+v", first.content, second.content)
	}
	var contents int
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM promotion_import_contents`).Scan(&contents); err != nil {
		t.Fatal(err)
	}
	if contents != 1 {
		t.Fatalf("content rows after identical race = %d, want 1", contents)
	}

	changedKey := inlineContentRequest(created, manifest[0], "put-second-key", bytes.NewBufferString("hello"))
	if _, err := promotionStore.PutInlineImportContent(context.Background(), changedKey); !errors.Is(err, ErrPromotionConflict) {
		t.Fatalf("second key for bound entry error = %v, want ErrPromotionConflict", err)
	}
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM promotion_import_contents`).Scan(&contents); err != nil {
		t.Fatal(err)
	}
	if contents != 1 {
		t.Fatalf("content rows after losing binding race = %d, want 1", contents)
	}

	// A takeover changes the fencing owner but not an already accepted
	// immutable content request. The new owner can recover a lost response by
	// the same content idempotency key instead of allocating a second row.
	if _, err := store.DB().Exec(`UPDATE promotion_imports
		SET owner_epoch = 2, owner_token_hash = ?
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		promotionTestHash("new-owner-token"), created.AllocationEpoch, created.AllocationSequence); err != nil {
		t.Fatal(err)
	}
	takeoverRetry := inlineContentRequest(created, manifest[0], "put-race", bytes.NewBufferString("hello"))
	takeoverRetry.OwnerEpoch = 2
	takeoverRetry.OwnerToken = "new-owner-token"
	recovered, err := promotionStore.PutInlineImportContent(context.Background(), takeoverRetry)
	if err != nil {
		t.Fatalf("content recovery after takeover: %v", err)
	}
	if recovered.ContentID != first.content.ContentID {
		t.Fatalf("content recovery after takeover ID = %q, want %q", recovered.ContentID, first.content.ContentID)
	}
}

func TestPromotionVerifyRejectsMissingOrExtraContentWithoutStateChange(t *testing.T) {
	store, promotionStore, created, manifest := createPromotionImportForContent(t, []promotion.ManifestEntry{
		{RelativePath: "file", Type: promotion.EntryTypeFile, Mode: 0o644, ExpectedSizeBytes: 5, ExpectedChecksumSHA256: promotionTestHash("hello")},
	}, "allocate-verify-incomplete")
	verify := PromotionVerifyRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID, OwnerEpoch: created.OwnerEpoch,
		OwnerToken: "owner-token", WriterLease: "writer-lease", ManifestHash: created.ManifestHash,
	}
	if _, err := promotionStore.VerifyImport(context.Background(), verify); !errors.Is(err, ErrPromotionConflict) {
		t.Fatalf("missing content VerifyImport error = %v, want ErrPromotionConflict", err)
	}
	var state string
	var version uint64
	if err := store.DB().QueryRow(`SELECT state, state_version FROM promotion_imports
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&state, &version); err != nil {
		t.Fatal(err)
	}
	if state != "CREATED" || version != 1 {
		t.Fatalf("state after incomplete verify = %s/%d, want CREATED/1", state, version)
	}
	if _, err := promotionStore.PutInlineImportContent(context.Background(), inlineContentRequest(created, manifest[0], "put-file", bytes.NewBufferString("hello"))); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`INSERT INTO promotion_import_contents
		(tenant_id, allocation_epoch, allocation_sequence, import_content_id,
		 inline_content_blob, inline_content_idempotency_key, inline_request_digest,
		 size_bytes, checksum_sha256, seal_state, ownership_state)
		VALUES ('tenant-a', ?, ?, 'pic_extra', 'x', 'extra-key', ?, 1, ?, 'INLINE', 'STAGED')`,
		created.AllocationEpoch, created.AllocationSequence, promotionTestHash("extra-request"), promotionTestHash("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := promotionStore.VerifyImport(context.Background(), verify); !errors.Is(err, ErrPromotionRecoveryRequired) {
		t.Fatalf("extra content VerifyImport error = %v, want ErrPromotionRecoveryRequired", err)
	}
	if err := store.DB().QueryRow(`SELECT state, state_version FROM promotion_imports
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&state, &version); err != nil {
		t.Fatal(err)
	}
	if state != "STAGING" || version != 2 {
		t.Fatalf("state after corrupt verify = %s/%d, want STAGING/2", state, version)
	}
}

func TestPromotionContentMutationsAreAuthorizedAndWriterFenced(t *testing.T) {
	store, promotionStore, created, manifest := createPromotionImportForContent(t, []promotion.ManifestEntry{
		{RelativePath: "file", Type: promotion.EntryTypeFile, Mode: 0o644, ExpectedSizeBytes: 5, ExpectedChecksumSHA256: promotionTestHash("hello")},
	}, "allocate-content-fenced")
	promotionStore.cfg.Authorizer.(*promotionTestAuthorizer).denied = true
	reader := &promotionCountingReader{body: bytes.NewBufferString("hello")}
	req := inlineContentRequest(created, manifest[0], "put-denied", reader)
	if _, err := promotionStore.PutInlineImportContent(context.Background(), req); err == nil {
		t.Fatal("PutInlineImportContent authorization error = nil")
	}
	if reader.reads != 0 {
		t.Fatalf("body reads after denied authorization = %d, want 0", reader.reads)
	}
	var contents int
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM promotion_import_contents`).Scan(&contents); err != nil {
		t.Fatal(err)
	}
	if contents != 0 {
		t.Fatalf("content rows after denied mutation = %d, want 0", contents)
	}
	promotionStore.cfg.Authorizer.(*promotionTestAuthorizer).denied = false
	if _, err := store.DB().Exec(`UPDATE promotion_namespace_capabilities
		SET minimum_writer_protocol = 2 WHERE tenant_id = 'tenant-a'`); err != nil {
		t.Fatal(err)
	}
	req.Body = bytes.NewBufferString("hello")
	if _, err := promotionStore.PutInlineImportContent(context.Background(), req); !errors.Is(err, ErrPromotionRestoreFenced) {
		t.Fatalf("old writer protocol content error = %v, want ErrPromotionRestoreFenced", err)
	}
	verify := PromotionVerifyRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID, OwnerEpoch: created.OwnerEpoch,
		OwnerToken: "owner-token", WriterLease: "writer-lease", ManifestHash: created.ManifestHash,
	}
	if _, err := promotionStore.VerifyImport(context.Background(), verify); !errors.Is(err, ErrPromotionRestoreFenced) {
		t.Fatalf("old writer protocol verify error = %v, want ErrPromotionRestoreFenced", err)
	}
}

func TestPromotionContentMutationLookupDoesNotDiscloseImportExistence(t *testing.T) {
	_, promotionStore, created, manifest := createPromotionImportForContent(t, []promotion.ManifestEntry{
		{RelativePath: "file", Type: promotion.EntryTypeFile, Mode: 0o644, ExpectedSizeBytes: 5, ExpectedChecksumSHA256: promotionTestHash("hello")},
	}, "allocate-content-nondisclosure")
	authorizer := promotionStore.cfg.Authorizer.(*promotionTestAuthorizer)
	authorizer.mu.Lock()
	authorizer.denied = true
	authorizer.mu.Unlock()

	deniedReader := &promotionCountingReader{body: bytes.NewBufferString("hello")}
	deniedPut := inlineContentRequest(created, manifest[0], "put-denied", deniedReader)
	if _, err := promotionStore.PutInlineImportContent(context.Background(), deniedPut); err != ErrNotFound {
		t.Fatalf("existing denied PutInlineImportContent error = %v, want canonical ErrNotFound", err)
	}
	if deniedReader.reads != 0 {
		t.Fatalf("existing denied body reads = %d, want 0", deniedReader.reads)
	}
	deniedVerify := PromotionVerifyRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID, OwnerEpoch: created.OwnerEpoch,
		OwnerToken: "owner-token", WriterLease: "writer-lease", ManifestHash: created.ManifestHash,
	}
	if _, err := promotionStore.VerifyImport(context.Background(), deniedVerify); err != ErrNotFound {
		t.Fatalf("existing denied VerifyImport error = %v, want canonical ErrNotFound", err)
	}

	authorizer.mu.Lock()
	authorizer.denied = false
	authorizer.mu.Unlock()
	unknownID, err := promotion.EncodeMigrationID(promotion.MigrationIdentity{
		AllocationEpoch: created.AllocationEpoch, AllocationSequence: created.AllocationSequence + 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	unknownReader := &promotionCountingReader{body: bytes.NewBufferString("hello")}
	unknownPut := inlineContentRequest(created, manifest[0], "put-unknown", unknownReader)
	unknownPut.MigrationID = unknownID
	if _, err := promotionStore.PutInlineImportContent(context.Background(), unknownPut); err != ErrNotFound {
		t.Fatalf("unknown PutInlineImportContent error = %v, want canonical ErrNotFound", err)
	}
	if unknownReader.reads != 0 {
		t.Fatalf("unknown body reads = %d, want 0", unknownReader.reads)
	}
	unknownVerify := deniedVerify
	unknownVerify.MigrationID = unknownID
	if _, err := promotionStore.VerifyImport(context.Background(), unknownVerify); err != ErrNotFound {
		t.Fatalf("unknown VerifyImport error = %v, want canonical ErrNotFound", err)
	}

	// The canonical response does not hide a broken allow path: the same
	// authorized request must still reach the content transaction.
	allowed := inlineContentRequest(created, manifest[0], "put-allowed", bytes.NewBufferString("hello"))
	if _, err := promotionStore.PutInlineImportContent(context.Background(), allowed); err != nil {
		t.Fatalf("authorized PutInlineImportContent: %v", err)
	}
}

func TestPromotionInlineBodyReadDoesNotHoldRestoreOrImportLocks(t *testing.T) {
	store, promotionStore, created, manifest := createPromotionImportForContent(t, []promotion.ManifestEntry{
		{RelativePath: "file", Type: promotion.EntryTypeFile, Mode: 0o644, ExpectedSizeBytes: 5, ExpectedChecksumSHA256: promotionTestHash("hello")},
	}, "allocate-content-blocked-reader")
	var databaseNow time.Time
	if err := store.DB().QueryRow(`SELECT CURRENT_TIMESTAMP(3)`).Scan(&databaseNow); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`UPDATE promotion_imports
		SET activity_deadline = ?
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		databaseNow.Add(150*time.Millisecond), created.AllocationEpoch, created.AllocationSequence); err != nil {
		t.Fatal(err)
	}
	reader := &promotionBlockingReader{
		started: make(chan struct{}), release: make(chan struct{}), body: bytes.NewBufferString("hello"),
	}
	request := inlineContentRequest(created, manifest[0], "put-blocked-reader", reader)
	result := make(chan error, 1)
	go func() {
		_, err := promotionStore.PutInlineImportContent(context.Background(), request)
		result <- err
	}()
	select {
	case <-reader.started:
	case <-time.After(5 * time.Second):
		t.Fatal("PutInlineImportContent did not reach body read")
	}
	time.Sleep(250 * time.Millisecond)
	updateCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := store.DB().ExecContext(updateCtx, `UPDATE promotion_namespace_capabilities
		SET admission_state = 'DRAINING' WHERE tenant_id = 'tenant-a'`); err != nil {
		t.Fatalf("restore fence could not advance while body stalled: %v", err)
	}
	close(reader.release)
	select {
	case err := <-result:
		if !errors.Is(err, ErrPromotionRestoreFenced) {
			t.Fatalf("stalled PutInlineImportContent error = %v, want ErrPromotionRestoreFenced", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("PutInlineImportContent did not finish after body release")
	}
	var contents, bindings int
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM promotion_import_contents
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&contents); err != nil {
		t.Fatal(err)
	}
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM promotion_import_entries
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?
		  AND import_content_id IS NOT NULL`, created.AllocationEpoch, created.AllocationSequence).Scan(&bindings); err != nil {
		t.Fatal(err)
	}
	if contents != 0 || bindings != 0 {
		t.Fatalf("stalled/fenced Put mutations = content %d, bindings %d; want 0/0", contents, bindings)
	}
}

func TestPromotionVerifyUsesFreshDatabaseTimeAtFinalCAS(t *testing.T) {
	store, promotionStore, created, manifest := createPromotionImportForContent(t, []promotion.ManifestEntry{
		{RelativePath: "file", Type: promotion.EntryTypeFile, Mode: 0o644, ExpectedSizeBytes: 5, ExpectedChecksumSHA256: promotionTestHash("hello")},
	}, "allocate-verify-final-time")
	if _, err := promotionStore.PutInlineImportContent(context.Background(), inlineContentRequest(created, manifest[0], "put-file", bytes.NewBufferString("hello"))); err != nil {
		t.Fatal(err)
	}
	var databaseNow time.Time
	if err := store.DB().QueryRow(`SELECT CURRENT_TIMESTAMP(3)`).Scan(&databaseNow); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`UPDATE promotion_imports
		SET activity_deadline = ?
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		databaseNow.Add(1500*time.Millisecond), created.AllocationEpoch, created.AllocationSequence); err != nil {
		t.Fatal(err)
	}
	blocker, err := store.DB().BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback() }()
	var contentID string
	if err := blocker.QueryRow(`SELECT import_content_id FROM promotion_import_contents
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ? FOR UPDATE`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&contentID); err != nil {
		t.Fatal(err)
	}
	verifyResult := make(chan error, 1)
	go func() {
		_, err := promotionStore.VerifyImport(context.Background(), PromotionVerifyRequest{
			TenantID: "tenant-a", MigrationID: created.MigrationID, OwnerEpoch: created.OwnerEpoch,
			OwnerToken: "owner-token", WriterLease: "writer-lease", ManifestHash: created.ManifestHash,
		})
		verifyResult <- err
	}()
	// The held content row makes a pre-deadline Verify wait after its initial
	// guard. Keeping it held past the deadline distinguishes the final-time
	// CAS check from a transaction-start-only implementation.
	select {
	case err := <-verifyResult:
		t.Fatalf("VerifyImport returned before content-row lock release: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	time.Sleep(1500 * time.Millisecond)
	if err := blocker.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-verifyResult:
		if !errors.Is(err, ErrPromotionDeadlineExceeded) {
			t.Fatalf("VerifyImport after deadline error = %v, want ErrPromotionDeadlineExceeded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("VerifyImport did not finish after content-row lock release")
	}
	var state, attemptID, reason string
	if err := store.DB().QueryRow(`SELECT state, cleanup_attempt_id, terminal_reason
		FROM promotion_imports WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&state, &attemptID, &reason); err != nil {
		t.Fatal(err)
	}
	if state != "ABORTING" || attemptID == "" || reason != "activity_deadline_exceeded" {
		t.Fatalf("Verify deadline cleanup = %s/%q/%q, want ABORTING/nonempty/activity_deadline_exceeded", state, attemptID, reason)
	}
}

func TestPromotionVerifiedRetryUsesFreshTimeAfterImportLock(t *testing.T) {
	store, promotionStore, created, manifest := createPromotionImportForContent(t, []promotion.ManifestEntry{
		{RelativePath: "file", Type: promotion.EntryTypeFile, Mode: 0o644, ExpectedSizeBytes: 5, ExpectedChecksumSHA256: promotionTestHash("hello")},
	}, "allocate-verified-retry-time")
	if _, err := promotionStore.PutInlineImportContent(context.Background(), inlineContentRequest(created, manifest[0], "put-file", bytes.NewBufferString("hello"))); err != nil {
		t.Fatal(err)
	}
	verify := PromotionVerifyRequest{
		TenantID: "tenant-a", MigrationID: created.MigrationID, OwnerEpoch: created.OwnerEpoch,
		OwnerToken: "owner-token", WriterLease: "writer-lease", ManifestHash: created.ManifestHash,
	}
	if _, err := promotionStore.VerifyImport(context.Background(), verify); err != nil {
		t.Fatal(err)
	}
	var databaseNow time.Time
	if err := store.DB().QueryRow(`SELECT CURRENT_TIMESTAMP(3)`).Scan(&databaseNow); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`UPDATE promotion_imports
		SET activity_deadline = ?
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		databaseNow.Add(1500*time.Millisecond), created.AllocationEpoch, created.AllocationSequence); err != nil {
		t.Fatal(err)
	}
	blocker, err := store.DB().BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback() }()
	var state string
	if err := blocker.QueryRow(`SELECT state FROM promotion_imports
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ? FOR UPDATE`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "VERIFIED" {
		t.Fatalf("locked import state = %q, want VERIFIED", state)
	}
	retryResult := make(chan error, 1)
	go func() {
		_, err := promotionStore.VerifyImport(context.Background(), verify)
		retryResult <- err
	}()
	// The retry takes the tenant/namespace guards and samples its first DB
	// time, then waits on the import row. It must not use that pre-lock sample
	// for the VERIFIED recovery return after the immutable deadline passes.
	select {
	case err := <-retryResult:
		t.Fatalf("verified retry returned before import-row lock release: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	time.Sleep(1500 * time.Millisecond)
	if err := blocker.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-retryResult:
		if !errors.Is(err, ErrPromotionDeadlineExceeded) {
			t.Fatalf("verified retry after deadline error = %v, want ErrPromotionDeadlineExceeded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("verified retry did not finish after import-row lock release")
	}
	var attemptID, reason string
	if err := store.DB().QueryRow(`SELECT state, cleanup_attempt_id, terminal_reason
		FROM promotion_imports WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).Scan(&state, &attemptID, &reason); err != nil {
		t.Fatal(err)
	}
	if state != "ABORTING" || attemptID == "" || reason != "activity_deadline_exceeded" {
		t.Fatalf("verified retry cleanup = %s/%q/%q, want ABORTING/nonempty/activity_deadline_exceeded", state, attemptID, reason)
	}
}

func TestPromotionContentDeadlineCreatesRecoverableCleanupOwner(t *testing.T) {
	store, promotionStore, created, manifest := createPromotionImportForContent(t, []promotion.ManifestEntry{
		{RelativePath: "file", Type: promotion.EntryTypeFile, Mode: 0o644, ExpectedSizeBytes: 5, ExpectedChecksumSHA256: promotionTestHash("hello")},
	}, "allocate-content-deadline")
	if _, err := store.DB().Exec(`UPDATE promotion_imports
		SET activity_deadline = DATE_SUB(NOW(3), INTERVAL 1 SECOND)
		WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence); err != nil {
		t.Fatal(err)
	}
	req := inlineContentRequest(created, manifest[0], "put-expired", bytes.NewBufferString("hello"))
	if _, err := promotionStore.PutInlineImportContent(context.Background(), req); !errors.Is(err, ErrPromotionDeadlineExceeded) {
		t.Fatalf("expired content error = %v, want ErrPromotionDeadlineExceeded", err)
	}
	var state, attemptID, reason string
	var writerGeneration uint64
	if err := store.DB().QueryRow(`SELECT state, cleanup_attempt_id, cleanup_writer_generation, terminal_reason
		FROM promotion_imports WHERE tenant_id = 'tenant-a' AND allocation_epoch = ? AND allocation_sequence = ?`,
		created.AllocationEpoch, created.AllocationSequence).
		Scan(&state, &attemptID, &writerGeneration, &reason); err != nil {
		t.Fatal(err)
	}
	if state != "ABORTING" || attemptID == "" || writerGeneration != 7 || reason != "activity_deadline_exceeded" {
		t.Fatalf("deadline cleanup = %s/%q/%d/%q", state, attemptID, writerGeneration, reason)
	}
	var contents int
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM promotion_import_contents`).Scan(&contents); err != nil {
		t.Fatal(err)
	}
	if contents != 0 {
		t.Fatalf("content rows after deadline winner = %d, want 0", contents)
	}
}
