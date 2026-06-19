package datastore

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testmysql"
	"github.com/mem9-ai/drive9/pkg/journal"
)

func newJournalStore(t *testing.T) *Store {
	t.Helper()
	initDatastoreSchema(t, testDSN)
	store, err := Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	testmysql.ResetDB(t, store.DB())
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestJournalCreateConcurrentIdempotency(t *testing.T) {
	store := newJournalStore(t)
	ctx := context.Background()
	tenantID := "tenant-journal-concurrent"
	req := journal.CreateRequest{
		JournalID: "jrn_concurrent",
		Kind:      "agent",
		Title:     "same create",
		Meta:      map[string]string{"repo": "drive9"},
	}

	const workers = 12
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			created, err := store.CreateJournal(ctx, tenantID, req)
			if err == nil && created.JournalID != req.JournalID {
				err = errors.New("unexpected journal id")
			}
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("CreateJournal concurrent retry: %v", err)
		}
	}
}

func TestJournalCreateAppendVerifyAndIdempotency(t *testing.T) {
	store := newJournalStore(t)
	ctx := context.Background()
	tenantID := "tenant-journal"

	created, err := store.CreateJournal(ctx, tenantID, journal.CreateRequest{
		JournalID: "jrn_test",
		Kind:      "agent",
		Title:     "test run",
		Meta:      map[string]string{"repo": "github.com/mem9-ai/drive9"},
	})
	if err != nil {
		t.Fatalf("CreateJournal: %v", err)
	}
	if created.HeadHash == "" || created.GenesisHash != created.HeadHash {
		t.Fatalf("genesis/head hash mismatch: %#v", created)
	}

	createdAgain, err := store.CreateJournal(ctx, tenantID, journal.CreateRequest{
		JournalID: "jrn_test",
		Kind:      "agent",
		Title:     "test run",
		Meta:      map[string]string{"repo": "github.com/mem9-ai/drive9"},
	})
	if err != nil {
		t.Fatalf("CreateJournal retry: %v", err)
	}
	if createdAgain.JournalID != created.JournalID || createdAgain.HeadHash != created.HeadHash {
		t.Fatalf("idempotent create = %#v, want %#v", createdAgain, created)
	}
	if _, err := store.CreateJournal(ctx, tenantID, journal.CreateRequest{
		JournalID: "jrn_test",
		Kind:      "agent",
		Title:     "different",
	}); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("CreateJournal conflict err = %v, want ErrJournalConflict", err)
	}

	occurred := time.Date(2026, 5, 12, 1, 2, 3, 456789000, time.UTC)
	resp, err := store.AppendJournalEntries(ctx, tenantID, "jrn_test", "app_test", JournalWriter{Type: "api_key", ID: "key-1"}, []journal.EntryInput{{
		Type:       "tool.call.completed",
		Status:     "ok",
		OccurredAt: &occurred,
		Subjects:   []string{"tool:exec_command"},
		Summary:    []byte(`{"cmd":"go test ./..."}`),
	}})
	if err != nil {
		t.Fatalf("AppendJournalEntries: %v", err)
	}
	if resp.FirstSeq != 1 || resp.LastSeq != 1 || resp.Count != 1 || resp.HeadHash == "" {
		t.Fatalf("append response = %#v", resp)
	}
	if resp.Idempotent {
		t.Fatalf("fresh append marked idempotent: %#v", resp)
	}
	respAgain, err := store.AppendJournalEntries(ctx, tenantID, "jrn_test", "app_test", JournalWriter{Type: "api_key", ID: "key-1"}, []journal.EntryInput{{
		Type:       "tool.call.completed",
		Status:     "ok",
		OccurredAt: &occurred,
		Subjects:   []string{"tool:exec_command"},
		Summary:    []byte(`{"cmd":"go test ./..."}`),
	}})
	if err != nil {
		t.Fatalf("AppendJournalEntries retry: %v", err)
	}
	wantReplay := *resp
	wantReplay.Idempotent = true
	if *respAgain != wantReplay {
		t.Fatalf("idempotent append = %#v, want %#v", respAgain, wantReplay)
	}
	if _, err := store.AppendJournalEntries(ctx, tenantID, "jrn_test", "app_test", JournalWriter{Type: "api_key", ID: "key-1"}, []journal.EntryInput{{
		Type:     "tool.call.completed",
		Status:   "error",
		Subjects: []string{"tool:exec_command"},
	}}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("AppendJournalEntries conflict err = %v, want ErrIdempotencyConflict", err)
	}

	entries, err := store.ListJournalEntries(ctx, tenantID, "jrn_test", 0, 10)
	if err != nil {
		t.Fatalf("ListJournalEntries: %v", err)
	}
	if len(entries) != 1 || entries[0].Seq != 1 || entries[0].Source != journal.SourceSelf {
		t.Fatalf("entries = %#v", entries)
	}
	if entries[0].Actor.Type != "api_key" || entries[0].Actor.ID != "key-1" {
		t.Fatalf("entry actor = %#v, want authenticated writer fallback", entries[0].Actor)
	}
	if got := journal.FormatTime(entries[0].OccurredAt); got != "2026-05-12T01:02:03.456Z" {
		t.Fatalf("occurred_at = %s, want millisecond UTC canonicalization", got)
	}

	matches, err := store.SearchJournal(ctx, tenantID, journal.SearchRequest{
		Subjects: []string{"tool:exec_command"},
		Limit:    10,
	})
	if err != nil {
		t.Fatalf("SearchJournal: %v", err)
	}
	if len(matches) != 1 || matches[0].JournalID != "jrn_test" || matches[0].Seq != 1 {
		t.Fatalf("matches = %#v", matches)
	}
	matches, err = store.SearchJournal(ctx, tenantID, journal.SearchRequest{
		ActorType: "api_key",
		ActorID:   "key-1",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("SearchJournal actor: %v", err)
	}
	if len(matches) != 1 || matches[0].JournalID != "jrn_test" || matches[0].Seq != 1 {
		t.Fatalf("actor matches = %#v", matches)
	}

	verify, err := store.VerifyJournal(ctx, tenantID, "jrn_test")
	if err != nil {
		t.Fatalf("VerifyJournal: %v", err)
	}
	if !verify.OK || !verify.HashChainOK || verify.Entries != 1 || verify.HeadHash != resp.HeadHash {
		t.Fatalf("verify = %#v", verify)
	}
	if verify.ProjectionOK == nil || !*verify.ProjectionOK {
		t.Fatalf("verify projection_ok = %#v, want true", verify.ProjectionOK)
	}
	if verify.SealOK != nil || verify.ArtifactBytesAvailable != nil {
		t.Fatalf("verify unchecked fields = seal:%#v artifact:%#v, want omitted", verify.SealOK, verify.ArtifactBytesAvailable)
	}
}

func TestAppendJournalRejectsClosedJournal(t *testing.T) {
	store := newJournalStore(t)
	ctx := context.Background()
	tenantID := "tenant-journal-closed"

	if _, err := store.CreateJournal(ctx, tenantID, journal.CreateRequest{
		JournalID: "jrn_closed",
		Kind:      "agent",
		Title:     "closed run",
	}); err != nil {
		t.Fatalf("CreateJournal: %v", err)
	}
	now := time.Now().UTC()
	if _, err := store.DB().ExecContext(ctx, `UPDATE journals
		SET closed_at = ?
		WHERE tenant_id = ? AND journal_id = ?`, now, tenantID, "jrn_closed"); err != nil {
		t.Fatalf("close journal: %v", err)
	}
	if _, err := store.AppendJournalEntries(ctx, tenantID, "jrn_closed", "app_closed", JournalWriter{Type: "api_key", ID: "key-1"}, []journal.EntryInput{{
		Type:   "tool.call.completed",
		Status: "ok",
	}}); !errors.Is(err, ErrJournalClosed) {
		t.Fatalf("AppendJournalEntries closed err = %v, want ErrJournalClosed", err)
	}
}

func TestJournalAppendTrustedSource(t *testing.T) {
	store := newJournalStore(t)
	ctx := context.Background()
	tenantID := "tenant-journal-source"

	if _, err := store.CreateJournal(ctx, tenantID, journal.CreateRequest{
		JournalID: "jrn_source",
	}); err != nil {
		t.Fatalf("CreateJournal: %v", err)
	}

	gatewayEntry := journal.EntryInput{
		Type:   "tool.call.completed",
		Source: journal.SourceGateway,
	}
	if _, err := store.AppendJournalEntries(ctx, tenantID, "jrn_source", "app_source_denied", JournalWriter{Type: "api_key", ID: "key-1"}, []journal.EntryInput{gatewayEntry}); !errors.Is(err, ErrJournalValidation) {
		t.Fatalf("AppendJournalEntries untrusted source err = %v, want ErrJournalValidation", err)
	}
	if _, err := store.AppendJournalEntries(ctx, tenantID, "jrn_source", "app_source_mismatch", JournalWriter{Type: "api_key", ID: "key-1", Source: journal.SourceGateway}, []journal.EntryInput{{
		Type:   "tool.call.completed",
		Source: journal.SourceImported,
	}}); !errors.Is(err, ErrJournalValidation) {
		t.Fatalf("AppendJournalEntries mismatched source err = %v, want ErrJournalValidation", err)
	}
	if _, err := store.AppendJournalEntries(ctx, tenantID, "jrn_source", "app_source_ok", JournalWriter{Type: "api_key", ID: "key-1", Source: journal.SourceGateway}, []journal.EntryInput{gatewayEntry}); err != nil {
		t.Fatalf("AppendJournalEntries trusted source: %v", err)
	}

	entries, err := store.ListJournalEntries(ctx, tenantID, "jrn_source", 0, 10)
	if err != nil {
		t.Fatalf("ListJournalEntries: %v", err)
	}
	if len(entries) != 1 || entries[0].Source != journal.SourceGateway {
		t.Fatalf("entries = %#v, want trusted gateway source", entries)
	}
}

func TestJournalAppendInheritsJournalActor(t *testing.T) {
	store := newJournalStore(t)
	ctx := context.Background()
	tenantID := "tenant-journal-actor"

	if _, err := store.CreateJournal(ctx, tenantID, journal.CreateRequest{
		JournalID: "jrn_actor",
		Actor:     journal.Actor{Type: "agent", ID: "codex"},
	}); err != nil {
		t.Fatalf("CreateJournal: %v", err)
	}
	if _, err := store.AppendJournalEntries(ctx, tenantID, "jrn_actor", "app_actor", JournalWriter{Type: "api_key", ID: "key-1"}, []journal.EntryInput{{
		Type: "tool.call.completed",
	}}); err != nil {
		t.Fatalf("AppendJournalEntries: %v", err)
	}
	entries, err := store.ListJournalEntries(ctx, tenantID, "jrn_actor", 0, 10)
	if err != nil {
		t.Fatalf("ListJournalEntries: %v", err)
	}
	if len(entries) != 1 || entries[0].Actor.Type != "agent" || entries[0].Actor.ID != "codex" {
		t.Fatalf("entries = %#v, want journal actor inherited", entries)
	}
	matches, err := store.SearchJournal(ctx, tenantID, journal.SearchRequest{
		ActorType: "agent",
		ActorID:   "codex",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("SearchJournal actor: %v", err)
	}
	if len(matches) != 1 || matches[0].JournalID != "jrn_actor" {
		t.Fatalf("actor matches = %#v", matches)
	}
}

func TestJournalListMissingAndVerifyProjection(t *testing.T) {
	store := newJournalStore(t)
	ctx := context.Background()
	tenantID := "tenant-journal-projection"

	if _, err := store.ListJournalEntries(ctx, tenantID, "jrn_missing", 0, 10); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ListJournalEntries missing err = %v, want ErrNotFound", err)
	}

	if _, err := store.CreateJournal(ctx, tenantID, journal.CreateRequest{
		JournalID: "jrn_label_projection",
		Labels:    []journal.Label{{Key: "env", Value: "prod"}, {Key: "env", Value: "us-east"}},
	}); err != nil {
		t.Fatalf("CreateJournal labels: %v", err)
	}
	verify, err := store.VerifyJournal(ctx, tenantID, "jrn_label_projection")
	if err != nil {
		t.Fatalf("VerifyJournal labels: %v", err)
	}
	if !verify.OK || verify.ProjectionOK == nil || !*verify.ProjectionOK {
		t.Fatalf("verify labels before corruption = %#v", verify)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM journal_labels WHERE tenant_id = ? AND journal_id = ?`, tenantID, "jrn_label_projection"); err != nil {
		t.Fatalf("corrupt label projection: %v", err)
	}
	verify, err = store.VerifyJournal(ctx, tenantID, "jrn_label_projection")
	if err != nil {
		t.Fatalf("VerifyJournal label corruption: %v", err)
	}
	if verify.OK || verify.ProjectionOK == nil || *verify.ProjectionOK {
		t.Fatalf("verify after label projection corruption = %#v", verify)
	}

	if _, err := store.CreateJournal(ctx, tenantID, journal.CreateRequest{
		JournalID: "jrn_label_case",
		Labels:    []journal.Label{{Key: "env", Value: "prod"}},
	}); err != nil {
		t.Fatalf("CreateJournal label case: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE journal_labels
		SET label_key = 'Env', label_hash = ?
		WHERE tenant_id = ? AND journal_id = ?`,
		journal.LabelHash("Env", "prod"), tenantID, "jrn_label_case"); err != nil {
		t.Fatalf("corrupt label canonicalization: %v", err)
	}
	verify, err = store.VerifyJournal(ctx, tenantID, "jrn_label_case")
	if err != nil {
		t.Fatalf("VerifyJournal label canonicalization: %v", err)
	}
	if verify.OK || verify.ProjectionOK == nil || *verify.ProjectionOK {
		t.Fatalf("verify after non-canonical label projection = %#v", verify)
	}

	if _, err := store.CreateJournal(ctx, tenantID, journal.CreateRequest{JournalID: "jrn_projection"}); err != nil {
		t.Fatalf("CreateJournal: %v", err)
	}
	if _, err := store.AppendJournalEntries(ctx, tenantID, "jrn_projection", "app_projection", JournalWriter{Type: "api_key", ID: "key-1"}, []journal.EntryInput{{
		Type:     "tool.call.completed",
		Subjects: []string{"tool:exec_command"},
	}}); err != nil {
		t.Fatalf("AppendJournalEntries: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `DELETE FROM journal_entry_subjects WHERE tenant_id = ? AND journal_id = ?`, tenantID, "jrn_projection"); err != nil {
		t.Fatalf("corrupt projection: %v", err)
	}
	verify, err = store.VerifyJournal(ctx, tenantID, "jrn_projection")
	if err != nil {
		t.Fatalf("VerifyJournal: %v", err)
	}
	if verify.OK || !verify.HashChainOK || verify.ProjectionOK == nil || *verify.ProjectionOK {
		t.Fatalf("verify after projection corruption = %#v", verify)
	}

	if _, err := store.CreateJournal(ctx, tenantID, journal.CreateRequest{JournalID: "jrn_subject_hash"}); err != nil {
		t.Fatalf("CreateJournal subject hash: %v", err)
	}
	if _, err := store.AppendJournalEntries(ctx, tenantID, "jrn_subject_hash", "app_subject_hash", JournalWriter{Type: "api_key", ID: "key-1"}, []journal.EntryInput{{
		Type:     "tool.call.completed",
		Subjects: []string{"file:/tmp/Z", "file:/tmp/a"},
	}}); err != nil {
		t.Fatalf("AppendJournalEntries subject hash: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE journal_entry_subjects SET subject_hash = 'sha256:bad' WHERE tenant_id = ? AND journal_id = ? LIMIT 1`, tenantID, "jrn_subject_hash"); err != nil {
		t.Fatalf("corrupt subject hash projection: %v", err)
	}
	verify, err = store.VerifyJournal(ctx, tenantID, "jrn_subject_hash")
	if err != nil {
		t.Fatalf("VerifyJournal subject hash corruption: %v", err)
	}
	if verify.OK || verify.ProjectionOK == nil || *verify.ProjectionOK {
		t.Fatalf("verify after subject hash corruption = %#v", verify)
	}

	if _, err := store.CreateJournal(ctx, tenantID, journal.CreateRequest{JournalID: "jrn_subject_case"}); err != nil {
		t.Fatalf("CreateJournal subject case: %v", err)
	}
	if _, err := store.AppendJournalEntries(ctx, tenantID, "jrn_subject_case", "app_subject_case", JournalWriter{Type: "api_key", ID: "key-1"}, []journal.EntryInput{{
		Type:     "tool.call.completed",
		Subjects: []string{"file:/tmp/Z"},
	}}); err != nil {
		t.Fatalf("AppendJournalEntries subject case: %v", err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE journal_entry_subjects
		SET subject_type = 'File', subject_hash = ?
		WHERE tenant_id = ? AND journal_id = ?`,
		journal.SubjectHash("File", "/tmp/Z"), tenantID, "jrn_subject_case"); err != nil {
		t.Fatalf("corrupt subject canonicalization: %v", err)
	}
	verify, err = store.VerifyJournal(ctx, tenantID, "jrn_subject_case")
	if err != nil {
		t.Fatalf("VerifyJournal subject canonicalization: %v", err)
	}
	if verify.OK || verify.ProjectionOK == nil || *verify.ProjectionOK {
		t.Fatalf("verify after non-canonical subject projection = %#v", verify)
	}
}

func TestJournalSearchLabelsAndValidation(t *testing.T) {
	store := newJournalStore(t)
	ctx := context.Background()
	tenantID := "tenant-journal-labels"

	if _, err := store.CreateJournal(ctx, tenantID, journal.CreateRequest{
		JournalID: "jrn_labels",
		Kind:      "agent",
		Labels: []journal.Label{
			{Key: "env", Value: "prod"},
			{Key: "env", Value: "us-east"},
			{Key: "repo", Value: "drive9"},
		},
	}); err != nil {
		t.Fatalf("CreateJournal: %v", err)
	}
	matches, err := store.SearchJournal(ctx, tenantID, journal.SearchRequest{
		Labels: []journal.Label{{Key: "env", Value: "prod"}, {Key: "env", Value: "us-east"}},
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("SearchJournal labels: %v", err)
	}
	if len(matches) != 1 || matches[0].JournalID != "jrn_labels" {
		t.Fatalf("label matches = %#v", matches)
	}
	if _, err := store.AppendJournalEntries(ctx, tenantID, "jrn_labels", "app_label_entry", JournalWriter{Type: "api_key", ID: "key-1"}, []journal.EntryInput{{
		Type:    "tool.call.completed",
		Summary: []byte(`{"cmd":"go test ./..."}`),
	}}); err != nil {
		t.Fatalf("AppendJournalEntries label entry: %v", err)
	}
	entryMatches, err := store.SearchJournal(ctx, tenantID, journal.SearchRequest{
		Labels:  []journal.Label{{Key: "env", Value: "prod"}},
		Entries: true,
		Limit:   10,
	})
	if err != nil {
		t.Fatalf("SearchJournal label entries: %v", err)
	}
	if len(entryMatches) != 1 || entryMatches[0].Entry == nil || entryMatches[0].Entry.JournalID != "jrn_labels" {
		t.Fatalf("label entry matches = %#v", entryMatches)
	}
	if _, err := store.SearchJournal(ctx, tenantID, journal.SearchRequest{
		Subjects: []string{"tool:exec_command", "bad-subject"},
		Limit:    10,
	}); !errors.Is(err, ErrJournalValidation) {
		t.Fatalf("SearchJournal invalid subject err = %v, want ErrJournalValidation", err)
	}
	if _, err := store.AppendJournalEntries(ctx, tenantID, "jrn_labels", "app_large_summary", JournalWriter{Type: "api_key", ID: "key-1"}, []journal.EntryInput{{
		Type:    "tool.call.completed",
		Summary: []byte(`{"x":"` + strings.Repeat("a", journal.MaxInlineSummaryBytes) + `"}`),
	}}); !errors.Is(err, ErrJournalPayloadTooLarge) {
		t.Fatalf("AppendJournalEntries oversized summary err = %v, want ErrJournalPayloadTooLarge", err)
	}
}

func TestJournalLabelSearchUsesProjectionTimeForPagination(t *testing.T) {
	store := newJournalStore(t)
	ctx := context.Background()
	tenantID := "tenant-journal-label-order"

	if _, err := store.CreateJournal(ctx, tenantID, journal.CreateRequest{
		JournalID: "jrn_label_old",
		Kind:      "agent",
		Labels:    []journal.Label{{Key: "env", Value: "prod"}},
	}); err != nil {
		t.Fatalf("CreateJournal old: %v", err)
	}
	if _, err := store.CreateJournal(ctx, tenantID, journal.CreateRequest{
		JournalID: "jrn_label_new",
		Kind:      "agent",
		Labels:    []journal.Label{{Key: "env", Value: "prod"}},
	}); err != nil {
		t.Fatalf("CreateJournal new: %v", err)
	}

	labelCreatedAt := journal.NormalizeTime(time.Now().Add(time.Hour))
	if _, err := store.DB().ExecContext(ctx, `UPDATE journal_labels
		SET created_at = ?
		WHERE tenant_id = ? AND journal_id = ? AND label_key = ? AND label_value = ?`,
		labelCreatedAt, tenantID, "jrn_label_old", "env", "prod"); err != nil {
		t.Fatalf("move label projection time: %v", err)
	}

	matches, err := store.SearchJournal(ctx, tenantID, journal.SearchRequest{
		Labels: []journal.Label{{Key: "env", Value: "prod"}},
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("SearchJournal labels: %v", err)
	}
	if len(matches) != 2 {
		t.Fatalf("label matches = %#v, want two journals", matches)
	}
	if matches[0].JournalID != "jrn_label_old" {
		t.Fatalf("first label match = %#v, want updated label projection first", matches[0])
	}
	if !matches[0].CursorAt.Equal(labelCreatedAt) {
		t.Fatalf("cursor time = %s, want label projection time %s", matches[0].CursorAt, labelCreatedAt)
	}
	if matches[0].CreatedAt.Equal(labelCreatedAt) {
		t.Fatalf("created_at leaked label projection time: %#v", matches[0])
	}

	nextPage, err := store.SearchJournal(ctx, tenantID, journal.SearchRequest{
		Labels: []journal.Label{{Key: "env", Value: "prod"}},
		Limit:  10,
		After: journal.SearchAfter{
			CreatedAt: matches[0].CursorAt,
			JournalID: matches[0].JournalID,
		},
	})
	if err != nil {
		t.Fatalf("SearchJournal labels after cursor: %v", err)
	}
	if len(nextPage) != 1 || nextPage[0].JournalID != "jrn_label_new" {
		t.Fatalf("next label page = %#v, want jrn_label_new only", nextPage)
	}
}

func TestJournalSearchJournalsAppliesTimeBounds(t *testing.T) {
	store := newJournalStore(t)
	ctx := context.Background()
	tenantID := "tenant-journal-time-bounds"

	for _, id := range []string{"jrn_time_old", "jrn_time_new"} {
		if _, err := store.CreateJournal(ctx, tenantID, journal.CreateRequest{
			JournalID: id,
			Kind:      "agent",
			Labels:    []journal.Label{{Key: "env", Value: "prod"}},
		}); err != nil {
			t.Fatalf("CreateJournal %s: %v", id, err)
		}
	}

	oldAt := journal.NormalizeTime(time.Date(2026, 5, 12, 0, 0, 0, 0, time.UTC))
	newAt := journal.NormalizeTime(time.Date(2026, 5, 12, 2, 0, 0, 0, time.UTC))
	cut := journal.NormalizeTime(time.Date(2026, 5, 12, 1, 0, 0, 0, time.UTC))
	setJournalSearchTime(t, store, tenantID, "jrn_time_old", oldAt)
	setJournalSearchTime(t, store, tenantID, "jrn_time_new", newAt)

	labelSince, err := store.SearchJournal(ctx, tenantID, journal.SearchRequest{
		Labels: []journal.Label{{Key: "env", Value: "prod"}},
		Since:  &cut,
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("SearchJournal label since: %v", err)
	}
	requireJournalMatchIDs(t, labelSince, "jrn_time_new")

	labelUntil, err := store.SearchJournal(ctx, tenantID, journal.SearchRequest{
		Labels: []journal.Label{{Key: "env", Value: "prod"}},
		Until:  &cut,
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("SearchJournal label until: %v", err)
	}
	requireJournalMatchIDs(t, labelUntil, "jrn_time_old")

	kindSince, err := store.SearchJournal(ctx, tenantID, journal.SearchRequest{
		Kind:  "agent",
		Since: &cut,
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("SearchJournal kind since: %v", err)
	}
	requireJournalMatchIDs(t, kindSince, "jrn_time_new")

	kindUntil, err := store.SearchJournal(ctx, tenantID, journal.SearchRequest{
		Kind:  "agent",
		Until: &cut,
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("SearchJournal kind until: %v", err)
	}
	requireJournalMatchIDs(t, kindUntil, "jrn_time_old")
}

func setJournalSearchTime(t *testing.T, store *Store, tenantID, journalID string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	if _, err := store.DB().ExecContext(ctx, `UPDATE journals
		SET created_at = ?, updated_at = ?
		WHERE tenant_id = ? AND journal_id = ?`, at, at, tenantID, journalID); err != nil {
		t.Fatalf("set journal time %s: %v", journalID, err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE journal_labels
		SET created_at = ?
		WHERE tenant_id = ? AND journal_id = ?`, at, tenantID, journalID); err != nil {
		t.Fatalf("set journal label time %s: %v", journalID, err)
	}
}

func requireJournalMatchIDs(t *testing.T, matches []journal.SearchMatch, want ...string) {
	t.Helper()
	if len(matches) != len(want) {
		t.Fatalf("matches = %#v, want ids %v", matches, want)
	}
	for i, id := range want {
		if matches[i].JournalID != id {
			t.Fatalf("matches[%d].JournalID = %q, want %q (matches=%#v)", i, matches[i].JournalID, id, matches)
		}
	}
}
