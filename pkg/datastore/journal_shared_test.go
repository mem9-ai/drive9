package datastore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/journal"
	"github.com/mem9-ai/drive9/pkg/tenant/schema"
)

// installSharedJournalSchema swaps the 5 journal tables to the shared shape.
func installSharedJournalSchema(t *testing.T) {
	t.Helper()
	journalTables := []string{
		"journal_entry_subjects",
		"journal_entries",
		"journal_append_requests",
		"journal_labels",
		"journals",
	}
	installSharedTables(t, journalTables, schema.JournalMySQLSharedSchemaStatements())
}

// newSharedJournalStore opens a Store bound to the shared journal schema with
// the given fs_id as its tenant row key.
func newSharedJournalStore(t *testing.T, fsID int64) *Store {
	t.Helper()
	return newSharedStore(t, fsID)
}

// runJournalCoreScenario exercises the journal core flow (create, idempotent
// create, conflicting create, append, idempotent append, conflicting append,
// list, search, verify) against a store. It is run against both schema
// shapes to prove behavioral parity.
func runJournalCoreScenario(t *testing.T, store *Store, tenantID, journalID string) {
	t.Helper()
	ctx := context.Background()

	created, err := store.CreateJournal(ctx, tenantID, journal.CreateRequest{
		JournalID: journalID,
		Kind:      "agent",
		Title:     "test run",
		Meta:      map[string]string{"repo": "github.com/mem9-ai/drive9"},
		Labels:    []journal.Label{{Key: "env", Value: "test"}},
	})
	if err != nil {
		t.Fatalf("CreateJournal: %v", err)
	}
	if created.TenantID != tenantID {
		t.Fatalf("created TenantID = %q, want %q", created.TenantID, tenantID)
	}
	if created.HeadHash == "" || created.GenesisHash != created.HeadHash {
		t.Fatalf("genesis/head hash mismatch: %#v", created)
	}

	createdAgain, err := store.CreateJournal(ctx, tenantID, journal.CreateRequest{
		JournalID: journalID,
		Kind:      "agent",
		Title:     "test run",
		Meta:      map[string]string{"repo": "github.com/mem9-ai/drive9"},
		Labels:    []journal.Label{{Key: "env", Value: "test"}},
	})
	if err != nil {
		t.Fatalf("CreateJournal retry: %v", err)
	}
	if createdAgain.JournalID != created.JournalID || createdAgain.HeadHash != created.HeadHash {
		t.Fatalf("idempotent create = %#v, want %#v", createdAgain, created)
	}
	if _, err := store.CreateJournal(ctx, tenantID, journal.CreateRequest{
		JournalID: journalID,
		Kind:      "agent",
		Title:     "different",
	}); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("CreateJournal conflict err = %v, want ErrJournalConflict", err)
	}

	occurred := time.Date(2026, 5, 12, 1, 2, 3, 456789000, time.UTC)
	entryInput := []journal.EntryInput{{
		Type:       "tool.call.completed",
		Status:     "ok",
		OccurredAt: &occurred,
		Subjects:   []string{"tool:exec_command"},
		Summary:    []byte(`{"cmd":"go test ./..."}`),
	}}
	resp, err := store.AppendJournalEntries(ctx, tenantID, journalID, "app_test", JournalWriter{Type: "api_key", ID: "key-1"}, entryInput)
	if err != nil {
		t.Fatalf("AppendJournalEntries: %v", err)
	}
	if resp.FirstSeq != 1 || resp.LastSeq != 1 || resp.Count != 1 || resp.HeadHash == "" {
		t.Fatalf("append response = %#v", resp)
	}
	if resp.Idempotent {
		t.Fatalf("fresh append marked idempotent: %#v", resp)
	}
	respAgain, err := store.AppendJournalEntries(ctx, tenantID, journalID, "app_test", JournalWriter{Type: "api_key", ID: "key-1"}, entryInput)
	if err != nil {
		t.Fatalf("AppendJournalEntries retry: %v", err)
	}
	wantReplay := *resp
	wantReplay.Idempotent = true
	if *respAgain != wantReplay {
		t.Fatalf("idempotent append = %#v, want %#v", respAgain, wantReplay)
	}
	if _, err := store.AppendJournalEntries(ctx, tenantID, journalID, "app_test", JournalWriter{Type: "api_key", ID: "key-1"}, []journal.EntryInput{{
		Type:     "tool.call.completed",
		Status:   "error",
		Subjects: []string{"tool:exec_command"},
	}}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("AppendJournalEntries conflict err = %v, want ErrIdempotencyConflict", err)
	}

	entries, err := store.ListJournalEntries(ctx, tenantID, journalID, 0, 10)
	if err != nil {
		t.Fatalf("ListJournalEntries: %v", err)
	}
	if len(entries) != 1 || entries[0].Seq != 1 || entries[0].Source != journal.SourceSelf {
		t.Fatalf("entries = %#v", entries)
	}
	if entries[0].TenantID != tenantID {
		t.Fatalf("entry TenantID = %q, want %q", entries[0].TenantID, tenantID)
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
	if len(matches) != 1 || matches[0].JournalID != journalID || matches[0].Seq != 1 {
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
	if len(matches) != 1 || matches[0].JournalID != journalID || matches[0].Seq != 1 {
		t.Fatalf("actor matches = %#v", matches)
	}

	verify, err := store.VerifyJournal(ctx, tenantID, journalID)
	if err != nil {
		t.Fatalf("VerifyJournal: %v", err)
	}
	if !verify.OK || !verify.HashChainOK || verify.Entries != 1 || verify.HeadHash != resp.HeadHash {
		t.Fatalf("verify = %#v", verify)
	}
	if verify.ProjectionOK == nil || !*verify.ProjectionOK {
		t.Fatalf("verify projection_ok = %#v, want true", verify.ProjectionOK)
	}
}

// TestJournalSharedShapeParity runs the same scenario used by the standalone
// journal tests against the shared (fs_id) schema shape.
func TestJournalSharedShapeParity(t *testing.T) {
	installSharedJournalSchema(t)
	store := newSharedJournalStore(t, 4200001)
	runJournalCoreScenario(t, store, "tenant-shared-parity", "jrn_shared_parity")
}

// TestJournalSharedShapeCrossTenantIsolation proves rows of one fs_id are
// invisible to another fs_id on the same shared tables, and that the same
// journal_id can coexist under two fs_ids with independent sequences.
func TestJournalSharedShapeCrossTenantIsolation(t *testing.T) {
	installSharedJournalSchema(t)
	ctx := context.Background()
	storeA := newSharedJournalStore(t, 4200002)
	storeB := newSharedJournalStore(t, 4200003)
	tenantA, tenantB := "tenant-iso-a", "tenant-iso-b"

	// Same journal_id under two different fs_ids must coexist.
	for _, tc := range []struct {
		store    *Store
		tenantID string
	}{
		{storeA, tenantA},
		{storeB, tenantB},
	} {
		if _, err := tc.store.CreateJournal(ctx, tc.tenantID, journal.CreateRequest{
			JournalID: "jrn_iso",
			Kind:      "agent",
		}); err != nil {
			t.Fatalf("CreateJournal %s: %v", tc.tenantID, err)
		}
	}
	if _, err := storeA.AppendJournalEntries(ctx, tenantA, "jrn_iso", "app_a", JournalWriter{Type: "api_key", ID: "k"}, []journal.EntryInput{{
		Type:     "note",
		Subjects: []string{"file:/a-only"},
	}}); err != nil {
		t.Fatalf("append A: %v", err)
	}

	// B must not see A's entries; B's own journal has an independent seq space.
	entriesB, err := storeB.ListJournalEntries(ctx, tenantB, "jrn_iso", 0, 10)
	if err != nil {
		t.Fatalf("ListJournalEntries B: %v", err)
	}
	if len(entriesB) != 0 {
		t.Fatalf("B sees %d entries, want 0 (cross-tenant leak)", len(entriesB))
	}
	matchesB, err := storeB.SearchJournal(ctx, tenantB, journal.SearchRequest{Subjects: []string{"file:/a-only"}, Limit: 10})
	if err != nil {
		t.Fatalf("SearchJournal B: %v", err)
	}
	if len(matchesB) != 0 {
		t.Fatalf("B search sees %d matches, want 0 (cross-tenant leak)", len(matchesB))
	}
	respB, err := storeB.AppendJournalEntries(ctx, tenantB, "jrn_iso", "app_b", JournalWriter{Type: "api_key", ID: "k"}, []journal.EntryInput{{
		Type:     "note",
		Subjects: []string{"file:/b-only"},
	}})
	if err != nil {
		t.Fatalf("append B: %v", err)
	}
	if respB.FirstSeq != 1 {
		t.Fatalf("B first seq = %d, want 1 (seq space must be per fs_id)", respB.FirstSeq)
	}

	// A journal id that only A created must be invisible to B.
	if _, err := storeA.CreateJournal(ctx, tenantA, journal.CreateRequest{JournalID: "jrn_a_only", Kind: "agent"}); err != nil {
		t.Fatalf("CreateJournal jrn_a_only: %v", err)
	}
	if _, err := storeB.ListJournalEntries(ctx, tenantB, "jrn_a_only", 0, 10); !errors.Is(err, ErrNotFound) {
		t.Fatalf("B list jrn_a_only err = %v, want ErrNotFound", err)
	}
	if _, err := storeB.VerifyJournal(ctx, tenantB, "jrn_a_only"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("B verify jrn_a_only err = %v, want ErrNotFound", err)
	}
}

// TestJournalSharedShapeStoresFsID asserts every journal table row carries
// the scope's fs_id as its tenant discriminator.
func TestJournalSharedShapeStoresFsID(t *testing.T) {
	installSharedJournalSchema(t)
	const fsID int64 = 4200004
	store := newSharedJournalStore(t, fsID)
	runJournalCoreScenario(t, store, "tenant-fsid-check", "jrn_fsid")

	for _, tbl := range []string{"journals", "journal_labels", "journal_append_requests", "journal_entries", "journal_entry_subjects"} {
		var got int64
		err := store.DB().QueryRow("SELECT COUNT(*) FROM "+tbl+" WHERE fs_id != ?", fsID).Scan(&got)
		if err != nil {
			t.Fatalf("count %s rows with foreign fs_id: %v", tbl, err)
		}
		if got != 0 {
			t.Fatalf("%s has %d rows with fs_id != %d", tbl, got, fsID)
		}
		var total int64
		if err := store.DB().QueryRow("SELECT COUNT(*) FROM " + tbl).Scan(&total); err != nil {
			t.Fatalf("count %s: %v", tbl, err)
		}
		if total == 0 {
			t.Fatalf("%s is empty; scenario should have written rows", tbl)
		}
	}
	// No residual tenant_id column in the shared shape.
	var n int
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = 'journals' AND column_name = 'tenant_id'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("shared journals table still has tenant_id column")
	}
}
