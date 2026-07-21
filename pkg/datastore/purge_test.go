package datastore

import (
	"context"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/journal"
	"github.com/mem9-ai/drive9/pkg/tenant/schema"
)

// TestPurgeTenantDataSharedShape seeds rows for two fs_ids across the shared
// table groups, purges one tenant, and asserts its rows are gone everywhere
// while the other tenant is untouched.
func TestPurgeTenantDataSharedShape(t *testing.T) {
	installSharedCoreFSSchema(t)
	installSharedJournalSchema(t)
	installSharedFSLayerSchema(t)
	installSharedGitSchema(t)
	installSharedTables(t, []string{
		"vault_audit_log", "vault_grants", "vault_tokens", "vault_secret_fields",
		"vault_secrets", "vault_deks",
	}, schema.VaultMySQLSharedSchemaStatements())

	ctx := context.Background()
	const fsA, fsB int64 = 4500001, 4500002
	storeA := newSharedStore(t, fsA)
	storeB := newSharedStore(t, fsB)
	now := time.Now()

	for _, tc := range []struct {
		store *Store
		pfx   string
	}{
		{storeA, "purge-a"},
		{storeB, "purge-b"},
	} {
		if err := tc.store.InsertFile(ctx, &File{
			FileID: tc.pfx + "-file", StorageType: StorageDB9, StorageRef: "inline:" + tc.pfx,
			SizeBytes: 1, Revision: 1, Status: StatusConfirmed, CreatedAt: now, ConfirmedAt: &now,
		}); err != nil {
			t.Fatalf("InsertFile %s: %v", tc.pfx, err)
		}
		if err := tc.store.InsertNode(ctx, &FileNode{
			NodeID: tc.pfx + "-node", Path: "/" + tc.pfx + "/f.txt", ParentPath: "/" + tc.pfx,
			Name: "f.txt", FileID: tc.pfx + "-file", CreatedAt: now,
		}); err != nil {
			t.Fatalf("InsertNode %s: %v", tc.pfx, err)
		}
		if _, err := tc.store.CreateJournal(ctx, tc.pfx, journal.CreateRequest{JournalID: "jrn_purge", Kind: "agent"}); err != nil {
			t.Fatalf("CreateJournal %s: %v", tc.pfx, err)
		}
		if _, err := tc.store.AppendJournalEntries(ctx, tc.pfx, "jrn_purge", "app-purge", JournalWriter{Type: "api_key", ID: "k"}, []journal.EntryInput{{Type: "note", Subjects: []string{"file:/x"}}}); err != nil {
			t.Fatalf("AppendJournalEntries %s: %v", tc.pfx, err)
		}
		if _, err := tc.store.InsertFSEvent(ctx, "/"+tc.pfx+"/f.txt", "write", "test", now.UnixMilli()); err != nil {
			t.Fatalf("InsertFSEvent %s: %v", tc.pfx, err)
		}
		if _, err := tc.store.DB().Exec(`INSERT INTO vault_deks (fs_id, wrapped_dek) VALUES (?, X'01')`,
			tc.store.Scope().FsID()); err != nil {
			t.Fatalf("insert vault_deks %s: %v", tc.pfx, err)
		}
	}

	if err := storeA.PurgeTenantData(ctx); err != nil {
		t.Fatalf("PurgeTenantData A: %v", err)
	}

	// Tenant A: everything gone.
	for _, tbl := range []string{
		"file_nodes", "inodes", "contents", "fs_events", "journals", "journal_entries", "vault_deks",
	} {
		if n := countFsIDRows(t, storeA, tbl, fsA); n != 0 {
			t.Fatalf("A %s rows after purge = %d, want 0", tbl, n)
		}
	}
	// Tenant B: fully intact (file, dentry, journal, events, vault row).
	if _, err := storeB.GetFile(ctx, "purge-b-file"); err != nil {
		t.Fatalf("B GetFile after A purge: %v", err)
	}
	if _, err := storeB.GetNode(ctx, "/purge-b/f.txt"); err != nil {
		t.Fatalf("B GetNode after A purge: %v", err)
	}
	entries, err := storeB.ListJournalEntries(ctx, "purge-b", "jrn_purge", 0, 10)
	if err != nil || len(entries) != 1 {
		t.Fatalf("B journal entries = %d, %v; want 1, nil", len(entries), err)
	}
	if n := countFsIDRows(t, storeB, "vault_deks", fsB); n != 1 {
		t.Fatalf("B vault_deks rows = %d, want 1", n)
	}
	eventsB, err := storeB.ListFSEventsSince(ctx, 0, 10)
	if err != nil || len(eventsB) != 1 {
		t.Fatalf("B fs events = %d, %v; want 1, nil", len(eventsB), err)
	}

	// Purge is idempotent: a second run is a no-op success.
	if err := storeA.PurgeTenantData(ctx); err != nil {
		t.Fatalf("PurgeTenantData A second run: %v", err)
	}
}

func TestPurgeTenantDataRejectsStandalone(t *testing.T) {
	store := newJournalStore(t) // standalone scope
	if err := store.PurgeTenantData(context.Background()); err == nil {
		t.Fatal("expected error on standalone store")
	}
}
