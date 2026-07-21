package datastore

import (
	"context"
	"fmt"
)

// purgeBatchSize bounds each DELETE batch when purging a tenant from shared
// tables, so a large tenant does not produce one huge statement.
const purgeBatchSize = 10000

// sharedPurgeTables lists every shared-schema table in child-before-parent
// order for tenant data purges. llm_usage is deliberately absent (no shared
// table; the central meta DB ledger is authoritative).
var sharedPurgeTables = []string{
	"journal_entry_subjects",
	"journal_entries",
	"journal_append_requests",
	"journal_labels",
	"journals",
	"vault_audit_log",
	"vault_grants",
	"vault_tokens",
	"vault_secret_fields",
	"vault_secrets",
	"vault_policies",
	"vault_deks",
	"git_workspace_object_packs",
	"git_workspace_overlay",
	"git_workspace_git_state",
	"git_workspace_tree_nodes",
	"git_workspaces",
	"fs_layer_checkpoints",
	"fs_layer_events",
	"fs_layer_tags",
	"fs_layer_entries",
	"fs_layers",
	"fs_events",
	"semantic_tasks",
	"file_gc_tasks",
	"uploads",
	"file_tags",
	"file_nodes",
	"semantic",
	"contents",
	"inodes",
}

// PurgeTenantData deletes all rows belonging to this store's tenant (fs_id)
// from every shared-schema table, in bounded batches. It is only valid on
// shared-scope stores; on a standalone store it returns an error because the
// tables have no fs_id column to restrict by.
//
// The purge runs without a transaction: each batch commits independently, so
// a large tenant's purge is interruptible and does not hold long-lived locks.
// Callers orchestrating a tenant delete must treat the operation as
// resumable — re-running it after a failure simply continues.
func (s *Store) PurgeTenantData(ctx context.Context) error {
	if !s.scope.Shared() {
		return fmt.Errorf("purge tenant data requires shared scope")
	}
	for _, tbl := range sharedPurgeTables {
		for {
			res, err := s.db.ExecContext(ctx,
				"DELETE FROM "+tbl+" WHERE fs_id = ? LIMIT ?", s.scope.FsID(), purgeBatchSize)
			if err != nil {
				if isMissingTableError(err) {
					break
				}
				return fmt.Errorf("purge table %s: %w", tbl, err)
			}
			n, err := res.RowsAffected()
			if err != nil {
				return fmt.Errorf("purge table %s rows affected: %w", tbl, err)
			}
			if n == 0 {
				break
			}
		}
	}
	return nil
}
