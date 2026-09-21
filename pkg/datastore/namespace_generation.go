package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
)

// promotionNamespaceMutationWriterProtocol is the first writer protocol that
// maintains parent child-set generations on every production namespace
// mutation. Promotion rollout keeps namespace_cas_ready false until all older
// writers are drained, then raises minimum_writer_protocol to this value.
const promotionNamespaceMutationWriterProtocol uint64 = 2

// newNamespaceEdgeIncarnation returns a fresh identity for one dentry
// incarnation. A delete followed by a create at the same path must never
// recover the deleted identity.
func newNamespaceEdgeIncarnation() (string, error) {
	return newPromotionID("ped")
}

// bumpPromotionParentGenerationsTx serializes a namespace mutation with any
// promotion target-CAS reader. It is a no-op until promotion namespace CAS is
// enabled for the tenant database, preserving the existing namespace path
// while the feature gate is off. Callers invoke it before mutating child rows;
// their transaction rollback also rolls back this increment on failure.
func (s *Store) bumpPromotionParentGenerationsTx(ctx context.Context, db execer, parentPaths ...string) error {
	parents := make([]string, 0, len(parentPaths))
	seen := make(map[string]struct{}, len(parentPaths))
	for _, parent := range parentPaths {
		if parent == "" {
			parent = "/"
		}
		if parent != "/" && !strings.HasSuffix(parent, "/") {
			parent += "/"
		}
		if _, ok := seen[parent]; ok {
			continue
		}
		seen[parent] = struct{}{}
		parents = append(parents, parent)
	}
	sort.Strings(parents)

	rows, err := db.QueryContext(ctx, `SELECT tenant_id, minimum_writer_protocol, root_children_generation, admission_state
		FROM promotion_namespace_capabilities
		WHERE namespace_cas_ready = TRUE
		ORDER BY tenant_id FOR UPDATE`)
	if err != nil {
		return fmt.Errorf("lock promotion namespace generation gate: %w", err)
	}
	type rootRow struct {
		tenantID string
		protocol uint64
		current  uint64
		state    string
	}
	var roots []rootRow
	for rows.Next() {
		var row rootRow
		if err := rows.Scan(&row.tenantID, &row.protocol, &row.current, &row.state); err != nil {
			_ = rows.Close()
			return err
		}
		if row.protocol != promotionNamespaceMutationWriterProtocol || row.state != "ACTIVE" {
			_ = rows.Close()
			return ErrPromotionRestoreFenced
		}
		roots = append(roots, row)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(roots) == 0 {
		return nil
	}

	for _, parent := range parents {
		if parent == "/" {
			for _, root := range roots {
				if root.current == math.MaxUint64 {
					return ErrPromotionRecoveryRequired
				}
				result, err := db.ExecContext(ctx, `UPDATE promotion_namespace_capabilities
					SET root_children_generation = root_children_generation + 1
					WHERE tenant_id = ? AND namespace_cas_ready = TRUE
					  AND admission_state = 'ACTIVE' AND minimum_writer_protocol = ?
					  AND root_children_generation = ?`, root.tenantID,
					promotionNamespaceMutationWriterProtocol, root.current)
				if err != nil {
					return fmt.Errorf("advance promotion root generation: %w", err)
				}
				if affected, err := result.RowsAffected(); err != nil || affected != 1 {
					return ErrPromotionRecoveryRequired
				}
			}
			continue
		}

		var isDirectory bool
		var edge string
		var generation uint64
		err := db.QueryRowContext(ctx, `SELECT is_directory, path_edge_incarnation, children_generation
			FROM file_nodes WHERE path_hash = ? AND path = ? FOR UPDATE`,
			fileNodePathHash(parent), parent).Scan(&isDirectory, &edge, &generation)
		if errors.Is(err, sql.ErrNoRows) || !isDirectory || edge == "" || generation == math.MaxUint64 {
			return ErrPromotionRecoveryRequired
		}
		if err != nil {
			return fmt.Errorf("lock promotion parent generation %q: %w", parent, err)
		}
		result, err := db.ExecContext(ctx, `UPDATE file_nodes
			SET children_generation = children_generation + 1
			WHERE path_hash = ? AND path = ? AND is_directory = TRUE
			  AND path_edge_incarnation = ? AND children_generation = ?`,
			fileNodePathHash(parent), parent, edge, generation)
		if err != nil {
			return fmt.Errorf("advance promotion parent generation %q: %w", parent, err)
		}
		if affected, err := result.RowsAffected(); err != nil || affected != 1 {
			return ErrPromotionRecoveryRequired
		}
	}
	return nil
}
