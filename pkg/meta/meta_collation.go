package meta

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// metaCollationTarget is one column whose comparison the schema contract pins to
// a collation, together with the indexes the contract requires around it.
type metaCollationTarget struct {
	table     string
	column    string
	collation string
	modifySQL string
	indexes   []metaCollationIndex
}

// metaCollationIndex is one index covering a pinned column, as the contract spells
// it. createSQL is the statement that recreates it, so the migration cannot drift
// from the CREATE TABLE definition.
type metaCollationIndex struct {
	name      string
	createSQL string
	columns   []string
	unique    bool
}

// metaPinnedCollationTargets derives the pinned columns and their indexes from the
// parsed schema contract. Deriving instead of listing them by hand is what keeps
// the DDL, the migration and the repair allow-list from disagreeing: a stale
// hand-written definition would rewrite the column on every boot.
func metaPinnedCollationTargets(spec metaSchemaSpec) []metaCollationTarget {
	var targets []metaCollationTarget
	for _, table := range spec.tables {
		for _, columnName := range sortedMetaColumnNames(table.columns) {
			column := table.columns[columnName]
			if column.collation == "" {
				continue
			}
			target := metaCollationTarget{
				table:     table.name,
				column:    columnName,
				collation: column.collation,
				modifySQL: column.modifySQL,
			}
			for _, indexName := range sortedMetaIndexNames(table.indexes) {
				index := table.indexes[indexName]
				columns := metaIndexColumns(index.createSQL)
				if !containsMetaColumn(columns, columnName) {
					continue
				}
				target.indexes = append(target.indexes, metaCollationIndex{
					name:      indexName,
					createSQL: index.createSQL,
					columns:   columns,
					unique:    strings.HasPrefix(normalizeMetaSQLFragment(index.createSQL), "create unique index "),
				})
			}
			targets = append(targets, target)
		}
	}
	return targets
}

// ensurePinnedCollations converts an existing control plane to the collations the
// contract declares. A case-insensitive collation on an OAuth subject column makes
// the lookup treat "SUBJ" and "subj" as one identity, which hands the caller
// another tenant's owner key; fresh installations get the collation from CREATE
// TABLE and this is the migration for the rest.
//
// It runs before diffMetaSchema, because TiDB refuses MODIFY COLUMN … COLLATE while
// an index covers the column (error 8200) and would fail the repair the planner
// would otherwise issue for the mismatch it is about to observe.
//
// It is also resumable: TiDB commits each DDL statement on its own, so a run
// interrupted between the drop, the modify and the recreate leaves a column that is
// already binary with a missing index. The index restore therefore does not depend
// on the collation branch.
func ensurePinnedCollations(ctx context.Context, db *sql.DB, spec metaSchemaSpec) error {
	for _, target := range metaPinnedCollationTargets(spec) {
		exists, collation, err := metaColumnCollation(ctx, db, target.table, target.column)
		if err != nil {
			return err
		}
		if !exists {
			// The table or column does not exist yet; applying the schema
			// statements creates it with the pinned collation and its indexes.
			continue
		}
		present := make(map[string]bool, len(target.indexes))
		for _, index := range target.indexes {
			indexed, err := metaIndexExists(ctx, db, target.table, index.name)
			if err != nil {
				return err
			}
			present[index.name] = indexed
		}
		if !strings.EqualFold(collation, target.collation) {
			// Hold the binding-writer gate across the whole conversion: the unique
			// subject index is absent between the drop and the recreate, and a write
			// that slipped in there would make the recreate fail (or, worse, leave a
			// duplicate subject binding behind on another engine).
			if err := withExternalBindingSchemaLock(ctx, db, func(lockCtx context.Context) error {
				return pinColumnCollation(lockCtx, db, target, present)
			}); err != nil {
				return err
			}
		}
		for _, index := range target.indexes {
			if present[index.name] {
				continue
			}
			if err := restoreMetaCollationIndex(ctx, db, target, index); err != nil {
				return err
			}
			present[index.name] = true
		}
	}
	return nil
}

// pinColumnCollation changes one column's collation. MySQL accepts the MODIFY with
// the indexes in place, so that is tried first; TiDB refuses it while an index
// covers the column, so on that error the covering indexes are dropped, the change
// retried, and the indexes recreated. present is updated with what was dropped, so
// the caller restores whatever this left out.
func pinColumnCollation(ctx context.Context, db *sql.DB, target metaCollationTarget, present map[string]bool) error {
	_, err := db.ExecContext(ctx, target.modifySQL)
	if err == nil {
		return nil
	}
	if !metaCollationRefusedOverIndex(err) {
		return fmt.Errorf("pin %s.%s collation: %w", target.table, target.column, err)
	}
	var dropped []metaCollationIndex
	for _, index := range target.indexes {
		if !present[index.name] {
			continue
		}
		if _, dropErr := db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s DROP INDEX %s", target.table, index.name)); dropErr != nil {
			return fmt.Errorf("drop %s.%s before collation change: %w", target.table, index.name, dropErr)
		}
		dropped = append(dropped, index)
	}
	_, modifyErr := db.ExecContext(ctx, target.modifySQL)
	// The indexes are restored whether or not the retry worked: leaving the table
	// without them would be a worse state than the one this migration found.
	for _, index := range dropped {
		if restoreErr := restoreMetaCollationIndex(ctx, db, target, index); restoreErr != nil {
			return restoreErr
		}
		present[index.name] = true
	}
	if modifyErr != nil {
		return fmt.Errorf("pin %s.%s collation: %w", target.table, target.column, modifyErr)
	}
	return nil
}

// restoreMetaCollationIndex recreates one index the contract requires. A unique
// index is preflighted so a resumed migration reports the duplicate rows instead of
// a raw duplicate-key error.
func restoreMetaCollationIndex(ctx context.Context, db *sql.DB, target metaCollationTarget, index metaCollationIndex) error {
	if index.unique {
		var duplicates int64
		checkSQL := fmt.Sprintf("SELECT COUNT(*) FROM (SELECT 1 FROM %s GROUP BY %s HAVING COUNT(*) > 1) AS dup",
			target.table, strings.Join(index.columns, ", "))
		if err := db.QueryRowContext(ctx, checkSQL).Scan(&duplicates); err != nil {
			return fmt.Errorf("check %s.%s duplicates: %w", target.table, index.name, err)
		}
		if duplicates > 0 {
			return fmt.Errorf("refusing to recreate unique index %s.%s: %d duplicate rows", target.table, index.name, duplicates)
		}
	}
	if _, err := db.ExecContext(ctx, index.createSQL); err != nil {
		if isIgnorableMetaSchemaError(err) {
			return nil
		}
		return fmt.Errorf("restore %s.%s: %w", target.table, index.name, err)
	}
	return nil
}

// metaCollationRefusedOverIndex reports the TiDB refusal to change a column's
// collation while an index covers it (error 8200). MySQL accepts the same
// statement, so the drop is only a TiDB requirement.
func metaCollationRefusedOverIndex(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "unsupported modifying collation")
}

// metaColumnCollation reports whether the column exists and, when it does, the
// collation the database reports for it.
func metaColumnCollation(ctx context.Context, db *sql.DB, table, column string) (bool, string, error) {
	var collation sql.NullString
	err := db.QueryRowContext(ctx, `SELECT collation_name FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?`,
		table, column).Scan(&collation)
	if err == sql.ErrNoRows {
		return false, "", nil
	}
	if err != nil {
		return false, "", fmt.Errorf("read %s.%s collation: %w", table, column, err)
	}
	return true, collation.String, nil
}

// metaIndexColumns lists the columns an index create statement covers, e.g.
// "CREATE UNIQUE INDEX uk ON t (provider, subject_key)" -> [provider subject_key].
func metaIndexColumns(createSQL string) []string {
	open := strings.Index(createSQL, "(")
	close := strings.LastIndex(createSQL, ")")
	if open < 0 || close < open {
		return nil
	}
	var columns []string
	for _, part := range strings.Split(createSQL[open+1:close], ",") {
		column := strings.Trim(strings.TrimSpace(part), "`")
		if cut := strings.IndexAny(column, "( "); cut > 0 {
			column = column[:cut]
		}
		if column == "" {
			continue
		}
		columns = append(columns, strings.ToLower(column))
	}
	return columns
}

func containsMetaColumn(columns []string, want string) bool {
	for _, column := range columns {
		if column == want {
			return true
		}
	}
	return false
}
