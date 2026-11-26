package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	mysql "github.com/go-sql-driver/mysql"
)

// metaCollationTarget is one column whose comparison the schema contract pins to
// a collation, together with the indexes the contract requires around it.
type metaCollationTarget struct {
	table      string
	column     string
	columnType string
	collation  string
	modifySQL  string
	indexes    []metaCollationIndex
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
// the DDL, the migration and the insert's index check from disagreeing: a stale
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
				table:      table.name,
				column:     columnName,
				columnType: column.columnType,
				collation:  column.collation,
				modifySQL:  column.modifySQL,
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

// metaPinnedIndex returns the contract's declaration of one index on a table that has
// a pinned column. Callers that have to check an index — the insert's fail-closed
// check — take it from here instead of restating the columns by hand, so the check and
// the CREATE TABLE cannot disagree.
func metaPinnedIndex(spec metaSchemaSpec, table, indexName string) (metaCollationIndex, bool) {
	for _, target := range metaPinnedCollationTargets(spec) {
		if target.table != table {
			continue
		}
		for _, index := range target.indexes {
			if index.name == indexName {
				return index, true
			}
		}
	}
	return metaCollationIndex{}, false
}

// ensurePinnedCollations converts an existing control plane to the collations the
// contract declares. A case-insensitive collation on an OAuth subject column makes
// the lookup treat "SUBJ" and "subj" as one identity, which hands the caller
// another tenant's owner key; fresh installations get the collation from CREATE
// TABLE and this is the migration for the rest.
//
// It runs before diffMetaSchema, because TiDB refuses MODIFY COLUMN … COLLATE while
// an index covers the column (error 8200) and the diff reports that mismatch without
// a repair statement, so nothing else would convert it.
//
// It is also resumable: TiDB commits each DDL statement on its own, so a run
// interrupted between the drop, the modify and the recreate leaves a column that is
// already binary with a missing index. The index restore therefore does not depend
// on the collation branch, and it runs under the same gate as the conversion.
//
// Rollout: the gate is what makes this build's writers safe, and an insert also
// refuses to run while the index is missing, so a failed recovery cannot be written
// into. A pod running a build from before the gate cannot be held back by it, so the
// conversion must not run while such pods serve traffic — either they are drained
// first, or the migration runs after the last of them is gone. What an old pod can
// do meanwhile is the behaviour this migration is replacing (a case-insensitive
// lookup on the column it has not converted yet); it cannot lower a converted
// column, and its writes can only make the recreate fail, which is fail-closed.
func ensurePinnedCollations(ctx context.Context, db *sql.DB, spec metaSchemaSpec) error {
	for _, target := range metaPinnedCollationTargets(spec) {
		needsSync, err := metaPinnedCollationNeedsSync(ctx, db, target)
		if err != nil {
			return err
		}
		if !needsSync {
			continue
		}
		// Everything that touches the index runs under the binding-writer gate — the
		// conversion AND the recovery of an index an earlier run left missing. A
		// writer that slipped in while the unique index was absent would make the
		// recreate fail, and a run that failed after the drop must not leave the
		// index missing for the next writer either.
		//
		// The state is re-read on the gated connection: the run that failed may have
		// been this process's predecessor, and the gate is what makes the second
		// look authoritative.
		if err := withExternalBindingSchemaLock(ctx, db, func(lockCtx context.Context, conn *sql.Conn) error {
			needsSync, err := metaPinnedCollationNeedsSync(lockCtx, conn, target)
			if err != nil || !needsSync {
				return err
			}
			return syncPinnedCollation(lockCtx, conn, target)
		}); err != nil {
			return err
		}
	}
	return nil
}

// metaQueryer is the subset of *sql.DB and *sql.Conn the migration needs, so the
// gated path can run every statement on the connection that holds the lock.
type metaQueryer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// metaPinnedCollationNeedsSync reports whether the column is missing the pinned
// collation or any index the contract requires around it. A column whose type has
// drifted is left alone: the schema check reports it, and this migration must not
// rewrite a definition it does not own.
func metaPinnedCollationNeedsSync(ctx context.Context, q metaQueryer, target metaCollationTarget) (bool, error) {
	exists, columnType, collation, err := metaColumnCollation(ctx, q, target.table, target.column)
	if err != nil {
		return false, err
	}
	if !exists {
		// The table or column does not exist yet; applying the schema statements
		// creates it with the pinned collation and its indexes.
		return false, nil
	}
	if !strings.EqualFold(normalizeMetaSQLFragment(columnType), normalizeMetaSQLFragment(target.columnType)) {
		if !strings.EqualFold(collation, target.collation) {
			// The mismatch the schema check has to report; converting would also
			// rewrite the column definition.
			return false, nil
		}
	} else if !strings.EqualFold(collation, target.collation) {
		return true, nil
	}
	for _, index := range target.indexes {
		present, healthy, err := metaIndexMatches(ctx, q, target.table, index)
		if err != nil {
			return false, err
		}
		if !present || !healthy {
			return true, nil
		}
	}
	return false, nil
}

// syncPinnedCollation brings one column's collation and its indexes in line with
// the contract. It runs on the gated connection, so TiDB's refusal to change an
// indexed column (error 8200) and the recovery of a missing index are both inside
// the window the gate protects.
func syncPinnedCollation(ctx context.Context, q metaQueryer, target metaCollationTarget) error {
	present := make(map[string]bool, len(target.indexes))
	wrongShape := make(map[string]bool, len(target.indexes))
	for _, index := range target.indexes {
		exists, healthy, err := metaIndexMatches(ctx, q, target.table, index)
		if err != nil {
			return err
		}
		if exists && healthy {
			present[index.name] = true
			continue
		}
		if exists {
			wrongShape[index.name] = true
		}
	}
	_, columnType, collation, err := metaColumnCollation(ctx, q, target.table, target.column)
	if err != nil {
		return err
	}
	typeMatches := strings.EqualFold(normalizeMetaSQLFragment(columnType), normalizeMetaSQLFragment(target.columnType))
	if typeMatches && !strings.EqualFold(collation, target.collation) {
		if err := pinColumnCollation(ctx, q, target, present, wrongShape); err != nil {
			return err
		}
	}
	for _, index := range target.indexes {
		if present[index.name] {
			continue
		}
		// A same-named index that is not the contract's (non-unique, or covering the
		// wrong columns) is replaced, not trusted.
		if wrongShape[index.name] {
			if _, err := q.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s DROP INDEX %s", target.table, index.name)); err != nil && !isIgnorableMetaSchemaError(err) {
				return fmt.Errorf("drop %s.%s before recreating it: %w", target.table, index.name, err)
			}
		}
		if err := restoreMetaCollationIndex(ctx, q, target, index); err != nil {
			return err
		}
		present[index.name] = true
	}
	return nil
}

// pinColumnCollation changes one column's collation. The change is tried first, because
// MySQL accepts it with the covering indexes in place; TiDB refuses it while any index
// covers the column, and only then does the migration look at what covers it. An index
// the contract does not declare is reported instead of dropped — dropping the contract's
// own indexes first and then failing the retry would leave the table worse than it was.
// That refusal is a TiDB behaviour: MySQL 8.4 converts the column with the undeclared
// index still in place, so there is nothing to report there.
func pinColumnCollation(ctx context.Context, q metaQueryer, target metaCollationTarget, present, wrongShape map[string]bool) error {
	_, err := q.ExecContext(ctx, target.modifySQL)
	if err == nil {
		return nil
	}
	if !metaCollationRefusedOverIndex(err) {
		return fmt.Errorf("pin %s.%s collation: %w", target.table, target.column, err)
	}
	covering, err := metaIndexesCoveringColumn(ctx, q, target.table, target.column)
	if err != nil {
		return err
	}
	known := make(map[string]bool, len(target.indexes))
	for _, index := range target.indexes {
		known[strings.ToLower(index.name)] = true
	}
	var extra []string
	for _, name := range covering {
		if !known[strings.ToLower(name)] {
			extra = append(extra, name)
		}
	}
	if len(extra) > 0 {
		return fmt.Errorf("%s.%s collation is not pinned and the column is covered by index(es) %s that the schema contract does not declare; drop them (renaming keeps them covering the column) so the migration can convert it",
			target.table, target.column, strings.Join(extra, ", "))
	}
	// A same-named but wrong-shaped index is in the contract by name, not by shape, so
	// it is neither present nor "extra"; it still blocks the change. It is dropped here
	// and recreated below as the contract declares it.
	for name := range wrongShape {
		if !wrongShape[name] {
			continue
		}
		if _, err := q.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s DROP INDEX %s", target.table, name)); err != nil && !isIgnorableMetaSchemaError(err) {
			return fmt.Errorf("drop %s.%s before the collation change: %w", target.table, name, err)
		}
		wrongShape[name] = false
	}
	var dropped []metaCollationIndex
	var dropErr error
	for _, index := range target.indexes {
		if !present[index.name] {
			continue
		}
		if _, err := q.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s DROP INDEX %s", target.table, index.name)); err != nil {
			dropErr = fmt.Errorf("drop %s.%s before collation change: %w", target.table, index.name, err)
			break
		}
		dropped = append(dropped, index)
	}
	var modifyErr error
	if dropErr == nil {
		if _, modifyErr = q.ExecContext(ctx, target.modifySQL); modifyErr != nil {
			modifyErr = fmt.Errorf("pin %s.%s collation: %w", target.table, target.column, modifyErr)
		}
	}
	// The indexes are restored whatever happened: leaving the table without them would be
	// a worse state than the one this migration found. The restore runs on a context
	// nothing cancels: the migration runs on its own context now, but the restore must
	// still finish the repair it started if a caller ever passes a cancellable one, or an
	// expired attempt would abandon a dropped index.
	restoreCtx := context.WithoutCancel(ctx)
	var restoreErr error
	for _, index := range dropped {
		if err := restoreMetaCollationIndex(restoreCtx, q, target, index); err != nil {
			if restoreErr == nil {
				restoreErr = err
			}
			continue
		}
		present[index.name] = true
	}
	// A failed restore is part of the outcome: the next start recovers the index, but the
	// error this run returns has to say that the table is missing one.
	return errors.Join(dropErr, modifyErr, restoreErr)
}

// restoreMetaCollationIndex recreates one index the contract requires. A unique
// index is preflighted so a resumed migration reports the duplicate rows instead of
// a raw duplicate-key error.
func restoreMetaCollationIndex(ctx context.Context, q metaQueryer, target metaCollationTarget, index metaCollationIndex) error {
	if index.unique {
		var duplicates int64
		checkSQL := fmt.Sprintf("SELECT COUNT(*) FROM (SELECT 1 FROM %s GROUP BY %s HAVING COUNT(*) > 1) AS dup",
			target.table, strings.Join(index.columns, ", "))
		if err := q.QueryRowContext(ctx, checkSQL).Scan(&duplicates); err != nil {
			return fmt.Errorf("check %s.%s duplicates: %w", target.table, index.name, err)
		}
		if duplicates > 0 {
			return fmt.Errorf("refusing to recreate unique index %s.%s: %d duplicate rows", target.table, index.name, duplicates)
		}
	}
	if _, err := q.ExecContext(ctx, index.createSQL); err != nil {
		if isIgnorableMetaSchemaError(err) {
			return nil
		}
		return fmt.Errorf("restore %s.%s: %w", target.table, index.name, err)
	}
	return nil
}

// metaCollationRefusedOverIndex reports the TiDB refusal to change a column's
// collation while an index covers it (error 8200). MySQL accepts the same
// statement, so the drop is only a TiDB requirement. The error number is checked
// first; the message match covers a driver that wraps the server error without
// keeping the number.
func metaCollationRefusedOverIndex(err error) bool {
	if err == nil {
		return false
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return mysqlErr.Number == 8200
	}
	return strings.Contains(strings.ToLower(err.Error()), "unsupported modifying collation")
}

// metaColumnCollation reports whether the column exists and, when it does, the
// collation the database reports for it.
func metaColumnCollation(ctx context.Context, q metaQueryer, table, column string) (bool, string, string, error) {
	var collation sql.NullString
	var columnType string
	err := q.QueryRowContext(ctx, `SELECT collation_name, column_type FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?`,
		table, column).Scan(&collation, &columnType)
	if errors.Is(err, sql.ErrNoRows) {
		return false, "", "", nil
	}
	if err != nil {
		return false, "", "", fmt.Errorf("read %s.%s collation: %w", table, column, err)
	}
	return true, columnType, collation.String, nil
}

// metaIndexMatches reports whether an index with that name is present and whether it
// is the index the contract declares: same uniqueness and the same columns in the
// same order. A same-named non-unique index (a manual schema edit) must not count as
// proof that uniqueness is enforced, and a present-but-wrong index has to be dropped
// before it can be recreated.
func metaIndexMatches(ctx context.Context, q metaQueryer, table string, index metaCollationIndex) (present, healthy bool, err error) {
	rows, err := q.QueryContext(ctx, `SELECT column_name, non_unique
		FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?
		ORDER BY seq_in_index`, table, index.name)
	if err != nil {
		return false, false, fmt.Errorf("read %s.%s index: %w", table, index.name, err)
	}
	defer func() { _ = rows.Close() }()
	var columns []string
	unique := false
	for rows.Next() {
		var column string
		var nonUnique int
		if err := rows.Scan(&column, &nonUnique); err != nil {
			return false, false, err
		}
		columns = append(columns, strings.ToLower(column))
		unique = nonUnique == 0
	}
	if err := rows.Err(); err != nil {
		return false, false, err
	}
	if len(columns) == 0 {
		return false, false, nil
	}
	if unique != index.unique {
		return true, false, nil
	}
	return true, strings.Join(columns, ",") == strings.Join(index.columns, ","), nil
}

// metaIndexesCoveringColumn lists every index that covers the column, contract or
// not: TiDB refuses a collation change while ANY index covers it, so an index this
// migration does not know about would make the retry fail after the contract's own
// indexes were already dropped.
func metaIndexesCoveringColumn(ctx context.Context, q metaQueryer, table, column string) ([]string, error) {
	rows, err := q.QueryContext(ctx, `SELECT DISTINCT index_name
		FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?
		ORDER BY index_name`, table, column)
	if err != nil {
		return nil, fmt.Errorf("read %s indexes covering %s: %w", table, column, err)
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
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
