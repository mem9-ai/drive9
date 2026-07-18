package schema

import (
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/internal/schemaspec"
)

// sharedCoreFSTables lists the ten Core FS tables that
// CoreFSTiDBSharedSchemaStatements mirrors from
// tidbAppEmbeddingBaseSchemaStatements. Git workspace, fs_layer, journal, and
// vault statements in the standalone list are out of scope here.
var sharedCoreFSTables = []string{
	"file_nodes",
	"inodes",
	"contents",
	"semantic",
	"file_tags",
	"uploads",
	"semantic_tasks",
	"file_gc_tasks",
	"llm_usage",
	"fs_events",
}

// sharedCoreFSPhysicalPKTables documents the two exception tables that keep
// their global AUTO_INCREMENT physical primary key unchanged in the shared
// shape (docs/TENANT_DB_REDESIGN.md §5.4) instead of gaining an fs_id-prefixed
// composite primary key.
var sharedCoreFSPhysicalPKTables = map[string]bool{
	"llm_usage": true,
	"fs_events": true,
}

// sharedCoreFSUnprefixedIndexes whitelists the standalone indexes that stay
// without an fs_id prefix in the shared shape (both live on the exception
// tables above).
var sharedCoreFSUnprefixedIndexes = map[string]bool{
	"idx_llm_usage_created": true,
	"idx_fs_events_created": true,
}

// sharedCoreFSOnlyIndexes lists the (fs_id, ...) lookup indexes that exist
// only in the shared shape, with their expected column lists.
var sharedCoreFSOnlyIndexes = map[string][]string{
	"idx_llm_usage_fs":     {"fs_id", "created_at"},
	"idx_fs_events_fs_seq": {"fs_id", "seq"},
}

type sharedCoreFSColumn struct {
	name string
	// def is the normalized column definition with any inline PRIMARY KEY
	// marker removed, so standalone and shared shapes compare equal.
	def string
}

type sharedCoreFSKey struct {
	unique bool
	cols   []string
}

type sharedCoreFSTable struct {
	columns []sharedCoreFSColumn
	pk      []string
	keys    map[string]sharedCoreFSKey
}

// TestCoreFSSharedSchemaMatchesStandaloneModuloFsID pins the shared Core FS
// DDL to the standalone one: the shared shape must be exactly the standalone
// shape plus an fs_id BIGINT NOT NULL first column, an fs_id prefix on the
// primary key and on every secondary index / unique constraint — modulo the
// documented llm_usage / fs_events exceptions. Any column, type, default, or
// key change on either side that is not mirrored on the other fails here.
// The comparison uses the MySQL shared variant, which carries no TiDB-only
// CLUSTERED keyword and maps VECTOR(n) columns to LONGTEXT (plain MySQL has
// no vector type); the standalone side is normalized the same way.
func TestCoreFSSharedSchemaMatchesStandaloneModuloFsID(t *testing.T) {
	standalone := parseSharedCoreFSSchema(t, tidbAppEmbeddingBaseSchemaStatements())
	shared := parseSharedCoreFSSchema(t, CoreFSMySQLSharedSchemaStatements())

	for _, tableName := range sharedCoreFSTables {
		standaloneTable, ok := standalone[tableName]
		if !ok {
			t.Fatalf("standalone DDL missing core table %s", tableName)
		}
		sharedTable, ok := shared[tableName]
		if !ok {
			t.Fatalf("shared DDL missing core table %s", tableName)
		}

		// Columns: fs_id BIGINT NOT NULL first, then the standalone columns in
		// the same order with identical definitions (modulo the VECTOR→LONGTEXT
		// MySQL rewrite).
		wantColumns := make([]sharedCoreFSColumn, 0, len(standaloneTable.columns)+1)
		wantColumns = append(wantColumns, sharedCoreFSColumn{name: "fs_id", def: "fs_id bigint not null"})
		for _, col := range standaloneTable.columns {
			wantColumns = append(wantColumns, sharedCoreFSColumn{name: col.name, def: schemaspec.NormalizeSQLFragment(stripTiDBVectorColumnType(col.def))})
		}
		if len(sharedTable.columns) != len(wantColumns) {
			t.Errorf("table %s: shared has %d columns, want %d (fs_id + standalone)",
				tableName, len(sharedTable.columns), len(wantColumns))
		}
		for i := 0; i < len(wantColumns) && i < len(sharedTable.columns); i++ {
			if sharedTable.columns[i] != wantColumns[i] {
				t.Errorf("table %s column %d: shared %q, want %q",
					tableName, i, sharedTable.columns[i].def, wantColumns[i].def)
			}
		}

		// Primary key: fs_id prefix, except on the documented exception tables.
		wantPK := make([]string, 0, len(standaloneTable.pk)+1)
		if !sharedCoreFSPhysicalPKTables[tableName] {
			wantPK = append(wantPK, "fs_id")
		}
		wantPK = append(wantPK, standaloneTable.pk...)
		if !equalSharedCoreFSColumns(sharedTable.pk, wantPK) {
			t.Errorf("table %s: shared primary key %v, want %v", tableName, sharedTable.pk, wantPK)
		}

		// Keys: every standalone key appears in shared with an fs_id prefix,
		// except the whitelisted unprefixed indexes.
		for keyName, standaloneKey := range standaloneTable.keys {
			sharedKey, ok := sharedTable.keys[keyName]
			if !ok {
				t.Errorf("table %s: shared missing key %s", tableName, keyName)
				continue
			}
			if sharedKey.unique != standaloneKey.unique {
				t.Errorf("table %s key %s: shared unique=%t, standalone unique=%t",
					tableName, keyName, sharedKey.unique, standaloneKey.unique)
			}
			wantCols := make([]string, 0, len(standaloneKey.cols)+1)
			if !sharedCoreFSUnprefixedIndexes[keyName] {
				wantCols = append(wantCols, "fs_id")
			}
			wantCols = append(wantCols, standaloneKey.cols...)
			if !equalSharedCoreFSColumns(sharedKey.cols, wantCols) {
				t.Errorf("table %s key %s: shared columns %v, want %v",
					tableName, keyName, sharedKey.cols, wantCols)
			}
		}
		// And shared has no extra keys beyond the whitelisted shared-only ones.
		for keyName, sharedKey := range sharedTable.keys {
			if wantCols, ok := sharedCoreFSOnlyIndexes[keyName]; ok {
				if sharedKey.unique {
					t.Errorf("table %s key %s: shared-only index must not be unique", tableName, keyName)
				}
				if !equalSharedCoreFSColumns(sharedKey.cols, wantCols) {
					t.Errorf("table %s key %s: shared columns %v, want %v",
						tableName, keyName, sharedKey.cols, wantCols)
				}
				continue
			}
			if _, ok := standaloneTable.keys[keyName]; !ok {
				t.Errorf("table %s: shared-only key %s is not whitelisted", tableName, keyName)
			}
		}
	}
}

// TestCoreFSTiDBSharedSchemaDeclaresClusteredPKs ensures the TiDB variant
// declares every composite primary key CLUSTERED (TiDB defaults composite PKs
// to NONCLUSTERED, which would scatter each tenant's rows), that the two
// exception tables keep their inline AUTO_INCREMENT primary key, and that the
// MySQL variant differs only by the mechanical dialect rewrites (CLUSTERED
// removal, VECTOR→LONGTEXT).
func TestCoreFSTiDBSharedSchemaDeclaresClusteredPKs(t *testing.T) {
	tidbStmts := CoreFSTiDBSharedSchemaStatements()
	mysqlStmts := CoreFSMySQLSharedSchemaStatements()
	if len(tidbStmts) != len(mysqlStmts) {
		t.Fatalf("variant length mismatch: tidb %d, mysql %d", len(tidbStmts), len(mysqlStmts))
	}
	if len(tidbStmts) != len(sharedCoreFSTables) {
		t.Fatalf("shared DDL has %d statements, want %d (one per core table)",
			len(tidbStmts), len(sharedCoreFSTables))
	}
	clusteredTables := 0
	for i := range tidbStmts {
		tableName, _, ok, err := schemaspec.ParseCreateTableStatement(tidbStmts[i])
		if err != nil || !ok {
			t.Fatalf("statement %d is not a CREATE TABLE: %v", i, err)
		}
		compositePK := strings.Contains(tidbStmts[i], "PRIMARY KEY (")
		switch {
		case sharedCoreFSPhysicalPKTables[tableName]:
			if compositePK {
				t.Errorf("table %s: exception table must keep its inline AUTO_INCREMENT primary key:\n%s",
					tableName, tidbStmts[i])
			}
			if strings.Contains(tidbStmts[i], "CLUSTERED") {
				t.Errorf("table %s: exception table must not declare CLUSTERED:\n%s", tableName, tidbStmts[i])
			}
		case !compositePK:
			t.Errorf("table %s: no composite primary key and not a documented exception:\n%s",
				tableName, tidbStmts[i])
		case !strings.Contains(tidbStmts[i], " CLUSTERED"):
			t.Errorf("table %s: composite primary key without CLUSTERED:\n%s", tableName, tidbStmts[i])
		default:
			clusteredTables++
		}
		if strings.Contains(mysqlStmts[i], "CLUSTERED") {
			t.Errorf("mysql variant retains CLUSTERED keyword:\n%s", mysqlStmts[i])
		}
		if got, want := mysqlStmts[i], mysqlCompatibleSharedStatement(tidbStmts[i]); got != want {
			t.Errorf("statement %d variants differ beyond the dialect rewrites:\ntidb rewritten: %s\nmysql: %s", i, want, got)
		}
	}
	if want := len(sharedCoreFSTables) - len(sharedCoreFSPhysicalPKTables); clusteredTables != want {
		t.Errorf("clustered composite primary keys = %d, want %d", clusteredTables, want)
	}
}

// TestTiDBSharedOptionalSchemaMatchesStandalone pins the shared optional
// semantic indexes to the standalone ones: the statements reference columns,
// not tenant keys, so they must stay identical.
func TestTiDBSharedOptionalSchemaMatchesStandalone(t *testing.T) {
	standalone := tidbAppEmbeddingOptionalSchemaStatements()
	shared := TiDBSharedOptionalSchemaStatements()
	if len(standalone) != len(shared) {
		t.Fatalf("statement count mismatch: standalone %d, shared %d", len(standalone), len(shared))
	}
	for i := range standalone {
		if got, want := schemaspec.NormalizeSQLFragment(shared[i]), schemaspec.NormalizeSQLFragment(standalone[i]); got != want {
			t.Errorf("optional statement %d drift:\nstandalone: %s\nshared: %s", i, want, got)
		}
	}
}

// parseSharedCoreFSSchema extracts the ten Core FS table shapes from a mixed
// schema statement list: CREATE TABLE bodies supply columns, primary keys,
// and table-level keys; standalone CREATE [UNIQUE] INDEX statements are
// attached to their table afterwards.
func parseSharedCoreFSSchema(t *testing.T, stmts []string) map[string]*sharedCoreFSTable {
	t.Helper()
	wanted := make(map[string]bool, len(sharedCoreFSTables))
	for _, name := range sharedCoreFSTables {
		wanted[name] = true
	}
	type externalIndex struct {
		table string
		name  string
		key   sharedCoreFSKey
	}
	tables := make(map[string]*sharedCoreFSTable)
	var indexes []externalIndex
	for _, stmt := range stmts {
		normalized := schemaspec.NormalizeSQLFragment(stmt)
		switch {
		case strings.HasPrefix(normalized, "create table "):
			tableName, table := parseSharedCoreFSCreateTable(t, stmt)
			if wanted[tableName] {
				tables[tableName] = table
			}
		case strings.HasPrefix(normalized, "create unique index "), strings.HasPrefix(normalized, "create index "):
			tableName, keyName, key := parseSharedCoreFSCreateIndex(t, stmt)
			if wanted[tableName] {
				indexes = append(indexes, externalIndex{table: tableName, name: keyName, key: key})
			}
		}
	}
	for _, idx := range indexes {
		table, ok := tables[idx.table]
		if !ok {
			t.Fatalf("index %s targets core table %s with no CREATE TABLE in the list", idx.name, idx.table)
		}
		table.keys[idx.name] = idx.key
	}
	return tables
}

func parseSharedCoreFSCreateTable(t *testing.T, stmt string) (string, *sharedCoreFSTable) {
	t.Helper()
	tableName, definitions, ok, err := schemaspec.ParseCreateTableStatement(stmt)
	if err != nil || !ok {
		t.Fatalf("parse create table: ok=%t err=%v", ok, err)
	}
	table := &sharedCoreFSTable{keys: make(map[string]sharedCoreFSKey)}
	for _, def := range schemaspec.SplitTopLevelComma(definitions) {
		normalized := schemaspec.NormalizeSQLFragment(def)
		switch {
		case strings.HasPrefix(normalized, "primary key"):
			table.pk = parseSharedCoreFSKeyColumns(t, tableName, normalized)
		case strings.HasPrefix(normalized, "unique key "):
			keyName, cols := parseSharedCoreFSTableKey(t, tableName, normalized, "unique key ")
			table.keys[keyName] = sharedCoreFSKey{unique: true, cols: cols}
		case strings.HasPrefix(normalized, "key "):
			keyName, cols := parseSharedCoreFSTableKey(t, tableName, normalized, "key ")
			table.keys[keyName] = sharedCoreFSKey{unique: false, cols: cols}
		default:
			columnName, rest := schemaspec.SplitIdentifierAndRest(def)
			columnDef := schemaspec.NormalizeSQLFragment(columnName + " " + rest)
			if strings.Contains(columnDef, " primary key") {
				table.pk = append(table.pk, strings.ToLower(columnName))
				columnDef = strings.ReplaceAll(columnDef, " primary key", "")
			}
			table.columns = append(table.columns, sharedCoreFSColumn{name: strings.ToLower(columnName), def: columnDef})
		}
	}
	return tableName, table
}

func parseSharedCoreFSCreateIndex(t *testing.T, stmt string) (tableName, keyName string, key sharedCoreFSKey) {
	t.Helper()
	normalized := schemaspec.NormalizeSQLFragment(stmt)
	rest := ""
	switch {
	case strings.HasPrefix(normalized, "create unique index "):
		key.unique = true
		rest = strings.TrimPrefix(normalized, "create unique index ")
	case strings.HasPrefix(normalized, "create index "):
		rest = strings.TrimPrefix(normalized, "create index ")
	default:
		t.Fatalf("not a CREATE INDEX statement: %q", stmt)
	}
	onIdx := strings.Index(rest, " on ")
	if onIdx < 0 {
		t.Fatalf("malformed CREATE INDEX statement: %q", stmt)
	}
	keyName = strings.TrimSpace(rest[:onIdx])
	tablePart := rest[onIdx+len(" on "):]
	openIdx := strings.Index(tablePart, "(")
	if openIdx < 0 {
		t.Fatalf("malformed CREATE INDEX statement: %q", stmt)
	}
	tableName = strings.TrimSpace(tablePart[:openIdx])
	key.cols = parseSharedCoreFSKeyColumns(t, tableName, tablePart)
	return tableName, keyName, key
}

func parseSharedCoreFSTableKey(t *testing.T, tableName, clause, prefix string) (string, []string) {
	t.Helper()
	rest := strings.TrimPrefix(clause, prefix)
	openIdx := strings.Index(rest, "(")
	if openIdx < 0 {
		t.Fatalf("table %s: malformed key clause %q", tableName, clause)
	}
	return strings.TrimSpace(rest[:openIdx]), parseSharedCoreFSKeyColumns(t, tableName, rest)
}

func parseSharedCoreFSKeyColumns(t *testing.T, tableName, clause string) []string {
	t.Helper()
	openIdx := strings.Index(clause, "(")
	closeIdx := strings.LastIndex(clause, ")")
	if openIdx < 0 || closeIdx < openIdx {
		t.Fatalf("table %s: malformed key clause %q", tableName, clause)
	}
	parts := strings.Split(clause[openIdx+1:closeIdx], ",")
	cols := make([]string, 0, len(parts))
	for _, part := range parts {
		cols = append(cols, strings.TrimSpace(part))
	}
	return cols
}

func equalSharedCoreFSColumns(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
