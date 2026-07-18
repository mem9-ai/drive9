package schema

import (
	"slices"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/internal/schemaspec"
)

// TestGitWorkspaceSharedSchemaMatchesStandaloneModuloFsID pins the shared git
// workspace DDL to the standalone one: every shared table must be the
// standalone table plus an fs_id BIGINT NOT NULL discriminator column in
// first position, with fs_id prefixed onto the primary key and every unique
// key / index. The comparison uses the MySQL variant, which carries no
// TiDB-only CLUSTERED keyword; the repair-style ALTERs in the standalone list
// are folded into the parsed standalone shape so they cannot drift either.
func TestGitWorkspaceSharedSchemaMatchesStandaloneModuloFsID(t *testing.T) {
	assertSharedDriftParity(t, GitWorkspaceTiDBSchemaStatements(), GitWorkspaceMySQLSharedSchemaStatements())
}

// TestGitWorkspaceTiDBSharedSchemaDeclaresClusteredPKs ensures the TiDB
// variant declares every composite primary key CLUSTERED (TiDB defaults
// composite PKs to NONCLUSTERED, which would scatter each tenant's rows), and
// that the MySQL variant differs only by the removed keyword.
func TestGitWorkspaceTiDBSharedSchemaDeclaresClusteredPKs(t *testing.T) {
	assertClusteredVariantParity(t, GitWorkspaceTiDBSharedSchemaStatements(), GitWorkspaceMySQLSharedSchemaStatements(), 5)
}

// driftColumn is one parsed column definition: name, normalized type, and
// normalized attributes (nullability/defaults) with any inline PRIMARY KEY
// marker removed — key membership is tracked separately on driftTable.
type driftColumn struct {
	name  string
	dtype string
	attrs string
}

// driftKey is one parsed non-primary key / unique constraint.
type driftKey struct {
	name   string
	unique bool
	cols   []string
}

// driftTable is the parsed shape of one table: columns in declaration order,
// primary key columns, and secondary keys.
type driftTable struct {
	name    string
	columns []driftColumn
	pk      []string
	keys    []driftKey
}

// assertSharedDriftParity compares a standalone statement list against its
// shared (fs_id) counterpart per table: shared columns must equal fs_id
// BIGINT NOT NULL followed by the standalone columns (order, types, and
// attributes preserved), and the shared primary key / unique keys / indexes
// must equal the standalone constraint columns with an fs_id prefix.
func assertSharedDriftParity(t *testing.T, standalone, shared []string) {
	t.Helper()
	wantTables := parseDriftTables(t, standalone)
	gotTables := parseDriftTables(t, shared)
	for name := range wantTables {
		if _, ok := gotTables[name]; !ok {
			t.Errorf("table %s missing from shared schema", name)
		}
	}
	for name := range gotTables {
		if _, ok := wantTables[name]; !ok {
			t.Errorf("table %s missing from standalone schema", name)
		}
	}
	for name, want := range wantTables {
		got, ok := gotTables[name]
		if !ok {
			continue
		}
		assertTableDriftParity(t, want, got)
	}
}

func assertTableDriftParity(t *testing.T, want, got *driftTable) {
	t.Helper()
	if len(got.columns) != len(want.columns)+1 {
		t.Errorf("table %s: shared column count %d, want %d (standalone %d + fs_id)",
			want.name, len(got.columns), len(want.columns)+1, len(want.columns))
		return
	}
	fsID := got.columns[0]
	if fsID.name != "fs_id" || fsID.dtype != "bigint" || fsID.attrs != "not null" {
		t.Errorf("table %s: first shared column = %q %q %q, want fs_id bigint not null",
			want.name, fsID.name, fsID.dtype, fsID.attrs)
	}
	for i, wc := range want.columns {
		gc := got.columns[i+1]
		wantAttrs := wc.attrs
		if slices.Contains(want.pk, wc.name) && !strings.Contains(wantAttrs, "not null") {
			// Standalone primary key members are implicitly NOT NULL; the
			// shared shape declares it explicitly because the primary key
			// constraint is declared separately from the column.
			wantAttrs = strings.TrimSpace("not null " + wantAttrs)
		}
		if gc.name != wc.name || gc.dtype != wc.dtype || gc.attrs != wantAttrs {
			t.Errorf("table %s: column %d drift: standalone (%s %s %s), shared (%s %s %s)",
				want.name, i, wc.name, wc.dtype, wantAttrs, gc.name, gc.dtype, gc.attrs)
		}
	}
	wantPK := append([]string{"fs_id"}, want.pk...)
	if !slices.Equal(got.pk, wantPK) {
		t.Errorf("table %s: primary key drift: standalone %v, shared %v, want %v",
			want.name, want.pk, got.pk, wantPK)
	}
	gotKeys := make(map[string]driftKey, len(got.keys))
	for _, k := range got.keys {
		gotKeys[k.name] = k
	}
	if len(got.keys) != len(want.keys) {
		t.Errorf("table %s: shared key count %d, want %d", want.name, len(got.keys), len(want.keys))
	}
	for _, wk := range want.keys {
		gk, ok := gotKeys[wk.name]
		if !ok {
			t.Errorf("table %s: key %s missing from shared schema", want.name, wk.name)
			continue
		}
		if gk.unique != wk.unique {
			t.Errorf("table %s: key %s uniqueness drift: standalone unique=%v, shared unique=%v",
				want.name, wk.name, wk.unique, gk.unique)
		}
		wantCols := append([]string{"fs_id"}, wk.cols...)
		if !slices.Equal(gk.cols, wantCols) {
			t.Errorf("table %s: key %s drift: standalone %v, shared %v, want %v",
				want.name, wk.name, wk.cols, gk.cols, wantCols)
		}
	}
	for _, gk := range got.keys {
		found := slices.ContainsFunc(want.keys, func(wk driftKey) bool { return wk.name == gk.name })
		if !found {
			t.Errorf("table %s: unexpected shared key %s", want.name, gk.name)
		}
	}
}

// assertClusteredVariantParity ensures the TiDB shared variant declares every
// primary key CLUSTERED (one per table) and that the MySQL variant differs
// only by the removed keyword.
func assertClusteredVariantParity(t *testing.T, tidb, mysql []string, wantTables int) {
	t.Helper()
	if len(tidb) != len(mysql) {
		t.Fatalf("variant length mismatch: tidb %d, mysql %d", len(tidb), len(mysql))
	}
	clusteredTables := 0
	for i := range tidb {
		if strings.Contains(tidb[i], "PRIMARY KEY") {
			if !strings.Contains(tidb[i], " CLUSTERED") {
				t.Errorf("statement %d has a primary key without CLUSTERED:\n%s", i, tidb[i])
			} else {
				clusteredTables++
			}
		}
		if strings.Contains(mysql[i], "CLUSTERED") {
			t.Errorf("mysql variant retains CLUSTERED keyword:\n%s", mysql[i])
		}
		if got, want := mysql[i], stripTiDBClusteredKeyword(tidb[i]); got != want {
			t.Errorf("statement %d variants differ beyond the keyword:\ntidb stripped: %s\nmysql: %s", i, want, got)
		}
	}
	if clusteredTables != wantTables {
		t.Fatalf("clustered primary keys = %d, want %d (one per table)", clusteredTables, wantTables)
	}
}

// parseDriftTables parses CREATE TABLE / CREATE INDEX / ALTER TABLE ADD COLUMN
// statements into per-table shapes. Standalone lists declare indexes as
// separate CREATE INDEX statements and may carry repair-style ALTERs; shared
// lists declare everything inline — parsing both into the same shape lets the
// drift tests compare them structurally.
func parseDriftTables(t *testing.T, stmts []string) map[string]*driftTable {
	t.Helper()
	tables := make(map[string]*driftTable)
	for _, stmt := range stmts {
		n := schemaspec.NormalizeSQLFragment(stmt)
		switch {
		case strings.HasPrefix(n, "create table"):
			tb := parseDriftCreateTable(t, stmt)
			if _, dup := tables[tb.name]; dup {
				t.Fatalf("duplicate CREATE TABLE for %s", tb.name)
			}
			tables[tb.name] = tb
		case strings.HasPrefix(n, "create unique index"), strings.HasPrefix(n, "create index"):
			key, table := parseDriftCreateIndex(t, stmt)
			tb, ok := tables[table]
			if !ok {
				t.Fatalf("CREATE INDEX %s references unknown table %s", key.name, table)
			}
			tb.keys = append(tb.keys, key)
		case strings.HasPrefix(n, "alter table"):
			table, col := parseDriftAlterAddColumn(t, stmt)
			tb, ok := tables[table]
			if !ok {
				t.Fatalf("ALTER TABLE references unknown table %s", table)
			}
			// Repair-style ADD COLUMNs are folded into the shared CREATE
			// TABLE; the standalone list may repeat columns the CREATE TABLE
			// already declares.
			if !driftHasColumn(tb, col.name) {
				tb.columns = append(tb.columns, col)
			}
		default:
			t.Fatalf("unhandled schema statement: %s", schemaspec.SQLSnippet(stmt))
		}
	}
	return tables
}

func parseDriftCreateTable(t *testing.T, stmt string) *driftTable {
	t.Helper()
	name, defs, ok, err := schemaspec.ParseCreateTableStatement(stmt)
	if err != nil || !ok {
		t.Fatalf("parse CREATE TABLE: ok=%v err=%v", ok, err)
	}
	tb := &driftTable{name: name}
	for _, def := range schemaspec.SplitTopLevelComma(defs) {
		n := schemaspec.NormalizeSQLFragment(def)
		switch {
		case strings.HasPrefix(n, "primary key"):
			tb.pk = splitDriftKeyColumns(def)
		case strings.HasPrefix(n, "unique key "), strings.HasPrefix(n, "unique index "):
			tb.keys = append(tb.keys, driftKey{name: driftKeyName(t, def), unique: true, cols: splitDriftKeyColumns(def)})
		case strings.HasPrefix(n, "key "), strings.HasPrefix(n, "index "):
			tb.keys = append(tb.keys, driftKey{name: driftKeyName(t, def), cols: splitDriftKeyColumns(def)})
		default:
			col, isPK := parseDriftColumn(def)
			tb.columns = append(tb.columns, col)
			if isPK {
				tb.pk = []string{col.name}
			}
		}
	}
	return tb
}

func parseDriftCreateIndex(t *testing.T, stmt string) (driftKey, string) {
	t.Helper()
	n := schemaspec.NormalizeSQLFragment(stmt)
	rest := strings.TrimPrefix(n, "create unique index ")
	unique := rest != n
	if !unique {
		rest = strings.TrimPrefix(n, "create index ")
	}
	rest = strings.TrimPrefix(rest, "if not exists ")
	onIdx := strings.Index(rest, " on ")
	if onIdx < 0 {
		t.Fatalf("CREATE INDEX without ON clause: %s", stmt)
	}
	keyName := rest[:onIdx]
	tableAndCols := rest[onIdx+len(" on "):]
	parenIdx := strings.Index(tableAndCols, "(")
	if parenIdx < 0 {
		t.Fatalf("CREATE INDEX without column list: %s", stmt)
	}
	table := strings.TrimSpace(tableAndCols[:parenIdx])
	key := driftKey{name: keyName, unique: unique, cols: splitDriftKeyColumns(tableAndCols[parenIdx:])}
	return key, table
}

func parseDriftAlterAddColumn(t *testing.T, stmt string) (string, driftColumn) {
	t.Helper()
	n := schemaspec.NormalizeSQLFragment(stmt)
	rest := strings.TrimPrefix(n, "alter table ")
	table, rest := schemaspec.SplitIdentifierAndRest(rest)
	colDef, ok := strings.CutPrefix(rest, "add column ")
	if !ok {
		t.Fatalf("unsupported ALTER TABLE statement (only ADD COLUMN repairs): %s", stmt)
	}
	col, isPK := parseDriftColumn(colDef)
	if isPK {
		t.Fatalf("unexpected ALTER TABLE ADD COLUMN ... PRIMARY KEY: %s", stmt)
	}
	return table, col
}

// parseDriftColumn parses one column definition. It reports whether the
// column carries an inline PRIMARY KEY marker; the marker is stripped from
// the returned attributes because key membership is compared separately.
func parseDriftColumn(def string) (driftColumn, bool) {
	name, rest := schemaspec.SplitIdentifierAndRest(def)
	dtype := schemaspec.ParseColumnType(rest)
	attrs := schemaspec.NormalizeSQLFragment(strings.TrimSpace(rest[len(dtype):]))
	isPK := strings.Contains(attrs, "primary key")
	if isPK {
		attrs = schemaspec.NormalizeSQLFragment(strings.ReplaceAll(attrs, "primary key", ""))
	}
	return driftColumn{
		name:  strings.ToLower(name),
		dtype: schemaspec.NormalizeSQLFragment(dtype),
		attrs: attrs,
	}, isPK
}

// splitDriftKeyColumns extracts the lowercased column list from a key /
// primary key definition, dropping any index prefix lengths (e.g. col(16)).
func splitDriftKeyColumns(def string) []string {
	start := strings.Index(def, "(")
	end := strings.LastIndex(def, ")")
	if start < 0 || end <= start {
		return nil
	}
	parts := strings.Split(def[start+1:end], ",")
	cols := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(strings.Trim(p, "`"))
		if i := strings.Index(p, "("); i >= 0 {
			p = p[:i]
		}
		cols = append(cols, strings.ToLower(p))
	}
	return cols
}

// driftKeyName returns the lowercased key name from an inline key definition
// such as "UNIQUE KEY uk_foo (a, b)" or "KEY idx_foo (a)".
func driftKeyName(t *testing.T, def string) string {
	t.Helper()
	idx := strings.Index(def, "(")
	if idx < 0 {
		t.Fatalf("key definition without column list: %s", def)
	}
	fields := strings.Fields(def[:idx])
	if len(fields) == 0 {
		t.Fatalf("key definition without name: %s", def)
	}
	return strings.ToLower(strings.Trim(fields[len(fields)-1], "`"))
}

func driftHasColumn(tb *driftTable, name string) bool {
	return slices.ContainsFunc(tb.columns, func(c driftColumn) bool { return c.name == name })
}
