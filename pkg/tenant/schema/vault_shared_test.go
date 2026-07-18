package schema

import (
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/internal/schemaspec"
)

// TestVaultSharedSchemaMatchesStandaloneModuloFsID pins the shared vault DDL
// to the standalone one: for the six tables that carry a tenant discriminator
// column, renaming tenant_id VARCHAR(64) -> fs_id BIGINT in the normalized
// standalone statements must reproduce the shared statements exactly, so the
// two shapes cannot drift apart silently. vault_secret_fields is skipped here
// — it has no tenant column and gains fs_id structurally; it is covered by
// TestVaultSharedSecretFieldsGainsFsID. The comparison uses the MySQL
// variant, which carries no TiDB-only CLUSTERED keyword.
func TestVaultSharedSchemaMatchesStandaloneModuloFsID(t *testing.T) {
	standalone := VaultTiDBSchemaStatements()
	shared := VaultMySQLSharedSchemaStatements()
	if len(standalone) != len(shared) {
		t.Fatalf("statement count mismatch: standalone %d, shared %d", len(standalone), len(shared))
	}
	renamed := 0
	for i := range standalone {
		name, _, ok, err := schemaspec.ParseCreateTableStatement(standalone[i])
		if err != nil || !ok {
			t.Fatalf("parse standalone statement %d: ok=%v err=%v", i, ok, err)
		}
		sharedName, _, ok, err := schemaspec.ParseCreateTableStatement(shared[i])
		if err != nil || !ok || sharedName != name {
			t.Fatalf("statement %d table mismatch: standalone %q, shared %q (ok=%v err=%v)", i, name, sharedName, ok, err)
		}
		if name == "vault_secret_fields" {
			continue
		}
		want := schemaspec.NormalizeSQLFragment(standalone[i])
		want = strings.ReplaceAll(want, "tenant_id varchar(64)", "fs_id bigint")
		want = strings.ReplaceAll(want, "tenant_id", "fs_id")
		got := schemaspec.NormalizeSQLFragment(shared[i])
		if got != want {
			t.Errorf("table %s drift:\nstandalone (tenant_id->fs_id): %s\nshared (mysql): %s", name, want, got)
		}
		renamed++
	}
	if renamed != 6 {
		t.Fatalf("renamed tables = %d, want 6 (all vault tables except vault_secret_fields)", renamed)
	}
}

// TestVaultSharedSecretFieldsGainsFsID pins the one structural change the
// text rewrite cannot express: vault_secret_fields has no tenant column in
// the standalone shape, so the shared table must consist of fs_id BIGINT NOT
// NULL followed by the standalone columns, with the primary key extended to
// (fs_id, secret_id, field_name). The comparison uses the MySQL variant,
// which carries no TiDB-only CLUSTERED keyword.
func TestVaultSharedSecretFieldsGainsFsID(t *testing.T) {
	standaloneDefs := createTableDefinitions(t, VaultTiDBSchemaStatements(), "vault_secret_fields")
	sharedDefs := createTableDefinitions(t, VaultMySQLSharedSchemaStatements(), "vault_secret_fields")

	standaloneCols, standalonePK := splitPrimaryKeyDef(t, standaloneDefs)
	sharedCols, sharedPK := splitPrimaryKeyDef(t, sharedDefs)

	if want := "primary key (secret_id, field_name)"; standalonePK != want {
		t.Fatalf("standalone primary key = %q, want %q", standalonePK, want)
	}
	if want := "primary key (fs_id, secret_id, field_name)"; sharedPK != want {
		t.Errorf("shared primary key = %q, want %q", sharedPK, want)
	}

	wantCols := append([]string{"fs_id bigint not null"}, standaloneCols...)
	if len(sharedCols) != len(wantCols) {
		t.Fatalf("shared column count = %d, want %d", len(sharedCols), len(wantCols))
	}
	for i := range wantCols {
		if sharedCols[i] != wantCols[i] {
			t.Errorf("column %d = %q, want %q", i, sharedCols[i], wantCols[i])
		}
	}
}

// TestVaultTiDBSharedSchemaDeclaresClusteredPKs ensures the TiDB variant
// declares every composite primary key CLUSTERED (TiDB defaults composite PKs
// to NONCLUSTERED, which would scatter each tenant's rows), and that the
// MySQL variant differs only by the removed keyword.
func TestVaultTiDBSharedSchemaDeclaresClusteredPKs(t *testing.T) {
	tidb := VaultTiDBSharedSchemaStatements()
	mysql := VaultMySQLSharedSchemaStatements()
	if len(tidb) != len(mysql) {
		t.Fatalf("variant length mismatch: tidb %d, mysql %d", len(tidb), len(mysql))
	}
	compositePKs := 0
	for i := range tidb {
		if hasCompositePrimaryKey(tidb[i]) {
			compositePKs++
			if !strings.Contains(tidb[i], " CLUSTERED") {
				t.Errorf("statement %d has a composite primary key without CLUSTERED:\n%s", i, tidb[i])
			}
		}
		if strings.Contains(mysql[i], "CLUSTERED") {
			t.Errorf("mysql variant retains CLUSTERED keyword:\n%s", mysql[i])
		}
		if got, want := mysql[i], stripTiDBClusteredKeyword(tidb[i]); got != want {
			t.Errorf("statement %d variants differ beyond the keyword:\ntidb stripped: %s\nmysql: %s", i, want, got)
		}
	}
	if compositePKs != 1 {
		t.Fatalf("composite primary keys = %d, want 1 (vault_secret_fields)", compositePKs)
	}
}

// createTableDefinitions returns the normalized top-level column/constraint
// definitions of the named table in stmts.
func createTableDefinitions(t *testing.T, stmts []string, table string) []string {
	t.Helper()
	for _, stmt := range stmts {
		name, defs, ok, err := schemaspec.ParseCreateTableStatement(stmt)
		if err != nil {
			t.Fatalf("parse statement: %v", err)
		}
		if !ok || name != table {
			continue
		}
		parts := schemaspec.SplitTopLevelComma(defs)
		normalized := make([]string, len(parts))
		for i, part := range parts {
			normalized[i] = schemaspec.NormalizeSQLFragment(part)
		}
		return normalized
	}
	t.Fatalf("table %q not found", table)
	return nil
}

// splitPrimaryKeyDef separates the trailing PRIMARY KEY constraint from the
// column definitions.
func splitPrimaryKeyDef(t *testing.T, defs []string) (cols []string, pk string) {
	t.Helper()
	last := defs[len(defs)-1]
	if !strings.HasPrefix(last, "primary key") {
		t.Fatalf("last definition is not a primary key: %q", last)
	}
	return defs[:len(defs)-1], last
}

// hasCompositePrimaryKey reports whether stmt declares a table-level primary
// key over more than one column (as opposed to an inline single-column
// PRIMARY KEY attribute).
func hasCompositePrimaryKey(stmt string) bool {
	idx := strings.Index(stmt, "PRIMARY KEY (")
	if idx < 0 {
		return false
	}
	rest := stmt[idx+len("PRIMARY KEY ("):]
	end := strings.Index(rest, ")")
	if end < 0 {
		return false
	}
	return strings.Contains(rest[:end], ",")
}
