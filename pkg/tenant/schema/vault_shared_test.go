package schema

import (
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/internal/schemaspec"
)

// TestVaultSharedSchemaMatchesStandaloneModuloFsID pins the shared vault DDL
// to the standalone one: for the six tables that carry a tenant discriminator
// column, the shared statement must be exactly the standalone statement with
// tenant_id VARCHAR(64) renamed to fs_id BIGINT, plus two structural changes
// the rename cannot express:
//
//   - the standalone single-column inline PRIMARY KEY on the surrogate id
//     column becomes a table-level composite PRIMARY KEY (fs_id, <id>), so
//     one tenant's rows stay physically co-located;
//   - single-column tenant indexes (idx_vault_*_tenant) are dropped: the
//     composite primary key's fs_id prefix already serves those lookups.
//
// vault_secret_fields is skipped here — it has no tenant column and gains
// fs_id structurally; it is covered by TestVaultSharedSecretFieldsGainsFsID.
// The comparison uses the MySQL variant, which carries no TiDB-only
// CLUSTERED keyword.
func TestVaultSharedSchemaMatchesStandaloneModuloFsID(t *testing.T) {
	standalone := VaultTiDBSchemaStatements()
	shared := VaultMySQLSharedSchemaStatements()
	if len(standalone) != len(shared) {
		t.Fatalf("statement count mismatch: standalone %d, shared %d", len(standalone), len(shared))
	}
	// idColumn maps each table whose inline single-column primary key becomes
	// a composite (fs_id, <id>) primary key in shared shape.
	idColumn := map[string]string{
		"vault_secrets":   "secret_id",
		"vault_tokens":    "token_id",
		"vault_grants":    "grant_id",
		"vault_policies":  "policy_id",
		"vault_audit_log": "event_id",
	}
	// droppedTenantIdx names the single-column tenant indexes removed in
	// shared shape because the composite primary key prefix covers them.
	droppedTenantIdx := map[string]bool{
		"index idx_vault_secrets_tenant (fs_id)": true,
		"index idx_vault_token_tenant (fs_id)":   true,
		"index idx_vault_grants_tenant (fs_id)":  true,
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
		standaloneDefs := createTableDefinitions(t, VaultTiDBSchemaStatements(), name)
		var want []string
		for _, def := range standaloneDefs {
			def = strings.ReplaceAll(def, "tenant_id varchar(64)", "fs_id bigint")
			def = strings.ReplaceAll(def, "tenant_id", "fs_id")
			if idCol, ok := idColumn[name]; ok && strings.HasPrefix(def, idCol+" ") {
				// The inline PRIMARY KEY attribute implies NOT NULL; the
				// composite table-level key makes the column explicitly so.
				def = strings.TrimSuffix(def, " primary key") + " not null"
			}
			if droppedTenantIdx[def] {
				continue
			}
			want = append(want, def)
		}
		if idCol, ok := idColumn[name]; ok {
			// Insert the composite primary key before the first index
			// definition, or at the end when the table has none.
			pk := "primary key (fs_id, " + idCol + ")"
			at := len(want)
			for j, def := range want {
				if strings.HasPrefix(def, "index ") || strings.HasPrefix(def, "unique index ") {
					at = j
					break
				}
			}
			want = append(want[:at], append([]string{pk}, want[at:]...)...)
		}
		got := createTableDefinitions(t, VaultMySQLSharedSchemaStatements(), name)
		if len(got) != len(want) {
			t.Fatalf("table %s definition count drift: got %d, want %d\ngot:  %v\nwant: %v", name, len(got), len(want), got, want)
		}
		for j := range want {
			if got[j] != want[j] {
				t.Errorf("table %s definition %d drift:\nstandalone-derived: %s\nshared (mysql):     %s", name, j, want[j], got[j])
			}
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
	if compositePKs != 6 {
		t.Fatalf("composite primary keys = %d, want 6 (every vault table except vault_deks)", compositePKs)
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
