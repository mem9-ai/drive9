package schema

import (
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/internal/schemaspec"
)

// TestJournalSharedSchemaMatchesStandaloneModuloFsID pins the shared journal
// DDL to the standalone one: after renaming the tenant discriminator column
// (tenant_id VARCHAR(64) -> fs_id BIGINT), the normalized statements must be
// identical, so the two shapes cannot drift apart silently. The comparison
// uses the MySQL variant, which carries no TiDB-only CLUSTERED keyword.
func TestJournalSharedSchemaMatchesStandaloneModuloFsID(t *testing.T) {
	standalone := JournalTiDBSchemaStatements()
	shared := JournalMySQLSharedSchemaStatements()
	if len(standalone) != len(shared) {
		t.Fatalf("statement count mismatch: standalone %d, shared %d", len(standalone), len(shared))
	}
	for i := range standalone {
		want := schemaspec.NormalizeSQLFragment(standalone[i])
		want = strings.ReplaceAll(want, "tenant_id varchar(64)", "fs_id bigint")
		want = strings.ReplaceAll(want, "tenant_id", "fs_id")
		got := schemaspec.NormalizeSQLFragment(shared[i])
		if got != want {
			t.Errorf("statement %d drift:\nstandalone (tenant_id->fs_id): %s\nshared (mysql): %s", i, want, got)
		}
	}
}

// TestJournalTiDBSharedSchemaDeclaresClusteredPKs ensures the TiDB variant
// declares every composite primary key CLUSTERED (TiDB defaults composite PKs
// to NONCLUSTERED, which would scatter each tenant's rows), and that the
// MySQL variant differs only by the removed keyword.
func TestJournalTiDBSharedSchemaDeclaresClusteredPKs(t *testing.T) {
	tidb := JournalTiDBSharedSchemaStatements()
	mysql := JournalMySQLSharedSchemaStatements()
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
	if clusteredTables != 5 {
		t.Fatalf("clustered primary keys = %d, want 5 (one per journal table)", clusteredTables)
	}
}
