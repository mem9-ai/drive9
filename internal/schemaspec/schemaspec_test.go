package schemaspec

import "testing"

func TestParseColumnTypeStopsAtCharacterSetAndCollate(t *testing.T) {
	got := ParseColumnType("VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL")
	if got != "VARCHAR(64)" {
		t.Fatalf("ParseColumnType()=%q, want %q", got, "VARCHAR(64)")
	}
}

func TestParseCreateTableStatementAcceptsShowCreateOutput(t *testing.T) {
	tableName, defs, ok, err := ParseCreateTableStatement("CREATE TABLE files (file_id VARCHAR(64) PRIMARY KEY, content_text LONGTEXT)")
	if err != nil {
		t.Fatalf("ParseCreateTableStatement() error: %v", err)
	}
	if !ok {
		t.Fatal("ParseCreateTableStatement() did not recognize CREATE TABLE")
	}
	if tableName != "files" {
		t.Fatalf("tableName=%q, want %q", tableName, "files")
	}
	if defs == "" {
		t.Fatal("expected non-empty definitions")
	}
}

func TestCanonicalStatementForHashIgnoresFormattingOnlySpacing(t *testing.T) {
	left := CanonicalStatementForHash("CREATE TABLE IF NOT EXISTS example_events (event_id VARCHAR(64) PRIMARY KEY, tenant_id VARCHAR(64) NOT NULL)")
	right := CanonicalStatementForHash("\nCREATE TABLE IF NOT EXISTS example_events (\n    event_id VARCHAR(64) PRIMARY KEY,\n    tenant_id VARCHAR(64) NOT NULL\n)\n")
	if left != right {
		t.Fatalf("CanonicalStatementForHash() mismatch: %q != %q", left, right)
	}
}
