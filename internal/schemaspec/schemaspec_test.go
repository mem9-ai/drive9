package schemaspec

import (
	"errors"
	"testing"

	mysql "github.com/go-sql-driver/mysql"
)

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

func TestIsSafeAddColumnRepairSQLAcceptsDefaultFunction(t *testing.T) {
	stmt := "ALTER TABLE tenants ADD COLUMN created_at DATETIME NOT NULL DEFAULT(CURRENT_TIMESTAMP())"
	if !IsSafeAddColumnRepairSQL(stmt) {
		t.Fatalf("expected %q to be safe", stmt)
	}
}

func TestIsIgnorableMySQLErrorRejectsDuplicateEntry(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "mysql duplicate key name",
			err:  &mysql.MySQLError{Number: 1061, Message: "Duplicate key name 'idx_status'"},
			want: true,
		},
		{
			name: "plain duplicate entry",
			err:  errors.New("duplicate entry 'abc' for key 'uk_hash'"),
			want: false,
		},
		{
			name: "plain already exists",
			err:  errors.New("index already exists"),
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsIgnorableMySQLError(tt.err); got != tt.want {
				t.Fatalf("IsIgnorableMySQLError()=%v, want %v", got, tt.want)
			}
		})
	}
}
