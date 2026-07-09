package schema

import (
	"fmt"
	"testing"

	mysql "github.com/go-sql-driver/mysql"
)

func TestIsExpectedSchemaInitRetryError(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		wantTransient bool
		wantExpected  bool
	}{
		{
			name:          "tidb information schema changed code",
			err:           &mysql.MySQLError{Number: 8028, Message: "Information schema is changed during the execution of the statement [try again later]"},
			wantTransient: true,
			wantExpected:  true,
		},
		{
			name:          "wrapped tidb retry message",
			err:           fmt.Errorf("exec ddl: %w", &mysql.MySQLError{Number: 1105, Message: "Information schema is changed during the execution of the statement [try again later]"}),
			wantTransient: true,
			wantExpected:  true,
		},
		{
			name:          "duplicate column",
			err:           &mysql.MySQLError{Number: 1060, Message: "Duplicate column name 'worktree_name'"},
			wantTransient: false,
			wantExpected:  true,
		},
		{
			name:          "table already exists",
			err:           &mysql.MySQLError{Number: 1050, Message: "Table 'fs_nodes' already exists"},
			wantTransient: false,
			wantExpected:  true,
		},
		{
			name:          "ordinary ddl error",
			err:           &mysql.MySQLError{Number: 1146, Message: "Table 'missing' doesn't exist"},
			wantTransient: false,
			wantExpected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsTransientSchemaError(tt.err); got != tt.wantTransient {
				t.Fatalf("IsTransientSchemaError() = %v, want %v", got, tt.wantTransient)
			}
			if got := IsExpectedSchemaInitRetryError(tt.err); got != tt.wantExpected {
				t.Fatalf("IsExpectedSchemaInitRetryError() = %v, want %v", got, tt.wantExpected)
			}
		})
	}
}
