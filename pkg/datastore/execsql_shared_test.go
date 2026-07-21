package datastore

import (
	"context"
	"errors"
	"testing"
)

// TestExecSQLSharedShapeRejected asserts the raw-SQL channel is closed in
// shared shape: both reads and file_tags writes fail with the sentinel before
// anything executes. No shared schema install is needed — the check fires
// before any table access.
func TestExecSQLSharedShapeRejected(t *testing.T) {
	store := newSharedStore(t, 4400020)
	ctx := context.Background()
	for _, q := range []string{
		"SELECT 1",
		"SELECT path FROM file_nodes",
		"INSERT INTO file_tags (file_id, tag_key, tag_value) VALUES ('a', 'b', 'c')",
		"UPDATE file_tags SET tag_value = 'x' WHERE tag_key = 'y'",
		"DELETE FROM file_tags WHERE file_id = 'a'",
	} {
		if _, err := store.ExecSQL(ctx, q); !errors.Is(err, ErrExecSQLNotSupportedShared) {
			t.Fatalf("ExecSQL(%q) err = %v, want ErrExecSQLNotSupportedShared", q, err)
		}
	}
}
