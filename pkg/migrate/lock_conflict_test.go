package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/mem9-ai/dat9/internal/testmysql"
	"github.com/mem9-ai/dat9/pkg/datastore"
)

func TestIsLockConflictError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"mysql deadlock", errors.New("Error 1213 (40001): Deadlock found when trying to get lock; try restarting transaction"), true},
		{"deadlock message only", errors.New("Deadlock found when trying to get lock"), true},
		{"sqlstate 40001 only", errors.New("SQLSTATE 40001 serialization failure"), true},
		{"wrapped", errors.New("backfill file_nodes: Error 1213 (40001): Deadlock found when trying to get lock"), true},
		{"unrelated error", errors.New("Error 1062 (23000): Duplicate entry"), false},
		{"connection error", errors.New("driver: bad connection"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isLockConflictError(tc.err); got != tc.want {
				t.Fatalf("isLockConflictError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// Regression for the CI flake where concurrent backend opens of the same
// tenant DB each ran the idempotent migration and the full-table backfill
// UPDATEs deadlocked (Error 1213). All concurrent runs must now succeed.
func TestSplitTablesMigratorConcurrentRuns(t *testing.T) {
	s, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = s.Close() }()
	ctx := context.Background()
	testmysql.ResetDB(t, s.DB())

	const fileCount = 300
	var fileVals, nodeVals strings.Builder
	for i := range fileCount {
		if i > 0 {
			fileVals.WriteString(",")
			nodeVals.WriteString(",")
		}
		fmt.Fprintf(&fileVals, "('f%04d', 'db9', 'ref%04d', %d, 1, 'CONFIRMED')", i, i, i)
		fmt.Fprintf(&nodeVals, "('n%04d', '/dir/f%04d.txt', '/dir', 'f%04d.txt', 0, 'f%04d', NULL)", i, i, i, i)
	}
	db := s.DB()
	if _, err := db.Exec("INSERT INTO files (file_id, storage_type, storage_ref, size_bytes, revision, status) VALUES " + fileVals.String()); err != nil {
		t.Fatalf("seed files: %v", err)
	}
	if _, err := db.Exec("INSERT INTO file_nodes (node_id, path, parent_path, name, is_directory, file_id, inode_id) VALUES " + nodeVals.String()); err != nil {
		t.Fatalf("seed file_nodes: %v", err)
	}
	if _, err := db.Exec("INSERT INTO file_nodes (node_id, path, parent_path, name, is_directory, file_id, inode_id) VALUES ('d0001', '/dir', '/', 'dir', 1, NULL, NULL)"); err != nil {
		t.Fatalf("seed dir node: %v", err)
	}

	const runners = 4
	var wg sync.WaitGroup
	errs := make([]error, runners)
	for i := range runners {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			conn, err := sql.Open("mysql", testDSN)
			if err != nil {
				errs[idx] = fmt.Errorf("open conn: %w", err)
				return
			}
			defer func() { _ = conn.Close() }()
			if _, err := NewSplitTablesMigrator(conn).Run(ctx); err != nil {
				errs[idx] = err
			}
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("concurrent run %d failed: %v", i, err)
		}
	}

	var inodeCount, orphanNodes int64
	if err := db.QueryRow("SELECT COUNT(*) FROM inodes").Scan(&inodeCount); err != nil {
		t.Fatalf("count inodes: %v", err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM file_nodes WHERE inode_id IS NULL").Scan(&orphanNodes); err != nil {
		t.Fatalf("count orphan nodes: %v", err)
	}
	if inodeCount != fileCount+1 {
		t.Errorf("inodes = %d, want %d", inodeCount, fileCount+1)
	}
	if orphanNodes != 0 {
		t.Errorf("file_nodes without inode_id = %d, want 0", orphanNodes)
	}
}
