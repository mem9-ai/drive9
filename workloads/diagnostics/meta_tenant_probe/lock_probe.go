package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// runLockProbe answers one question needed by the delete-path optimization:
// does the candidate SELECT ... FOR UPDATE (LEFT JOIN inodes, the shape used by
// scanDeleteCandidateTx) lock the inodes row as well as the file_nodes row?
//
// If it does, lockFileIDsForDeleteTx is a redundant statement (~5-7 ms per
// unlink). If it does not, the extra lock has to stay and the count check
// cannot be made non-locking.
func runLockProbe(ctx context.Context, db *sql.DB, schema string, fsID int64) error {
	var pathHash, path, fileID string
	if err := db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT path_hash, path, file_id FROM %s.file_nodes
		 WHERE fs_id = ? AND is_directory = 0 AND file_id IS NOT NULL AND inode_id IS NULL
		 ORDER BY created_at DESC LIMIT 1`, schema), fsID).Scan(&pathHash, &path, &fileID); err != nil {
		return fmt.Errorf("pick a probe file: %w", err)
	}
	fmt.Printf("probe file: %s (file_id=%s)\n", path, fileID)

	var txnMode string
	var lockWait int
	if err := db.QueryRowContext(ctx, `SELECT @@tidb_txn_mode, @@innodb_lock_wait_timeout`).Scan(&txnMode, &lockWait); err != nil {
		return fmt.Errorf("read session settings: %w", err)
	}
	fmt.Printf("session: tidb_txn_mode=%s innodb_lock_wait_timeout=%d\n", txnMode, lockWait)

	tx1, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx1.Rollback() }()

	rows, err := tx1.QueryContext(ctx, fmt.Sprintf(
		`SELECT fn.file_id FROM %s.file_nodes fn
		 LEFT JOIN %s.inodes i ON i.fs_id = fn.fs_id AND i.inode_id = fn.file_id AND i.status = 'CONFIRMED'
		 WHERE fn.fs_id = ? AND fn.path_hash = ? AND fn.path = ? FOR UPDATE`, schema, schema),
		fsID, pathHash, path)
	if err != nil {
		return fmt.Errorf("candidate select in tx1: %w", err)
	}
	for rows.Next() {
		var scanned sql.NullString
		if err := rows.Scan(&scanned); err != nil {
			_ = rows.Close()
			return err
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	fmt.Println("tx1: candidate SELECT ... FOR UPDATE executed, transaction still open (holding locks)")

	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx2.Rollback() }()
	if _, err := tx2.ExecContext(ctx, `SET innodb_lock_wait_timeout = 1`); err != nil {
		return fmt.Errorf("set lock wait timeout: %w", err)
	}

	start := time.Now()
	var status string
	qerr := tx2.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT status FROM %s.inodes WHERE fs_id = ? AND inode_id = ? FOR UPDATE`, schema),
		fsID, fileID).Scan(&status)
	elapsed := float64(time.Since(start).Microseconds()) / 1000

	switch {
	case qerr == nil:
		fmt.Printf("tx2: acquired the inodes row lock in %.1f ms (status=%s)\n", elapsed, status)
		fmt.Println("VERDICT: the candidate join does NOT lock the inodes row; lockFileIDsForDeleteTx is required")
	case strings.Contains(strings.ToLower(qerr.Error()), "lock wait"):
		fmt.Printf("tx2: blocked on the inodes row lock for %.1f ms then failed (%v)\n", elapsed, qerr)
		fmt.Println("VERDICT: the candidate join DOES lock the inodes row; lockFileIDsForDeleteTx is redundant")
	default:
		fmt.Printf("tx2: unexpected error after %.1f ms: %v\n", elapsed, qerr)
		fmt.Println("VERDICT: inconclusive, investigate the error")
	}

	if err := tx1.Rollback(); err != nil {
		return err
	}
	return nil
}
