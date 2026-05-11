package datastore

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Inode represents a row in the inodes table (POSIX inode metadata).
type Inode struct {
	InodeID     string
	SizeBytes   int64
	Revision    int64
	Status      FileStatus
	CreatedAt   time.Time
	Mtime       time.Time
	ConfirmedAt *time.Time
	ExpiresAt   *time.Time
}

// InsertInode inserts an inode row.
func (s *Store) InsertInode(ctx context.Context, inode *Inode) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO inodes
		(inode_id, size_bytes, revision, status, created_at, mtime, confirmed_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		inode.InodeID, inode.SizeBytes, inode.Revision, inode.Status,
		inode.CreatedAt.UTC(), inode.Mtime.UTC(), nilTime(inode.ConfirmedAt), nilTime(inode.ExpiresAt))
	return err
}

// InsertInodeTx inserts an inode row inside an existing transaction.
func (s *Store) InsertInodeTx(db execer, inode *Inode) error {
	_, err := db.Exec(`INSERT INTO inodes
		(inode_id, size_bytes, revision, status, created_at, mtime, confirmed_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		inode.InodeID, inode.SizeBytes, inode.Revision, inode.Status,
		inode.CreatedAt.UTC(), inode.Mtime.UTC(), nilTime(inode.ConfirmedAt), nilTime(inode.ExpiresAt))
	return err
}

// GetInode retrieves an inode by ID.
func (s *Store) GetInode(ctx context.Context, inodeID string) (*Inode, error) {
	row := s.db.QueryRowContext(ctx, `SELECT inode_id, size_bytes, revision, status,
		created_at, mtime, confirmed_at, expires_at
		FROM inodes WHERE inode_id = ?`, inodeID)
	return scanInode(row)
}

// UpdateInodeContentTx updates inode metadata (size, revision, status, mtime) inside a transaction.
func (s *Store) UpdateInodeContentTx(db execer, inodeID string, sizeBytes, revision int64, status FileStatus, confirmedAt time.Time) error {
	_, err := db.Exec(`UPDATE inodes SET size_bytes = ?, revision = ?, status = ?, mtime = ?, confirmed_at = ?
		WHERE inode_id = ?`,
		sizeBytes, revision, status, confirmedAt.UTC(), confirmedAt.UTC(), inodeID)
	return err
}

// MarkInodeDeletedTx marks an inode as DELETED inside a transaction.
func (s *Store) MarkInodeDeletedTx(db execer, inodeID string) error {
	_, err := db.Exec(`UPDATE inodes SET status = 'DELETED' WHERE inode_id = ?`, inodeID)
	return err
}

func nullTimeValue(v sql.NullTime) *time.Time {
	if v.Valid {
		t := v.Time
		return &t
	}
	return nil
}

func scanInode(row *sql.Row) (*Inode, error) {
	var inode Inode
	var confirmedAt, expiresAt sql.NullTime
	err := row.Scan(&inode.InodeID, &inode.SizeBytes, &inode.Revision, &inode.Status,
		&inode.CreatedAt, &inode.Mtime, &confirmedAt, &expiresAt)
	if err != nil {
		return nil, fmt.Errorf("scan inode: %w", err)
	}
	inode.ConfirmedAt = nullTimeValue(confirmedAt)
	inode.ExpiresAt = nullTimeValue(expiresAt)
	return &inode, nil
}
