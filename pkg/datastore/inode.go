package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Inode represents a row in the inodes table (POSIX inode metadata).
type Inode struct {
	InodeID     string
	SizeBytes   int64
	Revision    int64
	Mode        uint32
	Status      FileStatus
	CreatedAt   time.Time
	Mtime       time.Time
	ConfirmedAt *time.Time
	ExpiresAt   *time.Time
}

// InsertInode inserts an inode row.
func (s *Store) InsertInode(ctx context.Context, inode *Inode) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO inodes
		(`+s.scope.InsCols(`inode_id, size_bytes, revision, mode, status, created_at, mtime, confirmed_at, expires_at`)+`)
		VALUES (`+s.scope.InsVals(`?, ?, ?, ?, ?, ?, ?, ?, ?`)+`)`,
		s.scope.Args(inode.InodeID, inode.SizeBytes, inode.Revision, inode.Mode, inode.Status,
			inode.CreatedAt.UTC(), inode.Mtime.UTC(), nilTime(inode.ConfirmedAt), nilTime(inode.ExpiresAt))...)
	return err
}

// InsertInodeTx inserts an inode row inside an existing transaction.
func (s *Store) InsertInodeTx(db execer, inode *Inode) error {
	_, err := db.Exec(`INSERT INTO inodes
		(`+s.scope.InsCols(`inode_id, size_bytes, revision, mode, status, created_at, mtime, confirmed_at, expires_at`)+`)
		VALUES (`+s.scope.InsVals(`?, ?, ?, ?, ?, ?, ?, ?, ?`)+`)`,
		s.scope.Args(inode.InodeID, inode.SizeBytes, inode.Revision, inode.Mode, inode.Status,
			inode.CreatedAt.UTC(), inode.Mtime.UTC(), nilTime(inode.ConfirmedAt), nilTime(inode.ExpiresAt))...)
	return err
}

// GetInode retrieves an inode by ID.
func (s *Store) GetInode(ctx context.Context, inodeID string) (*Inode, error) {
	row := s.db.QueryRowContext(ctx, `SELECT inode_id, size_bytes, revision, mode, status,
		created_at, mtime, confirmed_at, expires_at
		FROM inodes WHERE `+s.scope.And(`inode_id = ?`), s.scope.Args(inodeID)...)
	return scanInode(row)
}

// UpdateInodeContentTx updates inode metadata (size, revision, status, mtime) inside a transaction.
func (s *Store) UpdateInodeContentTx(db execer, inodeID string, sizeBytes, revision int64, status FileStatus, confirmedAt time.Time) error {
	_, err := db.Exec(`UPDATE inodes SET size_bytes = ?, revision = ?, status = ?, mtime = ?, confirmed_at = ?
		WHERE `+s.scope.And(`inode_id = ?`),
		append([]any{sizeBytes, revision, status, confirmedAt.UTC(), confirmedAt.UTC()}, s.scope.Args(inodeID)...)...)
	return err
}

// UpdateInodeModeTx updates the mode (permission bits) of an inode.
func (s *Store) UpdateInodeModeTx(db execer, inodeID string, mode uint32) error {
	_, err := db.Exec(`UPDATE inodes SET mode = ? WHERE `+s.scope.And(`inode_id = ?`),
		append([]any{mode}, s.scope.Args(inodeID)...)...)
	return err
}

// MarkInodeDeletedTx marks an inode as DELETED inside a transaction.
func (s *Store) MarkInodeDeletedTx(db execer, inodeID string) error {
	_, err := db.Exec(`UPDATE inodes SET status = 'DELETED' WHERE `+s.scope.And(`inode_id = ?`), s.scope.Args(inodeID)...)
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
	err := row.Scan(&inode.InodeID, &inode.SizeBytes, &inode.Revision, &inode.Mode, &inode.Status,
		&inode.CreatedAt, &inode.Mtime, &confirmedAt, &expiresAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("scan inode: %w", err)
	}
	inode.ConfirmedAt = nullTimeValue(confirmedAt)
	inode.ExpiresAt = nullTimeValue(expiresAt)
	return &inode, nil
}
