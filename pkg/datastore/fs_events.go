package datastore

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// FSEventRow mirrors the wire-level ChangeEvent for durable fs_events storage.
type FSEventRow struct {
	Seq   uint64
	Path  string
	Op    string
	Actor string
	Ts    int64
}

// InsertFSEvent inserts a filesystem change event row and returns the assigned seq.
func (s *Store) InsertFSEvent(ctx context.Context, path, op, actor string, ts int64) (int64, error) {
	var seq int64
	err := s.InTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`INSERT INTO fs_events (path, op, actor, ts) VALUES (?, ?, ?, ?)`,
			path, op, actor, ts)
		if err != nil {
			return err
		}
		la, err := res.LastInsertId()
		if err != nil {
			return err
		}
		seq = la
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("insert fs_event: %w", err)
	}
	return seq, nil
}

// ListFSEventsSince returns events with seq > since, ordered by seq, up to limit.
func (s *Store) ListFSEventsSince(ctx context.Context, since int64, limit int) ([]FSEventRow, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT seq, path, op, actor, ts FROM fs_events WHERE seq > ? ORDER BY seq LIMIT ?`,
		since, limit)
	if err != nil {
		return nil, fmt.Errorf("list fs_events since %d: %w", since, err)
	}
	defer func() { _ = rows.Close() }()
	var events []FSEventRow
	for rows.Next() {
		var ev FSEventRow
		if err := rows.Scan(&ev.Seq, &ev.Path, &ev.Op, &ev.Actor, &ev.Ts); err != nil {
			return nil, fmt.Errorf("scan fs_event: %w", err)
		}
		events = append(events, ev)
	}
	return events, rows.Err()
}

// LatestFSEventSeq returns the current max seq in fs_events, or 0 if empty.
func (s *Store) LatestFSEventSeq(ctx context.Context) (int64, error) {
	var seq sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT MAX(seq) FROM fs_events`).Scan(&seq); err != nil {
		return 0, fmt.Errorf("latest fs_event seq: %w", err)
	}
	if !seq.Valid {
		return 0, nil
	}
	return seq.Int64, nil
}

// DeleteFSEventsBefore deletes fs_events rows older than the given created_at threshold.
func (s *Store) DeleteFSEventsBefore(ctx context.Context, before time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM fs_events WHERE created_at < ?`, before)
	if err != nil {
		return 0, fmt.Errorf("delete fs_events before %s: %w", before, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return n, nil
}