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
// Uses a direct ExecContext (no transaction) since this is a single autonomous
// INSERT — wrapping it in BEGIN/COMMIT adds 2 unnecessary RTTs per event.
func (s *Store) InsertFSEvent(ctx context.Context, path, op, actor string, ts int64) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO fs_events (path, op, actor, ts) VALUES (?, ?, ?, ?)`,
		path, op, actor, ts)
	if err != nil {
		return 0, fmt.Errorf("insert fs_event: %w", err)
	}
	seq, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("insert fs_event last insert id: %w", err)
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
		var actor sql.NullString
		if err := rows.Scan(&ev.Seq, &ev.Path, &ev.Op, &actor, &ev.Ts); err != nil {
			return nil, fmt.Errorf("scan fs_event: %w", err)
		}
		ev.Actor = actor.String
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

// OldestFSEventSeq returns the current min seq in fs_events, or 0 if empty.
func (s *Store) OldestFSEventSeq(ctx context.Context) (int64, error) {
	var seq sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT MIN(seq) FROM fs_events`).Scan(&seq); err != nil {
		return 0, fmt.Errorf("oldest fs_event seq: %w", err)
	}
	if !seq.Valid {
		return 0, nil
	}
	return seq.Int64, nil
}

// CountFSEvents returns the total number of rows in fs_events. Used by the
// leader cleanup goroutine to report drive9_fs_events_rows so operators can
// monitor table growth without direct DB access.
//
// Note: COUNT(*) is a full table scan on TiDB/MySQL InnoDB (unlike PostgreSQL's
// index-only count). This runs only every fsEventsCleanupInterval (5m) on the
// leader per tenant with a cached backend, so the cost is bounded and
// best-effort. If the table grows to millions of rows per tenant and this
// becomes expensive, switch to a bounded condition (e.g.
// `WHERE created_at > NOW() - INTERVAL 2 HOUR`) or an approximate count from
// information_schema.TABLES.
func (s *Store) CountFSEvents(ctx context.Context) (int64, error) {
	var count int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM fs_events`).Scan(&count); err != nil {
		return 0, fmt.Errorf("count fs_events: %w", err)
	}
	return count, nil
}

// DeleteFSEventsBefore deletes fs_events rows older than the given threshold.
// Retention is gated on created_at (DB server's clock at INSERT time), not on
// the ts field (publisher's clock at event emission). This means the retention
// guarantee is relative to DB insert time, not event time. If a future feature
// lets clients reason about retention by ts, a separate index on ts and a
// ts-based deletion path would be needed. For now, created_at is indexed and
// sufficient because the SSE protocol uses seq (not ts) for cursor management.
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