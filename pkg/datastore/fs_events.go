package datastore

import (
	"context"
	"database/sql"
	"time"
)

// FSEvent is a durable filesystem mutation event used by the SSE stream.
type FSEvent struct {
	Seq   uint64
	Path  string
	Op    string
	Actor string
	Ts    int64
}

const defaultFSEventReplayLimit = 10000

// InsertFSEvent appends a durable filesystem event and returns the assigned
// tenant-local sequence number.
func (s *Store) InsertFSEvent(ctx context.Context, path, op, actor string) (ev FSEvent, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "insert_fs_event", start, &err)

	ts := time.Now().UnixMilli()
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO fs_events (path, op, actor, ts) VALUES (?, ?, ?, ?)`,
		path, op, nullStr(actor), ts)
	if err != nil {
		return FSEvent{}, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return FSEvent{}, err
	}
	return FSEvent{
		Seq:   uint64(id),
		Path:  path,
		Op:    op,
		Actor: actor,
		Ts:    ts,
	}, nil
}

// FSEventBounds returns the oldest retained sequence, current head sequence,
// and whether any retained event exists. The count return is 0 for empty logs
// and 1 for non-empty logs; callers only use it as an emptiness signal.
func (s *Store) FSEventBounds(ctx context.Context) (oldestSeq, headSeq uint64, count int64, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "fs_event_bounds", start, &err)

	var oldest, head sql.NullInt64
	err = s.db.QueryRowContext(ctx, `SELECT MIN(seq), MAX(seq) FROM fs_events`).Scan(&oldest, &head)
	if err != nil {
		return 0, 0, 0, err
	}
	if oldest.Valid && oldest.Int64 > 0 {
		oldestSeq = uint64(oldest.Int64)
	}
	if head.Valid && head.Int64 > 0 {
		headSeq = uint64(head.Int64)
		count = 1
	}
	return oldestSeq, headSeq, count, nil
}

// ListFSEventsSince returns durable events with seq > since, ordered by seq.
func (s *Store) ListFSEventsSince(ctx context.Context, since uint64, limit int) (events []FSEvent, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "list_fs_events_since", start, &err)

	if limit <= 0 {
		limit = defaultFSEventReplayLimit
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT seq, path, op, actor, ts
		 FROM fs_events
		 WHERE seq > ?
		 ORDER BY seq ASC
		 LIMIT ?`, since, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]FSEvent, 0, 64)
	for rows.Next() {
		var ev FSEvent
		var actor sql.NullString
		if err := rows.Scan(&ev.Seq, &ev.Path, &ev.Op, &actor, &ev.Ts); err != nil {
			return nil, err
		}
		if actor.Valid {
			ev.Actor = actor.String
		}
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// PruneFSEventsBefore deletes retained events older than keepFromSeq.
func (s *Store) PruneFSEventsBefore(ctx context.Context, keepFromSeq uint64) (deleted int64, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "prune_fs_events", start, &err)

	if keepFromSeq == 0 {
		return 0, nil
	}
	res, err := s.db.ExecContext(ctx, `DELETE FROM fs_events WHERE seq < ?`, keepFromSeq)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
