package datastore

import (
	"context"
	"database/sql"
	"errors"
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

type fsEventActorKey struct{}
type fsEventCollectorKey struct{}

// WithFSEventActor attaches the actor recorded for filesystem mutation events.
func WithFSEventActor(ctx context.Context, actor string) context.Context {
	return context.WithValue(ctx, fsEventActorKey{}, actor)
}

// FSEventActor returns the actor attached to ctx, if any.
func FSEventActor(ctx context.Context) string {
	if v, ok := ctx.Value(fsEventActorKey{}).(string); ok {
		return v
	}
	return ""
}

// FSEventCollector captures durable events appended by mutation transactions so
// callers can fan them out only after the owning transaction commits.
type FSEventCollector struct {
	events []FSEvent
}

// NewFSEventCollector creates an empty durable event collector.
func NewFSEventCollector() *FSEventCollector {
	return &FSEventCollector{}
}

// WithFSEventCollector attaches c to ctx.
func WithFSEventCollector(ctx context.Context, c *FSEventCollector) context.Context {
	if c == nil {
		return ctx
	}
	return context.WithValue(ctx, fsEventCollectorKey{}, c)
}

// Events returns a copy of the collected durable events.
func (c *FSEventCollector) Events() []FSEvent {
	if c == nil || len(c.events) == 0 {
		return nil
	}
	out := make([]FSEvent, len(c.events))
	copy(out, c.events)
	return out
}

func recordCollectedFSEvent(ctx context.Context, ev FSEvent) {
	if c, ok := ctx.Value(fsEventCollectorKey{}).(*FSEventCollector); ok && c != nil {
		c.events = append(c.events, ev)
	}
}

// InsertFSEvent appends a durable filesystem event and returns the assigned
// tenant-local sequence number.
func (s *Store) InsertFSEvent(ctx context.Context, path, op, actor string) (ev FSEvent, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "insert_fs_event", start, &err)

	err = s.InTx(ctx, func(tx *sql.Tx) error {
		var txErr error
		ev, txErr = s.AppendFSEventTx(ctx, tx, path, op, actor)
		return txErr
	})
	return ev, err
}

// AppendFSEventTx appends a durable filesystem event inside the caller's
// transaction. Sequence allocation is also transactional, so rolled-back
// mutations do not consume event sequence numbers.
func (s *Store) AppendFSEventTx(ctx context.Context, db execer, path, op, actor string) (ev FSEvent, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "append_fs_event", start, &err)

	if actor == "" {
		actor = FSEventActor(ctx)
	}
	seq, err := nextFSEventSeqTx(ctx, db)
	if err != nil {
		return FSEvent{}, err
	}
	ts := time.Now().UnixMilli()
	if _, err := db.ExecContext(ctx,
		`INSERT INTO fs_events (seq, path, op, actor, ts) VALUES (?, ?, ?, ?, ?)`,
		seq, path, op, nullStr(actor), ts); err != nil {
		return FSEvent{}, err
	}
	ev = FSEvent{
		Seq:   seq,
		Path:  path,
		Op:    op,
		Actor: actor,
		Ts:    ts,
	}
	recordCollectedFSEvent(ctx, ev)
	return ev, nil
}

func nextFSEventSeqTx(ctx context.Context, db execer) (uint64, error) {
	var seq uint64
	err := db.QueryRowContext(ctx, `SELECT next_seq FROM fs_event_seq WHERE id = 1 FOR UPDATE`).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := db.ExecContext(ctx, `INSERT IGNORE INTO fs_event_seq (id, next_seq) SELECT 1, COALESCE(MAX(seq), 0) + 1 FROM fs_events`); err != nil {
			return 0, err
		}
		err = db.QueryRowContext(ctx, `SELECT next_seq FROM fs_event_seq WHERE id = 1 FOR UPDATE`).Scan(&seq)
	}
	if err != nil {
		return 0, err
	}
	if _, err := db.ExecContext(ctx, `UPDATE fs_event_seq SET next_seq = ? WHERE id = 1`, seq+1); err != nil {
		return 0, err
	}
	return seq, nil
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
