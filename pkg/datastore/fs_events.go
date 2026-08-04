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
		`INSERT INTO fs_events (`+s.scope.InsCols(`path, op, actor, ts`)+`) VALUES (`+s.scope.InsVals(`?, ?, ?, ?`)+`)`,
		s.scope.Args(path, op, actor, ts)...)
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
//
// In shared shape seq stays a table-global AUTO_INCREMENT, so interleaved
// tenants produce holes in each tenant's per-fs_id seq stream. Consumers must
// tolerate the gaps; the SSE gap/reset handling for that lives in pkg/server
// eventbus and is a separate change.
func (s *Store) ListFSEventsSince(ctx context.Context, since int64, limit int) ([]FSEventRow, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT seq, path, op, actor, ts FROM fs_events WHERE `+s.scope.And(`seq > ?`)+` ORDER BY seq LIMIT ?`,
		s.scope.Args(since, limit)...)
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
	q := `SELECT MAX(seq) FROM fs_events`
	var args []any
	if s.scope.Shared() {
		q += ` WHERE fs_id = ?`
		args = s.scope.Args()
	}
	var seq sql.NullInt64
	if err := s.db.QueryRowContext(ctx, q, args...).Scan(&seq); err != nil {
		return 0, fmt.Errorf("latest fs_event seq: %w", err)
	}
	if !seq.Valid {
		return 0, nil
	}
	return seq.Int64, nil
}

// OldestFSEventSeq returns the current min seq in fs_events, or 0 if empty.
func (s *Store) OldestFSEventSeq(ctx context.Context) (int64, error) {
	q := `SELECT MIN(seq) FROM fs_events`
	var args []any
	if s.scope.Shared() {
		q += ` WHERE fs_id = ?`
		args = s.scope.Args()
	}
	var seq sql.NullInt64
	if err := s.db.QueryRowContext(ctx, q, args...).Scan(&seq); err != nil {
		return 0, fmt.Errorf("oldest fs_event seq: %w", err)
	}
	if !seq.Valid {
		return 0, nil
	}
	return seq.Int64, nil
}

// fsEventsCountCap bounds the rows scanned by CountFSEvents. With 7-day
// retention a hot tenant's fs_events table can hold millions of rows, and a
// full-table COUNT(*) per maintenance interval would become expensive; the
// count is an observability gauge, not a correctness input, so a capped count
// is sufficient.
const fsEventsCountCap = 100000

// CountFSEvents returns the number of rows in fs_events, capped at
// fsEventsCountCap: the result is exact below the cap and equal to the cap
// when more rows exist. Used by the tenant worker's piggybacked maintenance to
// report drive9_fs_events_rows so operators can monitor table growth without
// direct DB access.
//
// The cap keeps the query bounded: COUNT(*) is a full table scan on
// TiDB/MySQL InnoDB (unlike PostgreSQL's index-only count), so the count runs
// over a LIMIT-ed subquery instead of the whole table.
func (s *Store) CountFSEvents(ctx context.Context) (int64, error) {
	q := `SELECT COUNT(*) FROM (SELECT 1 FROM fs_events`
	args := s.scope.Args()
	if s.scope.Shared() {
		q += ` WHERE fs_id = ?`
	}
	q += ` LIMIT ?) AS t`
	args = append(args, fsEventsCountCap)
	var count int64
	if err := s.db.QueryRowContext(ctx, q, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("count fs_events: %w", err)
	}
	return count, nil
}

// DeleteFSEventsBefore deletes fs_events rows older than the given threshold
// in batches of at most batchSize rows per DELETE statement, stopping after
// maxBatches batches. It returns the total number of deleted rows; hasMore is
// true when the batch cap was hit while (likely) more rows remain — callers
// should let the leftover drain on the next sweep cycle.
//
// Batching bounds the statement cost: the price of one unbounded DELETE is
// driven by table size, not rows matched, and a 7-day hot tenant can hold
// millions of rows — the first sweep after a long idle over-retention period
// (or after a retention decrease) would otherwise hit TiDB transaction size
// limits.
//
// Retention is gated on created_at (DB server's clock at INSERT time), not on
// the ts field (publisher's clock at event emission). This means the retention
// guarantee is relative to DB insert time, not event time. If a future feature
// lets clients reason about retention by ts, a separate index on ts and a
// ts-based deletion path would be needed. For now, created_at is indexed and
// sufficient because the SSE protocol uses seq (not ts) for cursor management.
func (s *Store) DeleteFSEventsBefore(ctx context.Context, before time.Time, batchSize, maxBatches int) (deleted int64, hasMore bool, err error) {
	if batchSize <= 0 {
		return 0, false, fmt.Errorf("delete fs_events: batchSize must be positive, got %d", batchSize)
	}
	if maxBatches <= 0 {
		return 0, false, fmt.Errorf("delete fs_events: maxBatches must be positive, got %d", maxBatches)
	}
	stmt := `DELETE FROM fs_events WHERE ` + s.scope.And(`created_at < ?`) + ` LIMIT ?`
	for batch := 0; batch < maxBatches; batch++ {
		// Check ctx between batches so a canceled sweep (server shutdown)
		// exits promptly instead of running the full batch cap.
		if err := ctx.Err(); err != nil {
			return deleted, false, fmt.Errorf("delete fs_events before %s (batch %d): %w", before, batch, err)
		}
		res, execErr := s.db.ExecContext(ctx, stmt, s.scope.Args(before, batchSize)...)
		if execErr != nil {
			return deleted, false, fmt.Errorf("delete fs_events before %s (batch %d): %w", before, batch, execErr)
		}
		n, rowsErr := res.RowsAffected()
		if rowsErr != nil {
			return deleted, false, fmt.Errorf("delete fs_events before %s rows affected: %w", before, rowsErr)
		}
		deleted += n
		if n < int64(batchSize) {
			// Short batch: no more rows older than before remain.
			return deleted, false, nil
		}
	}
	// The cap was hit after a full batch: more rows likely remain.
	return deleted, true, nil
}

// DeleteSharedFSEventsBefore is the shared multi-tenant pool variant of
// DeleteFSEventsBefore: it deletes rows older than before across ALL tenants
// (no fs_id predicate), because in the shared schema every tenant lives in
// one physical fs_events table and dead/idle tenants' rows are never reached
// by any per-tenant (fs_id-scoped) sweep. Callers must throttle GLOBALLY (one
// call sweeps the whole pool, so per-tenant throttling would sweep the same
// table repeatedly) and gate on the shared DB already serving traffic, per
// the shared-table storage policy in docs/design/sse-event-log-retention.md.
//
// It refuses to run on non-shared stores: an unscoped DELETE on a dedicated
// tenant DB would be a catastrophic bug, so the shape check is mandatory.
func (s *Store) DeleteSharedFSEventsBefore(ctx context.Context, before time.Time, batchSize, maxBatches int) (deleted int64, hasMore bool, err error) {
	if !s.scope.Shared() {
		return 0, false, fmt.Errorf("delete shared fs_events: store is not shared-schema shape")
	}
	if batchSize <= 0 {
		return 0, false, fmt.Errorf("delete shared fs_events: batchSize must be positive, got %d", batchSize)
	}
	if maxBatches <= 0 {
		return 0, false, fmt.Errorf("delete shared fs_events: maxBatches must be positive, got %d", maxBatches)
	}
	const stmt = `DELETE FROM fs_events WHERE created_at < ? LIMIT ?`
	for batch := 0; batch < maxBatches; batch++ {
		// Check ctx between batches so a canceled sweep (server shutdown)
		// exits promptly instead of running the full batch cap.
		if err := ctx.Err(); err != nil {
			return deleted, false, fmt.Errorf("delete shared fs_events before %s (batch %d): %w", before, batch, err)
		}
		res, execErr := s.db.ExecContext(ctx, stmt, before, batchSize)
		if execErr != nil {
			return deleted, false, fmt.Errorf("delete shared fs_events before %s (batch %d): %w", before, batch, execErr)
		}
		n, rowsErr := res.RowsAffected()
		if rowsErr != nil {
			return deleted, false, fmt.Errorf("delete shared fs_events before %s rows affected: %w", before, rowsErr)
		}
		deleted += n
		if n < int64(batchSize) {
			// Short batch: no more rows older than before remain.
			return deleted, false, nil
		}
	}
	// The cap was hit after a full batch: more rows likely remain.
	return deleted, true, nil
}
