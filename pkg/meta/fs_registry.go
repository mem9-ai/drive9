package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// withMetaLockConflictRetry runs fn with a few bounded retries on InnoDB
// lock-conflict errors (1213/1205/40001), which are expected when concurrent
// provision/Acquire paths allocate fs_ids for different tenants at the same
// time (unique-index gap locks on fs_registry). Mirrors the retry pattern
// used by the quota reservation path.
func withMetaLockConflictRetry(fn func() error) error {
	const maxAttempts = 4
	var err error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		if !isMetaLockConflictError(err) {
			return err
		}
		time.Sleep(time.Duration(20*(1<<attempt)) * time.Millisecond)
	}
	return err
}

// EnsureFsID returns the internal numeric fs_id for tenantID, allocating a new
// one on first call. The mapping is stable for the lifetime of the tenant and
// is safe to call concurrently: allocation uses INSERT IGNORE against the
// UNIQUE tenant_id column, so a losing racer re-reads the winner's id.
func (s *Store) EnsureFsID(ctx context.Context, tenantID string) (fsID int64, err error) {
	start := time.Now()
	defer observeMeta(ctx, "ensure_fs_id", start, &err)
	if tenantID == "" {
		return 0, fmt.Errorf("tenant id is required")
	}
	fsID, err = s.ResolveFsID(ctx, tenantID)
	if err == nil {
		return fsID, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return 0, err
	}
	if err := withMetaLockConflictRetry(func() error {
		_, err := s.db.ExecContext(ctx,
			`INSERT IGNORE INTO fs_registry (tenant_id) VALUES (?)`, tenantID)
		return err
	}); err != nil {
		return 0, fmt.Errorf("insert fs_registry row: %w", err)
	}
	fsID, err = s.ResolveFsID(ctx, tenantID)
	if err != nil {
		return 0, fmt.Errorf("resolve fs_id after insert: %w", err)
	}
	return fsID, nil
}

// ResolveFsID returns the fs_id for tenantID, or ErrNotFound when the tenant
// has not been registered yet.
func (s *Store) ResolveFsID(ctx context.Context, tenantID string) (fsID int64, err error) {
	start := time.Now()
	defer observeMeta(ctx, "resolve_fs_id", start, &err)
	if tenantID == "" {
		return 0, fmt.Errorf("tenant id is required")
	}
	err = s.db.QueryRowContext(ctx,
		`SELECT fs_id FROM fs_registry WHERE tenant_id = ?`, tenantID).Scan(&fsID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, ErrNotFound
		}
		return 0, fmt.Errorf("resolve fs_id for tenant %s: %w", tenantID, err)
	}
	return fsID, nil
}

// ResolveTenantID returns the tenant UUID for fsID, or ErrNotFound.
func (s *Store) ResolveTenantID(ctx context.Context, fsID int64) (tenantID string, err error) {
	start := time.Now()
	defer observeMeta(ctx, "resolve_tenant_id", start, &err)
	if fsID <= 0 {
		return "", fmt.Errorf("fs_id must be positive")
	}
	err = s.db.QueryRowContext(ctx,
		`SELECT tenant_id FROM fs_registry WHERE fs_id = ?`, fsID).Scan(&tenantID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", fmt.Errorf("resolve tenant for fs_id %d: %w", fsID, err)
	}
	return tenantID, nil
}

// BackfillFsRegistry registers every tenant that has no fs_id yet. It is
// idempotent and intended to run once at startup (leader-gated) so existing
// tenants are pre-allocated fs_ids before the routing layer needs them.
func (s *Store) BackfillFsRegistry(ctx context.Context) (inserted int64, err error) {
	start := time.Now()
	defer observeMeta(ctx, "backfill_fs_registry", start, &err)
	res, err := s.db.ExecContext(ctx,
		`INSERT IGNORE INTO fs_registry (tenant_id) SELECT id FROM tenants`)
	if err != nil {
		return 0, fmt.Errorf("backfill fs_registry: %w", err)
	}
	inserted, err = res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("backfill fs_registry rows affected: %w", err)
	}
	return inserted, nil
}
