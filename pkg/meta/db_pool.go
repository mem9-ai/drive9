package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// SharedDBOrgWildcard is the org_id sentinel matching any TiDB Cloud
// organization that has no exact db_pool row.
const SharedDBOrgWildcard = "*"

// SharedDBRoleShared marks a db_pool row as a multi-tenant shared-schema
// database. A "dedicated" role is reserved for future use and rejected for
// now.
const SharedDBRoleShared = "shared"

// Placement values for TenantPlacement.Placement.
const (
	PlacementShared    = "shared"
	PlacementDedicated = "dedicated"
)

// Schema shape values for TenantPlacement.SchemaShape.
const (
	SchemaShapeStandalone = "standalone"
	SchemaShapeShared     = "shared"
)

const sharedDBStatusActive = "active"

// SharedDB is one physical database registered in db_pool. PasswordCipher
// holds the same encrypted envelope as tenants.db_password; the plaintext
// never crosses this layer. TLSMode is the go-sql-driver tls DSN parameter
// verbatim ("true", "skip-verify", a custom registered config name, or ""
// for plaintext) so the runtime handle reopens the DB with exactly the TLS
// mode it was registered with. MaxTenants of 0 means unlimited.
type SharedDB struct {
	DbID           int64
	OrgID          string
	Role           string
	Host           string
	Port           int
	User           string
	PasswordCipher []byte
	Name           string
	TLSMode        string
	MaxTenants     int
	TenantCount    int
	Status         string
}

// TenantPlacement maps one filesystem (fs_id) to the physical database that
// hosts it. TargetDbID is reserved for future migrations; Epoch is reserved
// for optimistic concurrency during migrations and stays 1 unless a migration
// explicitly bumps it.
type TenantPlacement struct {
	FsID        int64
	DbID        int64
	Placement   string
	SchemaShape string
	Status      string
	TargetDbID  *int64
	Epoch       int64
}

// sharedDBSelectColumns lists the db_pool columns read into SharedDB. The
// `role` column is backtick-quoted because ROLE is reserved in MySQL 8.0.
const sharedDBSelectColumns = "db_id, org_id, `role`, db_host, db_port, db_user, db_password, " +
	"db_name, db_tls, max_tenants, tenant_count, status"

// RegisterSharedDB registers a physical database in db_pool, upserting on the
// uk_db_pool_endpoint (org_id, db_host, db_name) natural key. On a duplicate
// endpoint the connection fields are refreshed and tenant_count is preserved;
// the existing db_id is re-fetched and returned.
func (s *Store) RegisterSharedDB(ctx context.Context, in *SharedDB) (dbID int64, err error) {
	start := time.Now()
	defer observeMeta(ctx, "register_shared_db", start, &err)
	if in == nil {
		return 0, fmt.Errorf("shared db is required")
	}
	if in.Host == "" {
		return 0, fmt.Errorf("db host is required")
	}
	if in.Port <= 0 {
		return 0, fmt.Errorf("db port must be positive")
	}
	if in.User == "" {
		return 0, fmt.Errorf("db user is required")
	}
	if len(in.PasswordCipher) == 0 {
		return 0, fmt.Errorf("db password cipher is required")
	}
	if in.Name == "" {
		return 0, fmt.Errorf("db name is required")
	}
	if in.MaxTenants < 0 {
		return 0, fmt.Errorf("max tenants must not be negative")
	}
	if len(in.TLSMode) > 32 {
		return 0, fmt.Errorf("tls mode %q is too long", in.TLSMode)
	}
	role := in.Role
	if role == "" {
		role = SharedDBRoleShared
	}
	if role != SharedDBRoleShared {
		return 0, fmt.Errorf("unsupported db role %q", role)
	}
	status := in.Status
	if status == "" {
		status = sharedDBStatusActive
	}
	if _, err := s.db.ExecContext(ctx,
		"INSERT INTO db_pool (org_id, `role`, db_host, db_port, db_user, db_password, db_name, "+
			"db_tls, max_tenants, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "+
			"ON DUPLICATE KEY UPDATE db_port = VALUES(db_port), db_user = VALUES(db_user), "+
			"db_password = VALUES(db_password), db_tls = VALUES(db_tls), "+
			"max_tenants = VALUES(max_tenants), status = VALUES(status)",
		in.OrgID, role, in.Host, in.Port, in.User, in.PasswordCipher, in.Name,
		in.TLSMode, in.MaxTenants, status); err != nil {
		return 0, fmt.Errorf("upsert db_pool row: %w", err)
	}
	// ON DUPLICATE KEY UPDATE does not reliably report the existing
	// auto-increment id via LastInsertId, so re-fetch by endpoint.
	err = s.db.QueryRowContext(ctx,
		`SELECT db_id FROM db_pool WHERE org_id = ? AND db_host = ? AND db_name = ?`,
		in.OrgID, in.Host, in.Name).Scan(&dbID)
	if err != nil {
		return 0, fmt.Errorf("resolve db_id after upsert: %w", err)
	}
	return dbID, nil
}

// GetSharedDB returns the db_pool row for dbID, or ErrNotFound.
func (s *Store) GetSharedDB(ctx context.Context, dbID int64) (db *SharedDB, err error) {
	start := time.Now()
	defer observeMeta(ctx, "get_shared_db", start, &err)
	if dbID <= 0 {
		return nil, fmt.Errorf("db_id must be positive")
	}
	db, err = scanSharedDBRow(s.db.QueryRowContext(ctx,
		"SELECT "+sharedDBSelectColumns+" FROM db_pool WHERE db_id = ?", dbID))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get shared db %d: %w", dbID, err)
	}
	return db, nil
}

// FindSharedDBForOrg returns the active shared database serving orgID: an
// exact org_id match wins over the '*' wildcard row. An empty orgID matches
// only the wildcard row. Returns ErrNotFound when neither exists.
func (s *Store) FindSharedDBForOrg(ctx context.Context, orgID string) (db *SharedDB, err error) {
	start := time.Now()
	defer observeMeta(ctx, "find_shared_db_for_org", start, &err)
	if orgID != "" && orgID != SharedDBOrgWildcard {
		db, err = s.findActiveSharedDB(ctx, orgID)
		if err == nil {
			return db, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	db, err = s.findActiveSharedDB(ctx, SharedDBOrgWildcard)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return db, nil
}

func (s *Store) findActiveSharedDB(ctx context.Context, orgID string) (*SharedDB, error) {
	// Capacity is enforced at selection time: a pool at its max_tenants limit
	// is invisible to new placements (0 means unlimited). tenant_count is a
	// denormalized counter, so a rare drift can over- or under-admit; closing
	// that fully needs a transactional reserve, which is a planned follow-up.
	db, err := scanSharedDBRow(s.db.QueryRowContext(ctx,
		"SELECT "+sharedDBSelectColumns+" FROM db_pool "+
			"WHERE org_id = ? AND `role` = ? AND status = ? "+
			"AND (max_tenants = 0 OR tenant_count < max_tenants) ORDER BY db_id LIMIT 1",
		orgID, SharedDBRoleShared, sharedDBStatusActive))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("find shared db for org %q: %w", orgID, err)
	}
	return db, nil
}

// ListSharedDBs returns all active db_pool rows ordered by db_id.
func (s *Store) ListSharedDBs(ctx context.Context) (out []*SharedDB, err error) {
	start := time.Now()
	defer observeMeta(ctx, "list_shared_dbs", start, &err)
	rows, err := s.db.QueryContext(ctx,
		"SELECT "+sharedDBSelectColumns+" FROM db_pool WHERE `role` = ? AND status = ? ORDER BY db_id",
		SharedDBRoleShared, sharedDBStatusActive)
	if err != nil {
		return nil, fmt.Errorf("list shared dbs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var rec SharedDB
		if err = rows.Scan(&rec.DbID, &rec.OrgID, &rec.Role, &rec.Host, &rec.Port, &rec.User,
			&rec.PasswordCipher, &rec.Name, &rec.TLSMode, &rec.MaxTenants, &rec.TenantCount, &rec.Status); err != nil {
			return nil, err
		}
		out = append(out, &rec)
	}
	return out, rows.Err()
}

// IncrSharedDBTenantCount adjusts the denormalized tenant_count of a db_pool
// row by delta (negative to decrement), clamped at >= 0. Returns ErrNotFound
// when dbID is unknown.
func (s *Store) IncrSharedDBTenantCount(ctx context.Context, dbID int64, delta int) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "incr_shared_db_tenant_count", start, &err)
	if dbID <= 0 {
		return fmt.Errorf("db_id must be positive")
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE db_pool SET tenant_count = GREATEST(tenant_count + ?, 0) WHERE db_id = ?`,
		delta, dbID)
	if err != nil {
		return fmt.Errorf("adjust tenant_count for db %d: %w", dbID, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("tenant_count rows affected for db %d: %w", dbID, err)
	}
	if affected > 0 {
		return nil
	}
	// MySQL reports 0 affected rows when the value did not change (delta 0 or
	// a clamp no-op); confirm the row exists before reporting success.
	var one int
	err = s.db.QueryRowContext(ctx, `SELECT 1 FROM db_pool WHERE db_id = ?`, dbID).Scan(&one)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("check shared db %d: %w", dbID, err)
	}
	return nil
}

// UpsertTenantPlacement records or replaces the placement of a filesystem.
// The epoch is left untouched by upserts: it stays at its default of 1 unless
// a migration explicitly bumps it.
func (s *Store) UpsertTenantPlacement(ctx context.Context, p *TenantPlacement) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "upsert_tenant_placement", start, &err)
	if p == nil {
		return fmt.Errorf("tenant placement is required")
	}
	if p.FsID <= 0 {
		return fmt.Errorf("fs_id must be positive")
	}
	if p.DbID <= 0 {
		return fmt.Errorf("db_id must be positive")
	}
	switch p.Placement {
	case PlacementShared, PlacementDedicated:
	default:
		return fmt.Errorf("unsupported placement %q", p.Placement)
	}
	switch p.SchemaShape {
	case SchemaShapeStandalone, SchemaShapeShared:
	default:
		return fmt.Errorf("unsupported schema shape %q", p.SchemaShape)
	}
	status := p.Status
	if status == "" {
		status = sharedDBStatusActive
	}
	switch status {
	case sharedDBStatusActive, "migrating":
	default:
		return fmt.Errorf("unsupported placement status %q", status)
	}
	var target any
	if p.TargetDbID != nil {
		target = *p.TargetDbID
	}
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO tenant_placements (fs_id, db_id, placement, schema_shape, status, target_db_id)
			VALUES (?, ?, ?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE db_id = VALUES(db_id), placement = VALUES(placement),
				schema_shape = VALUES(schema_shape), status = VALUES(status),
				target_db_id = VALUES(target_db_id)`,
		p.FsID, p.DbID, p.Placement, p.SchemaShape, status, target); err != nil {
		return fmt.Errorf("upsert tenant placement for fs_id %d: %w", p.FsID, err)
	}
	return nil
}

// GetTenantPlacement returns the placement for fsID, or ErrNotFound. Callers
// treat a missing row as "standalone, legacy path".
func (s *Store) GetTenantPlacement(ctx context.Context, fsID int64) (p *TenantPlacement, err error) {
	start := time.Now()
	defer observeMeta(ctx, "get_tenant_placement", start, &err)
	if fsID <= 0 {
		return nil, fmt.Errorf("fs_id must be positive")
	}
	var target sql.NullInt64
	p = &TenantPlacement{}
	err = s.db.QueryRowContext(ctx,
		`SELECT fs_id, db_id, placement, schema_shape, status, target_db_id, epoch
			FROM tenant_placements WHERE fs_id = ?`, fsID).
		Scan(&p.FsID, &p.DbID, &p.Placement, &p.SchemaShape, &p.Status, &target, &p.Epoch)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get tenant placement for fs_id %d: %w", fsID, err)
	}
	if target.Valid {
		p.TargetDbID = &target.Int64
	}
	return p, nil
}

// DeleteTenantPlacement removes the placement for fsID. It is idempotent:
// deleting an unknown fsID succeeds.
func (s *Store) DeleteTenantPlacement(ctx context.Context, fsID int64) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "delete_tenant_placement", start, &err)
	if fsID <= 0 {
		return fmt.Errorf("fs_id must be positive")
	}
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM tenant_placements WHERE fs_id = ?`, fsID); err != nil {
		return fmt.Errorf("delete tenant placement for fs_id %d: %w", fsID, err)
	}
	return nil
}

// scanSharedDBRow scans one db_pool row selected with sharedDBSelectColumns.
func scanSharedDBRow(row *sql.Row) (*SharedDB, error) {
	var rec SharedDB
	if err := row.Scan(&rec.DbID, &rec.OrgID, &rec.Role, &rec.Host, &rec.Port, &rec.User,
		&rec.PasswordCipher, &rec.Name, &rec.TLSMode, &rec.MaxTenants, &rec.TenantCount, &rec.Status); err != nil {
		return nil, err
	}
	return &rec, nil
}
