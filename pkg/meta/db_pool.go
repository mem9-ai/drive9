package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
)

// SharedDBOrgWildcard is the org_id sentinel matching any TiDB Cloud
// organization that has no exact db_pool row.
const SharedDBOrgWildcard = "*"

// MaxTiDBCloudSpendingLimit is the spendingLimit.monthly API maximum. In the
// TiDB Cloud unit, 1,000,000 represents $10,000.
const MaxTiDBCloudSpendingLimit = int64(1_000_000)

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

// PlacementStatusDeleting retains the physical resource identity while a
// durable tenant cleanup job is still running. Capacity has already been
// released, so allocators must not treat this row as active inventory.
const PlacementStatusDeleting = "deleting"

const (
	SharedDBStatusProvisioning = "provisioning"
	SharedDBStatusActive       = "active"
	SharedDBStatusFailed       = "failed"
	SharedDBStatusDraining     = "draining"
)

const sharedDBStatusActive = SharedDBStatusActive

// SharedDBCapacityMode selects the capacity bound used by a reservation.
type SharedDBCapacityMode string

const (
	SharedDBCapacityNormal    SharedDBCapacityMode = "normal"
	SharedDBCapacityEmergency SharedDBCapacityMode = "emergency"
)

// SharedDBHardCap computes runtime emergency capacity from a persisted soft
// capacity. The ratio is deliberately not stored in db_pool.
func SharedDBHardCap(softCap int, ratio float64) (int, error) {
	if softCap <= 0 {
		return 0, fmt.Errorf("soft capacity must be positive")
	}
	if math.IsNaN(ratio) || math.IsInf(ratio, 0) || ratio < 1 {
		return 0, fmt.Errorf("hard-cap ratio must be finite and at least 1")
	}
	value := float64(softCap) * ratio
	if value > float64(int(^uint(0)>>1)) {
		return 0, fmt.Errorf("hard capacity overflows int")
	}
	return int(math.Ceil(value)), nil
}

// SharedDBReopenThresholdForRatio computes the runtime hysteresis threshold.
// The ratio is intentionally not persisted in db_pool; it is deployment
// policy and must be in the open interval (0,1).
func SharedDBReopenThresholdForRatio(softCap int, ratio float64) (int, error) {
	if softCap <= 0 {
		return 0, nil
	}
	if math.IsNaN(ratio) || math.IsInf(ratio, 0) || ratio <= 0 || ratio >= 1 {
		return 0, fmt.Errorf("reopen ratio must be finite and in (0,1)")
	}
	return int(math.Floor(float64(softCap) * ratio)), nil
}

func sharedDBUUID(raw string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return uuid.NewString(), nil
	}
	parsed, err := uuid.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("invalid shared db pool uuid %q: %w", raw, err)
	}
	return parsed.String(), nil
}

// SharedDB is one physical database registered in db_pool. PasswordCipher
// holds the same encrypted envelope as tenants.db_password; the plaintext
// never crosses this layer. TLSMode is the go-sql-driver tls DSN parameter
// verbatim ("true", "skip-verify", a custom registered config name, or ""
// for plaintext) so the runtime handle reopens the DB with exactly the TLS
// mode it was registered with. MaxTenants of 0 means unlimited.
type SharedDB struct {
	ID                      int64
	UUID                    string
	TiDBCloudOrganizationID string
	ClusterID               string
	ProvisioningKey         []byte
	CloudProvider           string
	Region                  string
	Role                    string
	Host                    string
	Port                    int
	User                    string
	PasswordCipher          []byte
	Name                    string
	TLSMode                 string
	MaxTenants              int
	TenantCount             int
	SoftCapReached          bool
	SpendingLimit           *int64
	SchemaVersion           int
	Status                  string
	CreatedAt               time.Time
	UpdatedAt               time.Time
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

// SharedDBPoolMetricSnapshot is the central-meta view used by the existing
// tenant metrics pass. TenantCount is the capacity counter maintained with
// placement writes; TenantStates is recomputed from placements so counter
// drift remains visible in exported metrics.
type SharedDBPoolMetricSnapshot struct {
	ID                      int64
	UUID                    string
	TiDBCloudOrganizationID string
	Status                  string
	MaxTenants              int
	TenantCount             int
	SoftCapReached          bool
	SpendingLimit           *int64
	TenantStates            []SharedDBPoolTenantStateCount
}

// SharedDBPoolTenantStateCount is one persisted tenant-status count within a
// physical shared DB pool.
type SharedDBPoolTenantStateCount struct {
	State TenantStatus
	Count int64
}

// sharedDBSelectColumns lists the db_pool columns read into SharedDB. The
// `role` column is backtick-quoted because ROLE is reserved in MySQL 8.0.
const sharedDBSelectColumns = "db_id, uuid, org_id, cluster_id, provisioning_key, cloud_provider, region, " +
	"`role`, db_host, db_port, db_user, db_password, db_name, db_tls, max_tenants, tenant_count, " +
	"soft_cap_reached, spending_limit, schema_version, status, created_at, updated_at"

// CreateManagedSharedDBPool commits the durable identity and fixed business
// policy for one Drive9-managed physical pool before any TiDB Cloud API call.
// Connection and cluster fields intentionally remain NULL while provisioning.
func (s *Store) CreateManagedSharedDBPool(ctx context.Context, in *SharedDB) (id int64, err error) {
	start := time.Now()
	defer observeMeta(ctx, "create_managed_shared_db_pool", start, &err)
	if in == nil {
		return 0, fmt.Errorf("shared db pool is required")
	}
	poolUUID, err := sharedDBUUID(in.UUID)
	if err != nil {
		return 0, err
	}
	if len(in.ProvisioningKey) != 32 {
		return 0, fmt.Errorf("provisioning key must be a 32-byte SHA-256 fingerprint")
	}
	if in.CloudProvider == "" {
		return 0, fmt.Errorf("cloud provider is required")
	}
	if in.Region == "" {
		return 0, fmt.Errorf("region is required")
	}
	if in.MaxTenants <= 0 {
		return 0, fmt.Errorf("managed max tenants must be positive")
	}
	if in.SpendingLimit == nil || *in.SpendingLimit != MaxTiDBCloudSpendingLimit {
		return 0, fmt.Errorf("managed spending limit must equal TiDB Cloud maximum %d", MaxTiDBCloudSpendingLimit)
	}
	var organizationID any
	if in.TiDBCloudOrganizationID != "" {
		organizationID = in.TiDBCloudOrganizationID
	}
	var passwordCipher any
	if len(in.PasswordCipher) != 0 {
		passwordCipher = in.PasswordCipher
	}
	var databaseName any
	if in.Name != "" {
		databaseName = in.Name
	}
	res, err := s.db.ExecContext(ctx, `INSERT INTO db_pool
		(uuid, org_id, provisioning_key, cloud_provider, region, `+"`role`"+`, db_tls,
		 db_password, db_name, max_tenants, tenant_count, soft_cap_reached, spending_limit, schema_version, status)
		VALUES (?, ?, ?, ?, ?, ?, '', ?, ?, ?, 0, 0, ?, 0, ?)`, poolUUID, organizationID, in.ProvisioningKey,
		in.CloudProvider, in.Region, SharedDBRoleShared, passwordCipher, databaseName,
		in.MaxTenants, *in.SpendingLimit,
		SharedDBStatusProvisioning)
	if err != nil {
		return 0, fmt.Errorf("insert managed db_pool row: %w", err)
	}
	id, err = res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("resolve managed db_pool id: %w", err)
	}
	return id, nil
}

// UpdateManagedSharedDBPoolCloudResult persists the cloud identity and any
// connection metadata currently available for a provisional managed pool. It
// clears provisioning_key only after the organization identity is durable.
// The pool remains provisioning until its shared schema is ready.
func (s *Store) UpdateManagedSharedDBPoolCloudResult(ctx context.Context, in *SharedDB) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "update_managed_shared_db_pool_cloud_result", start, &err)
	if in == nil || in.ID <= 0 {
		return fmt.Errorf("managed shared db pool id must be positive")
	}
	if in.TiDBCloudOrganizationID == "" {
		return fmt.Errorf("tidbcloud organization id is required")
	}
	if in.ClusterID == "" {
		return fmt.Errorf("cluster id is required")
	}
	if in.Port < 0 {
		return fmt.Errorf("db port must not be negative")
	}
	if len(in.TLSMode) > 32 {
		return fmt.Errorf("tls mode %q is too long", in.TLSMode)
	}
	nullString := func(value string) any {
		if value == "" {
			return nil
		}
		return value
	}
	nullInt := func(value int) any {
		if value == 0 {
			return nil
		}
		return value
	}
	nullBytes := func(value []byte) any {
		if len(value) == 0 {
			return nil
		}
		return value
	}
	res, err := s.db.ExecContext(ctx, `UPDATE db_pool
		SET org_id = ?, cluster_id = ?, provisioning_key = NULL,
			db_host = COALESCE(?, db_host), db_port = COALESCE(?, db_port),
			db_user = COALESCE(?, db_user), db_password = COALESCE(?, db_password),
			db_name = COALESCE(?, db_name), db_tls = ?
		WHERE db_id = ? AND status = ?`,
		in.TiDBCloudOrganizationID, in.ClusterID, nullString(in.Host), nullInt(in.Port),
		nullString(in.User), nullBytes(in.PasswordCipher), nullString(in.Name), in.TLSMode,
		in.ID, SharedDBStatusProvisioning)
	if err != nil {
		return fmt.Errorf("persist cloud result for db pool %d: %w", in.ID, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("cloud result rows affected for db pool %d: %w", in.ID, err)
	}
	if affected == 0 {
		var organizationID, clusterID sql.NullString
		checkErr := s.db.QueryRowContext(ctx, `SELECT org_id, cluster_id FROM db_pool
			WHERE db_id = ? AND status = ?`, in.ID, SharedDBStatusProvisioning).
			Scan(&organizationID, &clusterID)
		if errors.Is(checkErr, sql.ErrNoRows) {
			return ErrNotFound
		}
		if checkErr != nil {
			return fmt.Errorf("confirm cloud result for db pool %d: %w", in.ID, checkErr)
		}
		if organizationID.String != in.TiDBCloudOrganizationID || clusterID.String != in.ClusterID {
			return fmt.Errorf("db pool %d cloud identity is %q/%q, not %q/%q", in.ID,
				organizationID.String, clusterID.String, in.TiDBCloudOrganizationID, in.ClusterID)
		}
	}
	return nil
}

// PrepareManagedSharedDBPoolRoot durably stores the root credential and
// database name before the first Cloud create call. It is idempotent for a
// provisioning row that has not yet acquired a cluster identity.
func (s *Store) PrepareManagedSharedDBPoolRoot(ctx context.Context, dbID int64, passwordCipher []byte, databaseName string) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "prepare_managed_shared_db_pool_root", start, &err)
	if dbID <= 0 {
		return fmt.Errorf("db pool id must be positive")
	}
	if len(passwordCipher) == 0 {
		return fmt.Errorf("managed root password cipher is required")
	}
	if databaseName == "" {
		return fmt.Errorf("managed database name is required")
	}
	res, err := s.db.ExecContext(ctx, `UPDATE db_pool
		SET db_password = COALESCE(db_password, ?), db_name = COALESCE(db_name, ?)
		WHERE db_id = ? AND status = ? AND cluster_id IS NULL`,
		passwordCipher, databaseName, dbID, SharedDBStatusProvisioning)
	if err != nil {
		return fmt.Errorf("prepare managed db pool %d root credential: %w", dbID, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("prepare managed db pool %d rows affected: %w", dbID, err)
	}
	if affected > 0 {
		return nil
	}
	var exists int
	if err := s.db.QueryRowContext(ctx, `SELECT 1 FROM db_pool WHERE db_id = ? AND status = ? AND cluster_id IS NULL
		AND db_password IS NOT NULL AND db_name IS NOT NULL`, dbID, SharedDBStatusProvisioning).Scan(&exists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	return nil
}

// UpdateSharedDBSchemaVersion records the checked-in shared schema version
// after the idempotent ensure succeeds.
func (s *Store) UpdateSharedDBSchemaVersion(ctx context.Context, dbID int64, version int) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "update_shared_db_schema_version", start, &err)
	if dbID <= 0 {
		return fmt.Errorf("db_id must be positive")
	}
	if version <= 0 {
		return fmt.Errorf("schema version must be positive")
	}
	res, err := s.db.ExecContext(ctx, `UPDATE db_pool SET schema_version = ? WHERE db_id = ?`, version, dbID)
	if err != nil {
		return fmt.Errorf("update schema version for db pool %d: %w", dbID, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("schema version rows affected for db pool %d: %w", dbID, err)
	}
	if affected == 0 {
		var exists int
		if scanErr := s.db.QueryRowContext(ctx, `SELECT 1 FROM db_pool WHERE db_id = ?`, dbID).Scan(&exists); scanErr != nil {
			if errors.Is(scanErr, sql.ErrNoRows) {
				return ErrNotFound
			}
			return scanErr
		}
	}
	return nil
}

// ActivateSharedDBPool flips a managed pool to active only after the cloud
// identity, connection fields, and shared schema version are complete.
func (s *Store) ActivateSharedDBPool(ctx context.Context, dbID int64) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "activate_shared_db_pool", start, &err)
	if dbID <= 0 {
		return fmt.Errorf("db_id must be positive")
	}
	res, err := s.db.ExecContext(ctx, `UPDATE db_pool SET status = ?
		WHERE db_id = ? AND status = ? AND org_id IS NOT NULL AND cluster_id IS NOT NULL
			AND db_host IS NOT NULL AND db_port IS NOT NULL AND db_user IS NOT NULL
			AND db_password IS NOT NULL AND db_name IS NOT NULL AND schema_version > 0`,
		SharedDBStatusActive, dbID, SharedDBStatusProvisioning)
	if err != nil {
		return fmt.Errorf("activate db pool %d: %w", dbID, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("activate db pool rows affected %d: %w", dbID, err)
	}
	if affected > 0 {
		return nil
	}
	var status string
	if scanErr := s.db.QueryRowContext(ctx, `SELECT status FROM db_pool WHERE db_id = ?`, dbID).Scan(&status); scanErr != nil {
		if errors.Is(scanErr, sql.ErrNoRows) {
			return ErrNotFound
		}
		return scanErr
	}
	if status == SharedDBStatusActive {
		return nil
	}
	return fmt.Errorf("db pool %d is not ready for activation", dbID)
}

func (s *Store) MarkSharedDBPoolFailed(ctx context.Context, dbID int64) error {
	res, err := s.db.ExecContext(ctx, `UPDATE db_pool SET status = ?
		WHERE db_id = ? AND status = ? AND tenant_count = 0`,
		SharedDBStatusFailed, dbID, SharedDBStatusProvisioning)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		var status string
		var tenantCount int
		if err := s.db.QueryRowContext(ctx, `SELECT status, tenant_count FROM db_pool WHERE db_id = ?`, dbID).
			Scan(&status, &tenantCount); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if status == SharedDBStatusProvisioning && tenantCount > 0 {
			return fmt.Errorf("db pool %d has %d placed tenants and cannot be marked failed", dbID, tenantCount)
		}
		return ErrNotFound
	}
	return nil
}

// ListSharedDBsByStatus returns shared DB-pool rows in stable ID order.
func (s *Store) ListSharedDBsByStatus(ctx context.Context, status string, limit int) (out []*SharedDB, err error) {
	start := time.Now()
	defer observeMeta(ctx, "list_shared_dbs_by_status", start, &err)
	switch status {
	case SharedDBStatusProvisioning, SharedDBStatusActive, SharedDBStatusFailed, SharedDBStatusDraining:
	default:
		return nil, fmt.Errorf("unsupported shared db status %q", status)
	}
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx, "SELECT "+sharedDBSelectColumns+" FROM db_pool "+
		"WHERE `role` = ? AND status = ? ORDER BY db_id LIMIT ?", SharedDBRoleShared, status, limit)
	if err != nil {
		return nil, fmt.Errorf("list shared dbs in status %q: %w", status, err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		rec, scanErr := scanSharedDBScanner(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// FindSharedDBForAllocation selects the oldest eligible exact-organization
// pool, preferring active over provisioning. Before organization resolution,
// callers may instead supply the 32-byte provisioning fingerprint. Managed
// rows use one fixed physical spending limit, so tenant virtual values do not
// participate in allocation.
func (s *Store) FindSharedDBForAllocation(ctx context.Context, organizationID string, provisioningKey []byte) (db *SharedDB, err error) {
	start := time.Now()
	defer observeMeta(ctx, "find_shared_db_for_allocation", start, &err)
	identityColumn := "org_id"
	var identity any = organizationID
	if organizationID == "" {
		if len(provisioningKey) != 32 {
			return nil, fmt.Errorf("organization id or 32-byte provisioning key is required")
		}
		identityColumn = "provisioning_key"
		identity = provisioningKey
	}
	query := "SELECT " + sharedDBSelectColumns + " FROM db_pool d " +
		"WHERE d." + identityColumn + " = ? AND d.`role` = ? " +
		"AND d.status IN (?, ?) " +
		"AND (d.max_tenants = 0 OR (d.soft_cap_reached = 0 AND d.tenant_count < d.max_tenants)) " +
		"ORDER BY CASE d.status WHEN 'active' THEN 0 ELSE 1 END, d.db_id LIMIT 1"
	db, err = scanSharedDBRow(s.db.QueryRowContext(ctx, query, identity, SharedDBRoleShared,
		SharedDBStatusActive, SharedDBStatusProvisioning))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("find shared db for allocation: %w", err)
	}
	return db, nil
}

// FindSharedDBForEmergency selects the least-loaded active managed pool below
// its own runtime-derived hard cap. It is used only after a physical create
// failure on a direct request; refill and prewarm must never call it.
func (s *Store) FindSharedDBForEmergency(ctx context.Context, organizationID string, hardCapRatio float64) (*SharedDB, error) {
	if organizationID == "" {
		return nil, fmt.Errorf("organization id is required")
	}
	if hardCapRatio <= 1 {
		return nil, fmt.Errorf("invalid emergency capacity arguments")
	}
	query := "SELECT " + sharedDBSelectColumns + " FROM db_pool d " +
		"WHERE d.org_id = ? AND d.`role` = ? AND d.status = ? " +
		"AND d.spending_limit IS NOT NULL " +
		"AND d.max_tenants > 0 AND d.tenant_count < CEIL(d.max_tenants * ?) " +
		"ORDER BY d.tenant_count ASC, d.db_id ASC LIMIT 1"
	db, err := scanSharedDBRow(s.db.QueryRowContext(ctx, query, organizationID, SharedDBRoleShared,
		SharedDBStatusActive, hardCapRatio))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("find emergency shared db: %w", err)
	}
	return db, nil
}

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
	poolUUID, err := sharedDBUUID(in.UUID)
	if err != nil {
		return 0, err
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
		"INSERT INTO db_pool (uuid, org_id, `role`, db_host, db_port, db_user, db_password, db_name, "+
			"db_tls, max_tenants, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "+
			"ON DUPLICATE KEY UPDATE db_port = VALUES(db_port), db_user = VALUES(db_user), "+
			"db_password = VALUES(db_password), db_tls = VALUES(db_tls), "+
			"max_tenants = VALUES(max_tenants), status = VALUES(status)",
		poolUUID, in.TiDBCloudOrganizationID, role, in.Host, in.Port, in.User, in.PasswordCipher, in.Name,
		in.TLSMode, in.MaxTenants, status); err != nil {
		return 0, fmt.Errorf("upsert db_pool row: %w", err)
	}
	// ON DUPLICATE KEY UPDATE does not reliably report the existing
	// auto-increment id via LastInsertId, so re-fetch by endpoint.
	err = s.db.QueryRowContext(ctx,
		`SELECT db_id FROM db_pool WHERE org_id = ? AND db_host = ? AND db_name = ?`,
		in.TiDBCloudOrganizationID, in.Host, in.Name).Scan(&dbID)
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

// GetSharedDBForTenant resolves the physical shared resource identity used by
// credential authorization and metrics. tenants.cluster_id is intentionally
// not involved for shared-schema tenants.
func (s *Store) GetSharedDBForTenant(ctx context.Context, tenantID string) (*SharedDB, error) {
	db, err := scanSharedDBRow(s.db.QueryRowContext(ctx, `SELECT `+sharedDBSelectColumns+`
		FROM db_pool WHERE db_id = (SELECT p.db_id FROM tenant_placements p
		JOIN fs_registry f ON f.fs_id = p.fs_id WHERE f.tenant_id = ?)`, tenantID))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
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
	// Capacity is enforced at selection time: normal allocation only sees open
	// soft-cap pools (0 means unlimited); the transactional reserve remains the
	// final guard against races.
	db, err := scanSharedDBRow(s.db.QueryRowContext(ctx,
		"SELECT "+sharedDBSelectColumns+" FROM db_pool "+
			"WHERE org_id = ? AND `role` = ? AND status = ? "+
			"AND (max_tenants = 0 OR (soft_cap_reached = 0 AND tenant_count < max_tenants)) ORDER BY db_id LIMIT 1",
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
		rec, scanErr := scanSharedDBScanner(rows)
		if scanErr != nil {
			err = scanErr
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// ListSharedDBPoolMetricSnapshots returns one snapshot per physical shared
// database. It is intentionally a read-only aggregate used by the existing
// tenant metrics pass; it does not reconcile or mutate capacity counters.
func (s *Store) ListSharedDBPoolMetricSnapshots(ctx context.Context) (out []SharedDBPoolMetricSnapshot, err error) {
	start := time.Now()
	defer observeMeta(ctx, "list_shared_db_pool_metric_snapshots", start, &err)
	rows, err := s.db.QueryContext(ctx, `SELECT
			d.db_id, d.uuid, COALESCE(d.org_id, ''), d.status, d.max_tenants, d.tenant_count, d.soft_cap_reached,
			d.spending_limit,
			COALESCE(states.tenant_status, ''), COALESCE(states.tenant_count, 0)
		FROM db_pool d
		LEFT JOIN (
			SELECT p.db_id, t.status AS tenant_status, COUNT(*) AS tenant_count
			FROM tenant_placements p
			JOIN fs_registry f ON f.fs_id = p.fs_id
			JOIN tenants t ON t.id = f.tenant_id
			GROUP BY p.db_id, t.status
		) states ON states.db_id = d.db_id
		WHERE d.`+"`role`"+` = ?
		ORDER BY d.db_id, states.tenant_status`, SharedDBRoleShared)
	if err != nil {
		return nil, fmt.Errorf("list shared db pool metric snapshots: %w", err)
	}
	defer func() { _ = rows.Close() }()
	byID := make(map[int64]int)
	for rows.Next() {
		var id int64
		var dbPoolUUID, organizationID, status, tenantStatus string
		var maxTenants, tenantCount int
		var softCapReached bool
		var spendingLimit sql.NullInt64
		var stateCount int64
		if err := rows.Scan(&id, &dbPoolUUID, &organizationID, &status, &maxTenants, &tenantCount, &softCapReached,
			&spendingLimit, &tenantStatus, &stateCount); err != nil {
			return nil, fmt.Errorf("scan shared db pool metric snapshot: %w", err)
		}
		index, ok := byID[id]
		if !ok {
			snapshot := SharedDBPoolMetricSnapshot{
				ID: id, UUID: dbPoolUUID, TiDBCloudOrganizationID: organizationID, Status: status,
				MaxTenants: maxTenants, TenantCount: tenantCount, SoftCapReached: softCapReached,
			}
			if spendingLimit.Valid {
				value := spendingLimit.Int64
				snapshot.SpendingLimit = &value
			}
			out = append(out, snapshot)
			index = len(out) - 1
			byID[id] = index
		}
		if tenantStatus != "" && stateCount > 0 {
			out[index].TenantStates = append(out[index].TenantStates, SharedDBPoolTenantStateCount{
				State: TenantStatus(tenantStatus), Count: stateCount,
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list shared db pool metric snapshot rows: %w", err)
	}
	return out, nil
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

// ErrSharedDBCapacityExhausted is returned when a shared-pool reservation
// cannot be made because the target pool has no free capacity
// (max_tenants reached).
var ErrSharedDBCapacityExhausted = errors.New("shared db capacity exhausted")

// ErrSharedDBQuotaNotMaterialized is returned when a shared tenant quota
// mutation is attempted before the compatibility row has been persisted.
var ErrSharedDBQuotaNotMaterialized = errors.New("shared tenant quota is not materialized")

// CompleteSharedTenantProvision atomically performs every meta write that
// turns a pending tenant into a live shared-pool tenant, in one transaction:
//
//  1. the conditional capacity reservation (two concurrent provisions
//     cannot take the same last slot — the loser gets
//     ErrSharedDBCapacityExhausted),
//  2. the placement insert,
//  3. the provider re-label and readiness transition on the tenant row
//     (active only when the DB pool is active; otherwise provisioning), and
//  4. the owner API key insert (ErrDuplicate on key id collision).
//
// Either all of it commits or none does: a shared tenant can never become
// active without its placement row, its reserved capacity, and its owner
// key, and a failed attempt leaves no partial state behind.
func (s *Store) CompleteSharedTenantProvision(ctx context.Context, tenantID, provider string, p *TenantPlacement, k *APIKey) (err error) {
	return s.completeSharedTenantProvision(ctx, tenantID, provider, p, k, nil, SharedDBCapacityNormal, 0)
}

// CompleteSharedTenantProvisionEmergency uses a caller-computed runtime hard
// cap. It is reserved for direct-request fallback after physical DB creation
// fails; tenant-pool refill must use the normal wrapper.
func (s *Store) CompleteSharedTenantProvisionEmergency(ctx context.Context, tenantID, provider string, p *TenantPlacement, k *APIKey, hardCap int) error {
	if hardCap <= 0 {
		return fmt.Errorf("emergency hard cap must be positive")
	}
	return s.completeSharedTenantProvision(ctx, tenantID, provider, p, k, nil, SharedDBCapacityEmergency, hardCap)
}

// CompleteSharedTenantPoolMember reserves a shared placement and records a
// free logical-pool membership in the same transaction. Prewarmed members do
// not receive an owner key until claim.
func (s *Store) CompleteSharedTenantPoolMember(ctx context.Context, tenantID, provider string, p *TenantPlacement, membership *TenantPoolMembership) error {
	return s.completeSharedTenantProvision(ctx, tenantID, provider, p, nil, membership, SharedDBCapacityNormal, 0)
}

func (s *Store) completeSharedTenantProvision(ctx context.Context, tenantID, provider string, p *TenantPlacement, k *APIKey, membership *TenantPoolMembership, capacityMode SharedDBCapacityMode, hardCap int) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "complete_shared_tenant_provision", start, &err)
	if tenantID == "" {
		return fmt.Errorf("tenant id is required")
	}
	if provider != tidbCloudNativeSharedProvider {
		return fmt.Errorf("shared tenant provider %q is required", tidbCloudNativeSharedProvider)
	}
	if p == nil {
		return fmt.Errorf("tenant placement is required")
	}
	if p.FsID <= 0 {
		return fmt.Errorf("fs_id must be positive")
	}
	if p.DbID <= 0 {
		return fmt.Errorf("db_id must be positive")
	}
	if p.Placement != PlacementShared || p.SchemaShape != SchemaShapeShared {
		return fmt.Errorf("provision requires shared placement and schema shape")
	}
	if k == nil && membership == nil {
		return fmt.Errorf("api key or tenant pool membership is required")
	}
	if capacityMode != SharedDBCapacityNormal && capacityMode != SharedDBCapacityEmergency {
		return fmt.Errorf("unsupported shared db capacity mode %q", capacityMode)
	}
	var scopeKind APIKeyScopeKind
	if k != nil {
		scopeKind, err = apiKeyScopeKindForInsert(k)
		if err != nil {
			return err
		}
	}
	if membership != nil {
		if membership.TenantID != tenantID || strings.TrimSpace(membership.PoolID) == "" {
			return fmt.Errorf("valid tenant pool membership is required")
		}
	}
	status := p.Status
	if status == "" {
		status = sharedDBStatusActive
	}
	now := time.Now().UTC()
	return s.InTx(ctx, func(tx *sql.Tx) error {
		var dedicatedBindingCount int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM tenant_tidbcloud_org_bindings
			WHERE tenant_id = ?`, tenantID).Scan(&dedicatedBindingCount); err != nil {
			return err
		}
		if dedicatedBindingCount != 0 {
			return fmt.Errorf("tenant %q already has a dedicated tidbcloud org binding", tenantID)
		}
		var dbPoolStatus string
		var clusterID sql.NullString
		var maxTenants, tenantCount int
		var softCapReached bool
		if err := tx.QueryRowContext(ctx, `SELECT status, cluster_id, max_tenants, tenant_count, soft_cap_reached
			FROM db_pool WHERE db_id = ? FOR UPDATE`, p.DbID).
			Scan(&dbPoolStatus, &clusterID, &maxTenants, &tenantCount, &softCapReached); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("lock db pool %d: %w", p.DbID, err)
		}
		switch capacityMode {
		case SharedDBCapacityNormal:
			if dbPoolStatus != SharedDBStatusActive && (dbPoolStatus != SharedDBStatusProvisioning || (membership == nil && (!clusterID.Valid || clusterID.String == ""))) {
				return fmt.Errorf("db pool %d is not physically ready: status=%s cluster=%q membership=%t: %w", p.DbID, dbPoolStatus, clusterID.String, membership != nil, ErrSharedDBCapacityExhausted)
			}
			if maxTenants > 0 && (softCapReached || tenantCount >= maxTenants) {
				return fmt.Errorf("db pool %d soft capacity exhausted: count=%d max=%d latch=%t: %w", p.DbID, tenantCount, maxTenants, softCapReached, ErrSharedDBCapacityExhausted)
			}
		case SharedDBCapacityEmergency:
			if dbPoolStatus != SharedDBStatusActive || maxTenants <= 0 || hardCap < maxTenants || tenantCount >= hardCap {
				return ErrSharedDBCapacityExhausted
			}
		}
		var res sql.Result
		if capacityMode == SharedDBCapacityEmergency {
			res, err = tx.ExecContext(ctx, `UPDATE db_pool
				SET tenant_count = tenant_count + 1, soft_cap_reached = 1
				WHERE db_id = ? AND status = ?
					AND max_tenants > 0 AND tenant_count + 1 <= ?`,
				p.DbID, SharedDBStatusActive, hardCap)
		} else {
			normalQuery := `UPDATE db_pool
				SET soft_cap_reached = CASE
					WHEN max_tenants > 0 AND tenant_count + 1 >= max_tenants THEN 1
					ELSE soft_cap_reached END,
					tenant_count = tenant_count + 1
				WHERE db_id = ? AND (%s)
					AND (max_tenants = 0 OR (soft_cap_reached = 0 AND tenant_count + 1 <= max_tenants))`
			if membership != nil {
				normalQuery = fmt.Sprintf(normalQuery, "status IN (?, ?)")
				res, err = tx.ExecContext(ctx, normalQuery, p.DbID, SharedDBStatusActive, SharedDBStatusProvisioning)
			} else {
				normalQuery = fmt.Sprintf(normalQuery, "status = ? OR (status = ? AND cluster_id IS NOT NULL)")
				res, err = tx.ExecContext(ctx, normalQuery, p.DbID, SharedDBStatusActive, SharedDBStatusProvisioning)
			}
		}
		if err != nil {
			return fmt.Errorf("reserve capacity on db %d: %w", p.DbID, err)
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("reserve capacity rows affected for db %d: %w", p.DbID, err)
		}
		if affected == 0 {
			return ErrSharedDBCapacityExhausted
		}
		var tenantStatus TenantStatus
		switch dbPoolStatus {
		case SharedDBStatusActive:
			tenantStatus = TenantActive
		case SharedDBStatusProvisioning:
			tenantStatus = TenantProvisioning
		default:
			return fmt.Errorf("db pool %d is not allocatable in status %q", p.DbID, dbPoolStatus)
		}
		var target any
		if p.TargetDbID != nil {
			target = *p.TargetDbID
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO tenant_placements (fs_id, db_id, placement, schema_shape, status, target_db_id)
				VALUES (?, ?, ?, ?, ?, ?)`,
			p.FsID, p.DbID, p.Placement, p.SchemaShape, status, target); err != nil {
			return fmt.Errorf("insert placement for fs_id %d: %w", p.FsID, err)
		}
		res, err = tx.ExecContext(ctx,
			`UPDATE tenants SET provider = ?, status = ?, updated_at = ? WHERE id = ?`,
			provider, tenantStatus, now, tenantID)
		if err != nil {
			return fmt.Errorf("activate tenant %s: %w", tenantID, err)
		}
		affected, err = res.RowsAffected()
		if err != nil {
			return fmt.Errorf("activate tenant rows affected for %s: %w", tenantID, err)
		}
		if affected == 0 {
			return ErrNotFound
		}
		if k != nil {
			if _, err := tx.ExecContext(ctx, `INSERT INTO tenant_api_keys
			(id, tenant_id, key_name, jwt_ciphertext, jwt_hash, token_version, status, scope_kind,
			 issued_by_provider, issued_by_subject_key, issued_by_metadata_json,
			 issued_at, revoked_at, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				k.ID, k.TenantID, k.KeyName, k.JWTCiphertext, k.JWTHash, k.TokenVersion, k.Status, scopeKind,
				k.IssuedByProvider, k.IssuedBySubjectKey, nullableBytes(k.IssuedByMetadataJSON),
				k.IssuedAt.UTC(), k.RevokedAt, k.CreatedAt.UTC(), k.UpdatedAt.UTC()); err != nil {
				if isDuplicateEntry(err) {
					return ErrDuplicate
				}
				return fmt.Errorf("insert owner api key: %w", err)
			}
		}
		if membership != nil {
			createdAt := membership.CreatedAt
			if createdAt.IsZero() {
				createdAt = now
			}
			var organizationID any
			if strings.TrimSpace(membership.TiDBCloudOrganizationID) != "" {
				organizationID = strings.TrimSpace(membership.TiDBCloudOrganizationID)
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO tenant_pool_memberships
				(tenant_id, tidbcloud_organization_id, pool_id, pool_status, created_at, updated_at)
				VALUES (?, ?, ?, ?, ?, ?)`, tenantID, organizationID, membership.PoolID,
				TenantPoolBindingFree, createdAt, now); err != nil {
				if isDuplicateEntry(err) {
					return ErrDuplicate
				}
				return fmt.Errorf("insert tenant pool membership: %w", err)
			}
		}
		return nil
	})
}

// ActivateSharedTenantsBatch activates ready shared tenants after their
// physical DB pool becomes active. Direct-create tenants are eligible through
// an owner key; prewarmed tenants are eligible through a free pool membership.
func (s *Store) ActivateSharedTenantsBatch(ctx context.Context, dbID int64, limit int) (activated int, err error) {
	start := time.Now()
	defer observeMeta(ctx, "activate_shared_tenants_batch", start, &err)
	if dbID <= 0 {
		return 0, fmt.Errorf("db_id must be positive")
	}
	if limit <= 0 {
		return 0, fmt.Errorf("activation limit must be positive")
	}
	err = s.InTx(ctx, func(tx *sql.Tx) error {
		var poolStatus string
		if scanErr := tx.QueryRowContext(ctx, `SELECT status FROM db_pool WHERE db_id = ? FOR UPDATE`, dbID).Scan(&poolStatus); scanErr != nil {
			if errors.Is(scanErr, sql.ErrNoRows) {
				return ErrNotFound
			}
			return scanErr
		}
		if poolStatus != SharedDBStatusActive {
			return fmt.Errorf("db pool %d is not active", dbID)
		}
		rows, queryErr := tx.QueryContext(ctx, `SELECT t.id
			FROM tenants t
			JOIN fs_registry f ON f.tenant_id = t.id
			JOIN tenant_placements p ON p.fs_id = f.fs_id
			WHERE p.db_id = ? AND p.status = ? AND t.provider = ? AND t.status = ?
				AND (EXISTS (SELECT 1 FROM tenant_api_keys k
					WHERE k.tenant_id = t.id AND k.scope_kind = ? AND k.status = ?)
					OR EXISTS (SELECT 1 FROM tenant_pool_memberships m
						WHERE m.tenant_id = t.id AND m.pool_status = ?))
			ORDER BY t.created_at, t.id LIMIT ? FOR UPDATE`, dbID, SharedDBStatusActive,
			tidbCloudNativeSharedProvider, TenantProvisioning, APIKeyScopeKindOwner, APIKeyActive,
			TenantPoolBindingFree, limit)
		if queryErr != nil {
			return queryErr
		}
		var tenantIDs []string
		for rows.Next() {
			var tenantID string
			if scanErr := rows.Scan(&tenantID); scanErr != nil {
				_ = rows.Close()
				return scanErr
			}
			tenantIDs = append(tenantIDs, tenantID)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			_ = rows.Close()
			return rowsErr
		}
		if closeErr := rows.Close(); closeErr != nil {
			return closeErr
		}
		now := time.Now().UTC()
		for _, tenantID := range tenantIDs {
			res, updateErr := tx.ExecContext(ctx, `UPDATE tenants SET status = ?, updated_at = ?
				WHERE id = ? AND status = ? AND provider = ?`, TenantActive, now, tenantID,
				TenantProvisioning, tidbCloudNativeSharedProvider)
			if updateErr != nil {
				return updateErr
			}
			n, rowsErr := res.RowsAffected()
			if rowsErr != nil {
				return rowsErr
			}
			activated += int(n)
		}
		return nil
	})
	return activated, err
}

// DeleteTenantPlacementAndDecrCount atomically removes a tenant's placement
// and releases its capacity slot in a single transaction. Making the two
// writes atomic keeps the shared delete path retry-safe: a failure rolls both
// back, so a retried delete re-enters with the placement row still present
// and cannot orphan the row or double-decrement the counter. The reopen ratio
// is runtime policy supplied by the server and is never persisted. Returns
// ErrNotFound when the placement row does not exist.
func (s *Store) DeleteTenantPlacementAndDecrCount(ctx context.Context, fsID, dbID int64, reopenRatio float64) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "delete_tenant_placement_and_decr_count", start, &err)
	if fsID <= 0 {
		return fmt.Errorf("fs_id must be positive")
	}
	if dbID <= 0 {
		return fmt.Errorf("db_id must be positive")
	}
	if _, err := SharedDBReopenThresholdForRatio(1, reopenRatio); err != nil {
		return err
	}
	return s.InTx(ctx, func(tx *sql.Tx) error {
		return releaseTenantPlacementAndDecrCountTx(ctx, tx, fsID, dbID, reopenRatio, true)
	})
}

// FinalizeSharedTenantDeleteMetadata atomically releases shared DB capacity
// and logical-pool membership. Tenants requiring external cleanup retain a
// deleting placement as their authorization anchor until job finalization;
// tenants without external cleanup remove placement and become deleted in the
// same transaction. A failed transition always retains the active placement.
func (s *Store) FinalizeSharedTenantDeleteMetadata(ctx context.Context, tenantID string, fsID, dbID int64, reopenRatio float64, markDeleted bool) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "finalize_shared_tenant_delete_metadata", start, &err)
	if strings.TrimSpace(tenantID) == "" {
		return fmt.Errorf("tenant id is required")
	}
	if fsID <= 0 {
		return fmt.Errorf("fs_id must be positive")
	}
	if dbID <= 0 {
		return fmt.Errorf("db_id must be positive")
	}
	if _, err := SharedDBReopenThresholdForRatio(1, reopenRatio); err != nil {
		return err
	}
	return s.InTx(ctx, func(tx *sql.Tx) error {
		if err := releaseTenantPlacementAndDecrCountTx(ctx, tx, fsID, dbID, reopenRatio, markDeleted); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM tenant_pool_memberships WHERE tenant_id = ?`, tenantID); err != nil {
			return fmt.Errorf("delete tenant pool membership for %s: %w", tenantID, err)
		}
		if !markDeleted {
			return nil
		}
		now := time.Now().UTC()
		res, err := tx.ExecContext(ctx, `UPDATE tenants SET status = ?, updated_at = ? WHERE id = ?`, TenantDeleted, now, tenantID)
		if err != nil {
			return err
		}
		if err := requireAffected(res); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM tenant_tidbcloud_org_bindings WHERE tenant_id = ?`, tenantID); err != nil {
			return err
		}
		return nil
	})
}

func releaseTenantPlacementAndDecrCountTx(ctx context.Context, tx *sql.Tx, fsID, dbID int64, reopenRatio float64, deletePlacement bool) error {
	var (
		res sql.Result
		err error
	)
	if deletePlacement {
		res, err = tx.ExecContext(ctx, `DELETE FROM tenant_placements WHERE fs_id = ? AND db_id = ?`, fsID, dbID)
	} else {
		res, err = tx.ExecContext(ctx, `UPDATE tenant_placements SET status = ?
			WHERE fs_id = ? AND db_id = ? AND status <> ?`, PlacementStatusDeleting, fsID, dbID, PlacementStatusDeleting)
	}
	if err != nil {
		return fmt.Errorf("release tenant placement for fs_id %d: %w", fsID, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete tenant placement rows affected for fs_id %d: %w", fsID, err)
	}
	if affected == 0 {
		return ErrNotFound
	}
	var maxTenants, tenantCount int
	var softCapReached bool
	if err := tx.QueryRowContext(ctx, `SELECT max_tenants, tenant_count, soft_cap_reached FROM db_pool WHERE db_id = ? FOR UPDATE`, dbID).
		Scan(&maxTenants, &tenantCount, &softCapReached); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("lock capacity for db %d: %w", dbID, err)
	}
	reopenThreshold, err := SharedDBReopenThresholdForRatio(maxTenants, reopenRatio)
	if err != nil {
		return err
	}
	newTenantCount := tenantCount - 1
	if newTenantCount < 0 {
		newTenantCount = 0
	}
	nextSoftCapReached := softCapReached
	if maxTenants > 0 && newTenantCount <= reopenThreshold {
		nextSoftCapReached = false
	}
	_, err = tx.ExecContext(ctx, `UPDATE db_pool
			SET soft_cap_reached = ?, tenant_count = ?
			WHERE db_id = ?`, nextSoftCapReached, newTenantCount, dbID)
	if err != nil {
		return fmt.Errorf("release capacity on db %d: %w", dbID, err)
	}
	// The SELECT ... FOR UPDATE above already established that the pool
	// exists. RowsAffected may be zero when the delete leaves both values
	// unchanged (for example an already-zero counter), which is still a
	// valid atomic placement release.
	return nil
}

// scanSharedDBRow scans one db_pool row selected with sharedDBSelectColumns.
func scanSharedDBRow(row *sql.Row) (*SharedDB, error) {
	return scanSharedDBScanner(row)
}

type sharedDBScanner interface {
	Scan(dest ...any) error
}

func scanSharedDBScanner(row sharedDBScanner) (*SharedDB, error) {
	var rec SharedDB
	var organizationID, clusterID, cloudProvider, region sql.NullString
	var host, user, name sql.NullString
	var port, spendingLimit sql.NullInt64
	if err := row.Scan(&rec.ID, &rec.UUID, &organizationID, &clusterID, &rec.ProvisioningKey, &cloudProvider, &region,
		&rec.Role, &host, &port, &user, &rec.PasswordCipher, &name, &rec.TLSMode,
		&rec.MaxTenants, &rec.TenantCount, &rec.SoftCapReached, &spendingLimit, &rec.SchemaVersion, &rec.Status,
		&rec.CreatedAt, &rec.UpdatedAt); err != nil {
		return nil, err
	}
	rec.TiDBCloudOrganizationID = organizationID.String
	rec.ClusterID = clusterID.String
	rec.CloudProvider = cloudProvider.String
	rec.Region = region.String
	rec.Host = host.String
	rec.Port = int(port.Int64)
	rec.User = user.String
	rec.Name = name.String
	if spendingLimit.Valid {
		value := spendingLimit.Int64
		rec.SpendingLimit = &value
	}
	return &rec, nil
}
