package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	ObjectCredentialStatic = "static"
	ObjectCredentialRole   = "role"
)

// OrgObjectBackend is an org-owned object-store identity used to mint STS.
type OrgObjectBackend struct {
	ID               string
	OrganizationID   string
	Scheme           string
	Endpoint         string
	Region           string
	ForcePathStyle   bool
	Bucket           string
	Prefix           string
	CredentialKind   string
	RoleARN          string
	AccessKeyID      string
	SecretCipher     []byte
	ExternalIDCipher []byte
	MaxSessionTTLSec int
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

func (s *Store) InsertOrgObjectBackend(ctx context.Context, b *OrgObjectBackend) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "insert_org_object_backend", start, &err)
	if err := validateOrgObjectBackend(b); err != nil {
		return err
	}
	now := time.Now().UTC()
	if b.CreatedAt.IsZero() {
		b.CreatedAt = now
	}
	if b.UpdatedAt.IsZero() {
		b.UpdatedAt = now
	}
	if b.MaxSessionTTLSec <= 0 {
		b.MaxSessionTTLSec = 3600
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO org_object_backends
		(id, organization_id, scheme, endpoint, region, force_path_style, bucket, prefix, credential_kind, role_arn, access_key_id, secret_cipher, external_id_cipher, max_session_ttl_sec, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		b.ID, b.OrganizationID, b.Scheme, b.Endpoint, b.Region, boolToTiny(b.ForcePathStyle), b.Bucket, b.Prefix, b.CredentialKind, b.RoleARN, b.AccessKeyID, nullableBytes(b.SecretCipher), nullableBytes(b.ExternalIDCipher), b.MaxSessionTTLSec, b.CreatedAt, b.UpdatedAt)
	if isDuplicateEntry(err) {
		return ErrDuplicate
	}
	return err
}

func (s *Store) GetOrgObjectBackend(ctx context.Context, id string) (*OrgObjectBackend, error) {
	return scanOrgObjectBackend(s.db.QueryRowContext(ctx, orgObjectBackendSelect+` WHERE id = ?`, strings.TrimSpace(id)))
}

func (s *Store) GetOrgObjectBackendByBucket(ctx context.Context, organizationID, scheme, bucket string) (*OrgObjectBackend, error) {
	return scanOrgObjectBackend(s.db.QueryRowContext(ctx, orgObjectBackendSelect+` WHERE organization_id = ? AND scheme = ? AND bucket = ?`,
		strings.TrimSpace(organizationID), strings.TrimSpace(scheme), strings.TrimSpace(bucket)))
}

func (s *Store) ListOrgObjectBackends(ctx context.Context, organizationID string) ([]OrgObjectBackend, error) {
	rows, err := s.db.QueryContext(ctx, orgObjectBackendSelect+` WHERE organization_id = ? ORDER BY scheme, bucket`, strings.TrimSpace(organizationID))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []OrgObjectBackend
	for rows.Next() {
		rec, err := scanOrgObjectBackendRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (s *Store) DeleteOrgObjectBackend(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM org_object_backends WHERE id = ?`, strings.TrimSpace(id))
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) SetTenantObjectNamespaceID(ctx context.Context, tenantID, namespaceID string) error {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return fmt.Errorf("tenant_id is required")
	}
	res, err := s.db.ExecContext(ctx, `UPDATE tenant_tidbcloud_org_bindings SET object_namespace_id = ?, updated_at = ? WHERE tenant_id = ?`,
		strings.TrimSpace(namespaceID), time.Now().UTC(), tenantID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

const orgObjectBackendSelect = `SELECT id, organization_id, scheme, endpoint, region, force_path_style, bucket, prefix, credential_kind, role_arn, access_key_id, secret_cipher, external_id_cipher, max_session_ttl_sec, created_at, updated_at FROM org_object_backends`

type orgObjectScanner interface {
	Scan(dest ...any) error
}

func scanOrgObjectBackend(row orgObjectScanner) (*OrgObjectBackend, error) {
	rec, err := scanOrgObjectBackendRow(row)
	if err != nil {
		if errorsIsNoRows(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &rec, nil
}

func scanOrgObjectBackendRow(row orgObjectScanner) (OrgObjectBackend, error) {
	var rec OrgObjectBackend
	var force int
	var secret, ext []byte
	err := row.Scan(&rec.ID, &rec.OrganizationID, &rec.Scheme, &rec.Endpoint, &rec.Region, &force, &rec.Bucket, &rec.Prefix, &rec.CredentialKind, &rec.RoleARN, &rec.AccessKeyID, &secret, &ext, &rec.MaxSessionTTLSec, &rec.CreatedAt, &rec.UpdatedAt)
	if err != nil {
		return OrgObjectBackend{}, err
	}
	rec.ForcePathStyle = force != 0
	rec.SecretCipher = secret
	rec.ExternalIDCipher = ext
	return rec, nil
}

func errorsIsNoRows(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

func boolToTiny(v bool) int {
	if v {
		return 1
	}
	return 0
}

func validateOrgObjectBackend(b *OrgObjectBackend) error {
	if b == nil {
		return fmt.Errorf("org object backend is required")
	}
	if strings.TrimSpace(b.ID) == "" {
		return fmt.Errorf("id is required")
	}
	if strings.TrimSpace(b.OrganizationID) == "" {
		return fmt.Errorf("organization_id is required")
	}
	if strings.TrimSpace(b.Scheme) == "" {
		return fmt.Errorf("scheme is required")
	}
	if strings.TrimSpace(b.Bucket) == "" {
		return fmt.Errorf("bucket is required")
	}
	switch b.CredentialKind {
	case ObjectCredentialStatic:
		if strings.TrimSpace(b.AccessKeyID) == "" {
			return fmt.Errorf("access_key_id is required for static credentials")
		}
	case ObjectCredentialRole:
		if strings.TrimSpace(b.RoleARN) == "" {
			return fmt.Errorf("role_arn is required for role credentials")
		}
	default:
		return fmt.Errorf("credential_kind must be static or role")
	}
	return nil
}
