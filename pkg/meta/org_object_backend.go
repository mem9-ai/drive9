package meta

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
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
	Name             string
	Scheme           string
	Endpoint         string
	STSEndpoint      string
	Region           string
	AccountID        string
	ForcePathStyle   bool
	Bucket           string
	Prefix           string
	CredentialKind   string
	RoleARN          string
	AccessKeyID      string
	SecretCipher     []byte
	ExternalIDCipher []byte
	MaxSessionTTLSec int
	IdentityHash     string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

func (s *Store) InsertOrgObjectBackend(ctx context.Context, b *OrgObjectBackend) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "insert_org_object_backend", start, &err)
	if err := prepareOrgObjectBackend(b); err != nil {
		return err
	}
	now := time.Now().UTC()
	if b.CreatedAt.IsZero() {
		b.CreatedAt = now
	}
	if b.UpdatedAt.IsZero() {
		b.UpdatedAt = now
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO org_object_backends
		(id, organization_id, name, scheme, endpoint, sts_endpoint, region, account_id, force_path_style, bucket, prefix, credential_kind, role_arn, access_key_id, secret_cipher, external_id_cipher, max_session_ttl_sec, identity_hash, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		b.ID, b.OrganizationID, b.Name, b.Scheme, b.Endpoint, b.STSEndpoint, b.Region, b.AccountID, boolToTiny(b.ForcePathStyle), b.Bucket, b.Prefix, b.CredentialKind, b.RoleARN, b.AccessKeyID, nullableBytes(b.SecretCipher), nullableBytes(b.ExternalIDCipher), b.MaxSessionTTLSec, b.IdentityHash, b.CreatedAt, b.UpdatedAt)
	if isDuplicateEntry(err) {
		return ErrDuplicate
	}
	return err
}

func (s *Store) UpdateOrgObjectBackend(ctx context.Context, b *OrgObjectBackend) (err error) {
	start := time.Now()
	defer observeMeta(ctx, "update_org_object_backend", start, &err)
	if err := prepareOrgObjectBackend(b); err != nil {
		return err
	}
	b.UpdatedAt = time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `UPDATE org_object_backends SET
		name = ?, scheme = ?, endpoint = ?, sts_endpoint = ?, region = ?, account_id = ?, force_path_style = ?, bucket = ?, prefix = ?, credential_kind = ?, role_arn = ?, access_key_id = ?, secret_cipher = ?, external_id_cipher = ?, max_session_ttl_sec = ?, identity_hash = ?, updated_at = ?
		WHERE id = ?`,
		b.Name, b.Scheme, b.Endpoint, b.STSEndpoint, b.Region, b.AccountID, boolToTiny(b.ForcePathStyle), b.Bucket, b.Prefix, b.CredentialKind, b.RoleARN, b.AccessKeyID, nullableBytes(b.SecretCipher), nullableBytes(b.ExternalIDCipher), b.MaxSessionTTLSec, b.IdentityHash, b.UpdatedAt, b.ID)
	if err != nil {
		if isDuplicateEntry(err) {
			return ErrDuplicate
		}
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) GetOrgObjectBackend(ctx context.Context, id string) (*OrgObjectBackend, error) {
	return scanOrgObjectBackend(s.db.QueryRowContext(ctx, orgObjectBackendSelect+` WHERE id = ?`, strings.TrimSpace(id)))
}

func (s *Store) GetOrgObjectBackendByBucket(ctx context.Context, organizationID, scheme, bucket string) (*OrgObjectBackend, error) {
	rows, err := s.ListOrgObjectBackendsByBucket(ctx, organizationID, scheme, bucket)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, ErrNotFound
	}
	return &rows[0], nil
}

func (s *Store) ListOrgObjectBackendsByBucket(ctx context.Context, organizationID, scheme, bucket string) ([]OrgObjectBackend, error) {
	rows, err := s.db.QueryContext(ctx, orgObjectBackendSelect+` WHERE organization_id = ? AND scheme = ? AND bucket = ? ORDER BY CHAR_LENGTH(prefix) DESC, prefix, endpoint`,
		strings.TrimSpace(organizationID), strings.ToLower(strings.TrimSpace(scheme)), strings.TrimSpace(bucket))
	if err != nil {
		return nil, err
	}
	return scanOrgObjectBackendRows(rows)
}

func (s *Store) ListOrgObjectBackends(ctx context.Context, organizationID string) ([]OrgObjectBackend, error) {
	rows, err := s.db.QueryContext(ctx, orgObjectBackendSelect+` WHERE organization_id = ? ORDER BY scheme, bucket, prefix, name`, strings.TrimSpace(organizationID))
	if err != nil {
		return nil, err
	}
	return scanOrgObjectBackendRows(rows)
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

const orgObjectBackendSelect = `SELECT id, organization_id, name, scheme, endpoint, sts_endpoint, region, account_id, force_path_style, bucket, prefix, credential_kind, role_arn, access_key_id, secret_cipher, external_id_cipher, max_session_ttl_sec, identity_hash, created_at, updated_at FROM org_object_backends`

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

func scanOrgObjectBackendRows(rows *sql.Rows) ([]OrgObjectBackend, error) {
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

func scanOrgObjectBackendRow(row orgObjectScanner) (OrgObjectBackend, error) {
	var rec OrgObjectBackend
	var force int
	var secret, ext []byte
	err := row.Scan(&rec.ID, &rec.OrganizationID, &rec.Name, &rec.Scheme, &rec.Endpoint, &rec.STSEndpoint, &rec.Region, &rec.AccountID, &force, &rec.Bucket, &rec.Prefix, &rec.CredentialKind, &rec.RoleARN, &rec.AccessKeyID, &secret, &ext, &rec.MaxSessionTTLSec, &rec.IdentityHash, &rec.CreatedAt, &rec.UpdatedAt)
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

func prepareOrgObjectBackend(b *OrgObjectBackend) error {
	if b == nil {
		return fmt.Errorf("org object backend is required")
	}
	normalizeOrgObjectBackend(b)
	if b.MaxSessionTTLSec <= 0 {
		b.MaxSessionTTLSec = 3600
	}
	if b.MaxSessionTTLSec > 43200 {
		b.MaxSessionTTLSec = 43200
	}
	b.IdentityHash = orgObjectBackendIdentityHash(b)
	return validateOrgObjectBackend(b)
}

func normalizeOrgObjectBackend(b *OrgObjectBackend) {
	b.ID = strings.TrimSpace(b.ID)
	b.OrganizationID = strings.TrimSpace(b.OrganizationID)
	b.Name = strings.TrimSpace(b.Name)
	b.Scheme = strings.ToLower(strings.TrimSpace(b.Scheme))
	switch b.Scheme {
	case "gcs":
		b.Scheme = "gs"
	case "azure":
		b.Scheme = "az"
	}
	b.Endpoint = NormalizeObjectStoreEndpoint(b.Endpoint)
	b.STSEndpoint = strings.TrimSpace(b.STSEndpoint)
	b.Region = strings.TrimSpace(b.Region)
	b.AccountID = strings.TrimSpace(b.AccountID)
	b.Bucket = strings.TrimSpace(b.Bucket)
	b.Prefix = strings.Trim(strings.TrimSpace(b.Prefix), "/")
	b.CredentialKind = strings.TrimSpace(b.CredentialKind)
	b.RoleARN = strings.TrimSpace(b.RoleARN)
	b.AccessKeyID = strings.TrimSpace(b.AccessKeyID)
}

func orgObjectBackendIdentityHash(b *OrgObjectBackend) string {
	if b == nil {
		return ""
	}
	scheme := strings.ToLower(strings.TrimSpace(b.Scheme))
	switch scheme {
	case "gcs":
		scheme = "gs"
	case "azure":
		scheme = "az"
	}
	sum := sha256.Sum256([]byte(strings.Join([]string{
		strings.TrimSpace(b.OrganizationID),
		scheme,
		strings.TrimSpace(b.Bucket),
		strings.Trim(strings.TrimSpace(b.Prefix), "/"),
		NormalizeObjectStoreEndpoint(b.Endpoint),
	}, "\n")))
	return hex.EncodeToString(sum[:])
}

// NormalizeObjectStoreEndpoint canonicalizes a data-plane object endpoint for
// identity matching (scheme default https, lowercase host, no trailing slash).
func NormalizeObjectStoreEndpoint(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err == nil && u.Host != "" {
		host := strings.ToLower(u.Host)
		path := strings.TrimRight(u.Path, "/")
		scheme := strings.ToLower(u.Scheme)
		if scheme == "" {
			scheme = "https"
		}
		return strings.TrimRight(scheme+"://"+host+path, "/")
	}
	return strings.TrimRight(strings.ToLower(raw), "/")
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
		if b.Scheme != "gs" && strings.TrimSpace(b.AccessKeyID) == "" && strings.TrimSpace(b.AccountID) == "" {
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
