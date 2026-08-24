package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/google/uuid"

	"github.com/mem9-ai/drive9/pkg/meta"
)

type adminObjectBackendRequest struct {
	PublicKey       string `json:"public_key"`
	PrivateKey      string `json:"private_key"`
	Scheme          string `json:"scheme"`
	Endpoint        string `json:"endpoint"`
	Region          string `json:"region"`
	ForcePathStyle  bool   `json:"force_path_style"`
	Bucket          string `json:"bucket"`
	Prefix          string `json:"prefix"`
	CredentialKind  string `json:"credential_kind"`
	RoleARN         string `json:"role_arn"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	ExternalID      string `json:"external_id"`
	MaxSessionTTL   int    `json:"max_session_ttl_sec"`
}

type adminObjectBackendView struct {
	ID             string `json:"id"`
	OrganizationID string `json:"organization_id"`
	Scheme         string `json:"scheme"`
	Endpoint       string `json:"endpoint"`
	Region         string `json:"region"`
	ForcePathStyle bool   `json:"force_path_style"`
	Bucket         string `json:"bucket"`
	Prefix         string `json:"prefix"`
	CredentialKind string `json:"credential_kind"`
	RoleARN        string `json:"role_arn"`
	AccessKeyID    string `json:"access_key_id,omitempty"`
	HasSecret      bool   `json:"has_secret"`
	HasExternalID  bool   `json:"has_external_id"`
	MaxSessionTTL  int    `json:"max_session_ttl_sec"`
}

type adminObjectNamespaceRequest struct {
	PublicKey   string `json:"public_key"`
	PrivateKey  string `json:"private_key"`
	NamespaceID string `json:"namespace_id"`
}

type objectCredentialsRequest struct {
	URI   string `json:"uri"`
	Write bool   `json:"write"`
}

type objectCredentialsResponse struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	SessionToken    string `json:"session_token,omitempty"`
	Expiration      string `json:"expiration,omitempty"`
	Endpoint        string `json:"endpoint,omitempty"`
	Region          string `json:"region,omitempty"`
	ForcePathStyle  bool   `json:"force_path_style"`
	Scheme          string `json:"scheme"`
	Bucket          string `json:"bucket"`
	Prefix          string `json:"prefix"`
}

func (s *Server) adminObjectBackendsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.adminTenantAPIEnabled() {
			errJSON(w, http.StatusNotFound, "admin tenant API not enabled")
			return
		}
		rest := strings.TrimPrefix(r.URL.Path, "/v1/admin/object-backends")
		rest = strings.Trim(rest, "/")
		if rest == "" {
			switch r.Method {
			case http.MethodGet:
				s.handleAdminObjectBackendList(w, r)
			case http.MethodPost:
				s.handleAdminObjectBackendCreate(w, r)
			default:
				errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
			}
			return
		}
		if strings.Contains(rest, "/") {
			errJSON(w, http.StatusNotFound, "not found")
			return
		}
		switch r.Method {
		case http.MethodDelete:
			s.handleAdminObjectBackendDelete(w, r, rest)
		default:
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func (s *Server) handleAdminObjectBackendList(w http.ResponseWriter, r *http.Request) {
	cred, err := adminCredentialsFromHeaders(r)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	access, err := s.authorizeTiDBCloudAdminAccess(r.Context(), cred, "admin_object_backend_list")
	if err != nil {
		writeAdminTiDBCloudError(w, r.Context(), err, "list object backends")
		return
	}
	rows, err := s.meta.ListOrgObjectBackends(r.Context(), access.OrganizationID)
	if err != nil {
		errJSON(w, backendErrorStatus(r.Context(), err), "list object backends failed")
		return
	}
	out := make([]adminObjectBackendView, 0, len(rows))
	for i := range rows {
		out = append(out, viewOrgObjectBackend(&rows[i]))
	}
	writeJSON(w, http.StatusOK, map[string]any{"backends": out})
}

func (s *Server) handleAdminObjectBackendCreate(w http.ResponseWriter, r *http.Request) {
	var req adminObjectBackendRequest
	if err := decodeJSONBody(w, r, &req, true); err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	cred, err := adminCredentials(req.PublicKey, req.PrivateKey, r)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	access, err := s.authorizeTiDBCloudAdminAccess(r.Context(), cred, "admin_object_backend_create")
	if err != nil {
		writeAdminTiDBCloudError(w, r.Context(), err, "create object backend")
		return
	}
	kind := strings.TrimSpace(req.CredentialKind)
	if kind == "" {
		if strings.TrimSpace(req.RoleARN) != "" {
			kind = meta.ObjectCredentialRole
		} else {
			kind = meta.ObjectCredentialStatic
		}
	}
	rec := &meta.OrgObjectBackend{
		ID:               "obb_" + strings.ReplaceAll(uuid.NewString(), "-", ""),
		OrganizationID:   access.OrganizationID,
		Scheme:           strings.ToLower(strings.TrimSpace(req.Scheme)),
		Endpoint:         strings.TrimSpace(req.Endpoint),
		Region:           strings.TrimSpace(req.Region),
		ForcePathStyle:   req.ForcePathStyle,
		Bucket:           strings.TrimSpace(req.Bucket),
		Prefix:           strings.Trim(strings.TrimSpace(req.Prefix), "/"),
		CredentialKind:   kind,
		RoleARN:          strings.TrimSpace(req.RoleARN),
		AccessKeyID:      strings.TrimSpace(req.AccessKeyID),
		MaxSessionTTLSec: req.MaxSessionTTL,
	}
	if rec.Scheme == "" || rec.Bucket == "" {
		errJSON(w, http.StatusBadRequest, "scheme and bucket are required")
		return
	}
	if !mintableObjectScheme(rec.Scheme) {
		errJSON(w, http.StatusBadRequest, "scheme must be s3, cos, tos, or oss")
		return
	}
	if strings.Contains(rec.Prefix, "..") {
		errJSON(w, http.StatusBadRequest, "prefix must not contain ..")
		return
	}
	if rec.MaxSessionTTLSec > 43200 {
		rec.MaxSessionTTLSec = 43200
	}
	secret := strings.TrimSpace(req.SecretAccessKey)
	switch kind {
	case meta.ObjectCredentialRole:
		if rec.RoleARN == "" {
			errJSON(w, http.StatusBadRequest, "role_arn is required for credential_kind=role")
			return
		}
	case meta.ObjectCredentialStatic:
		if rec.AccessKeyID == "" || secret == "" {
			errJSON(w, http.StatusBadRequest, "access_key_id and secret_access_key are required for credential_kind=static")
			return
		}
	}
	if secret != "" {
		cipher, encErr := s.pool.Encrypt(r.Context(), []byte(secret))
		if encErr != nil {
			errJSON(w, http.StatusInternalServerError, "encrypt secret failed")
			return
		}
		rec.SecretCipher = cipher
	}
	if ext := strings.TrimSpace(req.ExternalID); ext != "" {
		cipher, encErr := s.pool.Encrypt(r.Context(), []byte(ext))
		if encErr != nil {
			errJSON(w, http.StatusInternalServerError, "encrypt external id failed")
			return
		}
		rec.ExternalIDCipher = cipher
	}
	if err := s.meta.InsertOrgObjectBackend(r.Context(), rec); err != nil {
		if errors.Is(err, meta.ErrDuplicate) {
			errJSON(w, http.StatusConflict, "object backend already exists for this bucket")
			return
		}
		errJSON(w, backendErrorStatus(r.Context(), err), "create object backend failed")
		return
	}
	writeJSON(w, http.StatusCreated, viewOrgObjectBackend(rec))
}

func (s *Server) handleAdminObjectBackendDelete(w http.ResponseWriter, r *http.Request, id string) {
	cred, err := adminCredentialsFromHeaders(r)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	access, err := s.authorizeTiDBCloudAdminAccess(r.Context(), cred, "admin_object_backend_delete")
	if err != nil {
		writeAdminTiDBCloudError(w, r.Context(), err, "delete object backend")
		return
	}
	row, err := s.meta.GetOrgObjectBackend(r.Context(), id)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "object backend not found")
			return
		}
		errJSON(w, backendErrorStatus(r.Context(), err), "lookup object backend failed")
		return
	}
	if row.OrganizationID != access.OrganizationID {
		errJSON(w, http.StatusForbidden, "object backend does not belong to this organization")
		return
	}
	if err := s.meta.DeleteOrgObjectBackend(r.Context(), id); err != nil {
		errJSON(w, backendErrorStatus(r.Context(), err), "delete object backend failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"id": id, "status": "deleted"})
}

func (s *Server) handleAdminTenantObjectNamespace(w http.ResponseWriter, r *http.Request, tenantID string) {
	switch r.Method {
	case http.MethodGet:
		cred, err := adminCredentialsFromHeaders(r)
		if err != nil {
			errJSON(w, http.StatusBadRequest, err.Error())
			return
		}
		access, err := s.authorizeTiDBCloudAdminAccess(r.Context(), cred, "admin_object_namespace_get")
		if err != nil {
			writeAdminTiDBCloudError(w, r.Context(), err, "get object namespace")
			return
		}
		_, binding, ok := s.authorizedAdminTenant(w, r, tenantID, access.OrganizationID, false, false)
		if !ok {
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{
			"tenant_id":    tenantID,
			"namespace_id": binding.ObjectNamespaceID,
		})
	case http.MethodPut, http.MethodDelete:
		var req adminObjectNamespaceRequest
		if r.Method == http.MethodPut {
			if err := decodeJSONBody(w, r, &req, true); err != nil {
				errJSON(w, http.StatusBadRequest, err.Error())
				return
			}
		}
		cred, err := adminCredentials(req.PublicKey, req.PrivateKey, r)
		if err != nil {
			errJSON(w, http.StatusBadRequest, err.Error())
			return
		}
		access, err := s.authorizeTiDBCloudAdminAccess(r.Context(), cred, "admin_object_namespace_set")
		if err != nil {
			writeAdminTiDBCloudError(w, r.Context(), err, "set object namespace")
			return
		}
		_, _, ok := s.authorizedAdminTenant(w, r, tenantID, access.OrganizationID, false, false)
		if !ok {
			return
		}
		ns := strings.TrimSpace(req.NamespaceID)
		if r.Method == http.MethodDelete {
			ns = ""
		} else {
			cleaned, nsErr := normalizeObjectNamespaceID(ns)
			if nsErr != nil {
				errJSON(w, http.StatusBadRequest, nsErr.Error())
				return
			}
			ns = cleaned
		}
		if err := s.meta.SetTenantObjectNamespaceID(r.Context(), tenantID, ns); err != nil {
			errJSON(w, backendErrorStatus(r.Context(), err), "update object namespace failed")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"tenant_id": tenantID, "namespace_id": ns})
	default:
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleObjectCredentials(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	scope := ScopeFromContext(r.Context())
	if scope == nil || scope.TenantID == "" {
		errJSON(w, http.StatusUnauthorized, "authentication required")
		return
	}
	var req objectCredentialsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errJSON(w, http.StatusBadRequest, "invalid json")
		return
	}
	scheme, bucket, key, err := parseMintObjectURI(strings.TrimSpace(req.URI))
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	if !mintableObjectScheme(scheme) {
		errJSON(w, http.StatusBadRequest, "server-minted credentials support s3, cos, tos, and oss; use --auth=local for other schemes")
		return
	}
	orgID := strings.TrimSpace(scope.TiDBCloudOrgID)
	if orgID == "" {
		if b, berr := s.meta.GetTenantTiDBCloudOrgBinding(r.Context(), scope.TenantID); berr == nil {
			orgID = b.OrganizationID
		}
	}
	if orgID == "" {
		errJSON(w, http.StatusConflict, "tenant is not bound to a TiDB Cloud organization")
		return
	}
	binding, err := s.meta.GetTenantTiDBCloudOrgBinding(r.Context(), scope.TenantID)
	if err != nil {
		errJSON(w, http.StatusConflict, "tenant organization binding not found")
		return
	}
	ns := strings.Trim(binding.ObjectNamespaceID, "/")
	if ns == "" {
		errJSON(w, http.StatusForbidden, "object namespace is not configured for this tenant")
		return
	}
	backend, err := s.meta.GetOrgObjectBackendByBucket(r.Context(), orgID, scheme, bucket)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusForbidden, "no object backend is configured for this bucket")
			return
		}
		errJSON(w, backendErrorStatus(r.Context(), err), "lookup object backend failed")
		return
	}
	key = strings.Trim(key, "/")
	allowed := ns
	if backend.Prefix != "" {
		allowed = strings.Trim(backend.Prefix, "/") + "/" + ns
	}
	if !objectKeyInNamespace(key, allowed) {
		errJSON(w, http.StatusForbidden, "uri is outside the tenant object namespace")
		return
	}
	creds, err := s.mintObjectSession(r.Context(), backend, allowed, req.Write)
	if err != nil {
		errJSON(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, creds)
}

func viewOrgObjectBackend(b *meta.OrgObjectBackend) adminObjectBackendView {
	return adminObjectBackendView{
		ID:             b.ID,
		OrganizationID: b.OrganizationID,
		Scheme:         b.Scheme,
		Endpoint:       b.Endpoint,
		Region:         b.Region,
		ForcePathStyle: b.ForcePathStyle,
		Bucket:         b.Bucket,
		Prefix:         b.Prefix,
		CredentialKind: b.CredentialKind,
		RoleARN:        b.RoleARN,
		AccessKeyID:    b.AccessKeyID,
		HasSecret:      len(b.SecretCipher) > 0,
		HasExternalID:  len(b.ExternalIDCipher) > 0,
		MaxSessionTTL:  b.MaxSessionTTLSec,
	}
}

func (s *Server) mintObjectSession(ctx context.Context, backend *meta.OrgObjectBackend, prefix string, write bool) (*objectCredentialsResponse, error) {
	secret := ""
	if len(backend.SecretCipher) > 0 {
		plain, err := s.pool.Decrypt(ctx, backend.SecretCipher)
		if err != nil {
			return nil, fmt.Errorf("decrypt object backend secret: %w", err)
		}
		secret = string(plain)
	}
	externalID := ""
	if len(backend.ExternalIDCipher) > 0 {
		plain, err := s.pool.Decrypt(ctx, backend.ExternalIDCipher)
		if err != nil {
			return nil, fmt.Errorf("decrypt object backend external id: %w", err)
		}
		externalID = string(plain)
	}
	ttl := backend.MaxSessionTTLSec
	if ttl <= 0 {
		ttl = 3600
	}
	if ttl > 43200 {
		ttl = 43200
	}
	policy := objectSessionPolicy(backend.Bucket, prefix, write)
	region := strings.TrimSpace(backend.Region)
	if region == "" {
		region = "us-east-1"
	}
	var cfg aws.Config
	var err error
	if strings.TrimSpace(backend.AccessKeyID) == "" {
		if backend.CredentialKind != meta.ObjectCredentialRole {
			return nil, fmt.Errorf("object backend is missing access_key_id")
		}
		cfg, err = config.LoadDefaultConfig(ctx, config.WithRegion(region))
	} else {
		cfg, err = config.LoadDefaultConfig(ctx,
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(backend.AccessKeyID, secret, "")),
			config.WithRegion(region))
	}
	if err != nil {
		return nil, fmt.Errorf("load sts config: %w", err)
	}
	cli := sts.NewFromConfig(cfg)
	var ak, sk, tok string
	var exp *time.Time
	switch backend.CredentialKind {
	case meta.ObjectCredentialRole:
		in := &sts.AssumeRoleInput{
			RoleArn:         aws.String(backend.RoleARN),
			RoleSessionName: aws.String("drive9-object"),
			DurationSeconds: aws.Int32(int32(ttl)),
			Policy:          aws.String(policy),
		}
		if externalID != "" {
			in.ExternalId = aws.String(externalID)
		}
		out, err := cli.AssumeRole(ctx, in)
		if err != nil {
			return nil, fmt.Errorf("assume role: %w", err)
		}
		ak = aws.ToString(out.Credentials.AccessKeyId)
		sk = aws.ToString(out.Credentials.SecretAccessKey)
		tok = aws.ToString(out.Credentials.SessionToken)
		if out.Credentials.Expiration != nil {
			t := out.Credentials.Expiration.UTC()
			exp = &t
		}
	default:
		out, err := cli.GetFederationToken(ctx, &sts.GetFederationTokenInput{
			Name:            aws.String("drive9-object"),
			DurationSeconds: aws.Int32(int32(ttl)),
			Policy:          aws.String(policy),
		})
		if err != nil {
			return nil, fmt.Errorf("get federation token: %w", err)
		}
		ak = aws.ToString(out.Credentials.AccessKeyId)
		sk = aws.ToString(out.Credentials.SecretAccessKey)
		tok = aws.ToString(out.Credentials.SessionToken)
		if out.Credentials.Expiration != nil {
			t := out.Credentials.Expiration.UTC()
			exp = &t
		}
	}
	resp := &objectCredentialsResponse{
		AccessKeyID:     ak,
		SecretAccessKey: sk,
		SessionToken:    tok,
		Endpoint:        backend.Endpoint,
		Region:          backend.Region,
		ForcePathStyle:  backend.ForcePathStyle,
		Scheme:          backend.Scheme,
		Bucket:          backend.Bucket,
		Prefix:          prefix,
	}
	if exp != nil {
		resp.Expiration = exp.Format(time.RFC3339)
	}
	return resp, nil
}

func mintableObjectScheme(scheme string) bool {
	switch strings.ToLower(strings.TrimSpace(scheme)) {
	case "s3", "cos", "tos", "oss":
		return true
	default:
		return false
	}
}

func normalizeObjectNamespaceID(ns string) (string, error) {
	ns = strings.TrimSpace(ns)
	ns = strings.Trim(ns, "/")
	if ns == "" {
		return "", fmt.Errorf("namespace_id is required")
	}
	if strings.ContainsAny(ns, "/\\") || strings.Contains(ns, "..") {
		return "", fmt.Errorf("namespace_id must not contain slashes or ..")
	}
	if len(ns) > 255 {
		return "", fmt.Errorf("namespace_id is too long")
	}
	return ns, nil
}

func objectKeyInNamespace(key, allowed string) bool {
	key = strings.Trim(key, "/")
	allowed = strings.Trim(allowed, "/")
	if allowed == "" {
		return false
	}
	return key == allowed || strings.HasPrefix(key+"/", allowed+"/")
}

func objectSessionPolicy(bucket, prefix string, write bool) string {
	prefix = strings.Trim(prefix, "/")
	actions := []string{`"s3:GetObject"`, `"s3:ListBucket"`}
	if write {
		actions = append(actions, `"s3:PutObject"`, `"s3:DeleteObject"`, `"s3:AbortMultipartUpload"`, `"s3:ListMultipartUploadParts"`)
	}
	objARN := fmt.Sprintf("arn:aws:s3:::%s/%s/*", bucket, prefix)
	bucketARN := fmt.Sprintf("arn:aws:s3:::%s", bucket)
	return fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":[%s],"Resource":[%q]},{"Effect":"Allow","Action":["s3:ListBucket"],"Resource":[%q],"Condition":{"StringLike":{"s3:prefix":[%q,%q]}}}]}`,
		strings.Join(actions, ","), objARN, bucketARN, prefix, prefix+"/*")
}

func parseMintObjectURI(raw string) (scheme, bucket, key string, err error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", "", "", fmt.Errorf("invalid object uri")
	}
	scheme = strings.ToLower(u.Scheme)
	switch scheme {
	case "gcs":
		scheme = "gs"
	case "azure":
		scheme = "az"
	case "s3", "cos", "tos", "oss", "gs", "az":
	default:
		return "", "", "", fmt.Errorf("unsupported object scheme %q", u.Scheme)
	}
	return scheme, u.Host, strings.TrimPrefix(u.Path, "/"), nil
}
