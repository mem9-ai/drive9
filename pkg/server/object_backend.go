package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/google/uuid"

	"github.com/mem9-ai/drive9/pkg/meta"
)

type adminObjectBackendRequest struct {
	PublicKey       string `json:"public_key"`
	PrivateKey      string `json:"private_key"`
	Name            string `json:"name"`
	Scheme          string `json:"scheme"`
	Endpoint        string `json:"endpoint"`
	STSEndpoint     string `json:"sts_endpoint"`
	Region          string `json:"region"`
	AccountID       string `json:"account_id"`
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

type adminObjectBackendPatchRequest struct {
	PublicKey       string  `json:"public_key"`
	PrivateKey      string  `json:"private_key"`
	Name            *string `json:"name"`
	Scheme          *string `json:"scheme"`
	Endpoint        *string `json:"endpoint"`
	STSEndpoint     *string `json:"sts_endpoint"`
	Region          *string `json:"region"`
	AccountID       *string `json:"account_id"`
	ForcePathStyle  *bool   `json:"force_path_style"`
	Bucket          *string `json:"bucket"`
	Prefix          *string `json:"prefix"`
	CredentialKind  *string `json:"credential_kind"`
	RoleARN         *string `json:"role_arn"`
	AccessKeyID     *string `json:"access_key_id"`
	SecretAccessKey *string `json:"secret_access_key"`
	ExternalID      *string `json:"external_id"`
	MaxSessionTTL   *int    `json:"max_session_ttl_sec"`
}

type adminObjectBackendView struct {
	ID             string `json:"id"`
	OrganizationID string `json:"organization_id"`
	Name           string `json:"name,omitempty"`
	Scheme         string `json:"scheme"`
	Endpoint       string `json:"endpoint"`
	STSEndpoint    string `json:"sts_endpoint,omitempty"`
	Region         string `json:"region"`
	AccountID      string `json:"account_id,omitempty"`
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
	AccessKeyID     string `json:"access_key_id,omitempty"`
	SecretAccessKey string `json:"secret_access_key,omitempty"`
	SessionToken    string `json:"session_token,omitempty"`
	SASURL          string `json:"sas_url,omitempty"`
	AccessToken     string `json:"access_token,omitempty"`
	Account         string `json:"account,omitempty"`
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
		case http.MethodGet:
			s.handleAdminObjectBackendGet(w, r, rest)
		case http.MethodPatch, http.MethodPut:
			s.handleAdminObjectBackendUpdate(w, r, rest)
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
		Name:             strings.TrimSpace(req.Name),
		Scheme:           strings.ToLower(strings.TrimSpace(req.Scheme)),
		Endpoint:         strings.TrimSpace(req.Endpoint),
		STSEndpoint:      strings.TrimSpace(req.STSEndpoint),
		Region:           strings.TrimSpace(req.Region),
		AccountID:        strings.TrimSpace(req.AccountID),
		ForcePathStyle:   req.ForcePathStyle,
		Bucket:           strings.TrimSpace(req.Bucket),
		Prefix:           strings.Trim(strings.TrimSpace(req.Prefix), "/"),
		CredentialKind:   kind,
		RoleARN:          strings.TrimSpace(req.RoleARN),
		AccessKeyID:      strings.TrimSpace(req.AccessKeyID),
		MaxSessionTTLSec: req.MaxSessionTTL,
	}
	switch rec.Scheme {
	case "gcs":
		rec.Scheme = "gs"
	case "azure":
		rec.Scheme = "az"
	}
	if rec.Scheme == "" || rec.Bucket == "" {
		errJSON(w, http.StatusBadRequest, "scheme and bucket are required")
		return
	}
	if !mintableObjectScheme(rec.Scheme) {
		errJSON(w, http.StatusBadRequest, "scheme must be s3, cos, tos, oss, gs, or az")
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
		switch rec.Scheme {
		case "gs":
			if secret == "" {
				errJSON(w, http.StatusBadRequest, "secret_access_key must be the GCS service-account JSON")
				return
			}
		case "az":
			if (rec.AccessKeyID == "" && rec.AccountID == "") || secret == "" {
				errJSON(w, http.StatusBadRequest, "azure requires account name (access_key_id or account_id) and account key")
				return
			}
		default:
			if rec.AccessKeyID == "" || secret == "" {
				errJSON(w, http.StatusBadRequest, "access_key_id and secret_access_key are required for credential_kind=static")
				return
			}
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
			errJSON(w, http.StatusConflict, "object backend already exists for this scheme/bucket/prefix/endpoint")
			return
		}
		errJSON(w, backendErrorStatus(r.Context(), err), "create object backend failed")
		return
	}
	writeJSON(w, http.StatusCreated, viewOrgObjectBackend(rec))
}

func (s *Server) handleAdminObjectBackendGet(w http.ResponseWriter, r *http.Request, id string) {
	cred, err := adminCredentialsFromHeaders(r)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	access, err := s.authorizeTiDBCloudAdminAccess(r.Context(), cred, "admin_object_backend_get")
	if err != nil {
		writeAdminTiDBCloudError(w, r.Context(), err, "get object backend")
		return
	}
	row, ok := s.orgObjectBackendForOrg(w, r, id, access.OrganizationID)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, viewOrgObjectBackend(row))
}

func (s *Server) handleAdminObjectBackendUpdate(w http.ResponseWriter, r *http.Request, id string) {
	var req adminObjectBackendPatchRequest
	if err := decodeJSONBody(w, r, &req, true); err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	cred, err := adminCredentials(req.PublicKey, req.PrivateKey, r)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	access, err := s.authorizeTiDBCloudAdminAccess(r.Context(), cred, "admin_object_backend_update")
	if err != nil {
		writeAdminTiDBCloudError(w, r.Context(), err, "update object backend")
		return
	}
	row, ok := s.orgObjectBackendForOrg(w, r, id, access.OrganizationID)
	if !ok {
		return
	}
	if req.Name != nil {
		row.Name = strings.TrimSpace(*req.Name)
	}
	if req.Scheme != nil {
		row.Scheme = strings.ToLower(strings.TrimSpace(*req.Scheme))
	}
	if req.Endpoint != nil {
		row.Endpoint = strings.TrimSpace(*req.Endpoint)
	}
	if req.STSEndpoint != nil {
		row.STSEndpoint = strings.TrimSpace(*req.STSEndpoint)
	}
	if req.Region != nil {
		row.Region = strings.TrimSpace(*req.Region)
	}
	if req.AccountID != nil {
		row.AccountID = strings.TrimSpace(*req.AccountID)
	}
	if req.ForcePathStyle != nil {
		row.ForcePathStyle = *req.ForcePathStyle
	}
	if req.Bucket != nil {
		row.Bucket = strings.TrimSpace(*req.Bucket)
	}
	if req.Prefix != nil {
		row.Prefix = strings.Trim(strings.TrimSpace(*req.Prefix), "/")
	}
	if req.CredentialKind != nil {
		row.CredentialKind = strings.TrimSpace(*req.CredentialKind)
	}
	if req.RoleARN != nil {
		row.RoleARN = strings.TrimSpace(*req.RoleARN)
	}
	if req.AccessKeyID != nil {
		row.AccessKeyID = strings.TrimSpace(*req.AccessKeyID)
	}
	if req.MaxSessionTTL != nil {
		row.MaxSessionTTLSec = *req.MaxSessionTTL
		if row.MaxSessionTTLSec > 43200 {
			row.MaxSessionTTLSec = 43200
		}
	}
	if row.Scheme == "" || row.Bucket == "" {
		errJSON(w, http.StatusBadRequest, "scheme and bucket are required")
		return
	}
	if !mintableObjectScheme(row.Scheme) {
		errJSON(w, http.StatusBadRequest, "scheme must be s3, cos, tos, oss, gs, or az")
		return
	}
	if strings.Contains(row.Prefix, "..") {
		errJSON(w, http.StatusBadRequest, "prefix must not contain ..")
		return
	}
	if req.SecretAccessKey != nil {
		secret := strings.TrimSpace(*req.SecretAccessKey)
		if secret == "" {
			row.SecretCipher = nil
		} else {
			cipher, encErr := s.pool.Encrypt(r.Context(), []byte(secret))
			if encErr != nil {
				errJSON(w, http.StatusInternalServerError, "encrypt secret failed")
				return
			}
			row.SecretCipher = cipher
		}
	}
	if req.ExternalID != nil {
		ext := strings.TrimSpace(*req.ExternalID)
		if ext == "" {
			row.ExternalIDCipher = nil
		} else {
			cipher, encErr := s.pool.Encrypt(r.Context(), []byte(ext))
			if encErr != nil {
				errJSON(w, http.StatusInternalServerError, "encrypt external id failed")
				return
			}
			row.ExternalIDCipher = cipher
		}
	}
	if err := s.meta.UpdateOrgObjectBackend(r.Context(), row); err != nil {
		if errors.Is(err, meta.ErrDuplicate) {
			errJSON(w, http.StatusConflict, "object backend already exists for this scheme/bucket/prefix/endpoint")
			return
		}
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "object backend not found")
			return
		}
		errJSON(w, backendErrorStatus(r.Context(), err), "update object backend failed")
		return
	}
	writeJSON(w, http.StatusOK, viewOrgObjectBackend(row))
}

func (s *Server) orgObjectBackendForOrg(w http.ResponseWriter, r *http.Request, id, orgID string) (*meta.OrgObjectBackend, bool) {
	row, err := s.meta.GetOrgObjectBackend(r.Context(), id)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "object backend not found")
			return nil, false
		}
		errJSON(w, backendErrorStatus(r.Context(), err), "lookup object backend failed")
		return nil, false
	}
	if row.OrganizationID != orgID {
		errJSON(w, http.StatusForbidden, "object backend does not belong to this organization")
		return nil, false
	}
	return row, true
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
	if scope.IsScoped {
		errJSON(w, http.StatusForbidden, "scoped token cannot mint object credentials")
		return
	}
	var req objectCredentialsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errJSON(w, http.StatusBadRequest, "invalid json")
		return
	}
	scheme, bucket, key, endpoint, err := parseMintObjectURI(strings.TrimSpace(req.URI))
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	if !mintableObjectScheme(scheme) {
		errJSON(w, http.StatusBadRequest, "server-minted credentials support s3, cos, tos, oss, gs, and az; use --auth=local for other schemes")
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
	rows, err := s.meta.ListOrgObjectBackendsByBucket(r.Context(), orgID, scheme, bucket)
	if err != nil {
		errJSON(w, backendErrorStatus(r.Context(), err), "lookup object backend failed")
		return
	}
	target, err := matchOrgObjectBackend(rows, scheme, bucket, key, endpoint, ns)
	if err != nil {
		switch {
		case errors.Is(err, errNoObjectBackend):
			errJSON(w, http.StatusForbidden, err.Error())
		case errors.Is(err, errURIOutsideNamespace):
			errJSON(w, http.StatusForbidden, err.Error())
		case errors.Is(err, errAmbiguousObjectBackend):
			errJSON(w, http.StatusConflict, err.Error())
		default:
			errJSON(w, http.StatusForbidden, err.Error())
		}
		return
	}
	creds, err := s.mintObjectSession(r.Context(), target.Backend, target.Allowed, req.Write)
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
		Name:           b.Name,
		Scheme:         b.Scheme,
		Endpoint:       b.Endpoint,
		STSEndpoint:    b.STSEndpoint,
		Region:         b.Region,
		AccountID:      b.AccountID,
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

func normalizeObjectNamespaceID(ns string) (string, error) {
	ns = strings.TrimSpace(ns)
	ns = strings.Trim(ns, "/")
	if ns == "" {
		return "", fmt.Errorf("namespace_id is required")
	}
	if strings.ContainsAny(ns, "/\\") || strings.Contains(ns, "..") {
		return "", fmt.Errorf("namespace_id must not contain slashes or parent-directory segments")
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

func parseMintObjectURI(raw string) (scheme, bucket, key, endpoint string, err error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", "", "", "", fmt.Errorf("invalid object uri")
	}
	scheme = strings.ToLower(u.Scheme)
	switch scheme {
	case "gcs":
		scheme = "gs"
	case "azure":
		scheme = "az"
	case "s3", "cos", "tos", "oss", "gs", "az":
	default:
		return "", "", "", "", fmt.Errorf("unsupported object scheme %q", u.Scheme)
	}
	return scheme, u.Host, strings.TrimPrefix(u.Path, "/"), strings.TrimSpace(u.Query().Get("endpoint")), nil
}
