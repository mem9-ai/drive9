package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/tenant"
	"github.com/mem9-ai/dat9/pkg/tenant/schema"
	"github.com/mem9-ai/dat9/pkg/tenant/token"
	"go.uber.org/zap"
)

type forkContextRequest struct {
	Name string `json:"name"`
}

type forkContextResponse struct {
	TenantID       string `json:"tenant_id"`
	APIKey         string `json:"api_key"`
	Status         string `json:"status"`
	ParentTenantID string `json:"parent_tenant_id"`
	Storage        string `json:"storage"`
}

func (s *Server) handleCtxFork(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.meta == nil || s.pool == nil || s.provisioner == nil || len(s.tokenSecret) == 0 {
		errJSON(w, http.StatusNotFound, "ctx fork not enabled")
		return
	}
	scope := ScopeFromContext(r.Context())
	if scope == nil || scope.TenantID == "" {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	var req forkContextRequest
	if r.Body != nil {
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			errJSON(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
			return
		}
	}
	if strings.TrimSpace(req.Name) == "" {
		req.Name = "fork"
	}

	resp, err := s.createForkTenant(r.Context(), scope.TenantID, req.Name)
	if err != nil {
		code := http.StatusInternalServerError
		var statusErr *forkStatusError
		if errors.As(err, &statusErr) {
			code = statusErr.code
		}
		errJSON(w, code, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
}

type forkStatusError struct {
	code int
	msg  string
}

func (e *forkStatusError) Error() string { return e.msg }

func forkErr(code int, msg string) error {
	return &forkStatusError{code: code, msg: msg}
}

func (s *Server) createForkTenant(ctx context.Context, sourceTenantID, displayName string) (*forkContextResponse, error) {
	_ = displayName
	source, err := s.meta.GetTenant(ctx, sourceTenantID)
	if err != nil {
		return nil, err
	}
	if source.Status != meta.TenantActive {
		return nil, forkErr(http.StatusConflict, "source tenant is not active")
	}
	if source.Provider != tenant.ProviderTiDBCloudStarter {
		return nil, forkErr(http.StatusConflict, "ctx fork requires TiDB Cloud Starter provider")
	}
	if source.StorageNamespaceID == "" {
		return nil, forkErr(http.StatusConflict, "source tenant storage namespace is not initialized")
	}
	branchProvisioner, ok := s.provisioner.(tenant.BranchProvisioner)
	if !ok {
		return nil, forkErr(http.StatusConflict, "ctx fork requires branch-capable provisioner")
	}
	if hasReservations, err := s.sourceHasActiveUploadReservations(ctx, source.ID); err != nil {
		return nil, err
	} else if hasReservations {
		return nil, forkErr(http.StatusConflict, "source tenant has active upload reservations")
	}

	forkID := token.NewID()
	now := time.Now().UTC()
	forkRoot := *source
	forkRoot.ID = forkID
	forkRoot.Status = meta.TenantProvisioning
	forkRoot.Kind = meta.TenantKindFork
	forkRoot.ParentTenantID = source.ID
	forkRoot.StorageNamespaceID = source.StorageNamespaceID
	forkRoot.BranchID = ""
	forkRoot.ClaimURL = ""
	forkRoot.ClaimExpiresAt = nil
	forkRoot.CreatedAt = now
	forkRoot.UpdatedAt = now
	if err := s.meta.InsertTenant(ctx, &forkRoot); err != nil {
		return nil, err
	}

	cluster, err := branchProvisioner.ProvisionBranch(ctx, forkID, clusterInfoFromTenant(source))
	if err != nil {
		_ = s.meta.UpdateTenantStatus(ctx, forkID, meta.TenantFailed)
		return nil, err
	}
	cluster.Provider = source.Provider
	cipherPass, err := s.pool.Encrypt(ctx, []byte(cluster.Password))
	if err != nil {
		_ = s.meta.UpdateTenantStatus(ctx, forkID, meta.TenantFailed)
		return nil, err
	}
	if err := s.meta.UpdateTenantConnection(ctx, forkID, &meta.Tenant{
		DBHost:           cluster.Host,
		DBPort:           cluster.Port,
		DBUser:           cluster.Username,
		DBPasswordCipher: cipherPass,
		DBName:           cluster.DBName,
		DBTLS:            true,
		Provider:         cluster.Provider,
		ClusterID:        cluster.ClusterID,
		BranchID:         cluster.BranchID,
		ClaimURL:         cluster.ClaimURL,
		ClaimExpiresAt:   cluster.ClaimExpiresAt,
	}); err != nil {
		_ = s.meta.UpdateTenantStatus(ctx, forkID, meta.TenantFailed)
		return nil, err
	}

	dsn := tenantDSN(cluster.Username, cluster.Password, cluster.Host, cluster.Port, cluster.DBName, true)
	if err := s.provisioner.InitSchema(ctx, dsn); err != nil {
		_ = s.meta.UpdateTenantStatus(ctx, forkID, meta.TenantFailed)
		return nil, err
	}
	store, err := datastore.Open(dsn)
	if err != nil {
		_ = s.meta.UpdateTenantStatus(ctx, forkID, meta.TenantFailed)
		return nil, err
	}
	defer func() { _ = store.Close() }()

	if active, err := store.HasActiveUploads(ctx); err != nil {
		_ = s.meta.UpdateTenantStatus(ctx, forkID, meta.TenantFailed)
		return nil, err
	} else if active {
		_ = s.meta.UpdateTenantStatus(ctx, forkID, meta.TenantFailed)
		return nil, forkErr(http.StatusConflict, "source snapshot contains active uploads")
	}
	if err := store.SanitizeForkRuntimeState(ctx); err != nil {
		_ = s.meta.UpdateTenantStatus(ctx, forkID, meta.TenantFailed)
		return nil, err
	}
	if err := s.meta.CopyQuotaConfig(ctx, source.ID, forkID); err != nil {
		_ = s.meta.UpdateTenantStatus(ctx, forkID, meta.TenantFailed)
		return nil, err
	}
	if err := s.backfillForkQuota(ctx, forkID, store); err != nil {
		_ = s.meta.UpdateTenantStatus(ctx, forkID, meta.TenantFailed)
		return nil, err
	}
	if err := s.meta.UpdateTenantSchemaVersion(ctx, forkID, schema.CurrentTiDBTenantSchemaVersion); err != nil {
		_ = s.meta.UpdateTenantStatus(ctx, forkID, meta.TenantFailed)
		return nil, err
	}
	if err := s.meta.UpdateTenantStatus(ctx, forkID, meta.TenantActive); err != nil {
		return nil, err
	}
	apiToken, err := token.IssueToken(s.tokenSecret, forkID, 1)
	if err != nil {
		_ = s.meta.UpdateTenantStatus(ctx, forkID, meta.TenantFailed)
		return nil, err
	}
	cipherToken, err := s.pool.Encrypt(ctx, []byte(apiToken))
	if err != nil {
		_ = s.meta.UpdateTenantStatus(ctx, forkID, meta.TenantFailed)
		return nil, err
	}
	if err := s.meta.InsertAPIKey(ctx, &meta.APIKey{
		ID:            token.NewID(),
		TenantID:      forkID,
		KeyName:       "default",
		JWTCiphertext: cipherToken,
		JWTHash:       token.HashToken(apiToken),
		TokenVersion:  1,
		Status:        meta.APIKeyActive,
		IssuedAt:      time.Now().UTC(),
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
	}); err != nil {
		_ = s.meta.UpdateTenantStatus(ctx, forkID, meta.TenantFailed)
		return nil, err
	}
	return &forkContextResponse{
		TenantID:       forkID,
		APIKey:         apiToken,
		Status:         string(meta.TenantActive),
		ParentTenantID: source.ID,
		Storage:        "shared",
	}, nil
}

func clusterInfoFromTenant(t *meta.Tenant) *tenant.ClusterInfo {
	return &tenant.ClusterInfo{
		TenantID:  t.ID,
		ClusterID: t.ClusterID,
		BranchID:  t.BranchID,
		Host:      t.DBHost,
		Port:      t.DBPort,
		Username:  t.DBUser,
		DBName:    t.DBName,
		Provider:  t.Provider,
	}
}

func (s *Server) sourceHasActiveUploadReservations(ctx context.Context, tenantID string) (bool, error) {
	if _, err := s.meta.ExpireActiveReservations(ctx); err != nil {
		return false, err
	}
	return s.meta.HasActiveUploadReservations(ctx, tenantID)
}

func (s *Server) backfillForkQuota(ctx context.Context, tenantID string, store *datastore.Store) error {
	var storageBytes int64
	var mediaCount int64
	cursor := ""
	for {
		files, next, err := store.ListConfirmedFileSummaries(ctx, cursor, 500)
		if err != nil {
			return err
		}
		for _, f := range files {
			isMedia := forkQuotaMediaContentType(f.ContentType)
			if err := s.meta.UpsertFileMeta(ctx, &meta.FileMeta{
				TenantID:  tenantID,
				FileID:    f.FileID,
				SizeBytes: f.SizeBytes,
				IsMedia:   isMedia,
			}); err != nil {
				return err
			}
			storageBytes += f.SizeBytes
			if isMedia {
				mediaCount++
			}
		}
		if next == "" {
			break
		}
		cursor = next
	}
	return s.meta.SetQuotaCounters(ctx, tenantID, storageBytes, mediaCount)
}

func forkQuotaMediaContentType(contentType string) bool {
	return strings.HasPrefix(contentType, "image/") || strings.HasPrefix(contentType, "audio/")
}

func (s *Server) handleCtxDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.meta == nil || s.pool == nil {
		errJSON(w, http.StatusNotFound, "ctx delete not enabled")
		return
	}
	scope := ScopeFromContext(r.Context())
	if scope == nil || scope.TenantID == "" {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	t, err := s.meta.GetTenant(r.Context(), scope.TenantID)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "tenant lookup failed")
		return
	}
	if t.Kind != meta.TenantKindFork {
		errJSON(w, http.StatusConflict, "only fork tenants can be deleted through ctx delete")
		return
	}
	if err := s.meta.UpdateTenantStatus(r.Context(), t.ID, meta.TenantDeleting); err != nil {
		errJSON(w, http.StatusInternalServerError, "failed to mark tenant deleting")
		return
	}
	_ = s.meta.RevokeTenantAPIKeys(r.Context(), t.ID)
	go s.cleanupForkTenant(backgroundWithTrace(r.Context()), t.ID)
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": string(meta.TenantDeleting)})
}

func (s *Server) cleanupForkTenant(ctx context.Context, tenantID string) {
	ctx = backgroundWithTrace(ctx)
	t, err := s.meta.GetTenant(ctx, tenantID)
	if err != nil {
		logger.Error(ctx, "fork_cleanup_get_tenant_failed", zap.String("tenant_id", tenantID), zap.Error(err))
		return
	}
	if t.Kind != meta.TenantKindFork {
		return
	}
	if err := s.pool.InvalidateAndWait(ctx, tenantID); err != nil {
		logger.Error(ctx, "fork_cleanup_pool_drain_failed", zap.String("tenant_id", tenantID), zap.Error(err))
		return
	}
	_ = s.meta.AbortActiveUploadReservations(ctx, tenantID)
	store, err := s.openTenantStore(ctx, t)
	if err != nil {
		logger.Error(ctx, "fork_cleanup_open_store_failed", zap.String("tenant_id", tenantID), zap.Error(err))
		return
	}
	defer func() { _ = store.Close() }()
	if err := store.SanitizeForkRuntimeState(ctx); err != nil {
		logger.Error(ctx, "fork_cleanup_sanitize_failed", zap.String("tenant_id", tenantID), zap.Error(err))
		return
	}
	if err := s.enqueueForkConfirmedRefs(ctx, t, store); err != nil {
		logger.Error(ctx, "fork_cleanup_enqueue_refs_failed", zap.String("tenant_id", tenantID), zap.Error(err))
		return
	}
	if branchProvisioner, ok := s.provisioner.(tenant.BranchProvisioner); ok && t.BranchID != "" {
		if err := branchProvisioner.DeleteBranch(ctx, t.ClusterID, t.BranchID); err != nil {
			logger.Error(ctx, "fork_cleanup_delete_branch_failed", zap.String("tenant_id", tenantID), zap.Error(err))
			return
		}
	}
	if err := s.meta.UpdateTenantStatus(ctx, tenantID, meta.TenantDeleted); err != nil {
		logger.Error(ctx, "fork_cleanup_mark_deleted_failed", zap.String("tenant_id", tenantID), zap.Error(err))
		return
	}
}

func (s *Server) openTenantStore(ctx context.Context, t *meta.Tenant) (*datastore.Store, error) {
	plain, err := s.pool.Decrypt(ctx, t.DBPasswordCipher)
	if err != nil {
		return nil, err
	}
	return datastore.Open(tenantDSN(t.DBUser, string(plain), t.DBHost, t.DBPort, t.DBName, t.DBTLS))
}

func (s *Server) enqueueForkConfirmedRefs(ctx context.Context, t *meta.Tenant, store *datastore.Store) error {
	cursor := ""
	for {
		refs, next, err := store.ListConfirmedS3Refs(ctx, cursor, 500)
		if err != nil {
			return err
		}
		for _, ref := range refs {
			if err := s.meta.EnqueueObjectGCCandidate(ctx, &meta.ObjectGCCandidateInput{
				NamespaceID:    t.StorageNamespaceID,
				StorageRef:     ref.StorageRef,
				StorageRefHash: ref.StorageRefHash,
				Reason:         meta.ObjectGCReasonForkDelete,
				SourceTenantID: t.ID,
			}); err != nil {
				return err
			}
		}
		if next == "" {
			return nil
		}
		cursor = next
	}
}

func (s *Server) resumeDeletingForkTenants() {
	ctx := backgroundWithTrace(context.Background())
	tenants, err := s.meta.ListTenantsByStatus(ctx, meta.TenantDeleting, 1000)
	if err != nil {
		logger.Error(ctx, "resume_deleting_forks_list_failed", zap.Error(err))
		return
	}
	for i := range tenants {
		t := tenants[i]
		if t.Kind != meta.TenantKindFork {
			continue
		}
		go s.cleanupForkTenant(backgroundWithTrace(context.Background()), t.ID)
	}
}
