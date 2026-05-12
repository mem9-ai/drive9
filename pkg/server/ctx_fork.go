package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
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
		} else if errors.Is(err, meta.ErrNotFound) {
			code = http.StatusNotFound
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
	_ = displayName // The CLI stores the user-facing ctx name locally; MetaDB tenants currently have no display-name column.
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
	forkRoot.DBPasswordCipher = append([]byte(nil), source.DBPasswordCipher...)
	if source.S3BucketKeyEnabled != nil {
		v := *source.S3BucketKeyEnabled
		forkRoot.S3BucketKeyEnabled = &v
	}
	forkRoot.ClaimURL = ""
	forkRoot.ClaimExpiresAt = nil
	forkRoot.CreatedAt = now
	forkRoot.UpdatedAt = now
	if err := s.meta.InsertTenant(ctx, &forkRoot); err != nil {
		return nil, err
	}

	cluster, err := branchProvisioner.ProvisionBranch(ctx, forkID, clusterInfoFromTenant(source))
	if cluster != nil {
		cluster.Provider = source.Provider
	}
	if err != nil {
		s.handleForkBranchProvisionError(ctx, forkID, branchProvisioner, cluster)
		return nil, err
	}
	if err := s.meta.UpdateTenantBranch(ctx, forkID, &meta.Tenant{
		Provider:       cluster.Provider,
		ClusterID:      cluster.ClusterID,
		BranchID:       cluster.BranchID,
		ClaimURL:       cluster.ClaimURL,
		ClaimExpiresAt: cluster.ClaimExpiresAt,
	}); err != nil {
		s.deleteUnpersistedForkBranch(ctx, forkID, branchProvisioner, cluster)
		return nil, err
	}
	cipherPass, err := s.pool.Encrypt(ctx, []byte(cluster.Password))
	if err != nil {
		s.markForkFailedAndCleanup(ctx, forkID)
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
		s.markForkFailedAndCleanup(ctx, forkID)
		return nil, err
	}

	dsn := tenantDSN(cluster.Username, cluster.Password, cluster.Host, cluster.Port, cluster.DBName, true)
	if err := s.provisioner.InitSchema(ctx, dsn); err != nil {
		s.markForkFailedAndCleanup(ctx, forkID)
		return nil, err
	}
	store, err := datastore.Open(dsn)
	if err != nil {
		s.markForkFailedAndCleanup(ctx, forkID)
		return nil, err
	}
	defer func() { _ = store.Close() }()

	if active, err := store.HasActiveUploads(ctx); err != nil {
		s.markForkFailedAndCleanup(ctx, forkID)
		return nil, err
	} else if active {
		s.markForkFailedAndCleanup(ctx, forkID)
		return nil, forkErr(http.StatusConflict, "source snapshot contains active uploads")
	}
	if err := store.SanitizeForkRuntimeState(ctx); err != nil {
		s.markForkFailedAndCleanup(ctx, forkID)
		return nil, err
	}
	if err := s.meta.CopyQuotaConfig(ctx, source.ID, forkID); err != nil {
		s.markForkFailedAndCleanup(ctx, forkID)
		return nil, err
	}
	if err := s.backfillForkQuota(ctx, forkID, store); err != nil {
		s.markForkFailedAndCleanup(ctx, forkID)
		return nil, err
	}
	if err := s.meta.UpdateTenantSchemaVersion(ctx, forkID, schema.CurrentTiDBTenantSchemaVersion); err != nil {
		s.markForkFailedAndCleanup(ctx, forkID)
		return nil, err
	}
	if err := s.meta.UpdateTenantStatus(ctx, forkID, meta.TenantActive); err != nil {
		s.markForkFailedAndCleanup(ctx, forkID)
		return nil, err
	}
	apiToken, err := token.IssueToken(s.tokenSecret, forkID, 1)
	if err != nil {
		s.markForkFailedAndCleanup(ctx, forkID)
		return nil, err
	}
	cipherToken, err := s.pool.Encrypt(ctx, []byte(apiToken))
	if err != nil {
		s.markForkFailedAndCleanup(ctx, forkID)
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
		s.markForkFailedAndCleanup(ctx, forkID)
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

func (s *Server) markForkFailed(ctx context.Context, forkID string) {
	if err := s.meta.UpdateTenantStatus(ctx, forkID, meta.TenantFailed); err != nil {
		logger.Error(ctx, "fork_mark_failed_failed", zap.String("tenant_id", forkID), zap.Error(err))
	}
}

func (s *Server) markForkFailedAndCleanup(ctx context.Context, forkID string) {
	s.markForkFailed(ctx, forkID)
	go s.cleanupForkTenant(backgroundWithTrace(ctx), forkID)
}

func (s *Server) handleForkBranchProvisionError(ctx context.Context, forkID string, branchProvisioner tenant.BranchProvisioner, cluster *tenant.ClusterInfo) {
	if cluster != nil && cluster.ClusterID != "" && cluster.BranchID != "" {
		if err := s.meta.UpdateTenantBranch(ctx, forkID, &meta.Tenant{
			Provider:       cluster.Provider,
			ClusterID:      cluster.ClusterID,
			BranchID:       cluster.BranchID,
			ClaimURL:       cluster.ClaimURL,
			ClaimExpiresAt: cluster.ClaimExpiresAt,
		}); err != nil {
			s.deleteUnpersistedForkBranch(ctx, forkID, branchProvisioner, cluster)
			return
		}
		s.markForkFailedAndCleanup(ctx, forkID)
		return
	}
	s.markForkFailed(ctx, forkID)
}

func (s *Server) deleteUnpersistedForkBranch(ctx context.Context, forkID string, branchProvisioner tenant.BranchProvisioner, cluster *tenant.ClusterInfo) {
	if cluster != nil && cluster.ClusterID != "" && cluster.BranchID != "" {
		if err := branchProvisioner.DeleteBranch(ctx, cluster.ClusterID, cluster.BranchID); err != nil {
			logger.Error(ctx, "fork_delete_unpersisted_branch_failed",
				zap.String("tenant_id", forkID),
				zap.String("cluster_id", cluster.ClusterID),
				zap.String("branch_id", cluster.BranchID),
				zap.Error(err))
			if perr := s.meta.UpdateTenantBranch(ctx, forkID, &meta.Tenant{
				Provider:       cluster.Provider,
				ClusterID:      cluster.ClusterID,
				BranchID:       cluster.BranchID,
				ClaimURL:       cluster.ClaimURL,
				ClaimExpiresAt: cluster.ClaimExpiresAt,
			}); perr != nil {
				logger.Error(ctx, "fork_persist_branch_after_delete_failure_failed",
					zap.String("tenant_id", forkID),
					zap.String("cluster_id", cluster.ClusterID),
					zap.String("branch_id", cluster.BranchID),
					zap.Error(perr))
				s.markForkFailed(ctx, forkID)
				return
			}
			s.markForkFailedAndCleanup(ctx, forkID)
			return
		}
	}
	if cluster == nil || cluster.ClusterID == "" || cluster.BranchID == "" {
		s.markForkFailed(ctx, forkID)
		return
	}
	if err := s.meta.UpdateTenantStatus(ctx, forkID, meta.TenantDeleted); err != nil {
		logger.Error(ctx, "fork_mark_deleted_after_unpersisted_branch_failed", zap.String("tenant_id", forkID), zap.Error(err))
	}
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
		if errors.Is(err, meta.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "tenant not found")
			return
		}
		errJSON(w, http.StatusInternalServerError, "tenant lookup failed")
		return
	}
	if t.Kind != meta.TenantKindFork {
		errJSON(w, http.StatusConflict, "only fork tenants can be deleted through ctx delete")
		return
	}
	if t.Status == meta.TenantDeleted {
		errJSON(w, http.StatusNotFound, "tenant not found")
		return
	}
	if t.Status == meta.TenantDeleting {
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": string(meta.TenantDeleting)})
		return
	}
	updated, err := s.meta.UpdateTenantStatusIf(r.Context(), t.ID, t.Status, meta.TenantDeleting)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "failed to mark tenant deleting")
		return
	}
	if !updated {
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": string(meta.TenantDeleting)})
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
	if t.BranchID == "" {
		logger.Error(ctx, "fork_cleanup_missing_branch_id", zap.String("tenant_id", tenantID), zap.String("status", string(t.Status)))
		return
	}
	if t.Status != meta.TenantDeleting {
		s.cleanupFailedForkBranch(ctx, t)
		return
	}
	store, err := s.openTenantStore(ctx, t)
	if err != nil {
		logger.Error(ctx, "fork_cleanup_open_store_failed", zap.String("tenant_id", tenantID), zap.Error(err))
		return
	}
	defer func() { _ = store.Close() }()
	if err := s.enqueueForkConfirmedRefs(ctx, t, store); err != nil {
		logger.Error(ctx, "fork_cleanup_enqueue_refs_failed", zap.String("tenant_id", tenantID), zap.Error(err))
		return
	}
	if err := s.enqueueForkFileGCTaskRefs(ctx, t, store); err != nil {
		logger.Error(ctx, "fork_cleanup_enqueue_file_gc_refs_failed", zap.String("tenant_id", tenantID), zap.Error(err))
		return
	}
	if err := store.SanitizeForkRuntimeState(ctx); err != nil {
		logger.Error(ctx, "fork_cleanup_sanitize_failed", zap.String("tenant_id", tenantID), zap.Error(err))
		return
	}
	branchProvisioner, ok := s.provisioner.(tenant.BranchProvisioner)
	if !ok {
		logger.Error(ctx, "fork_cleanup_branch_provisioner_missing", zap.String("tenant_id", tenantID))
		return
	}
	if err := branchProvisioner.DeleteBranch(ctx, t.ClusterID, t.BranchID); err != nil {
		logger.Error(ctx, "fork_cleanup_delete_branch_failed", zap.String("tenant_id", tenantID), zap.Error(err))
		return
	}
	if err := s.meta.UpdateTenantStatus(ctx, tenantID, meta.TenantDeleted); err != nil {
		logger.Error(ctx, "fork_cleanup_mark_deleted_failed", zap.String("tenant_id", tenantID), zap.Error(err))
		return
	}
}

func (s *Server) cleanupFailedForkBranch(ctx context.Context, t *meta.Tenant) {
	branchProvisioner, ok := s.provisioner.(tenant.BranchProvisioner)
	if !ok {
		logger.Error(ctx, "fork_cleanup_failed_branch_provisioner_missing", zap.String("tenant_id", t.ID))
		return
	}
	if err := branchProvisioner.DeleteBranch(ctx, t.ClusterID, t.BranchID); err != nil {
		logger.Error(ctx, "fork_cleanup_failed_delete_branch_failed", zap.String("tenant_id", t.ID), zap.Error(err))
		return
	}
	if err := s.meta.UpdateTenantStatus(ctx, t.ID, meta.TenantDeleted); err != nil {
		logger.Error(ctx, "fork_cleanup_failed_mark_deleted_failed", zap.String("tenant_id", t.ID), zap.Error(err))
	}
}

func (s *Server) openTenantStore(ctx context.Context, t *meta.Tenant) (*datastore.Store, error) {
	plain, err := s.pool.Decrypt(ctx, t.DBPasswordCipher)
	if err != nil {
		return nil, err
	}
	return datastore.Open(tenantDSN(t.DBUser, string(plain), t.DBHost, t.DBPort, t.DBName, t.DBTLS))
}

func (s *Server) enqueueForkFileGCTaskRefs(ctx context.Context, t *meta.Tenant, store *datastore.Store) error {
	cursor := ""
	for {
		refs, next, err := store.ListFileGCTaskS3Refs(ctx, cursor, 500)
		if err != nil {
			return err
		}
		for _, ref := range refs {
			if err := s.meta.EnqueueObjectGCCandidate(ctx, &meta.ObjectGCCandidateInput{
				NamespaceID:    t.StorageNamespaceID,
				StorageRef:     ref.StorageRef,
				StorageRefHash: ref.StorageRefHash,
				Reason:         meta.ObjectGCReasonFileDelete,
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
	for _, status := range []meta.TenantStatus{meta.TenantDeleting, meta.TenantFailed, meta.TenantProvisioning} {
		tenants, err := s.meta.ListTenantsByStatus(ctx, status, 1000)
		if err != nil {
			logger.Error(ctx, "resume_fork_cleanup_list_failed", zap.String("status", string(status)), zap.Error(err))
			continue
		}
		s.resumeForkCleanup(ctx, tenants)
	}
}

func (s *Server) resumeForkCleanup(ctx context.Context, tenants []meta.Tenant) {
	const maxConcurrentForkCleanup = 4
	sem := make(chan struct{}, maxConcurrentForkCleanup)
	var wg sync.WaitGroup
	for i := range tenants {
		t := tenants[i]
		if t.Kind != meta.TenantKindFork {
			continue
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(tenantID string) {
			defer wg.Done()
			defer func() { <-sem }()
			s.cleanupForkTenant(backgroundWithTrace(ctx), tenantID)
		}(t.ID)
	}
	wg.Wait()
}
