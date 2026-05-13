package server

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"math/big"
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

type forkRequest struct {
	Name string `json:"name"`
}

type forkResponse struct {
	TenantID       string `json:"tenant_id"`
	Name           string `json:"name"`
	APIKey         string `json:"api_key"`
	Status         string `json:"status"`
	Message        string `json:"message,omitempty"`
	ParentTenantID string `json:"parent_tenant_id"`
	Storage        string `json:"storage"`
}

var (
	forkProvisionRetryWindow    = 30 * time.Minute
	forkProvisionInitialBackoff = 2 * time.Second
	forkProvisionMaxBackoff     = 30 * time.Second
)

func (s *Server) startForkProvision(ctx context.Context, forkID string) {
	s.startForkWorker(ctx, func(workerCtx context.Context) {
		s.provisionForkTenantAsync(workerCtx, forkID)
	})
}

func (s *Server) startForkCleanup(ctx context.Context, forkID string) {
	s.startForkWorker(ctx, func(workerCtx context.Context) {
		s.cleanupForkTenant(workerCtx, forkID)
	})
}

func (s *Server) startForkWorker(ctx context.Context, fn func(context.Context)) {
	workerCtx := backgroundWithTrace(ctx)
	if s.forkWorkerCtx != nil {
		workerCtx = contextWithTrace(s.forkWorkerCtx, ctx)
	}

	s.forkWorkerMu.Lock()
	if s.forkWorkerClosed {
		s.forkWorkerMu.Unlock()
		logger.Warn(workerCtx, "fork_worker_start_after_close")
		return
	}
	s.forkWorkerWG.Add(1)
	s.forkWorkerMu.Unlock()

	go func() {
		defer s.forkWorkerWG.Done()
		fn(workerCtx)
	}()
}

func (s *Server) handleFork(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.handleForkCreate(w, r)
	case http.MethodDelete:
		s.handleForkDelete(w, r)
	default:
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleForkCreate(w http.ResponseWriter, r *http.Request) {
	if s.meta == nil || s.pool == nil || s.provisioner == nil || len(s.tokenSecret) == 0 {
		errJSON(w, http.StatusNotFound, "fork not enabled")
		return
	}
	scope := ScopeFromContext(r.Context())
	if scope == nil || scope.TenantID == "" {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	var req forkRequest
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
	w.WriteHeader(http.StatusAccepted)
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

func forkProvisioningMessage(t *meta.Tenant) string {
	if t == nil || t.BranchID == "" {
		return "Creating the database branch. This usually takes 30 seconds to a few minutes."
	}
	if t.DBHost == "" || t.DBPort == 0 || t.DBUser == "" {
		return "Waiting for the database branch to become ready. This usually takes 30 seconds to a few minutes."
	}
	return "Preparing fork metadata and quota counters. Large tenants may take a few minutes."
}

type forkFatalProvisionError struct {
	err error
}

func (e *forkFatalProvisionError) Error() string { return e.err.Error() }

func (e *forkFatalProvisionError) Unwrap() error { return e.err }

func (s *Server) createForkTenant(ctx context.Context, sourceTenantID, displayName string) (*forkResponse, error) {
	// The CLI stores the user-facing ctx name locally; MetaDB tenants currently
	// have no display-name column. We echo the name back in the response so the
	// CLI can confirm the server received it.
	source, err := s.meta.GetTenant(ctx, sourceTenantID)
	if err != nil {
		return nil, err
	}
	if source.Status != meta.TenantActive {
		return nil, forkErr(http.StatusConflict, "source tenant is not active")
	}
	if source.Provider != tenant.ProviderTiDBCloudStarter {
		return nil, forkErr(http.StatusConflict, "fork requires TiDB Cloud Starter provider")
	}
	if source.StorageNamespaceID == "" {
		return nil, forkErr(http.StatusConflict, "source tenant storage namespace is not initialized")
	}
	branchProvisioner, ok := s.provisioner.(tenant.AsyncBranchProvisioner)
	if !ok {
		return nil, forkErr(http.StatusConflict, "fork requires branch-capable provisioner")
	}
	if hasReservations, err := s.sourceHasActiveUploadReservations(ctx, source.ID); err != nil {
		return nil, err
	} else if hasReservations {
		return nil, forkErr(http.StatusConflict, "source tenant has active upload reservations")
	}
	forkPassword, err := generateForkDBPassword(24)
	if err != nil {
		return nil, err
	}
	forkPasswordCipher, err := s.pool.Encrypt(ctx, []byte(forkPassword))
	if err != nil {
		return nil, err
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
	forkRoot.DBPasswordCipher = forkPasswordCipher
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

	sourceCluster := clusterInfoFromTenant(source)
	sourceCluster.Password = forkPassword
	cluster, err := branchProvisioner.CreateBranch(ctx, forkID, sourceCluster)
	if cluster != nil {
		cluster.Provider = source.Provider
	}
	if err != nil {
		s.deleteForkBranchOrPersist(ctx, forkID, branchProvisioner, cluster)
		s.markForkFailed(ctx, forkID)
		return nil, err
	}
	if cluster == nil || cluster.ClusterID == "" || cluster.BranchID == "" {
		s.markForkFailed(ctx, forkID)
		return nil, forkErr(http.StatusBadGateway, "starter branch response missing required metadata")
	}
	if err := s.meta.UpdateTenantConnection(ctx, forkID, &meta.Tenant{
		DBHost:           cluster.Host,
		DBPort:           cluster.Port,
		DBUser:           cluster.Username,
		DBPasswordCipher: forkRoot.DBPasswordCipher,
		DBName:           cluster.DBName,
		DBTLS:            true,
		Provider:         cluster.Provider,
		ClusterID:        cluster.ClusterID,
		BranchID:         cluster.BranchID,
		ClaimURL:         cluster.ClaimURL,
		ClaimExpiresAt:   cluster.ClaimExpiresAt,
	}); err != nil {
		s.deleteForkBranchOrPersist(ctx, forkID, branchProvisioner, cluster)
		s.markForkFailed(ctx, forkID)
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

	s.startForkProvision(ctx, forkID)

	return &forkResponse{
		TenantID:       forkID,
		Name:           displayName,
		APIKey:         apiToken,
		Status:         string(meta.TenantProvisioning),
		Message:        forkProvisioningMessage(&forkRoot),
		ParentTenantID: source.ID,
		Storage:        "shared",
	}, nil
}

func (s *Server) provisionForkTenantAsync(ctx context.Context, forkID string) {
	ctx = ensureTrace(ctx)
	logger.Info(ctx, "fork_provision_started", zap.String("tenant_id", forkID))
	deadline := time.Now().Add(forkProvisionRetryWindow)
	backoff := forkProvisionInitialBackoff
	attempt := 1
	for {
		select {
		case <-ctx.Done():
			logger.Info(ctx, "fork_provision_stopped", zap.String("tenant_id", forkID), zap.Error(ctx.Err()))
			return
		default:
		}
		if err := s.provisionForkTenantOnce(ctx, forkID); err == nil {
			logger.Info(ctx, "fork_provision_ok", zap.String("tenant_id", forkID), zap.Int("attempt", attempt))
			return
		} else {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				logger.Info(ctx, "fork_provision_stopped", zap.String("tenant_id", forkID), zap.Int("attempt", attempt), zap.Error(err))
				return
			}
			var fatal *forkFatalProvisionError
			if errors.As(err, &fatal) || time.Now().After(deadline) {
				logger.Error(ctx, "fork_provision_failed",
					zap.String("tenant_id", forkID),
					zap.Int("attempt", attempt),
					zap.Error(err))
				s.markForkFailedAndCleanup(ctx, forkID)
				return
			}
			logger.Warn(ctx, "fork_provision_retry",
				zap.String("tenant_id", forkID),
				zap.Int("attempt", attempt),
				zap.Duration("backoff", backoff),
				zap.Error(err))
		}
		sleepFor := backoff
		if sleepFor > forkProvisionMaxBackoff {
			sleepFor = forkProvisionMaxBackoff
		}
		if time.Now().Add(sleepFor).After(deadline) {
			sleepFor = time.Until(deadline)
		}
		if sleepFor > 0 {
			timer := time.NewTimer(sleepFor)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				logger.Info(ctx, "fork_provision_stopped", zap.String("tenant_id", forkID), zap.Error(ctx.Err()))
				return
			case <-timer.C:
			}
		}
		backoff *= 2
		attempt++
	}
}

func (s *Server) provisionForkTenantOnce(ctx context.Context, forkID string) error {
	forkTenant, err := s.meta.GetTenant(ctx, forkID)
	if err != nil {
		return err
	}
	if forkTenant.Kind != meta.TenantKindFork {
		return &forkFatalProvisionError{err: forkErr(http.StatusConflict, "tenant is not a fork")}
	}
	switch forkTenant.Status {
	case meta.TenantActive, meta.TenantDeleting, meta.TenantDeleted:
		return nil
	case meta.TenantProvisioning:
	default:
		return &forkFatalProvisionError{err: forkErr(http.StatusConflict, "fork tenant is not provisioning")}
	}

	source, err := s.meta.GetTenant(ctx, forkTenant.ParentTenantID)
	if err != nil {
		return err
	}
	if source.Status != meta.TenantActive {
		return forkErr(http.StatusConflict, "source tenant is not active")
	}
	if source.Provider != tenant.ProviderTiDBCloudStarter {
		return &forkFatalProvisionError{err: forkErr(http.StatusConflict, "fork requires TiDB Cloud Starter provider")}
	}
	branchProvisioner, ok := s.provisioner.(tenant.AsyncBranchProvisioner)
	if !ok {
		return &forkFatalProvisionError{err: forkErr(http.StatusConflict, "fork requires branch-capable provisioner")}
	}
	if hasReservations, err := s.sourceHasActiveUploadReservations(ctx, source.ID); err != nil {
		return err
	} else if hasReservations {
		return forkErr(http.StatusConflict, "source tenant has active upload reservations")
	}

	dsn, err := s.ensureForkBranchConnection(ctx, forkTenant, source, branchProvisioner)
	if err != nil {
		return err
	}
	if err := s.provisioner.InitSchema(ctx, dsn); err != nil {
		return err
	}
	store, err := datastore.Open(dsn)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	if active, err := store.HasActiveUploads(ctx); err != nil {
		return err
	} else if active {
		return &forkFatalProvisionError{err: forkErr(http.StatusConflict, "source snapshot contains active uploads")}
	}
	if err := store.SanitizeForkRuntimeState(ctx); err != nil {
		return err
	}
	if err := s.meta.CopyQuotaConfig(ctx, source.ID, forkID); err != nil {
		return err
	}
	if err := s.backfillForkQuota(ctx, forkID, store); err != nil {
		return err
	}
	if err := s.meta.UpdateTenantSchemaVersion(ctx, forkID, schema.CurrentTiDBTenantSchemaVersion); err != nil {
		return err
	}
	return s.meta.UpdateTenantStatus(ctx, forkID, meta.TenantActive)
}

func (s *Server) ensureForkBranchConnection(ctx context.Context, forkTenant, source *meta.Tenant, branchProvisioner tenant.AsyncBranchProvisioner) (string, error) {
	if forkTenant.BranchID != "" {
		plain, err := s.pool.Decrypt(ctx, forkTenant.DBPasswordCipher)
		if err != nil {
			return "", err
		}
		if forkTenant.DBHost != "" && forkTenant.DBPort != 0 && forkTenant.DBUser != "" {
			return tenantDSN(forkTenant.DBUser, string(plain), forkTenant.DBHost, forkTenant.DBPort, forkTenant.DBName, forkTenant.DBTLS), nil
		}
		cluster, err := branchProvisioner.WaitForBranchActive(ctx, &tenant.ClusterInfo{
			TenantID:  forkTenant.ID,
			ClusterID: forkTenant.ClusterID,
			BranchID:  forkTenant.BranchID,
			Password:  string(plain),
			DBName:    forkTenant.DBName,
			Provider:  forkTenant.Provider,
		})
		if err != nil {
			return "", err
		}
		if err := s.meta.UpdateTenantConnection(ctx, forkTenant.ID, &meta.Tenant{
			DBHost:           cluster.Host,
			DBPort:           cluster.Port,
			DBUser:           cluster.Username,
			DBPasswordCipher: forkTenant.DBPasswordCipher,
			DBName:           cluster.DBName,
			DBTLS:            true,
			Provider:         cluster.Provider,
			ClusterID:        cluster.ClusterID,
			BranchID:         cluster.BranchID,
			ClaimURL:         cluster.ClaimURL,
			ClaimExpiresAt:   cluster.ClaimExpiresAt,
		}); err != nil {
			return "", err
		}
		return tenantDSN(cluster.Username, string(plain), cluster.Host, cluster.Port, cluster.DBName, true), nil
	}

	branchPassword, err := s.pool.Decrypt(ctx, forkTenant.DBPasswordCipher)
	if err != nil {
		return "", err
	}
	sourceCluster := clusterInfoFromTenant(source)
	sourceCluster.Password = string(branchPassword)
	cluster, err := branchProvisioner.CreateBranch(ctx, forkTenant.ID, sourceCluster)
	if cluster != nil {
		cluster.Provider = source.Provider
	}
	if err != nil {
		s.deleteForkBranchOrPersist(ctx, forkTenant.ID, branchProvisioner, cluster)
		return "", err
	}
	if cluster == nil || cluster.ClusterID == "" || cluster.BranchID == "" {
		return "", forkErr(http.StatusBadGateway, "starter branch response missing required metadata")
	}
	if err := s.meta.UpdateTenantConnection(ctx, forkTenant.ID, &meta.Tenant{
		DBHost:           cluster.Host,
		DBPort:           cluster.Port,
		DBUser:           cluster.Username,
		DBPasswordCipher: forkTenant.DBPasswordCipher,
		DBName:           cluster.DBName,
		DBTLS:            true,
		Provider:         cluster.Provider,
		ClusterID:        cluster.ClusterID,
		BranchID:         cluster.BranchID,
		ClaimURL:         cluster.ClaimURL,
		ClaimExpiresAt:   cluster.ClaimExpiresAt,
	}); err != nil {
		s.deleteForkBranchOrPersist(ctx, forkTenant.ID, branchProvisioner, cluster)
		return "", err
	}
	if cluster.Host == "" || cluster.Port == 0 || cluster.Username == "" {
		return "", forkErr(http.StatusServiceUnavailable, "database branch is not active yet")
	}
	return tenantDSN(cluster.Username, string(branchPassword), cluster.Host, cluster.Port, cluster.DBName, true), nil
}

func generateForkDBPassword(length int) (string, error) {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	max := big.NewInt(int64(len(chars)))
	for i := range b {
		n, err := rand.Int(rand.Reader, max)
		if err != nil {
			return "", err
		}
		b[i] = chars[n.Int64()]
	}
	return string(b), nil
}

func (s *Server) deleteForkBranchOrPersist(ctx context.Context, forkID string, branchProvisioner tenant.BranchProvisioner, cluster *tenant.ClusterInfo) {
	if cluster == nil || cluster.ClusterID == "" || cluster.BranchID == "" {
		return
	}
	if err := branchProvisioner.DeleteBranch(ctx, cluster.ClusterID, cluster.BranchID); err == nil {
		return
	}
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
	}
}

func (s *Server) markForkFailed(ctx context.Context, forkID string) {
	if err := s.meta.UpdateTenantStatus(ctx, forkID, meta.TenantFailed); err != nil {
		logger.Error(ctx, "fork_mark_failed_failed", zap.String("tenant_id", forkID), zap.Error(err))
	}
}

func (s *Server) markForkFailedAndCleanup(ctx context.Context, forkID string) {
	s.markForkFailed(ctx, forkID)
	s.startForkCleanup(ctx, forkID)
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

func (s *Server) handleForkDelete(w http.ResponseWriter, r *http.Request) {
	if s.meta == nil || s.pool == nil {
		errJSON(w, http.StatusNotFound, "fork delete not enabled")
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
		errJSON(w, http.StatusConflict, "only fork tenants can be deleted through fork delete")
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
	s.startForkCleanup(r.Context(), t.ID)
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": string(meta.TenantDeleting)})
}

func (s *Server) cleanupForkTenant(ctx context.Context, tenantID string) {
	ctx = ensureTrace(ctx)
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
	for _, status := range []meta.TenantStatus{meta.TenantDeleting, meta.TenantFailed} {
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
