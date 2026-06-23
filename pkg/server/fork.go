package server

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"github.com/mem9-ai/drive9/pkg/tenant/schema"
	"github.com/mem9-ai/drive9/pkg/tenant/token"
	"go.uber.org/zap"
)

type forkRequest struct {
	Name       string `json:"name"`
	PublicKey  string `json:"public_key"`
	PrivateKey string `json:"private_key"`
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

func (s *Server) startForkProvisionWithCredentials(ctx context.Context, forkID string, credentialReq *tenant.CredentialProvisionRequest) {
	s.startForkWorker(ctx, func(workerCtx context.Context) {
		s.provisionForkTenantAsyncWithCredentials(workerCtx, forkID, credentialReq)
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
	req, err := decodeForkRequest(w, r)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.Name) == "" {
		req.Name = "fork"
	}

	credentialReq, err := forkCredentialRequest(req)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}

	resp, err := s.createForkTenant(r.Context(), scope.TenantID, req.Name, credentialReq)
	if err != nil {
		var provisionErr *forkProvisionFailedError
		if errors.As(err, &provisionErr) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"tenant_id":        provisionErr.TenantID,
				"name":             provisionErr.Name,
				"api_key":          provisionErr.APIKey,
				"status":           string(meta.TenantFailed),
				"message":          "Fork provisioning failed. Use this API key to access and delete the fork, then retry creation.",
				"parent_tenant_id": scope.TenantID,
				"storage":          "shared",
			})
			return
		}
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

func decodeForkRequest(w http.ResponseWriter, r *http.Request) (forkRequest, error) {
	var req forkRequest
	if r.Body == nil {
		return req, nil
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return req, fmt.Errorf("invalid JSON body: %w", err)
	}
	var extra struct{}
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return req, fmt.Errorf("invalid JSON body: trailing data")
	}
	return req, nil
}

func forkProvisioningMessage(t *meta.Tenant) string {
	if t == nil || t.BranchID == "" {
		return ""
	}
	return "Migrating fork data. Large tenants may take a few minutes."
}

type forkFatalProvisionError struct {
	err error
}

func (e *forkFatalProvisionError) Error() string { return e.err.Error() }

func (e *forkFatalProvisionError) Unwrap() error { return e.err }

func forkCredentialRequest(req forkRequest) (*tenant.CredentialProvisionRequest, error) {
	out := tenant.CredentialProvisionRequest{
		PublicKey:  strings.TrimSpace(req.PublicKey),
		PrivateKey: strings.TrimSpace(req.PrivateKey),
	}
	if out.PublicKey == "" && out.PrivateKey == "" {
		return nil, nil
	}
	if out.PublicKey == "" || out.PrivateKey == "" {
		return nil, tenant.ErrPartialCredentials
	}
	return &out, nil
}

func forkResponseStatus(provider string) string {
	return string(meta.TenantProvisioning)
}

func forkResponseMessage(provider string, t *meta.Tenant) string {
	return forkProvisioningMessage(t)
}

func (s *Server) resolveForkCredentialRequest(provider string, req *tenant.CredentialProvisionRequest) (*tenant.CredentialProvisionRequest, error) {
	if provider != tenant.ProviderTiDBCloudNative {
		return nil, nil
	}
	if req == nil {
		req = resolveDefaultCredentials(s.provisioner)
		if req == nil {
			return nil, tenant.ErrCredentialsRequired
		}
	}
	if validator, ok := s.provisioner.(credentialProvisionRequestValidator); ok {
		if err := validator.ValidateCredentialProvisionRequest(*req); err != nil {
			return nil, err
		}
	}
	return req, nil
}

func (s *Server) forkBranchCreateSupported(req *tenant.CredentialProvisionRequest) bool {
	if req != nil {
		_, ok := s.provisioner.(tenant.CredentialBranchProvisioner)
		return ok
	}
	_, ok := s.provisioner.(tenant.AsyncBranchProvisioner)
	return ok
}

func (s *Server) createForkBranch(ctx context.Context, forkID string, source *tenant.ClusterInfo, req *tenant.CredentialProvisionRequest) (*tenant.ClusterInfo, error) {
	if req != nil {
		branchProvisioner, ok := s.provisioner.(tenant.CredentialBranchProvisioner)
		if !ok {
			return nil, fmt.Errorf("fork requires credential branch-capable provisioner")
		}
		return branchProvisioner.CreateBranchWithCredentials(ctx, forkID, source, *req)
	}
	branchProvisioner, ok := s.provisioner.(tenant.AsyncBranchProvisioner)
	if !ok {
		return nil, fmt.Errorf("fork requires branch-capable provisioner")
	}
	return branchProvisioner.CreateBranch(ctx, forkID, source)
}

func (s *Server) waitForForkBranchActive(ctx context.Context, branch *tenant.ClusterInfo, req *tenant.CredentialProvisionRequest) (*tenant.ClusterInfo, error) {
	if req != nil {
		branchProvisioner, ok := s.provisioner.(tenant.CredentialBranchProvisioner)
		if !ok {
			return nil, fmt.Errorf("fork requires credential branch-capable provisioner")
		}
		return branchProvisioner.WaitForBranchActiveWithCredentials(ctx, branch, *req)
	}
	branchProvisioner, ok := s.provisioner.(tenant.AsyncBranchProvisioner)
	if !ok {
		return nil, fmt.Errorf("fork requires branch-capable provisioner")
	}
	return branchProvisioner.WaitForBranchActive(ctx, branch)
}

func (s *Server) deleteForkBranch(ctx context.Context, clusterID, branchID string, req *tenant.CredentialProvisionRequest) error {
	if req != nil {
		branchProvisioner, ok := s.provisioner.(tenant.CredentialBranchProvisioner)
		if !ok {
			return fmt.Errorf("fork requires credential branch-capable provisioner")
		}
		return branchProvisioner.DeleteBranchWithCredentials(ctx, clusterID, branchID, *req)
	}
	branchProvisioner, ok := s.provisioner.(tenant.BranchProvisioner)
	if !ok {
		return fmt.Errorf("fork requires branch-capable provisioner")
	}
	return branchProvisioner.DeleteBranch(ctx, clusterID, branchID)
}

func (s *Server) createForkTenant(ctx context.Context, sourceTenantID, displayName string, credentialReq *tenant.CredentialProvisionRequest) (*forkResponse, error) {
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
	if !forkProviderSupported(source.Provider) {
		return nil, forkErr(http.StatusConflict, "fork is not supported in this TiDBCloud mode")
	}
	credentialReq, err = s.resolveForkCredentialRequest(source.Provider, credentialReq)
	if err != nil {
		return nil, forkErr(http.StatusBadRequest, err.Error())
	}
	if source.StorageNamespaceID == "" {
		return nil, forkErr(http.StatusConflict, "source tenant storage namespace is not initialized")
	}
	if !s.forkBranchCreateSupported(credentialReq) {
		return nil, forkErr(http.StatusConflict, "fork requires branch-capable provisioner")
	}
	if hasReservations, err := s.sourceHasActiveUploadReservations(ctx, source.ID); err != nil {
		logger.Error(ctx, "fork_check_upload_reservations_failed",
			zap.String("source_tenant_id", source.ID),
			zap.Error(err))
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
	if err := s.copyAutoEmbeddingProfileForFork(ctx, source.ID, forkID, source.Provider, now); err != nil {
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

	makeProvisionFailedErr := func(err error) error {
		return &forkProvisionFailedError{
			APIKey:   apiToken,
			TenantID: forkID,
			Name:     displayName,
			Err:      err,
		}
	}

	sourceCluster := clusterInfoFromTenant(source)
	sourceCluster.Password = forkPassword
	cluster, err := s.createForkBranch(ctx, forkID, sourceCluster, credentialReq)
	if cluster != nil {
		cluster.Provider = source.Provider
	}
	if err != nil {
		logger.Error(ctx, "fork_create_branch_failed",
			zap.String("source_tenant_id", source.ID),
			zap.String("fork_id", forkID),
			zap.String("cluster_id", sourceCluster.ClusterID),
			zap.Error(err))
		s.deleteForkBranchOrPersist(backgroundWithTrace(ctx), forkID, credentialReq, cluster)
		s.markForkFailedAndCleanup(ctx, forkID)
		return nil, makeProvisionFailedErr(err)
	}
	if cluster == nil || cluster.ClusterID == "" || cluster.BranchID == "" {
		cid := ""
		bid := ""
		if cluster != nil {
			cid = cluster.ClusterID
			bid = cluster.BranchID
		}
		logger.Error(ctx, "fork_branch_response_missing_metadata",
			zap.String("source_tenant_id", source.ID),
			zap.String("fork_id", forkID),
			zap.String("response_cluster_id", cid),
			zap.String("response_branch_id", bid))
		s.markForkFailedAndCleanup(ctx, forkID)
		return nil, makeProvisionFailedErr(forkErr(http.StatusBadGateway, "branch response missing required metadata"))
	}
	forkRoot.ClusterID = cluster.ClusterID
	forkRoot.BranchID = cluster.BranchID
	if err := s.meta.UpdateTenantConnection(ctx, forkID, &meta.Tenant{
		DBHost:           source.DBHost,
		DBPort:           source.DBPort,
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
		logger.Error(ctx, "fork_update_tenant_connection_failed",
			zap.String("source_tenant_id", source.ID),
			zap.String("fork_id", forkID),
			zap.Error(err))
		s.deleteForkBranchOrPersist(backgroundWithTrace(ctx), forkID, credentialReq, cluster)
		s.markForkFailedAndCleanup(ctx, forkID)
		return nil, makeProvisionFailedErr(err)
	}

	// When the branch endpoint is not yet available (branch still CREATING)
	// and we have per-request credentials, defer to async provisioning instead
	// of failing. The async provisioner will wait for the branch to become active.
	if source.Provider == tenant.ProviderTiDBCloudNative && cluster.Username == "" && credentialReq == nil && resolveDefaultCredentials(s.provisioner) == nil {
		logger.Error(ctx, "fork_missing_endpoint_no_credentials",
			zap.String("source_tenant_id", source.ID),
			zap.String("fork_id", forkID))
		if s.deleteForkBranchOrPersist(backgroundWithTrace(ctx), forkID, credentialReq, cluster) {
			if err := s.meta.UpdateTenantStatus(ctx, forkID, meta.TenantDeleted); err != nil {
				logger.Error(ctx, "fork_credential_gate_mark_deleted_failed",
					zap.String("tenant_id", forkID), zap.Error(err))
			}
		} else {
			s.markForkFailedAndCleanup(ctx, forkID)
		}
		return nil, makeProvisionFailedErr(forkErr(http.StatusBadRequest, "branch response missing endpoint; server default TiDB Cloud credential is required to provision this fork"))
	}

	if source.Provider == tenant.ProviderTiDBCloudNative {
		s.startForkProvisionWithCredentials(ctx, forkID, credentialReq)
	} else {
		s.startForkProvision(ctx, forkID)
	}

	return &forkResponse{
		TenantID:       forkID,
		Name:           displayName,
		APIKey:         apiToken,
		Status:         forkResponseStatus(source.Provider),
		Message:        forkResponseMessage(source.Provider, &forkRoot),
		ParentTenantID: source.ID,
		Storage:        "shared",
	}, nil
}

type forkProvisionFailedError struct {
	APIKey   string
	TenantID string
	Name     string
	Err      error
}

func (e *forkProvisionFailedError) Error() string {
	return e.Err.Error()
}

func (e *forkProvisionFailedError) Unwrap() error {
	return e.Err
}

func (s *Server) provisionForkTenantAsync(ctx context.Context, forkID string) {
	s.provisionForkTenantAsyncWithProvision(ctx, forkID, func() error {
		return s.provisionForkTenantOnce(ctx, forkID)
	})
}

func (s *Server) provisionForkTenantAsyncWithCredentials(ctx context.Context, forkID string, credentialReq *tenant.CredentialProvisionRequest) {
	s.provisionForkTenantAsyncWithProvision(ctx, forkID, func() error {
		return s.provisionForkTenantOnceWithCredentials(ctx, forkID, credentialReq)
	})
}

func (s *Server) provisionForkTenantAsyncWithProvision(ctx context.Context, forkID string, provision func() error) {
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
		if err := provision(); err == nil {
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
	return s.provisionForkTenantOnceWithCredentials(ctx, forkID, nil)
}

func (s *Server) provisionForkTenantOnceWithCredentials(ctx context.Context, forkID string, credentialReq *tenant.CredentialProvisionRequest) error {
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
	if !forkProviderSupported(source.Provider) {
		return &forkFatalProvisionError{err: forkErr(http.StatusConflict, "fork is not supported in this TiDBCloud mode")}
	}
	if !s.forkBranchCreateSupported(credentialReq) {
		return &forkFatalProvisionError{err: forkErr(http.StatusConflict, "fork requires branch-capable provisioner")}
	}
	if hasReservations, err := s.sourceHasActiveUploadReservations(ctx, source.ID); err != nil {
		return err
	} else if hasReservations {
		return forkErr(http.StatusConflict, "source tenant has active upload reservations")
	}

	dsn, err := s.ensureForkBranchConnection(ctx, forkTenant, source, credentialReq)
	if err != nil {
		return err
	}
	if err := s.schemaInitForTenant(forkID, forkTenant.Provider, s.provisioner.InitSchema)(ctx, dsn); err != nil {
		return err
	}
	if err := s.finalizeTenantSchemaInit(ctx, forkID, dsn, forkTenant.Provider); err != nil {
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
	profile, err := s.autoEmbeddingProfileForTenant(ctx, forkID)
	if err != nil {
		return err
	}
	version, err := schema.TiDBTenantSchemaVersionForAutoEmbeddingProfile(profile)
	if err != nil {
		return err
	}
	if err := s.meta.UpdateTenantSchemaVersion(ctx, forkID, version); err != nil {
		return err
	}
	return s.meta.UpdateTenantStatus(ctx, forkID, meta.TenantActive)
}

func (s *Server) ensureForkBranchConnection(ctx context.Context, forkTenant, source *meta.Tenant, credentialReq *tenant.CredentialProvisionRequest) (string, error) {
	if forkTenant.BranchID != "" {
		plain, err := s.pool.Decrypt(ctx, forkTenant.DBPasswordCipher)
		if err != nil {
			return "", err
		}
		if forkTenant.DBHost != "" && forkTenant.DBPort != 0 && forkTenant.DBUser != "" {
			return tenantDSN(forkTenant.DBUser, string(plain), forkTenant.DBHost, forkTenant.DBPort, forkTenant.DBName, forkTenant.DBTLS), nil
		}
		if branchProv, ok := s.provisioner.(tenant.CredentialBranchProvisioner); ok {
			username, err := branchProv.WaitForBranchUserWithCredentials(ctx, forkTenant.ClusterID, forkTenant.BranchID, *credentialReq)
			if err != nil {
				return "", err
			}
			host := forkTenant.DBHost
			port := forkTenant.DBPort
			if host == "" {
				host = source.DBHost
			}
			if port == 0 {
				port = source.DBPort
			}
			if err := s.meta.UpdateTenantConnection(ctx, forkTenant.ID, &meta.Tenant{
				DBHost:           host,
				DBPort:           port,
				DBUser:           username,
				DBPasswordCipher: forkTenant.DBPasswordCipher,
				DBName:           forkTenant.DBName,
				DBTLS:            true,
				Provider:         forkTenant.Provider,
				ClusterID:        forkTenant.ClusterID,
				BranchID:         forkTenant.BranchID,
			}); err != nil {
				return "", err
			}
			return tenantDSN(username, string(plain), host, port, forkTenant.DBName, true), nil
		}
		cluster, err := s.waitForForkBranchActive(ctx, &tenant.ClusterInfo{
			TenantID:  forkTenant.ID,
			ClusterID: forkTenant.ClusterID,
			BranchID:  forkTenant.BranchID,
			Password:  string(plain),
			DBName:    forkTenant.DBName,
			Provider:  forkTenant.Provider,
		}, credentialReq)
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
	cluster, err := s.createForkBranch(ctx, forkTenant.ID, sourceCluster, credentialReq)
	if cluster != nil {
		cluster.Provider = source.Provider
	}
	if err != nil {
		s.deleteForkBranchOrPersist(ctx, forkTenant.ID, credentialReq, cluster)
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
		s.deleteForkBranchOrPersist(ctx, forkTenant.ID, credentialReq, cluster)
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

// deleteForkBranchOrPersist returns true when no branch exists or branch
// deletion succeeds. On delete failure it persists branch metadata for a later
// credentialed retry and returns false.
func (s *Server) deleteForkBranchOrPersist(ctx context.Context, forkID string, credentialReq *tenant.CredentialProvisionRequest, cluster *tenant.ClusterInfo) bool {
	if cluster == nil || cluster.ClusterID == "" || cluster.BranchID == "" {
		return true
	}
	if err := s.deleteForkBranch(ctx, cluster.ClusterID, cluster.BranchID, credentialReq); err == nil {
		return true
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
	return false
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

func forkProviderSupported(provider string) bool {
	return provider == tenant.ProviderTiDBCloudStarter || provider == tenant.ProviderTiDBCloudNative
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
	if s.meta == nil || s.pool == nil || s.provisioner == nil {
		errJSON(w, http.StatusNotFound, "fork delete not enabled")
		return
	}
	scope := ScopeFromContext(r.Context())
	if scope == nil || scope.TenantID == "" {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return
	}
	req, err := decodeForkRequest(w, r)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	credentialReq, err := forkCredentialRequest(req)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
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
		if t.Provider == tenant.ProviderTiDBCloudNative {
			s.cleanupNativeFork(w, r, t, credentialReq)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": string(meta.TenantDeleting)})
		return
	}
	if t.Provider == tenant.ProviderTiDBCloudNative && t.Status == meta.TenantFailed {
		s.cleanupNativeFork(w, r, t, credentialReq)
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
	if t.Provider == tenant.ProviderTiDBCloudNative {
		s.cleanupNativeFork(w, r, t, credentialReq)
		return
	}
	_ = s.meta.RevokeTenantAPIKeys(r.Context(), t.ID)
	s.startForkCleanup(r.Context(), t.ID)
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": string(meta.TenantDeleting)})
}

func (s *Server) cleanupNativeFork(w http.ResponseWriter, r *http.Request, t *meta.Tenant, credentialReq *tenant.CredentialProvisionRequest) {
	if t.BranchID != "" {
		var err error
		credentialReq, err = s.resolveForkCredentialRequest(t.Provider, credentialReq)
		if err != nil {
			errJSON(w, http.StatusBadRequest, "invalid or missing TiDB Cloud credentials")
			return
		}
	}
	if err := s.cleanupForkTenantOnce(r.Context(), t.ID, credentialReq); err != nil {
		logger.Error(r.Context(), "native_fork_cleanup_failed", zap.String("tenant_id", t.ID), zap.Error(err))
		errJSON(w, http.StatusBadGateway, "fork delete cleanup failed")
		return
	}
	_ = s.meta.RevokeTenantAPIKeys(r.Context(), t.ID)
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": string(meta.TenantDeleted)})
}

func (s *Server) cleanupForkTenant(ctx context.Context, tenantID string) {
	ctx = ensureTrace(ctx)
	if err := s.cleanupForkTenantOnce(ctx, tenantID, nil); err != nil {
		logger.Error(ctx, "fork_cleanup_failed", zap.String("tenant_id", tenantID), zap.Error(err))
	}
}

func (s *Server) cleanupForkTenantOnce(ctx context.Context, tenantID string, credentialReq *tenant.CredentialProvisionRequest) error {
	t, err := s.meta.GetTenant(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("get fork tenant: %w", err)
	}
	if t.Kind != meta.TenantKindFork {
		return nil
	}
	if err := s.pool.InvalidateAndWait(ctx, tenantID); err != nil {
		return fmt.Errorf("drain fork backend: %w", err)
	}
	_ = s.meta.AbortActiveUploadReservations(ctx, tenantID)
	if t.BranchID == "" {
		logger.Warn(ctx, "fork_cleanup_missing_branch_id", zap.String("tenant_id", tenantID), zap.String("status", string(t.Status)))
		if t.Status == meta.TenantFailed || t.Status == meta.TenantDeleting {
			if err := s.meta.UpdateTenantStatus(ctx, tenantID, meta.TenantDeleted); err != nil {
				return fmt.Errorf("mark fork deleted: %w", err)
			}
		}
		return nil
	}
	if t.Provider == tenant.ProviderTiDBCloudNative && credentialReq == nil && resolveDefaultCredentials(s.provisioner) == nil {
		return tenant.ErrCredentialsRequired
	}
	if t.Status != meta.TenantDeleting {
		return s.cleanupFailedForkBranch(ctx, t, credentialReq)
	}
	store, err := s.openTenantStore(ctx, t)
	if err != nil {
		return fmt.Errorf("open fork store: %w", err)
	}
	defer func() { _ = store.Close() }()
	// Object GC candidates are keyed by namespace/ref hash, so retrying a
	// native fork delete safely re-enqueues refs before branch deletion.
	if err := s.enqueueForkConfirmedRefs(ctx, t, store); err != nil {
		return fmt.Errorf("enqueue fork refs: %w", err)
	}
	if err := s.enqueueForkFileGCTaskRefs(ctx, t, store); err != nil {
		return fmt.Errorf("enqueue fork file gc refs: %w", err)
	}
	if err := store.SanitizeForkRuntimeState(ctx); err != nil {
		return fmt.Errorf("sanitize fork runtime state: %w", err)
	}
	if err := s.deleteForkBranch(ctx, t.ClusterID, t.BranchID, credentialReq); err != nil {
		return fmt.Errorf("delete fork branch: %w", err)
	}
	if err := s.meta.UpdateTenantStatus(ctx, tenantID, meta.TenantDeleted); err != nil {
		return fmt.Errorf("mark fork deleted: %w", err)
	}
	return nil
}

func (s *Server) cleanupFailedForkBranch(ctx context.Context, t *meta.Tenant, credentialReq *tenant.CredentialProvisionRequest) error {
	if err := s.deleteForkBranch(ctx, t.ClusterID, t.BranchID, credentialReq); err != nil {
		return fmt.Errorf("delete failed fork branch: %w", err)
	}
	if err := s.meta.UpdateTenantStatus(ctx, t.ID, meta.TenantDeleted); err != nil {
		return fmt.Errorf("mark failed fork deleted: %w", err)
	}
	return nil
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
