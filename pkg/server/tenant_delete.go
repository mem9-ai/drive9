package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"go.uber.org/zap"
)

const (
	defaultTenantDeletePollInterval = time.Minute
	defaultTenantDeleteRetryDelay   = time.Hour
	defaultTenantDeleteRunningTTL   = 30 * time.Minute
	defaultTenantDeleteJobTimeout   = 25 * time.Minute
)

func (s *Server) handleTenantDelete(w http.ResponseWriter, r *http.Request) {
	if s.meta == nil || s.pool == nil || s.provisioner == nil || len(s.tokenSecret) == 0 {
		errJSON(w, http.StatusNotFound, "tenant delete not enabled")
		return
	}
	if r.Method != http.MethodDelete {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	scope, ok := ownerScopeFromRequest(w, r, "delete tenant")
	if !ok {
		return
	}
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
	if t.Kind != meta.TenantKindLive {
		errJSON(w, http.StatusConflict, "only live tenants can be deleted")
		return
	}
	if t.Status == meta.TenantDeleted {
		errJSON(w, http.StatusNotFound, "tenant not found")
		return
	}
	if t.Provider == tenant.ProviderTiDBZero {
		errJSON(w, http.StatusConflict, "tidb_zero tenants expire automatically and do not support delete")
		return
	}
	if t.Provider != tenant.ProviderTiDBCloudStarter && t.Provider != tenant.ProviderTiDBCloudNative {
		errJSON(w, http.StatusConflict, "tenant delete is not supported for provider")
		return
	}
	if t.StorageNamespaceID != "" {
		hasFork, err := s.meta.NamespaceHasNonDeletedFork(r.Context(), t.StorageNamespaceID)
		if err != nil {
			errJSON(w, http.StatusInternalServerError, "failed to check tenant forks")
			return
		}
		if hasFork {
			errJSON(w, http.StatusConflict, "tenant has non-deleted forks")
			return
		}
	}

	var req tenant.CredentialProvisionRequest
	if t.Status == meta.TenantDeleting {
		hasJob, err := s.meta.TenantDeleteJobExists(r.Context(), t.ID)
		if err != nil {
			errJSON(w, http.StatusInternalServerError, "failed to check tenant delete cleanup")
			return
		}
		if hasJob {
			_ = s.meta.RevokeTenantAPIKeys(r.Context(), t.ID)
			writeTenantDeleteStatus(w, meta.TenantDeleting)
			return
		}
	}

	if t.Provider == tenant.ProviderTiDBCloudNative {
		req, err = decodeCredentialDeprovisionRequest(w, r)
		if err != nil {
			if !errors.Is(err, tenant.ErrCredentialsRequired) {
				errJSON(w, http.StatusBadRequest, err.Error())
				return
			}
			defaultReq := resolveDefaultCredentials(s.provisioner)
			if defaultReq == nil {
				errJSON(w, http.StatusBadRequest, tenant.ErrCredentialsRequired.Error())
				return
			}
			req = *defaultReq
		}
	}

	if t.Status != meta.TenantDeleting {
		updated, err := s.meta.UpdateTenantStatusIf(r.Context(), t.ID, t.Status, meta.TenantDeleting)
		if err != nil {
			errJSON(w, http.StatusInternalServerError, "failed to mark tenant deleting")
			return
		}
		if !updated {
			writeTenantDeleteStatus(w, meta.TenantDeleting)
			return
		}
	}

	if err := s.pool.InvalidateAndWait(r.Context(), t.ID); err != nil {
		if t.Status != meta.TenantDeleting {
			_, _ = s.meta.UpdateTenantStatusIf(r.Context(), t.ID, meta.TenantDeleting, t.Status)
		}
		errJSON(w, http.StatusInternalServerError, "failed to drain tenant backend")
		return
	}
	if err := s.deprovisionTenantCluster(r.Context(), t, req); err != nil {
		if t.Status != meta.TenantDeleting {
			_, _ = s.meta.UpdateTenantStatusIf(r.Context(), t.ID, meta.TenantDeleting, t.Status)
		}
		errJSON(w, http.StatusBadGateway, fmt.Sprintf("delete tenant cluster failed: %v", err))
		return
	}

	_ = s.meta.AbortActiveUploadReservations(r.Context(), t.ID)

	status, err := s.enqueueTenantDeleteJob(r.Context(), t)
	if err != nil {
		// Cluster deprovision already succeeded. Keep owner keys active until
		// cleanup is durably enqueued so the same owner can retry the delete
		// request. This matters for tidb_cloud_native because customer TiDB Cloud
		// credentials are accepted per request and are not stored server-side.
		errJSON(w, http.StatusInternalServerError, "failed to enqueue tenant delete cleanup")
		return
	}
	_ = s.meta.RevokeTenantAPIKeys(r.Context(), t.ID)
	writeTenantDeleteStatus(w, status)
}

func (s *Server) enqueueTenantDeleteJob(ctx context.Context, t *meta.Tenant) (meta.TenantStatus, error) {
	if t.StorageNamespaceID == "" {
		if err := s.meta.UpdateTenantStatus(ctx, t.ID, meta.TenantDeleted); err != nil {
			return "", err
		}
		return meta.TenantDeleted, nil
	}
	ns, err := s.meta.GetStorageNamespace(ctx, t.StorageNamespaceID)
	if err != nil {
		return "", err
	}
	if err := s.meta.UpdateStorageNamespaceState(ctx, ns.ID, meta.StorageNamespaceDeleting); err != nil {
		return "", err
	}
	if err := s.meta.EnqueueTenantDeleteJob(ctx, &meta.TenantDeleteJob{
		TenantID:    t.ID,
		NamespaceID: ns.ID,
		Backend:     ns.Backend,
		Bucket:      ns.Bucket,
		Prefix:      ns.Prefix,
	}); err != nil {
		return "", err
	}
	return meta.TenantDeleting, nil
}

func writeTenantDeleteStatus(w http.ResponseWriter, status meta.TenantStatus) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": string(status)})
}

func (s *Server) deprovisionTenantCluster(ctx context.Context, t *meta.Tenant, req tenant.CredentialProvisionRequest) error {
	cluster := clusterInfoFromTenant(t)
	switch t.Provider {
	case tenant.ProviderTiDBCloudNative:
		deprovisioner, ok := s.provisioner.(tenant.CredentialDeprovisioner)
		if !ok {
			return fmt.Errorf("provisioner does not support credential deprovision")
		}
		return deprovisioner.DeprovisionWithCredentials(ctx, cluster, req)
	case tenant.ProviderTiDBCloudStarter:
		deprovisioner, ok := s.provisioner.(tenant.Deprovisioner)
		if !ok {
			return fmt.Errorf("provisioner does not support deprovision")
		}
		return deprovisioner.Deprovision(ctx, cluster)
	default:
		return fmt.Errorf("delete is not supported for provider %s", t.Provider)
	}
}

func decodeCredentialDeprovisionRequest(w http.ResponseWriter, r *http.Request) (tenant.CredentialProvisionRequest, error) {
	var raw struct {
		PublicKey  string `json:"public_key"`
		PrivateKey string `json:"private_key"`
	}
	req, err := decodeCredentialRequest(w, r, &raw, func() tenant.CredentialProvisionRequest {
		return tenant.CredentialProvisionRequest{
			PublicKey:  strings.TrimSpace(raw.PublicKey),
			PrivateKey: strings.TrimSpace(raw.PrivateKey),
		}
	})
	if err != nil {
		return tenant.CredentialProvisionRequest{}, err
	}
	return *req, nil
}

func (s *Server) processTenantDeleteJobs(ctx context.Context) {
	now := time.Now().UTC()
	if _, err := s.meta.RecoverStaleTenantDeleteJobs(ctx, now.Add(-defaultTenantDeleteRunningTTL)); err != nil {
		logger.Error(ctx, "tenant_delete_recover_stale_failed", zap.Error(err))
		return
	}
	jobs, err := s.meta.ListDueTenantDeleteJobs(ctx, now, 25)
	if err != nil {
		logger.Error(ctx, "tenant_delete_list_due_failed", zap.Error(err))
		return
	}
	for _, job := range jobs {
		if err := s.processTenantDeleteJob(ctx, job); err != nil {
			_ = s.meta.RetryTenantDeleteJob(ctx, job.TenantID, time.Now().UTC().Add(defaultTenantDeleteRetryDelay), err.Error())
		}
	}
}

func (s *Server) processTenantDeleteJob(ctx context.Context, job meta.TenantDeleteJob) error {
	updated, err := s.meta.MarkTenantDeleteJobRunning(ctx, job.TenantID)
	if err != nil {
		return err
	}
	if !updated {
		return nil
	}
	ns := &meta.StorageNamespace{
		ID:            job.NamespaceID,
		OwnerTenantID: job.TenantID,
		Backend:       job.Backend,
		Bucket:        job.Bucket,
		Prefix:        job.Prefix,
		State:         meta.StorageNamespaceDeleting,
	}
	s3c, err := s.pool.S3ForStorageNamespace(ctx, ns)
	if err != nil {
		return err
	}
	jobCtx, cancel := context.WithTimeout(ctx, defaultTenantDeleteJobTimeout)
	defer cancel()
	// The S3 client is already scoped to job.Prefix, so an empty relative
	// prefix deletes the whole storage namespace without opening tenant DB.
	res, err := s3c.DeletePrefix(jobCtx, "")
	if err != nil {
		return err
	}
	return s.meta.FinalizeTenantDelete(ctx, job.TenantID, job.NamespaceID, res.DeletedObjects, res.AbortedMultipartUploads)
}
