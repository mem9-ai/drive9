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
	if s.meta == nil || s.pool == nil || (s.provisioner == nil && s.legacyStarterProvisioner == nil) || len(s.tokenSecret) == 0 {
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
	// Shared-schema tenants follow a dedicated delete path: purge their rows
	// from the shared DB instead of deprovisioning a cluster (there is none).
	// The dispatch keys on the persisted provider, not the placement row —
	// the provider never changes mid-delete, so a retried delete always
	// routes back here even after the placement row has been removed.
	if tenant.IsSharedSchemaProvider(t.Provider) {
		s.handleSharedTenantDelete(w, r, t)
		return
	}
	if t.Provider == tenant.ProviderTiDBZero {
		errJSON(w, http.StatusConflict, "tidb_zero tenants expire automatically and do not support delete")
		return
	}
	if !tenant.SupportsClusterDelete(t.Provider) {
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
		status, msg := clientFacingErrorResponse(http.StatusBadGateway, "delete tenant cluster failed", err)
		errJSON(w, status, msg)
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

// handleSharedTenantDelete deletes a tenant placed on a shared-schema
// database: it purges the tenant's rows from the shared DB (never touching
// other tenants' rows and never deprovisioning the shared cluster), then runs
// the same S3-namespace cleanup and key revocation as the standalone path.
//
// The flow is safe to retry at any point: the dispatch keys on the persisted
// provider (durable), the purge is idempotent, and external cleanup retains a
// deleting placement as the physical-resource authorization anchor until the
// cleanup worker reaches the terminal state. Identity resolution is read-only
// (ResolveFsID): a delete path must never allocate a new fs_id.
func (s *Server) handleSharedTenantDelete(w http.ResponseWriter, r *http.Request, t *meta.Tenant) {
	ctx := r.Context()
	deleteJobExists := false
	if t.StorageNamespaceID != "" {
		hasFork, err := s.meta.NamespaceHasNonDeletedFork(ctx, t.StorageNamespaceID)
		if err != nil {
			errJSON(w, http.StatusInternalServerError, "failed to check tenant forks")
			return
		}
		if hasFork {
			errJSON(w, http.StatusConflict, "tenant has non-deleted forks")
			return
		}
	}
	if t.Status == meta.TenantDeleting {
		hasJob, err := s.meta.TenantDeleteJobExists(ctx, t.ID)
		if err != nil {
			errJSON(w, http.StatusInternalServerError, "failed to check tenant delete cleanup")
			return
		}
		deleteJobExists = hasJob
	}
	if t.Status != meta.TenantDeleting {
		updated, err := s.meta.UpdateTenantStatusIf(ctx, t.ID, t.Status, meta.TenantDeleting)
		if err != nil {
			errJSON(w, http.StatusInternalServerError, "failed to mark tenant deleting")
			return
		}
		if !updated {
			writeTenantDeleteStatus(w, meta.TenantDeleting)
			return
		}
	}
	if err := s.pool.InvalidateAndWait(ctx, t.ID); err != nil {
		_, _ = s.meta.UpdateTenantStatusIf(ctx, t.ID, meta.TenantDeleting, t.Status)
		errJSON(w, http.StatusInternalServerError, "failed to drain tenant backend")
		return
	}
	fsID, err := s.meta.ResolveFsID(ctx, t.ID)
	if err != nil {
		// A shared-schema tenant without an fs_registry row is a data
		// integrity problem; never allocate one on a delete path.
		logger.Error(ctx, "shared_tenant_delete_fsid_unresolvable", zap.String("tenant_id", t.ID), zap.Error(err))
		errJSON(w, http.StatusInternalServerError, "failed to resolve tenant fs_id")
		return
	}
	placement, err := s.meta.GetTenantPlacement(ctx, fsID)
	if err != nil && !errors.Is(err, meta.ErrNotFound) {
		errJSON(w, http.StatusInternalServerError, "failed to resolve tenant placement")
		return
	}
	if deleteJobExists && placement != nil && placement.Status == meta.PlacementStatusDeleting {
		_ = s.meta.RevokeTenantAPIKeys(ctx, t.ID)
		writeTenantDeleteStatus(w, meta.TenantDeleting)
		return
	}
	if placement != nil {
		if placement.Status != meta.PlacementStatusDeleting {
			if err := s.pool.PurgeSharedTenant(ctx, fsID, placement.DbID); err != nil {
				// Do not roll back the deleting mark; the purge is resumable and the
				// next delete request will continue it.
				logger.Error(ctx, "shared_tenant_purge_failed", zap.String("tenant_id", t.ID), zap.Int64("db_id", placement.DbID), zap.Error(err))
				errJSON(w, http.StatusInternalServerError, "failed to purge tenant data from shared db")
				return
			}

			_ = s.meta.AbortActiveUploadReservations(ctx, t.ID)
			markDeleted := t.StorageNamespaceID == ""
			if err := s.meta.FinalizeSharedTenantDeleteMetadata(ctx, t.ID, fsID, placement.DbID, s.sharedDBReopenRatio, markDeleted); err != nil {
				logger.Error(ctx, "shared_tenant_metadata_finalize_failed", zap.String("tenant_id", t.ID), zap.Error(err))
				errJSON(w, http.StatusInternalServerError, "failed to finalize shared tenant deletion")
				return
			}
			if markDeleted {
				_ = s.meta.RevokeTenantAPIKeys(ctx, t.ID)
				writeTenantDeleteStatus(w, meta.TenantDeleted)
				return
			}
		}

		status := meta.TenantDeleting
		if !deleteJobExists {
			var enqueueErr error
			status, enqueueErr = s.enqueueTenantDeleteJob(ctx, t)
			if enqueueErr != nil {
				errJSON(w, http.StatusInternalServerError, "failed to enqueue tenant delete cleanup")
				return
			}
		}
		_ = s.meta.RevokeTenantAPIKeys(ctx, t.ID)
		writeTenantDeleteStatus(w, status)
		return
	}
	logger.Error(ctx, "shared_tenant_delete_placement_missing", zap.String("tenant_id", t.ID), zap.Int64("fs_id", fsID))
	errJSON(w, http.StatusInternalServerError, "shared tenant placement is missing")
}

func (s *Server) enqueueTenantDeleteJob(ctx context.Context, t *meta.Tenant) (meta.TenantStatus, error) {
	if t.StorageNamespaceID == "" {
		if err := s.meta.MarkTenantDeleted(ctx, t.ID); err != nil {
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
	case tenant.ProviderTiDBCloudStarterLegacy:
		deprovisioner, ok := s.legacyStarterProvisioner.(tenant.Deprovisioner)
		if !ok {
			return fmt.Errorf("legacy starter provisioner does not support deprovision")
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
