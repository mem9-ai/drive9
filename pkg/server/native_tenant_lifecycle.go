package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/tenant"
	"github.com/mem9-ai/dat9/pkg/tenant/tidbcloudnative"
	"github.com/mem9-ai/dat9/pkg/tidbcloud"
)

func reconcileNativeTenantLifecycle(ctx context.Context, metaStore *meta.Store, pool *tenant.Pool, provisioner tenant.Provisioner, t *meta.Tenant) error {
	if t == nil || metaStore == nil || t.Provider != tenant.ProviderTiDBCloudNative {
		return nil
	}

	np, ok := provisioner.(*tidbcloudnative.Provisioner)
	if !ok {
		return nil
	}

	clusterID := t.ClusterID
	if clusterID == "" {
		clusterID = t.ID
	}

	lifecycle, err := np.ClusterLifecycleState(ctx, clusterID)
	if err != nil {
		if !errors.Is(err, tidbcloud.ErrClusterNotFound) {
			return fmt.Errorf("get native cluster lifecycle %s: %w", clusterID, err)
		}
		lifecycle = tidbcloud.ClusterLifecycleDeleted
	}

	desired := t.Status
	switch lifecycle {
	case tidbcloud.ClusterLifecycleDeleted, tidbcloud.ClusterLifecycleDeleting:
		desired = meta.TenantDeleted
	case tidbcloud.ClusterLifecycleProvisioning:
		if t.Status != meta.TenantFailed && t.Status != meta.TenantSuspended {
			desired = meta.TenantProvisioning
		}
	}

	if desired == t.Status {
		return nil
	}

	logger.Info(ctx, "server_event", eventFields(ctx, "native_tenant_lifecycle_reconciled",
		"tenant_id", t.ID,
		"cluster_id", clusterID,
		"from_status", t.Status,
		"to_status", desired,
		"cluster_lifecycle", lifecycle)...)
	if err := metaStore.UpdateTenantStatus(ctx, t.ID, desired); err != nil {
		return fmt.Errorf("update tenant status %s -> %s: %w", t.ID, desired, err)
	}
	if pool != nil {
		pool.Invalidate(t.ID)
	}
	t.Status = desired
	return nil
}
