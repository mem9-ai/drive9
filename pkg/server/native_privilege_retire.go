package server

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"github.com/mem9-ai/drive9/pkg/tenant/tidbcloudnative"
	"go.uber.org/zap"
)

const (
	// nativePrivilegeRetireBatch bounds one page of the tenant scan.
	nativePrivilegeRetireBatch = 100
	// nativePrivilegeRetireTimeout bounds one tenant's database work so a wedged
	// tenant cannot stall the leader's pass.
	nativePrivilegeRetireTimeout = 30 * time.Second
)

// retireNativeTenantVariablesPrivilege strips the legacy SYSTEM_VARIABLES_ADMIN ON *.*
// grant from native tenant roles.
//
// Tenants finalized before this version keep the grant: their stored DSN already names
// the restricted account, so the per-tenant setup never runs again for them, and the
// grant is a whole-cluster escalation reachable from tenant SQL (SET GLOBAL). The pass
// is leader-only, runs once per leadership generation, and is idempotent — a revoke of
// an absent grant is tolerated both by TiDB and by the helper.
func (s *Server) retireNativeTenantVariablesPrivilege(ctx context.Context) {
	if s.meta == nil || s.pool == nil || s.provisioner == nil {
		return
	}
	if s.provisioner.ProviderType() != tenant.ProviderTiDBCloudNative {
		return
	}
	cursor := time.Time{}
	afterID := ""
	retired, skipped, failed := 0, 0, 0
	for {
		page, err := s.meta.ListTenantsByStatusAfter(ctx, meta.TenantActive, cursor, afterID, nativePrivilegeRetireBatch)
		if err != nil {
			logger.Warn(ctx, "native_privilege_retire_scan_failed", zap.Error(err))
			return
		}
		if len(page) == 0 {
			break
		}
		for i := range page {
			switch s.retireNativeTenantVariablesPrivilegeFor(ctx, &page[i]) {
			case nativePrivilegeRetireDone:
				retired++
			case nativePrivilegeRetireSkipped:
				skipped++
			case nativePrivilegeRetireFailed:
				failed++
			}
		}
		last := page[len(page)-1]
		cursor, afterID = last.CreatedAt, last.ID
		if len(page) < nativePrivilegeRetireBatch {
			break
		}
	}
	if retired > 0 || failed > 0 {
		logger.Info(ctx, "native_privilege_retire_complete",
			zap.Int("retired", retired), zap.Int("skipped", skipped), zap.Int("failed", failed))
	}
}

type nativePrivilegeRetireResult int

const (
	nativePrivilegeRetireSkipped nativePrivilegeRetireResult = iota
	nativePrivilegeRetireDone
	nativePrivilegeRetireFailed
)

func (s *Server) retireNativeTenantVariablesPrivilegeFor(ctx context.Context, t *meta.Tenant) nativePrivilegeRetireResult {
	if t == nil || t.Provider != tenant.ProviderTiDBCloudNative {
		return nativePrivilegeRetireSkipped
	}
	// Only tenants whose stored user is the restricted account can carry the legacy
	// grant; a DSN still naming the root principal is handled by the setup path when
	// the tenant is finalized.
	if !strings.HasSuffix(strings.TrimSpace(t.DBUser), ".tdc_fs_sys") {
		return nativePrivilegeRetireSkipped
	}
	plain, err := s.pool.Decrypt(ctx, t.DBPasswordCipher)
	if err != nil {
		logger.Warn(ctx, "native_privilege_retire_decrypt_failed", zap.String("tenant_id", t.ID), zap.Error(err))
		return nativePrivilegeRetireFailed
	}
	dsn := tenantDSN(t.DBUser, string(plain), t.DBHost, t.DBPort, t.DBName, t.DBTLS, t.Provider)
	retireCtx, cancel := context.WithTimeout(ctx, nativePrivilegeRetireTimeout)
	defer cancel()
	if err := tidbcloudnative.RetireLegacySystemUserPrivilege(retireCtx, dsn); err != nil {
		// A password that no longer authenticates as the cluster root means the
		// credential was rotated by an operator, who already owns the account: record
		// it and keep going so one tenant cannot stall the pass.
		if isAccessDenied(err) {
			logger.Info(ctx, "native_privilege_retire_skipped_rotated", zap.String("tenant_id", t.ID))
			return nativePrivilegeRetireSkipped
		}
		logger.Warn(ctx, "native_privilege_retire_failed", zap.String("tenant_id", t.ID), zap.Error(err))
		metrics.RecordOperation("user_db_access", "native_privilege_retire", "error", 0)
		return nativePrivilegeRetireFailed
	}
	return nativePrivilegeRetireDone
}

// isAccessDenied reports whether err is the server rejecting the credentials
// themselves (ER_ACCESS_DENIED_ERROR).
func isAccessDenied(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1045
}
