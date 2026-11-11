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

// nativePrivilegeRetireBatchSize bounds one page of the tenant scan. A variable so a
// test can force the multi-page path without seeding a hundred tenants.
var nativePrivilegeRetireBatchSize = 100

const (
	// nativePrivilegeRetireTimeout bounds one tenant's database work so a wedged
	// tenant cannot stall the leader's pass.
	nativePrivilegeRetireTimeout = 30 * time.Second
	// nativePrivilegeRetireInterval paces the pass for as long as this pod leads. A
	// tenant that becomes active after a pass (a resumed provisioning flow, a fork
	// that starts serving) must not have to wait for the next leadership change to
	// lose the legacy grant, so the cleanup is periodic rather than one-shot.
	nativePrivilegeRetireInterval = 15 * time.Minute
	// nativePrivilegeRetireMaxBackoff caps a failing tenant's retry delay. It is
	// longer than one interval so a persistently failing tenant is skipped on some
	// passes instead of being attempted every tick.
	nativePrivilegeRetireMaxBackoff = 30 * time.Minute
)

// retireNativeTenantVariablesPrivilege strips the legacy SYSTEM_VARIABLES_ADMIN ON *.*
// grant from native tenant roles.
//
// Tenants finalized before this version keep the grant: their stored DSN already names
// the restricted account, so the per-tenant setup never runs again for them, and the
// grant is a whole-cluster escalation reachable from tenant SQL (SET GLOBAL). The pass
// is leader-only, repeats every nativePrivilegeRetireInterval while that leader lives,
// and is idempotent — a revoke of an absent grant is tolerated both by TiDB and by the
// helper.
func (s *Server) retireNativeTenantVariablesPrivilege(ctx context.Context) {
	if s.meta == nil || s.pool == nil {
		return
	}
	// No deployment-level provider gate: a native tenant recorded by an earlier
	// deployment outlives a provider switch, and the per-tenant check below decides
	// from the stored record. The remediation itself only needs the tenant's own
	// DSN and password.
	cursor := time.Time{}
	afterID := ""
	retired, skipped, failed := 0, 0, 0
	for {
		page, err := s.meta.ListTenantsByStatusAfter(ctx, meta.TenantActive, cursor, afterID, nativePrivilegeRetireBatchSize)
		if err != nil {
			if ctx.Err() != nil {
				// Shutdown mid-scan is not a failure of the pass.
				return
			}
			// One transient meta blip must not cost a whole interval: retry the page
			// once before giving up until the next tick.
			page, err = s.meta.ListTenantsByStatusAfter(ctx, meta.TenantActive, cursor, afterID, nativePrivilegeRetireBatchSize)
			if err != nil {
				if ctx.Err() == nil {
					logger.Warn(ctx, "native_privilege_retire_scan_failed", zap.Error(err))
				}
				return
			}
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
		if len(page) < nativePrivilegeRetireBatchSize {
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
	if s.nativePrivilegeRetireBackoff(t.ID) {
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
		s.recordNativePrivilegeRetireFailure(t.ID)
		return nativePrivilegeRetireFailed
	}
	s.clearNativePrivilegeRetireFailure(t.ID)
	return nativePrivilegeRetireDone
}

// nativePrivilegeRetireBackoff reports whether a tenant that failed recently is still
// inside its retry delay.
func (s *Server) nativePrivilegeRetireBackoff(tenantID string) bool {
	s.nativePrivilegeRetireMu.Lock()
	defer s.nativePrivilegeRetireMu.Unlock()
	retryAt, ok := s.nativePrivilegeRetireRetryAt[tenantID]
	return ok && time.Now().Before(retryAt)
}

// recordNativePrivilegeRetireFailure schedules the next attempt for a tenant with
// exponential backoff, capped so a permanently unreachable tenant keeps being retried
// while this pod leads.
func (s *Server) recordNativePrivilegeRetireFailure(tenantID string) {
	s.nativePrivilegeRetireMu.Lock()
	defer s.nativePrivilegeRetireMu.Unlock()
	if s.nativePrivilegeRetireFailures == nil {
		s.nativePrivilegeRetireFailures = map[string]int{}
	}
	if s.nativePrivilegeRetireRetryAt == nil {
		s.nativePrivilegeRetireRetryAt = map[string]time.Time{}
	}
	s.nativePrivilegeRetireFailures[tenantID]++
	delay := nativePrivilegeRetireMaxBackoff
	if shift := s.nativePrivilegeRetireFailures[tenantID] - 1; shift < 6 {
		if candidate := time.Minute << shift; candidate < delay {
			delay = candidate
		}
	}
	s.nativePrivilegeRetireRetryAt[tenantID] = time.Now().Add(delay)
}

// clearNativePrivilegeRetireFailure forgets a tenant's retry state after a successful
// pass.
func (s *Server) clearNativePrivilegeRetireFailure(tenantID string) {
	s.nativePrivilegeRetireMu.Lock()
	defer s.nativePrivilegeRetireMu.Unlock()
	delete(s.nativePrivilegeRetireFailures, tenantID)
	delete(s.nativePrivilegeRetireRetryAt, tenantID)
}

// isAccessDenied reports whether err is the server rejecting the credentials
// themselves (ER_ACCESS_DENIED_ERROR).
func isAccessDenied(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1045
}
