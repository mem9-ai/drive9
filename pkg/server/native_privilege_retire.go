package server

import (
	"context"
	"errors"
	"strconv"
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
	// nativePrivilegeRetireRecheckInterval memoizes a successful retire. The grant
	// cannot come back (setup no longer grants it and re-setup revokes it), so a
	// remediated tenant does not need a fresh connection on every pass; it is
	// re-checked once a day to catch an operator re-granting it.
	nativePrivilegeRetireRecheckInterval = 24 * time.Hour
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
	retired, skipped, failed := 0, 0, 0
	// The revoke is cluster-global (the fixed role lives on the instance), so tenants
	// sharing a cluster must not each open a connection: the first success retires the
	// cluster and the rest are memoized without a dial.
	clusterRetired := map[string]bool{}
	// Suspended tenants keep their database and therefore the legacy grant; each status
	// is scanned with its own keyset cursor.
	for _, status := range [...]meta.TenantStatus{meta.TenantActive, meta.TenantSuspended} {
		cursor := time.Time{}
		afterID := ""
		for {
			if ctx.Err() != nil {
				return
			}
			page, err := s.meta.ListTenantsByStatusAfter(ctx, status, cursor, afterID, nativePrivilegeRetireBatchSize)
			if err != nil {
				if ctx.Err() != nil {
					// Shutdown mid-scan is not a failure of the pass.
					return
				}
				// One transient meta blip must not cost a whole interval: retry the page
				// once before giving up until the next tick.
				page, err = s.meta.ListTenantsByStatusAfter(ctx, status, cursor, afterID, nativePrivilegeRetireBatchSize)
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
				if ctx.Err() != nil {
					return
				}
				t := &page[i]
				if cluster := nativePrivilegeRetireClusterKey(t); cluster != "" && clusterRetired[cluster] && nativePrivilegeRetireEligible(t) {
					s.markNativePrivilegeRetireDone(t.ID)
					skipped++
					continue
				}
				switch s.retireNativeTenantVariablesPrivilegeFor(ctx, t) {
				case nativePrivilegeRetireDone:
					if cluster := nativePrivilegeRetireClusterKey(t); cluster != "" {
						clusterRetired[cluster] = true
					}
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

// nativePrivilegeRetireEligible reports whether a tenant's stored record can carry the
// legacy grant at all: only native tenants whose stored user is the restricted account
// reach the fixed role. A DSN still naming the root principal is handled by the setup
// path when the tenant is finalized.
func nativePrivilegeRetireEligible(t *meta.Tenant) bool {
	return t != nil && t.Provider == tenant.ProviderTiDBCloudNative && tidbcloudnative.IsRestrictedSystemUser(t.DBUser)
}

func (s *Server) retireNativeTenantVariablesPrivilegeFor(ctx context.Context, t *meta.Tenant) nativePrivilegeRetireResult {
	if !nativePrivilegeRetireEligible(t) {
		return nativePrivilegeRetireSkipped
	}
	if s.nativePrivilegeRetireRecentlyDone(t.ID) {
		return nativePrivilegeRetireSkipped
	}
	if s.nativePrivilegeRetireBackoff(t.ID) {
		return nativePrivilegeRetireSkipped
	}
	plain, err := s.pool.Decrypt(ctx, t.DBPasswordCipher)
	if err != nil {
		// A wrong master key or corrupt cipher fails identically on every pass: back it
		// off like any other failure instead of dialling and logging every tick.
		logger.Warn(ctx, "native_privilege_retire_decrypt_failed", zap.String("tenant_id", t.ID), zap.Error(err))
		metrics.RecordOperation("user_db_access", "native_privilege_retire", "error", 0)
		s.recordNativePrivilegeRetireFailure(t.ID)
		return nativePrivilegeRetireFailed
	}
	dsn := tenantDSN(t.DBUser, string(plain), t.DBHost, t.DBPort, t.DBName, t.DBTLS, t.Provider)
	retireCtx, cancel := context.WithTimeout(ctx, nativePrivilegeRetireTimeout)
	defer cancel()
	if err := tidbcloudnative.RetireLegacySystemUserPrivilege(retireCtx, dsn); err != nil {
		if isAccessDenied(err) {
			// The stored password no longer authenticates as the cluster root: an operator
			// rotated the credential and owns the account now. Memoize it like a remediation
			// so the pass stops attempting a root login against that cluster every tick, and
			// re-check daily; one tenant cannot stall the pass meanwhile.
			logger.Info(ctx, "native_privilege_retire_skipped_rotated", zap.String("tenant_id", t.ID))
			s.markNativePrivilegeRetireDone(t.ID)
			return nativePrivilegeRetireSkipped
		}
		logger.Warn(ctx, "native_privilege_retire_failed", zap.String("tenant_id", t.ID), zap.Error(err))
		metrics.RecordOperation("user_db_access", "native_privilege_retire", "error", 0)
		s.recordNativePrivilegeRetireFailure(t.ID)
		return nativePrivilegeRetireFailed
	}
	s.clearNativePrivilegeRetireFailure(t.ID)
	s.markNativePrivilegeRetireDone(t.ID)
	return nativePrivilegeRetireDone
}

// nativePrivilegeRetireRecentlyDone reports whether a tenant was remediated inside the
// memo window, so a steady-state pass does not open a connection per tenant every tick.
func (s *Server) nativePrivilegeRetireRecentlyDone(tenantID string) bool {
	s.nativePrivilegeRetireMu.Lock()
	defer s.nativePrivilegeRetireMu.Unlock()
	doneAt, ok := s.nativePrivilegeRetireDone[tenantID]
	if !ok {
		return false
	}
	if time.Since(doneAt) < nativePrivilegeRetireRecheckInterval {
		return true
	}
	// A tenant deleted after remediation would otherwise keep its entry for the life of
	// the process; the first stale lookup drops it.
	delete(s.nativePrivilegeRetireDone, tenantID)
	return false
}

// markNativePrivilegeRetireDone records a successful retire.
func (s *Server) markNativePrivilegeRetireDone(tenantID string) {
	s.nativePrivilegeRetireMu.Lock()
	defer s.nativePrivilegeRetireMu.Unlock()
	if s.nativePrivilegeRetireDone == nil {
		s.nativePrivilegeRetireDone = map[string]time.Time{}
	}
	s.nativePrivilegeRetireDone[tenantID] = time.Now()
}

// nativePrivilegeRetireBackoff reports whether a tenant that failed recently is still
// inside its retry delay.
func (s *Server) nativePrivilegeRetireBackoff(tenantID string) bool {
	s.nativePrivilegeRetireMu.Lock()
	defer s.nativePrivilegeRetireMu.Unlock()
	retryAt, ok := s.nativePrivilegeRetireRetryAt[tenantID]
	if !ok {
		return false
	}
	if time.Now().Before(retryAt) {
		return true
	}
	// Only the expired scheduling entry goes: the attempt count has to survive so a
	// persistently failing tenant keeps backing off towards the cap instead of restarting
	// at one attempt on every pass. A success clears both through
	// clearNativePrivilegeRetireFailure.
	delete(s.nativePrivilegeRetireRetryAt, tenantID)
	return false
}

// nativePrivilegeRetireClusterKey identifies the cluster a tenant's database lives on,
// which is the scope of the revoke. The endpoint alone is not enough: one TiDB Cloud
// gateway can front several instances, and the account prefix is what names the
// instance, so deduping on host and port would let one instance's success skip another
// instance's tenants without ever connecting to them.
func nativePrivilegeRetireClusterKey(t *meta.Tenant) string {
	if t == nil || strings.TrimSpace(t.DBHost) == "" {
		return ""
	}
	user := strings.TrimSpace(t.DBUser)
	prefix := strings.TrimSuffix(user, tidbcloudnative.RestrictedSystemUserNameSuffix)
	if prefix == user {
		// Not a restricted account: fall back to the account name so distinct principals
		// on one endpoint are never folded together.
		prefix = user
	}
	return strings.TrimSpace(t.DBHost) + ":" + strconv.Itoa(t.DBPort) + "|" + prefix
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
