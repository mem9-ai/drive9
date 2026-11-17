package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"github.com/mem9-ai/drive9/pkg/tenant/tidbcloudnative"
	"go.uber.org/zap"
)

// nativeCredentialRemediationBatchSize bounds one page of the tenant scan. A variable so a
// test can force the multi-page path without seeding a hundred tenants.
var nativeCredentialRemediationBatchSize = 100

const (
	// nativeCredentialRemediationTimeout bounds one tenant's database work so a wedged
	// tenant cannot stall the leader's pass.
	nativeCredentialRemediationTimeout = 30 * time.Second
	// nativeCredentialRemediationInterval paces the pass for as long as this pod leads. A
	// tenant that becomes active after a pass (a resumed provisioning flow, a fork
	// that starts serving) must not have to wait for the next leadership change to
	// lose the legacy grant, so the cleanup is periodic rather than one-shot.
	nativeCredentialRemediationInterval = 15 * time.Minute
	// nativeCredentialRemediationRecheckInterval memoizes a successful remediation. The grant
	// cannot come back (setup no longer grants it and re-setup revokes it), so a
	// remediated tenant does not need a fresh connection on every pass; it is
	// re-checked once a day to catch an operator re-granting it.
	nativeCredentialRemediationRecheckInterval = 24 * time.Hour
	// nativeCredentialRemediationMaxBackoff caps a failing tenant's retry delay. It is
	// longer than one interval so a persistently failing tenant is skipped on some
	// passes instead of being attempted every tick.
	nativeCredentialRemediationMaxBackoff = 30 * time.Minute
)

// remediateNativeTenantCredentials fixes the native tenants the current provisioning path
// can no longer reach: it strips the legacy cluster-wide SYSTEM_VARIABLES_ADMIN grant and
// rotates a restricted account off the cluster root password onto its derived per-tenant
// one.
//
// Tenants finalized before this version keep both problems. Their stored DSN already names
// the restricted account, so the per-tenant setup never runs again for them, while the
// grant is a whole-cluster escalation reachable from tenant SQL (SET GLOBAL) and the shared
// password is a root credential stored in the control plane. The pass is leader-only,
// repeats every nativeCredentialRemediationInterval while that leader lives, and is
// idempotent: a revoke of an absent grant is tolerated, and the derived password is a pure
// function of the stored root secret and the tenant, so a retry converges on one value.
func (s *Server) remediateNativeTenantCredentials(ctx context.Context) {
	if s.meta == nil || s.pool == nil {
		return
	}
	// No deployment-level provider gate: a native tenant recorded by an earlier
	// deployment outlives a provider switch, and the per-tenant check below decides
	// from the stored record. The remediation itself only needs the tenant's own
	// DSN and password.
	remediated, skipped, failed, reviewRequired := 0, 0, 0, 0
	// No cluster-level dedupe: the revoke is cluster-global, but the credential rotation is
	// per account, so every tenant needs its own connection at least once. Steady state is
	// bounded by the per-tenant memo below instead (one dial per tenant per recheck window).
	// Suspended tenants keep their database and therefore the legacy grant; each status
	// is scanned with its own keyset cursor.
	for _, status := range [...]meta.TenantStatus{meta.TenantActive, meta.TenantSuspended} {
		cursor := time.Time{}
		afterID := ""
		for {
			if ctx.Err() != nil {
				return
			}
			page, err := s.meta.ListTenantsByStatusAfter(ctx, status, cursor, afterID, nativeCredentialRemediationBatchSize)
			if err != nil {
				if ctx.Err() != nil {
					// Shutdown mid-scan is not a failure of the pass.
					return
				}
				// One transient meta blip must not cost a whole interval: retry the page
				// once before giving up until the next tick.
				page, err = s.meta.ListTenantsByStatusAfter(ctx, status, cursor, afterID, nativeCredentialRemediationBatchSize)
				if err != nil {
					if ctx.Err() == nil {
						logger.Warn(ctx, "native_credential_remediation_scan_failed", zap.Error(err))
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
				switch s.remediateNativeTenantCredentialFor(ctx, t) {
				case nativeCredentialRemediationDone:
					remediated++
				case nativeCredentialRemediationSkipped:
					skipped++
				case nativeCredentialRemediationFailed:
					failed++
				case nativeCredentialRemediationReviewRequired:
					reviewRequired++
				}
			}
			last := page[len(page)-1]
			cursor, afterID = last.CreatedAt, last.ID
			if len(page) < nativeCredentialRemediationBatchSize {
				break
			}
		}
	}
	if remediated > 0 || failed > 0 || reviewRequired > 0 {
		logger.Info(ctx, "native_credential_remediation_complete",
			zap.Int("remediated", remediated), zap.Int("skipped", skipped), zap.Int("failed", failed),
			zap.Int("review_required", reviewRequired))
	}
}

type nativeCredentialRemediationResult int

const (
	nativeCredentialRemediationSkipped nativeCredentialRemediationResult = iota
	nativeCredentialRemediationDone
	nativeCredentialRemediationFailed
	// nativeCredentialRemediationReviewRequired means the tenant was recorded as still
	// possibly exposed instead of being claimed as remediated. It is reported separately so
	// the pass's log distinguishes "nothing to do" from "someone has to look".
	nativeCredentialRemediationReviewRequired
)

// nativeCredentialRemediationEligible reports whether a tenant's stored record can carry the
// legacy grant at all: only native tenants whose stored user is the restricted account
// reach the fixed role. A DSN still naming the root principal is handled by the setup
// path when the tenant is finalized.
func nativeCredentialRemediationEligible(t *meta.Tenant) bool {
	return t != nil && t.Provider == tenant.ProviderTiDBCloudNative && tidbcloudnative.IsRestrictedSystemUser(t.DBUser)
}

func (s *Server) remediateNativeTenantCredentialFor(ctx context.Context, t *meta.Tenant) nativeCredentialRemediationResult {
	if !nativeCredentialRemediationEligible(t) {
		return nativeCredentialRemediationSkipped
	}
	// A recorded outcome is authoritative: a remediated tenant needs no connection, and a
	// tenant awaiting review must not be retried as if a retry could fix it. Both are read
	// from the control plane so a restart or another leader does not re-dial them.
	if state, _, err := s.meta.TenantCredentialState(ctx, t.ID); err != nil {
		if !errors.Is(err, meta.ErrNotFound) {
			logger.Warn(ctx, "native_credential_state_read_failed", zap.String("tenant_id", t.ID), zap.Error(err))
			s.recordNativeCredentialRemediationFailure(t.ID)
			return nativeCredentialRemediationFailed
		}
	} else {
		if state == meta.TenantCredentialReviewRequired {
			logger.Warn(ctx, "native_credential_review_required",
				zap.String("tenant_id", t.ID),
				zap.String("detail", "stored credential does not authenticate as the cluster root, so the legacy cluster-wide grant could not be verified or revoked from here"),
				zap.Error(tidbcloudnative.ErrLegacyGrantUnverifiable))
			metrics.RecordOperation("user_db_access", "native_credential_remediation", "review_required", 0)
		}
		s.markNativeCredentialRemediationDone(t.ID)
		return nativeCredentialRemediationSkipped
	}
	if s.nativeCredentialRemediationRecentlyDone(t.ID) {
		return nativeCredentialRemediationSkipped
	}
	if s.nativeCredentialRemediationBackoff(t.ID) {
		return nativeCredentialRemediationSkipped
	}
	plain, err := s.pool.Decrypt(ctx, t.DBPasswordCipher)
	if err != nil {
		// A wrong master key or corrupt cipher fails identically on every pass: back it
		// off like any other failure instead of dialling and logging every tick.
		logger.Warn(ctx, "native_credential_remediation_decrypt_failed", zap.String("tenant_id", t.ID), zap.Error(err))
		metrics.RecordOperation("user_db_access", "native_credential_remediation", "error", 0)
		s.recordNativeCredentialRemediationFailure(t.ID)
		return nativeCredentialRemediationFailed
	}
	dsn := tenantDSN(t.DBUser, string(plain), t.DBHost, t.DBPort, t.DBName, t.DBTLS, t.Provider)
	remediateCtx, cancel := context.WithTimeout(ctx, nativeCredentialRemediationTimeout)
	defer cancel()
	rotatedPassword, rotated, err := tidbcloudnative.RotateLegacySystemUserCredential(remediateCtx, dsn, t.ID)
	if err != nil {
		if errors.Is(err, tidbcloudnative.ErrLegacyGrantUnverifiable) {
			// The stored credential does not authenticate as the cluster root, so this server
			// cannot check or revoke the legacy cluster-wide grant: the tenant is recorded as
			// still possibly exposed for an operator, never as remediated. Recording it also
			// stops the pass re-dialling a cluster it has no authority over every tick.
			if stateErr := s.meta.SetTenantCredentialState(ctx, t.ID, meta.TenantCredentialReviewRequired,
				"stored credential is not root-equivalent: legacy grant unverifiable"); stateErr != nil {
				logger.Warn(ctx, "native_credential_state_write_failed", zap.String("tenant_id", t.ID), zap.Error(stateErr))
				s.recordNativeCredentialRemediationFailure(t.ID)
				return nativeCredentialRemediationFailed
			}
			logger.Warn(ctx, "native_credential_review_required",
				zap.String("tenant_id", t.ID),
				zap.String("detail", "stored credential does not authenticate as the cluster root, so the legacy cluster-wide grant could not be verified or revoked from here"))
			metrics.RecordOperation("user_db_access", "native_credential_remediation", "review_required", 0)
			s.clearNativeCredentialRemediationFailure(t.ID)
			s.markNativeCredentialRemediationDone(t.ID)
			return nativeCredentialRemediationReviewRequired
		}
		if isAccessDenied(err) {
			// A statement was refused for lack of privilege: the account is managed by an
			// operator, and the grant state is theirs to own. Record it the same way.
			if stateErr := s.meta.SetTenantCredentialState(ctx, t.ID, meta.TenantCredentialReviewRequired,
				"privilege refused while remediating: tenant account is operator-managed"); stateErr != nil {
				logger.Warn(ctx, "native_credential_state_write_failed", zap.String("tenant_id", t.ID), zap.Error(stateErr))
				s.recordNativeCredentialRemediationFailure(t.ID)
				return nativeCredentialRemediationFailed
			}
			logger.Warn(ctx, "native_credential_review_required", zap.String("tenant_id", t.ID),
				zap.String("detail", "the tenant account is operator-managed; the legacy cluster-wide grant could not be revoked from here"))
			metrics.RecordOperation("user_db_access", "native_credential_remediation", "review_required", 0)
			s.clearNativeCredentialRemediationFailure(t.ID)
			s.markNativeCredentialRemediationDone(t.ID)
			return nativeCredentialRemediationReviewRequired
		}
		logger.Warn(ctx, "native_credential_remediation_failed", zap.String("tenant_id", t.ID), zap.Error(err))
		metrics.RecordOperation("user_db_access", "native_credential_remediation", "error", 0)
		s.recordNativeCredentialRemediationFailure(t.ID)
		return nativeCredentialRemediationFailed
	}
	if rotated {
		// The database now only accepts the derived password, so persist it before anything
		// else can observe the old one. A failure here is not recoverable by retrying from
		// the stored credential — it no longer authenticates — so record it as a failure and
		// retry, which re-derives the same password from the same root secret. The derivation
		// is what makes that retry safe.
		if err := s.persistRotatedTenantCredential(ctx, t, rotatedPassword); err != nil {
			logger.Warn(ctx, "native_credential_rotation_persist_failed", zap.String("tenant_id", t.ID), zap.Error(err))
			metrics.RecordOperation("user_db_access", "native_credential_remediation", "error", 0)
			s.recordNativeCredentialRemediationFailure(t.ID)
			return nativeCredentialRemediationFailed
		}
		// Drop this replica's own cached backend: its connections still carry the retired
		// password, and peers are told through the outbox signal the update writes.
		if s.pool != nil {
			s.pool.Invalidate(t.ID)
		}
		if stateErr := s.meta.SetTenantCredentialState(ctx, t.ID, meta.TenantCredentialRemediated, ""); stateErr != nil {
			// The password is rotated, so the next pass sees a non-root credential. Without
			// the marker it would read that as unverifiable and record the tenant for review,
			// so a failure here must stay a failure and be retried.
			logger.Warn(ctx, "native_credential_state_write_failed", zap.String("tenant_id", t.ID), zap.Error(stateErr))
			s.recordNativeCredentialRemediationFailure(t.ID)
			return nativeCredentialRemediationFailed
		}
		logger.Info(ctx, "native_credential_rotated", zap.String("tenant_id", t.ID))
	}
	s.clearNativeCredentialRemediationFailure(t.ID)
	s.markNativeCredentialRemediationDone(t.ID)
	return nativeCredentialRemediationDone
}

// persistRotatedTenantCredential stores the derived password for a tenant whose legacy
// account was just rotated. The update is a compare-and-set on the user name, which the
// rotation does not change: the value is derived from the root secret and the tenant, so two
// replicas that rotate concurrently write the same password rather than racing.
func (s *Server) persistRotatedTenantCredential(ctx context.Context, t *meta.Tenant, password string) error {
	cipherPass, err := s.pool.Encrypt(ctx, []byte(password))
	if err != nil {
		return fmt.Errorf("encrypt rotated tenant credential: %w", err)
	}
	updated, err := s.meta.UpdateTenantDBCredentialIf(ctx, t.ID, t.DBUser, t.DBUser, cipherPass)
	if err != nil {
		return fmt.Errorf("persist rotated tenant credential: %w", err)
	}
	if !updated {
		return fmt.Errorf("tenant credential changed while rotating")
	}
	return nil
}

// nativeCredentialRemediationRecentlyDone reports whether a tenant was remediated inside the
// memo window, so a steady-state pass does not open a connection per tenant every tick.
func (s *Server) nativeCredentialRemediationRecentlyDone(tenantID string) bool {
	s.nativeCredentialRemediationMu.Lock()
	defer s.nativeCredentialRemediationMu.Unlock()
	doneAt, ok := s.nativeCredentialRemediationDone[tenantID]
	if !ok {
		return false
	}
	if time.Since(doneAt) < nativeCredentialRemediationRecheckInterval {
		return true
	}
	// Prune the expired entry instead of letting it sit there: this only fires for a
	// tenant the scan still visits, so it bounds the map for live tenants rather than
	// sweeping up entries whose tenant was deleted while memoized.
	delete(s.nativeCredentialRemediationDone, tenantID)
	return false
}

// markNativeCredentialRemediationDone records a successful remediation.
func (s *Server) markNativeCredentialRemediationDone(tenantID string) {
	s.nativeCredentialRemediationMu.Lock()
	defer s.nativeCredentialRemediationMu.Unlock()
	if s.nativeCredentialRemediationDone == nil {
		s.nativeCredentialRemediationDone = map[string]time.Time{}
	}
	s.nativeCredentialRemediationDone[tenantID] = time.Now()
}

// nativeCredentialRemediationBackoff reports whether a tenant that failed recently is still
// inside its retry delay.
func (s *Server) nativeCredentialRemediationBackoff(tenantID string) bool {
	s.nativeCredentialRemediationMu.Lock()
	defer s.nativeCredentialRemediationMu.Unlock()
	retryAt, ok := s.nativeCredentialRemediationRetryAt[tenantID]
	if !ok {
		return false
	}
	if time.Now().Before(retryAt) {
		return true
	}
	// Only the expired scheduling entry goes: the attempt count has to survive so a
	// persistently failing tenant keeps backing off towards the cap instead of restarting
	// at one attempt on every pass. A success clears both through
	// clearNativeCredentialRemediationFailure.
	delete(s.nativeCredentialRemediationRetryAt, tenantID)
	return false
}

// recordNativeCredentialRemediationFailure schedules the next attempt for a tenant with
// exponential backoff, capped so a permanently unreachable tenant keeps being retried
// while this pod leads.
func (s *Server) recordNativeCredentialRemediationFailure(tenantID string) {
	s.nativeCredentialRemediationMu.Lock()
	defer s.nativeCredentialRemediationMu.Unlock()
	if s.nativeCredentialRemediationFailures == nil {
		s.nativeCredentialRemediationFailures = map[string]int{}
	}
	if s.nativeCredentialRemediationRetryAt == nil {
		s.nativeCredentialRemediationRetryAt = map[string]time.Time{}
	}
	s.nativeCredentialRemediationFailures[tenantID]++
	delay := nativeCredentialRemediationMaxBackoff
	if shift := s.nativeCredentialRemediationFailures[tenantID] - 1; shift < 6 {
		if candidate := time.Minute << shift; candidate < delay {
			delay = candidate
		}
	}
	s.nativeCredentialRemediationRetryAt[tenantID] = time.Now().Add(delay)
}

// clearNativeCredentialRemediationFailure forgets a tenant's retry state after a successful
// pass.
func (s *Server) clearNativeCredentialRemediationFailure(tenantID string) {
	s.nativeCredentialRemediationMu.Lock()
	defer s.nativeCredentialRemediationMu.Unlock()
	delete(s.nativeCredentialRemediationFailures, tenantID)
	delete(s.nativeCredentialRemediationRetryAt, tenantID)
}

// isAccessDenied reports whether err is the server rejecting the credentials
// themselves (ER_ACCESS_DENIED_ERROR).
func isAccessDenied(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1045
}
