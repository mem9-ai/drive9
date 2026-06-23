package backend

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
)

var (
	ErrUploadTooLarge           = errors.New("upload too large")
	ErrStorageQuotaExceeded     = errors.New("tenant storage quota exceeded")
	ErrMediaLLMQuotaExceeded    = errors.New("tenant media LLM file quota exceeded")
	ErrReservationNotFound      = errors.New("upload reservation not found")
	ErrReservationAlreadyExists = errors.New("upload reservation already exists")
)

// UseServerQuota reports whether this backend reads authoritative quota state
// from the central server DB rather than the per-tenant DB.
func (b *Dat9Backend) UseServerQuota() bool {
	return b.quotaSource == QuotaSourceServer && b.metaStore != nil
}

// --- Feature-flag dispatched quota checks ---

// ensureStorageQuota dispatches the storage quota check based on quotaSource.
// For uploads: when server quota is active, the server-first saga (reserveUploadOnServer)
// already claimed the reservation, so the tenant-DB check is skipped.
// For small writes: delegates to ensureStorageQuotaServer or ensureTenantStorageQuotaTx.
func (b *Dat9Backend) ensureStorageQuota(ctx context.Context, tx *sql.Tx, path string, newSize int64) error {
	if b.UseServerQuota() {
		// Server quota tracks total storage_bytes. For overwrites we need the
		// delta (newSize - currentSize) not the full newSize, otherwise the
		// check double-charges the existing file's bytes.
		currentSize, err := b.store.ConfirmedFileSizeByPathTx(tx, path)
		if err != nil {
			return fmt.Errorf("load current file size: %w", err)
		}
		deltaBytes := newSize - currentSize
		if deltaBytes <= 0 {
			return nil // shrinking or same size — no additional quota needed
		}
		return b.ensureStorageQuotaServer(ctx, tx, deltaBytes)
	}
	return b.ensureTenantStorageQuotaTx(tx, path, newSize)
}

// mediaLLMQuotaExceededCheck dispatches the media LLM quota check based on quotaSource.
func (b *Dat9Backend) mediaLLMQuotaExceededCheck(ctx context.Context) bool {
	if b.UseServerQuota() {
		return b.mediaLLMQuotaExceededServer(ctx)
	}
	return b.mediaLLMQuotaExceeded()
}

// mediaLLMQuotaExceededCheckTx dispatches the media LLM quota check (transactional variant).
func (b *Dat9Backend) mediaLLMQuotaExceededCheckTx(ctx context.Context, tx *sql.Tx) bool {
	if b.UseServerQuota() {
		return b.mediaLLMQuotaExceededServerTx(ctx, tx)
	}
	return b.mediaLLMQuotaExceededTx(tx)
}

// monthlyLLMCostExceededCheck dispatches the monthly LLM cost check based on quotaSource.
func (b *Dat9Backend) monthlyLLMCostExceededCheck(ctx context.Context) bool {
	if b.UseServerQuota() {
		return b.monthlyLLMCostExceededServer(ctx)
	}
	return b.monthlyLLMCostExceeded()
}

// monthlyLLMCostExceededServer checks the server DB monthly cost counter
// against the per-tenant config (falling back to the global default).
func (b *Dat9Backend) monthlyLLMCostExceededServer(ctx context.Context) bool {
	if b.metaStore == nil {
		return false
	}
	start := time.Now()
	total, err := b.metaStore.MonthlyLLMCostMillicents(ctx, b.tenantID)
	if err != nil {
		logger.Warn(ctx, "server_llm_cost_check_fail_open",
			zap.String("tenant_id", b.tenantID), zap.Error(err))
		metrics.RecordOperation("server_quota", "llm_cost_check", "fail_open", time.Since(start))
		return false // fail-open
	}
	// Use per-tenant config if available, otherwise fall back to global default.
	limit := b.maxMonthlyLLMCostMillicents
	cfg := b.cachedQuotaConfig(ctx)
	if cfg != nil && cfg.MaxMonthlyCostMC > 0 {
		limit = cfg.MaxMonthlyCostMC
	}
	if limit <= 0 {
		return false // no limit configured (global or per-tenant)
	}
	return total > limit
}

func (b *Dat9Backend) ensureUploadSizeAllowed(size int64) error {
	if size <= 0 || b.maxUploadBytes <= 0 {
		return nil
	}
	if size > b.maxUploadBytes {
		return fmt.Errorf("%w: max %d bytes", ErrUploadTooLarge, b.maxUploadBytes)
	}
	return nil
}

// --- Server-side storage quota (Rev 4 migration) ---

// ensureStorageQuotaServer performs an optimistic storage quota check against
// the server DB. Used for small writes (create, overwrite, patch) where the
// server-reserve-first protocol would add unnecessary latency.
// Falls back to the tenant-DB check when metaStore is not wired.
//
// Accepted race (Rev 4 / PR #251 review): this check is optimistic and can
// undercount when concurrent small overwrites on the same file race between
// read (GetQuotaUsage) and the paired mutation log entry that adjusts
// storage_bytes. The window is bounded by the small-write path (not
// multipart uploads, which use the server-reserve-first saga with a single
// atomic reserve+insert transaction — see upload_reservation.go). Reviewers
// @adversary / @adversary-1 accepted this tradeoff in thread #pr251:000001d1
// rather than paying the latency of a server-side reserve for every small
// write. Hard quota convergence is restored by MutationReplayWorker
// and the backfill-quota CLI tool.
func (b *Dat9Backend) ensureStorageQuotaServer(ctx context.Context, tx *sql.Tx, deltaBytes int64) error {
	if b.metaStore == nil || deltaBytes <= 0 {
		return nil // fail-open or no-op
	}

	cfg := b.cachedQuotaConfig(ctx)
	if cfg == nil {
		metrics.RecordOperation("server_quota", "storage_check", "fail_open", 0)
		return nil // fail-open: config unavailable
	}
	if cfg.MaxStorageBytes <= 0 {
		return nil // quota not configured
	}
	usage := b.loadQuotaUsage(ctx)
	if usage == nil {
		metrics.RecordOperation("server_quota", "storage_check", "fail_open", 0)
		return nil // fail-open: usage unavailable
	}
	recordTenantQuotaSnapshot(b.tenantID, usage, cfg)
	pendingStorageDelta, _, pendingOK := b.pendingQuotaOutboxDeltasTx(ctx, tx)
	if !pendingOK {
		metrics.RecordOperation("server_quota", "storage_check_pending_delta", "fail_open", 0)
	}
	projected := usage.StorageBytes + usage.ReservedBytes + pendingStorageDelta + deltaBytes
	if projected > cfg.MaxStorageBytes {
		metrics.RecordOperation("server_quota", "storage_check", "exceeded", 0)
		return fmt.Errorf("%w: server limit=%d used=%d reserved=%d pending=%d delta=%d",
			ErrStorageQuotaExceeded, cfg.MaxStorageBytes, usage.StorageBytes, usage.ReservedBytes, pendingStorageDelta, deltaBytes)
	}
	metrics.RecordOperation("server_quota", "storage_check", "ok", 0)
	return nil
}

// cachedQuotaConfig returns low-churn quota config from the per-tenant cache,
// falling back to a synchronous DB query when the cache is unavailable.
func (b *Dat9Backend) cachedQuotaConfig(ctx context.Context) *QuotaConfigView {
	if b.quotaConfigCache != nil {
		if cfg := b.quotaConfigCache.get(); cfg != nil {
			return cfg
		}
	}
	cfg, err := b.metaStore.GetQuotaConfig(ctx, b.tenantID)
	if err != nil {
		logger.Warn(ctx, "server_quota_config_fail_open",
			zap.String("tenant_id", b.tenantID), zap.Error(err))
		return nil
	}
	return cfg
}

// loadQuotaUsage reads high-churn quota counters directly from the central DB.
// Usage is not served from a per-process cache because multi-server deployments
// would otherwise widen the stale window for quota checks.
func (b *Dat9Backend) loadQuotaUsage(ctx context.Context) *QuotaUsageView {
	usage, err := b.metaStore.GetQuotaUsage(ctx, b.tenantID)
	if err != nil {
		logger.Warn(ctx, "server_quota_usage_fail_open",
			zap.String("tenant_id", b.tenantID), zap.Error(err))
		return nil
	}
	return usage
}

// --- Server-side media file count (Rev 4 migration) ---

// mediaLLMQuotaExceededServer checks the server DB counter for media file quota.
// Falls back to tenant-DB check when metaStore is not wired.
func (b *Dat9Backend) mediaLLMQuotaExceededServer(ctx context.Context) bool {
	if b.metaStore == nil {
		return b.mediaLLMQuotaExceeded() // fallback to tenant DB
	}
	cfg := b.cachedQuotaConfig(ctx)
	if cfg == nil {
		metrics.RecordOperation("server_quota", "media_check", "fail_open", 0)
		return false // fail-open: config unavailable
	}
	if cfg.MaxMediaLLMFiles <= 0 {
		return false
	}
	usage := b.loadQuotaUsage(ctx)
	if usage == nil {
		metrics.RecordOperation("server_quota", "media_check", "fail_open", 0)
		return false // fail-open: usage unavailable
	}
	recordTenantQuotaSnapshot(b.tenantID, usage, cfg)
	_, pendingMediaDelta, pendingOK := b.pendingQuotaOutboxDeltas(ctx)
	if !pendingOK {
		metrics.RecordOperation("server_quota", "media_check_pending_delta", "fail_open", 0)
	}
	return usage.MediaFileCount+pendingMediaDelta > cfg.MaxMediaLLMFiles
}

func recordTenantQuotaSnapshot(tenantID string, usage *QuotaUsageView, cfg *QuotaConfigView) {
	if tenantID == "" || usage == nil {
		return
	}
	metrics.RecordTenantStorageBytes(tenantID, "confirmed", usage.StorageBytes)
	metrics.RecordTenantStorageBytes(tenantID, "reserved", usage.ReservedBytes)
	metrics.RecordTenantMediaFiles(tenantID, "confirmed", usage.MediaFileCount)
	if cfg == nil {
		return
	}
	if cfg.MaxStorageBytes > 0 {
		metrics.RecordTenantStorageBytes(tenantID, "limit", cfg.MaxStorageBytes)
	}
	if cfg.MaxMediaLLMFiles > 0 {
		metrics.RecordTenantMediaFiles(tenantID, "limit", cfg.MaxMediaLLMFiles)
	}
}

// mediaLLMQuotaExceededServerTx checks the server DB counter for media file
// quota inside a transactional context. Falls back to tenant-DB when metaStore
// is not wired.
func (b *Dat9Backend) mediaLLMQuotaExceededServerTx(ctx context.Context, tx *sql.Tx) bool {
	if b.metaStore == nil {
		return b.mediaLLMQuotaExceededTx(tx) // fallback to tenant DB
	}
	cfg := b.cachedQuotaConfig(ctx)
	if cfg == nil {
		metrics.RecordOperation("server_quota", "media_check", "fail_open", 0)
		return false
	}
	if cfg.MaxMediaLLMFiles <= 0 {
		return false
	}
	usage := b.loadQuotaUsage(ctx)
	if usage == nil {
		metrics.RecordOperation("server_quota", "media_check", "fail_open", 0)
		return false
	}
	recordTenantQuotaSnapshot(b.tenantID, usage, cfg)
	_, pendingMediaDelta, pendingOK := b.pendingQuotaOutboxDeltasTx(ctx, tx)
	if !pendingOK {
		metrics.RecordOperation("server_quota", "media_check_pending_delta", "fail_open", 0)
	}
	return usage.MediaFileCount+pendingMediaDelta > cfg.MaxMediaLLMFiles
}

func (b *Dat9Backend) pendingQuotaOutboxDeltasTx(ctx context.Context, tx *sql.Tx) (storageDelta, mediaDelta int64, ok bool) {
	if b.store == nil || tx == nil {
		return 0, 0, true
	}
	storageDelta, mediaDelta, err := b.store.PendingQuotaOutboxDeltasTx(tx)
	if err != nil {
		logger.Warn(ctx, "server_quota_pending_outbox_delta_fail_open",
			zap.String("tenant_id", b.tenantID), zap.Error(err))
		return 0, 0, false
	}
	return storageDelta, mediaDelta, true
}

func (b *Dat9Backend) pendingQuotaOutboxDeltas(ctx context.Context) (storageDelta, mediaDelta int64, ok bool) {
	if b.store == nil {
		return 0, 0, true
	}
	storageDelta, mediaDelta, err := b.store.PendingQuotaOutboxDeltas(ctx)
	if err != nil {
		logger.Warn(ctx, "server_quota_pending_outbox_delta_fail_open",
			zap.String("tenant_id", b.tenantID), zap.Error(err))
		return 0, 0, false
	}
	return storageDelta, mediaDelta, true
}

// --- Tenant-DB quota checks (legacy fallback) ---

// mediaLLMQuotaExceededTx checks whether the tenant has exceeded its media LLM
// file quota inside a transaction. Returns true when the count of confirmed
// image+audio files strictly exceeds the configured limit. Using ">" (not ">=")
// is deliberate: the current file may already be counted (new inserts are
// CONFIRMED before enqueue in the same Tx), so ">" ensures the Nth file is
// still allowed and overwrites of existing media files are never blocked.
func (b *Dat9Backend) mediaLLMQuotaExceededTx(tx *sql.Tx) bool {
	if b.maxMediaLLMFiles <= 0 {
		return false
	}
	count, err := b.store.ConfirmedMediaFileCountTx(tx)
	if err != nil {
		logger.Warn(backgroundWithTrace(), "media_llm_quota_check_fail_open", zap.Error(err))
		metrics.RecordOperation("media_llm_budget", "quota_check", "fail_open", 0)
		return false
	}
	return count > b.maxMediaLLMFiles
}

// mediaLLMQuotaExceeded is the non-transactional variant for code paths that
// enqueue LLM tasks outside a database transaction (e.g. the legacy in-memory
// image extract queue).
func (b *Dat9Backend) mediaLLMQuotaExceeded() bool {
	if b.maxMediaLLMFiles <= 0 {
		return false
	}
	count, err := b.store.ConfirmedMediaFileCountTx(b.store.DB())
	if err != nil {
		logger.Warn(backgroundWithTrace(), "media_llm_quota_check_fail_open", zap.Error(err))
		metrics.RecordOperation("media_llm_budget", "quota_check", "fail_open", 0)
		return false
	}
	return count > b.maxMediaLLMFiles
}

// ensureTenantStorageQuotaTx is the legacy tenant-DB storage quota check.
// Used as fallback when metaStore is not wired.
func (b *Dat9Backend) ensureTenantStorageQuotaTx(tx *sql.Tx, path string, newSize int64) error {
	if newSize <= 0 || b.maxTenantStorageBytes <= 0 {
		return nil
	}
	confirmedBytes, err := b.store.ConfirmedStorageBytesTx(tx)
	if err != nil {
		return fmt.Errorf("load confirmed storage usage: %w", err)
	}
	reservedBytes, err := b.store.ActiveUploadReservedBytesTx(tx)
	if err != nil {
		return fmt.Errorf("load upload reservations: %w", err)
	}
	currentPathBytes, err := b.store.ConfirmedFileSizeByPathTx(tx, path)
	if err != nil {
		return fmt.Errorf("load current file size: %w", err)
	}
	deltaBytes := newSize - currentPathBytes
	if deltaBytes < 0 {
		deltaBytes = 0
	}
	totalBytes := confirmedBytes + reservedBytes + deltaBytes
	if totalBytes > b.maxTenantStorageBytes {
		return fmt.Errorf("%w: limit=%d used=%d reserved=%d current_path=%d requested=%d delta=%d",
			ErrStorageQuotaExceeded, b.maxTenantStorageBytes, confirmedBytes, reservedBytes,
			currentPathBytes, newSize, deltaBytes)
	}
	return nil
}
