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
	ErrFileSizeQuotaExceeded    = errors.New("tenant file size quota exceeded")
	ErrFileCountQuotaExceeded   = errors.New("tenant file count quota exceeded")
	ErrStorageQuotaExceeded     = errors.New("tenant storage quota exceeded")
	ErrMediaLLMQuotaExceeded    = errors.New("tenant media LLM file quota exceeded")
	ErrReservationNotFound      = errors.New("upload reservation not found")
	ErrReservationAlreadyExists = errors.New("upload reservation already exists")
	ErrQuotaReservationBusy     = errors.New("quota reservation busy")
)

type storageQuotaCheckResult struct {
	checked       bool
	limitBytes    int64
	storageBytes  int64
	reservedBytes int64
	pendingBytes  int64
	deltaBytes    int64
	projected     int64
}

// UseServerQuota reports whether this backend has central quota wired. Runtime
// quota accounting now uses the meta DB only; the legacy tenant-DB quota path is
// retained for old tests/tools but is no longer selected by a source knob.
func (b *Dat9Backend) UseServerQuota() bool {
	return b.metaStore != nil
}

func (b *Dat9Backend) ensureStorageQuota(ctx context.Context, tx *sql.Tx, path string, newSize int64) error {
	if !b.UseServerQuota() {
		return b.ensureTenantStorageQuotaTx(tx, path, newSize)
	}
	// Central quota tracks total storage_bytes. For overwrites we need the
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

// ensureCreateStorageQuota is the create-only variant of ensureStorageQuota.
// The caller has not attached a dentry yet, so the current path size is known
// to be zero; path conflicts later in the transaction roll back the write.
func (b *Dat9Backend) ensureCreateStorageQuota(ctx context.Context, tx *sql.Tx, newSize int64) error {
	if !b.UseServerQuota() {
		return b.ensureTenantCreateStorageQuotaTx(tx, newSize)
	}
	return b.ensureStorageQuotaServer(ctx, tx, newSize)
}

func (b *Dat9Backend) mediaLLMQuotaExceededCheckTx(ctx context.Context, tx *sql.Tx, currentMediaDelta int64) bool {
	if !b.UseServerQuota() {
		return b.mediaLLMQuotaExceededTx(tx)
	}
	return b.mediaLLMQuotaExceededServerTx(ctx, tx, currentMediaDelta)
}

func (b *Dat9Backend) monthlyLLMCostExceededCheck(ctx context.Context) bool {
	if !b.UseServerQuota() {
		return b.monthlyLLMCostExceeded()
	}
	return b.monthlyLLMCostExceededServer(ctx)
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
		b.recordTenantOperation("server_quota", "llm_cost_check", "fail_open", time.Since(start))
		return false // fail-open
	}
	// Use per-tenant config if available, otherwise fall back to global default.
	limit := b.maxMonthlyLLMCostMillicents
	if cfg := b.cachedQuotaConfig(ctx); cfg != nil && cfg.MaxMonthlyCostMC > 0 {
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

func (b *Dat9Backend) ensureFileSizeQuota(ctx context.Context, size int64) error {
	if size <= 0 || !b.UseServerQuota() {
		return nil
	}
	cfg := b.cachedQuotaConfig(ctx)
	if cfg == nil {
		b.recordTenantOperation("server_quota", "file_size_check", "fail_open", 0)
		return nil
	}
	if cfg.MaxFileSizeBytes <= 0 {
		return nil
	}
	if size > cfg.MaxFileSizeBytes {
		b.recordTenantOperation("server_quota", "file_size_check", "exceeded", 0)
		return fmt.Errorf("%w: server limit=%d requested=%d", ErrFileSizeQuotaExceeded, cfg.MaxFileSizeBytes, size)
	}
	b.recordTenantOperation("server_quota", "file_size_check", "ok", 0)
	return nil
}

// ensureStorageQuotaServer performs a soft optimistic storage quota check
// against the server DB. Used for small writes (create, overwrite, patch) where
// a server-reserve-first protocol or tenant-wide admission lock would add
// latency to the single-tenant write hot path.
//
// The check includes central confirmed/reserved usage plus this process's
// in-memory pending central mutations. It intentionally does not read the tenant
// DB; multi-pod small writes may briefly over-admit until meta mutation replay
// converges. Multipart uploads use the meta-DB reserve-first path in
// upload_reservation.go.
func (b *Dat9Backend) ensureStorageQuotaServer(ctx context.Context, tx *sql.Tx, deltaBytes int64) error {
	result, ok := b.checkStorageQuotaServerTx(ctx, tx, deltaBytes)
	if !ok {
		return nil
	}
	if result.exceeded() {
		b.recordTenantOperation("server_quota", "storage_check", "exceeded", 0)
		return result.quotaExceededError()
	}
	b.recordTenantOperation("server_quota", "storage_check", "ok", 0)
	return nil
}

// ensureFileCountQuotaServer is a soft optimistic file-count check for small
// create-like writes. Like storage soft admission, concurrent servers may
// briefly over-admit within the quota usage/pending-delta cache TTL. Upload
// reservations use live meta counters without touching the tenant DB.
func (b *Dat9Backend) ensureFileCountQuotaServer(ctx context.Context, tx *sql.Tx, currentFileDelta int64) error {
	if currentFileDelta <= 0 || !b.UseServerQuota() {
		return nil
	}
	cfg := b.cachedQuotaConfig(ctx)
	if cfg == nil {
		b.recordTenantOperation("server_quota", "file_count_check", "fail_open", 0)
		return nil
	}
	if cfg.MaxFileCount <= 0 {
		return nil
	}
	usage := b.cachedQuotaUsage(ctx)
	if usage == nil {
		b.recordTenantOperation("server_quota", "file_count_check", "fail_open", 0)
		return nil
	}
	recordTenantQuotaSnapshot(b.tenantID, b.tidbCloudOrgID, usage, cfg)
	_, pendingFileDelta, _ := b.pendingCentralMutationDeltas(ctx)
	projected := usage.FileCount + pendingFileDelta + currentFileDelta
	if projected > cfg.MaxFileCount {
		b.recordTenantOperation("server_quota", "file_count_check", "exceeded", 0)
		return fmt.Errorf("%w: server limit=%d used=%d pending=%d delta=%d",
			ErrFileCountQuotaExceeded, cfg.MaxFileCount, usage.FileCount, pendingFileDelta, currentFileDelta)
	}
	b.recordTenantOperation("server_quota", "file_count_check", "ok", 0)
	return nil
}

func (b *Dat9Backend) checkStorageQuotaServerTx(ctx context.Context, tx *sql.Tx, deltaBytes int64) (storageQuotaCheckResult, bool) {
	result := storageQuotaCheckResult{deltaBytes: deltaBytes}
	if b.metaStore == nil || deltaBytes <= 0 {
		return result, false
	}

	cfg := b.cachedQuotaConfig(ctx)
	if cfg == nil {
		b.recordTenantOperation("server_quota", "storage_check", "fail_open", 0)
		return result, false // fail-open: config unavailable
	}
	if cfg.MaxStorageBytes <= 0 {
		return result, false // quota not configured
	}
	usage := b.cachedQuotaUsage(ctx)
	if usage == nil {
		b.recordTenantOperation("server_quota", "storage_check", "fail_open", 0)
		return result, false // fail-open: usage unavailable
	}
	recordTenantQuotaSnapshot(b.tenantID, b.tidbCloudOrgID, usage, cfg)
	pendingStorageDelta, _, _ := b.pendingCentralMutationDeltas(ctx)
	result.checked = true
	result.limitBytes = cfg.MaxStorageBytes
	result.storageBytes = usage.StorageBytes
	result.reservedBytes = usage.ReservedBytes
	result.pendingBytes = pendingStorageDelta
	result.projected = usage.StorageBytes + usage.ReservedBytes + pendingStorageDelta + deltaBytes
	return result, true
}

// cachedQuotaConfig returns low-churn quota config from the per-tenant cache,
// falling back to a synchronous DB query when the cache is unavailable.
func (b *Dat9Backend) cachedQuotaConfig(ctx context.Context) *QuotaConfigView {
	if b.quotaConfigCache != nil {
		if cfg := b.quotaConfigCache.get(); cfg != nil {
			return cfg
		}
		return b.quotaConfigCache.load(ctx)
	}
	if b.metaStore == nil {
		return nil
	}
	cfg, err := b.metaStore.GetQuotaConfig(ctx, b.tenantID)
	if err != nil {
		logger.Warn(ctx, "server_quota_config_fail_open",
			zap.String("tenant_id", b.tenantID), zap.Error(err))
		return nil
	}
	return cfg
}

// loadQuotaUsage reads quota counters directly from the central DB. Use this
// for strict quota paths such as upload reservations; small-write soft checks
// use cachedQuotaUsage to avoid a central read on every write.
func (b *Dat9Backend) loadQuotaUsage(ctx context.Context) *QuotaUsageView {
	usage, err := b.metaStore.GetQuotaUsage(ctx, b.tenantID)
	if err != nil {
		logger.Warn(ctx, "server_quota_usage_fail_open",
			zap.String("tenant_id", b.tenantID), zap.Error(err))
		return nil
	}
	return usage
}

// cachedQuotaUsage reads central usage through a short TTL cache. It is only
// used by soft small-write checks; strict upload reservation paths call
// loadQuotaUsage directly.
func (b *Dat9Backend) cachedQuotaUsage(ctx context.Context) *QuotaUsageView {
	if b.quotaUsageCache != nil {
		return b.quotaUsageCache.get(ctx)
	}
	return b.loadQuotaUsage(ctx)
}

// --- Server-side media file count (Rev 4 migration) ---

func recordTenantQuotaSnapshot(tenantID, tidbCloudOrgID string, usage *QuotaUsageView, cfg *QuotaConfigView) {
	if tenantID == "" || usage == nil {
		return
	}
	metrics.RecordTenantStorageBytesWithOrg(tenantID, tidbCloudOrgID, "confirmed", usage.StorageBytes)
	metrics.RecordTenantStorageBytesWithOrg(tenantID, tidbCloudOrgID, "reserved", usage.ReservedBytes)
	metrics.RecordTenantMediaFilesWithOrg(tenantID, tidbCloudOrgID, "confirmed", usage.MediaFileCount)
	if cfg == nil {
		return
	}
	if cfg.MaxStorageBytes > 0 {
		metrics.RecordTenantStorageBytesWithOrg(tenantID, tidbCloudOrgID, "limit", cfg.MaxStorageBytes)
	}
	if cfg.MaxMediaLLMFiles > 0 {
		metrics.RecordTenantMediaFilesWithOrg(tenantID, tidbCloudOrgID, "limit", cfg.MaxMediaLLMFiles)
	}
}

func quotaMediaDelta(oldIsMedia, newIsMedia bool) int64 {
	switch {
	case !oldIsMedia && newIsMedia:
		return 1
	case oldIsMedia && !newIsMedia:
		return -1
	default:
		return 0
	}
}

// mediaLLMQuotaExceededServerTx checks the server DB counter for media file
// quota inside a transactional context. Like storage quota for small writes,
// this is a soft check and does not take the tenant-wide quota admission lock.
func (b *Dat9Backend) mediaLLMQuotaExceededServerTx(ctx context.Context, tx *sql.Tx, currentMediaDelta int64) bool {
	cfg := b.cachedQuotaConfig(ctx)
	if cfg == nil {
		b.recordTenantOperation("server_quota", "media_check", "fail_open", 0)
		return false
	}
	if cfg.MaxMediaLLMFiles <= 0 {
		return false
	}
	usage := b.cachedQuotaUsage(ctx)
	if usage == nil {
		b.recordTenantOperation("server_quota", "media_check", "fail_open", 0)
		return false
	}
	recordTenantQuotaSnapshot(b.tenantID, b.tidbCloudOrgID, usage, cfg)
	_, _, pendingMediaDelta := b.pendingCentralMutationDeltas(ctx)
	return usage.MediaFileCount+pendingMediaDelta+currentMediaDelta > cfg.MaxMediaLLMFiles
}

func (b *Dat9Backend) pendingCentralMutationDeltas(ctx context.Context) (storageDelta, fileDelta, mediaDelta int64) {
	if b.quotaPendingCache == nil {
		return 0, 0, 0
	}
	deltas, ok := b.quotaPendingCache.get(ctx)
	if !ok {
		return 0, 0, 0
	}
	return deltas.storageDelta, deltas.fileDelta, deltas.mediaDelta
}

func (b *Dat9Backend) addPendingCentralMutationDeltas(storageDelta, fileDelta, mediaDelta int64) {
	if b.quotaPendingCache != nil {
		b.quotaPendingCache.addPending(storageDelta, fileDelta, mediaDelta)
	}
}

func (b *Dat9Backend) clearPendingCentralMutationDeltas(storageDelta, fileDelta, mediaDelta int64) {
	if b.quotaPendingCache != nil {
		b.quotaPendingCache.clearPending(storageDelta, fileDelta, mediaDelta)
	}
}

func (r storageQuotaCheckResult) exceeded() bool {
	return r.checked && r.projected > r.limitBytes
}

func (r storageQuotaCheckResult) quotaExceededError() error {
	return fmt.Errorf("%w: server limit=%d used=%d reserved=%d pending=%d delta=%d",
		ErrStorageQuotaExceeded, r.limitBytes, r.storageBytes, r.reservedBytes, r.pendingBytes, r.deltaBytes)
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
		b.recordTenantOperation("media_llm_budget", "quota_check", "fail_open", 0)
		return false
	}
	return count > b.maxMediaLLMFiles
}

// videoLLMQuotaExceededTx checks whether the tenant has exceeded its per-tenant
// video extraction quota. Counts every video_extract_visual task row (any
// status) — each Vision API call consumes one unit, including re-extractions
// of the same file.
func (b *Dat9Backend) videoLLMQuotaExceededTx(tx *sql.Tx) bool {
	if b.maxVideoLLMFiles <= 0 {
		return false
	}
	var count int64
	if err := tx.QueryRow(`SELECT COUNT(*) FROM semantic_tasks WHERE task_type = 'video_extract_visual'`).Scan(&count); err != nil {
		logger.Warn(backgroundWithTrace(), "video_llm_quota_check_fail_open", zap.Error(err))
		return false
	}
	return count >= b.maxVideoLLMFiles
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

func (b *Dat9Backend) ensureTenantCreateStorageQuotaTx(tx *sql.Tx, newSize int64) error {
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
	totalBytes := confirmedBytes + reservedBytes + newSize
	if totalBytes > b.maxTenantStorageBytes {
		return fmt.Errorf("%w: limit=%d used=%d reserved=%d requested=%d delta=%d",
			ErrStorageQuotaExceeded, b.maxTenantStorageBytes, confirmedBytes, reservedBytes, newSize, newSize)
	}
	return nil
}
