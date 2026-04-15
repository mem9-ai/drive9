package backend

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/metrics"
	"go.uber.org/zap"
)

var (
	ErrUploadTooLarge        = errors.New("upload too large")
	ErrStorageQuotaExceeded  = errors.New("tenant storage quota exceeded")
	ErrMediaLLMQuotaExceeded = errors.New("tenant media LLM file quota exceeded")
)

func (b *Dat9Backend) ensureUploadSizeAllowed(size int64) error {
	if size <= 0 || b.maxUploadBytes <= 0 {
		return nil
	}
	if size > b.maxUploadBytes {
		return fmt.Errorf("%w: max %d bytes", ErrUploadTooLarge, b.maxUploadBytes)
	}
	return nil
}

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
