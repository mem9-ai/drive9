package tenant

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/meta"
)

// metaQuotaAdapter wraps *meta.Store to satisfy backend.MetaQuotaStore.
// This adapter bridges the meta package types to the backend view types,
// keeping the dependency direction: tenant → {backend, meta}.
type metaQuotaAdapter struct {
	s *meta.Store
}

// NewMetaQuotaAdapter wraps a *meta.Store to satisfy backend.MetaQuotaStore.
func NewMetaQuotaAdapter(s *meta.Store) backend.MetaQuotaStore {
	if s == nil {
		return nil
	}
	return &metaQuotaAdapter{s: s}
}

func (a *metaQuotaAdapter) GetQuotaConfig(ctx context.Context, tenantID string) (*backend.QuotaConfigView, error) {
	cfg, err := a.s.GetQuotaConfig(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	return &backend.QuotaConfigView{
		TenantID:         cfg.TenantID,
		MaxStorageBytes:  cfg.MaxStorageBytes,
		MaxMediaLLMFiles: cfg.MaxMediaLLMFiles,
		MaxMonthlyCostMC: cfg.MaxMonthlyCostMC,
	}, nil
}

func (a *metaQuotaAdapter) GetQuotaUsage(ctx context.Context, tenantID string) (*backend.QuotaUsageView, error) {
	u, err := a.s.GetQuotaUsage(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	return &backend.QuotaUsageView{
		TenantID:       u.TenantID,
		StorageBytes:   u.StorageBytes,
		ReservedBytes:  u.ReservedBytes,
		MediaFileCount: u.MediaFileCount,
	}, nil
}

func (a *metaQuotaAdapter) EnsureQuotaUsageRow(ctx context.Context, tenantID string) error {
	return a.s.EnsureQuotaUsageRow(ctx, tenantID)
}

func (a *metaQuotaAdapter) IncrStorageBytes(ctx context.Context, tenantID string, delta int64) error {
	return a.s.IncrStorageBytes(ctx, tenantID, delta)
}

func (a *metaQuotaAdapter) IncrStorageBytesTx(tx *sql.Tx, tenantID string, delta int64) error {
	return a.s.IncrStorageBytesTx(tx, tenantID, delta)
}

func (a *metaQuotaAdapter) IncrReservedBytes(ctx context.Context, tenantID string, delta int64) error {
	return a.s.IncrReservedBytes(ctx, tenantID, delta)
}

func (a *metaQuotaAdapter) IncrReservedBytesTx(tx *sql.Tx, tenantID string, delta int64) error {
	return a.s.IncrReservedBytesTx(tx, tenantID, delta)
}

func (a *metaQuotaAdapter) IncrMediaFileCount(ctx context.Context, tenantID string, delta int64) error {
	return a.s.IncrMediaFileCount(ctx, tenantID, delta)
}

func (a *metaQuotaAdapter) IncrMediaFileCountTx(tx *sql.Tx, tenantID string, delta int64) error {
	return a.s.IncrMediaFileCountTx(tx, tenantID, delta)
}

func (a *metaQuotaAdapter) TransferReservedToConfirmed(ctx context.Context, tenantID string, reservedDelta, storageDelta int64) error {
	return a.s.TransferReservedToConfirmed(ctx, tenantID, reservedDelta, storageDelta)
}

func (a *metaQuotaAdapter) TransferReservedToConfirmedTx(tx *sql.Tx, tenantID string, reservedDelta, storageDelta int64) error {
	return a.s.TransferReservedToConfirmedTx(tx, tenantID, reservedDelta, storageDelta)
}

// AtomicReserveAndInsertUpload is the preferred single-transaction API for the
// upload-initiate path. See meta.Store.AtomicReserveAndInsertUpload for
// invariants. Translates meta sentinels to backend sentinels for the caller.
func (a *metaQuotaAdapter) AtomicReserveAndInsertUpload(ctx context.Context, r *backend.UploadReservationView) error {
	err := a.s.AtomicReserveAndInsertUpload(ctx, &meta.UploadReservation{
		TenantID:      r.TenantID,
		UploadID:      r.UploadID,
		ReservedBytes: r.ReservedBytes,
		TargetPath:    r.TargetPath,
		Status:        r.Status,
		ExpiresAt:     r.ExpiresAt,
	})
	switch {
	case errors.Is(err, meta.ErrStorageQuotaExceeded):
		return backend.ErrStorageQuotaExceeded
	case errors.Is(err, meta.ErrReservationAlreadyExists):
		return backend.ErrReservationAlreadyExists
	}
	return err
}

func (a *metaQuotaAdapter) UpsertFileMeta(ctx context.Context, fm *backend.FileMetaView) error {
	return a.s.UpsertFileMeta(ctx, &meta.FileMeta{
		TenantID:  fm.TenantID,
		FileID:    fm.FileID,
		SizeBytes: fm.SizeBytes,
		IsMedia:   fm.IsMedia,
	})
}

func (a *metaQuotaAdapter) GetFileMeta(ctx context.Context, tenantID, fileID string) (*backend.FileMetaView, error) {
	fm, err := a.s.GetFileMeta(ctx, tenantID, fileID)
	if err != nil {
		return nil, err
	}
	return &backend.FileMetaView{
		TenantID:  fm.TenantID,
		FileID:    fm.FileID,
		SizeBytes: fm.SizeBytes,
		IsMedia:   fm.IsMedia,
	}, nil
}

func (a *metaQuotaAdapter) UpsertFileMetaTx(tx *sql.Tx, fm *backend.FileMetaView) error {
	return a.s.UpsertFileMetaTx(tx, &meta.FileMeta{
		TenantID:  fm.TenantID,
		FileID:    fm.FileID,
		SizeBytes: fm.SizeBytes,
		IsMedia:   fm.IsMedia,
	})
}

func (a *metaQuotaAdapter) DeleteFileMeta(ctx context.Context, tenantID, fileID string) error {
	return a.s.DeleteFileMeta(ctx, tenantID, fileID)
}

func (a *metaQuotaAdapter) DeleteFileMetaTx(tx *sql.Tx, tenantID, fileID string) error {
	return a.s.DeleteFileMetaTx(tx, tenantID, fileID)
}

func (a *metaQuotaAdapter) InsertUploadReservation(ctx context.Context, r *backend.UploadReservationView) error {
	return a.s.InsertUploadReservation(ctx, &meta.UploadReservation{
		TenantID:      r.TenantID,
		UploadID:      r.UploadID,
		ReservedBytes: r.ReservedBytes,
		TargetPath:    r.TargetPath,
		Status:        r.Status,
		ExpiresAt:     r.ExpiresAt,
	})
}

func (a *metaQuotaAdapter) UpdateUploadReservationStatus(ctx context.Context, tenantID, uploadID, status string) error {
	return a.s.UpdateUploadReservationStatus(ctx, tenantID, uploadID, status)
}

func (a *metaQuotaAdapter) UpdateUploadReservationStatusTx(tx *sql.Tx, tenantID, uploadID, status string) error {
	return a.s.UpdateUploadReservationStatusTx(tx, tenantID, uploadID, status)
}

func (a *metaQuotaAdapter) SettleActiveReservationTx(tx *sql.Tx, tenantID, uploadID, status string) (bool, error) {
	return a.s.SettleActiveReservationTx(tx, tenantID, uploadID, status)
}

func (a *metaQuotaAdapter) GetUploadReservation(ctx context.Context, tenantID, uploadID string) (*backend.UploadReservationView, error) {
	r, err := a.s.GetUploadReservation(ctx, tenantID, uploadID)
	if errors.Is(err, meta.ErrNotFound) {
		return nil, backend.ErrReservationNotFound
	}
	if err != nil {
		return nil, err
	}
	return &backend.UploadReservationView{
		TenantID:      r.TenantID,
		UploadID:      r.UploadID,
		ReservedBytes: r.ReservedBytes,
		TargetPath:    r.TargetPath,
		Status:        r.Status,
		ExpiresAt:     r.ExpiresAt,
	}, nil
}

func (a *metaQuotaAdapter) InsertCentralLLMUsage(ctx context.Context, r *backend.LLMUsageView) error {
	return a.s.InsertCentralLLMUsage(ctx, &meta.LLMUsageRecord{
		TenantID:       r.TenantID,
		TaskType:       r.TaskType,
		TaskID:         r.TaskID,
		CostMillicents: r.CostMillicents,
		RawUnits:       r.RawUnits,
		RawUnitType:    r.RawUnitType,
	})
}

func (a *metaQuotaAdapter) InsertCentralLLMUsageTx(tx *sql.Tx, r *backend.LLMUsageView) error {
	return a.s.InsertCentralLLMUsageTx(tx, &meta.LLMUsageRecord{
		TenantID:       r.TenantID,
		TaskType:       r.TaskType,
		TaskID:         r.TaskID,
		CostMillicents: r.CostMillicents,
		RawUnits:       r.RawUnits,
		RawUnitType:    r.RawUnitType,
	})
}

func (a *metaQuotaAdapter) IncrMonthlyLLMCost(ctx context.Context, tenantID string, costMC int64) error {
	return a.s.IncrMonthlyLLMCost(ctx, tenantID, costMC)
}

func (a *metaQuotaAdapter) IncrMonthlyLLMCostTx(tx *sql.Tx, tenantID string, costMC int64) error {
	return a.s.IncrMonthlyLLMCostTx(tx, tenantID, costMC)
}

func (a *metaQuotaAdapter) MonthlyLLMCostMillicents(ctx context.Context, tenantID string) (int64, error) {
	return a.s.MonthlyLLMCostMillicents(ctx, tenantID)
}

func (a *metaQuotaAdapter) InsertMutationLog(ctx context.Context, entry *backend.MutationLogView) (int64, error) {
	return a.s.InsertMutationLog(ctx, &meta.MutationLogEntry{
		TenantID:     entry.TenantID,
		MutationType: entry.MutationType,
		MutationData: entry.MutationData,
	})
}

func (a *metaQuotaAdapter) ListPendingMutations(ctx context.Context, minAge time.Duration, limit int) ([]backend.MutationLogView, error) {
	entries, err := a.s.ListPendingMutations(ctx, minAge, limit)
	if err != nil {
		return nil, err
	}
	views := make([]backend.MutationLogView, len(entries))
	for i, e := range entries {
		views[i] = backend.MutationLogView{
			ID:           e.ID,
			TenantID:     e.TenantID,
			MutationType: e.MutationType,
			MutationData: e.MutationData,
			RetryCount:   e.RetryCount,
		}
	}
	return views, nil
}

func (a *metaQuotaAdapter) MarkMutationAppliedTx(tx *sql.Tx, id int64) error {
	return a.s.MarkMutationAppliedTx(tx, id)
}

func (a *metaQuotaAdapter) IncrMutationRetry(ctx context.Context, id int64, maxRetries int) error {
	return a.s.IncrMutationRetry(ctx, id, maxRetries)
}

func (a *metaQuotaAdapter) InTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	return a.s.InTx(ctx, fn)
}
