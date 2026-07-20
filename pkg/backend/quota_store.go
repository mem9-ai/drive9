package backend

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
)

const defaultTenantMetricTiDBCloudOrgID = "guest"

// MetaQuotaStore defines the interface for central quota operations on the
// drive9 server DB. Implemented by *meta.Store; injected via Pool to avoid
// a circular dependency between backend and meta packages.
type MetaQuotaStore interface {
	// Config
	GetQuotaConfig(ctx context.Context, tenantID string) (*QuotaConfigView, error)
	GetQuotaConfigVersion(ctx context.Context, tenantID string) (string, error)

	// Counters
	GetQuotaUsage(ctx context.Context, tenantID string) (*QuotaUsageView, error)
	EnsureQuotaUsageRow(ctx context.Context, tenantID string) error
	IncrStorageBytes(ctx context.Context, tenantID string, delta int64) error
	IncrReservedBytes(ctx context.Context, tenantID string, delta int64) error
	IncrFileCount(ctx context.Context, tenantID string, delta int64) error
	IncrMediaFileCount(ctx context.Context, tenantID string, delta int64) error
	TransferReservedToConfirmed(ctx context.Context, tenantID string, reservedDelta, storageDelta int64) error
	AtomicReserveAndInsertUpload(ctx context.Context, r *UploadReservationView) error
	IncrStorageBytesTx(tx *sql.Tx, tenantID string, delta int64) error
	IncrReservedBytesTx(tx *sql.Tx, tenantID string, delta int64) error
	IncrFileCountTx(tx *sql.Tx, tenantID string, delta int64) error
	IncrMediaFileCountTx(tx *sql.Tx, tenantID string, delta int64) error
	TransferReservedToConfirmedTx(tx *sql.Tx, tenantID string, reservedDelta, storageDelta int64) error

	// File meta (server-authored shadow state)
	UpsertFileMeta(ctx context.Context, fm *FileMetaView) error
	GetFileMeta(ctx context.Context, tenantID, fileID string) (*FileMetaView, error)
	GetFileMetaForUpdateTx(tx *sql.Tx, tenantID, fileID string) (*FileMetaView, error)
	DeleteFileMeta(ctx context.Context, tenantID, fileID string) error
	UpsertFileMetaTx(tx *sql.Tx, fm *FileMetaView) error
	DeleteFileMetaTx(tx *sql.Tx, tenantID, fileID string) error
	DeleteFileMetaIfExistsTx(tx *sql.Tx, tenantID, fileID string) (bool, error)

	// Upload reservations
	InsertUploadReservation(ctx context.Context, r *UploadReservationView) error
	UpdateUploadReservationStatus(ctx context.Context, tenantID, uploadID, status string) error
	GetUploadReservation(ctx context.Context, tenantID, uploadID string) (*UploadReservationView, error)
	UpdateUploadReservationStatusTx(ctx context.Context, tx *sql.Tx, tenantID, uploadID, status string) error
	AbortActiveReservationTx(ctx context.Context, tx *sql.Tx, tenantID, uploadID string) (aborted bool, reservedBytes, fileCountDelta int64, err error)
	SettleActiveReservationTx(ctx context.Context, tx *sql.Tx, tenantID, uploadID, status string) (settled bool, fileCountDelta int64, err error)

	// LLM cost
	InsertCentralLLMUsage(ctx context.Context, r *LLMUsageView) error
	IncrMonthlyLLMCost(ctx context.Context, tenantID string, costMC int64) error
	MonthlyLLMCostMillicents(ctx context.Context, tenantID string) (int64, error)
	InsertCentralLLMUsageTx(tx *sql.Tx, r *LLMUsageView) error
	IncrMonthlyLLMCostTx(tx *sql.Tx, tenantID string, costMC int64) error

	// Mutation log
	InsertMutationLog(ctx context.Context, entry *MutationLogView) (int64, error)
	ListPendingMutations(ctx context.Context, minAge time.Duration, limit int) ([]MutationLogView, error)
	ObservePendingMutations(ctx context.Context) ([]MutationBacklogView, error)
	HasPendingFileMutation(ctx context.Context, tenantID, fileID string) (bool, error)
	MarkMutationAppliedTx(tx *sql.Tx, id int64) error
	IsMutationAlreadyAppliedError(err error) bool
	IncrMutationRetry(ctx context.Context, id int64, maxRetries int) error

	// Transaction support
	InTx(ctx context.Context, fn func(tx *sql.Tx) error) error
}

// QuotaConfigView is the backend-side view of per-tenant quota configuration.
type QuotaConfigView struct {
	TenantID         string
	MaxStorageBytes  int64
	MaxFileSizeBytes int64
	MaxFileCount     int64
	MaxMediaLLMFiles int64
	MaxMonthlyCostMC int64
}

// QuotaUsageView is the backend-side view of pre-aggregated quota counters.
type QuotaUsageView struct {
	TenantID       string
	StorageBytes   int64
	ReservedBytes  int64
	FileCount      int64
	MediaFileCount int64
}

// FileMetaView is the backend-side view of per-file quota metadata.
type FileMetaView struct {
	TenantID  string
	FileID    string
	SizeBytes int64
	IsMedia   bool
}

// UploadReservationView is the backend-side view of an upload reservation.
type UploadReservationView struct {
	TenantID       string
	UploadID       string
	ReservedBytes  int64
	FileCountDelta int64
	TargetPath     string
	Status         string
	ExpiresAt      time.Time
}

// LLMUsageView is the backend-side view of a billable LLM call record.
type LLMUsageView struct {
	TenantID       string
	TaskType       string
	TaskID         string
	CostMillicents int64
	RawUnits       int64
	RawUnitType    string
}

// MutationLogView is the backend-side view of a quota mutation log entry.
type MutationLogView struct {
	ID             int64 // populated only when read from ListPendingMutations
	TenantID       string
	TiDBCloudOrgID string
	MutationType   string
	MutationData   json.RawMessage
	RetryCount     int // populated only when read from ListPendingMutations
}

// MutationBacklogView is a tenant-level observation of pending quota mutation
// replay work. It is used only for runtime metrics and alerting.
type MutationBacklogView struct {
	TenantID                string
	TiDBCloudOrgID          string
	PendingCount            int64
	OldestPendingAgeSeconds float64
}

// SetMetaQuotaStore sets the central quota store on the backend.
// Called by tenant.Pool after backend creation.
func (b *Dat9Backend) SetMetaQuotaStore(ctx context.Context, tenantID string, mqs MetaQuotaStore) {
	b.SetTenantMetricScope(tenantID, b.tidbCloudOrgID)
	b.metaStore = mqs
	if mqs != nil {
		if err := mqs.EnsureQuotaUsageRow(ctx, tenantID); err != nil {
			logger.Warn(ctx, "ensure_quota_usage_row_failed",
				zap.String("tenant_id", tenantID),
				zap.Error(err))
		}
	}
	if mqs != nil {
		b.quotaConfigCache = newQuotaConfigCache(tenantID, b.tidbCloudOrgID, mqs)
		b.quotaUsageCache = newQuotaUsageCache(tenantID, b.tidbCloudOrgID, mqs, quotaUsageCacheTTL)
		b.quotaPendingCache = newQuotaPendingDeltasCache(b.tenantID, b.tidbCloudOrgID, nil, quotaPendingDeltasCacheTTL)
		b.startMutationWorker()
	}
}

// SetTenantMetricScope sets the tenant dimensions used by backend-owned
// metrics. It does not perform tenant lookup; callers must pass the resolved
// TiDB Cloud org explicitly.
func (b *Dat9Backend) SetTenantMetricScope(tenantID, tidbCloudOrgID string) {
	if strings.TrimSpace(tenantID) != "" {
		b.tenantID = tenantID
	}
	b.tidbCloudOrgID = normalizeTenantMetricTiDBCloudOrgID(tidbCloudOrgID)
}

// TenantID returns the tenant identifier for this backend instance.
func (b *Dat9Backend) TenantID() string {
	return b.tenantID
}

// TiDBCloudOrgID returns the TiDB Cloud org label value for tenant metrics.
func (b *Dat9Backend) TiDBCloudOrgID() string {
	return normalizeTenantMetricTiDBCloudOrgID(b.tidbCloudOrgID)
}

func normalizeTenantMetricTiDBCloudOrgID(orgID string) string {
	orgID = strings.TrimSpace(orgID)
	if orgID == "" {
		return defaultTenantMetricTiDBCloudOrgID
	}
	return orgID
}

func (b *Dat9Backend) recordTenantOperation(component, operation, result string, d time.Duration) {
	metrics.RecordTenantOperationWithOrg(b.tenantID, b.tidbCloudOrgID, component, operation, result, d)
}
