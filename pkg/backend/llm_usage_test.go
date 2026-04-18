package backend

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

// mockQuotaStore is a minimal test double for MetaQuotaStore that only
// implements MonthlyLLMCostMillicents. All other methods panic if called.
type mockQuotaStore struct {
	MetaQuotaStore // embed interface; unimplemented methods will nil-panic if called
	monthlyCost    int64
	monthlyCostErr error
}

func (m *mockQuotaStore) MonthlyLLMCostMillicents(_ context.Context, _ string) (int64, error) {
	return m.monthlyCost, m.monthlyCostErr
}

func (m *mockQuotaStore) GetQuotaConfig(_ context.Context, _ string) (*QuotaConfigView, error) {
	return &QuotaConfigView{}, nil
}

func (m *mockQuotaStore) GetQuotaUsage(_ context.Context, _ string) (*QuotaUsageView, error) {
	return &QuotaUsageView{}, nil
}

func (m *mockQuotaStore) EnsureQuotaUsageRow(_ context.Context, _ string) error { return nil }
func (m *mockQuotaStore) IncrStorageBytes(_ context.Context, _ string, _ int64) error { return nil }
func (m *mockQuotaStore) IncrReservedBytes(_ context.Context, _ string, _ int64) error { return nil }
func (m *mockQuotaStore) IncrMediaFileCount(_ context.Context, _ string, _ int64) error { return nil }
func (m *mockQuotaStore) TransferReservedToConfirmed(_ context.Context, _ string, _, _ int64) error {
	return nil
}
func (m *mockQuotaStore) AtomicReserveAndInsertUpload(_ context.Context, _ *UploadReservationView) error {
	return nil
}
func (m *mockQuotaStore) IncrStorageBytesTx(_ *sql.Tx, _ string, _ int64) error { return nil }
func (m *mockQuotaStore) IncrReservedBytesTx(_ *sql.Tx, _ string, _ int64) error { return nil }
func (m *mockQuotaStore) IncrMediaFileCountTx(_ *sql.Tx, _ string, _ int64) error { return nil }
func (m *mockQuotaStore) TransferReservedToConfirmedTx(_ *sql.Tx, _ string, _, _ int64) error {
	return nil
}

func (m *mockQuotaStore) UpsertFileMeta(_ context.Context, _ *FileMetaView) error      { return nil }
func (m *mockQuotaStore) GetFileMeta(_ context.Context, _, _ string) (*FileMetaView, error) {
	return nil, nil
}
func (m *mockQuotaStore) DeleteFileMeta(_ context.Context, _, _ string) error           { return nil }
func (m *mockQuotaStore) UpsertFileMetaTx(_ *sql.Tx, _ *FileMetaView) error             { return nil }
func (m *mockQuotaStore) DeleteFileMetaTx(_ *sql.Tx, _, _ string) error                 { return nil }

func (m *mockQuotaStore) InsertUploadReservation(_ context.Context, _ *UploadReservationView) error {
	return nil
}
func (m *mockQuotaStore) UpdateUploadReservationStatus(_ context.Context, _, _, _ string) error {
	return nil
}
func (m *mockQuotaStore) SettleActiveReservationTx(_ *sql.Tx, _, _, _ string) (bool, error) {
	return false, nil
}
func (m *mockQuotaStore) GetUploadReservation(_ context.Context, _, _ string) (*UploadReservationView, error) {
	return nil, nil
}

func (m *mockQuotaStore) InsertCentralLLMUsage(_ context.Context, _ *LLMUsageView) error { return nil }
func (m *mockQuotaStore) IncrMonthlyLLMCost(_ context.Context, _ string, _ int64) error  { return nil }
func (m *mockQuotaStore) InsertCentralLLMUsageTx(_ *sql.Tx, _ *LLMUsageView) error      { return nil }
func (m *mockQuotaStore) IncrMonthlyLLMCostTx(_ *sql.Tx, _ string, _ int64) error       { return nil }

func (m *mockQuotaStore) InsertMutationLog(_ context.Context, _ *MutationLogView) (int64, error) {
	return 0, nil
}
func (m *mockQuotaStore) InTx(_ context.Context, fn func(*sql.Tx) error) error {
	return fn(nil)
}
func (m *mockQuotaStore) SetQuotaCounters(_ context.Context, _ string, _, _ int64) error { return nil }

func TestMonthlyLLMCostExceeded_ServerQuota(t *testing.T) {
	mock := &mockQuotaStore{monthlyCost: 5000}
	b := &Dat9Backend{
		metaStore:                   mock,
		tenantID:                    "tenant-1",
		quotaSource:                 QuotaSourceServer,
		maxMonthlyLLMCostMillicents: 4000,
	}
	if !b.monthlyLLMCostExceeded() {
		t.Fatal("expected exceeded, got false")
	}
}

func TestMonthlyLLMCostExceeded_ServerQuota_NotExceeded(t *testing.T) {
	mock := &mockQuotaStore{monthlyCost: 3000}
	b := &Dat9Backend{
		metaStore:                   mock,
		tenantID:                    "tenant-1",
		quotaSource:                 QuotaSourceServer,
		maxMonthlyLLMCostMillicents: 4000,
	}
	if b.monthlyLLMCostExceeded() {
		t.Fatal("expected not exceeded, got true")
	}
}

func TestMonthlyLLMCostExceeded_ServerQuota_FailOpen(t *testing.T) {
	mock := &mockQuotaStore{monthlyCostErr: errors.New("meta db down")}
	b := &Dat9Backend{
		metaStore:                   mock,
		tenantID:                    "tenant-1",
		quotaSource:                 QuotaSourceServer,
		maxMonthlyLLMCostMillicents: 4000,
	}
	if b.monthlyLLMCostExceeded() {
		t.Fatal("expected fail-open (false), got true")
	}
}

func TestMonthlyLLMCostExceeded_DisabledBudget(t *testing.T) {
	b := &Dat9Backend{
		maxMonthlyLLMCostMillicents: 0,
	}
	if b.monthlyLLMCostExceeded() {
		t.Fatal("expected false when budget disabled")
	}
}
