package backend

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/mem9-ai/dat9/pkg/datastore"
)

// mockMetaLLMStore is a test double for MetaLLMUsageStore.
type mockMetaLLMStore struct {
	mu       sync.Mutex
	inserts  []mockLLMInsert
	monthly  int64
	queryErr error
}

type mockLLMInsert struct {
	TenantID, TaskType, TaskID string
	CostMillicents, RawUnits  int64
	RawUnitType               string
}

func (m *mockMetaLLMStore) InsertLLMUsage(_ context.Context, tenantID, taskType, taskID string, costMillicents, rawUnits int64, rawUnitType string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inserts = append(m.inserts, mockLLMInsert{
		TenantID: tenantID, TaskType: taskType, TaskID: taskID,
		CostMillicents: costMillicents, RawUnits: rawUnits, RawUnitType: rawUnitType,
	})
	return nil
}

func (m *mockMetaLLMStore) MonthlyLLMCostMillicents(_ context.Context, _ string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.monthly, m.queryErr
}

func TestInsertLLMUsage_MetaStore(t *testing.T) {
	mock := &mockMetaLLMStore{}
	b := &Dat9Backend{
		metaLLMStore:                  mock,
		tenantID:                      "tenant-1",
		visionCostPerKTokenMillicents: 100,
	}
	b.recordImageExtractUsage("task-1", ImageExtractUsage{PromptTokens: 500, CompletionTokens: 500})

	mock.mu.Lock()
	defer mock.mu.Unlock()
	if len(mock.inserts) != 1 {
		t.Fatalf("expected 1 insert, got %d", len(mock.inserts))
	}
	ins := mock.inserts[0]
	if ins.TenantID != "tenant-1" {
		t.Fatalf("tenant_id=%q, want tenant-1", ins.TenantID)
	}
	if ins.TaskType != "img_extract_text" {
		t.Fatalf("task_type=%q, want img_extract_text", ins.TaskType)
	}
	// 1000 tokens * 100 per 1K = 100
	if ins.CostMillicents != 100 {
		t.Fatalf("cost=%d, want 100", ins.CostMillicents)
	}
}

func TestInsertLLMUsage_AudioMetaStore(t *testing.T) {
	mock := &mockMetaLLMStore{}
	b := &Dat9Backend{
		metaLLMStore:                    mock,
		tenantID:                        "tenant-1",
		audioLLMCostPerKTokenMillicents: 200,
	}
	b.recordAudioExtractUsage("task-2", AudioExtractUsage{InputTokens: 300, OutputTokens: 200})

	mock.mu.Lock()
	defer mock.mu.Unlock()
	if len(mock.inserts) != 1 {
		t.Fatalf("expected 1 insert, got %d", len(mock.inserts))
	}
	ins := mock.inserts[0]
	if ins.TaskType != "audio_extract_text" {
		t.Fatalf("task_type=%q, want audio_extract_text", ins.TaskType)
	}
	// 500 tokens * 200 per 1K = 100
	if ins.CostMillicents != 100 {
		t.Fatalf("cost=%d, want 100", ins.CostMillicents)
	}
}

func TestMonthlyLLMCostExceeded_MetaStore(t *testing.T) {
	mock := &mockMetaLLMStore{monthly: 5000}
	b := &Dat9Backend{
		metaLLMStore:                mock,
		tenantID:                    "tenant-1",
		maxMonthlyLLMCostMillicents: 4000,
	}
	if !b.monthlyLLMCostExceeded() {
		t.Fatal("expected exceeded, got false")
	}
}

func TestMonthlyLLMCostExceeded_MetaStore_NotExceeded(t *testing.T) {
	mock := &mockMetaLLMStore{monthly: 3000}
	b := &Dat9Backend{
		metaLLMStore:                mock,
		tenantID:                    "tenant-1",
		maxMonthlyLLMCostMillicents: 4000,
	}
	if b.monthlyLLMCostExceeded() {
		t.Fatal("expected not exceeded, got true")
	}
}

func TestMonthlyLLMCostExceeded_MetaStoreFailure_FailOpen(t *testing.T) {
	mock := &mockMetaLLMStore{queryErr: errors.New("meta db down")}
	b := &Dat9Backend{
		metaLLMStore:                mock,
		tenantID:                    "tenant-1",
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

func TestMonthlyLLMCostExceeded_DualRead(t *testing.T) {
	// Meta store has 2000, tenant store has 2500.
	// Budget is 4000 millicents. Sum = 4500 > 4000 → exceeded.
	mock := &mockMetaLLMStore{monthly: 2000}
	store := newTestStore(t)
	// Insert some usage into the tenant store.
	if err := store.InsertLLMUsage("img_extract_text", "task-1", 2500, 100, "tokens"); err != nil {
		t.Fatal(err)
	}

	b := &Dat9Backend{
		store:                       store,
		metaLLMStore:                mock,
		tenantID:                    "tenant-1",
		maxMonthlyLLMCostMillicents: 4000,
		llmUsageDualRead:            true,
	}
	if !b.monthlyLLMCostExceeded() {
		t.Fatal("expected exceeded with dual-read (2000+2500=4500 > 4000), got false")
	}
}

func TestMonthlyLLMCostExceeded_DualRead_NotExceeded(t *testing.T) {
	// Meta store has 1000, tenant store has 1000. Sum = 2000 < 4000.
	mock := &mockMetaLLMStore{monthly: 1000}
	store := newTestStore(t)
	if err := store.InsertLLMUsage("img_extract_text", "task-1", 1000, 50, "tokens"); err != nil {
		t.Fatal(err)
	}

	b := &Dat9Backend{
		store:                       store,
		metaLLMStore:                mock,
		tenantID:                    "tenant-1",
		maxMonthlyLLMCostMillicents: 4000,
		llmUsageDualRead:            true,
	}
	if b.monthlyLLMCostExceeded() {
		t.Fatal("expected not exceeded with dual-read (1000+1000=2000 < 4000), got true")
	}
}

func TestMonthlyLLMCostExceeded_DualRead_TenantStoreFailure(t *testing.T) {
	// Meta store has 3000, tenant store query fails.
	// Should continue with meta-only total: 3000 < 4000 → not exceeded.
	mock := &mockMetaLLMStore{monthly: 3000}
	// Use a store with a closed DB to simulate failure.
	store := newTestStore(t)
	_ = store.Close() // close DB to make queries fail

	b := &Dat9Backend{
		store:                       store,
		metaLLMStore:                mock,
		tenantID:                    "tenant-1",
		maxMonthlyLLMCostMillicents: 4000,
		llmUsageDualRead:            true,
	}
	// Should not panic, should use meta-only total.
	if b.monthlyLLMCostExceeded() {
		t.Fatal("expected not exceeded (meta-only 3000 < 4000), got true")
	}
}

// newTestStore creates a datastore.Store backed by the test MySQL instance.
func newTestStore(t *testing.T) *datastore.Store {
	t.Helper()
	if testDSN == "" {
		t.Skip("test MySQL DSN not available")
	}
	store, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	// Clean llm_usage table.
	_, _ = store.DB().Exec("DELETE FROM llm_usage")
	return store
}
