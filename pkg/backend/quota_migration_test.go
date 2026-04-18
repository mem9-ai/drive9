package backend

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
)

type fakeMutationRecord struct {
	tenantID   string
	id         int64
	typ        string
	status     string
	retryCount int
	data       []byte
}

type fakeMetaQuotaStore struct {
	mu                sync.Mutex
	usage             map[string]*QuotaUsageView
	config            map[string]*QuotaConfigView
	fileMeta          map[string]*FileMetaView
	reservations      map[string]*UploadReservationView
	monthly           map[string]int64
	llmUsage          []LLMUsageView
	mutations         []fakeMutationRecord
	nextID            int64
	monthlyCostErr    error
	insertMutationErr error
}

func newFakeMetaQuotaStore() *fakeMetaQuotaStore {
	return &fakeMetaQuotaStore{
		usage:        make(map[string]*QuotaUsageView),
		config:       make(map[string]*QuotaConfigView),
		fileMeta:     make(map[string]*FileMetaView),
		reservations: make(map[string]*UploadReservationView),
		monthly:      make(map[string]int64),
		nextID:       1,
	}
}

func metaKey(tenantID, id string) string {
	return tenantID + "::" + id
}

func (f *fakeMetaQuotaStore) ensureUsageLocked(tenantID string) *QuotaUsageView {
	if u, ok := f.usage[tenantID]; ok {
		return u
	}
	u := &QuotaUsageView{TenantID: tenantID}
	f.usage[tenantID] = u
	return u
}

func (f *fakeMetaQuotaStore) GetQuotaConfig(ctx context.Context, tenantID string) (*QuotaConfigView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if cfg, ok := f.config[tenantID]; ok {
		cp := *cfg
		return &cp, nil
	}
	return &QuotaConfigView{TenantID: tenantID}, nil
}

func (f *fakeMetaQuotaStore) GetQuotaUsage(ctx context.Context, tenantID string) (*QuotaUsageView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	u := *f.ensureUsageLocked(tenantID)
	return &u, nil
}

func (f *fakeMetaQuotaStore) EnsureQuotaUsageRow(ctx context.Context, tenantID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureUsageLocked(tenantID)
	return nil
}

func (f *fakeMetaQuotaStore) IncrStorageBytes(ctx context.Context, tenantID string, delta int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureUsageLocked(tenantID).StorageBytes += delta
	return nil
}

func (f *fakeMetaQuotaStore) IncrStorageBytesTx(tx *sql.Tx, tenantID string, delta int64) error {
	return f.IncrStorageBytes(context.Background(), tenantID, delta)
}

func (f *fakeMetaQuotaStore) IncrReservedBytes(ctx context.Context, tenantID string, delta int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureUsageLocked(tenantID).ReservedBytes += delta
	return nil
}

func (f *fakeMetaQuotaStore) IncrReservedBytesTx(tx *sql.Tx, tenantID string, delta int64) error {
	return f.IncrReservedBytes(context.Background(), tenantID, delta)
}

func (f *fakeMetaQuotaStore) IncrMediaFileCount(ctx context.Context, tenantID string, delta int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureUsageLocked(tenantID).MediaFileCount += delta
	return nil
}

func (f *fakeMetaQuotaStore) IncrMediaFileCountTx(tx *sql.Tx, tenantID string, delta int64) error {
	return f.IncrMediaFileCount(context.Background(), tenantID, delta)
}

func (f *fakeMetaQuotaStore) TransferReservedToConfirmed(ctx context.Context, tenantID string, reservedDelta, storageDelta int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	u := f.ensureUsageLocked(tenantID)
	u.ReservedBytes += reservedDelta
	u.StorageBytes += storageDelta
	return nil
}

func (f *fakeMetaQuotaStore) TransferReservedToConfirmedTx(tx *sql.Tx, tenantID string, reservedDelta, storageDelta int64) error {
	return f.TransferReservedToConfirmed(context.Background(), tenantID, reservedDelta, storageDelta)
}

func (f *fakeMetaQuotaStore) AtomicReserveUpload(ctx context.Context, tenantID string, reserveBytes int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cfg, ok := f.config[tenantID]
	if !ok {
		cfg = &QuotaConfigView{TenantID: tenantID}
	}
	u := f.ensureUsageLocked(tenantID)
	if cfg.MaxStorageBytes > 0 && u.StorageBytes+u.ReservedBytes+reserveBytes > cfg.MaxStorageBytes {
		return ErrStorageQuotaExceeded
	}
	u.ReservedBytes += reserveBytes
	return nil
}

func (f *fakeMetaQuotaStore) UpsertFileMeta(ctx context.Context, fm *FileMetaView) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *fm
	f.fileMeta[metaKey(fm.TenantID, fm.FileID)] = &cp
	return nil
}

func (f *fakeMetaQuotaStore) UpsertFileMetaTx(tx *sql.Tx, fm *FileMetaView) error {
	return f.UpsertFileMeta(context.Background(), fm)
}

func (f *fakeMetaQuotaStore) GetFileMeta(ctx context.Context, tenantID, fileID string) (*FileMetaView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fm, ok := f.fileMeta[metaKey(tenantID, fileID)]
	if !ok {
		return nil, errors.New("not found")
	}
	cp := *fm
	return &cp, nil
}

func (f *fakeMetaQuotaStore) DeleteFileMeta(ctx context.Context, tenantID, fileID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.fileMeta, metaKey(tenantID, fileID))
	return nil
}

func (f *fakeMetaQuotaStore) DeleteFileMetaTx(tx *sql.Tx, tenantID, fileID string) error {
	return f.DeleteFileMeta(context.Background(), tenantID, fileID)
}

func (f *fakeMetaQuotaStore) InsertUploadReservation(ctx context.Context, r *UploadReservationView) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *r
	f.reservations[metaKey(r.TenantID, r.UploadID)] = &cp
	return nil
}

func (f *fakeMetaQuotaStore) UpdateUploadReservationStatus(ctx context.Context, tenantID, uploadID, status string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r, ok := f.reservations[metaKey(tenantID, uploadID)]; ok {
		r.Status = status
	}
	return nil
}

func (f *fakeMetaQuotaStore) UpdateUploadReservationStatusTx(tx *sql.Tx, tenantID, uploadID, status string) error {
	return f.UpdateUploadReservationStatus(context.Background(), tenantID, uploadID, status)
}

func (f *fakeMetaQuotaStore) GetUploadReservation(ctx context.Context, tenantID, uploadID string) (*UploadReservationView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.reservations[metaKey(tenantID, uploadID)]
	if !ok {
		return nil, ErrReservationNotFound
	}
	cp := *r
	return &cp, nil
}

func (f *fakeMetaQuotaStore) InsertCentralLLMUsage(ctx context.Context, r *LLMUsageView) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.llmUsage = append(f.llmUsage, *r)
	return nil
}

func (f *fakeMetaQuotaStore) InsertCentralLLMUsageTx(tx *sql.Tx, r *LLMUsageView) error {
	return f.InsertCentralLLMUsage(context.Background(), r)
}

func (f *fakeMetaQuotaStore) IncrMonthlyLLMCost(ctx context.Context, tenantID string, costMC int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.monthly[tenantID] += costMC
	return nil
}

func (f *fakeMetaQuotaStore) IncrMonthlyLLMCostTx(tx *sql.Tx, tenantID string, costMC int64) error {
	return f.IncrMonthlyLLMCost(context.Background(), tenantID, costMC)
}

func (f *fakeMetaQuotaStore) MonthlyLLMCostMillicents(ctx context.Context, tenantID string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.monthlyCostErr != nil {
		return 0, f.monthlyCostErr
	}
	return f.monthly[tenantID], nil
}

func (f *fakeMetaQuotaStore) InsertMutationLog(ctx context.Context, entry *MutationLogView) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.insertMutationErr != nil {
		return 0, f.insertMutationErr
	}
	id := f.nextID
	f.nextID++
	f.mutations = append(f.mutations, fakeMutationRecord{
		tenantID: entry.TenantID,
		id:       id,
		typ:      entry.MutationType,
		status:   "pending",
		data:     append([]byte(nil), entry.MutationData...),
	})
	return id, nil
}

func (f *fakeMetaQuotaStore) ListPendingMutations(ctx context.Context, minAge time.Duration, limit int) ([]MutationLogView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if limit <= 0 {
		limit = len(f.mutations)
	}
	out := make([]MutationLogView, 0, limit)
	for _, m := range f.mutations {
		if m.status != "pending" {
			continue
		}
		out = append(out, MutationLogView{
			ID:           m.id,
			TenantID:     m.tenantID,
			MutationType: m.typ,
			MutationData: append([]byte(nil), m.data...),
			RetryCount:   m.retryCount,
		})
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (f *fakeMetaQuotaStore) MarkMutationAppliedTx(tx *sql.Tx, id int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.mutations {
		if f.mutations[i].id == id {
			f.mutations[i].status = "applied"
			return nil
		}
	}
	return fmt.Errorf("mutation %d not found", id)
}

func (f *fakeMetaQuotaStore) IncrMutationRetry(ctx context.Context, id int64, maxRetries int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.mutations {
		if f.mutations[i].id != id {
			continue
		}
		f.mutations[i].retryCount++
		if maxRetries > 0 && f.mutations[i].retryCount >= maxRetries {
			f.mutations[i].status = "failed"
		}
		return nil
	}
	return nil
}

func (f *fakeMetaQuotaStore) InTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	return fn(nil)
}

func (f *fakeMetaQuotaStore) ExpireActiveReservations(ctx context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	var released int64
	for _, r := range f.reservations {
		if r.Status != "active" || !r.ExpiresAt.Before(now) {
			continue
		}
		r.Status = "aborted"
		f.ensureUsageLocked(r.TenantID).ReservedBytes -= r.ReservedBytes
		released += r.ReservedBytes
	}
	return released, nil
}

func newCentralQuotaBackend(t *testing.T) (*Dat9Backend, *fakeMetaQuotaStore) {
	t.Helper()
	b := newTestBackend(t)
	fake := newFakeMetaQuotaStore()
	fake.config["tenant-a"] = &QuotaConfigView{
		TenantID:         "tenant-a",
		MaxStorageBytes:  1 << 30,
		MaxMediaLLMFiles: 1000,
		MaxMonthlyCostMC: 1 << 30,
	}
	b.SetMetaQuotaStore("tenant-a", fake)
	return b, fake
}

func TestCentralQuotaFileMutationLifecycle(t *testing.T) {
	b, fake := newCentralQuotaBackend(t)

	if _, err := b.Write("/img.png", []byte("png-data"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatalf("create image: %v", err)
	}
	nf, err := b.Store().Stat(context.Background(), "/img.png")
	if err != nil {
		t.Fatalf("stat create: %v", err)
	}
	fm, err := fake.GetFileMeta(context.Background(), "tenant-a", nf.File.FileID)
	if err != nil {
		t.Fatalf("get file meta: %v", err)
	}
	if fm.SizeBytes != int64(len("png-data")) || !fm.IsMedia {
		t.Fatalf("file meta after create = %+v", fm)
	}
	usage, _ := fake.GetQuotaUsage(context.Background(), "tenant-a")
	if usage.StorageBytes != int64(len("png-data")) || usage.MediaFileCount != 1 {
		t.Fatalf("usage after create = %+v", usage)
	}

	if _, err := b.Write("/img.png", []byte("much-longer-png-data"), 0, filesystem.WriteFlagTruncate); err != nil {
		t.Fatalf("overwrite image: %v", err)
	}
	usage, _ = fake.GetQuotaUsage(context.Background(), "tenant-a")
	if usage.StorageBytes != int64(len("much-longer-png-data")) || usage.MediaFileCount != 1 {
		t.Fatalf("usage after overwrite = %+v", usage)
	}

	if err := b.Remove("/img.png"); err != nil {
		t.Fatalf("remove image: %v", err)
	}
	usage, _ = fake.GetQuotaUsage(context.Background(), "tenant-a")
	if usage.StorageBytes != 0 || usage.MediaFileCount != 0 {
		t.Fatalf("usage after delete = %+v", usage)
	}
	if _, err := fake.GetFileMeta(context.Background(), "tenant-a", nf.File.FileID); err == nil {
		t.Fatal("file meta should be deleted")
	}
	if got := []string{fake.mutations[0].typ, fake.mutations[1].typ, fake.mutations[2].typ}; got[0] != "file_create" || got[1] != "file_overwrite" || got[2] != "file_delete" {
		t.Fatalf("mutation types = %v", got)
	}
	for _, m := range fake.mutations {
		if m.status != "applied" {
			t.Fatalf("mutation %+v not applied", m)
		}
	}
}

func TestCentralQuotaUploadCompleteUpdatesShadowState(t *testing.T) {
	b, fake := newCentralQuotaBackend(t)
	ctx := context.Background()
	totalSize := int64(32)
	plan, err := b.InitiateUpload(ctx, "/clip.wav", totalSize)
	if err != nil {
		t.Fatalf("initiate upload: %v", err)
	}
	usage, _ := fake.GetQuotaUsage(ctx, "tenant-a")
	if usage.ReservedBytes != totalSize {
		t.Fatalf("reserved bytes after initiate = %d, want %d", usage.ReservedBytes, totalSize)
	}

	uploadAllPartsForPlan(t, b, plan, plan.UploadID, totalSize)
	if err := b.ConfirmUpload(ctx, plan.UploadID); err != nil {
		t.Fatalf("confirm upload: %v", err)
	}

	nf, err := b.Store().Stat(ctx, "/clip.wav")
	if err != nil {
		t.Fatalf("stat uploaded file: %v", err)
	}
	usage, _ = fake.GetQuotaUsage(ctx, "tenant-a")
	if usage.StorageBytes != totalSize || usage.ReservedBytes != 0 || usage.MediaFileCount != 1 {
		t.Fatalf("usage after confirm = %+v", usage)
	}
	fm, err := fake.GetFileMeta(ctx, "tenant-a", nf.File.FileID)
	if err != nil {
		t.Fatalf("get uploaded file meta: %v", err)
	}
	if fm.SizeBytes != totalSize || !fm.IsMedia {
		t.Fatalf("uploaded file meta = %+v", fm)
	}
	reservation, err := fake.GetUploadReservation(ctx, "tenant-a", plan.UploadID)
	if err != nil {
		t.Fatalf("get upload reservation: %v", err)
	}
	if reservation.Status != "completed" {
		t.Fatalf("reservation status = %q, want completed", reservation.Status)
	}
	last := fake.mutations[len(fake.mutations)-1]
	if last.typ != "upload_complete" || last.status != "applied" {
		t.Fatalf("last mutation = %+v", last)
	}
}

func TestMonthlyLLMCostExceededUsesCentralCounter(t *testing.T) {
	b, fake := newCentralQuotaBackend(t)
	b.quotaSource = QuotaSourceServer // enable server-side cost check
	b.maxMonthlyLLMCostMillicents = 100
	fake.monthly["tenant-a"] = 101
	if !b.monthlyLLMCostExceeded() {
		t.Fatal("expected central monthly budget to be exceeded")
	}
	fake.monthlyCostErr = errors.New("boom")
	if b.monthlyLLMCostExceeded() {
		t.Fatal("expected fail-open false on central monthly cost error")
	}
}

func TestRecordImageExtractUsageWritesCentralLedgerAndLegacyStore(t *testing.T) {
	b, fake := newCentralQuotaBackend(t)
	b.visionCostPerKTokenMillicents = 1000

	b.recordImageExtractUsage("task-1", ImageExtractUsage{
		PromptTokens:     120,
		CompletionTokens: 80,
	})

	total, err := b.store.MonthlyLLMCostMillicents()
	if err != nil {
		t.Fatalf("tenant monthly llm cost: %v", err)
	}
	if total != 200 {
		t.Fatalf("tenant monthly llm cost = %d, want 200", total)
	}
	if fake.monthly["tenant-a"] != 200 {
		t.Fatalf("central monthly llm cost = %d, want 200", fake.monthly["tenant-a"])
	}
	if len(fake.llmUsage) != 1 {
		t.Fatalf("central llm usage len = %d, want 1", len(fake.llmUsage))
	}
	got := fake.llmUsage[0]
	if got.TaskType != "img_extract_text" || got.TaskID != "task-1" || got.CostMillicents != 200 {
		t.Fatalf("central llm usage = %+v", got)
	}
	last := fake.mutations[len(fake.mutations)-1]
	if last.typ != "llm_cost_record" || last.status != "applied" {
		t.Fatalf("last mutation = %+v", last)
	}
}

func TestMonthlyLLMCostExceededFallsBackWhenMetaStoreNil(t *testing.T) {
	b := newTestBackend(t)
	b.maxMonthlyLLMCostMillicents = 50
	if err := b.store.InsertLLMUsage("img_extract_text", "task-2", 75, 75, "tokens"); err != nil {
		t.Fatalf("insert tenant llm usage: %v", err)
	}
	if !b.monthlyLLMCostExceeded() {
		t.Fatal("expected legacy tenant monthly budget path to remain functional")
	}
}
