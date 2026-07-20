package backend

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/meta"
)

var errFakeMutationAlreadyApplied = errors.New("mutation already applied")

type fakeMutationRecord struct {
	tenantID   string
	id         int64
	typ        string
	status     string
	retryCount int
	data       []byte
	createdAt  time.Time
}

type fakeMetaQuotaStore struct {
	mu                      sync.Mutex
	usage                   map[string]*QuotaUsageView
	config                  map[string]*QuotaConfigView
	fileMeta                map[string]*FileMetaView
	reservations            map[string]*UploadReservationView
	monthly                 map[string]int64
	llmUsage                []LLMUsageView
	objectGCCandidates      []meta.ObjectGCCandidateInput
	mutations               []fakeMutationRecord
	nextID                  int64
	markAppliedCalls        int // Finding B invariant: count MarkMutationAppliedTx calls (pre-guard, on-entry)
	observePendingCalls     int
	monthlyCostErr          error
	insertMutationErr       error
	objectGCCandidateErr    error
	insertReservationErr    error // injected into AtomicReserveAndInsertUpload to simulate INSERT failure inside the tx
	getReservationErr       error // injected into GetUploadReservation to simulate transient DB error
	inTxHook                func(context.Context) error
	alreadyAppliedOnMarkErr map[int64]bool
}

func (f *fakeMetaQuotaStore) EnqueueObjectGCCandidate(_ context.Context, c *meta.ObjectGCCandidateInput) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.objectGCCandidateErr != nil {
		return f.objectGCCandidateErr
	}
	if c == nil {
		return errors.New("nil object gc candidate")
	}
	cp := *c
	f.objectGCCandidates = append(f.objectGCCandidates, cp)
	return nil
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
	return &QuotaConfigView{
		TenantID:         tenantID,
		MaxStorageBytes:  meta.DefaultMaxStorageBytes(),
		MaxFileSizeBytes: meta.DefaultMaxFileSizeBytes(),
		MaxFileCount:     0,
		MaxMediaLLMFiles: 500,
		MaxVideoLLMFiles: 50,
		MaxMonthlyCostMC: 0,
	}, nil
}

func (f *fakeMetaQuotaStore) GetQuotaConfigVersion(ctx context.Context, tenantID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if cfg, ok := f.config[tenantID]; ok {
		return fmt.Sprintf("%d:%d:%d:%d:%d:%d", cfg.MaxStorageBytes, cfg.MaxFileSizeBytes, cfg.MaxFileCount, cfg.MaxMediaLLMFiles, cfg.MaxVideoLLMFiles, cfg.MaxMonthlyCostMC), nil
	}
	return "", nil
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

func (f *fakeMetaQuotaStore) IncrFileCount(ctx context.Context, tenantID string, delta int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureUsageLocked(tenantID).FileCount += delta
	return nil
}

func (f *fakeMetaQuotaStore) IncrFileCountTx(tx *sql.Tx, tenantID string, delta int64) error {
	return f.IncrFileCount(context.Background(), tenantID, delta)
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

// AtomicReserveAndInsertUpload models the real Store contract: claim
// reserved_bytes and insert the reservation row inside a single (fake) tx.
// If either step fails the state is fully rolled back — reserved_bytes is
// never bumped without a paired reservation row. Used by Round A / B1 tests.
func (f *fakeMetaQuotaStore) AtomicReserveAndInsertUpload(ctx context.Context, r *UploadReservationView) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cfg, ok := f.config[r.TenantID]
	if !ok {
		cfg = &QuotaConfigView{
			TenantID:         r.TenantID,
			MaxStorageBytes:  meta.DefaultMaxStorageBytes(),
			MaxFileSizeBytes: meta.DefaultMaxFileSizeBytes(),
			MaxMediaLLMFiles: 500,
		}
	}
	u := f.ensureUsageLocked(r.TenantID)
	if cfg.MaxStorageBytes > 0 && u.StorageBytes+u.ReservedBytes+r.ReservedBytes > cfg.MaxStorageBytes {
		return ErrStorageQuotaExceeded
	}
	if r.FileCountDelta > 0 && cfg.MaxFileCount > 0 && u.FileCount+r.FileCountDelta > cfg.MaxFileCount {
		return ErrFileCountQuotaExceeded
	}
	if _, exists := f.reservations[metaKey(r.TenantID, r.UploadID)]; exists {
		// Duplicate primary key; real tx would roll back here.
		return ErrReservationAlreadyExists
	}
	if f.insertReservationErr != nil {
		// Simulated INSERT failure inside the tx: reserved_bytes stays untouched.
		return f.insertReservationErr
	}
	u.ReservedBytes += r.ReservedBytes
	u.FileCount += r.FileCountDelta
	cp := *r
	f.reservations[metaKey(r.TenantID, r.UploadID)] = &cp
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
		return nil, meta.ErrNotFound
	}
	cp := *fm
	return &cp, nil
}

func (f *fakeMetaQuotaStore) GetFileMetaForUpdateTx(tx *sql.Tx, tenantID, fileID string) (*FileMetaView, error) {
	return f.GetFileMeta(context.Background(), tenantID, fileID)
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

func (f *fakeMetaQuotaStore) DeleteFileMetaIfExistsTx(tx *sql.Tx, tenantID, fileID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := metaKey(tenantID, fileID)
	if _, ok := f.fileMeta[key]; !ok {
		return false, nil
	}
	delete(f.fileMeta, key)
	return true, nil
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
	if r, ok := f.reservations[metaKey(tenantID, uploadID)]; ok && (r.Status == "active" || r.Status == "completing") {
		r.Status = status
	}
	return nil
}

func (f *fakeMetaQuotaStore) UpdateUploadReservationStatusTx(ctx context.Context, tx *sql.Tx, tenantID, uploadID, status string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r, ok := f.reservations[metaKey(tenantID, uploadID)]; ok && (r.Status == "active" || r.Status == "completing") {
		r.Status = status
	}
	return nil
}

func (f *fakeMetaQuotaStore) AbortActiveReservationTx(ctx context.Context, tx *sql.Tx, tenantID, uploadID string) (bool, int64, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.reservations[metaKey(tenantID, uploadID)]
	if !ok || (r.Status != "active" && r.Status != "completing") {
		return false, 0, 0, nil
	}
	r.Status = "aborted"
	return true, r.ReservedBytes, r.FileCountDelta, nil
}

// SettleActiveReservationTx models the atomic "active/completing → status"
// transition and reports whether a releasable row was actually settled.
func (f *fakeMetaQuotaStore) SettleActiveReservationTx(ctx context.Context, tx *sql.Tx, tenantID, uploadID, status string) (bool, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.reservations[metaKey(tenantID, uploadID)]
	if !ok || (r.Status != "active" && r.Status != "completing") {
		return false, 0, nil
	}
	r.Status = status
	return true, r.FileCountDelta, nil
}

func (f *fakeMetaQuotaStore) GetUploadReservation(ctx context.Context, tenantID, uploadID string) (*UploadReservationView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Simulated transient DB error distinct from ErrReservationNotFound. Used
	// by Round A / fix 4 tests to prove completeUploadReservation does NOT
	// silently drop the upload_complete mutation on transient lookup failure.
	if f.getReservationErr != nil {
		return nil, f.getReservationErr
	}
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
		tenantID:  entry.TenantID,
		id:        id,
		typ:       entry.MutationType,
		status:    "pending",
		data:      append([]byte(nil), entry.MutationData...),
		createdAt: time.Now().UTC(),
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

func (f *fakeMetaQuotaStore) ObservePendingMutations(ctx context.Context) ([]MutationBacklogView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observePendingCalls++
	now := time.Now().UTC()
	byTenant := make(map[string]*MutationBacklogView)
	oldest := make(map[string]time.Time)
	for _, m := range f.mutations {
		if m.status != "pending" {
			continue
		}
		obs := byTenant[m.tenantID]
		if obs == nil {
			obs = &MutationBacklogView{TenantID: m.tenantID}
			byTenant[m.tenantID] = obs
		}
		obs.PendingCount++
		createdAt := m.createdAt
		if createdAt.IsZero() {
			createdAt = now
		}
		if oldest[m.tenantID].IsZero() || createdAt.Before(oldest[m.tenantID]) {
			oldest[m.tenantID] = createdAt
		}
	}
	out := make([]MutationBacklogView, 0, len(byTenant))
	for tenantID, obs := range byTenant {
		age := now.Sub(oldest[tenantID].UTC()).Seconds()
		if age < 0 {
			age = 0
		}
		obs.OldestPendingAgeSeconds = age
		out = append(out, *obs)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TenantID < out[j].TenantID })
	return out, nil
}

func (f *fakeMetaQuotaStore) HasPendingFileMutation(ctx context.Context, tenantID, fileID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, m := range f.mutations {
		if m.tenantID != tenantID || m.status != "pending" {
			continue
		}
		if m.typ != "file_create" && m.typ != "file_overwrite" {
			continue
		}
		var data struct {
			FileID string `json:"file_id"`
		}
		if err := json.Unmarshal(m.data, &data); err != nil {
			return false, err
		}
		if data.FileID == fileID {
			return true, nil
		}
	}
	return false, nil
}

func (f *fakeMetaQuotaStore) MarkMutationAppliedTx(tx *sql.Tx, id int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Count BEFORE the guard — we want double-call regressions to fail the
	// MarkAppliedCalledExactlyOnce test even when the second call is a silent
	// no-op on an already-applied row.
	f.markAppliedCalls++
	for i := range f.mutations {
		if f.mutations[i].id == id {
			if f.alreadyAppliedOnMarkErr[id] {
				f.mutations[i].status = "applied"
				return fmt.Errorf("mutation %d: %w", id, errFakeMutationAlreadyApplied)
			}
			// Preserve the WHERE status='pending' guard shape used by the real
			// store. Most tests keep non-pending rows as no-ops; tests that need
			// the real already-applied race inject alreadyAppliedOnMarkErr above.
			if f.mutations[i].status != "pending" {
				return nil
			}
			f.mutations[i].status = "applied"
			return nil
		}
	}
	return fmt.Errorf("mutation %d not found", id)
}

func (f *fakeMetaQuotaStore) IsMutationAlreadyAppliedError(err error) bool {
	return errors.Is(err, errFakeMutationAlreadyApplied)
}

// mutationStatus is a test-only helper for reading mutation row status by id
// without exposing the slice internals. Used by Finding B invariant tests.
func (f *fakeMetaQuotaStore) mutationStatus(id int64) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.mutations {
		if f.mutations[i].id == id {
			return f.mutations[i].status
		}
	}
	return ""
}

func (f *fakeMetaQuotaStore) IncrMutationRetry(ctx context.Context, id int64, maxRetries int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.mutations {
		if f.mutations[i].id != id {
			continue
		}
		// Mirror the WHERE status='pending' guard of the real Store: refuse to
		// bump retry_count or flip status on an already-terminal row.
		if f.mutations[i].status != "pending" {
			return nil
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
	if f.inTxHook != nil {
		if err := f.inTxHook(ctx); err != nil {
			return err
		}
	}
	return fn(nil)
}

func (f *fakeMetaQuotaStore) ExpireActiveReservations(ctx context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	var released int64
	for _, r := range f.reservations {
		if (r.Status != "active" && r.Status != "completing") || !r.ExpiresAt.Before(now) {
			continue
		}
		r.Status = "aborted"
		u := f.ensureUsageLocked(r.TenantID)
		u.ReservedBytes -= r.ReservedBytes
		u.FileCount -= r.FileCountDelta
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
		MaxFileSizeBytes: meta.DefaultMaxFileSizeBytes(),
		MaxFileCount:     0,
		MaxMediaLLMFiles: 1000,
		MaxMonthlyCostMC: 1 << 30,
	}
	b.SetMetaQuotaStore(context.Background(), "tenant-a", fake)
	return b, fake
}

func drainCentralQuotaMutations(t *testing.T, b *Dat9Backend) {
	t.Helper()
	done := make(chan struct{})
	b.enqueueMutation(context.Background(), func(context.Context) {
		close(done)
	})
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for central quota mutations")
	}
}

func assertNoTenantQuotaOutboxRows(t *testing.T, b *Dat9Backend) {
	t.Helper()
	var tableExists int
	if err := b.Store().DB().QueryRowContext(context.Background(), `SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name = 'quota_outbox'`).Scan(&tableExists); err != nil {
		t.Fatalf("check tenant quota_outbox table: %v", err)
	}
	if tableExists == 0 {
		return
	}
	var count int
	if err := b.Store().DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM quota_outbox`).Scan(&count); err != nil {
		t.Fatalf("count tenant quota_outbox rows: %v", err)
	}
	if count != 0 {
		t.Fatalf("tenant quota_outbox rows = %d, want 0", count)
	}
}

func TestCentralQuotaFileMutationLifecycle(t *testing.T) {
	b, fake := newCentralQuotaBackend(t)

	if _, err := b.Write("/img.png", []byte("png-data"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatalf("create image: %v", err)
	}
	drainCentralQuotaMutations(t, b)
	assertNoTenantQuotaOutboxRows(t, b)
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
	if usage.StorageBytes != int64(len("png-data")) || usage.FileCount != 1 || usage.MediaFileCount != 1 {
		t.Fatalf("usage after create = %+v", usage)
	}

	if _, err := b.Write("/img.png", []byte("much-longer-png-data"), 0, filesystem.WriteFlagTruncate); err != nil {
		t.Fatalf("overwrite image: %v", err)
	}
	drainCentralQuotaMutations(t, b)
	assertNoTenantQuotaOutboxRows(t, b)
	usage, _ = fake.GetQuotaUsage(context.Background(), "tenant-a")
	if usage.StorageBytes != int64(len("much-longer-png-data")) || usage.FileCount != 1 || usage.MediaFileCount != 1 {
		t.Fatalf("usage after overwrite = %+v", usage)
	}
	current, err := b.Store().Stat(context.Background(), "/img.png")
	if err != nil {
		t.Fatalf("stat overwrite: %v", err)
	}

	if err := b.Remove("/img.png"); err != nil {
		t.Fatalf("remove image: %v", err)
	}
	usage, _ = fake.GetQuotaUsage(context.Background(), "tenant-a")
	if usage.StorageBytes != int64(len("much-longer-png-data")) || usage.FileCount != 1 || usage.MediaFileCount != 1 {
		t.Fatalf("usage before gc = %+v", usage)
	}
	task, err := b.Store().GetFileGCTaskByFileID(context.Background(), current.File.FileID)
	if err != nil {
		t.Fatalf("get file gc task: %v", err)
	}
	if task.Status != datastore.FileGCTaskQueued {
		t.Fatalf("gc task status = %s, want queued", task.Status)
	}
	processed, err := b.ProcessOneFileGCTask(context.Background())
	if err != nil {
		t.Fatalf("process file gc task: %v", err)
	}
	if !processed {
		t.Fatal("expected one file gc task to be processed")
	}
	usage, _ = fake.GetQuotaUsage(context.Background(), "tenant-a")
	if usage.StorageBytes != 0 || usage.FileCount != 0 || usage.MediaFileCount != 0 {
		t.Fatalf("usage after gc = %+v", usage)
	}
	if _, err := fake.GetFileMeta(context.Background(), "tenant-a", current.File.FileID); err == nil {
		t.Fatal("file meta should be deleted")
	}
	if got := []string{fake.mutations[0].typ, fake.mutations[1].typ}; got[0] != "file_create" || got[1] != "file_overwrite" {
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
	drainCentralQuotaMutations(t, b)
	assertNoTenantQuotaOutboxRows(t, b)

	nf, err := b.Store().Stat(ctx, "/clip.wav")
	if err != nil {
		t.Fatalf("stat uploaded file: %v", err)
	}
	usage, _ = fake.GetQuotaUsage(ctx, "tenant-a")
	if usage.StorageBytes != totalSize || usage.ReservedBytes != 0 || usage.FileCount != 1 || usage.MediaFileCount != 1 {
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

func TestCentralQuotaMutationLogInsertFailureSurfaces(t *testing.T) {
	b, fake := newCentralQuotaBackend(t)
	fake.insertMutationErr = errors.New("meta log unavailable")

	if _, err := b.Write("/lost.png", []byte("png-data"), 0, filesystem.WriteFlagCreate); err == nil {
		t.Fatal("create error = nil, want central quota mutation log error")
	}
	assertNoTenantQuotaOutboxRows(t, b)
	if len(fake.mutations) != 0 {
		t.Fatalf("mutation log rows = %d, want 0", len(fake.mutations))
	}
}

func TestCentralQuotaPendingDeltaRetainedThenExpiresAfterInlineApplyFailure(t *testing.T) {
	b, fake := newCentralQuotaBackend(t)
	applyErr := errors.New("meta apply unavailable")
	b.quotaPendingCache.pendingTTL = 10 * time.Millisecond

	if err := b.logAndEnqueueMutation(context.Background(), "file_create", fileCreateMutationData{
		FileID:    "file-pending",
		SizeBytes: 128,
		IsMedia:   false,
	}, quotaPendingDeltas{
		storageDelta: 128,
		fileDelta:    1,
	}, func(context.Context, *sql.Tx) error {
		return applyErr
	}); err != nil {
		t.Fatalf("log and enqueue mutation: %v", err)
	}
	drainCentralQuotaMutations(t, b)

	storageDelta, fileDelta, mediaDelta := b.pendingCentralMutationDeltas(context.Background())
	if storageDelta != 128 || fileDelta != 1 || mediaDelta != 0 {
		t.Fatalf("pending deltas = (%d,%d,%d), want retained pending mutation", storageDelta, fileDelta, mediaDelta)
	}
	if got := fake.mutations[len(fake.mutations)-1].status; got != "pending" {
		t.Fatalf("mutation status = %q, want pending for replay", got)
	}

	time.Sleep(20 * time.Millisecond)
	storageDelta, fileDelta, mediaDelta = b.pendingCentralMutationDeltas(context.Background())
	if storageDelta != 0 || fileDelta != 0 || mediaDelta != 0 {
		t.Fatalf("expired pending deltas = (%d,%d,%d), want zero after local TTL", storageDelta, fileDelta, mediaDelta)
	}
}

func TestCentralQuotaExpireReclaimsCompletingReservations(t *testing.T) {
	_, fake := newCentralQuotaBackend(t)
	ctx := context.Background()
	reservation := &UploadReservationView{
		TenantID:       "tenant-a",
		UploadID:       "upload-completing",
		ReservedBytes:  512,
		FileCountDelta: 1,
		TargetPath:     "/upload.bin",
		Status:         "active",
		ExpiresAt:      time.Now().Add(-time.Minute),
	}
	if err := fake.AtomicReserveAndInsertUpload(ctx, reservation); err != nil {
		t.Fatalf("reserve upload: %v", err)
	}
	if err := fake.UpdateUploadReservationStatus(ctx, "tenant-a", "upload-completing", "completing"); err != nil {
		t.Fatalf("mark completing: %v", err)
	}

	released, err := fake.ExpireActiveReservations(ctx)
	if err != nil {
		t.Fatalf("expire reservations: %v", err)
	}
	if released != 512 {
		t.Fatalf("released bytes = %d, want 512", released)
	}
	usage := fake.usage["tenant-a"]
	if usage.ReservedBytes != 0 || usage.FileCount != 0 {
		t.Fatalf("usage after expiry = %+v, want reserved/file count released", usage)
	}
	r, err := fake.GetUploadReservation(ctx, "tenant-a", "upload-completing")
	if err != nil {
		t.Fatalf("get reservation: %v", err)
	}
	if r.Status != "aborted" {
		t.Fatalf("reservation status = %q, want aborted", r.Status)
	}
}

func TestMonthlyLLMCostExceededUsesCentralCounter(t *testing.T) {
	b, fake := newCentralQuotaBackend(t)
	b.maxMonthlyLLMCostMillicents = 100
	fake.config["tenant-a"].MaxMonthlyCostMC = 100
	fake.monthly["tenant-a"] = 101
	if !b.monthlyLLMCostExceeded() {
		t.Fatal("expected central monthly budget to be exceeded")
	}
	fake.monthlyCostErr = errors.New("boom")
	if b.monthlyLLMCostExceeded() {
		t.Fatal("expected fail-open false on central monthly cost error")
	}
}

func TestRecordImageExtractUsageWritesCentralLedgerOnlyWhenServerQuotaActive(t *testing.T) {
	b, fake := newCentralQuotaBackend(t)
	b.visionCostPerKTokenMillicents = 1000

	b.recordImageExtractUsage("task-1", ImageExtractUsage{
		PromptTokens:     120,
		CompletionTokens: 80,
	})
	drainCentralQuotaMutations(t, b)

	total, err := b.store.MonthlyLLMCostMillicents()
	if err != nil {
		t.Fatalf("tenant monthly llm cost: %v", err)
	}
	if total != 0 {
		t.Fatalf("tenant monthly llm cost = %d, want 0 when server quota is active", total)
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
