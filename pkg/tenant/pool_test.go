package tenant

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/drive9/internal/testmysql"
	"github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/encrypt"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"github.com/mem9-ai/drive9/pkg/semantic"
	"github.com/mem9-ai/drive9/pkg/tenant/schema"
)

func TestNewPoolUsesDefaultMaxTenants(t *testing.T) {
	pool := NewPool(PoolConfig{}, nil)
	if pool.maxSize != defaultTenantPoolMaxTenants {
		t.Fatalf("max size = %d, want %d", pool.maxSize, defaultTenantPoolMaxTenants)
	}
}

func TestPoolAcquireInvalidateDefersCloseUntilRelease(t *testing.T) {
	pool, tenant := newTestPoolAndTenant(t, 2, "tenant-a")
	ctx := context.Background()

	b1, release1, err := pool.Acquire(ctx, tenant)
	if err != nil {
		t.Fatal(err)
	}
	store1 := b1.Store()
	assertStoreOpen(t, store1)

	pool.Invalidate(tenant.ID)
	assertStoreOpen(t, store1)

	b2, release2, err := pool.Acquire(ctx, tenant)
	if err != nil {
		t.Fatal(err)
	}
	if b1 == b2 {
		t.Fatal("expected acquire after invalidate to create a new backend")
	}
	assertStoreOpen(t, b2.Store())

	release1()
	assertStoreClosed(t, store1)
	assertStoreOpen(t, b2.Store())
	release2()
}

func TestCleanStorageNamespaceLocalPrefixRejectsUnsafeSegments(t *testing.T) {
	tests := []struct {
		name    string
		prefix  string
		want    string
		wantErr bool
	}{
		{name: "normal", prefix: "tenant/abc", want: "tenant/abc"},
		{name: "trim", prefix: "tenant/abc/", want: "tenant/abc"},
		{name: "empty", prefix: "", wantErr: true},
		{name: "absolute", prefix: "/tmp/tenant", wantErr: true},
		{name: "parent", prefix: "tenant/../other", wantErr: true},
		{name: "dot", prefix: "tenant/./other", wantErr: true},
		{name: "empty segment", prefix: "tenant//other", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := cleanStorageNamespaceLocalPrefix(tc.prefix)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("cleanStorageNamespaceLocalPrefix(%q) error = nil, want error", tc.prefix)
				}
				return
			}
			if err != nil {
				t.Fatalf("cleanStorageNamespaceLocalPrefix(%q): %v", tc.prefix, err)
			}
			if got != tc.want {
				t.Fatalf("cleanStorageNamespaceLocalPrefix(%q) = %q, want %q", tc.prefix, got, tc.want)
			}
		})
	}
}

func TestPoolAcquireEvictionRetiresPinnedEntry(t *testing.T) {
	pool, tenantA := newTestPoolAndTenant(t, 1, "tenant-a")
	ctx := context.Background()
	bA, releaseA, err := pool.Acquire(ctx, tenantA)
	if err != nil {
		t.Fatal(err)
	}
	storeA := bA.Store()
	assertStoreOpen(t, storeA)

	tenantB := cloneTenantForID(t, pool, tenantA, "tenant-b")
	bB, err := pool.Get(ctx, tenantB)
	if err != nil {
		t.Fatal(err)
	}
	if bB == nil {
		t.Fatal("expected second backend")
	}
	assertStoreOpen(t, bB.Store())
	assertStoreOpen(t, storeA)

	releaseA()
	assertStoreClosed(t, storeA)
	assertStoreOpen(t, bB.Store())
}

func TestPoolAcquireReloadsS3EncryptionPolicyForNextWrite(t *testing.T) {
	globalKeyID := "arn:aws:kms:ap-southeast-1:123456789012:key/pool-test"
	pool, tenant := newTestPoolAndTenantWithConfig(t, PoolConfig{
		MaxTenants: 2,
		S3Dir:      t.TempDir(),
		PublicURL:  "http://localhost:9091",
		S3EncryptionPolicy: meta.S3EncryptionPolicy{
			Mode:             meta.S3EncryptionModeSSEKMS,
			KMSKeyID:         globalKeyID,
			BucketKeyEnabled: true,
		},
	}, "tenant-a")
	disabledBucketKey := false
	tenant.S3EncryptionMode = meta.S3EncryptionModeNone
	tenant.S3BucketKeyEnabled = &disabledBucketKey
	ctx := context.Background()

	b1, release1, err := pool.Acquire(ctx, tenant)
	if err != nil {
		t.Fatal(err)
	}
	defer release1()
	if _, _, err := b1.WriteCtxIfRevisionWithTagsResult(ctx, "/before.bin", []byte("before"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, -1, nil, ""); err != nil {
		t.Fatalf("write before policy change: %v", err)
	}
	before, err := b1.Store().Stat(ctx, "/before.bin")
	if err != nil {
		t.Fatalf("stat before: %v", err)
	}
	if before.File.StorageEncryptionMode != datastore.StorageEncryptionNone {
		t.Fatalf("before encryption mode=%q, want %q", before.File.StorageEncryptionMode, datastore.StorageEncryptionNone)
	}

	changed := *tenant
	changed.S3EncryptionMode = meta.S3EncryptionModeSSEKMS
	b2, release2, err := pool.Acquire(ctx, &changed)
	if err != nil {
		t.Fatal(err)
	}
	defer release2()
	if b1 == b2 {
		t.Fatal("expected tenant policy change to recreate cached backend")
	}
	if _, _, err := b2.WriteCtxIfRevisionWithTagsResult(ctx, "/after.bin", []byte("after"), 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, -1, nil, ""); err != nil {
		t.Fatalf("write after policy change: %v", err)
	}
	after, err := b2.Store().Stat(ctx, "/after.bin")
	if err != nil {
		t.Fatalf("stat after: %v", err)
	}
	if after.File.StorageEncryptionMode != datastore.StorageEncryptionSSEKMS {
		t.Fatalf("after encryption mode=%q, want %q", after.File.StorageEncryptionMode, datastore.StorageEncryptionSSEKMS)
	}
	if after.File.StorageEncryptionKeyID != globalKeyID {
		t.Fatalf("after encryption key=%q, want %q", after.File.StorageEncryptionKeyID, globalKeyID)
	}
}

func newTestPoolAndTenant(t *testing.T, maxTenants int, tenantID string) (*Pool, *meta.Tenant) {
	t.Helper()
	return newTestPoolAndTenantWithConfig(t, PoolConfig{MaxTenants: maxTenants}, tenantID)
}

func newTestPoolAndTenantWithConfig(t *testing.T, cfg PoolConfig, tenantID string) (*Pool, *meta.Tenant) {
	t.Helper()
	initTenantPoolSchema(t, testDSN)
	resetStore, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	testmysql.ResetDB(t, resetStore.DB())
	if err := resetStore.Close(); err != nil {
		t.Fatal(err)
	}

	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := NewPool(cfg, enc)
	t.Cleanup(func() { pool.Close() })
	return pool, cloneTenantForID(t, pool, nil, tenantID)
}

func initTenantPoolSchema(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	stmts := []string{
		`CREATE TABLE IF NOT EXISTS file_nodes (node_id VARCHAR(64) PRIMARY KEY, path TEXT NOT NULL, path_hash VARCHAR(64) NOT NULL DEFAULT '', parent_path TEXT NOT NULL, parent_path_hash VARCHAR(64) NOT NULL DEFAULT '', name VARCHAR(255) NOT NULL, is_directory BOOLEAN NOT NULL DEFAULT FALSE, file_id VARCHAR(64), inode_id VARCHAR(64), created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3))`,
		`CREATE UNIQUE INDEX idx_path ON file_nodes(path_hash)`,
		`CREATE INDEX idx_parent ON file_nodes(parent_path_hash, name)`,
		`CREATE INDEX idx_file_id ON file_nodes(file_id)`,
		`CREATE INDEX idx_inode_id ON file_nodes(inode_id)`,
		`CREATE TABLE IF NOT EXISTS files (file_id VARCHAR(64) PRIMARY KEY, storage_type VARCHAR(32) NOT NULL, storage_ref TEXT NOT NULL, storage_ref_hash VARCHAR(64) NOT NULL DEFAULT '', content_blob LONGBLOB, content_type VARCHAR(255), size_bytes BIGINT NOT NULL DEFAULT 0, checksum_sha256 VARCHAR(128), revision BIGINT NOT NULL DEFAULT 1, status VARCHAR(32) NOT NULL DEFAULT 'PENDING', source_id VARCHAR(255), content_text LONGTEXT, description LONGTEXT, embedding LONGTEXT, embedding_revision BIGINT, description_embedding LONGTEXT, description_embedding_revision BIGINT, storage_encryption_mode VARCHAR(16) NOT NULL DEFAULT 'legacy', storage_encryption_key_id VARCHAR(256) NOT NULL DEFAULT '', created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), confirmed_at DATETIME(3), expires_at DATETIME(3))`,
		`CREATE INDEX idx_status ON files(status, created_at)`,
		`CREATE INDEX idx_files_storage_ref_hash ON files(storage_ref_hash)`,
		`CREATE TABLE IF NOT EXISTS inodes (inode_id VARCHAR(64) PRIMARY KEY, size_bytes BIGINT NOT NULL DEFAULT 0, revision BIGINT NOT NULL DEFAULT 1, mode INT NOT NULL DEFAULT 420, status VARCHAR(32) NOT NULL DEFAULT 'PENDING', created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), mtime DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), confirmed_at DATETIME(3), expires_at DATETIME(3))`,
		`CREATE INDEX idx_inodes_status ON inodes(status, created_at)`,
		`CREATE TABLE IF NOT EXISTS contents (inode_id VARCHAR(64) PRIMARY KEY, storage_type VARCHAR(32) NOT NULL, storage_ref TEXT NOT NULL, storage_ref_hash VARCHAR(64) NOT NULL DEFAULT '', storage_encryption_mode VARCHAR(16) NOT NULL DEFAULT 'legacy', storage_encryption_key_id VARCHAR(256) NOT NULL DEFAULT '', content_blob LONGBLOB, content_type VARCHAR(255), checksum_sha256 VARCHAR(128), source_id VARCHAR(255))`,
		`CREATE INDEX idx_contents_storage_ref_hash ON contents(storage_ref_hash)`,
		`CREATE TABLE IF NOT EXISTS semantic (inode_id VARCHAR(64) PRIMARY KEY, content_text LONGTEXT, description LONGTEXT, embedding LONGTEXT, embedding_revision BIGINT, description_embedding LONGTEXT, description_embedding_revision BIGINT)`,
		`CREATE TABLE IF NOT EXISTS file_tags (file_id VARCHAR(64) NOT NULL, inode_id VARCHAR(64), tag_key VARCHAR(255) NOT NULL, tag_value VARCHAR(255) NOT NULL DEFAULT '', PRIMARY KEY (file_id, tag_key))`,
		`CREATE INDEX idx_kv ON file_tags(tag_key, tag_value)`,
		`CREATE TABLE IF NOT EXISTS uploads (upload_id VARCHAR(64) PRIMARY KEY, file_id VARCHAR(64) NOT NULL, inode_id VARCHAR(64), target_path TEXT NOT NULL, target_path_hash VARCHAR(64) NOT NULL DEFAULT '', s3_upload_id VARCHAR(255) NOT NULL, s3_key VARCHAR(2048) NOT NULL, total_size BIGINT NOT NULL, part_size BIGINT NOT NULL, parts_total INT NOT NULL, expected_revision BIGINT, status VARCHAR(32) NOT NULL DEFAULT 'UPLOADING', fingerprint_sha256 VARCHAR(128), idempotency_key VARCHAR(255), description LONGTEXT, storage_encryption_mode VARCHAR(16) NOT NULL DEFAULT 'none', storage_encryption_key_id VARCHAR(256) NOT NULL DEFAULT '', created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3), expires_at DATETIME(3) NOT NULL, active_target_path_hash VARCHAR(64) AS (CASE WHEN status = 'UPLOADING' THEN target_path_hash ELSE NULL END) VIRTUAL)`,
		`CREATE INDEX idx_upload_path ON uploads(target_path_hash, status)`,
		`CREATE UNIQUE INDEX idx_idempotency ON uploads(idempotency_key)`,
		`CREATE UNIQUE INDEX idx_uploads_active ON uploads(active_target_path_hash)`,
		`CREATE TABLE IF NOT EXISTS semantic_tasks (task_id VARCHAR(64) PRIMARY KEY, task_type VARCHAR(32) NOT NULL, resource_id VARCHAR(64) NOT NULL, resource_version BIGINT NOT NULL, status VARCHAR(20) NOT NULL, attempt_count INT NOT NULL DEFAULT 0, max_attempts INT NOT NULL DEFAULT 5, receipt VARCHAR(128), leased_at DATETIME(3), lease_until DATETIME(3), available_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), payload_json JSON, last_error TEXT, created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), completed_at DATETIME(3))`,
		`CREATE UNIQUE INDEX uk_task_resource_version ON semantic_tasks(task_type, resource_id, resource_version)`,
		`CREATE INDEX idx_task_claim ON semantic_tasks(status, available_at, lease_until, created_at)`,
		`CREATE INDEX idx_task_claim_type ON semantic_tasks(status, task_type, available_at, created_at, task_id)`,
		`CREATE TABLE IF NOT EXISTS file_gc_tasks (task_id VARCHAR(64) PRIMARY KEY, file_id VARCHAR(64) NOT NULL, inode_id VARCHAR(64), storage_type VARCHAR(32) NOT NULL, storage_ref TEXT NOT NULL, size_bytes BIGINT NOT NULL DEFAULT 0, content_type VARCHAR(255), status VARCHAR(20) NOT NULL, attempt_count INT NOT NULL DEFAULT 0, max_attempts INT NOT NULL DEFAULT 0, receipt VARCHAR(128), leased_at DATETIME(3), lease_until DATETIME(3), available_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), last_error TEXT, created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), completed_at DATETIME(3))`,
		`CREATE UNIQUE INDEX uk_file_gc_file_id ON file_gc_tasks(file_id)`,
		`CREATE UNIQUE INDEX uk_file_gc_inode_id ON file_gc_tasks(inode_id)`,
		`CREATE INDEX idx_file_gc_claim ON file_gc_tasks(status, available_at, lease_until, created_at)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			msg := err.Error()
			if strings.Contains(msg, "Duplicate key name") || strings.Contains(msg, "already exists") {
				continue
			}
			t.Fatal(err)
		}
	}
}

func cloneTenantForID(t *testing.T, pool *Pool, src *meta.Tenant, tenantID string) *meta.Tenant {
	t.Helper()
	parsed, err := mysql.ParseDSN(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	host, port := "127.0.0.1", 3306
	if parsed.Addr != "" {
		h, p, _ := strings.Cut(parsed.Addr, ":")
		if h != "" {
			host = h
		}
		if p != "" {
			_, _ = fmt.Sscanf(p, "%d", &port)
		}
	}
	passwd := parsed.Passwd
	if src != nil {
		plain, err := pool.Decrypt(context.Background(), src.DBPasswordCipher)
		if err != nil {
			t.Fatal(err)
		}
		passwd = string(plain)
	}
	passCipher, err := pool.Encrypt(context.Background(), []byte(passwd))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	provider := ProviderDB9
	if src != nil {
		provider = src.Provider
	}
	return &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantActive,
		DBHost:           host,
		DBPort:           port,
		DBUser:           parsed.User,
		DBPasswordCipher: passCipher,
		DBName:           parsed.DBName,
		DBTLS:            false,
		Provider:         provider,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
}

func assertStoreOpen(t *testing.T, store *datastore.Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := store.DB().PingContext(ctx); err != nil {
		t.Fatalf("expected store to remain open: %v", err)
	}
}

func assertStoreClosed(t *testing.T, store *datastore.Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := store.DB().PingContext(ctx); err == nil {
		t.Fatal("expected store to be closed")
	}
}

func TestPoolAutoSemanticTaskTypes(t *testing.T) {
	var p *Pool
	if p.AutoSemanticTaskTypes() != nil {
		t.Fatal("nil pool should return nil task types")
	}
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	disabled := NewPool(PoolConfig{BackendOptions: backend.Options{}}, enc)
	defer disabled.Close()
	if disabled.AutoSemanticTaskTypes() != nil {
		t.Fatal("async image extract disabled should yield nil")
	}
	enabled := NewPool(PoolConfig{BackendOptions: backend.Options{
		AsyncImageExtract: backend.AsyncImageExtractOptions{Enabled: true},
	}}, enc)
	defer enabled.Close()
	got := enabled.AutoSemanticTaskTypes()
	if len(got) != 1 || got[0] != semantic.TaskTypeImgExtractText {
		t.Fatalf("got %#v, want [img_extract_text]", got)
	}

	audioOnly := NewPool(PoolConfig{BackendOptions: backend.Options{
		AsyncAudioExtract: backend.AsyncAudioExtractOptions{Enabled: true, Extractor: &poolDummyAudioExtractor{}},
	}}, enc)
	defer audioOnly.Close()
	gotAudio := audioOnly.AutoSemanticTaskTypes()
	if len(gotAudio) != 1 || gotAudio[0] != semantic.TaskTypeAudioExtractText {
		t.Fatalf("got %#v, want [audio_extract_text]", gotAudio)
	}

	both := NewPool(PoolConfig{BackendOptions: backend.Options{
		AsyncImageExtract: backend.AsyncImageExtractOptions{Enabled: true},
		AsyncAudioExtract: backend.AsyncAudioExtractOptions{Enabled: true, Extractor: &poolDummyAudioExtractor{}},
	}}, enc)
	defer both.Close()
	gotBoth := both.AutoSemanticTaskTypes()
	if len(gotBoth) != 2 || gotBoth[0] != semantic.TaskTypeImgExtractText || gotBoth[1] != semantic.TaskTypeAudioExtractText {
		t.Fatalf("got %#v, want [img_extract_text audio_extract_text]", gotBoth)
	}
}

func TestPoolCreateBackendPeriodicallyValidatesSchemaWhenVersionMatches(t *testing.T) {
	pool, tenant := newTestPoolAndTenant(t, 2, "tenant-validate")
	tenant.Provider = ProviderTiDBZero
	tenant.SchemaVersion = schema.CurrentTiDBTenantSchemaVersion

	origEnsure := ensureTiDBSchemaForAutoEmbeddingProfile
	origValidate := validateTiDBSchemaForAutoEmbeddingProfile
	origApply := applyTiDBAutoEmbeddingProviderConfig
	origEvery := periodicTiDBSchemaValidationEvery
	ensureCalls := 0
	validateCalls := 0
	ensureTiDBSchemaForAutoEmbeddingProfile = func(context.Context, *sql.DB, schema.TiDBAutoEmbeddingProfile) error {
		ensureCalls++
		return nil
	}
	validateTiDBSchemaForAutoEmbeddingProfile = func(context.Context, *sql.DB, schema.TiDBAutoEmbeddingProfile) error {
		validateCalls++
		return nil
	}
	applyTiDBAutoEmbeddingProviderConfig = func(context.Context, *sql.DB, schema.TiDBAutoEmbeddingProviderConfig) error {
		return nil
	}
	periodicTiDBSchemaValidationEvery = 4
	t.Cleanup(func() {
		ensureTiDBSchemaForAutoEmbeddingProfile = origEnsure
		validateTiDBSchemaForAutoEmbeddingProfile = origValidate
		applyTiDBAutoEmbeddingProviderConfig = origApply
		periodicTiDBSchemaValidationEvery = origEvery
	})

	for i := 0; i < 4; i++ {
		backend, store, err := pool.createBackend(context.Background(), tenant)
		if err != nil {
			t.Fatalf("createBackend() iteration %d: %v", i, err)
		}
		backend.Close()
		if err := store.Close(); err != nil {
			t.Fatalf("close store iteration %d: %v", i, err)
		}
	}

	if ensureCalls != 0 {
		t.Fatalf("ensureTiDBSchemaForAutoEmbeddingProfile called %d times, want 0", ensureCalls)
	}
	if validateCalls != 2 {
		t.Fatalf("validateTiDBSchemaForAutoEmbeddingProfile called %d times, want 2", validateCalls)
	}
}

func TestPoolCreateBackendEnsuresSchemaForTiDBCloudNative(t *testing.T) {
	pool, tenant := newTestPoolAndTenant(t, 2, "tenant-native-ensure")
	tenant.Provider = ProviderTiDBCloudNative
	tenant.SchemaVersion = 0

	origEnsure := ensureTiDBSchemaForAutoEmbeddingProfile
	origValidate := validateTiDBSchemaForAutoEmbeddingProfile
	origApply := applyTiDBAutoEmbeddingProviderConfig
	ensureCalls := 0
	ensureTiDBSchemaForAutoEmbeddingProfile = func(context.Context, *sql.DB, schema.TiDBAutoEmbeddingProfile) error {
		ensureCalls++
		return nil
	}
	validateTiDBSchemaForAutoEmbeddingProfile = func(context.Context, *sql.DB, schema.TiDBAutoEmbeddingProfile) error {
		return nil
	}
	applyTiDBAutoEmbeddingProviderConfig = func(context.Context, *sql.DB, schema.TiDBAutoEmbeddingProviderConfig) error {
		return nil
	}
	t.Cleanup(func() {
		ensureTiDBSchemaForAutoEmbeddingProfile = origEnsure
		validateTiDBSchemaForAutoEmbeddingProfile = origValidate
		applyTiDBAutoEmbeddingProviderConfig = origApply
	})

	backend, store, err := pool.createBackend(context.Background(), tenant)
	if err != nil {
		t.Fatalf("createBackend(): %v", err)
	}
	backend.Close()
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if ensureCalls != 1 {
		t.Fatalf("ensureTiDBSchemaForAutoEmbeddingProfile called %d times, want 1", ensureCalls)
	}
}

func TestPoolCreateBackendRepairsFTSOnlySchemaWhenDatabaseAutoEmbeddingDisabled(t *testing.T) {
	pool, tenant := newTestPoolAndTenantWithConfig(t, PoolConfig{
		MaxTenants:                   2,
		DisableDatabaseAutoEmbedding: true,
	}, "tenant-disabled-ensure")
	tenant.Provider = ProviderTiDBZero
	tenant.SchemaVersion = 0

	origEnsure := ensureTiDBSchemaForAutoEmbeddingProfile
	origValidate := validateTiDBSchemaForAutoEmbeddingProfile
	origEnsureFTS := ensureTiDBSchemaForFTSOnlyProfile
	origValidateFTS := validateTiDBSchemaForFTSOnlyProfile
	origApply := applyTiDBAutoEmbeddingProviderConfig
	autoEnsureCalls := 0
	ftsEnsureCalls := 0
	applyCalls := 0
	ensureTiDBSchemaForAutoEmbeddingProfile = func(context.Context, *sql.DB, schema.TiDBAutoEmbeddingProfile) error {
		autoEnsureCalls++
		return nil
	}
	ensureTiDBSchemaForFTSOnlyProfile = func(context.Context, *sql.DB, schema.TiDBAutoEmbeddingProfile) error {
		ftsEnsureCalls++
		return nil
	}
	validateTiDBSchemaForAutoEmbeddingProfile = func(context.Context, *sql.DB, schema.TiDBAutoEmbeddingProfile) error {
		return nil
	}
	validateTiDBSchemaForFTSOnlyProfile = func(context.Context, *sql.DB, schema.TiDBAutoEmbeddingProfile) error {
		return nil
	}
	applyTiDBAutoEmbeddingProviderConfig = func(context.Context, *sql.DB, schema.TiDBAutoEmbeddingProviderConfig) error {
		applyCalls++
		return nil
	}
	t.Cleanup(func() {
		ensureTiDBSchemaForAutoEmbeddingProfile = origEnsure
		validateTiDBSchemaForAutoEmbeddingProfile = origValidate
		ensureTiDBSchemaForFTSOnlyProfile = origEnsureFTS
		validateTiDBSchemaForFTSOnlyProfile = origValidateFTS
		applyTiDBAutoEmbeddingProviderConfig = origApply
	})

	b, store, err := pool.createBackend(context.Background(), tenant)
	if err != nil {
		t.Fatalf("createBackend(): %v", err)
	}
	defer func() {
		b.Close()
		_ = store.Close()
	}()
	if b.UsesDatabaseAutoEmbedding() {
		t.Fatal("backend runtime auto embedding enabled with DisableDatabaseAutoEmbedding=true")
	}
	if autoEnsureCalls != 0 {
		t.Fatalf("ensureTiDBSchemaForAutoEmbeddingProfile called %d times, want 0", autoEnsureCalls)
	}
	if ftsEnsureCalls != 1 {
		t.Fatalf("ensureTiDBSchemaForFTSOnlyProfile called %d times, want 1", ftsEnsureCalls)
	}
	if applyCalls != 0 {
		t.Fatalf("applyTiDBAutoEmbeddingProviderConfig called %d times, want 0", applyCalls)
	}
}

func TestPoolCreateBackendReturnsValidationErrorWhenPeriodicCheckFails(t *testing.T) {
	pool, tenant := newTestPoolAndTenant(t, 2, "tenant-validate-fail")
	tenant.Provider = ProviderTiDBZero
	tenant.SchemaVersion = schema.CurrentTiDBTenantSchemaVersion

	origEnsure := ensureTiDBSchemaForAutoEmbeddingProfile
	origValidate := validateTiDBSchemaForAutoEmbeddingProfile
	origApply := applyTiDBAutoEmbeddingProviderConfig
	origEvery := periodicTiDBSchemaValidationEvery
	ensureTiDBSchemaForAutoEmbeddingProfile = func(context.Context, *sql.DB, schema.TiDBAutoEmbeddingProfile) error {
		return nil
	}
	validateTiDBSchemaForAutoEmbeddingProfile = func(context.Context, *sql.DB, schema.TiDBAutoEmbeddingProfile) error {
		return fmt.Errorf("schema drift")
	}
	applyTiDBAutoEmbeddingProviderConfig = func(context.Context, *sql.DB, schema.TiDBAutoEmbeddingProviderConfig) error {
		return nil
	}
	periodicTiDBSchemaValidationEvery = 1
	t.Cleanup(func() {
		ensureTiDBSchemaForAutoEmbeddingProfile = origEnsure
		validateTiDBSchemaForAutoEmbeddingProfile = origValidate
		applyTiDBAutoEmbeddingProviderConfig = origApply
		periodicTiDBSchemaValidationEvery = origEvery
	})

	if _, _, err := pool.createBackend(context.Background(), tenant); err == nil {
		t.Fatal("expected periodic validation failure to propagate")
	} else if !strings.Contains(err.Error(), "validate tidb embedding schema") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// This test reads global metrics emitted by recordTenantSchemaVersionUpdateFailure
// via operationMetricValue, so it must stay non-parallel unless the metric state
// is isolated.
func TestRecordTenantSchemaVersionUpdateFailureRecordsMetric(t *testing.T) {
	recorder := httptest.NewRecorder()
	metrics.WritePrometheus(recorder)
	before := operationMetricValue(t, recorder.Body.String(), `component="tenant_pool",operation="update_schema_version_failed",result="error"`)

	recordTenantSchemaVersionUpdateFailure(context.Background(), "tenant-metric", 42, time.Millisecond, fmt.Errorf("meta unavailable"))

	recorder = httptest.NewRecorder()
	metrics.WritePrometheus(recorder)
	after := operationMetricValue(t, recorder.Body.String(), `component="tenant_pool",operation="update_schema_version_failed",result="error"`)
	if after != before+1 {
		t.Fatalf("expected update_schema_version_failed metric to increment by 1, before=%d after=%d", before, after)
	}
}

func TestTenantPoolErrorResultClassifiesExpectedDatabaseErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want tenantPoolResult
	}{
		{
			name: "tidb auth failed",
			err:  fmt.Errorf("open db: %w", &mysql.MySQLError{Number: 1045, Message: "Access denied for user"}),
			want: tenantPoolResultAuthFailed,
		},
		{
			name: "tidb auth failed legacy message",
			err:  fmt.Errorf("open db: Error 1045 (28000): Please check your user name and password and try again"),
			want: tenantPoolResultAuthFailed,
		},
		{
			name: "tidb usage quota exhausted",
			err:  fmt.Errorf("open db: %w", &mysql.MySQLError{Number: 1105, Message: "Due to the usage quota being exhausted, access to the cluster has been restricted"}),
			want: tenantPoolResultUsageQuotaExhausted,
		},
		{
			name: "tidb usage quota exhausted lowercase",
			err:  fmt.Errorf("open db: error 1105 (hy000): due to the usage quota being exhausted"),
			want: tenantPoolResultUsageQuotaExhausted,
		},
		{
			name: "not found",
			err:  meta.ErrNotFound,
			want: tenantPoolResultNotFound,
		},
		{
			name: "unexpected",
			err:  fmt.Errorf("open db: invalid connection"),
			want: tenantPoolResultError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tenantPoolErrorResult(tt.err); got != tt.want {
				t.Fatalf("tenantPoolErrorResult() = %q, want %q", got, tt.want)
			}
		})
	}
}

func operationMetricValue(t *testing.T, output, labels string) uint64 {
	t.Helper()
	prefix := `drive9_service_operations_total{` + labels + `} `
	for _, line := range strings.Split(output, "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		value, err := strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, prefix)), 10, 64)
		if err != nil {
			t.Fatalf("parse metric %q: %v", line, err)
		}
		return value
	}
	return 0
}

type poolDummyAudioExtractor struct{}

func (poolDummyAudioExtractor) ExtractAudioText(context.Context, backend.AudioExtractRequest) (string, backend.AudioExtractUsage, error) {
	return "", backend.AudioExtractUsage{}, nil
}

func TestIdleEviction(t *testing.T) {
	pool, tenant := newTestPoolAndTenantWithConfig(t, PoolConfig{
		MaxTenants:       2,
		IdleTimeout:      50 * time.Millisecond,
		IdleReapInterval: 10 * time.Millisecond,
	}, "tenant-idle")
	ctx := context.Background()

	b, release, err := pool.Acquire(ctx, tenant)
	if err != nil {
		t.Fatal(err)
	}
	store := b.Store()
	assertStoreOpen(t, store)
	release()

	// Wait for idle timeout to lapse, then reap manually.
	time.Sleep(60 * time.Millisecond)
	pool.reapOnce(ctx)

	assertStoreClosed(t, store)
	if _, ok := pool.items[tenant.ID]; ok {
		t.Fatal("expected entry to be evicted from pool")
	}
}

func TestIdleEvictionSkippedByRecentAcquire(t *testing.T) {
	pool, tenant := newTestPoolAndTenantWithConfig(t, PoolConfig{
		MaxTenants:       2,
		IdleTimeout:      50 * time.Millisecond,
		IdleReapInterval: 10 * time.Millisecond,
	}, "tenant-recent")
	ctx := context.Background()

	b, release, err := pool.Acquire(ctx, tenant)
	if err != nil {
		t.Fatal(err)
	}
	store := b.Store()
	release()

	// Re-acquire immediately — lastUsed is refreshed to now.
	b2, release2, err := pool.Acquire(ctx, tenant)
	if err != nil {
		t.Fatal(err)
	}
	if b2 != b {
		t.Fatal("expected same backend on cache hit")
	}
	release2()

	// Reap immediately — lastUsed is recent, should not evict.
	pool.reapOnce(ctx)
	assertStoreOpen(t, store)
	if _, ok := pool.items[tenant.ID]; !ok {
		t.Fatal("expected entry to remain in pool after recent acquire")
	}
}

func TestIdleEvictionNotRefreshedByAcquireCached(t *testing.T) {
	pool, tenant := newTestPoolAndTenantWithConfig(t, PoolConfig{
		MaxTenants:       2,
		IdleTimeout:      50 * time.Millisecond,
		IdleReapInterval: 10 * time.Millisecond,
	}, "tenant-cached")
	ctx := context.Background()

	// Cold-open via Acquire so the backend enters the pool.
	b, release, err := pool.Acquire(ctx, tenant)
	if err != nil {
		t.Fatal(err)
	}
	store := b.Store()
	release()

	// Wait long enough that lastUsed is stale, but use AcquireCached
	// before reaping — AcquireCached must NOT refresh lastUsed.
	time.Sleep(60 * time.Millisecond)
	b2, release2, ok := pool.AcquireCached(tenant)
	if !ok {
		t.Fatal("expected AcquireCached to hit warm cache")
	}
	if b2 != b {
		t.Fatal("expected same backend from AcquireCached")
	}
	release2()

	// Reap — entry should be evicted because AcquireCached did not
	// refresh lastUsed.
	pool.reapOnce(ctx)
	assertStoreClosed(t, store)
	if _, ok := pool.items[tenant.ID]; ok {
		t.Fatal("expected entry to be evicted despite AcquireCached touch")
	}
}

func TestIdleEvictionRespectsRefs(t *testing.T) {
	pool, tenant := newTestPoolAndTenantWithConfig(t, PoolConfig{
		MaxTenants:       2,
		IdleTimeout:      50 * time.Millisecond,
		IdleReapInterval: 10 * time.Millisecond,
	}, "tenant-pinned")
	ctx := context.Background()

	b, release, err := pool.Acquire(ctx, tenant)
	if err != nil {
		t.Fatal(err)
	}
	store := b.Store()

	// Wait for idle timeout, but don't release — refs > 0 must prevent
	// eviction.
	time.Sleep(60 * time.Millisecond)
	pool.reapOnce(ctx)
	assertStoreOpen(t, store)
	if _, ok := pool.items[tenant.ID]; !ok {
		t.Fatal("expected pinned entry to remain in pool")
	}

	// Release refreshes lastUsed (user release), so entry stays warm
	// for another IdleTimeout window.
	release()
	pool.reapOnce(ctx)
	assertStoreOpen(t, store)

	// Now wait for idle timeout to lapse after release, then reap.
	time.Sleep(60 * time.Millisecond)
	pool.reapOnce(ctx)
	assertStoreClosed(t, store)
}

func TestIdleEvictionDisabled(t *testing.T) {
	pool, tenant := newTestPoolAndTenantWithConfig(t, PoolConfig{
		MaxTenants:    2,
		IdleTimeout:   0, // disabled
	}, "tenant-disabled")
	ctx := context.Background()

	b, release, err := pool.Acquire(ctx, tenant)
	if err != nil {
		t.Fatal(err)
	}
	store := b.Store()
	release()

	// Wait well beyond any idle timeout, but since IdleTimeout=0 the
	// reaper is disabled and reapOnce is a no-op (idleTimeout is 0 so
	// now.Sub(lastUsed) > 0 is always true — but Start won't have been
	// called). Call reapOnce directly to confirm it's a no-op.
	time.Sleep(20 * time.Millisecond)
	pool.reapOnce(ctx)
	assertStoreOpen(t, store)
	if _, ok := pool.items[tenant.ID]; !ok {
		t.Fatal("expected entry to remain when idle eviction is disabled")
	}
}

func TestIdleEvictionStartNoOpWhenDisabled(t *testing.T) {
	pool := NewPool(PoolConfig{
		MaxTenants:  2,
		IdleTimeout: 0,
	}, nil)
	pool.Start(context.Background())
	if pool.reapStop != nil {
		t.Fatal("expected reapStop to be nil when IdleTimeout=0")
	}
}

// TestIdleEvictionReleaseRefreshesLastUsed verifies that a long-running user
// request (held longer than IdleTimeout) is not immediately evicted on
// release. The release path must refresh lastUsed so the idle timer starts
// counting from "user finished", not "user started".
func TestIdleEvictionReleaseRefreshesLastUsed(t *testing.T) {
	pool, tenant := newTestPoolAndTenantWithConfig(t, PoolConfig{
		MaxTenants:       2,
		IdleTimeout:      50 * time.Millisecond,
		IdleReapInterval: 10 * time.Millisecond,
	}, "tenant-long")
	ctx := context.Background()

	b, release, err := pool.Acquire(ctx, tenant)
	if err != nil {
		t.Fatal(err)
	}
	store := b.Store()

	// Hold the backend longer than IdleTimeout. The reaper must skip it
	// because refs > 0.
	time.Sleep(80 * time.Millisecond)
	pool.reapOnce(ctx)
	assertStoreOpen(t, store)

	// Release now — lastUsed is refreshed. Immediate reap must NOT evict.
	release()
	pool.reapOnce(ctx)
	assertStoreOpen(t, store)
	if _, ok := pool.items[tenant.ID]; !ok {
		t.Fatal("expected entry to remain after release refreshed lastUsed")
	}

	// Wait for idle timeout to lapse after release, then reap.
	time.Sleep(60 * time.Millisecond)
	pool.reapOnce(ctx)
	assertStoreClosed(t, store)
}

// TestIdleEvictionAcquireCachedReleaseDoesNotRefresh verifies that
// AcquireCached's release does not refresh lastUsed. A safety-net scan
// touch must not reset the idle timer even on release.
func TestIdleEvictionAcquireCachedReleaseDoesNotRefresh(t *testing.T) {
	pool, tenant := newTestPoolAndTenantWithConfig(t, PoolConfig{
		MaxTenants:       2,
		IdleTimeout:      50 * time.Millisecond,
		IdleReapInterval: 10 * time.Millisecond,
	}, "tenant-cached-release")
	ctx := context.Background()

	b, release, err := pool.Acquire(ctx, tenant)
	if err != nil {
		t.Fatal(err)
	}
	store := b.Store()
	release()

	// Wait until lastUsed is stale.
	time.Sleep(60 * time.Millisecond)

	// AcquireCached + release should not refresh lastUsed.
	_, release2, ok := pool.AcquireCached(tenant)
	if !ok {
		t.Fatal("expected AcquireCached to hit warm cache")
	}
	release2()

	// Reap — should evict because neither AcquireCached nor its release
	// refreshed lastUsed.
	pool.reapOnce(ctx)
	assertStoreClosed(t, store)
	if _, ok := pool.items[tenant.ID]; ok {
		t.Fatal("expected entry to be evicted — AcquireCached release must not refresh lastUsed")
	}
}

// TestIdleEvictionStartAutoEvictsAndCloseStops verifies the end-to-end
// lifecycle: Start launches the reaper, the ticker automatically evicts an
// idle entry, and Close stops the reaper cleanly.
func TestIdleEvictionStartAutoEvictsAndCloseStops(t *testing.T) {
	pool, tenant := newTestPoolAndTenantWithConfig(t, PoolConfig{
		MaxTenants:       2,
		IdleTimeout:      50 * time.Millisecond,
		IdleReapInterval: 10 * time.Millisecond,
	}, "tenant-e2e")
	ctx := context.Background()
	pool.Start(ctx)
	defer pool.Close()

	b, release, err := pool.Acquire(ctx, tenant)
	if err != nil {
		t.Fatal(err)
	}
	store := b.Store()
	release()

	// Wait for the ticker-driven reaper to auto-evict (within ~2s).
	deadline := time.Now().Add(2 * time.Second)
	evicted := false
	for time.Now().Before(deadline) {
		pool.mu.Lock()
		_, ok := pool.items[tenant.ID]
		pool.mu.Unlock()
		if !ok {
			evicted = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !evicted {
		t.Fatal("expected auto-eviction by ticker")
	}
	assertStoreClosed(t, store)
}
