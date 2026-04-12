package tenant

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/dat9/internal/testmysql"
	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/encrypt"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/semantic"
)

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

func newTestPoolAndTenant(t *testing.T, maxTenants int, tenantID string) (*Pool, *meta.Tenant) {
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
	pool := NewPool(PoolConfig{MaxTenants: maxTenants}, enc)
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
		`CREATE TABLE IF NOT EXISTS file_nodes (node_id VARCHAR(64) PRIMARY KEY, path VARCHAR(512) NOT NULL, parent_path VARCHAR(512) NOT NULL, name VARCHAR(255) NOT NULL, is_directory BOOLEAN NOT NULL DEFAULT FALSE, file_id VARCHAR(64), created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3))`,
		`CREATE UNIQUE INDEX idx_path ON file_nodes(path)`,
		`CREATE INDEX idx_parent ON file_nodes(parent_path)`,
		`CREATE INDEX idx_file_id ON file_nodes(file_id)`,
		`CREATE TABLE IF NOT EXISTS files (file_id VARCHAR(64) PRIMARY KEY, storage_type VARCHAR(32) NOT NULL, storage_ref TEXT NOT NULL, content_blob LONGBLOB, content_type VARCHAR(255), size_bytes BIGINT NOT NULL DEFAULT 0, checksum_sha256 VARCHAR(128), revision BIGINT NOT NULL DEFAULT 1, status VARCHAR(32) NOT NULL DEFAULT 'PENDING', source_id VARCHAR(255), content_text LONGTEXT, embedding LONGTEXT, embedding_revision BIGINT, created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), confirmed_at DATETIME(3), expires_at DATETIME(3))`,
		`CREATE INDEX idx_status ON files(status, created_at)`,
		`CREATE TABLE IF NOT EXISTS file_tags (file_id VARCHAR(64) NOT NULL, tag_key VARCHAR(255) NOT NULL, tag_value VARCHAR(255) NOT NULL DEFAULT '', PRIMARY KEY (file_id, tag_key))`,
		`CREATE INDEX idx_kv ON file_tags(tag_key, tag_value)`,
		`CREATE TABLE IF NOT EXISTS uploads (upload_id VARCHAR(64) PRIMARY KEY, file_id VARCHAR(64) NOT NULL, target_path VARCHAR(512) NOT NULL, s3_upload_id VARCHAR(255) NOT NULL, s3_key VARCHAR(2048) NOT NULL, total_size BIGINT NOT NULL, part_size BIGINT NOT NULL, parts_total INT NOT NULL, status VARCHAR(32) NOT NULL DEFAULT 'UPLOADING', fingerprint_sha256 VARCHAR(128), idempotency_key VARCHAR(255), created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3), expires_at DATETIME(3) NOT NULL, active_target_path VARCHAR(512) AS (CASE WHEN status = 'UPLOADING' THEN target_path ELSE NULL END) STORED)`,
		`CREATE INDEX idx_upload_path ON uploads(target_path, status)`,
		`CREATE UNIQUE INDEX idx_idempotency ON uploads(idempotency_key)`,
		`CREATE TABLE IF NOT EXISTS semantic_tasks (task_id VARCHAR(64) PRIMARY KEY, task_type VARCHAR(32) NOT NULL, resource_id VARCHAR(64) NOT NULL, resource_version BIGINT NOT NULL, status VARCHAR(20) NOT NULL, attempt_count INT NOT NULL DEFAULT 0, max_attempts INT NOT NULL DEFAULT 5, receipt VARCHAR(128), leased_at DATETIME(3), lease_until DATETIME(3), available_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), payload_json JSON, last_error TEXT, created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), completed_at DATETIME(3))`,
		`CREATE UNIQUE INDEX uk_task_resource_version ON semantic_tasks(task_type, resource_id, resource_version)`,
		`CREATE INDEX idx_task_claim ON semantic_tasks(status, available_at, lease_until, created_at)`,
		`CREATE INDEX idx_task_claim_type ON semantic_tasks(status, task_type, available_at, created_at, task_id)`,
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

type poolDummyAudioExtractor struct{}

func (poolDummyAudioExtractor) ExtractAudioText(context.Context, backend.AudioExtractRequest) (string, error) {
	return "", nil
}
