package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/dat9/internal/testmysql"
	"github.com/mem9-ai/dat9/pkg/encrypt"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/s3client"
	"github.com/mem9-ai/dat9/pkg/tenant"
	"github.com/mem9-ai/dat9/pkg/tenant/token"
)

type tenantDeleteRuntime struct {
	meta        *meta.Store
	pool        *tenant.Pool
	tokenSecret []byte
	tenantID    string
	apiKey      string
	prov        *fakeProvisioner
	server      *Server
}

func newTenantDeleteRuntime(t *testing.T, provider string, scopeKind meta.APIKeyScopeKind) *tenantDeleteRuntime {
	t.Helper()
	db := newTenantDeleteDBInfo(t)
	testmysql.ResetMetaDB(t, db.Meta.DB())
	testmysql.ResetDB(t, db.Meta.DB())
	t.Cleanup(func() {
		testmysql.ResetMetaDB(t, db.Meta.DB())
		testmysql.ResetDB(t, db.Meta.DB())
	})
	tenantID := token.NewID()
	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	apiKey, err := token.IssueToken(tokenSecret, tenantID, 1)
	if err != nil {
		t.Fatal(err)
	}
	apiKeyCipher, err := db.Pool.Encrypt(context.Background(), []byte(apiKey))
	if err != nil {
		t.Fatal(err)
	}
	dbPassCipher, err := db.Pool.Encrypt(context.Background(), []byte(db.DBPass))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := db.Meta.InsertTenant(context.Background(), &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantActive,
		Kind:             meta.TenantKindLive,
		DBHost:           db.DBHost,
		DBPort:           db.DBPort,
		DBUser:           db.DBUser,
		DBPasswordCipher: dbPassCipher,
		DBName:           db.DBName,
		DBTLS:            false,
		Provider:         provider,
		ClusterID:        "cluster-delete-1",
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Meta.InsertAPIKey(context.Background(), &meta.APIKey{
		ID:            token.NewID(),
		TenantID:      tenantID,
		KeyName:       "default",
		JWTCiphertext: apiKeyCipher,
		JWTHash:       token.HashToken(apiKey),
		TokenVersion:  1,
		Status:        meta.APIKeyActive,
		ScopeKind:     scopeKind,
		IssuedAt:      now,
		CreatedAt:     now,
		UpdatedAt:     now,
	}); err != nil {
		t.Fatal(err)
	}
	prov := &fakeProvisioner{provider: provider, cluster: &tenant.ClusterInfo{}}
	server := NewWithConfig(Config{Meta: db.Meta, Pool: db.Pool, Provisioner: prov, TokenSecret: tokenSecret})
	t.Cleanup(server.Close)
	return &tenantDeleteRuntime{
		meta:        db.Meta,
		pool:        db.Pool,
		tokenSecret: tokenSecret,
		tenantID:    tenantID,
		apiKey:      apiKey,
		prov:        prov,
		server:      server,
	}
}

func newTenantDeleteDBInfo(t *testing.T) *testDBInfo {
	t.Helper()
	if testDSN == "" {
		t.Skip("no test database available")
	}
	dropTenantSchemaLLMUsage(t)
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	initServerTenantSchema(t, testDSN)

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
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	enc, err := encrypt.NewLocalAESEncryptor(master)
	if err != nil {
		t.Fatal(err)
	}
	pool := tenant.NewPool(tenant.PoolConfig{S3Dir: mustTempDir(t), PublicURL: "http://localhost", SkipTiDBSchemaCheck: true}, enc)
	pool.SetMetaStore(metaStore)
	t.Cleanup(pool.Close)
	return &testDBInfo{Meta: metaStore, Pool: pool, DBHost: host, DBPort: port, DBUser: parsed.User, DBPass: parsed.Passwd, DBName: parsed.DBName}
}

func dropTenantSchemaLLMUsage(t *testing.T) {
	t.Helper()
	db := testmysql.OpenDB(t, testDSN)
	var tableCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'llm_usage'`).Scan(&tableCount); err != nil {
		t.Fatalf("check llm_usage table: %v", err)
	}
	if tableCount == 0 {
		return
	}
	var tenantIDColumnCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'llm_usage' AND column_name = 'tenant_id'`).Scan(&tenantIDColumnCount); err != nil {
		t.Fatalf("check llm_usage tenant_id column: %v", err)
	}
	if tenantIDColumnCount > 0 {
		return
	}
	if _, err := db.Exec("DROP TABLE llm_usage"); err != nil {
		t.Fatalf("drop tenant-schema llm_usage: %v", err)
	}
}

func (rt *tenantDeleteRuntime) deleteTenant(t *testing.T, body any) *http.Response {
	t.Helper()
	var reader *bytes.Reader
	if body == nil {
		reader = bytes.NewReader(nil)
	} else {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}
	req := httptest.NewRequest(http.MethodDelete, "/v1/tenant", reader)
	req.Header.Set("Authorization", "Bearer "+rt.apiKey)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	rt.server.ServeHTTP(rec, req)
	return rec.Result()
}

func TestTenantDeleteNativeRequiresOwnerAndTiDBCloudCredentials(t *testing.T) {
	rt := newTenantDeleteRuntime(t, tenant.ProviderTiDBCloudNative, meta.APIKeyScopeKindOwner)
	resp := rt.deleteTenant(t, map[string]string{"public_key": "public-1"})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 0 {
		t.Fatalf("deprovision calls = %d, want 0", got)
	}
	var status string
	if err := rt.meta.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", rt.tenantID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantActive) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantActive)
	}
}

func TestTenantDeleteRejectsScopedAPIKey(t *testing.T) {
	rt := newTenantDeleteRuntime(t, tenant.ProviderTiDBCloudNative, meta.APIKeyScopeKindFS)
	resp := rt.deleteTenant(t, map[string]string{"public_key": "public-1", "private_key": "private-1"})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusForbidden)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 0 {
		t.Fatalf("deprovision calls = %d, want 0", got)
	}
}

func TestTenantDeleteNativeRejectsDatabaseNameCredentialField(t *testing.T) {
	rt := newTenantDeleteRuntime(t, tenant.ProviderTiDBCloudNative, meta.APIKeyScopeKindOwner)
	req := httptest.NewRequest(http.MethodDelete, "/v1/tenant", strings.NewReader(`{"public_key":"public-1","private_key":"private-1","database_name":"ignored"}`))
	req.Header.Set("Authorization", "Bearer "+rt.apiKey)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	rt.server.ServeHTTP(rec, req)
	resp := rec.Result()
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 0 {
		t.Fatalf("deprovision calls = %d, want 0", got)
	}
}

func TestTenantDeleteNativeSucceedsWithCredentials(t *testing.T) {
	rt := newTenantDeleteRuntime(t, tenant.ProviderTiDBCloudNative, meta.APIKeyScopeKindOwner)
	resp := rt.deleteTenant(t, map[string]string{"public_key": "public-1", "private_key": "private-1"})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 1 {
		t.Fatalf("deprovision calls = %d, want 1", got)
	}
	if rt.prov.lastCredentialReq.PublicKey != "public-1" || rt.prov.lastCredentialReq.PrivateKey != "private-1" {
		t.Fatalf("credential request = %+v", rt.prov.lastCredentialReq)
	}
	if rt.prov.lastDeprovision == nil || rt.prov.lastDeprovision.ClusterID != "cluster-delete-1" {
		t.Fatalf("deprovision cluster = %+v", rt.prov.lastDeprovision)
	}
	assertTenantDeletedAndKeysRevoked(t, rt)
}

func TestTenantDeleteStarterUsesServerCredentials(t *testing.T) {
	rt := newTenantDeleteRuntime(t, tenant.ProviderTiDBCloudStarter, meta.APIKeyScopeKindOwner)
	resp := rt.deleteTenant(t, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 1 {
		t.Fatalf("deprovision calls = %d, want 1", got)
	}
	if rt.prov.lastCredentialReq.PublicKey != "" || rt.prov.lastCredentialReq.PrivateKey != "" {
		t.Fatalf("starter delete should not use customer credentials: %+v", rt.prov.lastCredentialReq)
	}
	assertTenantDeletedAndKeysRevoked(t, rt)
}

func TestTenantDeleteWaitsForBorrowedBackendBeforeClusterDelete(t *testing.T) {
	rt := newTenantDeleteRuntime(t, tenant.ProviderTiDBCloudStarter, meta.APIKeyScopeKindOwner)
	ctx := context.Background()
	tenantMeta, err := rt.meta.GetTenant(ctx, rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	_, release, err := rt.pool.Acquire(ctx, tenantMeta)
	if err != nil {
		t.Fatalf("acquire tenant backend: %v", err)
	}
	defer release()

	respCh := make(chan *http.Response, 1)
	go func() {
		respCh <- rt.deleteTenant(t, nil)
	}()
	waitTenantDeleteStatus(t, rt, meta.TenantDeleting)
	if got := rt.prov.deprovisionCalls.Load(); got != 0 {
		t.Fatalf("deprovision calls before backend release = %d, want 0", got)
	}
	select {
	case resp := <-respCh:
		defer func() { _ = resp.Body.Close() }()
		t.Fatalf("delete response returned before backend release: status %d", resp.StatusCode)
	default:
	}

	release()
	resp := receiveTenantDeleteResponse(t, respCh)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 1 {
		t.Fatalf("deprovision calls after backend release = %d, want 1", got)
	}
	assertTenantDeletingAndKeysRevoked(t, rt)
	rt.server.processTenantDeleteJobs(ctx)
	assertTenantDeletedAndKeysRevoked(t, rt)
}

func TestTenantDeleteRejectsNonDeletedFork(t *testing.T) {
	rt := newTenantDeleteRuntime(t, tenant.ProviderTiDBCloudStarter, meta.APIKeyScopeKindOwner)
	ctx := context.Background()
	tenantMeta, err := rt.meta.GetTenant(ctx, rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if _, release, err := rt.pool.Acquire(ctx, tenantMeta); err != nil {
		t.Fatalf("acquire tenant backend: %v", err)
	} else {
		release()
	}
	tenantMeta, err = rt.meta.GetTenant(ctx, rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := rt.meta.InsertTenant(ctx, &meta.Tenant{
		ID:                 token.NewID(),
		Status:             meta.TenantActive,
		Kind:               meta.TenantKindFork,
		ParentTenantID:     rt.tenantID,
		StorageNamespaceID: tenantMeta.StorageNamespaceID,
		DBHost:             tenantMeta.DBHost,
		DBPort:             tenantMeta.DBPort,
		DBUser:             tenantMeta.DBUser,
		DBPasswordCipher:   tenantMeta.DBPasswordCipher,
		DBName:             tenantMeta.DBName,
		DBTLS:              tenantMeta.DBTLS,
		Provider:           tenantMeta.Provider,
		ClusterID:          tenantMeta.ClusterID,
		BranchID:           "branch-a",
		SchemaVersion:      tenantMeta.SchemaVersion,
		CreatedAt:          now,
		UpdatedAt:          now,
	}); err != nil {
		t.Fatal(err)
	}

	resp := rt.deleteTenant(t, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusConflict)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 0 {
		t.Fatalf("deprovision calls = %d, want 0", got)
	}
	var status string
	if err := rt.meta.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", rt.tenantID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantActive) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantActive)
	}
}

func TestTenantDeleteRemovesS3PrefixAsyncAfterClusterDelete(t *testing.T) {
	rt := newTenantDeleteRuntime(t, tenant.ProviderTiDBCloudStarter, meta.APIKeyScopeKindOwner)
	ctx := context.Background()
	payload := deterministicObjectGCPayload(8*1024*1024, 0x72)

	tenantMeta, err := rt.meta.GetTenant(ctx, rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	backendFS, release, err := rt.pool.Acquire(ctx, tenantMeta)
	if err != nil {
		t.Fatalf("acquire tenant backend: %v", err)
	}
	if _, err := backendFS.Write("/large.bin", payload, 0, filesystem.WriteFlagCreate); err != nil {
		release()
		t.Fatalf("write s3 file: %v", err)
	}
	storageRef := objectGCFileStorageRef(t, backendFS, "/large.bin")
	tenantMeta, err = rt.meta.GetTenant(ctx, rt.tenantID)
	if err != nil {
		release()
		t.Fatal(err)
	}
	namespaceID := tenantMeta.StorageNamespaceID
	assertTenantDeleteS3ObjectBytes(t, rt, namespaceID, storageRef, payload)
	release()

	resp := rt.deleteTenant(t, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 1 {
		t.Fatalf("deprovision calls = %d, want 1", got)
	}
	assertTenantDeletingAndKeysRevoked(t, rt)
	assertTenantDeleteS3ObjectBytes(t, rt, namespaceID, storageRef, payload)

	rt.server.processTenantDeleteJobs(ctx)
	assertTenantDeletedAndKeysRevoked(t, rt)

	assertTenantDeleteS3ObjectMissing(t, rt, namespaceID, storageRef)
}

func TestTenantDeleteAsyncCleanupAbortsActiveMultipartUploads(t *testing.T) {
	rt := newTenantDeleteRuntime(t, tenant.ProviderTiDBCloudStarter, meta.APIKeyScopeKindOwner)
	ctx := context.Background()
	payload := deterministicObjectGCPayload(8*1024*1024, 0x29)

	tenantMeta, err := rt.meta.GetTenant(ctx, rt.tenantID)
	if err != nil {
		t.Fatal(err)
	}
	backendFS, release, err := rt.pool.Acquire(ctx, tenantMeta)
	if err != nil {
		t.Fatalf("acquire tenant backend: %v", err)
	}
	plan, err := backendFS.InitiateUpload(ctx, "/uploading.bin", int64(len(payload)))
	if err != nil {
		release()
		t.Fatalf("initiate upload: %v", err)
	}
	if len(plan.Parts) == 0 {
		release()
		t.Fatal("upload plan has no parts")
	}
	uploadBeforeDelete, err := backendFS.Store().GetUpload(ctx, plan.UploadID)
	if err != nil {
		release()
		t.Fatalf("get upload before delete: %v", err)
	}
	if _, err := backendFS.S3().(*s3client.LocalS3Client).UploadPart(ctx, uploadBeforeDelete.S3UploadID, plan.Parts[0].Number, bytes.NewReader(payload[:plan.Parts[0].Size])); err != nil {
		release()
		t.Fatalf("upload part: %v", err)
	}
	tenantMeta, err = rt.meta.GetTenant(ctx, rt.tenantID)
	if err != nil {
		release()
		t.Fatal(err)
	}
	namespaceID := tenantMeta.StorageNamespaceID
	release()

	resp := rt.deleteTenant(t, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 1 {
		t.Fatalf("deprovision calls = %d, want 1", got)
	}
	assertTenantDeletingAndKeysRevoked(t, rt)

	rt.server.processTenantDeleteJobs(ctx)
	assertTenantDeletedAndKeysRevoked(t, rt)

	s3c := tenantDeleteS3ForNamespace(t, rt, namespaceID)
	parts, err := s3c.ListParts(ctx, uploadBeforeDelete.S3Key, uploadBeforeDelete.S3UploadID)
	if err == nil {
		if len(parts) != 0 {
			t.Fatalf("multipart parts after delete = %d, want 0", len(parts))
		}
		t.Fatal("multipart upload still exists after tenant delete")
	}
	if !strings.Contains(err.Error(), "upload not found") {
		t.Fatalf("list parts after delete error = %v, want upload not found", err)
	}
}

func tenantDeleteS3ForNamespace(t *testing.T, rt *tenantDeleteRuntime, namespaceID string) s3client.S3Client {
	t.Helper()
	ns, err := rt.meta.GetStorageNamespace(context.Background(), namespaceID)
	if err != nil {
		t.Fatalf("get storage namespace: %v", err)
	}
	s3c, err := rt.pool.S3ForStorageNamespace(context.Background(), ns)
	if err != nil {
		t.Fatalf("s3 for storage namespace: %v", err)
	}
	return s3c
}

func assertTenantDeleteS3ObjectBytes(t *testing.T, rt *tenantDeleteRuntime, namespaceID, storageRef string, want []byte) {
	t.Helper()
	reader, err := tenantDeleteS3ForNamespace(t, rt, namespaceID).GetObject(context.Background(), storageRef)
	if err != nil {
		t.Fatalf("get s3 object %q: %v", storageRef, err)
	}
	defer func() { _ = reader.Close() }()
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read s3 object %q: %v", storageRef, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("s3 object %q bytes mismatch: got %d bytes want %d bytes", storageRef, len(got), len(want))
	}
}

func assertTenantDeleteS3ObjectMissing(t *testing.T, rt *tenantDeleteRuntime, namespaceID, storageRef string) {
	t.Helper()
	reader, err := tenantDeleteS3ForNamespace(t, rt, namespaceID).GetObject(context.Background(), storageRef)
	if err == nil {
		_ = reader.Close()
		t.Fatalf("s3 object %q still exists after tenant delete", storageRef)
	}
}

func TestTenantDeleteTiDBZeroUnsupported(t *testing.T) {
	rt := newTenantDeleteRuntime(t, tenant.ProviderTiDBZero, meta.APIKeyScopeKindOwner)
	resp := rt.deleteTenant(t, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusConflict)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 0 {
		t.Fatalf("deprovision calls = %d, want 0", got)
	}
}

func assertTenantDeletedAndKeysRevoked(t *testing.T, rt *tenantDeleteRuntime) {
	t.Helper()
	var status string
	if err := rt.meta.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", rt.tenantID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantDeleted) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantDeleted)
	}
	var activeKeys int
	if err := rt.meta.DB().QueryRow("SELECT COUNT(*) FROM tenant_api_keys WHERE tenant_id = ? AND status = ?", rt.tenantID, meta.APIKeyActive).Scan(&activeKeys); err != nil {
		t.Fatal(err)
	}
	if activeKeys != 0 {
		t.Fatalf("active api keys = %d, want 0", activeKeys)
	}
	var revokedKeys int
	if err := rt.meta.DB().QueryRow("SELECT COUNT(*) FROM tenant_api_keys WHERE tenant_id = ? AND status = ?", rt.tenantID, meta.APIKeyRevoked).Scan(&revokedKeys); err != nil {
		t.Fatal(err)
	}
	if revokedKeys != 1 {
		t.Fatalf("revoked api keys = %d, want 1", revokedKeys)
	}
}

func waitTenantDeleteStatus(t *testing.T, rt *tenantDeleteRuntime, want meta.TenantStatus) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var status string
		if err := rt.meta.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", rt.tenantID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == string(want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant status did not become %s, last status %s", want, status)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func receiveTenantDeleteResponse(t *testing.T, ch <-chan *http.Response) *http.Response {
	t.Helper()
	select {
	case resp := <-ch:
		return resp
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for delete response")
		return nil
	}
}

func assertTenantDeletingAndKeysRevoked(t *testing.T, rt *tenantDeleteRuntime) {
	t.Helper()
	var status string
	if err := rt.meta.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", rt.tenantID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantDeleting) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantDeleting)
	}
	var activeKeys int
	if err := rt.meta.DB().QueryRow("SELECT COUNT(*) FROM tenant_api_keys WHERE tenant_id = ? AND status = ?", rt.tenantID, meta.APIKeyActive).Scan(&activeKeys); err != nil {
		t.Fatal(err)
	}
	if activeKeys != 0 {
		t.Fatalf("active api keys = %d, want 0", activeKeys)
	}
}

func TestTenantDeleteClusterFailureLeavesTenantActiveForCredentialRetry(t *testing.T) {
	rt := newTenantDeleteRuntime(t, tenant.ProviderTiDBCloudNative, meta.APIKeyScopeKindOwner)
	rt.prov.deprovisionErr = fmt.Errorf("upstream down")
	resp := rt.deleteTenant(t, map[string]string{"public_key": "public-1", "private_key": "private-1"})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadGateway)
	}
	var status string
	if err := rt.meta.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", rt.tenantID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantActive) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantActive)
	}
}

func TestTenantDeleteCanRetryDeletingTenant(t *testing.T) {
	rt := newTenantDeleteRuntime(t, tenant.ProviderTiDBCloudNative, meta.APIKeyScopeKindOwner)
	if err := rt.meta.UpdateTenantStatus(context.Background(), rt.tenantID, meta.TenantDeleting); err != nil {
		t.Fatal(err)
	}

	resp := rt.deleteTenant(t, map[string]string{"public_key": "public-1", "private_key": "private-1"})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 1 {
		t.Fatalf("deprovision calls = %d, want 1", got)
	}
	rt.server.processTenantDeleteJobs(context.Background())
	assertTenantDeletedAndKeysRevoked(t, rt)
}

func TestTenantDeleteKeepsOwnerKeyWhenCleanupEnqueueFails(t *testing.T) {
	rt := newTenantDeleteRuntime(t, tenant.ProviderTiDBCloudNative, meta.APIKeyScopeKindOwner)
	ctx := context.Background()
	if _, err := rt.meta.DB().ExecContext(ctx, "UPDATE tenants SET storage_namespace_id = ? WHERE id = ?", "missing-namespace", rt.tenantID); err != nil {
		t.Fatal(err)
	}

	resp := rt.deleteTenant(t, map[string]string{"public_key": "public-1", "private_key": "private-1"})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusInternalServerError)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 1 {
		t.Fatalf("deprovision calls = %d, want 1", got)
	}
	var status string
	if err := rt.meta.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", rt.tenantID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantDeleting) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantDeleting)
	}
	var activeKeys int
	if err := rt.meta.DB().QueryRow("SELECT COUNT(*) FROM tenant_api_keys WHERE tenant_id = ? AND status = ?", rt.tenantID, meta.APIKeyActive).Scan(&activeKeys); err != nil {
		t.Fatal(err)
	}
	if activeKeys != 1 {
		t.Fatalf("active api keys = %d, want 1", activeKeys)
	}
}

func TestTenantDeleteDeletingTenantWithCleanupJobDoesNotRepeatClusterDelete(t *testing.T) {
	rt := newTenantDeleteRuntime(t, tenant.ProviderTiDBCloudNative, meta.APIKeyScopeKindOwner)
	ctx := context.Background()
	if err := rt.meta.UpdateTenantStatus(ctx, rt.tenantID, meta.TenantDeleting); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.EnqueueTenantDeleteJob(ctx, &meta.TenantDeleteJob{
		TenantID:    rt.tenantID,
		NamespaceID: rt.tenantID,
		Backend:     "local",
		Prefix:      rt.tenantID + "/",
	}); err != nil {
		t.Fatal(err)
	}

	resp := rt.deleteTenant(t, map[string]string{"public_key": "public-1", "private_key": "private-1"})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 0 {
		t.Fatalf("deprovision calls = %d, want 0", got)
	}
}
