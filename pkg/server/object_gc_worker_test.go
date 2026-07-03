package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/go-sql-driver/mysql"
	backendpkg "github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/encrypt"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
)

type objectGCTestRuntime struct {
	meta   *meta.Store
	pool   *tenant.Pool
	dbHost string
	dbPort int
	dbUser string
	dbPass string
	dbName string
}

func newObjectGCTestRuntime(t *testing.T) *objectGCTestRuntime {
	t.Helper()
	if testDSN == "" {
		t.Skip("no test database available")
	}
	initServerTenantSchema(t, testDSN)
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	cleanupForkTestTables(t, metaStore)

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
	return &objectGCTestRuntime{meta: metaStore, pool: pool, dbHost: host, dbPort: port, dbUser: parsed.User, dbPass: parsed.Passwd, dbName: parsed.DBName}
}

func (rt *objectGCTestRuntime) insertTenant(t *testing.T, id string, status meta.TenantStatus, kind meta.TenantKind, namespaceID string) {
	t.Helper()
	ctx := context.Background()
	passCipher, err := rt.pool.Encrypt(ctx, []byte(rt.dbPass))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := rt.meta.InsertTenant(ctx, &meta.Tenant{
		ID:                 id,
		Status:             status,
		Kind:               kind,
		ParentTenantID:     "",
		StorageNamespaceID: namespaceID,
		DBHost:             rt.dbHost,
		DBPort:             rt.dbPort,
		DBUser:             rt.dbUser,
		DBPasswordCipher:   passCipher,
		DBName:             rt.dbName,
		DBTLS:              false,
		Provider:           tenant.ProviderTiDBCloudNative,
		ClusterID:          "cluster-a",
		BranchID:           "branch-a",
		SchemaVersion:      1,
		CreatedAt:          now,
		UpdatedAt:          now,
	}); err != nil {
		t.Fatal(err)
	}
}

func (rt *objectGCTestRuntime) upsertNamespace(t *testing.T, id, ownerID string) {
	t.Helper()
	if err := rt.meta.UpsertStorageNamespace(context.Background(), &meta.StorageNamespace{
		ID:            id,
		OwnerTenantID: ownerID,
		Backend:       "local",
		Prefix:        ownerID + "/",
		State:         meta.StorageNamespaceActive,
	}); err != nil {
		t.Fatal(err)
	}
}

func (rt *objectGCTestRuntime) enqueueCandidate(t *testing.T, namespaceID, storageRef string) meta.ObjectGCCandidate {
	t.Helper()
	ctx := context.Background()
	if err := rt.meta.EnqueueObjectGCCandidate(ctx, &meta.ObjectGCCandidateInput{
		NamespaceID:    namespaceID,
		StorageRef:     storageRef,
		StorageRefHash: datastore.StorageRefHash(storageRef),
		Reason:         meta.ObjectGCReasonOverwrite,
		NotBefore:      time.Now().UTC().Add(-time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	candidates, err := rt.meta.ListDueObjectGCCandidates(ctx, time.Now().UTC(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidate count = %d, want 1: %+v", len(candidates), candidates)
	}
	return candidates[0]
}

func objectGCCandidateRow(t *testing.T, db *sql.DB, namespaceID, storageRef string) (meta.ObjectGCCandidateState, string) {
	t.Helper()
	var state meta.ObjectGCCandidateState
	var lastError sql.NullString
	if err := db.QueryRow(`SELECT state, last_error FROM object_gc_candidates WHERE namespace_id = ? AND storage_ref_hash = ? AND storage_ref = ?`,
		namespaceID, datastore.StorageRefHash(storageRef), storageRef).Scan(&state, &lastError); err != nil {
		t.Fatal(err)
	}
	return state, lastError.String
}

func TestObjectGCSkipsSharedBlobWhenForkExists(t *testing.T) {
	rt := newObjectGCTestRuntime(t)
	rt.upsertNamespace(t, "ns-a", "owner-a")
	rt.insertTenant(t, "fork-a", meta.TenantProvisioning, meta.TenantKindFork, "ns-a")
	candidate := rt.enqueueCandidate(t, "ns-a", "blobs/shared")

	worker := &objectGCWorker{meta: rt.meta, pool: rt.pool}
	if err := worker.processCandidate(context.Background(), candidate); err != nil {
		t.Fatal(err)
	}

	state, lastError := objectGCCandidateRow(t, rt.meta.DB(), "ns-a", "blobs/shared")
	if state != meta.ObjectGCCandidatePending || lastError != "namespace has active fork" {
		t.Fatalf("candidate state=%s last_error=%q", state, lastError)
	}
}

func TestObjectGCPostponesWhenNamespaceOwnerInactive(t *testing.T) {
	rt := newObjectGCTestRuntime(t)
	rt.upsertNamespace(t, "ns-a", "owner-a")
	rt.insertTenant(t, "owner-a", meta.TenantProvisioning, meta.TenantKindLive, "ns-a")
	candidate := rt.enqueueCandidate(t, "ns-a", "blobs/inactive-owner")

	worker := &objectGCWorker{meta: rt.meta, pool: rt.pool}
	if err := worker.processCandidate(context.Background(), candidate); err != nil {
		t.Fatal(err)
	}

	state, lastError := objectGCCandidateRow(t, rt.meta.DB(), "ns-a", "blobs/inactive-owner")
	if state != meta.ObjectGCCandidatePending || lastError != "namespace owner is not active" {
		t.Fatalf("candidate state=%s last_error=%q", state, lastError)
	}
}

func TestObjectGCDeletesUnreachableBlob(t *testing.T) {
	rt := newObjectGCTestRuntime(t)
	rt.upsertNamespace(t, "ns-a", "owner-a")
	rt.insertTenant(t, "owner-a", meta.TenantActive, meta.TenantKindLive, "ns-a")
	candidate := rt.enqueueCandidate(t, "ns-a", "blobs/unreachable")

	worker := &objectGCWorker{meta: rt.meta, pool: rt.pool}
	if err := worker.processCandidate(context.Background(), candidate); err != nil {
		t.Fatal(err)
	}

	state, lastError := objectGCCandidateRow(t, rt.meta.DB(), "ns-a", "blobs/unreachable")
	if state != meta.ObjectGCCandidateDeleted || lastError != "" {
		t.Fatalf("candidate state=%s last_error=%q", state, lastError)
	}
}

func TestObjectGCPostponesReachableBlob(t *testing.T) {
	rt := newObjectGCTestRuntime(t)
	ctx := context.Background()
	namespaceID := "ns-reachable"
	ownerID := "owner-reachable"
	path := "/reachable.bin"
	payload := deterministicObjectGCPayload(8*1024*1024, 0x61)

	rt.upsertNamespace(t, namespaceID, ownerID)
	rt.insertTenant(t, ownerID, meta.TenantActive, meta.TenantKindLive, namespaceID)
	owner := objectGCTenant(t, rt, ownerID)
	ownerBackend, release, err := rt.pool.Acquire(ctx, owner)
	if err != nil {
		t.Fatalf("acquire owner backend: %v", err)
	}
	if _, err := ownerBackend.Write(path, payload, 0, filesystem.WriteFlagCreate); err != nil {
		release()
		t.Fatalf("create s3 file: %v", err)
	}
	storageRef := objectGCFileStorageRef(t, ownerBackend, path)
	assertObjectGCS3ObjectBytes(t, ownerBackend, storageRef, payload)
	release()

	candidate := rt.enqueueCandidate(t, namespaceID, storageRef)
	worker := &objectGCWorker{meta: rt.meta, pool: rt.pool}
	if err := worker.processCandidate(ctx, candidate); err != nil {
		t.Fatal(err)
	}

	state, lastError := objectGCCandidateRow(t, rt.meta.DB(), namespaceID, storageRef)
	if state != meta.ObjectGCCandidatePending || lastError != "storage ref still reachable" {
		t.Fatalf("candidate state=%s last_error=%q", state, lastError)
	}
	ownerBackend, release, err = rt.pool.Acquire(ctx, owner)
	if err != nil {
		t.Fatalf("reacquire owner backend: %v", err)
	}
	defer release()
	assertObjectGCS3ObjectBytes(t, ownerBackend, storageRef, payload)
	assertObjectGCBackendVisibleBytes(t, ownerBackend, path, payload)
}

func TestObjectGCSweepsTierTransitionObsoleteBlobKeepsCurrentContent(t *testing.T) {
	rt := newObjectGCTestRuntime(t)
	ctx := context.Background()
	namespaceID := "ns-tier-sweep"
	ownerID := "owner-tier-sweep"
	path := "/tier-sweep.bin"
	initialInline := deterministicObjectGCPayload(10*1024, 0x12)
	largeS3 := deterministicObjectGCPayload(8*1024*1024, 0x34)
	finalInline := deterministicObjectGCPayload(10*1024, 0x56)

	rt.upsertNamespace(t, namespaceID, ownerID)
	rt.insertTenant(t, ownerID, meta.TenantActive, meta.TenantKindLive, namespaceID)
	owner := objectGCTenant(t, rt, ownerID)
	ownerBackend, release, err := rt.pool.Acquire(ctx, owner)
	if err != nil {
		t.Fatalf("acquire owner backend: %v", err)
	}
	if _, err := ownerBackend.Write(path, initialInline, 0, filesystem.WriteFlagCreate); err != nil {
		release()
		t.Fatalf("create inline file: %v", err)
	}
	if _, err := ownerBackend.Write(path, largeS3, 0, filesystem.WriteFlagTruncate); err != nil {
		release()
		t.Fatalf("overwrite inline -> s3: %v", err)
	}
	obsoleteRef := objectGCFileStorageRef(t, ownerBackend, path)
	assertObjectGCS3ObjectBytes(t, ownerBackend, obsoleteRef, largeS3)
	if _, err := ownerBackend.Write(path, finalInline, 0, filesystem.WriteFlagTruncate); err != nil {
		release()
		t.Fatalf("overwrite s3 -> inline: %v", err)
	}
	assertObjectGCBackendVisibleBytes(t, ownerBackend, path, finalInline)
	if reachable, err := ownerBackend.HasConfirmedS3StorageRef(ctx, datastore.StorageRefHash(obsoleteRef), obsoleteRef); err != nil {
		release()
		t.Fatalf("check obsolete ref reachability: %v", err)
	} else if reachable {
		release()
		t.Fatalf("obsolete ref %q is still confirmed after inline overwrite", obsoleteRef)
	}
	release()

	candidates, err := rt.meta.ListDueObjectGCCandidates(ctx, time.Now().UTC().Add(8*24*time.Hour), 10)
	if err != nil {
		t.Fatalf("list due object gc candidates: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidate count = %d, want 1: %+v", len(candidates), candidates)
	}
	candidate := candidates[0]
	if candidate.NamespaceID != namespaceID || candidate.StorageRef != obsoleteRef || candidate.Reason != meta.ObjectGCReasonOverwrite {
		t.Fatalf("candidate = %+v, want namespace=%q ref=%q reason=%q", candidate, namespaceID, obsoleteRef, meta.ObjectGCReasonOverwrite)
	}

	worker := &objectGCWorker{meta: rt.meta, pool: rt.pool}
	if err := worker.processCandidate(ctx, candidate); err != nil {
		t.Fatal(err)
	}

	state, lastError := objectGCCandidateRow(t, rt.meta.DB(), namespaceID, obsoleteRef)
	if state != meta.ObjectGCCandidateDeleted || lastError != "" {
		t.Fatalf("candidate state=%s last_error=%q", state, lastError)
	}
	ownerBackend, release, err = rt.pool.Acquire(ctx, owner)
	if err != nil {
		t.Fatalf("reacquire owner backend: %v", err)
	}
	defer release()
	assertObjectGCS3ObjectMissing(t, ownerBackend, obsoleteRef)
	assertObjectGCBackendVisibleBytes(t, ownerBackend, path, finalInline)
	currentRef := objectGCFileStorageRef(t, ownerBackend, path)
	if currentRef != "inline" {
		t.Fatalf("current storage ref = %q, want inline", currentRef)
	}
}

func objectGCTenant(t *testing.T, rt *objectGCTestRuntime, tenantID string) *meta.Tenant {
	t.Helper()
	tenant, err := rt.meta.GetTenant(context.Background(), tenantID)
	if err != nil {
		t.Fatalf("get tenant %s: %v", tenantID, err)
	}
	return tenant
}

func objectGCFileStorageRef(t *testing.T, backendFS *backendpkg.Dat9Backend, path string) string {
	t.Helper()
	node, err := backendFS.Store().Stat(context.Background(), path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if node.File == nil {
		t.Fatalf("stat %s returned no file", path)
	}
	return node.File.StorageRef
}

func assertObjectGCBackendVisibleBytes(t *testing.T, backendFS *backendpkg.Dat9Backend, path string, want []byte) {
	t.Helper()
	info, err := backendFS.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if info.Size != int64(len(want)) {
		t.Fatalf("visible size for %s = %d, want %d", path, info.Size, len(want))
	}
	got, err := backendFS.Read(path, 0, int64(len(want)+1))
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("visible bytes for %s mismatch: got %d bytes want %d bytes", path, len(got), len(want))
	}
}

func assertObjectGCS3ObjectBytes(t *testing.T, backendFS *backendpkg.Dat9Backend, storageRef string, want []byte) {
	t.Helper()
	reader, err := backendFS.S3().GetObject(context.Background(), storageRef)
	if err != nil {
		t.Fatalf("get s3 object %q: %v", storageRef, err)
	}
	defer func() { _ = reader.Close() }()
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read s3 object %q: %v", storageRef, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("s3 object %q mismatch: got %d bytes want %d bytes", storageRef, len(got), len(want))
	}
}

func assertObjectGCS3ObjectMissing(t *testing.T, backendFS *backendpkg.Dat9Backend, storageRef string) {
	t.Helper()
	reader, err := backendFS.S3().GetObject(context.Background(), storageRef)
	if err == nil {
		_ = reader.Close()
		t.Fatalf("s3 object %q still exists after object gc", storageRef)
	}
}

func deterministicObjectGCPayload(size int, seed byte) []byte {
	out := make([]byte, size)
	for idx := range out {
		out[idx] = byte((idx*29 + int(seed)) % 251)
	}
	return out
}
