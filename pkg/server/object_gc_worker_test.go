package server

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/encrypt"
	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/tenant"
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
		Provider:           tenant.ProviderTiDBCloudStarter,
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
