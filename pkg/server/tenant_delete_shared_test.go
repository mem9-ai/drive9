package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testmysql"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"github.com/mem9-ai/drive9/pkg/tenant/token"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// TestSharedTenantDeleteRetriesAfterPlacementRemoval covers the shared-delete
// retry window: an earlier attempt purged the tenant's rows and removed the
// placement but failed before enqueueing cleanup. The retried delete must not
// fall back to the standalone/provider path (no cluster deprovision) — the
// persisted provider routes it back to the shared path, which skips the
// finished purge and completes the delete.
func TestSharedTenantDeleteRetriesAfterPlacementRemoval(t *testing.T) {
	rt := newTenantDeleteRuntime(t, tenant.ProviderTiDBCloudNativeShared, meta.APIKeyScopeKindOwner)
	ctx := context.Background()
	if _, err := rt.meta.EnsureFsID(ctx, rt.tenantID); err != nil {
		t.Fatalf("EnsureFsID: %v", err)
	}
	if err := rt.meta.UpdateTenantStatus(ctx, rt.tenantID, meta.TenantDeleting); err != nil {
		t.Fatal(err)
	}
	if err := rt.meta.UpsertTenantPoolMembership(ctx, &meta.TenantPoolMembership{
		TenantID: rt.tenantID, TiDBCloudOrganizationID: "org-shared-delete", PoolID: "pool-shared-delete",
		PoolStatus: meta.TenantPoolBindingUsed,
	}); err != nil {
		t.Fatalf("UpsertTenantPoolMembership: %v", err)
	}

	resp := rt.deleteTenant(t, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}
	// The standalone cluster-deprovision path must never run for a shared
	// tenant, even when its placement row is already gone.
	if got := rt.prov.deprovisionCalls.Load(); got != 0 {
		t.Fatalf("deprovision calls = %d, want 0", got)
	}
	var status string
	if err := rt.meta.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", rt.tenantID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantDeleted) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantDeleted)
	}
	if _, err := rt.meta.GetTenantPoolMembership(ctx, rt.tenantID); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("shared membership after delete = %v, want ErrNotFound", err)
	}
}

// TestSharedTenantDeleteWithCleanupJobShortCircuits mirrors the standalone
// retry short-circuit: a deleting shared tenant with an existing cleanup job
// returns the deleting status immediately — without purging again and without
// touching the placement row.
func TestSharedTenantDeleteWithCleanupJobShortCircuits(t *testing.T) {
	rt := newTenantDeleteRuntime(t, tenant.ProviderTiDBCloudNativeShared, meta.APIKeyScopeKindOwner)
	ctx := context.Background()
	fsID, err := rt.meta.EnsureFsID(ctx, rt.tenantID)
	if err != nil {
		t.Fatalf("EnsureFsID: %v", err)
	}
	dbID, err := rt.meta.RegisterSharedDB(ctx, &meta.SharedDB{
		TiDBCloudOrganizationID: "org-shared-delete",
		Host:                    "shared.example.com",
		Port:                    4000,
		User:                    "root",
		PasswordCipher:          []byte("cipher"),
		Name:                    "shared_db",
	})
	if err != nil {
		t.Fatalf("RegisterSharedDB: %v", err)
	}
	if err := rt.meta.UpsertTenantPlacement(ctx, &meta.TenantPlacement{
		FsID:        fsID,
		DbID:        dbID,
		Placement:   meta.PlacementShared,
		SchemaShape: meta.SchemaShapeShared,
	}); err != nil {
		t.Fatalf("UpsertTenantPlacement: %v", err)
	}
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

	resp := rt.deleteTenant(t, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 0 {
		t.Fatalf("deprovision calls = %d, want 0", got)
	}
	// The short-circuit must leave the placement row alone: no purge ran.
	if _, err := rt.meta.GetTenantPlacement(ctx, fsID); err != nil {
		t.Fatalf("placement after short-circuit: %v", err)
	}
}

// TestSharedTenantDeleteWithoutFsIDFails proves the shared delete path never
// allocates identity: a shared tenant without an fs_registry row fails the
// delete instead of minting a phantom fs_id (ResolveFsID, not EnsureFsID).
func TestSharedTenantDeleteWithoutFsIDFails(t *testing.T) {
	// The server's one-time startup backfill re-registers any tenant without
	// an fs_id, so the row deletion below must happen strictly after that
	// backfill has run — otherwise the test races it (the backfill recreates
	// the row and the delete returns 202). Watch the global logger for the
	// backfill's completion marker.
	core, recorded := observer.New(zap.InfoLevel)
	prev := logger.L()
	logger.Set(zap.New(core))
	t.Cleanup(func() { logger.Set(prev) })

	rt := newTenantDeleteRuntime(t, tenant.ProviderTiDBCloudNativeShared, meta.APIKeyScopeKindOwner)
	ctx := context.Background()
	deadline := time.Now().Add(10 * time.Second)
	for recorded.FilterField(zap.String("event", "fs_registry_backfill_done")).Len() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("startup fs_registry backfill did not complete in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// InsertTenant allocated an fs_id at provision time; simulate a tenant
	// whose registry row was reaped out of band — the delete must fail, not
	// mint a new one.
	if _, err := rt.meta.DB().ExecContext(ctx, "DELETE FROM fs_registry WHERE tenant_id = ?", rt.tenantID); err != nil {
		t.Fatal(err)
	}

	resp := rt.deleteTenant(t, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusInternalServerError)
	}
	if _, err := rt.meta.ResolveFsID(ctx, rt.tenantID); err == nil {
		t.Fatal("delete allocated an fs_id for the tenant being deleted")
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 0 {
		t.Fatalf("deprovision calls = %d, want 0", got)
	}
}

// TestSharedTenantDeleteFullPurgePath runs the complete owner delete on a
// shared tenant: drain, purge, placement removal, and final delete — the
// provider-routed dispatch never calls the cluster deprovisioner.
func TestSharedTenantDeleteFullPurgePath(t *testing.T) {
	rt := newTenantDeleteRuntime(t, tenant.ProviderTiDBCloudNativeShared, meta.APIKeyScopeKindOwner)
	ctx := context.Background()
	fsID, err := rt.meta.EnsureFsID(ctx, rt.tenantID)
	if err != nil {
		t.Fatalf("EnsureFsID: %v", err)
	}
	dbID, err := rt.meta.RegisterSharedDB(ctx, &meta.SharedDB{
		TiDBCloudOrganizationID: "org-shared-delete",
		Host:                    "shared.example.com",
		Port:                    4000,
		User:                    "root",
		PasswordCipher:          []byte("cipher"),
		Name:                    "shared_db",
	})
	if err != nil {
		t.Fatalf("RegisterSharedDB: %v", err)
	}
	if err := rt.meta.UpsertTenantPlacement(ctx, &meta.TenantPlacement{
		FsID:        fsID,
		DbID:        dbID,
		Placement:   meta.PlacementShared,
		SchemaShape: meta.SchemaShapeShared,
	}); err != nil {
		t.Fatalf("UpsertTenantPlacement: %v", err)
	}

	resp := rt.deleteTenant(t, nil)
	defer func() { _ = resp.Body.Close() }()
	// The purge needs a reachable shared DB, which "shared.example.com" is
	// not — the delete must fail there WITHOUT deprovisioning any cluster and
	// leave the tenant in deleting state for a later retry.
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d (unreachable shared db)", resp.StatusCode, http.StatusInternalServerError)
	}
	if got := rt.prov.deprovisionCalls.Load(); got != 0 {
		t.Fatalf("deprovision calls = %d, want 0", got)
	}
	var status string
	if err := rt.meta.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", rt.tenantID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantDeleting) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantDeleting)
	}
	// The placement row must survive the failed purge so the retry can find
	// the shared DB again.
	if _, err := rt.meta.GetTenantPlacement(ctx, fsID); err != nil {
		t.Fatalf("placement after failed purge: %v", err)
	}
}

// TestAdminTenantCreateRejectsSharedPoolOrg proves the admin tenant API fails
// closed when the caller's org matches a registered shared pool: no claim, no
// provision, no tenant row — instead of creating a shared tenant with no
// valid admin lifecycle.
func TestAdminTenantCreateRejectsSharedPoolOrg(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	ctx := context.Background()
	rt.prov.listPages = []*tenant.ManagedClusterListResult{
		{Clusters: []tenant.CloudClusterInfo{{OrganizationID: "org-shared-1"}}},
	}
	if _, err := rt.meta.RegisterSharedDB(ctx, &meta.SharedDB{
		TiDBCloudOrganizationID: "org-shared-1",
		Host:                    "shared.example.com",
		Port:                    4000,
		User:                    "root",
		PasswordCipher:          []byte("cipher"),
		Name:                    "shared_db",
	}); err != nil {
		t.Fatalf("RegisterSharedDB: %v", err)
	}

	body, err := json.Marshal(map[string]string{"public_key": "pk", "private_key": "sk"})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.server)
	defer ts.Close()
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/admin/tenants", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusConflict)
	}
	var tenantCount int
	if err := rt.meta.DB().QueryRow("SELECT COUNT(*) FROM tenants").Scan(&tenantCount); err != nil {
		t.Fatal(err)
	}
	// Only the pre-existing quota runtime tenant may exist.
	if tenantCount != 1 {
		t.Fatalf("tenants = %d, want 1 (no admin-created tenant)", tenantCount)
	}
}

// TestAdminTenantGetRejectsSharedProvider documents the admin/shared
// ownership contract from the other side: a shared-schema tenant (provider
// tidb_cloud_native_shared) is rejected by admin authorization instead of
// being managed as a dedicated cluster.
func TestAdminTenantGetRejectsSharedProvider(t *testing.T) {
	rt := newQuotaRuntime(t, tenant.ProviderTiDBCloudNative)
	// The server runs the cloud-native provisioner; this tenant is placed on
	// a shared pool, so its persisted provider is the shared one.
	if err := rt.meta.UpdateTenantProvider(context.Background(), rt.tenantID, tenant.ProviderTiDBCloudNativeShared); err != nil {
		t.Fatalf("UpdateTenantProvider: %v", err)
	}
	ts := httptest.NewServer(rt.server)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/tenants/"+rt.tenantID, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-TiDBCloud-Public-Key", "pk")
	req.Header.Set("X-TiDBCloud-Private-Key", "sk")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusConflict)
	}
}

// TestSharedTenantDeletePlacementRemovalFailureStaysRetryable injects a
// failure into the placement-removal transaction (the pool row disappears
// between purge and removal) and proves the delete fails closed: 500, tenant
// stays deleting, the placement row survives for the retry, and the owner
// key stays active — instead of orphaning the placement on a terminal
// tenant.
func TestSharedTenantDeletePlacementRemovalFailureStaysRetryable(t *testing.T) {
	rt := newTenantDeleteRuntime(t, tenant.ProviderTiDBCloudNativeShared, meta.APIKeyScopeKindOwner)
	ctx := context.Background()
	fsID, err := rt.meta.EnsureFsID(ctx, rt.tenantID)
	if err != nil {
		t.Fatalf("EnsureFsID: %v", err)
	}
	db := newTenantDeleteDBInfo(t)
	passCipher, err := rt.pool.Encrypt(ctx, []byte(db.DBPass))
	if err != nil {
		t.Fatal(err)
	}
	dbID, err := rt.meta.RegisterSharedDB(ctx, &meta.SharedDB{
		TiDBCloudOrganizationID: "org-shared-delete", Host: db.DBHost, Port: db.DBPort, User: db.DBUser,
		PasswordCipher: passCipher, Name: db.DBName,
	})
	if err != nil {
		t.Fatalf("RegisterSharedDB: %v", err)
	}
	if err := rt.meta.UpsertTenantPlacement(ctx, &meta.TenantPlacement{
		FsID: fsID, DbID: dbID, Placement: meta.PlacementShared, SchemaShape: meta.SchemaShapeShared,
	}); err != nil {
		t.Fatalf("UpsertTenantPlacement: %v", err)
	}
	// Pre-warm the shared handle with a scratch database in which every
	// shared-schema table is absent: the purge in the delete below reuses
	// the cached handle and succeeds trivially, but the placement removal
	// transaction cannot release capacity on a missing pool row and must
	// roll back. (The test DB carries standalone-shape tables of the same
	// names, so drop them first — the next test's schema init recreates
	// them.)
	for _, tbl := range []string{
		"journal_entry_subjects", "journal_entries", "journal_append_requests", "journal_labels", "journals",
		"vault_audit_log", "vault_grants", "vault_tokens", "vault_secret_fields", "vault_secrets", "vault_deks",
		"git_workspace_object_packs", "git_workspace_overlay", "git_workspace_git_state", "git_workspace_tree_nodes", "git_workspaces",
		"fs_layer_checkpoints", "fs_layer_events", "fs_layer_tags", "fs_layer_entries", "fs_layers",
		"fs_events", "semantic_tasks", "file_gc_tasks", "uploads", "file_tags", "file_nodes", "semantic", "contents", "inodes",
	} {
		if _, err := rt.meta.DB().ExecContext(ctx, "DROP TABLE IF EXISTS "+tbl); err != nil {
			t.Fatalf("drop %s: %v", tbl, err)
		}
	}
	if err := rt.pool.PurgeSharedTenant(ctx, fsID, dbID); err != nil {
		t.Fatalf("pre-warm purge: %v", err)
	}
	if _, err := rt.meta.DB().ExecContext(ctx, "DELETE FROM db_pool WHERE db_id = ?", dbID); err != nil {
		t.Fatal(err)
	}

	resp := rt.deleteTenant(t, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusInternalServerError)
	}
	var status string
	if err := rt.meta.DB().QueryRow("SELECT status FROM tenants WHERE id = ?", rt.tenantID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(meta.TenantDeleting) {
		t.Fatalf("tenant status = %s, want %s", status, meta.TenantDeleting)
	}
	if _, err := rt.meta.GetTenantPlacement(ctx, fsID); err != nil {
		t.Fatalf("placement must survive the rolled-back removal: %v", err)
	}
	var activeKeys int
	if err := rt.meta.DB().QueryRow("SELECT COUNT(*) FROM tenant_api_keys WHERE tenant_id = ? AND status = ?", rt.tenantID, meta.APIKeyActive).Scan(&activeKeys); err != nil {
		t.Fatal(err)
	}
	if activeKeys != 1 {
		t.Fatalf("active api keys = %d, want 1 (owner can retry)", activeKeys)
	}
}

// TestFindSharedDBForProvisionSkipsNonTiDBProviders proves a wildcard shared
// pool only ever captures TiDB-dialect tenants: db9 (PostgreSQL) tenants must
// never be placed on the TiDB shared schema and relabeled
// tidb_cloud_native_shared.
func TestFindSharedDBForProvisionSkipsNonTiDBProviders(t *testing.T) {
	db := newTenantDeleteDBInfo(t)
	testmysql.ResetMetaDB(t, db.Meta.DB())
	ctx := context.Background()
	if _, err := db.Meta.RegisterSharedDB(ctx, &meta.SharedDB{
		TiDBCloudOrganizationID: meta.SharedDBOrgWildcard, Host: "shared.example.com", Port: 4000, User: "root",
		PasswordCipher: []byte("cipher"), Name: "shared_db",
	}); err != nil {
		t.Fatalf("RegisterSharedDB: %v", err)
	}
	s := &Server{meta: db.Meta}

	got, err := s.findSharedDBForProvision(ctx, tenant.ProviderDB9, provisionTenantOptions{})
	if err != nil {
		t.Fatalf("db9 lookup: %v", err)
	}
	if got != nil {
		t.Fatalf("db9 matched shared pool %v, want no match", got)
	}
	// TiDB-dialect providers still route: tidb_zero matches the wildcard.
	got, err = s.findSharedDBForProvision(ctx, tenant.ProviderTiDBZero, provisionTenantOptions{})
	if err != nil {
		t.Fatalf("tidb_zero lookup: %v", err)
	}
	if got == nil {
		t.Fatal("tidb_zero did not match the wildcard pool, want match")
	}
}

// TestProvisionTenantOnSharedDBCommitsAtomically covers the happy path of
// the atomic provision transition: placement, provider re-label, active
// status, capacity slot, and the owner key all land together.
func TestProvisionTenantOnSharedDBCommitsAtomically(t *testing.T) {
	db := newTenantDeleteDBInfo(t)
	testmysql.ResetMetaDB(t, db.Meta.DB())
	ctx := context.Background()
	dbID, err := db.Meta.RegisterSharedDB(ctx, &meta.SharedDB{
		TiDBCloudOrganizationID: "org-provision-ok", Host: "shared.example.com", Port: 4000, User: "root",
		PasswordCipher: []byte("cipher"), Name: "shared_db", MaxTenants: 10,
	})
	if err != nil {
		t.Fatalf("RegisterSharedDB: %v", err)
	}
	sharedDB, err := db.Meta.GetSharedDB(ctx, dbID)
	if err != nil {
		t.Fatalf("GetSharedDB: %v", err)
	}
	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	s := &Server{meta: db.Meta, pool: db.Pool, tokenSecret: tokenSecret}
	now := time.Now().UTC()
	tenantID := token.NewID()
	if err := db.Meta.InsertTenant(ctx, &meta.Tenant{
		ID: tenantID, Status: meta.TenantPending, Kind: meta.TenantKindLive,
		Provider: tenant.ProviderTiDBCloudNative, DBPasswordCipher: []byte{},
		SchemaVersion: 1, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("InsertTenant: %v", err)
	}

	res, err := s.provisionTenantOnSharedDB(ctx, tenantID, sharedDB, tenant.ProviderTiDBCloudNative, "default", provisionTenantOptions{}, now)
	if err != nil {
		t.Fatalf("provisionTenantOnSharedDB: %v", err)
	}
	if res.Provider != tenant.ProviderTiDBCloudNativeShared || res.APIKey == "" {
		t.Fatalf("result = provider %q api_key %q", res.Provider, res.APIKey)
	}
	got, err := db.Meta.GetTenant(ctx, tenantID)
	if err != nil {
		t.Fatalf("GetTenant: %v", err)
	}
	if got.Provider != tenant.ProviderTiDBCloudNativeShared || got.Status != meta.TenantActive {
		t.Fatalf("tenant = provider %q status %q, want tidb_cloud_native_shared/active", got.Provider, got.Status)
	}
	fsID, err := db.Meta.ResolveFsID(ctx, tenantID)
	if err != nil {
		t.Fatalf("ResolveFsID: %v", err)
	}
	p, err := db.Meta.GetTenantPlacement(ctx, fsID)
	if err != nil {
		t.Fatalf("GetTenantPlacement: %v", err)
	}
	if p.DbID != dbID || p.SchemaShape != meta.SchemaShapeShared {
		t.Fatalf("placement = %+v", p)
	}
	dbRow, err := db.Meta.GetSharedDB(ctx, dbID)
	if err != nil {
		t.Fatalf("GetSharedDB: %v", err)
	}
	if dbRow.TenantCount != 1 {
		t.Fatalf("TenantCount = %d, want 1", dbRow.TenantCount)
	}
	var activeKeys int
	if err := db.Meta.DB().QueryRow("SELECT COUNT(*) FROM tenant_api_keys WHERE tenant_id = ? AND status = ?", tenantID, meta.APIKeyActive).Scan(&activeKeys); err != nil {
		t.Fatal(err)
	}
	if activeKeys != 1 {
		t.Fatalf("active owner keys = %d, want 1", activeKeys)
	}
}

// TestProvisionTenantOnSharedDBFailureLeavesNoPartialState proves the atomic
// transition leaves nothing behind on failure: when any write of the
// provision transaction fails, no placement row, no reserved capacity, no
// owner key persists, and the tenant never becomes active — there is no
// best-effort compensation window that could orphan an active shared tenant.
func TestProvisionTenantOnSharedDBFailureLeavesNoPartialState(t *testing.T) {
	db := newTenantDeleteDBInfo(t)
	testmysql.ResetMetaDB(t, db.Meta.DB())
	ctx := context.Background()
	dbID, err := db.Meta.RegisterSharedDB(ctx, &meta.SharedDB{
		TiDBCloudOrganizationID: "org-provision-fail", Host: "shared.example.com", Port: 4000, User: "root",
		PasswordCipher: []byte("cipher"), Name: "shared_db", MaxTenants: 10,
	})
	if err != nil {
		t.Fatalf("RegisterSharedDB: %v", err)
	}
	sharedDB, err := db.Meta.GetSharedDB(ctx, dbID)
	if err != nil {
		t.Fatalf("GetSharedDB: %v", err)
	}
	tokenSecret := make([]byte, 32)
	if _, err := rand.Read(tokenSecret); err != nil {
		t.Fatal(err)
	}
	s := &Server{meta: db.Meta, pool: db.Pool, tokenSecret: tokenSecret}
	tenantID := token.NewID()
	// No tenant row is inserted, so the tenant activation inside the
	// transaction fails and rolls back the capacity reservation, the
	// placement, and the owner key insert.
	if _, err := s.provisionTenantOnSharedDB(ctx, tenantID, sharedDB, tenant.ProviderTiDBCloudNative, "default", provisionTenantOptions{}, time.Now().UTC()); err == nil {
		t.Fatal("provisionTenantOnSharedDB succeeded, want error")
	} else {
		var pe *provisionTenantError
		if !errors.As(err, &pe) || pe.status != http.StatusInternalServerError {
			t.Fatalf("error = %v, want provisionTenantError 500", err)
		}
	}
	fsID, err := db.Meta.ResolveFsID(ctx, tenantID)
	if err != nil {
		t.Fatalf("ResolveFsID: %v", err)
	}
	if _, err := db.Meta.GetTenantPlacement(ctx, fsID); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("placement after failed provision = %v, want ErrNotFound", err)
	}
	got, err := db.Meta.GetSharedDB(ctx, dbID)
	if err != nil {
		t.Fatalf("GetSharedDB after failure: %v", err)
	}
	if got.TenantCount != 0 {
		t.Fatalf("TenantCount = %d, want 0 (nothing reserved)", got.TenantCount)
	}
	var keyCount int
	if err := db.Meta.DB().QueryRow("SELECT COUNT(*) FROM tenant_api_keys WHERE tenant_id = ?", tenantID).Scan(&keyCount); err != nil {
		t.Fatal(err)
	}
	if keyCount != 0 {
		t.Fatalf("owner keys after failed provision = %d, want 0", keyCount)
	}
}
