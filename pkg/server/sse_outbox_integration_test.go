package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/drive9/internal/testtidb"
	"github.com/mem9-ai/drive9/pkg/encrypt"
	"github.com/mem9-ai/drive9/pkg/leader"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"github.com/mem9-ai/drive9/pkg/tenant/token"
	"go.uber.org/zap"
)

// sseOutboxTestCluster sets up two drive9 server instances (simulating two pods)
// sharing the same central meta DB, each with its own eventBuses and the SSE
// notify infrastructure (poller + pod registry). A single tenant is
// provisioned so both pods can serve SSE events for it.
type sseOutboxTestCluster struct {
	metaStore *meta.Store
	podA      *Server
	podB      *Server
	tenantID  string
	token     string
}

// newSSEOutboxTestCluster creates two server instances with SSE cross-pod
// notification enabled. Both share the same MySQL meta DB and the same tenant
// (whose data DB is also the test MySQL). Each pod runs its own outbox poller
// and the pods are registered as peers of each other.
func newSSEOutboxTestCluster(t *testing.T) *sseOutboxTestCluster {
	t.Helper()

	// Open the shared meta store and reset it.
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	testtidb.ResetMetaDB(t, metaStore.DB())
	// Clean up pod tables ResetMetaDB does not cover. Fail on error so stale
	// rows don't leak between tests.
	ctx := context.Background()
	for _, table := range []string{"pod_subscriptions", "pod_registry"} {
		if _, err := metaStore.DB().ExecContext(ctx, "DELETE FROM "+table); err != nil {
			t.Fatalf("clean up %s: %v", table, err)
		}
	}

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
			if n, err2 := parseInt(p); err2 == nil {
				port = n
			}
		}
	}

	// Encryption for tenant DB password.
	masterKey := make([]byte, 32)
	for i := range masterKey {
		masterKey[i] = 0xAB
	}
	enc, err := encrypt.NewLocalAESEncryptor(masterKey)
	if err != nil {
		t.Fatal(err)
	}

	// Provision a tenant.
	tenantID := token.NewID()
	now := time.Now().UTC()
	passCipher, err := enc.Encrypt(context.Background(), []byte(parsed.Passwd))
	if err != nil {
		t.Fatal(err)
	}
	if err := metaStore.InsertTenant(context.Background(), &meta.Tenant{
		ID:               tenantID,
		Status:           meta.TenantActive,
		DBHost:           host,
		DBPort:           port,
		DBUser:           parsed.User,
		DBPasswordCipher: passCipher,
		DBName:           parsed.DBName,
		DBTLS:            false,
		Provider:         tenant.ProviderDB9,
		SchemaVersion:    1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		t.Fatal(err)
	}

	// Token signing key.
	tokenSecret := make([]byte, 32)
	for i := range tokenSecret {
		tokenSecret[i] = 0xCD
	}
	// Issue an API key for the tenant.
	tok, err := token.IssueToken(tokenSecret, tenantID, 1)
	if err != nil {
		t.Fatal(err)
	}
	tokCipher, err := enc.Encrypt(context.Background(), []byte(tok))
	if err != nil {
		t.Fatal(err)
	}
	if err := metaStore.InsertAPIKey(context.Background(), &meta.APIKey{
		ID:            token.NewID(),
		TenantID:      tenantID,
		KeyName:       "default",
		JWTCiphertext: tokCipher,
		JWTHash:       token.HashToken(tok),
		TokenVersion:  1,
		Status:        meta.APIKeyActive,
		IssuedAt:      now,
		CreatedAt:     now,
		UpdatedAt:     now,
	}); err != nil {
		t.Fatal(err)
	}

	// Shared SSE config for both pods.
	s3Dir := t.TempDir()

	// Helper to create a pool for each pod (each pool needs its own enc).
	newPool := func() *tenant.Pool {
		poolEnc, err := encrypt.NewLocalAESEncryptor(masterKey)
		if err != nil {
			t.Fatal(err)
		}
		pool := tenant.NewPool(tenant.PoolConfig{
			S3Dir:     s3Dir,
			PublicURL: "http://127.0.0.1",
		}, poolEnc)
		pool.SetMetaStore(metaStore)
		t.Cleanup(func() { pool.Close() })
		return pool
	}

	// Pod A.
	leaderMgrA := leader.NewManager(nil, leader.WithDisabled())
	podA := NewWithConfig(Config{
		Meta:        metaStore,
		Pool:        newPool(),
		Provisioner: &fakeProvisioner{provider: tenant.ProviderDB9},
		TokenSecret: tokenSecret,
		S3Dir:       s3Dir,
		PodID:       "pod-a",
		PodAddr:     "http://127.0.0.1:18001",
		Leader:      leaderMgrA,
		Logger:      zap.NewNop(),
	})
	t.Cleanup(func() { podA.Close() })

	// Pod B.
	leaderMgrB := leader.NewManager(nil, leader.WithDisabled())
	podB := NewWithConfig(Config{
		Meta:        metaStore,
		Pool:        newPool(),
		Provisioner: &fakeProvisioner{provider: tenant.ProviderDB9},
		TokenSecret: tokenSecret,
		S3Dir:       s3Dir,
		PodID:       "pod-b",
		PodAddr:     "http://127.0.0.1:18002",
		Leader:      leaderMgrB,
		Logger:      zap.NewNop(),
	})
	t.Cleanup(func() { podB.Close() })

	return &sseOutboxTestCluster{
		metaStore: metaStore,
		podA:      podA,
		podB:      podB,
		tenantID:  tenantID,
		token:     tok,
	}
}

// parseInt is a small helper to avoid importing strconv in this test file.
func parseInt(s string) (int, error) {
	var n int
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("not a digit: %c", c)
		}
		n = n*10 + int(c-'0')
	}
	return n, nil
}

// TestSSEOutboxCrossPodPollerDelivery verifies the end-to-end flow:
//  1. Pod B has an SSE subscriber for a tenant (bus with listener).
//  2. Pod A writes a tenant_notify_outbox row with the SSE bit set
//     (simulating a cross-pod write via publishEvent).
//  3. Pod B's tenantOutboxPoller discovers the outbox row and wakes its local
//     subscriber via Publish.
//
// This tests the 200ms unified outbox poller path.
func TestSSEOutboxCrossPodPollerDelivery(t *testing.T) {
	tc := newSSEOutboxTestCluster(t)

	// Pod B: create a bus for the tenant and subscribe.
	busB := tc.podB.events.get(tc.tenantID, nil)
	subID, notify := busB.Subscribe()
	defer busB.Unsubscribe(subID)

	// Pod A: write a unified outbox row (simulating publishEvent's outbox step).
	if err := tc.metaStore.InsertTenantNotify(context.Background(), tc.tenantID, WorkSSE); err != nil {
		t.Fatal(err)
	}

	// Pod B's tenantOutboxPoller should discover the outbox row within ~3s and
	// wake the subscriber.
	select {
	case _, open := <-notify:
		if !open {
			t.Fatal("notify channel closed")
		}
		// Success: cross-pod outbox delivery via the poller.
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for cross-pod outbox delivery via poller")
	}
}

// TestSSEOutboxPodRegistryHeartbeat verifies that the pod_registry goroutine
// writes heartbeat rows and that the leader can list active pods.
func TestSSEOutboxPodRegistryHeartbeat(t *testing.T) {
	tc := newSSEOutboxTestCluster(t)

	// Both pods auto-register via the heartbeat loop on Start (initial
	// heartbeat is synchronous). Verify they appear in ListActivePods.
	pods, err := tc.metaStore.ListActivePods(context.Background(), "pod-a")
	if err != nil {
		t.Fatal(err)
	}
	foundPodB := false
	for _, p := range pods {
		if p.PodID == "pod-b" {
			foundPodB = true
			break
		}
	}
	if !foundPodB {
		t.Fatalf("pod-b not found in active pods; got %d pods", len(pods))
	}

	// Also verify pod-a is there (query as pod-b).
	podsFromB, err := tc.metaStore.ListActivePods(context.Background(), "pod-b")
	if err != nil {
		t.Fatal(err)
	}
	foundPodA := false
	for _, p := range podsFromB {
		if p.PodID == "pod-a" {
			foundPodA = true
			break
		}
	}
	if !foundPodA {
		t.Fatalf("pod-a not found in active pods from pod-b perspective; got %d pods", len(podsFromB))
	}
}

// TestSSEOutboxSubscriptionReporting verifies that the subscription loop
// reports the active tenant set to pod_subscriptions and prunes stale entries.
func TestSSEOutboxSubscriptionReporting(t *testing.T) {
	tc := newSSEOutboxTestCluster(t)

	// Pod B: create a bus with a subscriber for the tenant.
	busB := tc.podB.events.get(tc.tenantID, nil)
	subID, _ := busB.Subscribe()
	defer busB.Unsubscribe(subID)

	// Manually report subscriptions (simulating the subscriptionLoop ticker).
	tc.podB.podRegistry.reportSubscriptions(context.Background())

	// Verify pod_subscriptions has the tenant for pod-b. Retry briefly because
	// the Subscribe call may not have been visible to activeTenantIDs on a
	// slow CI machine.
	var subs []string
	found := false
	for range 10 {
		var err2 error
		subs, err2 = tc.metaStore.ListPodSubscriptions(context.Background(), "pod-b")
		if err2 != nil {
			t.Fatal(err2)
		}
		for _, s := range subs {
			if s == tc.tenantID {
				found = true
				break
			}
		}
		if found {
			break
		}
		// Re-report in case the bus listener wasn't visible yet.
		tc.podB.podRegistry.reportSubscriptions(context.Background())
		time.Sleep(50 * time.Millisecond)
	}
	if !found {
		t.Fatalf("tenant %s not in pod-b subscriptions; got %v", tc.tenantID, subs)
	}

	// Now unsubscribe and report again — the subscription should be pruned.
	busB.Unsubscribe(subID)
	tc.podB.podRegistry.reportSubscriptions(context.Background())

	subsAfter, err := tc.metaStore.ListPodSubscriptions(context.Background(), "pod-b")
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range subsAfter {
		if s == tc.tenantID {
			t.Fatalf("tenant %s should have been pruned from pod-b subscriptions", tc.tenantID)
		}
	}
}

// TestSSEOutboxStalePodSweep verifies that the leader's SweepStalePods marks
// pods with expired heartbeats as stale and cleans up their subscriptions and
// outbox cursor rows.
func TestSSEOutboxStalePodSweep(t *testing.T) {
	tc := newSSEOutboxTestCluster(t)

	// Register a "dead" pod with a stale heartbeat, a subscription, and an
	// outbox cursor row.
	if err := tc.metaStore.UpsertPod(context.Background(), "dead-pod", "http://127.0.0.1:1"); err != nil {
		t.Fatal(err)
	}
	if err := tc.metaStore.UpsertPodSubscriptions(context.Background(), "dead-pod", []string{tc.tenantID}); err != nil {
		t.Fatal(err)
	}
	if err := tc.metaStore.UpsertTenantOutboxCursor(context.Background(), "dead-pod", 42); err != nil {
		t.Fatal(err)
	}

	// Manually set its heartbeat to be old enough to be stale.
	_, err := tc.metaStore.DB().ExecContext(context.Background(),
		`UPDATE pod_registry SET last_heartbeat = ? WHERE pod_id = ?`,
		time.Now().Add(-2*time.Minute), "dead-pod")
	if err != nil {
		t.Fatal(err)
	}

	// Run the sweep (as the leader would).
	tc.podA.podRegistry.SweepStalePods(context.Background())

	// The dead pod should now be stale.
	pods, err := tc.metaStore.ListActivePods(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range pods {
		if p.PodID == "dead-pod" {
			t.Fatal("dead-pod should have been marked stale, but is still active")
		}
	}

	// Its subscriptions should be cleaned up.
	subs, err := tc.metaStore.ListPodSubscriptions(context.Background(), "dead-pod")
	if err != nil {
		t.Fatal(err)
	}
	if len(subs) != 0 {
		t.Fatalf("dead-pod subscriptions should be empty after sweep; got %v", subs)
	}

	// Its tenant_outbox_cursor row should be deleted too, so the stale cursor
	// cannot hold back outbox pruning (MIN(last_id) floor).
	if _, err := tc.metaStore.GetTenantOutboxCursor(context.Background(), "dead-pod"); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("dead-pod cursor should be deleted after sweep; got err=%v", err)
	}
}

// TestSSEOutboxOutboxCleanup verifies that the leader's cleanupTenantNotifyOutbox
// prunes old outbox rows.
func TestSSEOutboxOutboxCleanup(t *testing.T) {
	tc := newSSEOutboxTestCluster(t)

	// Insert an outbox row with an old created_at.
	_, err := tc.metaStore.DB().ExecContext(context.Background(),
		`INSERT INTO tenant_notify_outbox (tenant_id, work_mask, created_at) VALUES (?, ?, ?)`,
		tc.tenantID, WorkSSE, time.Now().Add(-2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}

	// Insert a fresh row that should be retained.
	if err := tc.metaStore.InsertTenantNotify(context.Background(), tc.tenantID, WorkSSE); err != nil {
		t.Fatal(err)
	}

	// Run cleanup with the default retention (1h). The old row should be pruned.
	tc.podA.cleanupTenantNotifyOutbox(context.Background())

	// Verify only the fresh row remains for this tenant.
	rows, err := tc.metaStore.ListTenantNotifySince(context.Background(), 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	var count int
	for _, r := range rows {
		if r.TenantID == tc.tenantID {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected 1 outbox row for tenant after cleanup, got %d", count)
	}
	var tenantRow *meta.TenantNotifyRow
	for i := range rows {
		if rows[i].TenantID == tc.tenantID {
			tenantRow = &rows[i]
			break
		}
	}
	if tenantRow == nil {
		t.Fatal("tenant row not found after cleanup")
	}
	if tenantRow.WorkMask != WorkSSE {
		t.Fatalf("expected remaining row work_mask=%d, got %d", WorkSSE, tenantRow.WorkMask)
	}
}

// TestResolveRetryStoreRefreshesBus verifies the production store resolver:
// for an active tenant it re-acquires the backend through the pool and
// refreshes the bus's cached store pointer; for a tenant that no longer
// exists it reports errTenantGone so buffered events drop instead of spinning.
func TestResolveRetryStoreRefreshesBus(t *testing.T) {
	tc := newSSEOutboxTestCluster(t)

	// A bus with no cached store (no traffic yet): the resolver must acquire
	// one through the pool and refresh the pointer as a side effect.
	bus := tc.podA.events.get(tc.tenantID, nil)
	if bus.store.Load() != nil {
		t.Fatal("precondition: bus store should be nil before resolve")
	}
	inserter, err := tc.podA.resolveRetryStore(context.Background(), tc.tenantID, bus)
	if err != nil {
		t.Fatalf("resolveRetryStore: %v", err)
	}
	if inserter == nil {
		t.Fatal("resolver returned nil inserter for an active tenant")
	}
	if bus.store.Load() == nil {
		t.Fatal("bus store pointer not refreshed by the resolver")
	}

	// The resolved store actually inserts (create the standalone fs_events
	// table first — the cluster provisions the tenant row, not the schema).
	for _, ddl := range []string{
		`CREATE TABLE IF NOT EXISTS fs_events (
			seq        BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
			path       TEXT NOT NULL,
			op         VARCHAR(64) NOT NULL,
			actor      VARCHAR(255),
			ts         BIGINT NOT NULL,
			created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
		)`,
		`CREATE INDEX idx_fs_events_created ON fs_events(created_at)`,
	} {
		if _, err := tc.metaStore.DB().ExecContext(context.Background(), ddl); err != nil && !strings.Contains(err.Error(), "Duplicate key") {
			t.Fatalf("apply fs_events DDL: %v", err)
		}
	}
	if _, err := inserter.InsertFSEvent(context.Background(), "/resolved.txt", "write", "tester", time.Now().UnixMilli()); err != nil {
		t.Fatalf("insert via resolved store: %v", err)
	}

	// A tenant that does not exist yields errTenantGone (entries drop with
	// reason tenant_gone instead of spinning).
	if _, err := tc.podA.resolveRetryStore(context.Background(), "no-such-tenant", nil); !errors.Is(err, errTenantGone) {
		t.Fatalf("err = %v, want errTenantGone", err)
	}
}
