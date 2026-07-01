package server

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/drive9/internal/testmysql"
	"github.com/mem9-ai/drive9/pkg/encrypt"
	"github.com/mem9-ai/drive9/pkg/leader"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"github.com/mem9-ai/drive9/pkg/tenant/token"
	"go.uber.org/zap"
)

// sseOutboxTestCluster sets up two drive9 server instances (simulating two pods)
// sharing the same central meta DB, each with its own eventBuses and the new SSE
// notify infrastructure (poller + pod notifier + pod registry). A single tenant
// is provisioned so both pods can serve SSE events for it.
type sseOutboxTestCluster struct {
	metaStore *meta.Store
	podA      *Server
	podB      *Server
	podAAddr  string
	podBAddr  string
	tenantID  string
	token     string
}

// newSSEOutboxTestCluster creates two server instances with SSE cross-pod
// notification enabled. Both share the same MySQL meta DB and the same tenant
// (whose data DB is also the test MySQL). Each pod runs its own notifyPoller
// and the pods are registered as peers of each other.
func newSSEOutboxTestCluster(t *testing.T) *sseOutboxTestCluster {
	t.Helper()

	// Open the shared meta store and reset it.
	metaStore, err := meta.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })
	testmysql.ResetMetaDB(t, metaStore.DB())
	// Clean up SSE notify tables (ResetMetaDB may not know about new tables).
	// Fail on error so stale rows don't leak between tests.
	ctx := context.Background()
	for _, table := range []string{"sse_notify_outbox", "tenant_notify_outbox", "tenant_outbox_cursor", "pod_subscriptions", "pod_registry", "tenant_api_keys", "tenants"} {
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
	podNotifySecret := []byte("test-pod-secret")
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

	// Pre-allocate httptest listeners so we know the addresses before creating
	// the servers. This lets us pass the correct PodAddr to NewWithConfig.
	lnA, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lnA.Close() })
	lnB, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lnB.Close() })
	podAAddr := "http://" + lnA.Addr().String()
	podBAddr := "http://" + lnB.Addr().String()

	// Pod A: create server with the pre-allocated address.
	leaderMgrA := leader.NewManager(nil, leader.WithDisabled())
	podA := NewWithConfig(Config{
		Meta:            metaStore,
		Pool:            newPool(),
		Provisioner:     &fakeProvisioner{provider: tenant.ProviderDB9},
		TokenSecret:     tokenSecret,
		S3Dir:           s3Dir,
		PodID:           "pod-a",
		PodAddr:         podAAddr,
		PodNotifySecret: podNotifySecret,
		Leader:          leaderMgrA,
		Logger:          zap.NewNop(),
	})
	t.Cleanup(func() { podA.Close() })

	podASrv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == sseNotifyInternalRoute {
			podA.handleInternalSSENotify(w, r)
			return
		}
		http.NotFound(w, r)
	}))
	podASrv.Listener = lnA
	podASrv.Start()
	t.Cleanup(podASrv.Close)

	// Pod B.
	leaderMgrB := leader.NewManager(nil, leader.WithDisabled())
	podB := NewWithConfig(Config{
		Meta:            metaStore,
		Pool:            newPool(),
		Provisioner:     &fakeProvisioner{provider: tenant.ProviderDB9},
		TokenSecret:     tokenSecret,
		S3Dir:           s3Dir,
		PodID:           "pod-b",
		PodAddr:         podBAddr,
		PodNotifySecret: podNotifySecret,
		Leader:          leaderMgrB,
		Logger:          zap.NewNop(),
	})
	t.Cleanup(func() { podB.Close() })

	podBSrv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == sseNotifyInternalRoute {
			podB.handleInternalSSENotify(w, r)
			return
		}
		http.NotFound(w, r)
	}))
	podBSrv.Listener = lnB
	podBSrv.Start()
	t.Cleanup(podBSrv.Close)

	return &sseOutboxTestCluster{
		metaStore: metaStore,
		podA:      podA,
		podB:      podB,
		podAAddr:  podASrv.URL,
		podBAddr:  podBSrv.URL,
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

	// Verify pod_subscriptions has the tenant for pod-b.
	subs, err := tc.metaStore.ListPodSubscriptions(context.Background(), "pod-b")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, s := range subs {
		if s == tc.tenantID {
			found = true
			break
		}
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

// TestSSEOutboxInternalEndpointRejection verifies that the internal endpoint
// rejects requests with wrong/missing auth and wrong method.
func TestSSEOutboxInternalEndpointRejection(t *testing.T) {
	tc := newSSEOutboxTestCluster(t)

	// Wrong method.
	req := httptest.NewRequest(http.MethodGet, sseNotifyInternalRoute, nil)
	w := httptest.NewRecorder()
	tc.podA.handleInternalSSENotify(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 for GET, got %d", w.Code)
	}

	// Missing auth.
	body := `{"tenant_id":"x","seq":1}`
	req2 := httptest.NewRequest(http.MethodPost, sseNotifyInternalRoute, bytes.NewReader([]byte(body)))
	w2 := httptest.NewRecorder()
	tc.podA.handleInternalSSENotify(w2, req2)
	if w2.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing auth, got %d", w2.Code)
	}

	// Wrong auth.
	req3 := httptest.NewRequest(http.MethodPost, sseNotifyInternalRoute, bytes.NewReader([]byte(body)))
	req3.Header.Set("Authorization", "Bearer wrong")
	w3 := httptest.NewRecorder()
	tc.podA.handleInternalSSENotify(w3, req3)
	if w3.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for wrong auth, got %d", w3.Code)
	}
}

// TestSSEOutboxStalePodSweep verifies that the leader's SweepStalePods marks
// pods with expired heartbeats as stale and cleans up their subscriptions.
func TestSSEOutboxStalePodSweep(t *testing.T) {
	tc := newSSEOutboxTestCluster(t)

	// Register a "dead" pod with a stale heartbeat and a subscription.
	if err := tc.metaStore.UpsertPod(context.Background(), "dead-pod", "http://127.0.0.1:1"); err != nil {
		t.Fatal(err)
	}
	if err := tc.metaStore.UpsertPodSubscriptions(context.Background(), "dead-pod", []string{tc.tenantID}); err != nil {
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