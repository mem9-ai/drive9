package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestPodNotifierPushWakesSubscriber verifies the pod-to-pod HTTP push path:
// when Notify is called with a tenant that has subscribers on a peer pod, the
// peer receives the POST and wakes its local SSE subscribers via Publish.
func TestPodNotifierPushWakesSubscriber(t *testing.T) {
	metaStore := newTestMetaStoreForNotify(t)
	store := newTestStoreForEventBus(t)

	// Simulate the receiving pod: an EventBus for tenant "push-tenant" with a
	// subscriber, and an HTTP handler that mimics handleInternalSSENotify.
	receiverBuses := newEventBuses()
	receiverBus := receiverBuses.get("push-tenant", store)
	subID, notify := receiverBus.Subscribe()
	defer receiverBus.Unsubscribe(subID)

	var receivedCount int
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != sseNotifyInternalRoute {
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var req notifyPushRequest
		_ = json.Unmarshal(body, &req)
		if bus := receiverBuses.getIfExists(req.TenantID); bus != nil {
			bus.Publish()
		}
		receivedCount++
		w.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()

	// Register the receiver pod in pod_registry and its subscription for the tenant.
	if err := metaStore.UpsertPod(context.Background(), "receiver-pod", receiver.URL); err != nil {
		t.Fatal(err)
	}
	if err := metaStore.UpsertPodSubscriptions(context.Background(), "receiver-pod", []string{"push-tenant"}); err != nil {
		t.Fatal(err)
	}

	// Create the notifier on the "sender" pod. It will read the route table
	// from the central DB and push to the receiver.
	notifier := newPodNotifier(metaStore, "sender-pod", []byte("test-secret"))
	notifier.Start(context.Background())
	defer notifier.Stop()

	// Wait for the route table to refresh (the notifier refreshes on Start).
	// Give it a moment to read pod_registry + pod_subscriptions.
	time.Sleep(6 * time.Second)

	// Notify should push to the receiver, which wakes the subscriber.
	notifier.Notify("push-tenant", 42)

	select {
	case _, open := <-notify:
		if !open {
			t.Fatal("notify channel closed")
		}
		// Success: the push woke the subscriber.
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for push to wake subscriber")
	}
}

// TestPodNotifierFireAndForget verifies that Notify does not block even when the
// peer is unreachable. The push is dispatched in a goroutine with a timeout.
func TestPodNotifierFireAndForget(t *testing.T) {
	metaStore := newTestMetaStoreForNotify(t)

	// Register a peer with an unreachable address.
	if err := metaStore.UpsertPod(context.Background(), "dead-pod", "http://127.0.0.1:1"); err != nil {
		t.Fatal(err)
	}
	if err := metaStore.UpsertPodSubscriptions(context.Background(), "dead-pod", []string{"fire-forget-tenant"}); err != nil {
		t.Fatal(err)
	}

	notifier := newPodNotifier(metaStore, "sender-pod", []byte("test-secret"))
	// Use a short push timeout so the test doesn't hang waiting for the
	// unreachable peer's goroutine to time out.
	notifier.pushTimeout = 500 * time.Millisecond
	notifier.Start(context.Background())
	defer notifier.Stop()

	// Wait for route refresh.
	time.Sleep(6 * time.Second)

	// Notify must return immediately (fire-and-forget), even though the peer
	// is unreachable. The goroutine handling the failed POST will time out in
	// the background.
	done := make(chan struct{})
	go func() {
		notifier.Notify("fire-forget-tenant", 1)
		close(done)
	}()
	select {
	case <-done:
		// Success: returned without blocking.
	case <-time.After(time.Second):
		t.Fatal("Notify blocked on unreachable peer")
	}
}

// TestPodNotifierNoPeersIsNoop verifies that when no peers are registered (or
// no peers subscribe to the tenant), Notify is a no-op.
func TestPodNotifierNoPeersIsNoop(t *testing.T) {
	metaStore := newTestMetaStoreForNotify(t)
	notifier := newNotifyPoller(metaStore, newEventBuses(), 50*time.Millisecond)
	_ = notifier // just ensure it constructs without error

	pn := newPodNotifier(metaStore, "self-pod", []byte("test-secret"))
	pn.Start(context.Background())
	defer pn.Stop()

	// No peers registered — Notify should be a no-op (no panic, no block).
	pn.Notify("no-peers-tenant", 1)
}

// TestHandleInternalSSENotify verifies the internal HTTP endpoint: a POST with
// valid auth and a tenant_id wakes the local EventBus subscriber; a POST with
// invalid auth is rejected.
func TestHandleInternalSSENotify(t *testing.T) {
	store := newTestStoreForEventBus(t)
	buses := newEventBuses()
	bus := buses.get("notify-tenant", store)
	subID, notify := bus.Subscribe()
	defer bus.Unsubscribe(subID)

	srv := &Server{
		events:          buses,
		podNotifySecret: []byte("shared-secret"),
	}

	// Valid request should wake the subscriber.
	body := `{"tenant_id":"notify-tenant","seq":1}`
	req := httptest.NewRequest(http.MethodPost, sseNotifyInternalRoute, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer shared-secret")
	w := httptest.NewRecorder()
	srv.handleInternalSSENotify(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}

	select {
	case _, open := <-notify:
		if !open {
			t.Fatal("notify channel closed")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for notify after internal push")
	}

	// Invalid auth should be rejected.
	req2 := httptest.NewRequest(http.MethodPost, sseNotifyInternalRoute, strings.NewReader(body))
	req2.Header.Set("Authorization", "Bearer wrong-secret")
	w2 := httptest.NewRecorder()
	srv.handleInternalSSENotify(w2, req2)
	if w2.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for invalid secret, got %d", w2.Code)
	}

	// Missing auth should be rejected.
	req3 := httptest.NewRequest(http.MethodPost, sseNotifyInternalRoute, strings.NewReader(body))
	w3 := httptest.NewRecorder()
	srv.handleInternalSSENotify(w3, req3)
	if w3.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing token, got %d", w3.Code)
	}

	// No secret configured should return 404.
	srv2 := &Server{events: buses}
	req4 := httptest.NewRequest(http.MethodPost, sseNotifyInternalRoute, strings.NewReader(body))
	req4.Header.Set("Authorization", "Bearer shared-secret")
	w4 := httptest.NewRecorder()
	srv2.handleInternalSSENotify(w4, req4)
	if w4.Code != http.StatusNotFound {
		t.Fatalf("expected 404 when no secret configured, got %d", w4.Code)
	}
}