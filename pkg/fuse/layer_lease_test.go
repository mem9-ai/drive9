package fuse

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestLayerRenewRetriesBeforeSafetyMargin(t *testing.T) {
	const layerID = "layer_01j8z6kh2m9q7w3r5t6y8v0p1b"
	retried := make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		if calls.Load() == 2 {
			close(retried)
		}
		_ = json.NewEncoder(w).Encode(client.LayerMountLease{LayerID: layerID, Fence: 1, MountLeaseTTLMS: 1000})
	}))
	defer server.Close()
	b := &layerMountBinding{LayerID: layerID, Fence: 1, MountID: "test-mount", BaseClient: client.New(server.URL, "")}
	b.startRenewLoop(time.Now(), 1000, layerLeaseTiming{
		renewInterval: 500 * time.Millisecond, retryInterval: 10 * time.Millisecond,
		selfFenceMargin: 200 * time.Millisecond, callTimeout: 100 * time.Millisecond,
	})
	defer b.StopRenew()
	select {
	case <-retried:
	case <-b.renewDone:
		t.Fatal("mount self-fenced after a single temporary error without retrying")
	case <-time.After(3 * time.Second):
		t.Fatal("renewal did not retry")
	}
}

func TestLayerStalledRenewStopsAtSafetyMargin(t *testing.T) {
	const layerID = "layer_01j8z6kh2m9q7w3r5t6y8v0p1b"
	started := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	defer close(release)
	b := &layerMountBinding{LayerID: layerID, Fence: 1, MountID: "test-mount", BaseClient: client.New(server.URL, "")}
	b.startRenewLoop(time.Now(), 500, layerLeaseTiming{
		renewInterval: 50 * time.Millisecond, retryInterval: 10 * time.Millisecond,
		selfFenceMargin: 200 * time.Millisecond, callTimeout: 5 * time.Second,
	})
	defer b.StopRenew()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("renewal did not start")
	}
	select {
	case <-b.renewDone:
		if !errors.Is(b.checkWriteAllowed(), ErrLayerLeaseSelfFenced) {
			t.Fatal("old lease still permits writes after the safety margin")
		}
	case <-time.After(time.Second):
		t.Fatal("a stalled renewal delayed self-fencing beyond lease expiration")
	}
}

func TestRenewReturnAtOldCutoffUsesConfirmedLease(t *testing.T) {
	for _, tt := range []struct {
		name   string
		ttl    int64
		fenced bool
	}{
		{"extended", 60000, false},
		{"already inside new margin", 1000, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := &layerMountBinding{renewDone: make(chan struct{})}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				calls := 0
				go b.runRenewLoop(ctx, time.Now().Add(5*time.Second), layerLeaseTiming{
					renewInterval: time.Second, selfFenceMargin: time.Second, callTimeout: 5 * time.Second,
				}, func(call context.Context, _ string, _ client.LayerMountRenewRequest) (*client.LayerMountLease, error) {
					calls++
					// The response was accepted, then the caller resumed at the old cutoff.
					<-call.Done()
					return &client.LayerMountLease{MountLeaseTTLMS: tt.ttl}, nil
				})
				time.Sleep(4 * time.Second)
				synctest.Wait()
				if calls != 1 || b.fenced.Load() != tt.fenced {
					t.Fatalf("renewals=%d fenced=%v, want 1 and %v", calls, b.fenced.Load(), tt.fenced)
				}
				cancel()
				<-b.renewDone
			})
		})
	}
}

func TestBindAndExpireConflictsReportBusyLease(t *testing.T) {
	for _, err := range []error{client.ErrConflict, &client.StatusError{StatusCode: http.StatusConflict}} {
		assertMountExit(t, classifyLayerBindError("acquire layer lease", err), ExitLayerLeaseBusy, ExitReasonLayerLeaseBusy)
	}
	assertMountExit(t, classifyLayerBindError("acquire layer lease", &client.StatusError{StatusCode: http.StatusForbidden}), ExitStartupPermanent, ExitReasonStartupPermanent)
}
