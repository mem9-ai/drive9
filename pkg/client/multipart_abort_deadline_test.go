package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A best-effort cleanup must never hold a caller past its own deadline: when
// Complete and Abort both stall, the upload has to return around the caller's
// deadline instead of waiting out another abort timeout (5s by default).
// The new WebDAV commit path reaches this on every oversized PUT.
func TestMultipartUploadOutlivesNeitherCompleteNorAbortStall(t *testing.T) {
	// Teardown must not wait for the deliberately stalled handlers: release
	// them before the server closes (Cleanups run LIFO, so srv.Close is
	// registered first).
	release := make(chan struct{})
	var releaseOnce sync.Once
	stop := func() { releaseOnce.Do(func() { close(release) }) }
	stall := func(r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}
	var aborts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(uploadPlanV2{UploadID: "stalled", PartSize: 8, TotalParts: 1})
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/stalled/presign-batch":
			_ = json.NewEncoder(w).Encode(struct {
				Parts []presignedPart `json:"parts"`
			}{Parts: []presignedPart{{
				Number: 1, URL: fmt.Sprintf("http://%s/part", r.Host), Size: 8, ExpiresAt: time.Now().Add(time.Minute),
			}}})
		case r.Method == http.MethodPut && r.URL.Path == "/part":
			w.Header().Set("ETag", `"etag"`)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/stalled/complete":
			stall(r) // until the caller gives up
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/stalled/abort":
			aborts.Add(1)
			stall(r) // stalls too: the cleanup must be detached
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(stop)

	api := New(srv.URL, "")
	api.multipartAbortTimeout = 5 * time.Second // the default the finding names
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := api.WriteMultipartStreamConditional(ctx, "/file", bytes.NewReader([]byte("12345678")), 8, nil, -1)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("stalled complete reported success")
	}
	if elapsed > time.Second {
		t.Fatalf("upload returned after %s; the abort cleanup outlived the caller deadline", elapsed)
	}
	// The cleanup must still be attempted (detached), not skipped.
	deadline := time.Now().Add(time.Second)
	for aborts.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if aborts.Load() == 0 {
		t.Fatal("aborted upload session was leaked: no cleanup request")
	}
}

// The cleanup still RUNS when the caller has already given up — detached, not
// skipped: the upload session must not leak either.
func TestMultipartAbortRunsDetachedAfterDeadline(t *testing.T) {
	c := New("http://127.0.0.1:0", "")
	c.multipartAbortTimeout = 2 * time.Second

	started := make(chan struct{})
	released := make(chan struct{})
	parent, cancel := context.WithCancel(context.Background())
	cancel() // the caller is already gone

	done := make(chan struct{})
	go func() {
		c.runMultipartAbort(parent, func(ctx context.Context) error {
			close(started)
			<-ctx.Done() // stays bounded by the cleanup's own timeout
			close(released)
			return ctx.Err()
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("cleanup blocked the caller after its deadline")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("cleanup never ran")
	}
}
