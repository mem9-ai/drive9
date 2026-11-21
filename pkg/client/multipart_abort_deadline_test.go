package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	completeReached := make(chan struct{})
	var completeStarted sync.Once
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
			completeStarted.Do(func() { close(completeReached) })
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
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	result := make(chan error, 1)
	go func() {
		result <- api.WriteMultipartStreamConditional(ctx, "/file", bytes.NewReader([]byte("12345678")), 8, nil, -1)
	}()
	// The scenario under test is a STALLED Complete: a failure before that
	// phase would return quickly and prove nothing about the abort bound.
	select {
	case <-completeReached:
	case <-time.After(2 * time.Second):
		t.Fatal("Complete was never reached; the upload failed earlier")
	}
	var err error
	select {
	case err = <-result:
	case <-time.After(2 * time.Second):
		t.Fatal("upload did not return after its deadline")
	}
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
	c.multipartAbortTimeout = 100 * time.Millisecond

	started := make(chan struct{})
	released := make(chan struct{})
	release := make(chan struct{})
	var cleanupErr atomic.Value
	parent, cancel := context.WithCancel(context.Background())
	cancel() // the caller is already gone

	done := make(chan struct{})
	go func() {
		c.runMultipartAbort(parent, func(ctx context.Context) error {
			close(started)
			select {
			case <-ctx.Done(): // the cleanup's OWN bounded context
			case <-release:
			}
			cleanupErr.Store(ctx.Err())
			close(released)
			return ctx.Err()
		})
		close(done)
	}()
	t.Cleanup(func() { close(release) }) // never strand the goroutine on a regression

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
	// The detached cleanup must reach its own timeout, not block forever: a
	// cleanup context that never expires would leave the session open.
	select {
	case <-released:
	case <-time.After(2 * time.Second):
		t.Fatal("detached cleanup never expired on its own")
	}
	if err, _ := cleanupErr.Load().(error); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("detached cleanup error = %v, want context.DeadlineExceeded", err)
	}
}

// A caller that gives up WHILE the synchronous cleanup is running must stop
// waiting immediately; the cleanup keeps running on its own bounded context.
func TestMultipartAbortStopsWaitingWhenCallerCancels(t *testing.T) {
	c := New("http://127.0.0.1:0", "")
	c.multipartAbortTimeout = 5 * time.Second

	parent, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	release := make(chan struct{})
	cleanupDone := make(chan struct{})
	var once sync.Once
	stop := func() { once.Do(func() { close(release) }) }
	t.Cleanup(stop)
	fn := func(ctx context.Context) error {
		close(started)
		select {
		case <-ctx.Done():
		case <-release:
		}
		close(cleanupDone)
		return ctx.Err()
	}

	returned := make(chan struct{})
	go func() {
		c.runMultipartAbort(parent, fn)
		close(returned)
	}()
	<-started
	cancel()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("cleanup held the caller past its cancellation")
	}
	stop()
	select {
	case <-cleanupDone:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not finish after release")
	}
}
