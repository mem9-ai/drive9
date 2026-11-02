package core

// Fifth review round: the detached namespace context keeps the caller's
// explicit deadline and honors LegacyInterruptibleMutations.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// A GetAttr with an explicit 100 ms deadline aborts at its deadline —
// the detach must not stretch the caller's budget to the detached
// window while the response is parked.
func TestPortDetachedGetAttrKeepsCallerDeadline(t *testing.T) {
	var hits atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && r.URL.Path == "/v1/fs/f" {
			hits.Add(1)
			time.Sleep(300 * time.Millisecond)
		}
		testbackend.NewBackend().ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	c, err := New(cl, Options{})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = c.Do(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/f"}, func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		return fs.Result{}, nil
	})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("detached getattr outlived its caller deadline")
	}
	if elapsed > 250*time.Millisecond {
		t.Fatalf("getattr took %v, want the 100 ms caller deadline to bound it", elapsed)
	}
}

// LegacyInterruptibleMutations keeps cancellation live: a canceled
// in-flight mutation surfaces the cancellation instead of detaching.
func TestPortLegacyInterruptibleMutationsStayCancellable(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			time.Sleep(300 * time.Millisecond)
		}
		testbackend.NewBackend().ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	c, err := New(cl, Options{LegacyInterruptibleMutations: true})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := c.Do(ctx, &fs.Op{Kind: fs.OpMkdir, Path: "/d"}, func(ctx context.Context, op *fs.Op) (fs.Result, error) {
			return fs.Result{}, nil
		})
		errCh <- err
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("canceled legacy-interruptible mutation returned success")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("mutation ignored cancellation")
	}
}
