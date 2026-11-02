package writeback

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// A synchronous truncate/close upload must own the same per-path gate as
// background uploads. Otherwise two owners publish one cached generation with
// the same base revision, turning an acknowledged truncate into a false EIO.
func TestUploadSyncSerializesCachedGeneration(t *testing.T) {
	for _, background := range []bool{false, true} {
		name := "sync"
		if background {
			name = "background"
		}
		t.Run(name, func(t *testing.T) {
			started := make(chan struct{})
			release := make(chan struct{})
			duplicate := make(chan struct{}, 1)
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			var calls atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPut || r.Header.Get("X-Dat9-Expected-Revision") != "1" {
					t.Errorf("unexpected upload: %s expected revision %q", r.Method, r.Header.Get("X-Dat9-Expected-Revision"))
				}
				if calls.Add(1) != 1 {
					duplicate <- struct{}{}
					w.WriteHeader(http.StatusConflict)
					return
				}
				close(started)
				<-release
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"revision":2}`))
			}))
			t.Cleanup(ts.Close)
			cache, err := staging.NewWriteBackCache(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := cache.PutWithBaseRev("/truncate", nil, 0, staging.PendingOverwrite, 1); err != nil {
				t.Fatal(err)
			}
			uploader := NewWriteBackUploader(newTestClient(ts.URL), cache, 1)
			t.Cleanup(uploader.DrainAll)
			t.Cleanup(unblock)
			first := make(chan error, 1)
			go func() { first <- uploader.UploadSync(context.Background(), "/truncate") }()
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("first upload did not reach the server")
			}
			second := make(chan error, 1)
			go func() {
				if background {
					uploader.uploadOne("/truncate")
					second <- nil
				} else {
					second <- uploader.UploadSync(context.Background(), "/truncate")
				}
			}()
			// Keep the first publication in flight while the competing owner
			// attempts to drain the same generation. No successful upload is
			// retried or suppressed by the fixture.
			select {
			case <-duplicate:
			case <-time.After(150 * time.Millisecond):
			}
			unblock()
			for _, done := range []chan error{first, second} {
				select {
				case err := <-done:
					if err != nil {
						t.Errorf("same-generation upload failed: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("upload did not drain")
				}
			}
			if calls.Load() != 1 {
				t.Fatalf("same cached generation published %d times, want 1", calls.Load())
			}
			if _, ok := cache.GetMeta("/truncate"); ok {
				t.Fatal("acknowledged generation remained pending")
			}
		})
	}
}
