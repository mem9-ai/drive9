package extent

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/juicedata/juicefs/pkg/object"
)

// gcsFake is a minimal GCS JSON API stub. It answers every request with a fixed
// status and records the Authorization header, which is enough to pin the
// credential, the endpoint, and the not-found mappings without a live bucket.
type gcsFake struct {
	mu     sync.Mutex
	status int
	auth   string
	hits   int
}

func (f *gcsFake) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.auth = r.Header.Get("Authorization")
		f.hits++
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(f.status)
		_, _ = fmt.Fprintf(w, `{"error":{"code":%d,"message":"stub"}}`, f.status)
	})
}

func (f *gcsFake) snapshot() (auth string, hits int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.auth, f.hits
}

func newGCSFake(t *testing.T, status int) (*gcsFake, string) {
	t.Helper()
	f := &gcsFake{status: status}
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	return f, srv.URL
}

func TestOpenStorageGCSRequiresBucketAndToken(t *testing.T) {
	cases := []struct {
		name string
		cred *Credential
		want string
	}{
		{"missing bucket", &Credential{Scheme: SchemeGCS, AccessToken: "tok"}, "bucket"},
		{"missing token", &Credential{Scheme: SchemeGCS, Bucket: "bucket"}, "token"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := OpenStorage(tc.cred, nil)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want an error naming %q", err, tc.want)
			}
		})
	}
}

// TestOpenStorageGCSAppliesTenantPrefix pins that a gcs credential uses the
// bucket the server named and still gets the tenant block prefix applied by
// OpenStorage, exactly like the s3 path.
func TestOpenStorageGCSAppliesTenantPrefix(t *testing.T) {
	st, err := OpenStorage(&Credential{
		Scheme:      SchemeGCS,
		Bucket:      "prod-drive9-gcs-us-east1",
		AccessToken: "downscoped-token",
		Prefix:      "tenants/fs-1/t/tenant-z/",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	rs, ok := st.(*refreshingStore)
	if !ok {
		t.Fatalf("type = %T, want *refreshingStore", st)
	}
	if rs.scheme != SchemeGCS {
		t.Fatalf("scheme = %q, want %q", rs.scheme, SchemeGCS)
	}
	if rs.Prefix() != "tenants/fs-1/t/tenant-z/" {
		t.Fatalf("prefix = %q", rs.Prefix())
	}
}

// TestOpenInnerGCSStore pins the bucket-scoped store itself: OpenStorage wraps
// it in object.WithPrefix, so the concrete type is only visible here.
func TestOpenInnerGCSStore(t *testing.T) {
	st, err := openInner(&Credential{Scheme: SchemeGCS, Bucket: "prod-drive9-gcs-us-east1", AccessToken: "tok"})
	if err != nil {
		t.Fatal(err)
	}
	gs, ok := st.(*gcsTokenStorage)
	if !ok {
		t.Fatalf("type = %T, want *gcsTokenStorage", st)
	}
	defer gs.Shutdown()
	if gs.bucket != "prod-drive9-gcs-us-east1" {
		t.Fatalf("bucket = %q", gs.bucket)
	}
	if got, want := gs.String(), "gs://prod-drive9-gcs-us-east1/"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

// TestGCSStoreHonoursEndpointAndPresentsToken proves the store authenticates
// with the server's short-lived token rather than the mount's own workload
// identity (which is what bounds it to the tenant prefix), that a credential
// endpoint is honoured, and that a missing object maps to os.ErrNotExist.
func TestGCSStoreHonoursEndpointAndPresentsToken(t *testing.T) {
	f, url := newGCSFake(t, http.StatusNotFound)
	ctx := context.Background()

	st, err := openInner(&Credential{
		Scheme:      SchemeGCS,
		Bucket:      "bucket",
		AccessToken: "downscoped-token",
		Endpoint:    url,
	})
	if err != nil {
		t.Fatal(err)
	}
	gs := st.(*gcsTokenStorage)
	defer gs.Shutdown()

	if _, err := gs.Head(ctx, "t/tenant-z/chunks/1/2/3_0_4"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Head error = %v, want os.ErrNotExist", err)
	}
	auth, hits := f.snapshot()
	if hits == 0 {
		t.Fatal("credential endpoint was ignored: the fake received no request")
	}
	if auth != "Bearer downscoped-token" {
		t.Fatalf("Authorization = %q, want the downscoped token", auth)
	}
}

// TestGCSDeleteIsIdempotentWhenMissing mirrors JuiceFS's gs backend: a delete
// for an object that is already gone is a success, even when the SDK returns
// the sentinel wrapped.
func TestGCSDeleteIsIdempotentWhenMissing(t *testing.T) {
	_, url := newGCSFake(t, http.StatusNotFound)
	gs, err := newGCSTokenStorage(context.Background(), "bucket", "tok", url)
	if err != nil {
		t.Fatal(err)
	}
	defer gs.Shutdown()
	if err := gs.Delete(context.Background(), "t/tenant-z/chunks/1"); err != nil {
		t.Fatalf("Delete on a missing object = %v, want nil", err)
	}
}

// TestGCSWriterUsesExtentChunkSize pins the upload chunk contract: a dropped
// ChunkSize would otherwise only show up as buffered uploads in production.
func TestGCSWriterUsesExtentChunkSize(t *testing.T) {
	gs, err := newGCSTokenStorage(context.Background(), "bucket", "tok", "")
	if err != nil {
		t.Fatal(err)
	}
	defer gs.Shutdown()
	if w := gs.writer(context.Background(), "t/tenant-z/chunks/1"); w.ChunkSize != gcsExtentChunkSize {
		t.Fatalf("ChunkSize = %d, want %d", w.ChunkSize, gcsExtentChunkSize)
	}
}

// shutdownRecorder wraps a working store and records Shutdown calls, used to
// pin the lifecycle wiring.
type shutdownRecorder struct {
	object.ObjectStorage
	shutdowns atomic.Int64
}

func (s *shutdownRecorder) Shutdown() { s.shutdowns.Add(1) }

func newShutdownRecorder(t *testing.T) *shutdownRecorder {
	t.Helper()
	inner, err := object.CreateStorage("file", t.TempDir(), "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	return &shutdownRecorder{ObjectStorage: inner}
}

// TestRefreshingStoreShutdownReachesInner pins that Shutdown forwards through
// the prefix wrapper to the store that owns the client.
func TestRefreshingStoreShutdownReachesInner(t *testing.T) {
	rec := newShutdownRecorder(t)
	st := &refreshingStore{inner: object.WithPrefix(rec, "t/x/")}
	st.Shutdown()
	if got := rec.shutdowns.Load(); got != 1 {
		t.Fatalf("inner shutdowns = %d, want 1", got)
	}
}

// TestCloseRuntimeShutsStorage pins that teardown releases the object store even
// when there is no JuiceFS session to close.
func TestCloseRuntimeShutsStorage(t *testing.T) {
	rec := newShutdownRecorder(t)
	if err := CloseRuntime(&Runtime{Storage: rec}); err != nil {
		t.Fatal(err)
	}
	if got := rec.shutdowns.Load(); got != 1 {
		t.Fatalf("storage shutdowns = %d, want 1", got)
	}
}
