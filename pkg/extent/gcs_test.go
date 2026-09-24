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
	"time"

	"github.com/juicedata/juicefs/pkg/object"
)

// gcsFake is a minimal GCS JSON API stub. It answers every request with a fixed
// status and records the Authorization header, which is enough to pin the
// credential, the endpoint, and the not-found mappings without a live bucket.
type gcsFake struct {
	mu     sync.Mutex
	status int
	// deleteStatus overrides status for DELETE requests (0 keeps status).
	deleteStatus int
	auth         string
	uri          string
	hits         int
}

func (f *gcsFake) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.auth = r.Header.Get("Authorization")
		f.uri = r.URL.Path
		f.hits++
		status := f.status
		if r.Method == http.MethodDelete && f.deleteStatus != 0 {
			status = f.deleteStatus
		}
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if status >= http.StatusBadRequest {
			_, _ = fmt.Fprintf(w, `{"error":{"code":%d,"message":"stub"}}`, status)
		}
	})
}

func (f *gcsFake) snapshot() (auth string, hits int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.auth, f.hits
}

func (f *gcsFake) lastPath() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.uri
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
// bucket the server named and that the tenant block prefix reaches the actual
// request, not just Prefix(): a regression that left Get/Head keys unprefixed
// would still pass a Prefix()-only assertion.
func TestOpenStorageGCSAppliesTenantPrefix(t *testing.T) {
	f, url := newGCSFake(t, http.StatusNotFound)
	st, err := OpenStorage(&Credential{
		Scheme:      SchemeGCS,
		Bucket:      "prod-drive9-gcs-us-east1",
		AccessToken: "downscoped-token",
		Prefix:      "tenants/fs-1/t/tenant-z/",
		Endpoint:    url + "/storage/v1/",
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
	if _, err := st.Head(context.Background(), "chunks/1"); err == nil {
		t.Fatal("Head against the fake must fail with not-found")
	}
	if uri := f.lastPath(); !strings.Contains(uri, "tenant-z") {
		t.Fatalf("request path %q does not contain the tenant prefix", uri)
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
		// Slash-less base path: it must be normalised so requests still land
		// under /storage/v1/b/.
		Endpoint: url + "/storage/v1",
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
	if uri := f.lastPath(); !strings.HasPrefix(uri, "/storage/v1/b/") {
		t.Fatalf("request path %q is not under the JSON API base", uri)
	}
}

// TestGCSUploadUsesAPIRoot pins the upload leg's contract: BasePath feeds reads,
// but the generated client builds resumable uploads from an absolute
// /upload/storage/v1/... reference that drops any base path, which is why the
// endpoint must be the API root.
func TestGCSUploadUsesAPIRoot(t *testing.T) {
	f, url := newGCSFake(t, http.StatusNotFound)
	gs, err := newGCSTokenStorage(context.Background(), "bucket", "tok", url+"/storage/v1/")
	if err != nil {
		t.Fatal(err)
	}
	defer gs.Shutdown()
	// The fake 404s the resumable session; the request path is what matters.
	_ = gs.Put(context.Background(), "t/tenant-z/chunks/1", strings.NewReader("x"))
	if uri := f.lastPath(); !strings.HasPrefix(uri, "/upload/storage/v1/b/") {
		t.Fatalf("upload path %q must be the API root's /upload/... (BasePath only feeds reads)", uri)
	}
}

// TestGCSEmulatorHostMustBeLoopback pins the fail-closed guard: storage.NewClient
// drops the bearer token and installs an unvalidated endpoint whenever
// STORAGE_EMULATOR_HOST is set, so a non-loopback value must be rejected.
func TestGCSEmulatorHostMustBeLoopback(t *testing.T) {
	ctx := context.Background()
	t.Setenv("STORAGE_EMULATOR_HOST", "evil.example")
	if _, err := newGCSTokenStorage(ctx, "bucket", "tok", ""); err == nil {
		t.Fatal("a non-loopback STORAGE_EMULATOR_HOST must be rejected")
	}
	t.Setenv("STORAGE_EMULATOR_HOST", "127.0.0.1:9000")
	if _, err := newGCSTokenStorage(ctx, "bucket", "tok", "https://gw.example.com/storage/v1/"); err == nil {
		t.Fatal("STORAGE_EMULATOR_HOST with an explicit endpoint must be rejected")
	}
	gs, err := newGCSTokenStorage(ctx, "bucket", "tok", "")
	if err != nil {
		t.Fatalf("loopback emulator host rejected: %v", err)
	}
	gs.Shutdown()
}

// TestGCSDeleteIsIdempotent mirrors JuiceFS's gs backend: deleting an object is
// a success whether the object was there (200) or already gone (404).
func TestGCSDeleteIsIdempotent(t *testing.T) {
	for _, tc := range []struct {
		name         string
		deleteStatus int
	}{
		{"present", http.StatusOK},
		{"already gone", http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, url := newGCSFake(t, http.StatusNotFound)
			f.deleteStatus = tc.deleteStatus
			gs, err := newGCSTokenStorage(context.Background(), "bucket", "tok", url+"/storage/v1/")
			if err != nil {
				t.Fatal(err)
			}
			defer gs.Shutdown()
			if err := gs.Delete(context.Background(), "t/tenant-z/chunks/1"); err != nil {
				t.Fatalf("Delete = %v, want nil", err)
			}
		})
	}
}

// TestClosedStoreRefusesIO pins that every I/O entry point fails closed after
// Shutdown instead of serving a released store.
func TestClosedStoreRefusesIO(t *testing.T) {
	rec := newShutdownRecorder(t)
	st := &refreshingStore{inner: object.WithPrefix(rec, "t/x/"), cred: Credential{Scheme: SchemeGCS}}
	st.Shutdown()
	ctx := context.Background()
	if _, err := st.Get(ctx, "k", 0, 0); !errors.Is(err, errExtentStoreClosed) {
		t.Fatalf("Get err = %v, want %v", err, errExtentStoreClosed)
	}
	if err := st.Put(ctx, "k", strings.NewReader("x")); !errors.Is(err, errExtentStoreClosed) {
		t.Fatalf("Put err = %v, want %v", err, errExtentStoreClosed)
	}
	if err := st.Delete(ctx, "k"); !errors.Is(err, errExtentStoreClosed) {
		t.Fatalf("Delete err = %v, want %v", err, errExtentStoreClosed)
	}
	if _, err := st.Head(ctx, "k"); !errors.Is(err, errExtentStoreClosed) {
		t.Fatalf("Head err = %v, want %v", err, errExtentStoreClosed)
	}
}

// TestSupersededStoreRetiredOnlyWhenIdle pins the refcount: a store held by an
// in-flight operation is not shut down by a refresh until the last holder
// releases.
func TestSupersededStoreRetiredOnlyWhenIdle(t *testing.T) {
	ctx := context.Background()
	rec := newShutdownRecorder(t)
	st := &refreshingStore{
		src:    staticCredentialSource{cred: &Credential{Scheme: SchemeGCS, Bucket: "bucket", AccessToken: "fresh", ExpiresAt: time.Now().Add(-time.Minute).Format(time.RFC3339)}},
		inner:  object.WithPrefix(rec, "t/x/"),
		cred:   Credential{Scheme: SchemeGCS},
		scheme: SchemeGCS,
	}
	// First holder: the seeded credential has no expiry, so no refresh happens.
	_, release1, err := st.acquireInner(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Make the current credential due so the next acquire refreshes.
	st.mu.Lock()
	st.cred.ExpiresAt = time.Now().Add(-time.Minute).Format(time.RFC3339)
	st.mu.Unlock()

	_, release2, err := st.acquireInner(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { object.Shutdown(st.inner) })
	if got := rec.shutdowns.Load(); got != 0 {
		t.Fatalf("superseded store shut down with a holder: %d", got)
	}
	release1()
	if got := rec.shutdowns.Load(); got != 0 {
		t.Fatalf("superseded store shut down with a holder: %d", got)
	}
	release2()
	if got := rec.shutdowns.Load(); got != 1 {
		t.Fatalf("superseded store shutdowns = %d, want 1", got)
	}
}

// TestGCSEndpointValidation pins the gcs endpoint contract: an https JSON API
// base path, with plaintext allowed only on loopback.
func TestGCSEndpointValidation(t *testing.T) {
	ctx := context.Background()
	for _, endpoint := range []string{
		"http://127.0.0.1:9000",                  // bare host, no API root
		"https://storage.googleapis.com",         // bare host, no API root
		"ftp://host/storage/v1/",                 // unsupported scheme
		"http://storage.example/storage/v1/",     // plaintext off loopback
		"https://gw.example.com/gcs/storage/v1/", // path prefix: uploads would drop it
	} {
		t.Run("reject "+endpoint, func(t *testing.T) {
			if _, err := newGCSTokenStorage(ctx, "bucket", "tok", endpoint); err == nil {
				t.Fatalf("endpoint %q must be rejected", endpoint)
			}
		})
	}
	for _, endpoint := range []string{
		"https://emulator.example/storage/v1/",
		"https://emulator.example/storage/v1", // no trailing slash: normalised, not rejected
		"http://127.0.0.1:9000/storage/v1/",   // loopback emulator
	} {
		t.Run("accept "+endpoint, func(t *testing.T) {
			gs, err := newGCSTokenStorage(ctx, "bucket", "tok", endpoint)
			if err != nil {
				t.Fatal(err)
			}
			gs.Shutdown()
		})
	}
}

// TestOpenStorageCanonicalScheme pins that the Cloud Storage scheme is
// normalized: "gs" (the object-backend canonicalization) and "gcs" (the extent
// wire value) both report "gcs", so String() and metric labels stay stable.
func TestOpenStorageCanonicalScheme(t *testing.T) {
	for _, scheme := range []string{"gcs", "gs", " GS ", "GCS"} {
		t.Run(scheme, func(t *testing.T) {
			st, err := OpenStorage(&Credential{Scheme: scheme, Bucket: "bucket", AccessToken: "tok"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			rs := st.(*refreshingStore)
			if rs.Scheme() != SchemeGCS {
				t.Fatalf("Scheme() = %q, want %q", rs.Scheme(), SchemeGCS)
			}
			if rs.String() != "drive9-gcs" {
				t.Fatalf("String() = %q", rs.String())
			}
		})
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

// TestRefreshDisposesSupersededStore pins the per-renewal disposal: replacing
// the inner store on a credential refresh shuts the superseded one down exactly
// once (a GCS store owns a client), and a failed refresh disposes nothing.
func TestRefreshDisposesSupersededStore(t *testing.T) {
	expired := time.Now().Add(-time.Minute).Format(time.RFC3339)

	t.Run("success disposes the superseded store", func(t *testing.T) {
		rec := newShutdownRecorder(t)
		st := &refreshingStore{
			src: staticCredentialSource{cred: &Credential{
				Scheme: SchemeGCS, Bucket: "bucket", AccessToken: "fresh", ExpiresAt: expired,
			}},
			inner:  object.WithPrefix(rec, "t/x/"),
			cred:   Credential{Scheme: SchemeGCS, Bucket: "bucket", AccessToken: "old", ExpiresAt: expired},
			scheme: SchemeGCS,
		}
		t.Cleanup(func() { object.Shutdown(st.inner) })
		st.mu.Lock()
		_, err := st.innerStoreLocked(context.Background())
		st.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		if got := rec.shutdowns.Load(); got != 1 {
			t.Fatalf("superseded shutdowns = %d, want 1", got)
		}
	})

	t.Run("failed open disposes nothing", func(t *testing.T) {
		rec := newShutdownRecorder(t)
		st := &refreshingStore{
			// Missing access token: OpenStorage fails, so the old store must stay.
			src:    staticCredentialSource{cred: &Credential{Scheme: SchemeGCS, Bucket: "bucket", ExpiresAt: expired}},
			inner:  object.WithPrefix(rec, "t/x/"),
			cred:   Credential{Scheme: SchemeGCS, Bucket: "bucket", AccessToken: "old", ExpiresAt: expired},
			scheme: SchemeGCS,
		}
		st.mu.Lock()
		_, err := st.innerStoreLocked(context.Background())
		st.mu.Unlock()
		if err == nil {
			t.Fatal("expected the refresh open to fail")
		}
		if got := rec.shutdowns.Load(); got != 0 {
			t.Fatalf("superseded shutdowns = %d, want 0", got)
		}
	})
}
