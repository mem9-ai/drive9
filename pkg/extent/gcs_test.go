package extent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/api/option"
)

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

// TestGCSTokenStoragePresentsDownscopedToken proves the store authenticates
// with the server's short-lived token rather than the mount's own workload
// identity, which is what bounds it to the tenant prefix.
func TestGCSTokenStoragePresentsDownscopedToken(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":{"code":404,"message":"Not Found"}}`))
	}))
	t.Cleanup(srv.Close)

	ctx := context.Background()
	gs, err := newGCSTokenStorage(ctx, "bucket", "downscoped-token", option.WithEndpoint(srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer gs.Shutdown()

	if _, err := gs.Head(ctx, "t/tenant-z/chunks/1/2/3_0_4"); err == nil {
		t.Fatal("Head against the fake must fail with not-found")
	}
	if gotAuth != "Bearer downscoped-token" {
		t.Fatalf("Authorization = %q, want the downscoped token", gotAuth)
	}
}
