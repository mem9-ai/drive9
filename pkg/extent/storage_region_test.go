package extent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/juicedata/juicefs/pkg/object"
)

func TestResolveRegionalEndpoint(t *testing.T) {
	for _, tc := range []struct{ region, want string }{
		{"us-west-2", "https://s3.us-west-2.amazonaws.com"},
		{"ap-southeast-1", "https://s3.ap-southeast-1.amazonaws.com"},
		{"cn-north-1", "https://s3.cn-north-1.amazonaws.com.cn"},
		{"eusc-de-east-1", "https://s3.eusc-de-east-1.amazonaws.eu"},
		{"us-iso-east-1", "https://s3.us-iso-east-1.c2s.ic.gov"},
		{"", ""},
	} {
		if got := resolveRegionalEndpoint(tc.region); got != tc.want {
			t.Fatalf("resolveRegionalEndpoint(%q) = %q, want %q", tc.region, got, tc.want)
		}
	}
}

func TestWithS3EnvScopesRegionAndRestores(t *testing.T) {
	t.Setenv("AWS_REGION", "eu-west-1")
	// t.Setenv first so the framework restores whatever the process had, even
	// though the test itself needs the variable unset.
	t.Setenv("AWS_DEFAULT_REGION", "")
	if err := os.Unsetenv("AWS_DEFAULT_REGION"); err != nil {
		t.Fatal(err)
	}
	seen := map[string]string{}
	if _, err := withS3Env("us-west-2", true, func() (object.ObjectStorage, error) {
		for _, k := range jfsS3RegionEnvs {
			seen[k] = os.Getenv(k)
		}
		seen[jfsS3VHostStyleEnv] = os.Getenv(jfsS3VHostStyleEnv)
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, k := range jfsS3RegionEnvs {
		if seen[k] != "us-west-2" {
			t.Fatalf("%s during create = %q, want the credential region", k, seen[k])
		}
	}
	if seen[jfsS3VHostStyleEnv] != "0" {
		t.Fatalf("%s during create = %q, want path style \"0\"", jfsS3VHostStyleEnv, seen[jfsS3VHostStyleEnv])
	}
	if got := os.Getenv("AWS_REGION"); got != "eu-west-1" {
		t.Fatalf("AWS_REGION after create = %q, want the previous value", got)
	}
	if _, ok := os.LookupEnv("AWS_DEFAULT_REGION"); ok {
		t.Fatal("AWS_DEFAULT_REGION must stay unset after create")
	}
	if _, ok := os.LookupEnv(jfsS3VHostStyleEnv); ok {
		t.Fatal("JFS_S3_VHOST_STYLE must stay unset after create")
	}

	// A credential without a region (older servers) must not touch the process
	// environment at all.
	t.Setenv("AWS_REGION", "eu-west-1")
	if _, err := withS3Env("", false, func() (object.ObjectStorage, error) {
		if got := os.Getenv("AWS_REGION"); got != "eu-west-1" {
			t.Fatalf("AWS_REGION during create = %q, want it untouched", got)
		}
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestS3StorageSignsWithCredentialRegion proves the credential's region wins
// over the endpoint host for a compatible endpoint: the host has no
// *.amazonaws.com zone to parse, so without the scoped AWS_REGION the SDK would
// sign for its default region and every request would fail the bucket's region
// check.
func TestS3StorageSignsWithCredentialRegion(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code></Error>`))
	}))
	defer srv.Close()

	st, err := createS3Storage(endpointWithBucket(srv.URL, "bucket"), "AK", "SK", "", "us-west-2", true)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = st.Head(context.Background(), "k")
	if !strings.Contains(gotAuth, "/us-west-2/s3/aws4_request") {
		t.Fatalf("Authorization = %q, want the us-west-2 signing scope", gotAuth)
	}
}

// TestS3EndpointForCredential pins which endpoint the mount hands JuiceFS: the
// credential's region must win over a plain AWS host (the legacy global
// endpoint is exactly the incident this guards against), while endpoints whose
// addressing the region resolver would not reproduce are preserved.
func TestS3EndpointForCredential(t *testing.T) {
	for _, tc := range []struct{ name, endpoint, region, want string }{
		{"no endpoint uses the region", "", "us-west-2", "https://s3.us-west-2.amazonaws.com"},
		{"legacy global endpoint yields to the region", "https://s3.amazonaws.com", "us-west-2", "https://s3.us-west-2.amazonaws.com"},
		{"wrong regional endpoint yields to the region", "https://s3.us-east-1.amazonaws.com", "us-west-2", "https://s3.us-west-2.amazonaws.com"},
		{"legacy hyphen spelling yields to the region", "https://s3-us-east-1.amazonaws.com", "us-west-2", "https://s3.us-west-2.amazonaws.com"},
		{"trailing slash is still the standard host", "https://s3.amazonaws.com/", "us-west-2", "https://s3.us-west-2.amazonaws.com"},
		{"endpoint with a bucket path is kept", "https://s3.us-west-2.amazonaws.com/bucket", "us-west-2", "https://s3.us-west-2.amazonaws.com/bucket"},
		{"compatible endpoint is kept", "https://minio.example:9000", "us-west-2", "https://minio.example:9000"},
		{"dual-stack endpoint is kept", "https://s3.dualstack.us-east-1.amazonaws.com", "us-west-2", "https://s3.dualstack.us-east-1.amazonaws.com"},
		{"fips endpoint is kept", "https://s3-fips.us-east-1.amazonaws.com", "us-west-2", "https://s3-fips.us-east-1.amazonaws.com"},
		{"no region keeps the endpoint", "https://s3.amazonaws.com", "", "https://s3.amazonaws.com"},
		{"no region and no endpoint stays empty", "", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := s3EndpointForCredential(tc.endpoint, tc.region); got != tc.want {
				t.Fatalf("s3EndpointForCredential(%q, %q) = %q, want %q", tc.endpoint, tc.region, got, tc.want)
			}
		})
	}
}

// TestOpenStorageDerivesEndpointFromRegion proves a credential that carries no
// endpoint but does carry a region still opens: the client resolves the
// regional endpoint itself instead of making JuiceFS call GetBucketLocation,
// which the minted session policy does not grant.
func TestOpenStorageDerivesEndpointFromRegion(t *testing.T) {
	// The mount runs with the operator's environment: isolate the AWS knobs so
	// the "did construction leak?" assertions are meaningful, and let t.Setenv
	// restore whatever the runner had.
	for _, k := range append([]string{jfsS3VHostStyleEnv}, jfsS3RegionEnvs...) {
		t.Setenv(k, "")
		if err := os.Unsetenv(k); err != nil {
			t.Fatal(err)
		}
	}
	st, err := OpenStorage(&Credential{
		Scheme:          SchemeS3,
		Bucket:          "drive9-local",
		Prefix:          "t/abc/",
		AccessKeyID:     "AK",
		SecretAccessKey: "SK",
		Region:          "us-west-2",
		ForcePathStyle:  true,
	}, nil)
	if err != nil {
		t.Fatalf("open with region-only credential: %v", err)
	}
	if st == nil {
		t.Fatal("storage = nil")
	}
	for _, k := range append([]string{jfsS3VHostStyleEnv}, jfsS3RegionEnvs...) {
		if _, ok := os.LookupEnv(k); ok {
			t.Fatalf("%s leaked into the process", k)
		}
	}
}

// TestWithS3EnvRestoresOnPanic proves the env restore is deferred: a panicking
// construction must not leave the process-wide variables overridden, or every
// later mount in this process would sign with the wrong region/style.
func TestWithS3EnvRestoresOnPanic(t *testing.T) {
	t.Setenv("AWS_REGION", "eu-west-1")
	t.Setenv("AWS_DEFAULT_REGION", "")
	if err := os.Unsetenv("AWS_DEFAULT_REGION"); err != nil {
		t.Fatal(err)
	}
	t.Setenv(jfsS3VHostStyleEnv, "")
	if err := os.Unsetenv(jfsS3VHostStyleEnv); err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("create did not panic")
			}
		}()
		_, _ = withS3Env("us-west-2", true, func() (object.ObjectStorage, error) {
			panic("construction failed")
		})
	}()
	if got := os.Getenv("AWS_REGION"); got != "eu-west-1" {
		t.Fatalf("AWS_REGION after panic = %q, want the previous value", got)
	}
	if _, ok := os.LookupEnv("AWS_DEFAULT_REGION"); ok {
		t.Fatal("AWS_DEFAULT_REGION must stay unset after panic")
	}
	if _, ok := os.LookupEnv(jfsS3VHostStyleEnv); ok {
		t.Fatal("JFS_S3_VHOST_STYLE must stay unset after panic")
	}
	// The mutex must have been released as well, or the next mount deadlocks.
	if _, err := withS3Env("", false, func() (object.ObjectStorage, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}
}
