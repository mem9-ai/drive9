package extent

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"

	"github.com/mem9-ai/drive9/pkg/metrics"
)

func TestOpenStorageFilePrefix(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStorage(&Credential{
		Scheme:   SchemeFile,
		Endpoint: dir,
		Prefix:   "t/abc/",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Put(context.Background(), "chunks/x", bytes.NewReader([]byte("hi"))); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "t/abc/chunks/x")); err != nil {
		t.Fatalf("expected prefixed key: %v", err)
	}
}

func TestOpenStorageRejectsDrive9Proxy(t *testing.T) {
	_, err := OpenStorage(&Credential{
		Scheme:   "drive9",
		Endpoint: "http://example/v1/extent/blocks",
	}, nil)
	if err == nil {
		t.Fatal("expected error for drive9 block proxy scheme")
	}
}

func TestOpenStorageS3StaticConstructs(t *testing.T) {
	st, err := OpenStorage(&Credential{
		Scheme:          SchemeS3,
		Endpoint:        "http://127.0.0.1:19000",
		Bucket:          "drive9-local",
		Prefix:          "t/abc/",
		AccessKeyID:     "drive9minio",
		SecretAccessKey: "drive9minio",
		ForcePathStyle:  true,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	rs, ok := st.(*refreshingStore)
	if !ok {
		t.Fatalf("type=%T, want *refreshingStore", st)
	}
	if rs.scheme != SchemeS3 {
		t.Fatalf("scheme=%s, want s3", rs.scheme)
	}
	// openInner -> createS3Storage honours force_path_style through
	// JFS_S3_VHOST_STYLE and must not leave the variable behind.
	if _, ok := os.LookupEnv(jfsS3VHostStyleEnv); ok {
		t.Fatal("OpenStorage leaked JFS_S3_VHOST_STYLE into the process")
	}
}

func TestCredentialRefreshLeadIsJitteredWithinBounds(t *testing.T) {
	const (
		base   = 2 * time.Minute
		jitter = time.Minute
	)
	tests := []struct {
		name string
		base time.Duration
		jit  time.Duration
		stub func(int64) int64
		want time.Duration
	}{
		{name: "min jitter renews at the full window", base: base, jit: jitter, stub: func(int64) int64 { return 0 }, want: base},
		{name: "max jitter renews a jitter earlier", base: base, jit: jitter, stub: func(int64) int64 { return int64(jitter) }, want: base - jitter},
		{name: "no jitter keeps the base window", base: base, jit: 0, stub: func(int64) int64 { return 0 }, want: base},
		{name: "nil source keeps the base window", base: base, jit: jitter, stub: nil, want: base},
		{name: "zero base", base: 0, jit: jitter, stub: func(int64) int64 { return 0 }, want: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := credentialRefreshLead(tc.base, tc.jit, tc.stub); got != tc.want {
				t.Fatalf("credentialRefreshLead(%v, %v) = %v, want %v", tc.base, tc.jit, got, tc.want)
			}
		})
	}
}

func TestCredentialRefreshLeadStaysInRangeWithRealRand(t *testing.T) {
	const (
		base   = credentialRefreshWindow
		jitter = credentialRefreshJitter
	)
	for i := 0; i < 200; i++ {
		got := credentialRefreshLead(base, jitter, rand.Int64N)
		if got > base || got < base-jitter {
			t.Fatalf("lead %v outside [%v, %v]", got, base-jitter, base)
		}
	}
}

func TestOpenStorageJittersRefreshLeadPerStore(t *testing.T) {
	st, err := OpenStorage(&Credential{
		Scheme:   SchemeFile,
		Endpoint: t.TempDir(),
		Prefix:   "t/abc/",
		TenantID: "abc",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	rs, ok := st.(*refreshingStore)
	if !ok {
		t.Fatalf("type=%T, want *refreshingStore", st)
	}
	if rs.refreshLead > credentialRefreshWindow || rs.refreshLead < credentialRefreshWindow-credentialRefreshJitter {
		t.Fatalf("refreshLead=%v outside [%v, %v]", rs.refreshLead, credentialRefreshWindow-credentialRefreshJitter, credentialRefreshWindow)
	}
}

func TestTenantLabelPrefersCredentialFieldThenPrefix(t *testing.T) {
	tests := []struct {
		name string
		cred Credential
		want string
	}{
		{name: "explicit tenant id", cred: Credential{TenantID: "tenant-a", Prefix: "t/other/"}, want: "tenant-a"},
		{name: "parsed from prefix", cred: Credential{Prefix: "t/tenant-b/"}, want: "tenant-b"},
		{name: "prefix without trailing slash", cred: Credential{Prefix: "t/tenant-c"}, want: "tenant-c"},
		{name: "no tenant", cred: Credential{Prefix: ""}, want: ""},
		{name: "empty tenant segment", cred: Credential{Prefix: "t/"}, want: ""},
		{name: "nested path is not a tenant", cred: Credential{Prefix: "t/a/b/"}, want: ""},
		{name: "unrelated prefix", cred: Credential{Prefix: "tenants/a/"}, want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &refreshingStore{cred: tc.cred}
			if got := s.tenantLabel(); got != tc.want {
				t.Fatalf("tenantLabel() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestVHostStyleValueFollowsForcePathStyle(t *testing.T) {
	if got := vhostStyleValue(true); got != "0" {
		t.Fatalf("vhostStyleValue(true) = %q, want \"0\" (JuiceFS path style)", got)
	}
	if got := vhostStyleValue(false); got != "1" {
		t.Fatalf("vhostStyleValue(false) = %q, want \"1\" (JuiceFS virtual-host style)", got)
	}
}

func TestWithS3StyleEnvVisibleDuringCreateAndRestoredAfter(t *testing.T) {
	// The env var is the only path-style knob JuiceFS reads, so the value must
	// be observable while the storage is constructed.
	t.Setenv(jfsS3VHostStyleEnv, "keep-me")
	seen := ""
	if _, err := withS3StyleEnv(false, func() (object.ObjectStorage, error) {
		seen = os.Getenv(jfsS3VHostStyleEnv)
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if seen != "1" {
		t.Fatalf("JFS_S3_VHOST_STYLE during create = %q, want \"1\"", seen)
	}
	if got := os.Getenv(jfsS3VHostStyleEnv); got != "keep-me" {
		t.Fatalf("JFS_S3_VHOST_STYLE after create = %q, want the previous value", got)
	}

	// And it must be removed again when it was not present before.
	t.Setenv(jfsS3VHostStyleEnv, "0")
	if err := os.Unsetenv(jfsS3VHostStyleEnv); err != nil {
		t.Fatal(err)
	}
	if _, err := withS3StyleEnv(true, func() (object.ObjectStorage, error) {
		if got := os.Getenv(jfsS3VHostStyleEnv); got != "0" {
			t.Fatalf("JFS_S3_VHOST_STYLE during create = %q, want \"0\"", got)
		}
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok := os.LookupEnv(jfsS3VHostStyleEnv); ok {
		t.Fatal("JFS_S3_VHOST_STYLE must stay unset after create")
	}
}

func TestCreateS3StorageHonoursForcePathStyle(t *testing.T) {
	if _, ok := os.LookupEnv(jfsS3VHostStyleEnv); ok {
		t.Fatal("test precondition: JFS_S3_VHOST_STYLE must be unset")
	}
	st, err := createS3Storage("http://127.0.0.1:19000/drive9-local", "drive9minio", "drive9minio", "", true)
	if err != nil {
		t.Fatalf("path-style storage: %v", err)
	}
	if st == nil {
		t.Fatal("path-style storage is nil")
	}
	if got := os.Getenv(jfsS3VHostStyleEnv); got != "" {
		t.Fatalf("JFS_S3_VHOST_STYLE after construction = %q, want unset", got)
	}
	vhost, err := createS3Storage("http://127.0.0.1:19000/drive9-local", "drive9minio", "drive9minio", "", false)
	if err != nil {
		t.Fatalf("virtual-host storage: %v", err)
	}
	if vhost == nil {
		t.Fatal("virtual-host storage is nil")
	}
}

type failingCredentialSource struct{ err error }

func (f failingCredentialSource) GetDataCredential(context.Context) (*Credential, error) {
	return nil, f.err
}

func TestRefreshFailureKeepsServingAndRecordsMetric(t *testing.T) {
	dir := t.TempDir()
	cred := &Credential{
		Scheme:    SchemeFile,
		Endpoint:  dir,
		Prefix:    "t/tenant-refresh/",
		TenantID:  "tenant-refresh",
		ExpiresAt: time.Now().Add(30 * time.Second).UTC().Format(time.RFC3339),
	}
	src := failingCredentialSource{err: errors.New("server said 403")}
	st, err := OpenStorage(cred, src)
	if err != nil {
		t.Fatal(err)
	}
	// The credential is inside the refresh window, so this Put attempts a
	// refresh, fails, and must keep using the old inner store.
	if err := st.Put(context.Background(), "chunks/x", bytes.NewReader([]byte("hi"))); err != nil {
		t.Fatalf("put after failed refresh: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "t/tenant-refresh/chunks/x")); err != nil {
		t.Fatalf("old inner store was not kept: %v", err)
	}

	rec := httptest.NewRecorder()
	metrics.WritePrometheus(rec)
	want := `drive9_service_operations_total{component="extent_storage",operation="refresh_credential",result="error"}`
	if !strings.Contains(rec.Body.String(), want) {
		t.Fatalf("refresh failure metric missing:\n%s", rec.Body.String())
	}
}

func TestRefreshSuccessSwapsInnerStoreAndRecordsMetric(t *testing.T) {
	oldDir := t.TempDir()
	newDir := t.TempDir()
	cred := &Credential{
		Scheme:    SchemeFile,
		Endpoint:  oldDir,
		Prefix:    "t/tenant-swap/",
		TenantID:  "tenant-swap",
		ExpiresAt: time.Now().Add(30 * time.Second).UTC().Format(time.RFC3339),
	}
	src := staticCredentialSource{cred: &Credential{
		Scheme:    SchemeFile,
		Endpoint:  newDir,
		Prefix:    "t/tenant-swap/",
		TenantID:  "tenant-swap",
		ExpiresAt: time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
	}}
	st, err := OpenStorage(cred, src)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Put(context.Background(), "chunks/y", bytes.NewReader([]byte("hi"))); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(newDir, "t/tenant-swap/chunks/y")); err != nil {
		t.Fatalf("refreshed credential was not used: %v", err)
	}
	if _, err := os.Stat(filepath.Join(oldDir, "t/tenant-swap/chunks/y")); !os.IsNotExist(err) {
		t.Fatalf("write went to the stale store: %v", err)
	}

	rec := httptest.NewRecorder()
	metrics.WritePrometheus(rec)
	want := `drive9_service_operations_total{component="extent_storage",operation="refresh_credential",result="ok"}`
	if !strings.Contains(rec.Body.String(), want) {
		t.Fatalf("refresh success metric missing:\n%s", rec.Body.String())
	}
}

type staticCredentialSource struct{ cred *Credential }

func (s staticCredentialSource) GetDataCredential(context.Context) (*Credential, error) {
	return s.cred, nil
}
