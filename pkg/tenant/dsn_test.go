package tenant

import (
	"strings"
	"testing"
)

func TestMySQLTLSParam(t *testing.T) {
	t.Parallel()
	cases := []struct {
		tls      bool
		provider string
		host     string
		want     string
	}{
		{true, ProviderLocal, "127.0.0.1", "true"},
		{true, ProviderTiDBCloudNative, "127.0.0.1", "true"},
		{false, ProviderTiDBCloudNative, "gateway.us-east-1.prod.aws.tidbcloud.com", "skip-verify"},
		{false, ProviderTiDBCloudNative, "127.0.0.1", ""},
		{false, ProviderTiDBCloudNative, "localhost", ""},
		{false, ProviderTiDBCloudNative, "::1", ""},
		{false, ProviderDB9, "example.com", ""},
		{false, ProviderLocal, "127.0.0.1", ""},
	}
	for _, tc := range cases {
		got := MySQLTLSParam(tc.tls, tc.provider, tc.host)
		if got != tc.want {
			t.Errorf("MySQLTLSParam(tls=%v, provider=%s, host=%q) = %q, want %q",
				tc.tls, tc.provider, tc.host, got, tc.want)
		}
	}
}

func TestFormatTenantMySQLDSN(t *testing.T) {
	t.Parallel()
	dsn := FormatTenantMySQLDSN("u", "p", "127.0.0.1", 4000, "drive9_t", false, ProviderTiDBCloudNative)
	if strings.Contains(dsn, "tls=") {
		t.Fatalf("loopback native DSN unexpectedly has tls: %s", dsn)
	}
	dsn = FormatTenantMySQLDSN("u", "p", "db.example.com", 4000, "drive9_t", false, ProviderTiDBCloudNative)
	if !strings.Contains(dsn, "tls=skip-verify") {
		t.Fatalf("public native DSN = %q, want tls=skip-verify", dsn)
	}
	dsn = FormatTenantMySQLDSN("u", "p", "::1", 4000, "drive9_t", false, ProviderLocal)
	if !strings.Contains(dsn, "tcp([::1]:4000)") {
		t.Fatalf("ipv6 DSN = %q, want bracketed host", dsn)
	}
}
