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
		{false, ProviderTiDBCloudNative, "gateway.us-east-1.prod.aws.tidbcloud.com", "true"},
		{false, ProviderTiDBCloudNative, "127.0.0.1", ""},
		// A non-native, non-loopback host must never fall back to plaintext:
		// DBTLS=false plus the skip-verify opt-out still means encrypted.
		{false, ProviderDB9, "db9.example.com", "true"},
		{false, ProviderTiDBZero, "zero.example.com", "true"},
		{true, ProviderDB9, "db9.example.com", "true"},
		{false, ProviderTiDBCloudNative, "localhost", ""},
		{false, ProviderTiDBCloudNative, "::1", ""},
		// Previously this returned "" (plaintext) for a remote db9 host; a
		// remote tenant DB always carries a password in the DSN, so TLS is not
		// optional there.
		{false, ProviderDB9, "example.com", "true"},
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
	if !strings.Contains(dsn, "tls=true") {
		t.Fatalf("public native DSN = %q, want verified tls=true", dsn)
	}
	dsn = FormatTenantMySQLDSN("u", "p", "::1", 4000, "drive9_t", false, ProviderLocal)
	if !strings.Contains(dsn, "tcp([::1]:4000)") {
		t.Fatalf("ipv6 DSN = %q, want bracketed host", dsn)
	}
}

// DRIVE9_DB_TLS_SKIP_VERIFY trades verification for encryption, never the other
// way around: a non-loopback host keeps TLS even when DBTLS is false.
func TestMySQLTLSParamSkipVerifyNeverDowngradesToPlaintext(t *testing.T) {
	t.Setenv(EnvDBTLSSkipVerify, "1")
	for _, provider := range []string{ProviderTiDBCloudNative, ProviderDB9, ProviderTiDBZero, ProviderTiDBCloudStarterLegacy} {
		if got := MySQLTLSParam(false, provider, "db.example.com"); got != "skip-verify" {
			t.Errorf("MySQLTLSParam(false, %s, remote) = %q, want skip-verify", provider, got)
		}
		if got := MySQLTLSParam(true, provider, "db.example.com"); got != "skip-verify" {
			t.Errorf("MySQLTLSParam(true, %s, remote) = %q, want skip-verify", provider, got)
		}
	}
	// Loopback still speaks plaintext: local TiDB has no TLS listener.
	if got := MySQLTLSParam(false, ProviderDB9, "127.0.0.1"); got != "" {
		t.Errorf("loopback DSN tls = %q, want no TLS param", got)
	}
}
