package tenant

import (
	"fmt"
	"net"
	"os"
	"strings"
)

// EnvDBTLSSkipVerify is the explicit opt-out from verified tenant DB TLS.
// It exists only for deployments whose database endpoint presents a
// certificate the client cannot validate (typically a private-link gateway
// whose certificate is not issued for the address the client dials). Without
// it, every non-loopback tenant connection is verified.
const EnvDBTLSSkipVerify = "DRIVE9_DB_TLS_SKIP_VERIFY"

// DBTLSSkipVerifyRequested reports whether the deployment explicitly opted out
// of tenant DB certificate verification.
func DBTLSSkipVerifyRequested() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(EnvDBTLSSkipVerify))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// MySQLTLSParam returns the go-sql-driver `tls` DSN value for a tenant DB
// connection. Loopback hosts never request TLS: local unistore TiDB (and most
// testcontainers mappings) do not speak it.
//
// Every non-loopback host gets TLS regardless of the provider or of the stored
// DBTLS flag: the DSN carries the tenant's database password, so an unverified
// or absent channel would expose it to anyone on the path.
// DRIVE9_DB_TLS_SKIP_VERIFY only trades certificate verification for
// encryption — it never removes encryption — so a deployment that opts out
// still gets a TLS connection, not a plaintext one.
func MySQLTLSParam(tlsEnabled bool, provider, host string) string {
	skipVerify := DBTLSSkipVerifyRequested()
	if tlsEnabled {
		if skipVerify {
			return "skip-verify"
		}
		return "true"
	}
	if IsLoopbackDBHost(host) {
		return ""
	}
	if skipVerify {
		return "skip-verify"
	}
	return "true"
}

// IsLoopbackDBHost reports whether host is a loopback name or address.
func IsLoopbackDBHost(host string) bool {
	h := strings.TrimSpace(host)
	h = strings.Trim(h, "[]")
	if h == "" {
		return false
	}
	if strings.EqualFold(h, "localhost") || strings.EqualFold(h, "localhost.localdomain") {
		return true
	}
	ip := net.ParseIP(h)
	return ip != nil && ip.IsLoopback()
}

// FormatTenantMySQLDSN builds a tenant user-DB DSN.
func FormatTenantMySQLDSN(user, password, host string, port int, dbName string, tlsEnabled bool, provider string) string {
	query := "parseTime=true"
	if mode := MySQLTLSParam(tlsEnabled, provider, host); mode != "" {
		query += "&tls=" + mode
	}
	return fmt.Sprintf("%s:%s@tcp(%s)/%s?%s", user, password, mysqlTCPAddr(host, port), dbName, query)
}

func mysqlTCPAddr(host string, port int) string {
	host = strings.TrimSpace(host)
	host = strings.Trim(host, "[]")
	if strings.Contains(host, ":") {
		return fmt.Sprintf("[%s]:%d", host, port)
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return fmt.Sprintf("%s:%d", host, port)
}
