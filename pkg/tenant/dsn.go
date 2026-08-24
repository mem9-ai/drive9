package tenant

import (
	"fmt"
	"net"
	"strings"
)

// MySQLTLSParam returns the go-sql-driver `tls` DSN value for a tenant DB
// connection. Loopback hosts never request TLS: local unistore TiDB (and most
// testcontainers mappings) do not speak it. Public TiDB Cloud Native endpoints
// still get skip-verify when DBTLS is unset.
func MySQLTLSParam(tlsEnabled bool, provider, host string) string {
	if tlsEnabled {
		return "true"
	}
	if UsesTiDBCloudNativeCredentials(provider) && !IsLoopbackDBHost(host) {
		return "skip-verify"
	}
	return ""
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
