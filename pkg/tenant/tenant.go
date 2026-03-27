// Package tenant implements the control plane tenant store for multi-tenant auth.
// Each tenant has a unique API key mapped 1:1 to a dedicated db9/TiDB cluster.
package tenant

import (
	"crypto/tls"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
)

type Status string

const (
	StatusProvisioning Status = "provisioning"
	StatusActive       Status = "active"
	StatusSuspended    Status = "suspended"
	StatusDeleted      Status = "deleted"
)

// Tenant represents a row in the control plane tenants table.
type Tenant struct {
	ID               string
	APIKeyPrefix     string // first 8 chars of the raw API key (for logs/debug)
	APIKeyHash       string // SHA-256 hex of the full API key
	Status           Status
	DBHost           string
	DBPort           int
	DBUser           string
	DBPasswordEnc    []byte // AES-GCM encrypted
	DBName           string
	DBTLS            string // TLS mode: "", "true", "skip-verify", "custom"
	DBParams         string // extra DSN params (e.g. "timeout=5s&readTimeout=10s")
	S3Bucket         string
	S3KeyPrefix      string
	ClusterID        string
	ProvisionerType  string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// DSN builds a MySQL DSN using mysql.Config for safe escaping of special characters.
// tlsMode: "" = no TLS, "true" = system CA, "skip-verify" = skip verification.
func DSN(host string, port int, user, password, dbName, tlsMode string) string {
	cfg := mysql.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%d", host, port)
	cfg.User = user
	cfg.Passwd = password
	cfg.DBName = dbName
	cfg.ParseTime = true

	if tlsMode != "" {
		cfg.TLSConfig = tlsMode
	}

	return cfg.FormatDSN()
}

// TLSConfigName registers a TLS config for TiDB Cloud connections and returns its name.
// Call this once at startup when using cloud endpoints that require TLS.
func TLSConfigName() string {
	const name = "tidb-cloud"
	mysql.RegisterTLSConfig(name, &tls.Config{
		MinVersion: tls.VersionTLS12,
	})
	return name
}
