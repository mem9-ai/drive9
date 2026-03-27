// Package tenant implements the control plane tenant store for multi-tenant auth.
// Each tenant has a unique API key mapped 1:1 to a dedicated db9/TiDB cluster.
package tenant

import (
	"strconv"
	"time"
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
	S3Bucket         string
	S3KeyPrefix      string
	ClusterID        string
	ProvisionerType  string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// DSN builds a MySQL DSN from the tenant's (decrypted) connection info.
func DSN(host string, port int, user, password, dbName string) string {
	return user + ":" + password + "@tcp(" + host + ":" + strconv.Itoa(port) + ")/" + dbName + "?parseTime=true"
}
