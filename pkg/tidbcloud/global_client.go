package tidbcloud

import (
	"context"
	"fmt"
	"strconv"
)

// ClusterInfo holds cluster metadata returned from the global server.
type ClusterInfo struct {
	ClusterID     string
	OrgID         uint64
	Host          string
	Port          int
	Username      string
	ProxyEndpoint string // cluster proxy HTTP endpoint for internal SQL execution
	Version       string // TiDB version string (e.g. "v8.1.1")
}

// ZeroInstanceInfo holds connection info returned directly from the zero-instance service.
// Unlike the cluster path, the password is returned in plaintext (no KMS decryption needed).
type ZeroInstanceInfo struct {
	ID       string
	Host     string
	Port     int
	Username string
	Password string
}

// GlobalClient abstracts calls to TiDB Cloud Global Server.
type GlobalClient interface {
	// GetZeroInstance returns the zero-instance connection info.
	// The returned ZeroInstanceInfo contains host, port, username, and plaintext password.
	GetZeroInstance(ctx context.Context, instanceID string) (*ZeroInstanceInfo, error)

	// GetClusterInfo retrieves cluster connection metadata by cluster ID.
	GetClusterInfo(ctx context.Context, clusterID string) (*ClusterInfo, error)

	// GetEncryptedCloudAdminPwd returns the base64-encoded encrypted cloud_admin
	// password for the given cluster.
	GetEncryptedCloudAdminPwd(ctx context.Context, clusterID string) (string, error)
}

// ErrClusterNotFound indicates a cluster was not found in the global server.
var ErrClusterNotFound = fmt.Errorf("cluster not found")

// ErrInstanceNotFound indicates a zero instance was not found.
var ErrInstanceNotFound = fmt.Errorf("zero instance not found")

// ParseClusterIDUint64 parses a cluster ID string to uint64.
func ParseClusterIDUint64(clusterID string) (uint64, error) {
	id, err := strconv.ParseUint(clusterID, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid cluster id %s: %w", clusterID, err)
	}
	return id, nil
}
