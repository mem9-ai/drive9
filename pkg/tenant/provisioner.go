package tenant

import "context"

// ClusterInfo holds connection details for a newly provisioned cluster.
type ClusterInfo struct {
	ClusterID string
	Host      string
	Port      int
	Username  string
	Password  string // plaintext — encrypt before storing
	DBName    string
}

// Provisioner abstracts cluster acquisition and schema initialization.
// Implementations include TiDB Cloud Starter and db9.
type Provisioner interface {
	// Provision acquires a new cluster and returns its connection info.
	Provision(ctx context.Context) (*ClusterInfo, error)

	// InitSchema runs dat9 schema migrations on the provisioned cluster.
	// The dsn is built from ClusterInfo by the caller.
	InitSchema(ctx context.Context, dsn string) error

	// ProviderType returns the provisioner identifier (e.g. "tidb_starter", "db9").
	ProviderType() string
}
