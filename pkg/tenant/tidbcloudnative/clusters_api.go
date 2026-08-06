package tidbcloudnative

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/mem9-ai/drive9/pkg/tenant"
)

// EnvTiDBCloudClustersBackend selects the TiDB Cloud control-plane implementation.
//
//	"" or "http" — real TiDB Cloud OpenAPI (default)
//	"local"      — LocalClustersAPI (Docker/Podman TiDB instances)
const EnvTiDBCloudClustersBackend = "DRIVE9_TIDBCLOUD_CLUSTERS_BACKEND"

// ClustersAPI is the TiDB Cloud control-plane surface used by Provisioner.
// HTTPClustersAPI talks to the real OpenAPI; LocalClustersAPI simulates it with
// one local TiDB instance per Cloud cluster so drive9 warm-pool logic
// can run unchanged.
type ClustersAPI interface {
	CreateCluster(ctx context.Context, publicKey, privateKey string, body []byte) (*clusterInfo, error)
	// BatchCreateClusters may return a non-empty subset together with a non-nil
	// error (partial success). Callers must process returned clusters first.
	BatchCreateClusters(ctx context.Context, publicKey, privateKey string, body []byte) ([]clusterInfo, error)
	GetCluster(ctx context.Context, publicKey, privateKey, clusterID string) (*clusterInfo, error)
	ListClusters(ctx context.Context, publicKey, privateKey string, query url.Values) (clusters []clusterInfo, nextPageToken string, err error)
	PatchCluster(ctx context.Context, publicKey, privateKey, clusterID string, body []byte) error
	DeleteCluster(ctx context.Context, publicKey, privateKey, clusterID string) error
	CreateBranch(ctx context.Context, publicKey, privateKey, clusterID string, body []byte) (*branchInfo, error)
	GetBranch(ctx context.Context, publicKey, privateKey, clusterID, branchID string) (*branchInfo, error)
	DeleteBranch(ctx context.Context, publicKey, privateKey, clusterID, branchID string) error
	ResolveAPIKey(ctx context.Context, publicKey, privateKey string) (*tenant.TiDBCloudAPIKeyIdentity, error)
}

// ClustersBackendFromEnv returns the configured backend name (http|local).
func ClustersBackendFromEnv() string {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(EnvTiDBCloudClustersBackend)))
	switch v {
	case "", "http", "cloud", "openapi":
		return "http"
	case "local", "docker":
		return "local"
	default:
		return v
	}
}

// IsLocalClustersBackend reports whether the process is configured for the
// local Docker control-plane backend.
func IsLocalClustersBackend() bool {
	return ClustersBackendFromEnv() == "local"
}

// DBTLSModeForBackend returns the mysql driver TLS mode for tenant DB
// connections. Local Docker TiDB has no TLS.
func DBTLSModeForBackend() string {
	if IsLocalClustersBackend() {
		return ""
	}
	v := strings.TrimSpace(strings.ToLower(os.Getenv(EnvTiDBCloudNativeUsePrivateEndpoint)))
	if v == "1" || v == "true" || v == "yes" {
		return "skip-verify"
	}
	return "true"
}

type apiStatusError struct {
	op   string
	code int
	body string
}

func (e *apiStatusError) Error() string {
	return statusErrorMessage(e.op, e.code, e.body)
}

func (e *apiStatusError) Unwrap() error {
	return statusError(tenant.TiDBCloudAPIServiceCluster, e.op, e.code, e.body)
}

func newAPIStatusError(op string, code int, body string) error {
	return &apiStatusError{op: op, code: code, body: body}
}

func mapQuotaAPIError(op string, err error) error {
	var se *apiStatusError
	if err == nil {
		return nil
	}
	if !asAPIStatus(err, &se) {
		return err
	}
	return quotaStatusError(op, se.code, se.body)
}

func asAPIStatus(err error, target **apiStatusError) bool {
	if err == nil {
		return false
	}
	if se, ok := err.(*apiStatusError); ok {
		*target = se
		return true
	}
	return false
}

func requireCreds(publicKey, privateKey string) error {
	if strings.TrimSpace(publicKey) == "" || strings.TrimSpace(privateKey) == "" {
		return tenant.ErrCredentialsRequired
	}
	return nil
}

func httpStatusOK(code int) bool {
	return code >= http.StatusOK && code < http.StatusMultipleChoices
}

func unexpectedBackend(name string) error {
	return fmt.Errorf("unsupported %s=%q (want http or local)", EnvTiDBCloudClustersBackend, name)
}
