package tidbcloud

import (
	"context"
	"fmt"
	"strconv"

	serverlessv1 "github.com/tidbcloud/tidb-management-service/api/spec/global/serverless/v1"
	zerov1beta1 "github.com/tidbcloud/tidb-management-service/api/spec/tidb_cloud_open_api/zero/v1beta1"
	mgmtv1 "github.com/tidbcloud/tidb-management-service/api/spec/tidb_mgmt_service/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// grpcGlobalClient implements GlobalClient using gRPC stubs.
type grpcGlobalClient struct {
	mgmtCluster mgmtv1.ClusterServiceClient
	serverless  serverlessv1.ServerlessServiceClient
	zero        zerov1beta1.ZeroInstanceServiceClient
}

// NewGRPCGlobalClient creates a GlobalClient backed by the given gRPC service stubs.
// mgmtCluster and zero both live on the tidb-mgmt-service address.
// serverless lives on the serverless-global-service address.
func NewGRPCGlobalClient(
	mgmtCluster mgmtv1.ClusterServiceClient,
	serverless serverlessv1.ServerlessServiceClient,
	zero zerov1beta1.ZeroInstanceServiceClient,
) GlobalClient {
	return &grpcGlobalClient{
		mgmtCluster: mgmtCluster,
		serverless:  serverless,
		zero:        zero,
	}
}

func (g *grpcGlobalClient) GetZeroInstance(ctx context.Context, instanceID string) (*ZeroInstanceInfo, error) {
	resp, err := g.zero.GetZeroInstance(ctx, &zerov1beta1.GetZeroInstanceRequest{
		Id: instanceID,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, fmt.Errorf("get zero instance %s: %w", instanceID, ErrInstanceNotFound)
		}
		return nil, fmt.Errorf("get zero instance %s: %w", instanceID, err)
	}
	inst := resp.GetInstance()
	if inst == nil {
		return nil, fmt.Errorf("get zero instance %s: empty response", instanceID)
	}
	conn := inst.GetConnection()
	if conn == nil {
		return nil, fmt.Errorf("get zero instance %s: no connection info", instanceID)
	}
	return &ZeroInstanceInfo{
		ID:       inst.GetId(),
		Host:     conn.GetHost(),
		Port:     int(conn.GetPort()),
		Username: conn.GetUsername(),
		Password: conn.GetPassword(),
	}, nil
}

func (g *grpcGlobalClient) GetClusterInfo(ctx context.Context, clusterID string) (*ClusterInfo, error) {
	id, err := strconv.ParseUint(clusterID, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid cluster id %s: %w", clusterID, err)
	}
	resp, err := g.mgmtCluster.ListClusters(ctx, &mgmtv1.ListClustersRequest{
		Filter: &mgmtv1.ClusterFilter{
			ClusterIds: []uint64{id},
			View:       mgmtv1.ClusterFilter_CLUSTER_VIEW_FULL,
		},
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, fmt.Errorf("get cluster %s: %w", clusterID, ErrClusterNotFound)
		}
		return nil, fmt.Errorf("get cluster %s: %w", clusterID, err)
	}
	clusters := resp.GetClusters()
	if len(clusters) == 0 {
		return nil, fmt.Errorf("get cluster %s: %w", clusterID, ErrClusterNotFound)
	}

	sc := clusters[0].GetServerlessCluster()
	if sc == nil {
		return nil, fmt.Errorf("cluster %s is not a serverless cluster", clusterID)
	}
	regional := sc.GetCluster()
	if regional == nil {
		return nil, fmt.Errorf("cluster %s has no regional cluster info", clusterID)
	}
	pub := regional.GetEndpoints().GetPublic()
	if pub == nil || pub.GetHost() == "" {
		return nil, fmt.Errorf("cluster %s has no public endpoint", clusterID)
	}

	username := "cloud_admin"
	prefix := regional.GetUserPrefix()
	if prefix != "" {
		username = prefix + ".cloud_admin"
	}

	return &ClusterInfo{
		ClusterID:     clusterID,
		OrgID:         clusters[0].GetOrganizationId(),
		Host:          pub.GetHost(),
		Port:          int(pub.GetPort()),
		Username:      username,
		Version:       regional.GetVersion(),
		ProxyEndpoint: sc.GetInternalEndpoint(),
		UserPrefix:    prefix,
	}, nil
}

func (g *grpcGlobalClient) GetEncryptedCloudAdminPwd(ctx context.Context, clusterID string) (string, error) {
	resp, err := g.serverless.GetEncryptedCloudAdminPwd(ctx, &serverlessv1.ServerlessServiceGetEncryptedCloudAdminPwdRequest{
		ClusterId: clusterID,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return "", fmt.Errorf("get encrypted password for cluster %s: %w", clusterID, ErrClusterNotFound)
		}
		return "", fmt.Errorf("get encrypted password for cluster %s: %w", clusterID, err)
	}
	return resp.GetEncryptedPasswd(), nil
}
