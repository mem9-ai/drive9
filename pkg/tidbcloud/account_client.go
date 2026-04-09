package tidbcloud

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	accountpb "github.com/tidbcloud/account/idl/pbgen/proto/account"
	"go.uber.org/zap"

	"github.com/mem9-ai/dat9/pkg/logger"
)

// AccountClient abstracts calls to TiDB Cloud Account service for authorization.
type AccountClient interface {
	// Authorize verifies the request's auth context has permission to operate
	// the given cluster. Returns nil on success.
	// Returns ErrAuthMissing if no credentials are found in the request.
	// Returns ErrAuthForbidden if credentials are valid but lack permission.
	Authorize(ctx context.Context, r *http.Request, clusterID string) error
}

// ErrAuthMissing indicates the request lacks valid authentication credentials.
var ErrAuthMissing = fmt.Errorf("authentication required")

// ErrAuthForbidden indicates the credentials are valid but lack permission.
var ErrAuthForbidden = fmt.Errorf("forbidden")

// identityInfo is the resolved identity from the account service.
type identityInfo struct {
	userID uint64
	orgID  uint64
}

// grpcAccountClient authenticates HTTP requests by calling the TiDB Cloud
// Account service directly. Supports two auth methods:
//
//  1. OAuth token: Authorization: Bearer <token> → GetUserByToken
//  2. API key: X-Auth-Method: digest/basic + X-Auth-Content → GetApiKeyByAccessKey
type grpcAccountClient struct {
	account accountpb.AccountAPIServiceClient
}

// NewGRPCAccountClient creates an AccountClient backed by the account-provider
// gRPC service.
func NewGRPCAccountClient(account accountpb.AccountAPIServiceClient) AccountClient {
	return &grpcAccountClient{account: account}
}

func (c *grpcAccountClient) Authorize(ctx context.Context, r *http.Request, _ string) error {
	identity, err := c.authenticate(ctx, r)
	if err != nil {
		return err
	}

	// Verify the user belongs to the org.
	resp, err := c.account.VerifyUserOrgAndProjects(ctx, &accountpb.VerifyUserOrgAndProjectsReq{
		UserId: identity.userID,
		OrgId:  identity.orgID,
	})
	if err != nil {
		return fmt.Errorf("verify user org: %w", err)
	}
	if !resp.GetResult() {
		return fmt.Errorf("%w: user %d not authorized for org %d", ErrAuthForbidden, identity.userID, identity.orgID)
	}
	return nil
}

// authenticate extracts credentials from the request and resolves identity via
// the account service. Logic mirrors authsdk.AuthnIdentity.
func (c *grpcAccountClient) authenticate(ctx context.Context, r *http.Request) (*identityInfo, error) {
	// 1. OAuth token: Authorization: Bearer <token>
	if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
		token := strings.TrimPrefix(auth, "Bearer ")
		return c.authByUserToken(ctx, token)
	}

	method := strings.ToLower(strings.TrimSpace(r.Header.Get("X-Auth-Method")))

	// 2. API key: X-Auth-Method: digest/basic + X-Auth-Content: {"public_key":"<ak>"}
	if method == "digest" || method == "basic" {
		content := r.Header.Get("X-Auth-Content")
		if content == "" {
			return nil, fmt.Errorf("%w: X-Auth-Content is empty", ErrAuthMissing)
		}
		var payload struct {
			PublicKey string `json:"public_key"`
		}
		if err := json.Unmarshal([]byte(content), &payload); err != nil || payload.PublicKey == "" {
			return nil, fmt.Errorf("%w: invalid X-Auth-Content", ErrAuthMissing)
		}
		return c.authByAPIKey(ctx, payload.PublicKey)
	}

	return nil, fmt.Errorf("%w: no valid auth credentials found", ErrAuthMissing)
}

// authByUserToken validates an OAuth token via GetUserByToken.
// Mirrors authsdk.AuthnUserToken.
func (c *grpcAccountClient) authByUserToken(ctx context.Context, token string) (*identityInfo, error) {
	resp, err := c.account.GetUserByToken(ctx, &accountpb.GetUserByTokenRequest{
		Token: token,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrAuthMissing, err)
	}
	if resp.GetBaseResp().GetErrCode() != 0 {
		return nil, fmt.Errorf("%w: %s", ErrAuthMissing, resp.GetBaseResp().GetErrMsg())
	}
	user := resp.GetUser()
	if user == nil || user.GetId() == 0 {
		return nil, fmt.Errorf("%w: invalid token", ErrAuthMissing)
	}
	if user.GetStatus() != accountpb.UserStatus_USER_STATUS_ACTIVE {
		return nil, fmt.Errorf("%w: user status not active", ErrAuthForbidden)
	}
	// For OAuth tokens the org is not embedded in the token.
	// Resolve from user's org list.
	orgs, err := c.account.ListOrgsByUser(ctx, &accountpb.ListOrgsByUserRequest{
		UserId: user.GetId(),
	})
	if err != nil {
		return nil, fmt.Errorf("list orgs for user %d: %w", user.GetId(), err)
	}
	orgList := orgs.GetOrgs()
	if len(orgList) == 0 {
		return nil, fmt.Errorf("%w: user %d has no org", ErrAuthForbidden, user.GetId())
	}
	return &identityInfo{userID: user.GetId(), orgID: orgList[0].GetId()}, nil
}

// authByAPIKey validates an API key (public_key) via GetApiKeyByAccessKey.
// Mirrors authsdk.AuthnApiKeyAK + GetOrgIdFromApiKeyRsp.
func (c *grpcAccountClient) authByAPIKey(ctx context.Context, accessKey string) (*identityInfo, error) {
	resp, err := c.account.GetApiKeyByAccessKey(ctx, &accountpb.GetApiKeyByAccessKeyReq{
		AccessKey: accessKey,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrAuthMissing, err)
	}
	if resp.GetBaseResp().GetErrCode() != 0 {
		return nil, fmt.Errorf("%w: %s", ErrAuthMissing, resp.GetBaseResp().GetErrMsg())
	}

	// Extract org_id from resource_infos (scope_type=ORG).
	var orgID uint64
	for _, ri := range resp.GetResourceInfos() {
		if ri.GetScopeType() == "ORG" {
			orgID = ri.GetResourceId()
			break
		}
	}
	if orgID == 0 {
		logger.L().Warn("API key has no ORG scope in resource_infos", zap.String("access_key", accessKey))
		return nil, fmt.Errorf("%w: API key has no org scope", ErrAuthForbidden)
	}
	apiKey := resp.GetApiKey()
	if apiKey == nil || apiKey.GetId() == 0 {
		return nil, fmt.Errorf("%w: invalid API key", ErrAuthMissing)
	}
	return &identityInfo{userID: apiKey.GetId(), orgID: orgID}, nil
}
