package tidbcloud

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"go.uber.org/zap"

	accountpb "github.com/tidbcloud/account/idl/pbgen/proto/account"

	"github.com/mem9-ai/dat9/pkg/logger"
)

// AccountClient abstracts calls to TiDB Cloud Account service for authentication.
type AccountClient interface {
	// Authorize authenticates the request and returns the caller's org ID.
	// It does NOT perform cluster-level authorization; callers (e.g.
	// tidbcloudnative.Provisioner.Authorize) must verify that the returned
	// orgID matches the target cluster's org via GetClusterInfo.
	// Returns ErrAuthMissing if no credentials are found in the request.
	// Returns ErrAuthForbidden if credentials are valid but lack permission.
	Authorize(ctx context.Context, r *http.Request, clusterID string) (orgID uint64, err error)
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

func (c *grpcAccountClient) Authorize(ctx context.Context, r *http.Request, _ string) (uint64, error) {
	identity, err := c.authenticate(ctx, r)
	if err != nil {
		return 0, err
	}
	return identity.orgID, nil
}

// authenticate extracts credentials from the request and resolves identity via
// the account service.
//
// In production, Kong authenticates the request first and forwards identity via
// headers:
//   - Bearer token: X-Auth-Method=bear, X-Auth-Raw=Bearer <token>
//   - API key:      X-Auth-Method=digest/basic, X-Auth-Content={"public_key":"<ak>"}
//
// As a fallback (e.g. local dev without Kong), the raw Authorization header is
// also accepted.
func (c *grpcAccountClient) authenticate(ctx context.Context, r *http.Request) (*identityInfo, error) {
	method := strings.ToLower(strings.TrimSpace(r.Header.Get("X-Auth-Method")))

	// 1. Bearer token via Kong: X-Auth-Method=bear, X-Auth-Raw=Bearer <token>
	if method == "bear" {
		raw := r.Header.Get("X-Auth-Raw")
		if strings.TrimSpace(raw) == "" {
			return nil, fmt.Errorf("%w: X-Auth-Raw is empty", ErrAuthMissing)
		}
		token := strings.TrimSpace(raw)
		if strings.HasPrefix(strings.ToLower(raw), "bearer ") {
			token = strings.TrimSpace(raw[len("Bearer "):])
		}
		if token == "" {
			return nil, fmt.Errorf("%w: X-Auth-Raw bearer token is empty", ErrAuthMissing)
		}
		return c.authByUserToken(ctx, token)
	}

	// 2. API key via Kong: X-Auth-Method=digest/basic, X-Auth-Content={"public_key":"<ak>"}
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

	// 3. Fallback: direct Authorization header (local dev without Kong).
	if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
		token := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
		if token == "" {
			return nil, fmt.Errorf("%w: bearer token is empty", ErrAuthMissing)
		}
		return c.authByUserToken(ctx, token)
	}

	// Log actual auth headers to diagnose unexpected combinations.
	logger.L().Warn("authenticate: unrecognized auth headers",
		zap.String("x-auth-method", method),
		zap.Bool("has-x-auth-raw", r.Header.Get("X-Auth-Raw") != ""),
		zap.Bool("has-x-auth-content", r.Header.Get("X-Auth-Content") != ""),
		zap.Bool("has-authorization", r.Header.Get("Authorization") != ""),
	)
	return nil, fmt.Errorf("%w: no valid auth credentials found", ErrAuthMissing)
}

// authByUserToken validates an OAuth token via GetUserByToken.
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
		logger.L().Warn("API key has no ORG scope in resource_infos")
		return nil, fmt.Errorf("%w: API key has no org scope", ErrAuthForbidden)
	}
	apiKey := resp.GetApiKey()
	if apiKey == nil || apiKey.GetId() == 0 {
		return nil, fmt.Errorf("%w: invalid API key", ErrAuthMissing)
	}
	return &identityInfo{userID: apiKey.GetId(), orgID: orgID}, nil
}
