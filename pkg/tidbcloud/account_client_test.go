package tidbcloud

import (
	"context"
	"errors"
	"net/http"
	"testing"

	accountpb "github.com/tidbcloud/account/idl/pbgen/proto/account"
	"google.golang.org/grpc"
)

// mockAccountClient is a minimal mock for the methods used by grpcAccountClient.
type mockAccountClient struct {
	accountpb.AccountAPIServiceClient // embed to satisfy the full interface

	getUserByTokenFn           func(ctx context.Context, in *accountpb.GetUserByTokenRequest, opts ...grpc.CallOption) (*accountpb.GetUserByTokenResponse, error)
	listOrgsByUserFn           func(ctx context.Context, in *accountpb.ListOrgsByUserRequest, opts ...grpc.CallOption) (*accountpb.ListOrgsByUserResponse, error)
	getApiKeyByAccessKeyFn     func(ctx context.Context, in *accountpb.GetApiKeyByAccessKeyReq, opts ...grpc.CallOption) (*accountpb.GetApiKeyByAccessKeyRsp, error)
	verifyUserOrgAndProjectsFn func(ctx context.Context, in *accountpb.VerifyUserOrgAndProjectsReq, opts ...grpc.CallOption) (*accountpb.VerifyUserOrgAndProjectsResp, error)
}

func (m *mockAccountClient) GetUserByToken(ctx context.Context, in *accountpb.GetUserByTokenRequest, opts ...grpc.CallOption) (*accountpb.GetUserByTokenResponse, error) {
	return m.getUserByTokenFn(ctx, in, opts...)
}

func (m *mockAccountClient) ListOrgsByUser(ctx context.Context, in *accountpb.ListOrgsByUserRequest, opts ...grpc.CallOption) (*accountpb.ListOrgsByUserResponse, error) {
	return m.listOrgsByUserFn(ctx, in, opts...)
}

func (m *mockAccountClient) GetApiKeyByAccessKey(ctx context.Context, in *accountpb.GetApiKeyByAccessKeyReq, opts ...grpc.CallOption) (*accountpb.GetApiKeyByAccessKeyRsp, error) {
	return m.getApiKeyByAccessKeyFn(ctx, in, opts...)
}

func (m *mockAccountClient) VerifyUserOrgAndProjects(ctx context.Context, in *accountpb.VerifyUserOrgAndProjectsReq, opts ...grpc.CallOption) (*accountpb.VerifyUserOrgAndProjectsResp, error) {
	return m.verifyUserOrgAndProjectsFn(ctx, in, opts...)
}

func TestAuthorize_OAuthToken_Success(t *testing.T) {
	mock := &mockAccountClient{
		getUserByTokenFn: func(_ context.Context, in *accountpb.GetUserByTokenRequest, _ ...grpc.CallOption) (*accountpb.GetUserByTokenResponse, error) {
			if in.Token != "valid-token" {
				t.Fatalf("unexpected token: %s", in.Token)
			}
			return &accountpb.GetUserByTokenResponse{
				User: &accountpb.User{Id: 100, Status: accountpb.UserStatus_USER_STATUS_ACTIVE},
			}, nil
		},
		listOrgsByUserFn: func(_ context.Context, in *accountpb.ListOrgsByUserRequest, _ ...grpc.CallOption) (*accountpb.ListOrgsByUserResponse, error) {
			if in.UserId != 100 {
				t.Fatalf("unexpected user ID: %d", in.UserId)
			}
			return &accountpb.ListOrgsByUserResponse{
				Orgs: []*accountpb.Org{{Id: 200}},
			}, nil
		},
		verifyUserOrgAndProjectsFn: func(_ context.Context, in *accountpb.VerifyUserOrgAndProjectsReq, _ ...grpc.CallOption) (*accountpb.VerifyUserOrgAndProjectsResp, error) {
			if in.UserId != 100 || in.OrgId != 200 {
				t.Fatalf("unexpected verify params: user=%d org=%d", in.UserId, in.OrgId)
			}
			return &accountpb.VerifyUserOrgAndProjectsResp{Result: true}, nil
		},
	}

	client := NewGRPCAccountClient(mock)
	r, _ := http.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer valid-token")

	if err := client.Authorize(context.Background(), r, "cluster-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAuthorize_APIKey_Success(t *testing.T) {
	mock := &mockAccountClient{
		getApiKeyByAccessKeyFn: func(_ context.Context, in *accountpb.GetApiKeyByAccessKeyReq, _ ...grpc.CallOption) (*accountpb.GetApiKeyByAccessKeyRsp, error) {
			if in.AccessKey != "my-access-key" {
				t.Fatalf("unexpected access key: %s", in.AccessKey)
			}
			return &accountpb.GetApiKeyByAccessKeyRsp{
				ApiKey: &accountpb.ApiKey{Id: 300},
				ResourceInfos: []*accountpb.ApiKeyResourceInfo{
					{ScopeType: "ORG", ResourceId: 400},
				},
			}, nil
		},
		verifyUserOrgAndProjectsFn: func(_ context.Context, in *accountpb.VerifyUserOrgAndProjectsReq, _ ...grpc.CallOption) (*accountpb.VerifyUserOrgAndProjectsResp, error) {
			if in.UserId != 300 || in.OrgId != 400 {
				t.Fatalf("unexpected verify params: user=%d org=%d", in.UserId, in.OrgId)
			}
			return &accountpb.VerifyUserOrgAndProjectsResp{Result: true}, nil
		},
	}

	client := NewGRPCAccountClient(mock)
	r, _ := http.NewRequest("GET", "/", nil)
	r.Header.Set("X-Auth-Method", "digest")
	r.Header.Set("X-Auth-Content", `{"public_key":"my-access-key"}`)

	if err := client.Authorize(context.Background(), r, "cluster-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAuthorize_NoCredentials(t *testing.T) {
	client := NewGRPCAccountClient(&mockAccountClient{})
	r, _ := http.NewRequest("GET", "/", nil)

	err := client.Authorize(context.Background(), r, "cluster-1")
	if err == nil {
		t.Fatal("expected error for missing credentials")
	}
	if !errors.Is(err, ErrAuthMissing) {
		t.Fatalf("expected ErrAuthMissing, got: %v", err)
	}
}

func TestAuthorize_OAuthToken_InactiveUser(t *testing.T) {
	mock := &mockAccountClient{
		getUserByTokenFn: func(_ context.Context, _ *accountpb.GetUserByTokenRequest, _ ...grpc.CallOption) (*accountpb.GetUserByTokenResponse, error) {
			return &accountpb.GetUserByTokenResponse{
				User: &accountpb.User{Id: 100, Status: accountpb.UserStatus_USER_STATUS_INACTIVE},
			}, nil
		},
	}

	client := NewGRPCAccountClient(mock)
	r, _ := http.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer some-token")

	err := client.Authorize(context.Background(), r, "cluster-1")
	if err == nil {
		t.Fatal("expected error for inactive user")
	}
	if !errors.Is(err, ErrAuthForbidden) {
		t.Fatalf("expected ErrAuthForbidden, got: %v", err)
	}
}

func TestAuthorize_OAuthToken_NoOrgs(t *testing.T) {
	mock := &mockAccountClient{
		getUserByTokenFn: func(_ context.Context, _ *accountpb.GetUserByTokenRequest, _ ...grpc.CallOption) (*accountpb.GetUserByTokenResponse, error) {
			return &accountpb.GetUserByTokenResponse{
				User: &accountpb.User{Id: 100, Status: accountpb.UserStatus_USER_STATUS_ACTIVE},
			}, nil
		},
		listOrgsByUserFn: func(_ context.Context, _ *accountpb.ListOrgsByUserRequest, _ ...grpc.CallOption) (*accountpb.ListOrgsByUserResponse, error) {
			return &accountpb.ListOrgsByUserResponse{Orgs: nil}, nil
		},
	}

	client := NewGRPCAccountClient(mock)
	r, _ := http.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer some-token")

	err := client.Authorize(context.Background(), r, "cluster-1")
	if err == nil {
		t.Fatal("expected error for user with no orgs")
	}
	if !errors.Is(err, ErrAuthForbidden) {
		t.Fatalf("expected ErrAuthForbidden, got: %v", err)
	}
}

func TestAuthorize_OAuthToken_VerifyFails(t *testing.T) {
	mock := &mockAccountClient{
		getUserByTokenFn: func(_ context.Context, _ *accountpb.GetUserByTokenRequest, _ ...grpc.CallOption) (*accountpb.GetUserByTokenResponse, error) {
			return &accountpb.GetUserByTokenResponse{
				User: &accountpb.User{Id: 100, Status: accountpb.UserStatus_USER_STATUS_ACTIVE},
			}, nil
		},
		listOrgsByUserFn: func(_ context.Context, _ *accountpb.ListOrgsByUserRequest, _ ...grpc.CallOption) (*accountpb.ListOrgsByUserResponse, error) {
			return &accountpb.ListOrgsByUserResponse{
				Orgs: []*accountpb.Org{{Id: 200}},
			}, nil
		},
		verifyUserOrgAndProjectsFn: func(_ context.Context, _ *accountpb.VerifyUserOrgAndProjectsReq, _ ...grpc.CallOption) (*accountpb.VerifyUserOrgAndProjectsResp, error) {
			return &accountpb.VerifyUserOrgAndProjectsResp{Result: false}, nil
		},
	}

	client := NewGRPCAccountClient(mock)
	r, _ := http.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer some-token")

	err := client.Authorize(context.Background(), r, "cluster-1")
	if err == nil {
		t.Fatal("expected error for failed verify")
	}
	if !errors.Is(err, ErrAuthForbidden) {
		t.Fatalf("expected ErrAuthForbidden, got: %v", err)
	}
}

func TestAuthorize_APIKey_NoOrgScope(t *testing.T) {
	mock := &mockAccountClient{
		getApiKeyByAccessKeyFn: func(_ context.Context, _ *accountpb.GetApiKeyByAccessKeyReq, _ ...grpc.CallOption) (*accountpb.GetApiKeyByAccessKeyRsp, error) {
			return &accountpb.GetApiKeyByAccessKeyRsp{
				ApiKey:        &accountpb.ApiKey{Id: 300},
				ResourceInfos: []*accountpb.ApiKeyResourceInfo{},
			}, nil
		},
	}

	client := NewGRPCAccountClient(mock)
	r, _ := http.NewRequest("GET", "/", nil)
	r.Header.Set("X-Auth-Method", "basic")
	r.Header.Set("X-Auth-Content", `{"public_key":"my-key"}`)

	err := client.Authorize(context.Background(), r, "cluster-1")
	if err == nil {
		t.Fatal("expected error for API key with no org scope")
	}
	if !errors.Is(err, ErrAuthForbidden) {
		t.Fatalf("expected ErrAuthForbidden, got: %v", err)
	}
}

func TestAuthorize_APIKey_EmptyContent(t *testing.T) {
	client := NewGRPCAccountClient(&mockAccountClient{})
	r, _ := http.NewRequest("GET", "/", nil)
	r.Header.Set("X-Auth-Method", "digest")
	// No X-Auth-Content header

	err := client.Authorize(context.Background(), r, "cluster-1")
	if err == nil {
		t.Fatal("expected error for empty auth content")
	}
	if !errors.Is(err, ErrAuthMissing) {
		t.Fatalf("expected ErrAuthMissing, got: %v", err)
	}
}

func TestAuthorize_APIKey_InvalidJSON(t *testing.T) {
	client := NewGRPCAccountClient(&mockAccountClient{})
	r, _ := http.NewRequest("GET", "/", nil)
	r.Header.Set("X-Auth-Method", "digest")
	r.Header.Set("X-Auth-Content", "not-json")

	err := client.Authorize(context.Background(), r, "cluster-1")
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
	if !errors.Is(err, ErrAuthMissing) {
		t.Fatalf("expected ErrAuthMissing, got: %v", err)
	}
}
