package tidbcloudnative

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/tenant"
	"go.uber.org/zap"
)

// HTTPClustersAPI implements ClustersAPI against TiDB Cloud OpenAPI + IAM.
type HTTPClustersAPI struct {
	apiURL string
	iamURL string
	client *http.Client
}

func NewHTTPClustersAPI(apiURL, iamURL string, client *http.Client) *HTTPClustersAPI {
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	return &HTTPClustersAPI{
		apiURL: strings.TrimRight(strings.TrimSpace(apiURL), "/"),
		iamURL: strings.TrimRight(strings.TrimSpace(iamURL), "/"),
		client: client,
	}
}

func (a *HTTPClustersAPI) CreateCluster(ctx context.Context, publicKey, privateKey string, body []byte) (*clusterInfo, error) {
	if err := requireCreds(publicKey, privateKey); err != nil {
		return nil, err
	}
	resp, err := a.doDigestAuthRequest(ctx, publicKey, privateKey, http.MethodPost, a.apiURL+"/v1beta1/clusters", body)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if !httpStatusOK(resp.StatusCode) {
		raw, readErr := readUpstreamBody(resp.Body, upstreamErrorBodyLimit+1)
		if readErr != nil {
			return nil, readErr
		}
		return nil, newAPIStatusError("provision", resp.StatusCode, sanitizeUpstreamBody(raw))
	}
	raw, readErr := readUpstreamBody(resp.Body, upstreamClusterBodyLimit)
	if readErr != nil {
		return nil, readErr
	}
	return parseClusterInfo(raw)
}

func (a *HTTPClustersAPI) BatchCreateClusters(ctx context.Context, publicKey, privateKey string, body []byte) ([]clusterInfo, error) {
	if err := requireCreds(publicKey, privateKey); err != nil {
		return nil, err
	}
	resp, err := a.doDigestAuthRequest(ctx, publicKey, privateKey, http.MethodPost, a.apiURL+"/v1beta1/clusters:batchCreate", body)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if !httpStatusOK(resp.StatusCode) {
		raw, readErr := readUpstreamBody(resp.Body, upstreamErrorBodyLimit+1)
		if readErr != nil {
			return nil, readErr
		}
		return nil, newAPIStatusError("batch provision", resp.StatusCode, sanitizeUpstreamBody(raw))
	}
	raw, err := readUpstreamBody(resp.Body, upstreamClusterBodyLimit)
	if err != nil {
		return nil, err
	}
	var created clusterListResponse
	if err := json.Unmarshal(raw, &created); err != nil {
		return nil, err
	}
	return created.Clusters, nil
}

func (a *HTTPClustersAPI) GetCluster(ctx context.Context, publicKey, privateKey, clusterID string) (*clusterInfo, error) {
	if err := requireCreds(publicKey, privateKey); err != nil {
		return nil, err
	}
	clusterID = strings.TrimSpace(clusterID)
	if clusterID == "" {
		return nil, fmt.Errorf("cluster id is required")
	}
	endpoint := fmt.Sprintf("%s/v1beta1/clusters/%s?view=BASIC", a.apiURL, url.PathEscape(clusterID))
	resp, err := a.doDigestAuthRequest(ctx, publicKey, privateKey, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("get cluster basic info: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if !httpStatusOK(resp.StatusCode) {
		raw, readErr := readUpstreamBody(resp.Body, upstreamErrorBodyLimit+1)
		if readErr != nil {
			return nil, fmt.Errorf("read cluster get error body: %w", readErr)
		}
		return nil, newAPIStatusError("cluster get", resp.StatusCode, sanitizeUpstreamBody(raw))
	}
	raw, readErr := readUpstreamBody(resp.Body, upstreamClusterBodyLimit)
	if readErr != nil {
		return nil, fmt.Errorf("read cluster body: %w", readErr)
	}
	info, err := parseClusterInfo(raw)
	if err != nil {
		return nil, fmt.Errorf("parse cluster info: %w", err)
	}
	if info.Labels == nil {
		info.Labels = make(map[string]string)
	}
	return info, nil
}

func (a *HTTPClustersAPI) ListClusters(ctx context.Context, publicKey, privateKey string, query url.Values) ([]clusterInfo, string, error) {
	if err := requireCreds(publicKey, privateKey); err != nil {
		return nil, "", err
	}
	endpoint := a.apiURL + "/v1beta1/clusters"
	if encoded := query.Encode(); encoded != "" {
		endpoint += "?" + encoded
	}
	resp, err := a.doDigestAuthRequest(ctx, publicKey, privateKey, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if !httpStatusOK(resp.StatusCode) {
		raw, readErr := readUpstreamBody(resp.Body, upstreamErrorBodyLimit+1)
		if readErr != nil {
			return nil, "", fmt.Errorf("read cluster list error body: %w", readErr)
		}
		return nil, "", newAPIStatusError("cluster list", resp.StatusCode, sanitizeUpstreamBody(raw))
	}
	raw, readErr := readUpstreamBody(resp.Body, upstreamClusterBodyLimit)
	if readErr != nil {
		return nil, "", fmt.Errorf("read cluster list body: %w", readErr)
	}
	list, err := parseClusterList(raw)
	if err != nil {
		return nil, "", fmt.Errorf("parse cluster list: %w", err)
	}
	return list.Clusters, strings.TrimSpace(list.NextPageToken), nil
}

func (a *HTTPClustersAPI) PatchCluster(ctx context.Context, publicKey, privateKey, clusterID string, body []byte) error {
	if err := requireCreds(publicKey, privateKey); err != nil {
		return err
	}
	clusterID = strings.TrimSpace(clusterID)
	if clusterID == "" {
		return fmt.Errorf("cluster id is required")
	}
	endpoint := fmt.Sprintf("%s/v1beta1/clusters/%s", a.apiURL, url.PathEscape(clusterID))
	resp, err := a.doDigestAuthRequest(ctx, publicKey, privateKey, http.MethodPatch, endpoint, body)
	if err != nil {
		return fmt.Errorf("patch cluster: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if !httpStatusOK(resp.StatusCode) {
		raw, readErr := readUpstreamBody(resp.Body, upstreamErrorBodyLimit+1)
		if readErr != nil {
			return fmt.Errorf("read cluster patch error body: %w", readErr)
		}
		return newAPIStatusError("cluster patch", resp.StatusCode, sanitizeUpstreamBody(raw))
	}
	return nil
}

func (a *HTTPClustersAPI) DeleteCluster(ctx context.Context, publicKey, privateKey, clusterID string) error {
	if err := requireCreds(publicKey, privateKey); err != nil {
		return err
	}
	clusterID = strings.TrimSpace(clusterID)
	if clusterID == "" {
		return fmt.Errorf("cluster id is required")
	}
	endpoint := fmt.Sprintf("%s/v1beta1/clusters/%s", a.apiURL, url.PathEscape(clusterID))
	resp, err := a.doDigestAuthRequest(ctx, publicKey, privateKey, http.MethodDelete, endpoint, nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotFound {
		return nil
	}
	raw, readErr := readUpstreamBody(resp.Body, upstreamErrorBodyLimit+1)
	if readErr != nil {
		return readErr
	}
	return newAPIStatusError("cluster delete", resp.StatusCode, sanitizeUpstreamBody(raw))
}

func (a *HTTPClustersAPI) CreateBranch(ctx context.Context, publicKey, privateKey, clusterID string, body []byte) (*branchInfo, error) {
	if err := requireCreds(publicKey, privateKey); err != nil {
		return nil, err
	}
	endpoint := fmt.Sprintf("%s/v1beta1/clusters/%s/branches", a.apiURL, url.PathEscape(clusterID))
	resp, err := a.doDigestAuthRequest(ctx, publicKey, privateKey, http.MethodPost, endpoint, body)
	if err != nil {
		return nil, fmt.Errorf("create branch request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		raw, readErr := readUpstreamBody(resp.Body, upstreamErrorBodyLimit+1)
		if readErr != nil {
			return nil, readErr
		}
		return nil, newAPIStatusError("branch provision", resp.StatusCode, sanitizeUpstreamBody(raw))
	}
	raw, readErr := readUpstreamBody(resp.Body, upstreamClusterBodyLimit)
	if readErr != nil {
		return nil, readErr
	}
	return parseBranchInfo(raw)
}

func (a *HTTPClustersAPI) GetBranch(ctx context.Context, publicKey, privateKey, clusterID, branchID string) (*branchInfo, error) {
	if err := requireCreds(publicKey, privateKey); err != nil {
		return nil, err
	}
	endpoint := fmt.Sprintf("%s/v1beta1/clusters/%s/branches/%s?view=BASIC", a.apiURL, url.PathEscape(clusterID), url.PathEscape(branchID))
	resp, err := a.doDigestAuthRequest(ctx, publicKey, privateKey, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		raw, readErr := readUpstreamBody(resp.Body, upstreamErrorBodyLimit+1)
		if readErr != nil {
			return nil, readErr
		}
		return nil, newAPIStatusError("branch get", resp.StatusCode, sanitizeUpstreamBody(raw))
	}
	raw, readErr := readUpstreamBody(resp.Body, upstreamClusterBodyLimit)
	if readErr != nil {
		return nil, readErr
	}
	return parseBranchInfo(raw)
}

func (a *HTTPClustersAPI) DeleteBranch(ctx context.Context, publicKey, privateKey, clusterID, branchID string) error {
	if err := requireCreds(publicKey, privateKey); err != nil {
		return err
	}
	endpoint := fmt.Sprintf("%s/v1beta1/clusters/%s/branches/%s", a.apiURL, url.PathEscape(clusterID), url.PathEscape(branchID))
	resp, err := a.doDigestAuthRequest(ctx, publicKey, privateKey, http.MethodDelete, endpoint, nil)
	if err != nil {
		return fmt.Errorf("delete branch request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotFound {
		return nil
	}
	raw, readErr := readUpstreamBody(resp.Body, upstreamErrorBodyLimit+1)
	if readErr != nil {
		return readErr
	}
	return newAPIStatusError("branch delete", resp.StatusCode, sanitizeUpstreamBody(raw))
}

func (a *HTTPClustersAPI) ResolveAPIKey(ctx context.Context, publicKey, privateKey string) (*tenant.TiDBCloudAPIKeyIdentity, error) {
	publicKey = strings.TrimSpace(publicKey)
	privateKey = strings.TrimSpace(privateKey)
	if publicKey == "" || privateKey == "" {
		return nil, tenant.ErrCredentialsRequired
	}
	endpoint := fmt.Sprintf("%s/v1beta1/apikeys/%s", a.iamURL, url.PathEscape(publicKey))
	resp, err := a.doDigestAuthRequest(ctx, publicKey, privateKey, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if !httpStatusOK(resp.StatusCode) {
		_, readErr := readUpstreamBody(resp.Body, upstreamErrorBodyLimit+1)
		if readErr != nil {
			return nil, readErr
		}
		return nil, fmt.Errorf("%s", statusError("IAM API key lookup", resp.StatusCode, ""))
	}
	var info struct {
		Name      string `json:"name"`
		AccessKey string `json:"accessKey"`
		Role      string `json:"role"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, upstreamClusterBodyLimit)).Decode(&info); err != nil {
		return nil, fmt.Errorf("decode IAM API key response: %w", err)
	}
	if strings.TrimSpace(info.AccessKey) != publicKey {
		return nil, fmt.Errorf("IAM API key response does not match request credentials")
	}
	parts := strings.Split(strings.Trim(strings.TrimSpace(info.Name), "/"), "/")
	if len(parts) < 2 || parts[0] != "orgs" || strings.TrimSpace(parts[1]) == "" {
		return nil, fmt.Errorf("IAM API key response is missing organization")
	}
	role := strings.TrimSpace(info.Role)
	if role != tenant.TiDBCloudRoleOrgOwner && role != tenant.TiDBCloudRoleProjectOwner {
		return nil, fmt.Errorf("%w: role %q; org:owner or project:owner is required", tenant.ErrTiDBCloudRoleInsufficient, role)
	}
	return &tenant.TiDBCloudAPIKeyIdentity{OrganizationID: strings.TrimSpace(parts[1]), Role: role}, nil
}

func (a *HTTPClustersAPI) doDigestAuthRequest(ctx context.Context, publicKey, privateKey, method, uri string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, uri, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := a.client.Do(req)
	if err != nil {
		err = redactRequestError(err, uri)
		logger.Error(ctx, "tidbcloud_api_request",
			zap.String("method", method),
			zap.String("path", requestPath(uri)),
			zap.String("result", "error"),
			zap.Int64("duration_ms", time.Since(start).Milliseconds()),
			zap.Error(err))
		return nil, err
	}
	if resp.StatusCode != http.StatusUnauthorized {
		logger.Info(ctx, "tidbcloud_api_request",
			zap.String("method", method),
			zap.String("path", requestPath(uri)),
			zap.Int("status", resp.StatusCode),
			zap.Int64("duration_ms", time.Since(start).Milliseconds()))
		return resp, nil
	}
	_ = resp.Body.Close()

	wwwAuth := resp.Header.Get("WWW-Authenticate")
	nonce, realm, qop := parseDigestChallenge(wwwAuth)
	if nonce == "" {
		return nil, fmt.Errorf("invalid digest challenge")
	}
	auth, err := buildDigestAuth(publicKey, privateKey, method, uri, nonce, realm, qop)
	if err != nil {
		return nil, err
	}
	req2, err := http.NewRequestWithContext(ctx, method, uri, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Authorization", auth)
	start2 := time.Now()
	resp2, err := a.client.Do(req2)
	if err != nil {
		err = redactRequestError(err, uri)
		logger.Error(ctx, "tidbcloud_api_request",
			zap.String("method", method),
			zap.String("path", requestPath(uri)),
			zap.String("result", "error"),
			zap.Int64("duration_ms", time.Since(start2).Milliseconds()),
			zap.Error(err))
		return nil, err
	}
	logger.Info(ctx, "tidbcloud_api_request",
		zap.String("method", method),
		zap.String("path", requestPath(uri)),
		zap.Int("status", resp2.StatusCode),
		zap.Int64("duration_ms", time.Since(start2).Milliseconds()))
	return resp2, nil
}
