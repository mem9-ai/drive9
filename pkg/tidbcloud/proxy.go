package tidbcloud

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// ProxyAuth0Config holds OAuth2 client-credentials configuration for
// authenticating requests to the cluster proxy's JWT middleware.
// When all fields are non-empty, CreateServiceUserViaProxy obtains a
// Bearer token and attaches it to the request.
type ProxyAuth0Config struct {
	Domain       string // Auth0 domain, e.g. "foo.us.auth0.com"
	ClientID     string
	ClientSecret string
	Audience     string
}

// proxyOperator represents the JSON operator in a proxy request.
type proxyOperator struct {
	Username   string `json:"Username"`
	AuthMethod string `json:"AuthMethod"`
	Credential string `json:"Credential"`
}

// proxyExecuteRequest is the JSON body for POST /v1beta2/execute.
type proxyExecuteRequest struct {
	Operator  *proxyOperator `json:"operator"`
	ClusterID uint64         `json:"clusterID"`
	Queries   []string       `json:"queries"`
}

// proxyExecuteResponse is the JSON response from POST /v1beta2/execute.
type proxyExecuteResponse struct {
	ErrNumber uint16 `json:"errNumber"`
	ErrMsg    string `json:"errMessage"`
}

// CreateServiceUserViaProxy creates a dedicated fs_admin SQL user for drive9
// by calling the internal cluster proxy's /v1beta2/execute endpoint.
//
// It creates a dedicated _tidbcloud_fs database and a custom role_fs_admin role
// with the minimum DDL/DML privileges needed on that database (CREATE, ALTER,
// DROP, INDEX, SELECT, INSERT, UPDATE, DELETE), then creates (or updates) the
// service user and assigns the role as its default role.
//
// operatorUser / operatorPass are credentials for an existing DB user
// (typically root) that the proxy uses to authenticate the request.
// newUser / newPass are the credentials for the new service user to create.
// auth0Cfg provides OAuth2 client-credentials for the proxy's JWT middleware;
// when nil or empty, no Authorization header is sent (suitable for dev/staging
// environments where Auth0 is disabled).
func CreateServiceUserViaProxy(ctx context.Context, proxyEndpoint string, clusterID uint64, operatorUser, operatorPass, newUser, newPass string, auth0Cfg *ProxyAuth0Config) error {
	if proxyEndpoint == "" {
		return fmt.Errorf("create service user: proxy endpoint is empty")
	}
	if err := validateSQLIdentifier(newUser); err != nil {
		return fmt.Errorf("create service user: invalid username: %w", err)
	}
	if err := validateSQLPassword(newPass); err != nil {
		return fmt.Errorf("create service user: invalid password: %w", err)
	}

	const roleName = "role_fs_admin"
	const dbName = "_tidbcloud_fs"
	queries := []string{
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName),
		fmt.Sprintf("CREATE ROLE IF NOT EXISTS '%s'", roleName),
		fmt.Sprintf("GRANT CREATE, ALTER, DROP, INDEX, SELECT, INSERT, UPDATE, DELETE ON %s.* TO '%s'", dbName, roleName),
		fmt.Sprintf("CREATE USER IF NOT EXISTS '%s' IDENTIFIED BY '%s'", newUser, newPass),
		fmt.Sprintf("ALTER USER '%s' IDENTIFIED BY '%s'", newUser, newPass),
		fmt.Sprintf("GRANT '%s' TO '%s'", roleName, newUser),
		fmt.Sprintf("SET DEFAULT ROLE '%s' TO '%s'", roleName, newUser),
	}

	body := proxyExecuteRequest{
		Operator: &proxyOperator{
			Username:   operatorUser,
			AuthMethod: "password",
			Credential: base64.URLEncoding.EncodeToString([]byte(operatorPass)),
		},
		ClusterID: clusterID,
		Queries:   queries,
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("create service user: marshal request: %w", err)
	}

	url := proxyEndpoint + "/v1beta2/execute"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create service user: new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Attach Auth0 JWT if configured.
	if auth0Cfg != nil && auth0Cfg.Domain != "" && auth0Cfg.ClientID != "" && auth0Cfg.ClientSecret != "" {
		token, err := getAuth0ClientToken(ctx, auth0Cfg)
		if err != nil {
			return fmt.Errorf("create service user: get auth0 token: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}

	// The internal cluster proxy uses a certificate that does not match
	// the ELB hostname, so we skip TLS verification for this internal call.
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // internal service proxy with mismatched cert
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("create service user via proxy: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("create service user: read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("create service user: proxy returned %d: %s", resp.StatusCode, string(respBody))
	}

	var result proxyExecuteResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return fmt.Errorf("create service user: unmarshal response: %w", err)
	}
	if result.ErrNumber != 0 {
		return fmt.Errorf("create service user: SQL error %d: %s", result.ErrNumber, result.ErrMsg)
	}

	return nil
}

// validateSQLIdentifier rejects strings that contain characters unsafe for
// SQL single-quoted identifiers (single quotes, backslashes, control chars).
func validateSQLIdentifier(s string) error {
	if s == "" {
		return fmt.Errorf("must not be empty")
	}
	for _, c := range s {
		if c == '\'' || c == '\\' || c < 0x20 {
			return fmt.Errorf("contains forbidden character %q", c)
		}
	}
	return nil
}

// validateSQLPassword rejects passwords that could break single-quoted SQL
// string literals.
func validateSQLPassword(s string) error {
	if s == "" {
		return fmt.Errorf("must not be empty")
	}
	for _, c := range s {
		if c == '\'' || c == '\\' || c < 0x20 {
			return fmt.Errorf("contains forbidden character %q", c)
		}
	}
	return nil
}

// getAuth0ClientToken obtains an OAuth2 client-credentials token from Auth0,
// matching the pattern used by tidb-management-service's cluster proxy client.
func getAuth0ClientToken(ctx context.Context, cfg *ProxyAuth0Config) (string, error) {
	tokenURL := "https://" + cfg.Domain + "/oauth/token"

	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {cfg.ClientID},
		"client_secret": {cfg.ClientSecret},
		"audience":      {cfg.Audience},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL,
		bytes.NewBufferString(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("build auth0 token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("auth0 token request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("read auth0 response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("auth0 returned %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return "", fmt.Errorf("decode auth0 response: %w", err)
	}
	if tokenResp.AccessToken == "" {
		return "", fmt.Errorf("auth0 returned empty access_token")
	}
	return tokenResp.AccessToken, nil
}
