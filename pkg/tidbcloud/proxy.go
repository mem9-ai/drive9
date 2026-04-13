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
	"time"
)

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

// CreateServiceUserViaProxy creates a SQL user with role_admin privileges
// by calling the internal cluster proxy's execute endpoint.
//
// operatorUser / operatorPass are credentials for an existing DB user
// (typically root) that the proxy uses to authenticate the request.
// newUser / newPass are the credentials for the new service user to create.
func CreateServiceUserViaProxy(ctx context.Context, proxyEndpoint string, clusterID uint64, operatorUser, operatorPass, newUser, newPass string) error {
	if proxyEndpoint == "" {
		return fmt.Errorf("create service user: proxy endpoint is empty")
	}

	queries := []string{
		fmt.Sprintf("CREATE USER IF NOT EXISTS '%s' IDENTIFIED BY '%s'", newUser, newPass),
		fmt.Sprintf("GRANT 'role_admin' TO '%s'", newUser),
		fmt.Sprintf("SET DEFAULT ROLE ALL TO '%s'", newUser),
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
