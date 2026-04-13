package tidbcloud

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/logger"
	"go.uber.org/zap"
)

const (
	proxyExecutePath = "/v1beta2/execute"
	proxyAuthMethod  = "password"
	proxyTimeout     = 120 * time.Second
)

// ClusterProxyClient executes SQL against a TiDB cluster through the cluster
// proxy HTTP service (service-proxy). This bypasses the public load balancer,
// which blocks cloud_admin connections.
type ClusterProxyClient struct {
	baseURL   string
	clusterID uint64
	username  string
	password  string
	client    *http.Client
}

// NewClusterProxyClient creates a proxy client for the given cluster.
func NewClusterProxyClient(proxyEndpoint string, clusterID uint64, username, password string) *ClusterProxyClient {
	baseURL := proxyEndpoint
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		baseURL = "https://" + baseURL
	}

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: true, // #nosec G402 — internal service-proxy; cert SANs don't cover the ELB hostname
		},
		DisableKeepAlives: true,
	}

	return &ClusterProxyClient{
		baseURL:   baseURL,
		clusterID: clusterID,
		username:  username,
		password:  password,
		client: &http.Client{
			Transport: transport,
			Timeout:   proxyTimeout,
		},
	}
}

type proxyOperator struct {
	Username   string `json:"username"`
	AuthMethod string `json:"authMethod"`
	Credential string `json:"credential"`
}

type proxyExecuteRequest struct {
	Operator  *proxyOperator `json:"operator"`
	ClusterID uint64         `json:"clusterID"`
	Query     string         `json:"query"`
}

type proxyExecuteResponse struct {
	ErrNumber  int    `json:"errNumber"`
	ErrMessage string `json:"errMessage"`
}

// ExecuteSQL executes a single SQL statement via the cluster proxy.
func (c *ClusterProxyClient) ExecuteSQL(ctx context.Context, sql string) error {
	op := &proxyOperator{
		Username:   c.username,
		AuthMethod: proxyAuthMethod,
		Credential: base64.URLEncoding.EncodeToString([]byte(c.password)),
	}

	reqBody := &proxyExecuteRequest{
		Operator:  op,
		ClusterID: c.clusterID,
		Query:     sql,
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	url := c.baseURL + proxyExecutePath
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("proxy request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		logger.Warn(ctx, "proxy_execute_http_error",
			zap.Int("status", resp.StatusCode),
			zap.String("body", truncate(string(body), 200)))
		return fmt.Errorf("proxy HTTP %d: %s", resp.StatusCode, truncate(string(body), 200))
	}

	var result proxyExecuteResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("unmarshal response: %w", err)
	}

	if result.ErrNumber != 0 {
		return fmt.Errorf("sql err: %d %s", result.ErrNumber, result.ErrMessage)
	}

	return nil
}

// CreateServiceUser creates a dedicated database user for drive9 runtime
// operations via the cluster proxy. The user is assigned the role_admin role
// (via GRANT ROLE, which does not require GRANT OPTION) instead of individual
// privilege GRANTs that would fail with error 8121.
func (c *ClusterProxyClient) CreateServiceUser(ctx context.Context, userPrefix string) (*ServiceUser, error) {
	password, err := generatePassword(32)
	if err != nil {
		return nil, fmt.Errorf("generate password: %w", err)
	}

	bareUser := "fs_admin"
	qualifiedUser := bareUser
	if userPrefix != "" {
		qualifiedUser = userPrefix + "." + bareUser
	}

	escapedUser := escapeSQLString(qualifiedUser)
	escapedPassword := escapeSQLString(password)

	stmts := []string{
		fmt.Sprintf("CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY '%s'",
			escapedUser, escapedPassword),
		fmt.Sprintf("ALTER USER '%s'@'%%' IDENTIFIED BY '%s'",
			escapedUser, escapedPassword),
		// Use GRANT ROLE instead of GRANT privilege — cloud_admin has ROLE_ADMIN
		// but NOT GRANT OPTION, so explicit privilege grants would fail with
		// error 8121 ("privilege check for 'Grant Option' fail").
		fmt.Sprintf("GRANT 'role_admin' TO '%s'@'%%'", escapedUser),
		fmt.Sprintf("SET DEFAULT ROLE ALL TO '%s'@'%%'", escapedUser),
	}

	for _, stmt := range stmts {
		if err := c.ExecuteSQL(ctx, stmt); err != nil {
			return nil, fmt.Errorf("create service user: %w", err)
		}
	}

	logger.Info(ctx, "proxy_service_user_created",
		zap.String("user", qualifiedUser))

	return &ServiceUser{
		Username: qualifiedUser,
		Password: password,
	}, nil
}

func generatePassword(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func escapeSQLString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `''`)
	return s
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
