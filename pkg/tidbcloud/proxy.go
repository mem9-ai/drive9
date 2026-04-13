package tidbcloud

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
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

// ProxySQLError is returned when the cluster proxy reports a SQL-level error.
type ProxySQLError struct {
	ErrNumber  int
	ErrMessage string
}

func (e *ProxySQLError) Error() string {
	return fmt.Sprintf("sql err: %d %s", e.ErrNumber, e.ErrMessage)
}

// isGrantPrivilegeError returns true for SQL errors that indicate the operator
// lacks privilege to grant roles (1227 = need SUPER/ROLE_ADMIN) or grant
// specific privileges (8121 = need GRANT OPTION).
func isGrantPrivilegeError(err error) bool {
	var sqlErr *ProxySQLError
	if errors.As(err, &sqlErr) {
		return sqlErr.ErrNumber == 1227 || sqlErr.ErrNumber == 8121
	}
	return false
}

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
		return &ProxySQLError{ErrNumber: result.ErrNumber, ErrMessage: result.ErrMessage}
	}

	return nil
}

const (
	// drive9DBName is the dedicated database created for fs_admin when
	// role_admin is unavailable and the user cannot access "mysql".
	drive9DBName = "_drive9_fs"
)

// CreateServiceUser creates a dedicated database user for drive9 runtime
// operations via the cluster proxy. It always creates the user and sets a
// password.
//
// Privilege strategy (in order of preference):
//  1. GRANT role_admin — gives full admin on "mysql" database.
//  2. If role_admin unavailable: return DBName = _drive9_fs so provisioning
//     can create and initialize the dedicated database via the public LB.
//
// The returned ServiceUser.DBName indicates which database to use.
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

	// Phase 1 — must succeed: create the user and set its password.
	required := []struct {
		step string
		stmt string
	}{
		{
			step: "create user",
			stmt: fmt.Sprintf("CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY '%s'",
				escapedUser, escapedPassword),
		},
		{
			step: "alter user password",
			stmt: fmt.Sprintf("ALTER USER '%s'@'%%' IDENTIFIED BY '%s'",
				escapedUser, escapedPassword),
		},
	}
	for _, s := range required {
		if err := c.ExecuteSQL(ctx, s.stmt); err != nil {
			return nil, fmt.Errorf("create service user (%s): %w", s.step, err)
		}
	}

	// Phase 2 — try granting role_admin (full admin on "mysql").
	roleGranted := true
	roleStmts := []struct {
		step string
		stmt string
	}{
		{
			step: "grant role_admin",
			stmt: fmt.Sprintf("GRANT 'role_admin' TO '%s'@'%%'", escapedUser),
		},
		{
			step: "set default role",
			stmt: fmt.Sprintf("SET DEFAULT ROLE ALL TO '%s'@'%%'", escapedUser),
		},
	}
	for _, s := range roleStmts {
		if err := c.ExecuteSQL(ctx, s.stmt); err != nil {
			if !isGrantPrivilegeError(err) {
				return nil, fmt.Errorf("create service user (%s): %w", s.step, err)
			}
			logger.Warn(ctx, "proxy_service_user_grant_role_skipped",
				zap.String("step", s.step),
				zap.String("user", qualifiedUser),
				zap.Error(err))
			roleGranted = false
			break
		}
	}

	if roleGranted {
		logger.Info(ctx, "proxy_service_user_created",
			zap.String("user", qualifiedUser),
			zap.String("db", "mysql"))
		return &ServiceUser{
			Username: qualifiedUser,
			Password: password,
			DBName:   "mysql",
		}, nil
	}

	// Phase 3 — role_admin unavailable: fs_admin will use a dedicated
	// database instead of "mysql". The database will be created by
	// fs_admin itself during provisioning (no cloud_admin GRANT needed).
	logger.Info(ctx, "proxy_service_user_created",
		zap.String("user", qualifiedUser),
		zap.String("db", drive9DBName),
		zap.Bool("role_granted", false))

	return &ServiceUser{
		Username: qualifiedUser,
		Password: password,
		DBName:   drive9DBName,
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
