package s3client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

const camMetaBase = "http://metadata.tencentyun.com/latest/meta-data/cam/security-credentials/"

func isTencentEndpoint(endpoint string) bool {
	if len(endpoint) == 0 {
		return false
	}
	host := endpoint
	if i := strings.Index(endpoint, "://"); i >= 0 {
		host = endpoint[i+3:]
	}
	if i := strings.IndexAny(host, "/:?"); i >= 0 {
		host = host[:i]
	}
	return host == "myqcloud.com" || strings.HasSuffix(host, ".myqcloud.com")
}

func tencentCredentials() (accessKeyID, secretAccessKey, securityToken string) {
	accessKeyID = os.Getenv("TENCENTCLOUD_SECRET_ID")
	if accessKeyID == "" {
		accessKeyID = os.Getenv("TENCENTCLOUD_SECRETID")
	}
	return accessKeyID,
		tencentSecretKey(),
		os.Getenv("TENCENTCLOUD_SECURITY_TOKEN")
}

func tencentSecretKey() string {
	key := os.Getenv("TENCENTCLOUD_SECRET_KEY")
	if key == "" {
		key = os.Getenv("TENCENTCLOUD_SECRETKEY")
	}
	return key
}

type camProvider struct {
	mu        sync.Mutex
	cached    aws.Credentials
	expiresAt time.Time
	client    *http.Client
}

func newCAMProvider() *camProvider {
	return &camProvider{
		client: &http.Client{Timeout: 5 * time.Second},
	}
}

func (p *camProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if time.Now().Before(p.expiresAt.Add(-5 * time.Minute)) {
		return p.cached, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, camMetaBase, nil)
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("cam: create list roles request: %w", err)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("cam: list roles: %w", err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("cam: read list roles response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return aws.Credentials{}, fmt.Errorf("cam: list roles: status %d", resp.StatusCode)
	}
	roleName := strings.TrimSpace(string(body))
	if roleName == "" {
		return aws.Credentials{}, fmt.Errorf("cam: no role bound to instance")
	}

	req, err = http.NewRequestWithContext(ctx, http.MethodGet, camMetaBase+roleName, nil)
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("cam: create get credentials request: %w", err)
	}
	resp, err = p.client.Do(req)
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("cam: get credentials for %s: %w", roleName, err)
	}
	body, err = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("cam: read get credentials response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return aws.Credentials{}, fmt.Errorf("cam: get credentials: status %d", resp.StatusCode)
	}

	var creds struct {
		TmpSecretId  string `json:"TmpSecretId"`
		TmpSecretKey string `json:"TmpSecretKey"`
		Token        string `json:"Token"`
		Expiration   string `json:"Expiration"`
	}
	if err := json.Unmarshal(body, &creds); err != nil {
		return aws.Credentials{}, fmt.Errorf("cam: parse credentials: %w", err)
	}

	expiry, err := time.Parse(time.RFC3339, creds.Expiration)
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("cam: parse expiration %q: %w", creds.Expiration, err)
	}

	p.cached = aws.Credentials{
		AccessKeyID:     creds.TmpSecretId,
		SecretAccessKey: creds.TmpSecretKey,
		SessionToken:    creds.Token,
		Source:          "TencentCAMRole",
		CanExpire:       true,
		Expires:         expiry,
	}
	p.expiresAt = expiry
	return p.cached, nil
}

func credentialsForTencent(cfg AWSConfig) (aws.CredentialsProvider, error) {
	if cfg.AccessKeyID != "" {
		if cfg.SecretAccessKey == "" {
			return nil, fmt.Errorf("s3: AccessKeyID is set but SecretAccessKey is empty")
		}
		return credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken), nil
	}
	accessKeyID, secretAccessKey, sessionToken := tencentCredentials()
	if accessKeyID != "" {
		if secretAccessKey == "" {
			return nil, fmt.Errorf("s3: TENCENTCLOUD_SECRET_ID is set but TENCENTCLOUD_SECRET_KEY is empty")
		}
		return credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, sessionToken), nil
	}
	return newCAMProvider(), nil
}

func newTencent(ctx context.Context, cfg AWSConfig) (*AWSS3Client, error) {
	provider, err := credentialsForTencent(cfg)
	if err != nil {
		return nil, err
	}
	return buildS3Client(ctx, cfg, provider)
}
