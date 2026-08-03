package s3client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

const camMetaBase = "http://metadata.tencentyun.com/latest/meta-data/cam/security-credentials/"

var ErrCAMMetadataUnavailable = errors.New("cam metadata temporarily unavailable")

const (
	camMetadataRequestTimeout = 2 * time.Second
	camMetadataMaxAttempts    = 3
	camMetadataRetryBackoff   = 100 * time.Millisecond
	camCredentialRefreshAhead = 5 * time.Minute
	camCredentialFallbackMin  = time.Minute

	camMetadataListRolesOperation      = "cam_metadata_list_roles"
	camMetadataGetCredentialsOperation = "cam_metadata_get_credentials"
)

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
	mu             sync.Mutex
	cached         aws.Credentials
	expiresAt      time.Time
	client         *http.Client
	metadataURL    string
	maxAttempts    int
	retryBackoff   time.Duration
	refreshAhead   time.Duration
	fallbackMin    time.Duration
	fallbackResult bool
}

func newCAMProvider() *camProvider {
	return &camProvider{
		client:       &http.Client{Timeout: camMetadataRequestTimeout},
		metadataURL:  camMetaBase,
		maxAttempts:  camMetadataMaxAttempts,
		retryBackoff: camMetadataRetryBackoff,
		refreshAhead: camCredentialRefreshAhead,
		fallbackMin:  camCredentialFallbackMin,
	}
}

func newCAMCredentialsProvider(provider *camProvider, refreshAhead time.Duration) *aws.CredentialsCache {
	return aws.NewCredentialsCache(provider, func(options *aws.CredentialsCacheOptions) {
		options.ExpiryWindow = refreshAhead
	})
}

func (p *camProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	if now.Before(p.expiresAt.Add(-p.refreshAhead)) {
		p.fallbackResult = false
		return p.cached, nil
	}

	creds, expiry, err := p.retrieveFresh(ctx)
	if err != nil {
		if errors.Is(err, ErrCAMMetadataUnavailable) &&
			p.cached.AccessKeyID != "" &&
			time.Now().Before(p.expiresAt.Add(-p.fallbackMin)) {
			p.fallbackResult = true
			return p.cached, nil
		}
		p.fallbackResult = false
		return aws.Credentials{}, err
	}
	p.cached = creds
	p.expiresAt = expiry
	p.fallbackResult = false
	return p.cached, nil
}

func (p *camProvider) AdjustExpiresBy(creds aws.Credentials, duration time.Duration) (aws.Credentials, error) {
	p.mu.Lock()
	fallbackResult := p.fallbackResult
	p.fallbackResult = false
	p.mu.Unlock()
	if !creds.CanExpire {
		return creds, nil
	}
	if fallbackResult {
		creds.Expires = creds.Expires.Add(-p.fallbackMin)
		return creds, nil
	}
	creds.Expires = creds.Expires.Add(duration)
	return creds, nil
}

func (p *camProvider) retrieveFresh(ctx context.Context) (aws.Credentials, time.Time, error) {
	listStart := time.Now()
	body, err := p.fetchMetadata(ctx, p.metadataURL, "cam: list roles")
	if err != nil {
		recordS3Operation(camMetadataListRolesOperation, camMetadataResult(err), listStart)
		return aws.Credentials{}, time.Time{}, err
	}
	roleName := strings.TrimSpace(string(body))
	if roleName == "" {
		recordS3Operation(camMetadataListRolesOperation, "role_missing", listStart)
		return aws.Credentials{}, time.Time{}, fmt.Errorf("cam: no role bound to instance")
	}
	recordS3Operation(camMetadataListRolesOperation, "ok", listStart)

	credentialsStart := time.Now()
	body, err = p.fetchMetadata(ctx, p.metadataURL+roleName, fmt.Sprintf("cam: get credentials for %s", roleName))
	if err != nil {
		recordS3Operation(camMetadataGetCredentialsOperation, camMetadataResult(err), credentialsStart)
		return aws.Credentials{}, time.Time{}, err
	}

	var creds struct {
		TmpSecretId  string `json:"TmpSecretId"`
		TmpSecretKey string `json:"TmpSecretKey"`
		Token        string `json:"Token"`
		Expiration   string `json:"Expiration"`
	}
	if err := json.Unmarshal(body, &creds); err != nil {
		recordS3Operation(camMetadataGetCredentialsOperation, "invalid_response", credentialsStart)
		return aws.Credentials{}, time.Time{}, fmt.Errorf("cam: parse credentials: %w", err)
	}
	if creds.TmpSecretId == "" || creds.TmpSecretKey == "" {
		recordS3Operation(camMetadataGetCredentialsOperation, "invalid_response", credentialsStart)
		return aws.Credentials{}, time.Time{}, fmt.Errorf("cam: credentials response is missing access key fields")
	}

	expiry, err := time.Parse(time.RFC3339, creds.Expiration)
	if err != nil {
		recordS3Operation(camMetadataGetCredentialsOperation, "invalid_response", credentialsStart)
		return aws.Credentials{}, time.Time{}, fmt.Errorf("cam: parse expiration %q: %w", creds.Expiration, err)
	}
	recordS3Operation(camMetadataGetCredentialsOperation, "ok", credentialsStart)

	return aws.Credentials{
		AccessKeyID:     creds.TmpSecretId,
		SecretAccessKey: creds.TmpSecretKey,
		SessionToken:    creds.Token,
		Source:          "TencentCAMRole",
		CanExpire:       true,
		Expires:         expiry,
	}, expiry, nil
}

func (p *camProvider) fetchMetadata(ctx context.Context, url, operation string) ([]byte, error) {
	attempts := p.maxAttempts
	if attempts < 1 {
		attempts = 1
	}
	var lastErr error
	lastRetryable := false
	for attempt := 1; attempt <= attempts; attempt++ {
		body, retryable, err := p.fetchMetadataOnce(ctx, url, operation)
		if err == nil {
			return body, nil
		}
		lastErr = err
		lastRetryable = retryable
		if !retryable || attempt == attempts {
			break
		}
		if err := waitForCAMRetry(ctx, p.retryBackoff*time.Duration(attempt)); err != nil {
			return nil, fmt.Errorf("%s: %w", operation, err)
		}
	}
	if lastRetryable {
		return nil, &camMetadataUnavailableError{err: lastErr}
	}
	return nil, lastErr
}

func (p *camProvider) fetchMetadataOnce(ctx context.Context, url, operation string) ([]byte, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, false, fmt.Errorf("%s: create request: %w", operation, err)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, ctx.Err() == nil, fmt.Errorf("%s: %w", operation, err)
	}
	body, readErr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if readErr != nil {
		return nil, ctx.Err() == nil, fmt.Errorf("%s: read response: %w", operation, readErr)
	}
	if resp.StatusCode != http.StatusOK {
		retryable := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= http.StatusInternalServerError
		return nil, retryable, fmt.Errorf("%s: %w", operation, &camMetadataStatusError{statusCode: resp.StatusCode})
	}
	return body, false, nil
}

type camMetadataStatusError struct {
	statusCode int
}

func (e *camMetadataStatusError) Error() string {
	return fmt.Sprintf("metadata status %d", e.statusCode)
}

type camMetadataUnavailableError struct {
	err error
}

func (e *camMetadataUnavailableError) Error() string {
	return e.err.Error()
}

func (e *camMetadataUnavailableError) Unwrap() error {
	return e.err
}

func (e *camMetadataUnavailableError) Is(target error) bool {
	return target == ErrCAMMetadataUnavailable
}

func camMetadataResult(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var networkErr net.Error
	if errors.As(err, &networkErr) && networkErr.Timeout() {
		return "timeout"
	}
	var statusErr *camMetadataStatusError
	if errors.As(err, &statusErr) {
		switch {
		case statusErr.statusCode == http.StatusTooManyRequests:
			return "throttled"
		case statusErr.statusCode >= http.StatusInternalServerError:
			return "server_error"
		default:
			return "rejected"
		}
	}
	return "unavailable"
}

func waitForCAMRetry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
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
	provider := newCAMProvider()
	return newCAMCredentialsProvider(provider, provider.refreshAhead), nil
}

func newTencent(ctx context.Context, cfg AWSConfig) (*AWSS3Client, error) {
	provider, err := credentialsForTencent(cfg)
	if err != nil {
		return nil, err
	}
	return buildS3Client(ctx, cfg, provider)
}
