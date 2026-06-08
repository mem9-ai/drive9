package s3client

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	alibabasdk "github.com/aliyun/alibaba-cloud-sdk-go/sdk"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/sts"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// aliyunCredentials returns the Alibaba Cloud access credentials from the
// standard ALIBABA_CLOUD_* environment variables. Returns empty strings when
// the variables are not set.
func aliyunCredentials() (accessKeyID, secretAccessKey, securityToken string) {
	return os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_ID"),
		os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET"),
		os.Getenv("ALIBABA_CLOUD_SECURITY_TOKEN")
}

// isAliyunEndpoint reports whether endpoint points to an Aliyun OSS service.
func isAliyunEndpoint(endpoint string) bool {
	return len(endpoint) > 0 && containsAliyunDomain(endpoint)
}

func containsAliyunDomain(s string) bool {
	// Cover oss-*.aliyuncs.com and kms*.aliyuncs.com variants.
	for i := 0; i+len("aliyuncs.com") <= len(s); i++ {
		if s[i:i+len("aliyuncs.com")] == "aliyuncs.com" {
			return true
		}
	}
	return false
}

// rrsaCredentials detects whether ACK RRSA env vars are present and returns a
// refreshing aws.CredentialsProvider that exchanges the OIDC token for
// temporary Alibaba Cloud STS credentials via AssumeRoleWithOIDC.
// Returns nil, false when RRSA env vars are not set.
func rrsaCredentialsProvider() (aws.CredentialsProvider, bool) {
	roleARN := os.Getenv("ALIBABA_CLOUD_ROLE_ARN")
	oidcProviderARN := os.Getenv("ALIBABA_CLOUD_OIDC_PROVIDER_ARN")
	tokenFile := os.Getenv("ALIBABA_CLOUD_OIDC_TOKEN_FILE")
	if roleARN == "" || oidcProviderARN == "" || tokenFile == "" {
		return nil, false
	}
	return aws.NewCredentialsCache(&rrsaProvider{
		roleARN:         roleARN,
		oidcProviderARN: oidcProviderARN,
		tokenFile:       tokenFile,
	}), true
}

// rrsaProvider implements aws.CredentialsProvider using ACK RRSA.
type rrsaProvider struct {
	roleARN         string
	oidcProviderARN string
	tokenFile       string

	mu          sync.Mutex
	cached      aws.Credentials
	expiresAt   time.Time
}

func (p *rrsaProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if time.Now().Before(p.expiresAt.Add(-5 * time.Minute)) {
		return p.cached, nil
	}
	tokenBytes, err := os.ReadFile(p.tokenFile)
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("rrsa: read oidc token: %w", err)
	}
	cfg := alibabasdk.NewConfig().WithScheme("HTTPS")
	stsClient, err := sts.NewClientWithOptions("ap-southeast-1", cfg, nil)
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("rrsa: create sts client: %w", err)
	}
	req := sts.CreateAssumeRoleWithOIDCRequest()
	req.RoleArn = p.roleARN
	req.OIDCProviderArn = p.oidcProviderARN
	req.OIDCToken = string(tokenBytes)
	req.RoleSessionName = "drive9-rrsa"
	req.DurationSeconds = "3600"

	resp, err := stsClient.AssumeRoleWithOIDC(req)
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("rrsa: AssumeRoleWithOIDC: %w", err)
	}
	creds := resp.Credentials
	expiry, _ := time.Parse(time.RFC3339, creds.Expiration)
	p.cached = aws.Credentials{
		AccessKeyID:     creds.AccessKeyId,
		SecretAccessKey: creds.AccessKeySecret,
		SessionToken:    creds.SecurityToken,
		Source:          "AliyunRRSA",
		CanExpire:       true,
		Expires:         expiry,
	}
	p.expiresAt = expiry
	return p.cached, nil
}

// credentialsForAliyun resolves the Alibaba Cloud credential priority chain:
//  1. cfg.AccessKeyID (explicit static)
//  2. ACK RRSA: OIDC token file → STS AssumeRoleWithOIDC
//  3. ALIBABA_CLOUD_ACCESS_KEY_ID / SECRET env vars (static)
//
// Returns nil, nil when no credentials are configured; the SDK will fail on
// first use.
func credentialsForAliyun(cfg AWSConfig) (aws.CredentialsProvider, error) {
	if cfg.AccessKeyID != "" {
		return credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken), nil
	}
	if p, ok := rrsaCredentialsProvider(); ok {
		return p, nil
	}
	accessKeyID, secretAccessKey, sessionToken := aliyunCredentials()
	if accessKeyID != "" {
		return credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, sessionToken), nil
	}
	return nil, nil
}

// newAliyun builds an AWSS3Client using the Aliyun credential priority chain.
func newAliyun(ctx context.Context, cfg AWSConfig) (*AWSS3Client, error) {
	provider, err := credentialsForAliyun(cfg)
	if err != nil {
		return nil, err
	}
	return buildS3Client(ctx, cfg, provider)
}

