package s3client

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
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
		os.Getenv("TENCENTCLOUD_SECRET_KEY"),
		os.Getenv("TENCENTCLOUD_SECURITY_TOKEN")
}

func credentialsForTencent(cfg AWSConfig) (aws.CredentialsProvider, error) {
	if cfg.AccessKeyID != "" {
		return credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken), nil
	}
	accessKeyID, secretAccessKey, sessionToken := tencentCredentials()
	if accessKeyID != "" {
		if secretAccessKey == "" {
			return nil, fmt.Errorf("s3: TENCENTCLOUD_SECRET_ID is set but TENCENTCLOUD_SECRET_KEY is empty")
		}
		return credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, sessionToken), nil
	}
	return nil, nil
}

func newTencent(ctx context.Context, cfg AWSConfig) (*AWSS3Client, error) {
	provider, err := credentialsForTencent(cfg)
	if err != nil {
		return nil, err
	}
	return buildS3Client(ctx, cfg, provider)
}
