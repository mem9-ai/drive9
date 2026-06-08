package s3client

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func TestIsAliyunEndpoint(t *testing.T) {
	tests := []struct {
		endpoint string
		want     bool
	}{
		{"https://oss-cn-hangzhou.aliyuncs.com", true},
		{"https://oss-ap-southeast-1.aliyuncs.com", true},
		{"https://kms.cn-hangzhou.aliyuncs.com", true},
		{"http://oss-cn-beijing.aliyuncs.com", true},
		{"https://s3.amazonaws.com", false},
		{"https://s3.us-east-1.amazonaws.com", false},
		{"https://minio.example.com:9000", false},
		{"", false},
	}
	for _, tt := range tests {
		if got := isAliyunEndpoint(tt.endpoint); got != tt.want {
			t.Errorf("isAliyunEndpoint(%q) = %v, want %v", tt.endpoint, got, tt.want)
		}
	}
}

func TestCredentialsForAliyunEnvFallback(t *testing.T) {
	// Ensure RRSA env vars are absent so RRSA is skipped.
	t.Setenv("ALIBABA_CLOUD_ROLE_ARN", "")
	t.Setenv("ALIBABA_CLOUD_OIDC_PROVIDER_ARN", "")
	t.Setenv("ALIBABA_CLOUD_OIDC_TOKEN_FILE", "")
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "env-ak")
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "env-sk")
	t.Setenv("ALIBABA_CLOUD_SECURITY_TOKEN", "env-token")

	provider, err := credentialsForAliyun(AWSConfig{})
	if err != nil {
		t.Fatalf("credentialsForAliyun() error = %v", err)
	}
	if provider == nil {
		t.Fatal("credentialsForAliyun() provider = nil, want non-nil")
	}
	creds, err := provider.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("Retrieve() error = %v", err)
	}
	if creds.AccessKeyID != "env-ak" {
		t.Errorf("AccessKeyID = %q, want %q", creds.AccessKeyID, "env-ak")
	}
	if creds.SecretAccessKey != "env-sk" {
		t.Errorf("SecretAccessKey = %q, want %q", creds.SecretAccessKey, "env-sk")
	}
	if creds.SessionToken != "env-token" {
		t.Errorf("SessionToken = %q, want %q", creds.SessionToken, "env-token")
	}
}

func TestCredentialsForAliyunRRSAPriority(t *testing.T) {
	// Write a dummy token file so rrsaCredentialsProvider detects RRSA.
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenFile, []byte("dummy-token"), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("ALIBABA_CLOUD_ROLE_ARN", "acs:ram::123456789:role/test")
	t.Setenv("ALIBABA_CLOUD_OIDC_PROVIDER_ARN", "acs:ram::123456789:oidc-provider/test")
	t.Setenv("ALIBABA_CLOUD_OIDC_TOKEN_FILE", tokenFile)
	// Also set env key to confirm RRSA takes priority over it.
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "env-ak")
	t.Setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "env-sk")

	// Pass region explicitly so the RRSA provider can construct the STS client.
	provider, err := credentialsForAliyun(AWSConfig{Region: "cn-hangzhou"})
	if err != nil {
		t.Fatalf("credentialsForAliyun() error = %v", err)
	}
	if provider == nil {
		t.Fatal("credentialsForAliyun() provider = nil, want non-nil")
	}

	// Verify the returned provider is *rrsaProvider (not a static credentials provider),
	// confirming that RRSA takes priority over the ALIBABA_CLOUD_ACCESS_KEY_* env vars.
	if _, ok := provider.(*rrsaProvider); !ok {
		t.Fatalf("credentialsForAliyun() provider type = %T, want *rrsaProvider", provider)
	}
}

func TestRRSAProviderCacheHit(t *testing.T) {
	future := time.Now().Add(30 * time.Minute)
	p := &rrsaProvider{
		region:          "cn-hangzhou",
		roleARN:         "arn",
		oidcProviderARN: "parn",
		tokenFile:       "/nonexistent",
		expiresAt:       future,
		cached: aws.Credentials{
			AccessKeyID:     "cached-ak",
			SecretAccessKey: "cached-sk",
			SessionToken:    "cached-token",
			Source:          "AliyunRRSA",
			CanExpire:       true,
			Expires:         future,
		},
	}

	creds, err := p.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("Retrieve() error = %v", err)
	}
	if creds.AccessKeyID != "cached-ak" {
		t.Errorf("AccessKeyID = %q, want %q", creds.AccessKeyID, "cached-ak")
	}
	if creds.Source != "AliyunRRSA" {
		t.Errorf("Source = %q, want %q", creds.Source, "AliyunRRSA")
	}
}

func TestRRSAProviderTokenFileNotFound(t *testing.T) {
	p := &rrsaProvider{
		region:          "cn-hangzhou",
		roleARN:         "arn",
		oidcProviderARN: "parn",
		tokenFile:       "/nonexistent/oidc-token",
	}

	_, err := p.Retrieve(context.Background())
	if err == nil {
		t.Fatal("Retrieve() error = nil, want error for missing token file")
	}
}
