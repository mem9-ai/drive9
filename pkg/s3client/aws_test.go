package s3client

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestAWSConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     AWSConfig
		wantErr bool
	}{
		{
			name:    "missing bucket",
			cfg:     AWSConfig{},
			wantErr: true,
		},
		{
			name: "access key without secret",
			cfg: AWSConfig{
				Bucket:      "bucket",
				AccessKeyID: "ak",
			},
			wantErr: true,
		},
		{
			name: "secret without access key",
			cfg: AWSConfig{
				Bucket:          "bucket",
				SecretAccessKey: "sk",
			},
			wantErr: true,
		},
		{
			name: "session token without static credentials",
			cfg: AWSConfig{
				Bucket:       "bucket",
				SessionToken: "token",
			},
			wantErr: true,
		},
		{
			name: "static credentials",
			cfg: AWSConfig{
				Bucket:          "bucket",
				AccessKeyID:     "ak",
				SecretAccessKey: "sk",
			},
		},
		{
			name: "static credentials with session token",
			cfg: AWSConfig{
				Bucket:          "bucket",
				AccessKeyID:     "ak",
				SecretAccessKey: "sk",
				SessionToken:    "token",
			},
		},
		{
			name: "default credentials chain",
			cfg: AWSConfig{
				Bucket: "bucket",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStaticCredentialsProviderFromAWSConfig(t *testing.T) {
	provider, ok, err := staticCredentialsProvider(AWSConfig{
		AccessKeyID:     "test-ak",
		SecretAccessKey: "test-sk",
		SessionToken:    "test-token",
	})
	if err != nil {
		t.Fatalf("staticCredentialsProvider() error = %v", err)
	}
	if !ok {
		t.Fatal("staticCredentialsProvider() ok = false, want true")
	}

	creds, err := provider.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("Retrieve() error = %v", err)
	}
	if creds.AccessKeyID != "test-ak" {
		t.Fatalf("AccessKeyID = %q, want %q", creds.AccessKeyID, "test-ak")
	}
	if creds.SecretAccessKey != "test-sk" {
		t.Fatalf("SecretAccessKey = %q, want %q", creds.SecretAccessKey, "test-sk")
	}
	if creds.SessionToken != "test-token" {
		t.Fatalf("SessionToken = %q, want %q", creds.SessionToken, "test-token")
	}

	provider, ok, err = staticCredentialsProvider(AWSConfig{})
	if err != nil {
		t.Fatalf("staticCredentialsProvider(empty) error = %v", err)
	}
	if ok {
		t.Fatal("staticCredentialsProvider(empty) ok = true, want false")
	}
	if provider != nil {
		t.Fatalf("staticCredentialsProvider(empty) provider = %#v, want nil", provider)
	}
}

func TestS3LogValueHelpers(t *testing.T) {
	if got := CredentialLogValue("ak"); got != "static" {
		t.Fatalf("CredentialLogValue(static) = %q, want %q", got, "static")
	}
	if got := CredentialLogValue(""); got != "default-credentials" {
		t.Fatalf("CredentialLogValue(default) = %q, want %q", got, "default-credentials")
	}
	if got := RoleLogValue(""); got != "none" {
		t.Fatalf("RoleLogValue(empty) = %q, want %q", got, "none")
	}
	if got := RoleLogValue("arn:aws:iam::123456789012:role/test"); got != "arn:aws:iam::123456789012:role/test" {
		t.Fatalf("RoleLogValue(arn) = %q, want %q", got, "arn:aws:iam::123456789012:role/test")
	}
}

func TestApplyS3Options(t *testing.T) {
	opts := s3.Options{}
	applyS3Options(AWSConfig{
		Endpoint:       "https://minio.example:9000",
		ForcePathStyle: true,
	})(&opts)

	if opts.BaseEndpoint == nil {
		t.Fatal("BaseEndpoint = nil, want non-nil")
	}
	if got := *opts.BaseEndpoint; got != "https://minio.example:9000" {
		t.Fatalf("BaseEndpoint = %q, want %q", got, "https://minio.example:9000")
	}
	if !opts.UsePathStyle {
		t.Fatal("UsePathStyle = false, want true")
	}

	opts = s3.Options{}
	applyS3Options(AWSConfig{})(&opts)
	if opts.BaseEndpoint != nil {
		t.Fatal("BaseEndpoint != nil, want nil")
	}
	if opts.UsePathStyle {
		t.Fatal("UsePathStyle = true, want false")
	}
}
