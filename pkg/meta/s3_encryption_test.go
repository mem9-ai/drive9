package meta

import (
	"strings"
	"testing"

	"github.com/mem9-ai/dat9/pkg/s3client"
)

func TestResolveS3EncryptionPolicyInheritGlobalSSEKMS(t *testing.T) {
	resolved, err := ResolveS3EncryptionPolicy(
		S3EncryptionPolicy{
			Mode:             S3EncryptionModeSSEKMS,
			KMSKeyID:         "arn:aws:kms:ap-southeast-1:123456789012:key/test",
			BucketKeyEnabled: true,
		},
		S3EncryptionPolicy{Mode: S3EncryptionModeInherit},
	)
	if err != nil {
		t.Fatalf("ResolveS3EncryptionPolicy error = %v", err)
	}
	if resolved.Mode != s3client.EncryptionModeSSEKMS {
		t.Fatalf("mode = %q, want sse-kms", resolved.Mode)
	}
	if resolved.KMSKeyID != "arn:aws:kms:ap-southeast-1:123456789012:key/test" {
		t.Fatalf("kms key = %q", resolved.KMSKeyID)
	}
	if !resolved.BucketKeyEnabled {
		t.Fatal("bucket key enabled = false, want true")
	}

	opts := resolved.EncryptionOpts(map[string]string{"tenant_id": "tenant-1"})
	if opts.Mode != s3client.EncryptionModeSSEKMS || opts.KMSKeyID != resolved.KMSKeyID {
		t.Fatalf("unexpected opts: %+v", opts)
	}
	if opts.EncryptionContext["tenant_id"] != "tenant-1" {
		t.Fatalf("context = %#v", opts.EncryptionContext)
	}
}

func TestResolveS3EncryptionPolicyZeroTenantFallsBackToGlobal(t *testing.T) {
	resolved, err := ResolveS3EncryptionPolicy(
		S3EncryptionPolicy{
			Mode:             S3EncryptionModeSSEKMS,
			KMSKeyID:         "arn:aws:kms:ap-southeast-1:123456789012:key/test",
			BucketKeyEnabled: true,
		},
		S3EncryptionPolicy{},
	)
	if err != nil {
		t.Fatalf("ResolveS3EncryptionPolicy error = %v", err)
	}
	if resolved.Mode != s3client.EncryptionModeSSEKMS {
		t.Fatalf("mode = %q, want sse-kms", resolved.Mode)
	}
}

func TestResolveS3EncryptionPolicyInheritGlobalNone(t *testing.T) {
	resolved, err := ResolveS3EncryptionPolicy(
		S3EncryptionPolicy{Mode: S3EncryptionModeNone, BucketKeyEnabled: true},
		S3EncryptionPolicy{Mode: S3EncryptionModeInherit},
	)
	if err != nil {
		t.Fatalf("ResolveS3EncryptionPolicy error = %v", err)
	}
	if resolved.Mode != s3client.EncryptionModeNone {
		t.Fatalf("mode = %q, want none", resolved.Mode)
	}
	if resolved.KMSKeyID != "" {
		t.Fatalf("kms key = %q, want empty", resolved.KMSKeyID)
	}
	if resolved.BucketKeyEnabled {
		t.Fatal("bucket key enabled = true, want false for none")
	}
	opts := resolved.EncryptionOpts(map[string]string{"tenant_id": "tenant-1"})
	if len(opts.EncryptionContext) != 0 {
		t.Fatalf("non-KMS opts context = %#v, want empty", opts.EncryptionContext)
	}
}

func TestResolveS3EncryptionPolicyTenantExplicitNoneOverridesGlobal(t *testing.T) {
	resolved, err := ResolveS3EncryptionPolicy(
		S3EncryptionPolicy{
			Mode:             S3EncryptionModeSSEKMS,
			KMSKeyID:         "arn:aws:kms:ap-southeast-1:123456789012:key/test",
			BucketKeyEnabled: true,
		},
		S3EncryptionPolicy{Mode: S3EncryptionModeNone, BucketKeyEnabled: true},
	)
	if err != nil {
		t.Fatalf("ResolveS3EncryptionPolicy error = %v", err)
	}
	if resolved.Mode != s3client.EncryptionModeNone {
		t.Fatalf("mode = %q, want none", resolved.Mode)
	}
	if resolved.KMSKeyID != "" || resolved.BucketKeyEnabled {
		t.Fatalf("unexpected resolved non-encryption fields: %+v", resolved)
	}
}

func TestResolveS3EncryptionPolicySSES3OptsExcludeContext(t *testing.T) {
	resolved, err := ResolveS3EncryptionPolicy(
		S3EncryptionPolicy{Mode: S3EncryptionModeNone},
		S3EncryptionPolicy{Mode: S3EncryptionModeSSES3},
	)
	if err != nil {
		t.Fatalf("ResolveS3EncryptionPolicy error = %v", err)
	}
	opts := resolved.EncryptionOpts(map[string]string{"tenant_id": "tenant-1"})
	if opts.Mode != s3client.EncryptionModeSSES3 {
		t.Fatalf("mode = %q, want sse-s3", opts.Mode)
	}
	if len(opts.EncryptionContext) != 0 {
		t.Fatalf("sse-s3 opts context = %#v, want empty", opts.EncryptionContext)
	}
}

func TestResolveS3EncryptionPolicyTenantExplicitSSEKMSUsesGlobalKey(t *testing.T) {
	resolved, err := ResolveS3EncryptionPolicy(
		S3EncryptionPolicy{
			Mode:             S3EncryptionModeNone,
			KMSKeyID:         "arn:aws:kms:ap-southeast-1:123456789012:key/test",
			BucketKeyEnabled: true,
		},
		S3EncryptionPolicy{Mode: S3EncryptionModeSSEKMS, BucketKeyEnabled: false},
	)
	if err != nil {
		t.Fatalf("ResolveS3EncryptionPolicy error = %v", err)
	}
	if resolved.Mode != s3client.EncryptionModeSSEKMS {
		t.Fatalf("mode = %q, want sse-kms", resolved.Mode)
	}
	if resolved.KMSKeyID != "arn:aws:kms:ap-southeast-1:123456789012:key/test" {
		t.Fatalf("kms key = %q", resolved.KMSKeyID)
	}
	if resolved.BucketKeyEnabled {
		t.Fatal("bucket key enabled = true, want explicit tenant false")
	}
}

func TestResolveS3EncryptionPolicyTenantSSEKMSRequiresGlobalKey(t *testing.T) {
	_, err := ResolveS3EncryptionPolicy(
		S3EncryptionPolicy{Mode: S3EncryptionModeNone},
		S3EncryptionPolicy{Mode: S3EncryptionModeSSEKMS},
	)
	if err == nil {
		t.Fatal("ResolveS3EncryptionPolicy error = nil, want error")
	}
	if !strings.Contains(err.Error(), "requires KMS key ID") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateGlobalS3EncryptionPolicyRejectsMissingKMSKey(t *testing.T) {
	err := ValidateGlobalS3EncryptionPolicy(S3EncryptionPolicy{Mode: S3EncryptionModeSSEKMS})
	if err == nil {
		t.Fatal("ValidateGlobalS3EncryptionPolicy error = nil, want error")
	}
	if !strings.Contains(err.Error(), "requires KMS key ID") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateTenantS3EncryptionPolicyP1RejectsTenantKMSKey(t *testing.T) {
	err := ValidateTenantS3EncryptionPolicyP1(S3EncryptionPolicy{
		Mode:     S3EncryptionModeSSEKMS,
		KMSKeyID: "arn:aws:kms:ap-southeast-1:123456789012:key/tenant",
	})
	if err == nil {
		t.Fatal("ValidateTenantS3EncryptionPolicyP1 error = nil, want error")
	}
	if !strings.Contains(err.Error(), "tenant-specific") {
		t.Fatalf("unexpected error: %v", err)
	}
}
