package meta

import (
	"fmt"

	"github.com/mem9-ai/dat9/pkg/s3client"
)

type S3EncryptionMode string

const (
	S3EncryptionModeInherit S3EncryptionMode = "inherit"
	S3EncryptionModeNone    S3EncryptionMode = S3EncryptionMode(s3client.EncryptionModeNone)
	S3EncryptionModeSSES3   S3EncryptionMode = S3EncryptionMode(s3client.EncryptionModeSSES3)
	S3EncryptionModeSSEKMS  S3EncryptionMode = S3EncryptionMode(s3client.EncryptionModeSSEKMS)
	S3EncryptionModeDSSEKMS S3EncryptionMode = S3EncryptionMode(s3client.EncryptionModeDSSEKMS)
)

type S3EncryptionPolicy struct {
	Mode             S3EncryptionMode
	KMSKeyID         string
	BucketKeyEnabled bool
}

type ResolvedS3EncryptionPolicy struct {
	Mode             s3client.EncryptionMode
	KMSKeyID         string
	BucketKeyEnabled bool
}

func DefaultS3EncryptionPolicy() S3EncryptionPolicy {
	return S3EncryptionPolicy{
		Mode:             S3EncryptionModeNone,
		BucketKeyEnabled: true,
	}
}

func ResolveS3EncryptionPolicy(global S3EncryptionPolicy, tenant S3EncryptionPolicy) (ResolvedS3EncryptionPolicy, error) {
	if err := ValidateGlobalS3EncryptionPolicy(global); err != nil {
		return ResolvedS3EncryptionPolicy{}, err
	}
	if err := ValidateTenantS3EncryptionPolicyP1(tenant); err != nil {
		return ResolvedS3EncryptionPolicy{}, err
	}

	global = normalizeGlobalS3EncryptionPolicy(global)
	tenant = normalizeTenantS3EncryptionPolicy(tenant)
	source := global
	if tenant.Mode != S3EncryptionModeInherit {
		source = tenant
		source.KMSKeyID = global.KMSKeyID
	}

	resolved := ResolvedS3EncryptionPolicy{
		Mode:             s3client.EncryptionMode(source.Mode),
		KMSKeyID:         source.KMSKeyID,
		BucketKeyEnabled: source.BucketKeyEnabled,
	}
	switch resolved.Mode {
	case s3client.EncryptionModeNone, s3client.EncryptionModeSSES3:
		resolved.KMSKeyID = ""
		resolved.BucketKeyEnabled = false
	case s3client.EncryptionModeSSEKMS:
		if resolved.KMSKeyID == "" {
			return ResolvedS3EncryptionPolicy{}, fmt.Errorf("sse-kms encryption requires KMS key ID")
		}
	case s3client.EncryptionModeDSSEKMS:
		if resolved.KMSKeyID == "" {
			return ResolvedS3EncryptionPolicy{}, fmt.Errorf("dsse-kms encryption requires KMS key ID")
		}
		if resolved.BucketKeyEnabled {
			return ResolvedS3EncryptionPolicy{}, fmt.Errorf("bucket key is not supported for dsse-kms encryption")
		}
	default:
		return ResolvedS3EncryptionPolicy{}, fmt.Errorf("unsupported resolved S3 encryption mode %q", resolved.Mode)
	}
	return resolved, nil
}

func (p ResolvedS3EncryptionPolicy) EncryptionOpts(context map[string]string) s3client.EncryptionOpts {
	opts := s3client.EncryptionOpts{
		Mode:             p.Mode,
		KMSKeyID:         p.KMSKeyID,
		BucketKeyEnabled: p.BucketKeyEnabled,
	}
	if p.Mode == s3client.EncryptionModeSSEKMS || p.Mode == s3client.EncryptionModeDSSEKMS {
		opts.EncryptionContext = context
	}
	return opts
}

func ValidateGlobalS3EncryptionPolicy(policy S3EncryptionPolicy) error {
	policy = normalizeGlobalS3EncryptionPolicy(policy)
	switch policy.Mode {
	case S3EncryptionModeNone, S3EncryptionModeSSES3:
		return nil
	case S3EncryptionModeSSEKMS, S3EncryptionModeDSSEKMS:
		if policy.KMSKeyID == "" {
			return fmt.Errorf("%s encryption requires KMS key ID", policy.Mode)
		}
		if policy.Mode == S3EncryptionModeDSSEKMS && policy.BucketKeyEnabled {
			return fmt.Errorf("bucket key is not supported for dsse-kms encryption")
		}
		return nil
	default:
		return fmt.Errorf("unsupported global S3 encryption mode %q", policy.Mode)
	}
}

func ValidateTenantS3EncryptionPolicyP1(policy S3EncryptionPolicy) error {
	policy = normalizeTenantS3EncryptionPolicy(policy)
	if policy.KMSKeyID != "" {
		return fmt.Errorf("tenant-specific S3 KMS key ID is not supported in P1")
	}
	switch policy.Mode {
	case S3EncryptionModeInherit, S3EncryptionModeNone, S3EncryptionModeSSES3, S3EncryptionModeSSEKMS, S3EncryptionModeDSSEKMS:
		return nil
	default:
		return fmt.Errorf("unsupported tenant S3 encryption mode %q", policy.Mode)
	}
}

func normalizeGlobalS3EncryptionPolicy(policy S3EncryptionPolicy) S3EncryptionPolicy {
	if policy.Mode == "" {
		policy.Mode = S3EncryptionModeNone
	}
	return policy
}

func normalizeTenantS3EncryptionPolicy(policy S3EncryptionPolicy) S3EncryptionPolicy {
	if policy.Mode == "" {
		policy.Mode = S3EncryptionModeInherit
	}
	return policy
}
