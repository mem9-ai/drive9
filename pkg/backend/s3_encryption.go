package backend

import (
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/s3client"
)

func (b *Dat9Backend) s3WriteEncryption(objectKey string) (s3client.EncryptionOpts, datastore.StorageEncryptionMode, string) {
	policy := b.s3EncryptionPolicy
	context := map[string]string(nil)
	if b.tenantID != "" {
		context = map[string]string{"tenant_id": b.tenantID}
	}
	if objectKey != "" {
		if context == nil {
			context = map[string]string{}
		}
		context["object_key"] = objectKey
	}
	return policy.EncryptionOpts(context), storageEncryptionModeFromS3(policy.Mode), policy.KMSKeyID
}

// ExtentObjectEncryption reports the resolved per-object SSE policy this
// tenant's uploads use. The extent data plane cannot honour it: its blocks are
// written by JuiceFS's S3 backend, which has no option to set SSE headers, so
// the data-credential mint refuses instead of silently writing unencrypted
// blocks on a deployment that requires them.
func (b *Dat9Backend) ExtentObjectEncryption() (s3client.EncryptionMode, string) {
	if b == nil {
		return s3client.EncryptionModeNone, ""
	}
	return b.s3EncryptionPolicy.Mode, b.s3EncryptionPolicy.KMSKeyID
}

func storageEncryptionModeFromS3(mode s3client.EncryptionMode) datastore.StorageEncryptionMode {
	switch mode {
	case s3client.EncryptionModeLegacy:
		return datastore.StorageEncryptionLegacy
	case s3client.EncryptionModeNone:
		return datastore.StorageEncryptionNone
	case s3client.EncryptionModeSSES3:
		return datastore.StorageEncryptionSSES3
	case s3client.EncryptionModeSSEKMS:
		return datastore.StorageEncryptionSSEKMS
	case s3client.EncryptionModeDSSEKMS:
		return datastore.StorageEncryptionDSSEKMS
	default:
		return datastore.StorageEncryptionNone
	}
}
