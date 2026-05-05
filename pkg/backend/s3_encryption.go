package backend

import (
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/s3client"
)

func (b *Dat9Backend) s3WriteEncryption() (s3client.EncryptionOpts, datastore.StorageEncryptionMode, string) {
	policy := b.s3EncryptionPolicy
	context := map[string]string(nil)
	if b.tenantID != "" {
		context = map[string]string{"tenant_id": b.tenantID}
	}
	return policy.EncryptionOpts(context), datastore.StorageEncryptionMode(policy.Mode), policy.KMSKeyID
}

func (b *Dat9Backend) fileStorageEncryptionMode(storageType datastore.StorageType) datastore.StorageEncryptionMode {
	if storageType != datastore.StorageS3 {
		return datastore.StorageEncryptionNone
	}
	_, mode, _ := b.s3WriteEncryption()
	return mode
}

func (b *Dat9Backend) fileStorageEncryptionKeyID(storageType datastore.StorageType) string {
	if storageType != datastore.StorageS3 {
		return ""
	}
	_, _, keyID := b.s3WriteEncryption()
	return keyID
}
