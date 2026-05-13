package backend

import (
	"context"
	"fmt"
	"os"
)

// HasConfirmedS3StorageRef checks the current tenant branch for an exact
// confirmed S3 object reference. The hash is used only as an indexed prefilter.
func (b *Dat9Backend) HasConfirmedS3StorageRef(ctx context.Context, storageRefHash, storageRef string) (bool, error) {
	if b == nil || b.store == nil {
		return false, fmt.Errorf("backend store is not configured")
	}
	return b.store.HasConfirmedS3StorageRef(ctx, storageRefHash, storageRef)
}

// DeleteS3ObjectForGC is the only exported physical S3 delete path for
// confirmed blob lifecycle cleanup.
func (b *Dat9Backend) DeleteS3ObjectForGC(ctx context.Context, storageRef string) error {
	if b == nil || b.s3 == nil {
		return fmt.Errorf("s3 client is not configured")
	}
	if storageRef == "" {
		return nil
	}
	if err := b.s3.DeleteObject(ctx, storageRef); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
