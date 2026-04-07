package backend

import (
	"database/sql"
	"errors"
	"fmt"
)

var (
	ErrUploadTooLarge       = errors.New("upload too large")
	ErrStorageQuotaExceeded = errors.New("tenant storage quota exceeded")
)

func (b *Dat9Backend) ensureUploadSizeAllowed(size int64) error {
	if size <= 0 || b.maxUploadBytes <= 0 {
		return nil
	}
	if size > b.maxUploadBytes {
		return fmt.Errorf("%w: max %d bytes", ErrUploadTooLarge, b.maxUploadBytes)
	}
	return nil
}

func (b *Dat9Backend) ensureTenantStorageQuotaTx(tx *sql.Tx, path string, newSize int64) error {
	if newSize <= 0 || b.maxTenantStorageBytes <= 0 {
		return nil
	}
	confirmedBytes, err := b.store.ConfirmedStorageBytesTx(tx)
	if err != nil {
		return fmt.Errorf("load confirmed storage usage: %w", err)
	}
	reservedBytes, err := b.store.ActiveUploadReservedBytesTx(tx)
	if err != nil {
		return fmt.Errorf("load upload reservations: %w", err)
	}
	currentPathBytes, err := b.store.ConfirmedFileSizeByPathTx(tx, path)
	if err != nil {
		return fmt.Errorf("load current file size: %w", err)
	}
	deltaBytes := newSize - currentPathBytes
	if deltaBytes < 0 {
		deltaBytes = 0
	}
	totalBytes := confirmedBytes + reservedBytes + deltaBytes
	if totalBytes > b.maxTenantStorageBytes {
		return fmt.Errorf("%w: limit=%d used=%d reserved=%d current_path=%d requested=%d delta=%d",
			ErrStorageQuotaExceeded, b.maxTenantStorageBytes, confirmedBytes, reservedBytes,
			currentPathBytes, newSize, deltaBytes)
	}
	return nil
}
