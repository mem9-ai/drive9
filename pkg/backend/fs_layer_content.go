package backend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/pathutil"
	"go.uber.org/zap"
)

// PutFSLayerObject stores a layer file payload outside fs_layer_entries.content_blob.
func (b *Dat9Backend) PutFSLayerObject(ctx context.Context, layerID, path string, body io.Reader, size int64) (*datastore.FSLayerEntry, error) {
	if b == nil {
		return nil, fmt.Errorf("nil backend")
	}
	if b.s3 == nil {
		return nil, fmt.Errorf("s3 client not configured")
	}
	if err := b.ensureUploadSizeAllowed(size); err != nil {
		return nil, err
	}
	canonical, err := pathutil.Canonicalize(path)
	if err != nil {
		return nil, err
	}
	storageRef := "layers/" + layerID + "/" + b.genID()
	encOpts, encMode, encKeyID := b.s3WriteEncryption(storageRef)
	hasher := sha256.New()
	reader := io.TeeReader(body, hasher)
	if err := b.s3.PutObject(ctx, storageRef, reader, size, encOpts); err != nil {
		logger.Error(ctx, "backend_layer_put_object_failed", zap.String("layer_id", layerID), zap.String("path", canonical), zap.String("storage_ref", storageRef), zap.Int64("bytes", size), zap.Error(err))
		return nil, fmt.Errorf("put layer object: %w", err)
	}
	return &datastore.FSLayerEntry{
		Path:                   canonical,
		Op:                     datastore.FSLayerEntryOpUpsert,
		Kind:                   datastore.FSLayerEntryKindFile,
		StorageType:            string(datastore.StorageS3),
		StorageRef:             storageRef,
		StorageRefHash:         datastore.StorageRefHash(storageRef),
		StorageEncryptionMode:  encMode,
		StorageEncryptionKeyID: encKeyID,
		ChecksumSHA256:         hex.EncodeToString(hasher.Sum(nil)),
		SizeBytes:              size,
	}, nil
}

// ReadFSLayerEntryData materializes an fs-layer file entry from inline or object storage.
func (b *Dat9Backend) ReadFSLayerEntryData(ctx context.Context, entry *datastore.FSLayerEntry) ([]byte, error) {
	rc, err := b.OpenFSLayerEntryData(ctx, entry)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	return io.ReadAll(rc)
}

// OpenFSLayerEntryData opens an fs-layer file entry for streaming reads.
func (b *Dat9Backend) OpenFSLayerEntryData(ctx context.Context, entry *datastore.FSLayerEntry) (io.ReadCloser, error) {
	if entry == nil {
		return nil, fmt.Errorf("nil fs layer entry")
	}
	if entry.StorageType == string(datastore.StorageS3) || entry.StorageRef != "" {
		if b.s3 == nil {
			return nil, fmt.Errorf("s3 client not configured")
		}
		rc, err := b.s3.GetObject(ctx, entry.StorageRef)
		if err != nil {
			return nil, fmt.Errorf("read layer object %s: %w", entry.StorageRef, err)
		}
		return rc, nil
	}
	return io.NopCloser(bytes.NewReader(entry.ContentBlob)), nil
}

// WriteStoredObjectCtxIfRevision applies an already-stored layer payload to the
// base filesystem without routing the bytes through content_blob.
func (b *Dat9Backend) WriteStoredObjectCtxIfRevision(ctx context.Context, path string, entry *datastore.FSLayerEntry) (int64, error) {
	if entry == nil {
		return 0, fmt.Errorf("nil fs layer entry")
	}
	canonical, err := pathutil.Canonicalize(path)
	if err != nil {
		return 0, err
	}
	if err := rejectRootFileNodePath(canonical); err != nil {
		return 0, err
	}
	if entry.StorageRef == "" && (entry.StorageType == "" || entry.StorageType == string(datastore.StorageDB9)) {
		_, rev, err := b.WriteCtxIfRevisionWithTagsResult(ctx, canonical, entry.ContentBlob, 0, filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate, entry.BaseRevision, nil, "")
		return rev, err
	}
	if entry.StorageType == "" {
		return 0, fmt.Errorf("missing fs layer storage_type for external payload %s", entry.Path)
	}
	if entry.StorageType != string(datastore.StorageS3) {
		return 0, fmt.Errorf("unsupported fs layer storage type %q", entry.StorageType)
	}
	if entry.StorageRef == "" {
		return 0, fmt.Errorf("missing fs layer storage_ref for %s", entry.Path)
	}
	if err := b.ensureUploadSizeAllowed(entry.SizeBytes); err != nil {
		return 0, err
	}
	storageEncryptionMode := entry.StorageEncryptionMode
	if storageEncryptionMode == "" {
		storageEncryptionMode = datastore.StorageEncryptionNone
	}
	storageEncryptionKeyID := entry.StorageEncryptionKeyID

	contentType := detectContentType(canonical, nil)
	now := time.Now()
	nf, statErr := b.store.Stat(ctx, canonical)
	if errorsIsNotFound(statErr) {
		fileID := b.genID()
		var quotaOutboxEnqueued bool
		err := b.store.InTx(ctx, func(tx *sql.Tx) error {
			if err := b.ensureStorageQuota(ctx, tx, canonical, entry.SizeBytes); err != nil {
				return err
			}
			if err := b.store.InsertFileTx(tx, &datastore.File{
				FileID:                 fileID,
				StorageType:            datastore.StorageType(entry.StorageType),
				StorageRef:             entry.StorageRef,
				StorageEncryptionMode:  storageEncryptionMode,
				StorageEncryptionKeyID: storageEncryptionKeyID,
				ContentType:            contentType,
				SizeBytes:              entry.SizeBytes,
				ChecksumSHA256:         entry.ChecksumSHA256,
				Revision:               1,
				Status:                 datastore.StatusConfirmed,
				CreatedAt:              now,
				ConfirmedAt:            &now,
			}); err != nil {
				return err
			}
			if err := b.store.EnsureParentDirsTx(tx, canonical, b.genID); err != nil {
				return err
			}
			if err := b.store.InsertNodeTx(tx, &datastore.FileNode{
				NodeID:     b.genID(),
				Path:       canonical,
				ParentPath: pathutil.ParentPath(canonical),
				Name:       pathutil.BaseName(canonical),
				FileID:     fileID,
				CreatedAt:  now,
			}); err != nil {
				return err
			}
			created, err := b.enqueueQuotaFileCreateOutboxTx(tx, fileID, entry.SizeBytes, contentType)
			if err != nil {
				return err
			}
			quotaOutboxEnqueued = created
			return nil
		})
		if err != nil {
			return 0, err
		}
		if quotaOutboxEnqueued {
			b.notifyQuotaOutbox(true)
		} else {
			b.syncCentralFileCreate(ctx, fileID, entry.SizeBytes, contentType)
		}
		return 1, nil
	}
	if statErr != nil {
		return 0, statErr
	}
	if nf.File == nil {
		return 0, fmt.Errorf("no file entity")
	}
	if nf.Node.IsDirectory {
		return 0, datastore.ErrPathConflict
	}

	oldStorageType := nf.File.StorageType
	oldStorageRef := nf.File.StorageRef
	oldSize := nf.File.SizeBytes
	oldContentType := nf.File.ContentType
	var newRev int64
	var quotaOutboxEnqueued bool
	err = b.store.InTx(ctx, func(tx *sql.Tx) error {
		currentMeta, err := b.store.GetFileStorageMetaForUpdateTx(tx, nf.File.FileID)
		if err != nil {
			return err
		}
		oldStorageType = currentMeta.StorageType
		oldStorageRef = currentMeta.StorageRef
		oldSize = currentMeta.SizeBytes
		oldContentType = currentMeta.ContentType
		if entry.BaseRevision > 0 && currentMeta.Revision != entry.BaseRevision {
			return datastore.ErrRevisionConflict
		}
		if b.UseServerQuota() {
			if deltaBytes := entry.SizeBytes - currentMeta.SizeBytes; deltaBytes > 0 {
				if err := b.ensureStorageQuotaServer(ctx, tx, deltaBytes); err != nil {
					return err
				}
			}
		} else if err := b.ensureStorageQuota(ctx, tx, canonical, entry.SizeBytes); err != nil {
			return err
		}
		var txErr error
		if b.UsesDatabaseAutoEmbedding() {
			if entry.BaseRevision > 0 {
				newRev, txErr = b.store.UpdateFileContentAutoEmbeddingIfRevisionTx(tx, nf.File.FileID, entry.BaseRevision, datastore.StorageType(entry.StorageType), entry.StorageRef, contentType, entry.ChecksumSHA256, "", nil, entry.SizeBytes, "")
			} else {
				newRev, txErr = b.store.UpdateFileContentAutoEmbeddingTx(tx, nf.File.FileID, datastore.StorageType(entry.StorageType), entry.StorageRef, contentType, entry.ChecksumSHA256, "", nil, entry.SizeBytes, "")
			}
		} else {
			if entry.BaseRevision > 0 {
				newRev, txErr = b.store.UpdateFileContentIfRevisionTx(tx, nf.File.FileID, entry.BaseRevision, datastore.StorageType(entry.StorageType), entry.StorageRef, contentType, entry.ChecksumSHA256, "", nil, entry.SizeBytes, "")
			} else {
				newRev, txErr = b.store.UpdateFileContentTx(tx, nf.File.FileID, datastore.StorageType(entry.StorageType), entry.StorageRef, contentType, entry.ChecksumSHA256, "", nil, entry.SizeBytes, "")
			}
		}
		if txErr != nil {
			return txErr
		}
		if err := b.store.UpdateFileStorageEncryptionTx(tx, nf.File.FileID, storageEncryptionMode, storageEncryptionKeyID); err != nil {
			return err
		}
		created, err := b.enqueueQuotaFileOverwriteOutboxTx(tx, nf.File.FileID, oldSize, oldContentType, entry.SizeBytes, contentType)
		if err != nil {
			return err
		}
		quotaOutboxEnqueued = created
		return nil
	})
	if err != nil {
		return 0, err
	}
	if quotaOutboxEnqueued {
		b.notifyQuotaOutbox(true)
	} else {
		b.syncCentralFileOverwrite(ctx, nf.File.FileID, oldSize, oldContentType, entry.SizeBytes, contentType)
	}
	b.deleteBlobIfS3Ctx(ctx, oldStorageType, oldStorageRef, entry.StorageRef)
	return newRev, nil
}

func errorsIsNotFound(err error) bool {
	return errors.Is(err, datastore.ErrNotFound)
}
