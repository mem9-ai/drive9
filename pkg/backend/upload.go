package backend

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/pathutil"
	"github.com/mem9-ai/dat9/pkg/s3client"
)

// UploadPlan is returned by InitiateUpload for the 202 response.
type UploadPlan struct {
	UploadID string                 `json:"upload_id"`
	Key      string                 `json:"key"`
	Parts    []*s3client.UploadPartURL `json:"parts"`
}

// S3 returns the S3Client (nil when not configured).
func (b *Dat9Backend) S3() s3client.S3Client { return b.s3 }

// IsLargeFile returns true if the given size exceeds the small file threshold
// and S3 is configured.
func (b *Dat9Backend) IsLargeFile(size int64) bool {
	return b.s3 != nil && size >= smallFileThreshold
}

// InitiateUpload creates a multipart upload for a large file.
// Returns an UploadPlan with presigned URLs for all parts.
func (b *Dat9Backend) InitiateUpload(ctx context.Context, path string, totalSize int64) (*UploadPlan, error) {
	path, err := pathutil.Canonicalize(path)
	if err != nil {
		return nil, err
	}
	if b.s3 == nil {
		return nil, fmt.Errorf("S3 not configured")
	}

	fileID := b.genID()
	s3Key := "blobs/" + fileID

	// Create S3 multipart upload
	mpu, err := b.s3.CreateMultipartUpload(ctx, s3Key)
	if err != nil {
		return nil, fmt.Errorf("create multipart upload: %w", err)
	}

	// Calculate parts
	parts := s3client.CalcParts(totalSize, s3client.PartSize)

	// Presign all part URLs
	urls := make([]*s3client.UploadPartURL, len(parts))
	for i, p := range parts {
		u, err := b.s3.PresignUploadPart(ctx, s3Key, mpu.UploadID, p.Number, s3client.UploadTTL)
		if err != nil {
			b.s3.AbortMultipartUpload(ctx, s3Key, mpu.UploadID)
			return nil, fmt.Errorf("presign part %d: %w", p.Number, err)
		}
		u.Size = p.Size
		urls[i] = u
	}

	now := time.Now()
	uploadID := b.genID()

	// Insert PENDING file record
	if err := b.store.InsertFile(&meta.File{
		FileID:      fileID,
		StorageType: meta.StorageS3,
		StorageRef:  s3Key,
		SizeBytes:   totalSize,
		Revision:    1,
		Status:      meta.StatusPending,
		CreatedAt:   now,
	}); err != nil {
		b.s3.AbortMultipartUpload(ctx, s3Key, mpu.UploadID)
		return nil, err
	}

	// Insert upload record
	if err := b.store.InsertUpload(&meta.Upload{
		UploadID:   uploadID,
		FileID:     fileID,
		TargetPath: path,
		S3UploadID: mpu.UploadID,
		S3Key:      s3Key,
		TotalSize:  totalSize,
		PartSize:   s3client.PartSize,
		PartsTotal: len(parts),
		Status:     meta.UploadUploading,
		CreatedAt:  now,
		UpdatedAt:  now,
		ExpiresAt:  now.Add(24 * time.Hour),
	}); err != nil {
		b.s3.AbortMultipartUpload(ctx, s3Key, mpu.UploadID)
		return nil, err
	}

	return &UploadPlan{
		UploadID: uploadID,
		Key:      s3Key,
		Parts:    urls,
	}, nil
}

// ConfirmUpload completes the multipart upload and creates the file node.
func (b *Dat9Backend) ConfirmUpload(ctx context.Context, uploadID string) error {
	upload, err := b.store.GetUpload(uploadID)
	if err != nil {
		return err
	}
	if upload.Status != meta.UploadUploading {
		return meta.ErrUploadNotActive
	}

	// List uploaded parts from S3
	parts, err := b.s3.ListParts(ctx, upload.S3Key, upload.S3UploadID)
	if err != nil {
		return fmt.Errorf("list parts: %w", err)
	}

	// Complete S3 multipart upload (idempotent, outside transaction)
	if err := b.s3.CompleteMultipartUpload(ctx, upload.S3Key, upload.S3UploadID, parts); err != nil {
		return fmt.Errorf("complete multipart: %w", err)
	}

	// Atomically: confirm file, complete upload, ensure parents, create node
	return b.store.InTx(func(tx *sql.Tx) error {
		if err := b.store.ConfirmFileTx(tx, upload.FileID); err != nil {
			return err
		}
		if err := b.store.CompleteUploadTx(tx, uploadID); err != nil {
			return err
		}
		if err := b.store.EnsureParentDirsTx(tx, upload.TargetPath, b.genID); err != nil {
			return err
		}
		return b.store.InsertNodeTx(tx, &meta.FileNode{
			NodeID:     b.genID(),
			Path:       upload.TargetPath,
			ParentPath: pathutil.ParentPath(upload.TargetPath),
			Name:       pathutil.BaseName(upload.TargetPath),
			FileID:     upload.FileID,
			CreatedAt:  time.Now(),
		})
	})
}

// ResumeUpload returns presigned URLs for the missing parts of an in-progress upload.
func (b *Dat9Backend) ResumeUpload(ctx context.Context, uploadID string) (*UploadPlan, error) {
	upload, err := b.store.GetUpload(uploadID)
	if err != nil {
		return nil, err
	}
	if upload.Status != meta.UploadUploading {
		return nil, meta.ErrUploadNotActive
	}

	// Check expiry
	if time.Now().After(upload.ExpiresAt) {
		b.store.AbortUpload(uploadID)
		return nil, meta.ErrUploadExpired
	}

	// List already-uploaded parts
	uploaded, err := b.s3.ListParts(ctx, upload.S3Key, upload.S3UploadID)
	if err != nil {
		return nil, fmt.Errorf("list parts: %w", err)
	}

	uploadedSet := make(map[int]bool, len(uploaded))
	for _, p := range uploaded {
		uploadedSet[p.Number] = true
	}

	// Calculate all expected parts
	allParts := s3client.CalcParts(upload.TotalSize, upload.PartSize)

	// Presign only the missing parts
	var urls []*s3client.UploadPartURL
	for _, p := range allParts {
		if uploadedSet[p.Number] {
			continue
		}
		u, err := b.s3.PresignUploadPart(ctx, upload.S3Key, upload.S3UploadID, p.Number, s3client.UploadTTL)
		if err != nil {
			return nil, fmt.Errorf("presign part %d: %w", p.Number, err)
		}
		u.Size = p.Size
		urls = append(urls, u)
	}

	return &UploadPlan{
		UploadID: uploadID,
		Key:      upload.S3Key,
		Parts:    urls,
	}, nil
}

// AbortUpload cancels an in-progress upload.
func (b *Dat9Backend) AbortUpload(ctx context.Context, uploadID string) error {
	upload, err := b.store.GetUpload(uploadID)
	if err != nil {
		return err
	}
	if upload.Status != meta.UploadUploading {
		return meta.ErrUploadNotActive
	}

	b.s3.AbortMultipartUpload(ctx, upload.S3Key, upload.S3UploadID)
	return b.store.AbortUpload(uploadID)
}

// GetUpload returns the upload record.
func (b *Dat9Backend) GetUpload(uploadID string) (*meta.Upload, error) {
	return b.store.GetUpload(uploadID)
}

// ListUploads returns uploads for a given path and status.
func (b *Dat9Backend) ListUploads(path string, status meta.UploadStatus) ([]*meta.Upload, error) {
	path, err := pathutil.Canonicalize(path)
	if err != nil {
		return nil, err
	}
	return b.store.ListUploadsByPath(path, status)
}

// PresignGetObject returns a presigned URL for reading an S3-stored file.
func (b *Dat9Backend) PresignGetObject(ctx context.Context, path string) (string, error) {
	path, err := pathutil.Canonicalize(path)
	if err != nil {
		return "", err
	}
	nf, err := b.store.Stat(path)
	if err != nil {
		return "", err
	}
	if nf.File == nil {
		return "", fmt.Errorf("no file entity for path: %s", path)
	}
	if nf.File.StorageType != meta.StorageS3 {
		return "", fmt.Errorf("file is not S3-stored: %s", path)
	}
	return b.s3.PresignGetObject(ctx, nf.File.StorageRef, s3client.DownloadTTL)
}
