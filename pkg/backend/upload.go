package backend

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/metrics"
	"github.com/mem9-ai/dat9/pkg/pathutil"
	"github.com/mem9-ai/dat9/pkg/s3client"
	"go.uber.org/zap"
)

// UploadPlan is returned by InitiateUpload for the 202 response.
type UploadPlan struct {
	UploadID string                    `json:"upload_id"`
	Key      string                    `json:"key"`
	PartSize int64                     `json:"part_size"`
	Parts    []*s3client.UploadPartURL `json:"parts"`
}

// ChecksumContract describes the checksum capabilities for v2 uploads.
type ChecksumContract struct {
	Supported []string `json:"supported"`
	Required  bool     `json:"required"`
}

// UploadPlanV2 is returned by InitiateUploadV2 — no presigned URLs.
type UploadPlanV2 struct {
	UploadID         string           `json:"upload_id"`
	Key              string           `json:"key"`
	PartSize         int64            `json:"part_size"`
	TotalParts       int              `json:"total_parts"`
	ExpiresAt        string           `json:"expires_at"`
	Resumable        bool             `json:"resumable"`
	ChecksumContract ChecksumContract `json:"checksum_contract"`
}

// MaxMultipartParts is the S3 hard limit on parts per multipart upload.
const MaxMultipartParts = 10000

// MaxPresignBatch is the maximum number of parts that can be presigned in a single batch request.
const MaxPresignBatch = 500

// PresignChecksum is an optional checksum for a presign request (algorithm-neutral wire format).
type PresignChecksum struct {
	Algorithm string `json:"algorithm"`
	Value     string `json:"value"`
}

// PresignPartEntry is a single entry in a batch presign request.
type PresignPartEntry struct {
	PartNumber int              `json:"part_number"`
	Checksum   *PresignChecksum `json:"checksum,omitempty"`
}

var ErrPartChecksumCountMismatch = errors.New("part checksum count mismatch")

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
	return b.InitiateUploadWithChecksums(ctx, path, totalSize, nil)
}

func (b *Dat9Backend) InitiateUploadWithChecksums(ctx context.Context, path string, totalSize int64, partChecksums []string) (*UploadPlan, error) {
	start := time.Now()

	path, err := pathutil.Canonicalize(path)
	if err != nil {
		logger.Warn(ctx, "backend_initiate_upload_invalid_path", zap.String("path", path), zap.Error(err))
		metrics.RecordOperation("backend", "initiate_upload", "error", time.Since(start))
		return nil, err
	}
	if b.s3 == nil {
		err := fmt.Errorf("S3 not configured")
		logger.Error(ctx, "backend_initiate_upload_s3_missing", zap.String("path", path), zap.Error(err))
		metrics.RecordOperation("backend", "initiate_upload", "error", time.Since(start))
		return nil, err
	}

	// Enforce one active upload per path
	existing, err := b.store.GetUploadByPath(ctx, path)
	if err == nil && existing != nil {
		metrics.RecordOperation("backend", "initiate_upload", "conflict", time.Since(start))
		return nil, datastore.ErrUploadConflict
	}

	fileID := b.genID()
	s3Key := "blobs/" + fileID

	// Create S3 multipart upload
	mpu, err := b.s3.CreateMultipartUpload(ctx, s3Key)
	if err != nil {
		logger.Error(ctx, "backend_initiate_upload_create_multipart_failed", zap.String("path", path), zap.Error(err))
		metrics.RecordOperation("backend", "initiate_upload", "error", time.Since(start))
		return nil, fmt.Errorf("create multipart upload: %w", err)
	}

	// Calculate parts
	parts := s3client.CalcParts(totalSize, s3client.PartSize)
	if len(partChecksums) > 0 && len(partChecksums) != len(parts) {
		metrics.RecordOperation("backend", "initiate_upload", "error", time.Since(start))
		return nil, fmt.Errorf("%w: got %d, expected %d", ErrPartChecksumCountMismatch, len(partChecksums), len(parts))
	}

	// Presign all part URLs
	urls := make([]*s3client.UploadPartURL, len(parts))
	for i, p := range parts {
		checksum := ""
		if len(partChecksums) > 0 {
			checksum = partChecksums[i]
		}
		u, err := b.s3.PresignUploadPart(ctx, s3Key, mpu.UploadID, p.Number, p.Size, checksum, s3client.UploadTTL)
		if err != nil {
			_ = b.s3.AbortMultipartUpload(ctx, s3Key, mpu.UploadID)
			logger.Error(ctx, "backend_initiate_upload_presign_failed", zap.String("path", path), zap.Int("part_number", p.Number), zap.Error(err))
			metrics.RecordOperation("backend", "initiate_upload", "error", time.Since(start))
			return nil, fmt.Errorf("presign part %d: %w", p.Number, err)
		}
		urls[i] = u
	}

	now := time.Now()
	uploadID := b.genID()

	// Insert PENDING file record
	if err := b.store.InsertFile(ctx, &datastore.File{
		FileID:      fileID,
		StorageType: datastore.StorageS3,
		StorageRef:  s3Key,
		SizeBytes:   totalSize,
		Revision:    1,
		Status:      datastore.StatusPending,
		CreatedAt:   now,
	}); err != nil {
		_ = b.s3.AbortMultipartUpload(ctx, s3Key, mpu.UploadID)
		logger.Error(ctx, "backend_initiate_upload_insert_file_failed", zap.String("path", path), zap.Error(err))
		metrics.RecordOperation("backend", "initiate_upload", "error", time.Since(start))
		return nil, err
	}

	// Insert upload record
	if err := b.store.InsertUpload(ctx, &datastore.Upload{
		UploadID:   uploadID,
		FileID:     fileID,
		TargetPath: path,
		S3UploadID: mpu.UploadID,
		S3Key:      s3Key,
		TotalSize:  totalSize,
		PartSize:   s3client.PartSize,
		PartsTotal: len(parts),
		Status:     datastore.UploadUploading,
		CreatedAt:  now,
		UpdatedAt:  now,
		ExpiresAt:  now.Add(24 * time.Hour),
	}); err != nil {
		_ = b.s3.AbortMultipartUpload(ctx, s3Key, mpu.UploadID)
		logger.Error(ctx, "backend_initiate_upload_insert_upload_failed", zap.String("path", path), zap.Error(err))
		metrics.RecordOperation("backend", "initiate_upload", "error", time.Since(start))
		return nil, err
	}
	metrics.RecordOperation("backend", "initiate_upload", "ok", time.Since(start))

	return &UploadPlan{
		UploadID: uploadID,
		Key:      s3Key,
		PartSize: s3client.PartSize,
		Parts:    urls,
	}, nil
}

// InitiateUploadV2 creates a multipart upload with adaptive part size.
// Unlike v1, it does NOT presign any URLs — clients fetch them on demand.
func (b *Dat9Backend) InitiateUploadV2(ctx context.Context, path string, totalSize int64) (*UploadPlanV2, error) {
	start := time.Now()

	path, err := pathutil.Canonicalize(path)
	if err != nil {
		logger.Warn(ctx, "backend_initiate_upload_v2_invalid_path", zap.String("path", path), zap.Error(err))
		metrics.RecordOperation("backend", "initiate_upload_v2", "error", time.Since(start))
		return nil, err
	}
	if b.s3 == nil {
		err := fmt.Errorf("S3 not configured")
		logger.Error(ctx, "backend_initiate_upload_v2_s3_missing", zap.String("path", path), zap.Error(err))
		metrics.RecordOperation("backend", "initiate_upload_v2", "error", time.Since(start))
		return nil, err
	}

	existing, err := b.store.GetUploadByPath(ctx, path)
	if err == nil && existing != nil {
		metrics.RecordOperation("backend", "initiate_upload_v2", "conflict", time.Since(start))
		return nil, datastore.ErrUploadConflict
	}

	partSize := s3client.CalcAdaptivePartSize(totalSize)
	parts := s3client.CalcParts(totalSize, partSize)
	if len(parts) > MaxMultipartParts {
		err := fmt.Errorf("file too large: %d parts exceeds S3 limit of %d", len(parts), MaxMultipartParts)
		logger.Warn(ctx, "backend_initiate_upload_v2_too_many_parts", zap.String("path", path), zap.Int("parts", len(parts)), zap.Int64("total_size", totalSize))
		metrics.RecordOperation("backend", "initiate_upload_v2", "error", time.Since(start))
		return nil, err
	}

	fileID := b.genID()
	s3Key := "blobs/" + fileID

	mpu, err := b.s3.CreateMultipartUpload(ctx, s3Key)
	if err != nil {
		logger.Error(ctx, "backend_initiate_upload_v2_create_multipart_failed", zap.String("path", path), zap.Error(err))
		metrics.RecordOperation("backend", "initiate_upload_v2", "error", time.Since(start))
		return nil, fmt.Errorf("create multipart upload: %w", err)
	}

	now := time.Now()
	uploadID := b.genID()
	expiresAt := now.Add(24 * time.Hour)

	if err := b.store.InsertFile(ctx, &datastore.File{
		FileID:      fileID,
		StorageType: datastore.StorageS3,
		StorageRef:  s3Key,
		SizeBytes:   totalSize,
		Revision:    1,
		Status:      datastore.StatusPending,
		CreatedAt:   now,
	}); err != nil {
		_ = b.s3.AbortMultipartUpload(ctx, s3Key, mpu.UploadID)
		logger.Error(ctx, "backend_initiate_upload_v2_insert_file_failed", zap.String("path", path), zap.Error(err))
		metrics.RecordOperation("backend", "initiate_upload_v2", "error", time.Since(start))
		return nil, err
	}

	if err := b.store.InsertUpload(ctx, &datastore.Upload{
		UploadID:   uploadID,
		FileID:     fileID,
		TargetPath: path,
		S3UploadID: mpu.UploadID,
		S3Key:      s3Key,
		TotalSize:  totalSize,
		PartSize:   partSize,
		PartsTotal: len(parts),
		Status:     datastore.UploadInitiated,
		CreatedAt:  now,
		UpdatedAt:  now,
		ExpiresAt:  expiresAt,
	}); err != nil {
		_ = b.s3.AbortMultipartUpload(ctx, s3Key, mpu.UploadID)
		logger.Error(ctx, "backend_initiate_upload_v2_insert_upload_failed", zap.String("path", path), zap.Error(err))
		metrics.RecordOperation("backend", "initiate_upload_v2", "error", time.Since(start))
		return nil, err
	}
	metrics.RecordOperation("backend", "initiate_upload_v2", "ok", time.Since(start))

	return &UploadPlanV2{
		UploadID:         uploadID,
		Key:              s3Key,
		PartSize:         partSize,
		TotalParts:       len(parts),
		ExpiresAt:        expiresAt.Format(time.RFC3339),
		Resumable:        false,
		ChecksumContract: ChecksumContract{
			Supported: []string{"SHA-256"},
			Required:  false,
		},
	}, nil
}

// ErrUnsupportedAlgorithm is returned when a client supplies a checksum algorithm
// not in ChecksumContract.Supported.
var ErrUnsupportedAlgorithm = fmt.Errorf("unsupported checksum algorithm")

// resolveChecksumSHA256 extracts the SHA-256 value from an optional checksum.
// Phase 1 only supports SHA-256; unsupported algorithms are rejected with
// ErrUnsupportedAlgorithm so the contract stays honest.
func resolveChecksumSHA256(cs *PresignChecksum) (string, error) {
	if cs == nil || cs.Value == "" {
		return "", nil
	}
	if cs.Algorithm == "sha256" || cs.Algorithm == "SHA256" || cs.Algorithm == "SHA-256" {
		return cs.Value, nil
	}
	return "", fmt.Errorf("%w: %s", ErrUnsupportedAlgorithm, cs.Algorithm)
}

// PresignPart presigns a single part URL for an active upload.
// Transitions INITIATED → UPLOADING on first presign.
func (b *Dat9Backend) PresignPart(ctx context.Context, uploadID string, partNumber int, checksum *PresignChecksum) (*s3client.UploadPartURL, error) {
	start := time.Now()

	upload, err := b.store.GetUpload(ctx, uploadID)
	if err != nil {
		metrics.RecordOperation("backend", "presign_part", "error", time.Since(start))
		return nil, err
	}
	if upload.Status != datastore.UploadUploading && upload.Status != datastore.UploadInitiated {
		metrics.RecordOperation("backend", "presign_part", "not_active", time.Since(start))
		return nil, datastore.ErrUploadNotActive
	}
	if time.Now().After(upload.ExpiresAt) {
		_ = b.AbortUploadV2(ctx, uploadID)
		metrics.RecordOperation("backend", "presign_part", "expired", time.Since(start))
		return nil, datastore.ErrUploadExpired
	}
	if upload.Status == datastore.UploadInitiated {
		if err := b.store.TransitionUploadStatus(ctx, uploadID, datastore.UploadInitiated, datastore.UploadUploading); err != nil {
			logger.Error(ctx, "backend_presign_part_status_transition_failed", zap.String("upload_id", uploadID), zap.Error(err))
			metrics.RecordOperation("backend", "presign_part", "error", time.Since(start))
			return nil, err
		}
	}
	if partNumber < 1 || partNumber > upload.PartsTotal {
		metrics.RecordOperation("backend", "presign_part", "error", time.Since(start))
		return nil, fmt.Errorf("invalid part number %d: must be between 1 and %d", partNumber, upload.PartsTotal)
	}

	parts := s3client.CalcParts(upload.TotalSize, upload.PartSize)
	partSize := parts[partNumber-1].Size

	checksumSHA256, err := resolveChecksumSHA256(checksum)
	if err != nil {
		metrics.RecordOperation("backend", "presign_part", "error", time.Since(start))
		return nil, err
	}
	u, err := b.s3.PresignUploadPart(ctx, upload.S3Key, upload.S3UploadID, partNumber, partSize, checksumSHA256, s3client.UploadTTL)
	if err != nil {
		logger.Error(ctx, "backend_presign_part_failed", zap.String("upload_id", uploadID), zap.Int("part_number", partNumber), zap.Error(err))
		metrics.RecordOperation("backend", "presign_part", "error", time.Since(start))
		return nil, fmt.Errorf("presign part %d: %w", partNumber, err)
	}
	metrics.RecordOperation("backend", "presign_part", "ok", time.Since(start))
	return u, nil
}

// PresignParts presigns multiple part URLs for an active upload.
// Transitions INITIATED → UPLOADING on first presign.
func (b *Dat9Backend) PresignParts(ctx context.Context, uploadID string, entries []PresignPartEntry) ([]*s3client.UploadPartURL, error) {
	start := time.Now()

	upload, err := b.store.GetUpload(ctx, uploadID)
	if err != nil {
		metrics.RecordOperation("backend", "presign_parts", "error", time.Since(start))
		return nil, err
	}
	if upload.Status != datastore.UploadUploading && upload.Status != datastore.UploadInitiated {
		metrics.RecordOperation("backend", "presign_parts", "not_active", time.Since(start))
		return nil, datastore.ErrUploadNotActive
	}
	if time.Now().After(upload.ExpiresAt) {
		_ = b.AbortUploadV2(ctx, uploadID)
		metrics.RecordOperation("backend", "presign_parts", "expired", time.Since(start))
		return nil, datastore.ErrUploadExpired
	}
	if len(entries) > MaxPresignBatch {
		metrics.RecordOperation("backend", "presign_parts", "error", time.Since(start))
		return nil, fmt.Errorf("batch too large: %d parts exceeds limit of %d", len(entries), MaxPresignBatch)
	}
	// Reject duplicate part numbers in the batch.
	seen := make(map[int]bool, len(entries))
	for _, e := range entries {
		if seen[e.PartNumber] {
			metrics.RecordOperation("backend", "presign_parts", "error", time.Since(start))
			return nil, fmt.Errorf("duplicate part number %d in batch", e.PartNumber)
		}
		seen[e.PartNumber] = true
	}
	if upload.Status == datastore.UploadInitiated {
		if err := b.store.TransitionUploadStatus(ctx, uploadID, datastore.UploadInitiated, datastore.UploadUploading); err != nil {
			logger.Error(ctx, "backend_presign_parts_status_transition_failed", zap.String("upload_id", uploadID), zap.Error(err))
			metrics.RecordOperation("backend", "presign_parts", "error", time.Since(start))
			return nil, err
		}
	}

	parts := s3client.CalcParts(upload.TotalSize, upload.PartSize)

	urls := make([]*s3client.UploadPartURL, len(entries))
	for i, e := range entries {
		pn := e.PartNumber
		if pn < 1 || pn > upload.PartsTotal {
			metrics.RecordOperation("backend", "presign_parts", "error", time.Since(start))
			return nil, fmt.Errorf("invalid part number %d: must be between 1 and %d", pn, upload.PartsTotal)
		}
		partSize := parts[pn-1].Size
		checksumSHA256, err := resolveChecksumSHA256(e.Checksum)
		if err != nil {
			metrics.RecordOperation("backend", "presign_parts", "error", time.Since(start))
			return nil, err
		}
		u, err := b.s3.PresignUploadPart(ctx, upload.S3Key, upload.S3UploadID, pn, partSize, checksumSHA256, s3client.UploadTTL)
		if err != nil {
			logger.Error(ctx, "backend_presign_parts_failed", zap.String("upload_id", uploadID), zap.Int("part_number", pn), zap.Error(err))
			metrics.RecordOperation("backend", "presign_parts", "error", time.Since(start))
			return nil, fmt.Errorf("presign part %d: %w", pn, err)
		}
		urls[i] = u
	}
	metrics.RecordOperation("backend", "presign_parts", "ok", time.Since(start))
	return urls, nil
}

// CompletePart is a client-supplied part reference for v2 complete.
type CompletePart struct {
	Number int    `json:"number"`
	ETag   string `json:"etag"`
}

// ConfirmUploadV2 validates client-supplied parts against S3, then completes the upload.
func (b *Dat9Backend) ConfirmUploadV2(ctx context.Context, uploadID string, clientParts []CompletePart) error {
	start := time.Now()

	upload, err := b.store.GetUpload(ctx, uploadID)
	if err != nil {
		metrics.RecordOperation("backend", "confirm_upload_v2", "error", time.Since(start))
		return err
	}
	if upload.Status != datastore.UploadUploading {
		metrics.RecordOperation("backend", "confirm_upload_v2", "not_active", time.Since(start))
		return datastore.ErrUploadNotActive
	}
	if time.Now().After(upload.ExpiresAt) {
		_ = b.AbortUploadV2(ctx, uploadID)
		metrics.RecordOperation("backend", "confirm_upload_v2", "expired", time.Since(start))
		return datastore.ErrUploadExpired
	}

	// Validate client-supplied part count
	if len(clientParts) != upload.PartsTotal {
		metrics.RecordOperation("backend", "confirm_upload_v2", "error", time.Since(start))
		return fmt.Errorf("part count mismatch: client sent %d, expected %d", len(clientParts), upload.PartsTotal)
	}

	// Reject duplicate part numbers and validate completeness (all 1..N present)
	clientPartMap := make(map[int]string, len(clientParts))
	for _, cp := range clientParts {
		if _, dup := clientPartMap[cp.Number]; dup {
			metrics.RecordOperation("backend", "confirm_upload_v2", "error", time.Since(start))
			return fmt.Errorf("duplicate part number %d in complete request", cp.Number)
		}
		if cp.Number < 1 || cp.Number > upload.PartsTotal {
			metrics.RecordOperation("backend", "confirm_upload_v2", "error", time.Since(start))
			return fmt.Errorf("invalid part number %d: must be between 1 and %d", cp.Number, upload.PartsTotal)
		}
		clientPartMap[cp.Number] = cp.ETag
	}

	// List uploaded parts from S3
	s3Parts, err := b.s3.ListParts(ctx, upload.S3Key, upload.S3UploadID)
	if err != nil {
		logger.Error(ctx, "backend_confirm_upload_v2_list_parts_failed", zap.String("upload_id", uploadID), zap.Error(err))
		metrics.RecordOperation("backend", "confirm_upload_v2", "error", time.Since(start))
		return fmt.Errorf("list parts: %w", err)
	}

	// Cross-validate: every client part must match an S3 part ETag
	s3PartMap := make(map[int]string, len(s3Parts))
	for _, p := range s3Parts {
		s3PartMap[p.Number] = p.ETag
	}
	for partNum, clientETag := range clientPartMap {
		s3ETag, ok := s3PartMap[partNum]
		if !ok {
			metrics.RecordOperation("backend", "confirm_upload_v2", "error", time.Since(start))
			return fmt.Errorf("part %d not found in S3", partNum)
		}
		if clientETag != s3ETag {
			metrics.RecordOperation("backend", "confirm_upload_v2", "error", time.Since(start))
			return fmt.Errorf("part %d ETag mismatch: client=%q, S3=%q", partNum, clientETag, s3ETag)
		}
	}

	metrics.RecordOperation("backend", "confirm_upload_v2", "ok", time.Since(start))
	// Delegate to common confirm logic (which re-lists parts, verifies sizes, and completes)
	return b.ConfirmUpload(ctx, uploadID)
}

// ConfirmUpload completes the multipart upload and creates the file node.
func (b *Dat9Backend) ConfirmUpload(ctx context.Context, uploadID string) error {
	start := time.Now()

	upload, err := b.store.GetUpload(ctx, uploadID)
	if err != nil {
		metrics.RecordOperation("backend", "confirm_upload", "error", time.Since(start))
		return err
	}
	if upload.Status != datastore.UploadUploading {
		metrics.RecordOperation("backend", "confirm_upload", "not_active", time.Since(start))
		return datastore.ErrUploadNotActive
	}

	// List uploaded parts from S3
	parts, err := b.s3.ListParts(ctx, upload.S3Key, upload.S3UploadID)
	if err != nil {
		logger.Error(ctx, "backend_confirm_upload_list_parts_failed", zap.String("upload_id", uploadID), zap.Error(err))
		metrics.RecordOperation("backend", "confirm_upload", "error", time.Since(start))
		return fmt.Errorf("list parts: %w", err)
	}

	// Verify all parts are present, correctly sized, and have ETags
	if len(parts) != upload.PartsTotal {
		metrics.RecordOperation("backend", "confirm_upload", "incomplete", time.Since(start))
		return fmt.Errorf("incomplete upload: got %d parts, expected %d", len(parts), upload.PartsTotal)
	}
	expectedParts := s3client.CalcParts(upload.TotalSize, upload.PartSize)
	for i, p := range parts {
		if p.Size != expectedParts[i].Size {
			metrics.RecordOperation("backend", "confirm_upload", "error", time.Since(start))
			return fmt.Errorf("part %d size mismatch: got %d, expected %d", p.Number, p.Size, expectedParts[i].Size)
		}
		if p.ETag == "" {
			metrics.RecordOperation("backend", "confirm_upload", "error", time.Since(start))
			return fmt.Errorf("part %d missing ETag", p.Number)
		}
	}

	// Complete S3 multipart upload (idempotent, outside transaction)
	if err := b.s3.CompleteMultipartUpload(ctx, upload.S3Key, upload.S3UploadID, parts); err != nil {
		logger.Error(ctx, "backend_confirm_upload_complete_multipart_failed", zap.String("upload_id", uploadID), zap.Error(err))
		metrics.RecordOperation("backend", "confirm_upload", "error", time.Since(start))
		return fmt.Errorf("complete multipart: %w", err)
	}

	// Atomically: complete upload, ensure parents, create or overwrite node.
	// Overwrite preserves inode identity by updating the existing files row
	// in place so every hard link keeps pointing at the same file_id.
	var oldStorageRef string
	var oldStorageType datastore.StorageType
	var isOverwrite bool
	var confirmedFileID string
	var confirmedRevision int64
	contentType := detectContentType(upload.TargetPath, nil)
	if err := b.store.InTx(ctx, func(tx *sql.Tx) error {
		if err := b.store.CompleteUploadTx(tx, uploadID); err != nil {
			return err
		}
		if err := b.store.EnsureParentDirsTx(tx, upload.TargetPath, b.genID); err != nil {
			return err
		}

		var existingFileID sql.NullString
		err := tx.QueryRow(`SELECT file_id FROM file_nodes WHERE path = ?`, upload.TargetPath).Scan(&existingFileID)
		if err == nil && existingFileID.Valid {
			isOverwrite = true
			confirmedFileID = existingFileID.String

			var oldRef string
			if err := tx.QueryRow(`SELECT storage_type, storage_ref FROM files WHERE file_id = ?`, existingFileID.String).Scan(&oldStorageType, &oldRef); err == nil {
				oldStorageRef = oldRef
			}

			var newRev int64
			if b.UsesDatabaseAutoEmbedding() {
				newRev, err = b.store.UpdateFileContentAutoEmbeddingTx(tx,
					existingFileID.String, datastore.StorageS3, upload.S3Key,
					contentType, "", "", nil, upload.TotalSize,
				)
			} else {
				newRev, err = b.store.UpdateFileContentTx(tx,
					existingFileID.String, datastore.StorageS3, upload.S3Key,
					contentType, "", "", nil, upload.TotalSize,
				)
			}
			if err != nil {
				return err
			}
			confirmedRevision = newRev

			_, err = tx.Exec(`UPDATE files SET status = 'DELETED' WHERE file_id = ?`, upload.FileID)
			if err != nil {
				return err
			}
			// Rebind upload record to the surviving inode so the uploads row
			// never points at a tombstoned file.
			_, err = tx.Exec(`UPDATE uploads SET file_id = ? WHERE upload_id = ?`,
				existingFileID.String, uploadID)
			if err != nil {
				return err
			}
			if b.UsesDatabaseAutoEmbedding() {
				if b.hasAsyncImageTextSource(upload.TargetPath, contentType) {
					return b.enqueueImgExtractTaskTx(tx, confirmedFileID, confirmedRevision, upload.TargetPath, contentType)
				}
				return nil
			}
			if b.shouldEnqueueEmbedForRevision(upload.TargetPath, contentType, "") {
				return b.enqueueEmbedTaskTx(tx, confirmedFileID, confirmedRevision)
			}
			return nil
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		if b.UsesDatabaseAutoEmbedding() {
			if err := b.store.ConfirmPendingFileAutoEmbeddingTx(tx,
				upload.FileID, datastore.StorageS3, upload.S3Key, contentType, upload.TotalSize,
			); err != nil {
				return err
			}
		} else {
			now := time.Now().UTC()
			res, err := tx.Exec(`UPDATE files SET storage_type = ?, storage_ref = ?, content_type = ?,
				size_bytes = ?, checksum_sha256 = NULL, content_text = NULL,
				embedding = NULL, embedding_revision = NULL,
				status = 'CONFIRMED', confirmed_at = ?
				WHERE file_id = ? AND status = 'PENDING'`,
				datastore.StorageS3, upload.S3Key, contentType, upload.TotalSize, now, upload.FileID)
			if err != nil {
				return err
			}
			rowsAffected, err := res.RowsAffected()
			if err != nil {
				return err
			}
			if rowsAffected == 0 {
				return datastore.ErrNotFound
			}
		}
		confirmedFileID = upload.FileID
		confirmedRevision = 1
		if err := b.store.InsertNodeTx(tx, &datastore.FileNode{
			NodeID:     b.genID(),
			Path:       upload.TargetPath,
			ParentPath: pathutil.ParentPath(upload.TargetPath),
			Name:       pathutil.BaseName(upload.TargetPath),
			FileID:     upload.FileID,
			CreatedAt:  time.Now(),
		}); err != nil {
			return err
		}
		if b.UsesDatabaseAutoEmbedding() {
			if b.hasAsyncImageTextSource(upload.TargetPath, contentType) {
				return b.enqueueImgExtractTaskTx(tx, confirmedFileID, confirmedRevision, upload.TargetPath, contentType)
			}
			return nil
		}
		if b.shouldEnqueueEmbedForRevision(upload.TargetPath, contentType, "") {
			return b.enqueueEmbedTaskTx(tx, confirmedFileID, confirmedRevision)
		}
		return nil
	}); err != nil {
		logger.Error(ctx, "backend_confirm_upload_tx_failed", zap.String("upload_id", uploadID), zap.Error(err))
		metrics.RecordOperation("backend", "confirm_upload", "error", time.Since(start))
		return err
	}
	if isOverwrite {
		b.deleteBlobIfS3Ctx(ctx, oldStorageType, oldStorageRef, upload.S3Key)
	}
	// Temporary compatibility: app embedding still relies on the legacy
	// backend-owned image queue until its image task flow also moves to
	// semantic_tasks.
	if b.UsesDatabaseAutoEmbedding() {
		metrics.RecordOperation("backend", "confirm_upload", "ok", time.Since(start))
		return nil
	}
	b.enqueueImageExtractForUpload(ctx, upload, isOverwrite)

	metrics.RecordOperation("backend", "confirm_upload", "ok", time.Since(start))
	return nil
}

// ResumeUpload returns presigned URLs for the missing parts of an in-progress upload.
func (b *Dat9Backend) ResumeUpload(ctx context.Context, uploadID string) (*UploadPlan, error) {
	return b.ResumeUploadWithChecksums(ctx, uploadID, nil)
}

func (b *Dat9Backend) ResumeUploadWithChecksums(ctx context.Context, uploadID string, partChecksums []string) (*UploadPlan, error) {
	start := time.Now()

	upload, err := b.store.GetUpload(ctx, uploadID)
	if err != nil {
		metrics.RecordOperation("backend", "resume_upload", "error", time.Since(start))
		return nil, err
	}
	if upload.Status != datastore.UploadUploading {
		metrics.RecordOperation("backend", "resume_upload", "not_active", time.Since(start))
		return nil, datastore.ErrUploadNotActive
	}

	// Check expiry — best-effort abort of S3 multipart, then mark metadata.
	// S3 lifecycle rules (AbortIncompleteMultipartUpload) handle orphaned parts
	// if the abort call fails transiently.
	if time.Now().After(upload.ExpiresAt) {
		if err := b.s3.AbortMultipartUpload(ctx, upload.S3Key, upload.S3UploadID); err != nil {
			logger.Warn(ctx, "backend_resume_upload_abort_expired_failed", zap.String("upload_id", uploadID), zap.Error(err))
		}
		_ = b.store.AbortUpload(ctx, uploadID)
		metrics.RecordOperation("backend", "resume_upload", "expired", time.Since(start))
		return nil, datastore.ErrUploadExpired
	}

	// List already-uploaded parts
	uploaded, err := b.s3.ListParts(ctx, upload.S3Key, upload.S3UploadID)
	if err != nil {
		logger.Error(ctx, "backend_resume_upload_list_parts_failed", zap.String("upload_id", uploadID), zap.Error(err))
		metrics.RecordOperation("backend", "resume_upload", "error", time.Since(start))
		return nil, fmt.Errorf("list parts: %w", err)
	}

	uploadedSet := make(map[int]bool, len(uploaded))
	for _, p := range uploaded {
		uploadedSet[p.Number] = true
	}

	// Calculate all expected parts
	allParts := s3client.CalcParts(upload.TotalSize, upload.PartSize)
	if len(partChecksums) > 0 && len(partChecksums) != len(allParts) {
		metrics.RecordOperation("backend", "resume_upload", "error", time.Since(start))
		return nil, fmt.Errorf("%w: got %d, expected %d", ErrPartChecksumCountMismatch, len(partChecksums), len(allParts))
	}

	// Presign only the missing parts
	var urls []*s3client.UploadPartURL
	for _, p := range allParts {
		if uploadedSet[p.Number] {
			continue
		}
		checksum := ""
		if len(partChecksums) > 0 {
			checksum = partChecksums[p.Number-1]
		}
		u, err := b.s3.PresignUploadPart(ctx, upload.S3Key, upload.S3UploadID, p.Number, p.Size, checksum, s3client.UploadTTL)
		if err != nil {
			logger.Error(ctx, "backend_resume_upload_presign_failed", zap.String("upload_id", uploadID), zap.Int("part_number", p.Number), zap.Error(err))
			metrics.RecordOperation("backend", "resume_upload", "error", time.Since(start))
			return nil, fmt.Errorf("presign part %d: %w", p.Number, err)
		}
		urls = append(urls, u)
	}

	metrics.RecordOperation("backend", "resume_upload", "ok", time.Since(start))
	return &UploadPlan{
		UploadID: uploadID,
		Key:      upload.S3Key,
		PartSize: upload.PartSize,
		Parts:    urls,
	}, nil
}

// AbortUpload cancels an in-progress upload.
func (b *Dat9Backend) AbortUpload(ctx context.Context, uploadID string) error {
	start := time.Now()
	upload, err := b.store.GetUpload(ctx, uploadID)
	if err != nil {
		metrics.RecordOperation("backend", "abort_upload", "error", time.Since(start))
		return err
	}
	if upload.Status != datastore.UploadUploading {
		metrics.RecordOperation("backend", "abort_upload", "not_active", time.Since(start))
		return datastore.ErrUploadNotActive
	}

	_ = b.s3.AbortMultipartUpload(ctx, upload.S3Key, upload.S3UploadID)
	if err := b.store.AbortUpload(ctx, uploadID); err != nil {
		logger.Error(ctx, "backend_abort_upload_store_failed", zap.String("upload_id", uploadID), zap.Error(err))
		metrics.RecordOperation("backend", "abort_upload", "error", time.Since(start))
		return err
	}
	metrics.RecordOperation("backend", "abort_upload", "ok", time.Since(start))
	return nil
}

// AbortUploadV2 cancels an upload (idempotent — returns nil for not-found or already-aborted).
// Cleans up: aborts S3 multipart, marks upload ABORTED, marks pending file DELETED.
func (b *Dat9Backend) AbortUploadV2(ctx context.Context, uploadID string) error {
	start := time.Now()
	upload, err := b.store.GetUpload(ctx, uploadID)
	if err != nil {
		// Not found → idempotent success
		if errors.Is(err, datastore.ErrNotFound) {
			metrics.RecordOperation("backend", "abort_upload_v2", "ok", time.Since(start))
			return nil
		}
		metrics.RecordOperation("backend", "abort_upload_v2", "error", time.Since(start))
		return err
	}
	// Already terminal → idempotent success
	if upload.Status == datastore.UploadAborted || upload.Status == datastore.UploadCompleted || upload.Status == datastore.UploadExpired {
		metrics.RecordOperation("backend", "abort_upload_v2", "ok", time.Since(start))
		return nil
	}

	_ = b.s3.AbortMultipartUpload(ctx, upload.S3Key, upload.S3UploadID)
	if err := b.store.AbortUploadV2(ctx, uploadID); err != nil {
		logger.Error(ctx, "backend_abort_upload_v2_store_failed", zap.String("upload_id", uploadID), zap.Error(err))
		metrics.RecordOperation("backend", "abort_upload_v2", "error", time.Since(start))
		return err
	}
	// Clean up the pending file row created at initiate time.
	if upload.FileID != "" {
		if err := b.store.MarkFileDeleted(ctx, upload.FileID); err != nil {
			logger.Warn(ctx, "backend_abort_upload_v2_mark_file_deleted_failed", zap.String("upload_id", uploadID), zap.String("file_id", upload.FileID), zap.Error(err))
		}
	}
	metrics.RecordOperation("backend", "abort_upload_v2", "ok", time.Since(start))
	return nil
}

// GetUpload returns the upload record.
func (b *Dat9Backend) GetUpload(ctx context.Context, uploadID string) (*datastore.Upload, error) {
	return b.store.GetUpload(ctx, uploadID)
}

// ListUploads returns uploads for a given path and status.
func (b *Dat9Backend) ListUploads(ctx context.Context, path string, status datastore.UploadStatus) ([]*datastore.Upload, error) {
	path, err := pathutil.Canonicalize(path)
	if err != nil {
		return nil, err
	}
	return b.store.ListUploadsByPath(ctx, path, status)
}

// PresignGetObject returns a presigned URL for reading an S3-stored file.
func (b *Dat9Backend) PresignGetObject(ctx context.Context, path string) (string, error) {
	start := time.Now()
	path, err := pathutil.Canonicalize(path)
	if err != nil {
		metrics.RecordOperation("backend", "presign_get_object", "error", time.Since(start))
		return "", err
	}
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		metrics.RecordOperation("backend", "presign_get_object", "error", time.Since(start))
		return "", err
	}
	if nf.File == nil {
		metrics.RecordOperation("backend", "presign_get_object", "error", time.Since(start))
		return "", fmt.Errorf("no file entity for path: %s", path)
	}
	if nf.File.StorageType != datastore.StorageS3 {
		metrics.RecordOperation("backend", "presign_get_object", "error", time.Since(start))
		return "", fmt.Errorf("file is not S3-stored: %s", path)
	}
	url, err := b.s3.PresignGetObject(ctx, nf.File.StorageRef, s3client.DownloadTTL)
	if err != nil {
		logger.Error(ctx, "backend_presign_get_object_failed", zap.String("path", path), zap.Error(err))
		metrics.RecordOperation("backend", "presign_get_object", "error", time.Since(start))
		return "", err
	}
	metrics.RecordOperation("backend", "presign_get_object", "ok", time.Since(start))
	return url, nil
}
