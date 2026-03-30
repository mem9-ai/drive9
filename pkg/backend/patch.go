package backend

import (
	"context"
	"fmt"
	"time"

	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/metrics"
	"github.com/mem9-ai/dat9/pkg/pathutil"
	"github.com/mem9-ai/dat9/pkg/s3client"
	"go.uber.org/zap"
)

// PatchPlan is returned by InitiatePatchUpload. It tells the client which parts
// to upload (dirty) and provides presigned Range-GET URLs so the client can
// download the original data for those parts, merge local modifications, and
// upload the result. Unchanged parts are copied server-side via S3
// UploadPartCopy — no data flows through the client for those.
type PatchPlan struct {
	UploadID    string              `json:"upload_id"`
	PartSize    int64               `json:"part_size"`
	UploadParts []*PatchUploadPart  `json:"upload_parts"`  // parts the client must upload
	CopiedParts []int               `json:"copied_parts"`  // part numbers copied server-side
}

// PatchUploadPart describes one dirty part that the client needs to upload.
type PatchUploadPart struct {
	Number    int               `json:"number"`
	URL       string            `json:"url"`               // presigned PUT URL for uploading
	Size      int64             `json:"size"`               // expected part size
	Headers   map[string]string `json:"headers,omitempty"`  // required headers for the PUT
	ExpiresAt time.Time         `json:"expires_at"`
	ReadURL     string            `json:"read_url,omitempty"`     // presigned GET URL to download original part data (empty for parts beyond original file)
	ReadHeaders map[string]string `json:"read_headers,omitempty"` // required headers for the GET (e.g. Range, signed into the presigned URL)
}

// InitiatePatchUpload creates a multipart upload for modifying an existing
// large file. Only the dirty parts are uploaded by the client; unchanged parts
// are copied server-side within S3 using UploadPartCopy.
//
// Parameters:
//   - path: the file to patch
//   - newSize: the total size of the file after patching
//   - dirtyParts: 1-based part numbers that the client has modified
func (b *Dat9Backend) InitiatePatchUpload(ctx context.Context, path string, newSize int64, dirtyParts []int) (*PatchPlan, error) {
	start := time.Now()

	path, err := pathutil.Canonicalize(path)
	if err != nil {
		metrics.RecordOperation("backend", "patch_upload", "error", time.Since(start))
		return nil, err
	}
	if b.s3 == nil {
		metrics.RecordOperation("backend", "patch_upload", "error", time.Since(start))
		return nil, fmt.Errorf("S3 not configured")
	}

	// Look up existing file to get its S3 key
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		metrics.RecordOperation("backend", "patch_upload", "error", time.Since(start))
		return nil, fmt.Errorf("stat existing file: %w", err)
	}
	if nf.File == nil || nf.File.StorageType != datastore.StorageS3 {
		metrics.RecordOperation("backend", "patch_upload", "error", time.Since(start))
		return nil, fmt.Errorf("file is not S3-stored: %s", path)
	}

	sourceKey := nf.File.StorageRef
	origSize := nf.File.SizeBytes

	// Enforce one active upload per path
	existing, err := b.store.GetUploadByPath(ctx, path)
	if err == nil && existing != nil {
		metrics.RecordOperation("backend", "patch_upload", "conflict", time.Since(start))
		return nil, datastore.ErrUploadConflict
	}

	// Create new S3 multipart upload (new key — old object stays until confirm)
	fileID := b.genID()
	newS3Key := "blobs/" + fileID

	mpu, err := b.s3.CreateMultipartUpload(ctx, newS3Key)
	if err != nil {
		logger.Error(ctx, "backend_patch_upload_create_mpu_failed", zap.String("path", path), zap.Error(err))
		metrics.RecordOperation("backend", "patch_upload", "error", time.Since(start))
		return nil, fmt.Errorf("create multipart upload: %w", err)
	}

	// Calculate new parts
	newParts := s3client.CalcParts(newSize, s3client.PartSize)

	// Build dirty set for O(1) lookup
	dirtySet := make(map[int]bool, len(dirtyParts))
	for _, p := range dirtyParts {
		dirtySet[p] = true
	}

	// How many parts did the original file have?
	origPartCount := 0
	if origSize > 0 {
		origPartCount = len(s3client.CalcParts(origSize, s3client.PartSize))
	}

	plan := &PatchPlan{
		UploadID: "", // set below after DB insert
		PartSize: s3client.PartSize,
	}

	// Process each part
	for _, p := range newParts {
		if !dirtySet[p.Number] && p.Number <= origPartCount {
			// Unchanged part within original file range → server-side copy
			partStart := int64(p.Number-1) * s3client.PartSize
			partEnd := partStart + p.Size - 1
			// Clamp to original file size (last part may be smaller)
			if partEnd >= origSize {
				partEnd = origSize - 1
			}

			_, err := b.s3.UploadPartCopy(ctx, newS3Key, mpu.UploadID, p.Number, sourceKey, partStart, partEnd)
			if err != nil {
				_ = b.s3.AbortMultipartUpload(ctx, newS3Key, mpu.UploadID)
				logger.Error(ctx, "backend_patch_upload_copy_failed", zap.String("path", path), zap.Int("part", p.Number), zap.Error(err))
				metrics.RecordOperation("backend", "patch_upload", "error", time.Since(start))
				return nil, fmt.Errorf("copy part %d: %w", p.Number, err)
			}
			plan.CopiedParts = append(plan.CopiedParts, p.Number)
		} else {
			// Dirty part or new part beyond original → client must upload
			u, err := b.s3.PresignUploadPart(ctx, newS3Key, mpu.UploadID, p.Number, p.Size, "", s3client.UploadTTL)
			if err != nil {
				_ = b.s3.AbortMultipartUpload(ctx, newS3Key, mpu.UploadID)
				logger.Error(ctx, "backend_patch_upload_presign_failed", zap.String("path", path), zap.Int("part", p.Number), zap.Error(err))
				metrics.RecordOperation("backend", "patch_upload", "error", time.Since(start))
				return nil, fmt.Errorf("presign part %d: %w", p.Number, err)
			}

			pup := &PatchUploadPart{
				Number:    u.Number,
				URL:       u.URL,
				Size:      u.Size,
				Headers:   u.Headers,
				ExpiresAt: u.ExpiresAt,
			}

			// If this part overlaps with the original file, provide a read URL
			// so the client can download the original data for merging.
			if p.Number <= origPartCount {
				partStart := int64(p.Number-1) * s3client.PartSize
				partEnd := partStart + p.Size - 1
				if partEnd >= origSize {
					partEnd = origSize - 1
				}
				readURL, err := b.s3.PresignGetObjectRange(ctx, sourceKey, partStart, partEnd, s3client.DownloadTTL)
				if err != nil {
					logger.Warn(ctx, "backend_patch_upload_presign_read_failed", zap.String("path", path), zap.Int("part", p.Number), zap.Error(err))
					// Non-fatal: client can still upload the full part without merging
				} else {
					pup.ReadURL = readURL
					// AWS presigned URLs with Range bake the header into the
					// signature — the client MUST send the matching Range header.
					pup.ReadHeaders = map[string]string{
						"Range": fmt.Sprintf("bytes=%d-%d", partStart, partEnd),
					}
				}
			}

			plan.UploadParts = append(plan.UploadParts, pup)
		}
	}

	// Insert DB records (same pattern as InitiateUploadWithChecksums)
	now := time.Now()
	uploadID := b.genID()

	if err := b.store.InsertFile(ctx, &datastore.File{
		FileID:      fileID,
		StorageType: datastore.StorageS3,
		StorageRef:  newS3Key,
		SizeBytes:   newSize,
		Revision:    1,
		Status:      datastore.StatusPending,
		CreatedAt:   now,
	}); err != nil {
		_ = b.s3.AbortMultipartUpload(ctx, newS3Key, mpu.UploadID)
		logger.Error(ctx, "backend_patch_upload_insert_file_failed", zap.String("path", path), zap.Error(err))
		metrics.RecordOperation("backend", "patch_upload", "error", time.Since(start))
		return nil, err
	}

	if err := b.store.InsertUpload(ctx, &datastore.Upload{
		UploadID:   uploadID,
		FileID:     fileID,
		TargetPath: path,
		S3UploadID: mpu.UploadID,
		S3Key:      newS3Key,
		TotalSize:  newSize,
		PartSize:   s3client.PartSize,
		PartsTotal: len(newParts),
		Status:     datastore.UploadUploading,
		CreatedAt:  now,
		UpdatedAt:  now,
		ExpiresAt:  now.Add(24 * time.Hour),
	}); err != nil {
		_ = b.s3.AbortMultipartUpload(ctx, newS3Key, mpu.UploadID)
		logger.Error(ctx, "backend_patch_upload_insert_upload_failed", zap.String("path", path), zap.Error(err))
		metrics.RecordOperation("backend", "patch_upload", "error", time.Since(start))
		return nil, err
	}

	plan.UploadID = uploadID
	metrics.RecordOperation("backend", "patch_upload", "ok", time.Since(start))

	logger.Info(ctx, "backend_patch_upload_initiated",
		zap.String("path", path),
		zap.Int64("new_size", newSize),
		zap.Int("total_parts", len(newParts)),
		zap.Int("dirty_parts", len(plan.UploadParts)),
		zap.Int("copied_parts", len(plan.CopiedParts)),
	)

	return plan, nil
}
