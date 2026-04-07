// Package s3client defines the S3-compatible object store interface for dat9.
// Plan 9 philosophy: S3 is just another file server behind an interface.
// P0 implementation uses local filesystem; production uses AWS SDK.
package s3client

import (
	"context"
	"io"
	"time"
)

// ChecksumAlgo identifies the checksum algorithm for a multipart upload.
type ChecksumAlgo string

const (
	ChecksumAlgoNone   ChecksumAlgo = ""       // no checksum algorithm declared
	ChecksumAlgoSHA256 ChecksumAlgo = "SHA256"
	ChecksumAlgoCRC32C ChecksumAlgo = "CRC32C"
)

// Part represents a single part in a multipart upload.
type Part struct {
	Number         int    // 1-based part number
	Size           int64  // part size in bytes
	ETag           string // returned by S3 after upload
	ChecksumSHA256 string // base64-encoded SHA-256, set when client uploads with checksum
	ChecksumCRC32C string // base64-encoded CRC32C, set when client uploads with CRC32C checksum
}

// UploadPartURL is a presigned URL for uploading one part.
type UploadPartURL struct {
	Number         int               `json:"number"`                    // 1-based part number
	URL            string            `json:"url"`                       // presigned PUT URL
	Size           int64             `json:"size"`                      // expected part size
	ChecksumSHA256 string            `json:"checksum_sha256,omitempty"` // expected SHA-256 checksum for signed uploads
	ChecksumCRC32C string            `json:"checksum_crc32c,omitempty"` // expected CRC32C checksum for signed uploads
	Headers        map[string]string `json:"headers,omitempty"`         // required headers for presigned PUT
	ExpiresAt      time.Time         `json:"expires_at"`                // URL expiry
}

// MultipartUpload holds the state of an initiated multipart upload.
type MultipartUpload struct {
	UploadID string
	Key      string
}

// S3Client abstracts S3-compatible object store operations.
// Implementations: LocalS3Client (testing), AWSS3Client (production).
type S3Client interface {
	// CreateMultipartUpload initiates a new multipart upload with the given checksum algorithm.
	CreateMultipartUpload(ctx context.Context, key string, algo ChecksumAlgo) (*MultipartUpload, error)

	// PresignUploadPart returns a presigned URL for uploading a specific part.
	// partSize is bound into the presigned URL as Content-Length per §11.2.
	// algo + checksumValue work together: the value is signed into the URL under the
	// header corresponding to algo. Pass empty checksumValue to skip checksum signing.
	PresignUploadPart(ctx context.Context, key, uploadID string, partNumber int, partSize int64, algo ChecksumAlgo, checksumValue string, ttl time.Duration) (*UploadPartURL, error)

	// CompleteMultipartUpload finalizes the upload with the given parts.
	CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []Part) error

	// AbortMultipartUpload cancels an in-progress multipart upload.
	AbortMultipartUpload(ctx context.Context, key, uploadID string) error

	// ListParts returns the parts that have been uploaded for a multipart upload.
	ListParts(ctx context.Context, key, uploadID string) ([]Part, error)

	// PresignGetObject returns a presigned URL for reading an object.
	PresignGetObject(ctx context.Context, key string, ttl time.Duration) (string, error)

	// PutObject uploads a small object directly (used for testing/fallback).
	PutObject(ctx context.Context, key string, body io.Reader, size int64) error

	// GetObject reads an object's contents.
	GetObject(ctx context.Context, key string) (io.ReadCloser, error)

	// DeleteObject removes an object.
	DeleteObject(ctx context.Context, key string) error

	// UploadPartCopy copies a byte range from an existing S3 object into a part
	// of an in-progress multipart upload. The copy happens server-side within S3
	// — no data flows through the caller. startByte and endByte are inclusive.
	// Returns the ETag of the copied part.
	UploadPartCopy(ctx context.Context, destKey, uploadID string, partNumber int, sourceKey string, startByte, endByte int64) (string, error)

	// PresignGetObjectRange returns a presigned URL for reading a byte range of
	// an object. startByte and endByte are inclusive.
	PresignGetObjectRange(ctx context.Context, key string, startByte, endByte int64, ttl time.Duration) (string, error)
}

// Default presigned URL TTLs per design doc §11.2.
const (
	UploadTTL   = 10 * time.Minute
	DownloadTTL = 10 * time.Minute
)

// PartSize is the default multipart part size (8MB).
const PartSize = 8 << 20

// MinPartSize is the S3 minimum part size (5MB).
const MinPartSize = 5 << 20

// MaxAdaptivePartSize is the upper bound for adaptive part size (512 MiB).
const MaxAdaptivePartSize = 512 << 20

// MaxPartSize is the S3 hard limit for a single multipart part (5 GiB).
const MaxPartSize = 5 << 30

// CalcAdaptivePartSize returns a part size tuned for totalSize.
// Formula: ceil(fileSize / 10000) aligned up to 1 MiB, clamped to [8 MiB, 512 MiB].
// This is the single authoritative implementation — do not duplicate elsewhere.
func CalcAdaptivePartSize(totalSize int64) int64 {
	const align = 1 << 20 // 1 MiB alignment

	// ceil(totalSize / 10000)
	ps := (totalSize + 9999) / 10000

	// Align up to 1 MiB boundary
	ps = ((ps + align - 1) / align) * align

	// Clamp
	if ps < PartSize {
		ps = PartSize // 8 MiB minimum
	}
	if ps > MaxAdaptivePartSize {
		ps = MaxAdaptivePartSize
	}
	return ps
}

// CalcParts computes the number of parts and individual part sizes.
func CalcParts(totalSize int64, partSize int64) []Part {
	if partSize <= 0 {
		partSize = PartSize
	}
	n := int((totalSize + partSize - 1) / partSize)
	parts := make([]Part, n)
	for i := 0; i < n; i++ {
		size := partSize
		if i == n-1 {
			size = totalSize - int64(i)*partSize
		}
		parts[i] = Part{Number: i + 1, Size: size}
	}
	return parts
}
