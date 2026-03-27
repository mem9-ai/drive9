// Package s3client defines the S3-compatible object store interface for dat9.
// Plan 9 philosophy: S3 is just another file server behind an interface.
// P0 implementation uses local filesystem; production uses AWS SDK.
package s3client

import (
	"context"
	"io"
	"time"
)

// Part represents a single part in a multipart upload.
type Part struct {
	Number         int    // 1-based part number
	Size           int64  // part size in bytes
	ETag           string // returned by S3 after upload
	ChecksumSHA256 string // base64-encoded SHA-256, set when client uploads with checksum
}

// UploadPartURL is a presigned URL for uploading one part.
type UploadPartURL struct {
	Number    int       // 1-based part number
	URL       string    // presigned PUT URL
	Size      int64     // expected part size
	ExpiresAt time.Time // URL expiry
}

// MultipartUpload holds the state of an initiated multipart upload.
type MultipartUpload struct {
	UploadID string
	Key      string
}

// S3Client abstracts S3-compatible object store operations.
// Implementations: LocalS3Client (testing), AWSS3Client (production).
type S3Client interface {
	// CreateMultipartUpload initiates a new multipart upload.
	CreateMultipartUpload(ctx context.Context, key string) (*MultipartUpload, error)

	// PresignUploadPart returns a presigned URL for uploading a specific part.
	// partSize is bound into the presigned URL as Content-Length per §11.2.
	PresignUploadPart(ctx context.Context, key, uploadID string, partNumber int, partSize int64, ttl time.Duration) (*UploadPartURL, error)

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
}

// Default presigned URL TTLs per design doc §11.2.
const (
	UploadTTL   = 120 * time.Second
	DownloadTTL = 60 * time.Second
)

// PartSize is the default multipart part size (8MB).
const PartSize = 8 << 20

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
